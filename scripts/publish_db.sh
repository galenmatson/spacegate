#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Server publish locations
DL_ROOT="${SPACEGATE_DL_ROOT:-/srv/spacegate/dl}"
DL_DB_DIR="$DL_ROOT/db"
DL_REPORTS_DIR="$DL_ROOT/reports"
DL_CURRENT_LINK="$DL_ROOT/current"         # symlink to db/<build>.7z
DL_CURRENT_JSON="$DL_ROOT/current.json"    # metadata file (optional but recommended)

# Spacegate state
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-python3}"
SERVED_CURRENT="$STATE_DIR/served/current"

require_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1" >&2; exit 1; }; }

main() {
  require_cmd readlink
  require_cmd realpath
  require_cmd sha256sum
  require_cmd stat
  require_cmd "$PYTHON_BIN"

  if [[ ! -e "$SERVED_CURRENT" ]]; then
    echo "Error: $SERVED_CURRENT not found. Promote a build first." >&2
    exit 1
  fi

  local current_target build_dir build_id
  current_target="$(readlink -f "$SERVED_CURRENT")"
  build_dir="$(realpath "$current_target")"
  build_id="$(basename "$build_dir")"

  if [[ ! -f "$build_dir/core.duckdb" ]]; then
    echo "Error: $build_dir/core.duckdb not found (not a valid promoted build?)" >&2
    exit 1
  fi

  mkdir -p "$DL_DB_DIR" "$DL_REPORTS_DIR"

  local out_archive="$DL_DB_DIR/${build_id}.7z"

  # Prefer 7z if available, otherwise fall back to tar+zstd (more universally scriptable)
  if command -v 7z >/dev/null 2>&1; then
    echo "Publishing: $build_dir -> $out_archive"
    # Store a folder named <build_id>/ in the archive (so extraction is clean)
    (cd "$(dirname "$build_dir")" && 7z a -t7z -mx=9 -mmt=on "$out_archive" "$build_id")
  else
    require_cmd tar
    require_cmd zstd
    out_archive="$DL_DB_DIR/${build_id}.tar.zst"
    echo "7z not found; publishing tar.zst: $build_dir -> $out_archive"
    (cd "$(dirname "$build_dir")" && tar -cf - "$build_id" | zstd -19 -T0 -o "$out_archive")
  fi

  local rel_target
  rel_target="db/$(basename "$out_archive")"
  ln -sfn "$rel_target" "$DL_CURRENT_LINK"

  # Publish reports alongside the archive so bootstrap clients can discover
  # build quality/provenance metadata via current.json.
  local src_reports_dir="$STATE_DIR/reports/$build_id"
  local dst_reports_dir="$DL_REPORTS_DIR/$build_id"
  mkdir -p "$dst_reports_dir"

  local -a report_files=(
    "qc_report.json"
    "match_report.json"
    "provenance_report.json"
    "system_grouping_report.json"
  )
  local report_name
  for report_name in "${report_files[@]}"; do
    if [[ -f "$src_reports_dir/$report_name" ]]; then
      cp -f "$src_reports_dir/$report_name" "$dst_reports_dir/$report_name"
    fi
  done
  if [[ -f "$STATE_DIR/reports/manifests/core_manifest.json" ]]; then
    cp -f "$STATE_DIR/reports/manifests/core_manifest.json" "$dst_reports_dir/core_manifest.json"
  fi

  # Metadata (installers + report discovery + provenance summary)
  local bytes sha
  bytes="$(stat -c '%s' "$out_archive")"
  sha="$(sha256sum "$out_archive" | awk '{print $1}')"

  "$PYTHON_BIN" - "$DL_CURRENT_JSON" "$DL_ROOT" "$build_id" "$rel_target" "$bytes" "$sha" "$dst_reports_dir" <<'PY'
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys

out_json = Path(sys.argv[1])
dl_root = Path(sys.argv[2]).resolve()
build_id = sys.argv[3]
artifact_path = sys.argv[4]
bytes_written = int(sys.argv[5])
artifact_sha = sys.argv[6]
reports_dir = Path(sys.argv[7]).resolve()

meta = {
    "build_id": build_id,
    "file": artifact_path,      # legacy field used by older bootstrap clients
    "artifact": artifact_path,  # preferred field
    "bytes": bytes_written,
    "sha256": artifact_sha,
    "generated_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    "reports": {},
    "summary": {},
    "provenance": {},
}


def rel_from_root(path: Path) -> str:
    try:
        return path.resolve().relative_to(dl_root).as_posix()
    except ValueError:
        return path.name


if reports_dir.exists():
    for report_path in sorted(reports_dir.glob("*.json")):
        data = report_path.read_bytes()
        report_sha = hashlib.sha256(data).hexdigest()
        report_key = report_path.stem
        rel_path = rel_from_root(report_path)
        meta["reports"][report_key] = {
            "path": rel_path,
            "bytes": len(data),
            "sha256": report_sha,
        }

        try:
            payload = json.loads(data.decode("utf-8"))
        except Exception:
            continue

        if report_key == "qc_report" and isinstance(payload, dict):
            meta["summary"]["row_counts"] = payload.get("counts", {})
            meta["summary"]["qc"] = {
                "dist_invariant_violations": payload.get("dist_invariant_violations"),
                "provenance_missing_stars": payload.get("provenance_missing_stars"),
                "morton": payload.get("morton"),
            }
        elif report_key == "match_report" and isinstance(payload, dict):
            counts = payload.get("match_counts", [])
            by_method = {}
            for row in counts:
                if isinstance(row, dict) and "method" in row and "count" in row:
                    by_method[str(row["method"])] = int(row["count"])
            unmatched = by_method.get("unmatched", 0)
            total = sum(by_method.values())
            matched = total - unmatched
            meta["summary"]["match"] = {
                "by_method": by_method,
                "matched": matched,
                "unmatched": unmatched,
                "total": total,
                "match_rate": (matched / total) if total else None,
            }
        elif report_key == "provenance_report" and isinstance(payload, dict):
            athyg = payload.get("athyg", {})
            nasa = payload.get("nasa_exoplanet_archive", {})
            meta["provenance"] = {
                "athyg": {
                    "source_url": athyg.get("source_url"),
                    "part1_sha256": athyg.get("part1", {}).get("sha256"),
                    "part2_sha256": athyg.get("part2", {}).get("sha256"),
                    "retrieved_at": athyg.get("part2", {}).get("retrieved_at") or athyg.get("part1", {}).get("retrieved_at"),
                },
                "nasa_exoplanet_archive": {
                    "source_url": nasa.get("url"),
                    "sha256": nasa.get("sha256"),
                    "retrieved_at": nasa.get("retrieved_at"),
                },
            }

out_json.write_text(json.dumps(meta, indent=2) + "\n")
PY

  echo "OK:"
  echo "  Archive: $out_archive"
  echo "  Current: $DL_CURRENT_LINK -> $rel_target"
  echo "  Meta:    $DL_CURRENT_JSON"
  echo "  Reports: $dst_reports_dir"
}

main "$@"
