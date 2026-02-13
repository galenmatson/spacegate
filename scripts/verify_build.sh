#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
SERVED_DIR="$STATE_DIR/served"
OUT_DIR="$STATE_DIR/out"
REPORTS_DIR="$STATE_DIR/reports"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/verify_build.sh [BUILD_ID]

If BUILD_ID is not provided, the script verifies $SPACEGATE_STATE_DIR/served/current.
USAGE
}

resolve_path() {
  local path="$1"
  if command -v readlink >/dev/null 2>&1; then
    readlink -f "$path" 2>/dev/null || true
  fi
}

main() {
  if [[ -z "$PYTHON_BIN" ]]; then
    if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
      PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
    else
      PYTHON_BIN="python3"
    fi
  fi

  if ! "$PYTHON_BIN" - <<'PY' >/dev/null 2>&1; then
import duckdb
PY
    echo "Error: python module 'duckdb' not found." >&2
    echo "Tip: activate the project venv or install requirements:" >&2
    echo "  cd $ROOT_DIR" >&2
    echo "  python3 -m venv .venv && source .venv/bin/activate" >&2
    echo "  pip install -r requirements.txt" >&2
    exit 1
  fi

  local build_id="${1:-}"
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  local build_dir=""
  if [[ -n "$build_id" ]]; then
    build_dir="$OUT_DIR/$build_id"
  else
    if [[ ! -e "$SERVED_DIR/current" ]]; then
      echo "Error: $SERVED_DIR/current not found" >&2
      exit 1
    fi
    build_dir="$(resolve_path "$SERVED_DIR/current")"
    if [[ -z "$build_dir" ]]; then
      build_dir="$SERVED_DIR/current"
    fi
    build_id="$(basename "$build_dir")"
  fi

  echo "Verify target: $build_dir"

  if [[ ! -d "$build_dir" ]]; then
    echo "Error: build directory not found: $build_dir" >&2
    exit 1
  fi

  local core_db="$build_dir/core.duckdb"
  local parquet_dir="$build_dir/parquet"
  local reports_dir="$REPORTS_DIR/$build_id"

  if [[ ! -f "$core_db" ]]; then
    echo "Error: missing $core_db" >&2
    exit 1
  fi
  echo "OK: core.duckdb"

  if [[ ! -f "$parquet_dir/stars.parquet" || ! -f "$parquet_dir/systems.parquet" || ! -f "$parquet_dir/planets.parquet" ]]; then
    echo "Error: missing parquet files in $parquet_dir" >&2
    exit 1
  fi
  echo "OK: parquet exports"

  if [[ ! -d "$reports_dir" ]]; then
    echo "Error: missing reports directory $reports_dir" >&2
    exit 1
  fi
  echo "OK: reports directory"

  local qc_report="$reports_dir/qc_report.json"
  local match_report="$reports_dir/match_report.json"
  local prov_report="$reports_dir/provenance_report.json"

  if [[ ! -f "$qc_report" || ! -f "$match_report" || ! -f "$prov_report" ]]; then
    echo "Error: missing QC reports in $reports_dir" >&2
    exit 1
  fi
  echo "OK: QC reports"

  "$PYTHON_BIN" - <<'PY' "$qc_report" "$build_id"
import json
import sys
from pathlib import Path

qc_path = Path(sys.argv[1])
build_id = sys.argv[2]

data = json.loads(qc_path.read_text())

if data.get("build_id") != build_id:
    raise SystemExit(f"QC build_id mismatch: {data.get('build_id')} != {build_id}")

violations = data.get("dist_invariant_violations")
if violations is None:
    raise SystemExit("QC report missing dist_invariant_violations")
if violations != 0:
    raise SystemExit(f"Distance invariant violations: {violations}")

counts = data.get("counts") or {}
if not all(counts.get(k, 0) > 0 for k in ("stars", "systems", "planets")):
    raise SystemExit(f"Invalid counts in QC report: {counts}")

print("OK: qc_report.json")
PY

  "$PYTHON_BIN" - <<'PY' "$core_db"
import sys
import duckdb

db_path = sys.argv[1]
con = duckdb.connect(db_path, read_only=True)

rows = con.execute(
    """
    select
      s.system_name,
      st.star_name,
      st.spectral_type_raw,
      p.planet_name,
      s.dist_ly,
      s.ra_deg,
      s.dec_deg,
      p.match_method,
      p.match_confidence
    from planets p
    join stars st on p.star_id = st.star_id
    join systems s on p.system_id = s.system_id
    where p.star_id is not null and p.system_id is not null
    order by random()
    limit 4
    """
).fetchall()

if not rows:
    raise SystemExit("No joined planet/star/system rows found in core.duckdb")

headers = [
    "system_name",
    "star_name",
    "spectral_type_raw",
    "planet_name",
    "dist_ly",
    "ra_deg",
    "dec_deg",
    "match_method",
    "match_confidence",
]

print("OK: sample join")
print("| " + " | ".join(headers) + " |")
print("| " + " | ".join(["---"] * len(headers)) + " |")
for row in rows:
    values = ["" if v is None else str(v) for v in row]
    print("| " + " | ".join(values) + " |")
PY

  echo "Verified build $build_id"
}

main "$@"
