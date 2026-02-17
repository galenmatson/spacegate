#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Server publish locations
DL_ROOT="${SPACEGATE_DL_ROOT:-/srv/spacegate/dl}"
DL_DB_DIR="$DL_ROOT/db"
DL_CURRENT_LINK="$DL_ROOT/current"         # symlink to db/<build>.7z
DL_CURRENT_JSON="$DL_ROOT/current.json"    # metadata file (optional but recommended)

# Spacegate state
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
SERVED_CURRENT="$STATE_DIR/served/current"

require_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1" >&2; exit 1; }; }

main() {
  require_cmd readlink
  require_cmd realpath
  require_cmd sha256sum
  require_cmd stat

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

  mkdir -p "$DL_DB_DIR"

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

  # Metadata (nice for installers + debugging)
  local bytes sha
  bytes="$(stat -c '%s' "$out_archive")"
  sha="$(sha256sum "$out_archive" | awk '{print $1}')"

  cat > "$DL_CURRENT_JSON" <<JSON
{
  "build_id": "$(printf '%s' "$build_id")",
  "file": "$(printf '%s' "$rel_target")",
  "artifact": "$(printf '%s' "$rel_target")",
  "bytes": $bytes,
  "sha256": "$(printf '%s' "$sha")"
}
JSON

  echo "OK:"
  echo "  Archive: $out_archive"
  echo "  Current: $DL_CURRENT_LINK -> $rel_target"
  echo "  Meta:    $DL_CURRENT_JSON"
}

main "$@"
