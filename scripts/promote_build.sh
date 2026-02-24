#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
OUT_DIR="$STATE_DIR/out"
SERVED_DIR="$STATE_DIR/served"

usage() {
  cat <<'USAGE'
Usage:
  scripts/promote_build.sh [BUILD_ID]

If BUILD_ID is not provided, the latest $SPACEGATE_STATE_DIR/out/* directory (by name sort) is promoted.
USAGE
}

relative_path() {
  local from_dir="$1"
  local to_path="$2"
  python3 - "$from_dir" "$to_path" <<'PY'
import os
import sys
print(os.path.relpath(sys.argv[2], sys.argv[1]))
PY
}

select_latest_build() {
  local -a builds=()
  while IFS= read -r name; do
    builds+=("$name")
  done < <(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort)

  if [[ ${#builds[@]} -eq 0 ]]; then
    echo "Error: no builds found in $OUT_DIR" >&2
    exit 1
  fi

  printf '%s' "${builds[-1]}"
}

require_artifacts() {
  local build_dir="$1"
  local core_db="$build_dir/core.duckdb"
  local parquet_dir="$build_dir/parquet"
  local reports_dir="$STATE_DIR/reports/$(basename "$build_dir")"

  if [[ ! -f "$core_db" ]]; then
    echo "Error: missing $core_db" >&2
    exit 1
  fi

  if [[ ! -d "$parquet_dir" ]]; then
    echo "Error: missing parquet directory $parquet_dir" >&2
    exit 1
  fi

  if ! find "$parquet_dir" -maxdepth 1 -type f -name '*.parquet' -print -quit | grep -q .; then
    echo "Error: no parquet files found in $parquet_dir" >&2
    exit 1
  fi

  if [[ ! -d "$reports_dir" ]]; then
    echo "Warning: reports directory not found: $reports_dir" >&2
  fi
}

main() {
  local build_id="${1:-}"
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  if [[ -z "$build_id" ]]; then
    build_id="$(select_latest_build)"
  fi

  local build_dir="$OUT_DIR/$build_id"
  if [[ ! -d "$build_dir" ]]; then
    echo "Error: build directory not found: $build_dir" >&2
    exit 1
  fi

  require_artifacts "$build_dir"

  mkdir -p "$SERVED_DIR"

  # Atomic pointer update: replace the symlink in a single operation.
  local rel_target=""
  rel_target="$(relative_path "$SERVED_DIR" "$build_dir")"
  ln -sfn "$rel_target" "$SERVED_DIR/current"

  printf 'Promoted build %s -> %s/current (%s)\n' "$build_id" "$SERVED_DIR" "$rel_target"
  echo "Next: scripts/verify_build.sh to validate the promoted build."
}

main "$@"
