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
PROMOTE_ENFORCE_PROFILE_SLO="${SPACEGATE_PROMOTE_ENFORCE_PROFILE_SLO:-0}"
PROMOTE_SLO_BASE_URL="${SPACEGATE_PROMOTE_SLO_BASE_URL:-http://127.0.0.1:8000}"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/promote_build.sh [BUILD_ID]

If BUILD_ID is not provided, the latest promotable $SPACEGATE_STATE_DIR/out/* directory (by name sort) is promoted.
By default this also runs coolness scoring for the promoted build; set SPACEGATE_AUTO_SCORE_COOLNESS=0 to skip.
Profile-specific SLO checks are opt-in for sliced profile builds; set SPACEGATE_PROMOTE_ENFORCE_PROFILE_SLO=1 to enforce.
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
  local best_name=""
  local best_mtime="-1"
  while IFS= read -r name; do
    if [[ "$name" == *.tmp ]]; then
      continue
    fi
    local build_dir="$OUT_DIR/$name"
    if is_promotable_build "$build_dir"; then
      local core_db="$build_dir/core.duckdb"
      local mtime=""
      mtime="$(stat -c '%Y' "$core_db" 2>/dev/null || echo "")"
      if [[ -z "$mtime" ]]; then
        continue
      fi
      if (( mtime > best_mtime )); then
        best_mtime="$mtime"
        best_name="$name"
      fi
    fi
  done < <(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort)

  if [[ -z "$best_name" ]]; then
    echo "Error: no promotable builds found in $OUT_DIR" >&2
    exit 1
  fi

  printf '%s' "$best_name"
}

is_promotable_build() {
  local build_dir="$1"
  local core_db="$build_dir/core.duckdb"
  local arm_db="$build_dir/arm.duckdb"
  local parquet_dir="$build_dir/parquet"

  [[ -f "$core_db" ]] || return 1
  [[ -f "$arm_db" ]] || return 1
  [[ -d "$parquet_dir" ]] || return 1
  find "$parquet_dir" -maxdepth 1 -type f -name '*.parquet' -print -quit | grep -q .
}

resolve_python() {
  if [[ -n "$PYTHON_BIN" ]]; then
    echo "$PYTHON_BIN"
    return
  fi
  if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
    echo "$ROOT_DIR/.venv/bin/python"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return
  fi
  if command -v python >/dev/null 2>&1; then
    echo "python"
    return
  fi
  echo ""
}

read_current_target() {
  if [[ -e "$SERVED_DIR/current" ]]; then
    resolve_path "$SERVED_DIR/current"
  fi
}

resolve_path() {
  local path="$1"
  if command -v readlink >/dev/null 2>&1; then
    readlink -f "$path" 2>/dev/null || true
  fi
}

set_current_symlink() {
  local target="$1"
  local rel_target=""
  rel_target="$(relative_path "$SERVED_DIR" "$target")"
  ln -sfn "$rel_target" "$SERVED_DIR/current"
}

run_profile_slo_gate() {
  local build_id="$1"
  local previous_target="$2"
  if [[ "$PROMOTE_ENFORCE_PROFILE_SLO" != "1" ]]; then
    echo "Skipping profile SLO gate (SPACEGATE_PROMOTE_ENFORCE_PROFILE_SLO=$PROMOTE_ENFORCE_PROFILE_SLO)"
    return 0
  fi

  local py
  py="$(resolve_python)"
  if [[ -z "$py" ]]; then
    echo "Error: python not found; cannot run profile SLO gate." >&2
    return 1
  fi

  local slo_script="$ROOT_DIR/scripts/check_profile_slo.py"
  if [[ ! -x "$slo_script" ]]; then
    echo "Error: missing executable $slo_script" >&2
    return 1
  fi

  echo "Running profile SLO gate for promoted build: $build_id"
  if "$py" "$slo_script" \
    --build-id "$build_id" \
    --state-dir "$STATE_DIR" \
    --base-url "$PROMOTE_SLO_BASE_URL"; then
    echo "Profile SLO gate passed."
    return 0
  fi

  echo "Error: profile SLO gate failed for $build_id." >&2
  if [[ -n "$previous_target" && -d "$previous_target" ]]; then
    set_current_symlink "$previous_target"
    echo "Rolled back served/current to previous build: $(basename "$previous_target")" >&2
  else
    rm -f "$SERVED_DIR/current"
    echo "No previous promoted build available; served/current cleared." >&2
  fi
  return 1
}

require_artifacts() {
  local build_dir="$1"
  local core_db="$build_dir/core.duckdb"
  local arm_db="$build_dir/arm.duckdb"
  local parquet_dir="$build_dir/parquet"
  local reports_dir="$STATE_DIR/reports/$(basename "$build_dir")"

  if [[ ! -f "$core_db" ]]; then
    echo "Error: missing $core_db" >&2
    exit 1
  fi

  if [[ ! -f "$arm_db" ]]; then
    echo "Error: missing $arm_db" >&2
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
  local auto_score_coolness="${SPACEGATE_AUTO_SCORE_COOLNESS:-1}"
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
  local previous_target=""
  previous_target="$(read_current_target)"

  # Atomic pointer update: replace the symlink in a single operation.
  set_current_symlink "$build_dir"
  local rel_target=""
  rel_target="$(relative_path "$SERVED_DIR" "$build_dir")"

  printf 'Promoted build %s -> %s/current (%s)\n' "$build_id" "$SERVED_DIR" "$rel_target"
  run_profile_slo_gate "$build_id" "$previous_target"
  if [[ "$auto_score_coolness" == "1" ]]; then
    echo "Running coolness scoring for promoted build: $build_id"
    "$ROOT_DIR/scripts/score_coolness.sh" score --build-id "$build_id"
  else
    echo "Skipping auto coolness scoring (SPACEGATE_AUTO_SCORE_COOLNESS=$auto_score_coolness)"
  fi
  echo "Next: scripts/verify_build.sh to validate the promoted build."
}

main "$@"
