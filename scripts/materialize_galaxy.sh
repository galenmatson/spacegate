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
REPORTS_DIR="$STATE_DIR/reports"

usage() {
  cat <<'USAGE'
Usage:
  scripts/materialize_galaxy.sh [BUILD_ID] [--profile-version vN]

Creates `galaxy.duckdb` alias for an existing full build and stamps layer metadata.
If BUILD_ID is omitted, uses served/current.

Options:
  --profile-version vN   Value for slice_profile_version (default: v1)
USAGE
}

resolve_current_build_id() {
  if [[ ! -e "$SERVED_DIR/current" ]]; then
    echo "Error: served/current not found; pass BUILD_ID explicitly." >&2
    exit 1
  fi
  local resolved
  resolved="$(readlink -f "$SERVED_DIR/current" 2>/dev/null || true)"
  if [[ -z "$resolved" ]]; then
    echo "Error: unable to resolve served/current path." >&2
    exit 1
  fi
  basename "$resolved"
}

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

main() {
  local build_id=""
  local profile_version="v1"
  local arg
  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
      -h|--help)
        usage
        exit 0
        ;;
      --profile-version)
        profile_version="${2:-}"
        shift 2
        ;;
      *)
        if [[ -z "$build_id" ]]; then
          build_id="$arg"
          shift 1
        else
          echo "Unknown argument: $arg" >&2
          usage
          exit 1
        fi
        ;;
    esac
  done

  if [[ -z "$build_id" ]]; then
    build_id="$(resolve_current_build_id)"
  fi

  local build_dir="$OUT_DIR/$build_id"
  local core_db="$build_dir/core.duckdb"
  local galaxy_db="$build_dir/galaxy.duckdb"
  local reports_dir="$REPORTS_DIR/$build_id"
  local report_path="$reports_dir/galaxy_materialization_report.json"

  if [[ ! -d "$build_dir" ]]; then
    echo "Error: build not found: $build_dir" >&2
    exit 1
  fi
  if [[ ! -f "$core_db" ]]; then
    echo "Error: missing core DB: $core_db" >&2
    exit 1
  fi

  mkdir -p "$reports_dir"

  local alias_mode="existing"
  if [[ ! -e "$galaxy_db" ]]; then
    if ln "$core_db" "$galaxy_db" 2>/dev/null; then
      alias_mode="hardlink"
    elif ln -s "core.duckdb" "$galaxy_db" 2>/dev/null; then
      alias_mode="symlink"
    else
      cp -a "$core_db" "$galaxy_db"
      alias_mode="copy"
    fi
  fi

  local metadata_status="ok"
  if ! duckdb "$core_db" -c "
    create table if not exists build_metadata (key text, value text);
    delete from build_metadata where key in (
      'build_layer',
      'slice_profile_id',
      'slice_profile_version',
      'source_galaxy_build_id'
    );
    insert into build_metadata values
      ('build_layer', 'galaxy'),
      ('slice_profile_id', 'galaxy.full'),
      ('slice_profile_version', '$profile_version'),
      ('source_galaxy_build_id', '$build_id');
  " >/dev/null 2>&1; then
    metadata_status="skipped_lock_or_error"
  fi

  cat >"$report_path" <<JSON
{
  "build_id": "$build_id",
  "generated_at_utc": "$(timestamp_utc)",
  "core_db": "$core_db",
  "galaxy_db": "$galaxy_db",
  "alias_mode": "$alias_mode",
  "build_metadata_update": "$metadata_status",
  "slice_profile_id": "galaxy.full",
  "slice_profile_version": "$profile_version"
}
JSON

  echo "Galaxy materialization complete."
  echo "  build_id: $build_id"
  echo "  galaxy_db: $galaxy_db ($alias_mode)"
  echo "  build_metadata_update: $metadata_status"
  echo "  report: $report_path"
}

main "$@"
