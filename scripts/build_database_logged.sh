#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
mkdir -p "$LOG_DIR"

ts="$(date -u +"%Y%m%dT%H%M%SZ")"
LOG_FILE="${SPACEGATE_BUILD_LOG:-$LOG_DIR/build_database_${ts}.log}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/build_database_logged.sh [build_database args...]

Runs scripts/build_database.sh with project environment loading and durable logging.
The wrapped build exit code is preserved.

Examples:
  scripts/build_database_logged.sh --full-refresh
  SPACEGATE_BUILD_LOG=/data/spacegate/state/logs/build_database_resume.log scripts/build_database_logged.sh
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

set +e
{
  set -euo pipefail
  echo "==> Spacegate logged database build start $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "root_dir=$ROOT_DIR"
  echo "state_dir=$STATE_DIR"
  echo "log_file=$LOG_FILE"
  echo "args=$*"
  "$ROOT_DIR/scripts/build_database.sh" "$@"
  echo "==> Spacegate logged database build complete $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
} 2>&1 | tee -a "$LOG_FILE"
pipeline_status=("${PIPESTATUS[@]}")
build_status="${pipeline_status[0]}"
tee_status="${pipeline_status[1]}"
set -e

if [[ "$tee_status" -ne 0 ]]; then
  echo "Error: failed to write build log: $LOG_FILE" >&2
  exit "$tee_status"
fi
exit "$build_status"
