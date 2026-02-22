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
LOG_FILE="$LOG_DIR/generate_snapshots.log"

mkdir -p "$LOG_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Error: python not found." >&2
  exit 1
fi

log() {
  local ts msg
  msg="$1"
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$msg" | tee -a "$LOG_FILE"
}

log "Snapshot generation begin"
"$PYTHON_BIN" "$ROOT_DIR/scripts/generate_snapshots.py" "$@" | tee -a "$LOG_FILE"
log "Snapshot generation complete"
