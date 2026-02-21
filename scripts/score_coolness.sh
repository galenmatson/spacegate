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
LOG_FILE="$LOG_DIR/score_coolness.log"

mkdir -p "$LOG_DIR"

PYTHON_BIN=""
if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Error: python3 not found." >&2
  exit 1
fi

log() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$msg" | tee -a "$LOG_FILE"
}

cmd="${1:-score}"
if [[ "$cmd" == "score" || "$cmd" == -* ]]; then
  log "Coolness scoring begin"
  "$PYTHON_BIN" - <<'PY' >/dev/null 2>&1 || {
import duckdb
PY
    echo "Error: python module 'duckdb' not found for $PYTHON_BIN" >&2
    echo "Tip: run scripts/setup_spacegate.sh (or install requirements.txt in the root venv)." >&2
    exit 1
  }
  "$PYTHON_BIN" "$ROOT_DIR/scripts/score_coolness.py" "$@" | tee -a "$LOG_FILE"
  log "Coolness scoring complete"
else
  log "Coolness profile command begin ($cmd)"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/score_coolness.py" "$@" | tee -a "$LOG_FILE"
  log "Coolness profile command complete ($cmd)"
fi
