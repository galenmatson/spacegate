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
LOG_FILE="$LOG_DIR/refresh_sol_volatile.log"
mkdir -p "$LOG_DIR"
TODAY_UTC="$(date -u +%Y-%m-%d)"
TOMORROW_UTC="$(date -u -d '+1 day' +%Y-%m-%d 2>/dev/null || /bin/date -u -d '+1 day' +%Y-%m-%d)"

log() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$msg" | tee -a "$LOG_FILE"
}

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

normalize_csv() {
  local in_path="$1"
  local out_path="$2"
  mkdir -p "$(dirname "$out_path")"
  "$PYTHON_BIN" - "$in_path" "$out_path" <<'PY'
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
bom = b"\xef\xbb\xbf"

with src.open("rb") as f, dst.open("wb") as out:
    first = f.read(3)
    if first != bom:
        f.seek(0)
    prev_cr = False
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        if prev_cr:
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
            else:
                out.write(b"\n")
            prev_cr = False
        if chunk.endswith(b"\r"):
            prev_cr = True
            chunk = chunk[:-1]
        chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        out.write(chunk)
    if prev_cr:
        out.write(b"\n")
PY
}

log "Sol volatile refresh start"

if [[ "${SPACEGATE_ENABLE_SOL_AUTHORITY:-1}" != "0" ]]; then
  log "Fetch: Sol authority"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_sol_authority.py" \
    --state-dir "$STATE_DIR" \
    --start-time "${SPACEGATE_SOL_AUTHORITY_START_TIME:-2016-01-01}" \
    --stop-time "${SPACEGATE_SOL_AUTHORITY_STOP_TIME:-2016-01-02}" \
    --timeout-s "${SPACEGATE_SOL_AUTHORITY_TIMEOUT_S:-120}" \
    --retries "${SPACEGATE_SOL_AUTHORITY_RETRIES:-4}"
  normalize_csv \
    "$STATE_DIR/raw/sol_authority/sol_system_objects.csv" \
    "$STATE_DIR/cooked/sol_authority/sol_system_objects.csv"
else
  log "Skip Sol authority refresh (SPACEGATE_ENABLE_SOL_AUTHORITY=0)"
fi

if [[ "${SPACEGATE_ENABLE_SOL_ARTIFICIAL:-1}" != "0" ]]; then
  log "Fetch: Sol artificial"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_sol_artificial.py" \
    --state-dir "$STATE_DIR" \
    --start-time "${SPACEGATE_SOL_ARTIFICIAL_START_TIME:-$TODAY_UTC}" \
    --stop-time "${SPACEGATE_SOL_ARTIFICIAL_STOP_TIME:-$TOMORROW_UTC}" \
    --timeout-s "${SPACEGATE_SOL_ARTIFICIAL_TIMEOUT_S:-120}" \
    --retries "${SPACEGATE_SOL_ARTIFICIAL_RETRIES:-4}"
  normalize_csv \
    "$STATE_DIR/raw/sol_artificial/sol_artificial_objects.csv" \
    "$STATE_DIR/cooked/sol_artificial/sol_artificial_objects.csv"
else
  log "Skip Sol artificial refresh (SPACEGATE_ENABLE_SOL_ARTIFICIAL=0)"
fi

report_path="$("$PYTHON_BIN" "$ROOT_DIR/scripts/report_sol_volatile.py" --state-dir "$STATE_DIR")"
log "Sol volatile refresh complete (report=${report_path})"
echo "Next: run scripts/ingest_core.sh and scripts/promote_build.sh when you want refreshed Sol volatile rows in the served build."
