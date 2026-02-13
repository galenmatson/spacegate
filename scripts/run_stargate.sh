#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
PID_FILE="$LOG_DIR/spacegate_api.pid"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-$ROOT_DIR/services/api/.venv/bin/python}"
HOST="${SPACEGATE_API_HOST:-0.0.0.0}"
PORT="${SPACEGATE_API_PORT:-8000}"
APP_PATH="${SPACEGATE_API_APP:-$ROOT_DIR/services/api/app.main:app}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_stargate.sh [--restart|--stop]

Starts the Spacegate API with uvicorn.

Options:
  --restart   Stop the previous pidfile-tracked process, then start.
  --stop      Stop the pidfile-tracked process and exit.
USAGE
}

ensure_python() {
  if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="python3"
  fi
}

ensure_uvicorn() {
  if ! "$PYTHON_BIN" - <<'PY' >/dev/null 2>&1; then
import uvicorn
PY
    echo "Error: uvicorn not found for $PYTHON_BIN" >&2
    echo "Tip: create and install API venv:" >&2
    echo "  cd $ROOT_DIR/services/api" >&2
    echo "  python3 -m venv .venv && source .venv/bin/activate" >&2
    echo "  pip install -r requirements.txt" >&2
    exit 1
  fi
}

pid_is_running() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

read_pid() {
  if [[ -f "$PID_FILE" ]]; then
    cat "$PID_FILE"
  fi
}

stop_pidfile_process() {
  local pid
  pid="$(read_pid)"
  if [[ -z "$pid" ]]; then
    echo "No pidfile found at $PID_FILE" >&2
    return 1
  fi
  if pid_is_running "$pid"; then
    echo "Stopping process $pid" >&2
    kill "$pid"
    for _ in {1..20}; do
      if pid_is_running "$pid"; then
        sleep 0.2
      else
        break
      fi
    done
    if pid_is_running "$pid"; then
      echo "Process $pid did not stop; sending SIGKILL" >&2
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  else
    echo "Pidfile process not running (stale pid: $pid)" >&2
  fi
  rm -f "$PID_FILE"
}

port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -lnt | awk '{print $4}' | grep -q ":$port$"
  elif command -v lsof >/dev/null 2>&1; then
    lsof -iTCP -sTCP:LISTEN -P | awk '{print $9}' | grep -q ":$port$"
  else
    return 1
  fi
}

main() {
  local action="start"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --restart)
        action="restart"
        shift 1
        ;;
      --stop)
        action="stop"
        shift 1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done

  mkdir -p "$LOG_DIR"
  ensure_python
  ensure_uvicorn

  if [[ "$action" == "stop" ]]; then
    stop_pidfile_process
    exit 0
  fi

  if [[ "$action" == "restart" ]]; then
    stop_pidfile_process || true
  fi

  if [[ -f "$PID_FILE" ]]; then
    pid="$(read_pid)"
    if pid_is_running "$pid"; then
      echo "Server already running with pid $pid (pidfile $PID_FILE)" >&2
      exit 1
    fi
  fi

  if port_in_use "$PORT"; then
    echo "Port $PORT appears to be in use. Refusing to start." >&2
    echo "If this is the Spacegate server and you want to restart it, run with --restart." >&2
    exit 1
  fi

  echo "Starting Spacegate API on $HOST:$PORT" >&2
  echo "Using $PYTHON_BIN" >&2
  echo "PID file: $PID_FILE" >&2

  "$PYTHON_BIN" -m uvicorn "$APP_PATH" --host "$HOST" --port "$PORT" &
  echo $! > "$PID_FILE"

  echo "Started pid $(cat "$PID_FILE")" >&2
}

main "$@"
