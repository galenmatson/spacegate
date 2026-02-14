#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
PID_FILE="$LOG_DIR/spacegate_api.pid"
WEB_PID_FILE="$LOG_DIR/spacegate_web.pid"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-$ROOT_DIR/services/api/.venv/bin/python}"
API_DIR="${SPACEGATE_API_DIR:-$ROOT_DIR/services/api}"
HOST="${SPACEGATE_API_HOST:-0.0.0.0}"
PORT="${SPACEGATE_API_PORT:-8000}"
APP_PATH="${SPACEGATE_API_APP:-app.main:app}"
VERIFY_BIN="${SPACEGATE_VERIFY_BIN:-$ROOT_DIR/scripts/verify_build.sh}"
WEB_DIR="${SPACEGATE_WEB_DIR:-$ROOT_DIR/services/web}"
WEB_HOST="${SPACEGATE_WEB_HOST:-0.0.0.0}"
WEB_PORT="${SPACEGATE_WEB_PORT:-5173}"
WEB_ENABLE="${SPACEGATE_WEB_ENABLE:-1}"
VITE_API_PROXY="${VITE_API_PROXY:-http://127.0.0.1:$PORT}"
VITE_API_BASE="${VITE_API_BASE:-}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_spacegate.sh [--restart|--stop|--stop-api|--stop-web|--force]

Starts the Spacegate API with uvicorn.

Options:
  --restart   Stop the previous pidfile-tracked process, then start.
  --stop      Stop the pidfile-tracked API and web processes and exit.
  --stop-api  Stop only the pidfile-tracked API process.
  --stop-web  Stop only the pidfile-tracked web process.
  --force     If pidfile is missing, kill the process bound to the port.
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

ensure_npm() {
  if ! command -v npm >/dev/null 2>&1; then
    echo "Error: npm not found. Install Node.js to run the web UI." >&2
    echo "Tip: run scripts/setup_spacegate.sh to install dependencies." >&2
    exit 1
  fi
}

ensure_app_path() {
  if [[ "$APP_PATH" == *"/"* ]]; then
    echo "Error: SPACEGATE_API_APP should be a module path like app.main:app" >&2
    echo "Got: $APP_PATH" >&2
    exit 1
  fi
}

verify_build() {
  if [[ ! -x "$VERIFY_BIN" ]]; then
    echo "Error: verify script not found at $VERIFY_BIN" >&2
    exit 1
  fi
  local tmp_out
  tmp_out="$(mktemp)"
  if ! "$VERIFY_BIN" >"$tmp_out" 2>&1; then
    echo "Error: build verification failed. Output:" >&2
    cat "$tmp_out" >&2
    rm -f "$tmp_out"
    exit 1
  fi
  rm -f "$tmp_out"
  echo "OK: build verified" >&2
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

read_web_pid() {
  if [[ -f "$WEB_PID_FILE" ]]; then
    cat "$WEB_PID_FILE"
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

stop_web_process() {
  local pid
  pid="$(read_web_pid)"
  if [[ -z "$pid" ]]; then
    echo "No web pidfile found at $WEB_PID_FILE" >&2
    return 1
  fi
  if pid_is_running "$pid"; then
    echo "Stopping web process $pid" >&2
    kill "$pid"
    for _ in {1..20}; do
      if pid_is_running "$pid"; then
        sleep 0.2
      else
        break
      fi
    done
    if pid_is_running "$pid"; then
      echo "Web process $pid did not stop; sending SIGKILL" >&2
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  else
    echo "Web pidfile process not running (stale pid: $pid)" >&2
  fi
  rm -f "$WEB_PID_FILE"
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

pid_from_port() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    local line
    line="$(ss -ltnpH 2>/dev/null | awk -v p=\":$port\" '$4 ~ p || $5 ~ p {print; exit}')"
    if [[ -z "$line" ]]; then
      line="$(ss -ltnpH 2>/dev/null | grep -m1 -F ":$port" || true)"
    fi
    echo "$line" | sed -n 's/.*pid=\\([0-9]\\+\\).*/\\1/p'
  elif command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN | awk 'NR==2{print $2}'
  fi
}

describe_pid() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  ps -p "$pid" -o pid=,comm=,args=
}

main() {
  local action="start"
  local stop_api_only=0
  local stop_web_only=0
  local force_stop=0
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
      --stop-api)
        action="stop"
        stop_api_only=1
        shift 1
        ;;
      --stop-web)
        action="stop"
        stop_web_only=1
        shift 1
        ;;
      --force)
        force_stop=1
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
  ensure_app_path

  if [[ "$action" == "stop" ]]; then
    if [[ $stop_web_only -eq 0 ]]; then
      stop_pidfile_process || true
    fi
    if [[ $stop_api_only -eq 0 && "$WEB_ENABLE" == "1" ]]; then
      stop_web_process || true
    fi

    if [[ $force_stop -eq 1 ]]; then
      if [[ $stop_web_only -eq 0 ]]; then
        api_pid="$(pid_from_port "$PORT" || true)"
        if [[ -n "${api_pid:-}" ]]; then
          echo "Force-stopping process on port $PORT: $(describe_pid "$api_pid")" >&2
          kill "$api_pid" >/dev/null 2>&1 || true
        else
          echo "No process found on port $PORT" >&2
        fi
      fi
      if [[ $stop_api_only -eq 0 && "$WEB_ENABLE" == "1" ]]; then
        web_pid="$(pid_from_port "$WEB_PORT" || true)"
        if [[ -n "${web_pid:-}" ]]; then
          echo "Force-stopping process on port $WEB_PORT: $(describe_pid "$web_pid")" >&2
          kill "$web_pid" >/dev/null 2>&1 || true
        else
          echo "No process found on port $WEB_PORT" >&2
        fi
      fi
    fi
    exit 0
  fi

  if [[ "$action" == "restart" ]]; then
    stop_pidfile_process || true
    if [[ "$WEB_ENABLE" == "1" ]]; then
      stop_web_process || true
    fi
  fi

  verify_build

  if [[ -f "$PID_FILE" ]]; then
    pid="$(read_pid)"
    if pid_is_running "$pid"; then
      echo "Server already running with pid $pid (pidfile $PID_FILE)" >&2
      exit 1
    fi
  fi

  if [[ "$WEB_ENABLE" == "1" && -f "$WEB_PID_FILE" ]]; then
    wpid="$(read_web_pid)"
    if pid_is_running "$wpid"; then
      echo "Web server already running with pid $wpid (pidfile $WEB_PID_FILE)" >&2
      exit 1
    fi
  fi

  if port_in_use "$PORT"; then
    echo "Port $PORT appears to be in use. Refusing to start." >&2
    echo "If this is the Spacegate server and you want to restart it, run with --restart." >&2
    exit 1
  fi

  if [[ "$WEB_ENABLE" == "1" ]]; then
    if port_in_use "$WEB_PORT"; then
      echo "Port $WEB_PORT appears to be in use. Refusing to start web UI." >&2
      exit 1
    fi
  fi

  echo "Starting Spacegate API on $HOST:$PORT" >&2
  echo "Using $PYTHON_BIN" >&2
  echo "PID file: $PID_FILE" >&2

  (
    cd "$API_DIR"
    "$PYTHON_BIN" -m uvicorn "$APP_PATH" --host "$HOST" --port "$PORT"
  ) &
  pid=$!
  echo "$pid" > "$PID_FILE"

  sleep 0.5
  if ! pid_is_running "$pid"; then
    echo "Server failed to start (pid $pid exited). See logs above." >&2
    rm -f "$PID_FILE"
    exit 1
  fi

  echo "Started API pid $(cat "$PID_FILE")" >&2

  if [[ "$WEB_ENABLE" == "1" ]]; then
    ensure_npm
    if [[ ! -d "$WEB_DIR/node_modules" ]]; then
      echo "Error: $WEB_DIR/node_modules not found. Run scripts/setup_spacegate.sh first." >&2
      stop_pidfile_process || true
      exit 1
    fi
    echo "Starting Spacegate Web on $WEB_HOST:$WEB_PORT" >&2
    echo "Web pid file: $WEB_PID_FILE" >&2
    (
      cd "$WEB_DIR"
      VITE_API_PROXY="$VITE_API_PROXY" VITE_API_BASE="$VITE_API_BASE" npm run dev -- --host "$WEB_HOST" --port "$WEB_PORT"
    ) &
    wpid=$!
    echo "$wpid" > "$WEB_PID_FILE"
    sleep 0.5
    if ! pid_is_running "$wpid"; then
      echo "Web server failed to start (pid $wpid exited). See logs above." >&2
      rm -f "$WEB_PID_FILE"
      stop_pidfile_process || true
      exit 1
    fi
    echo "Started Web pid $(cat "$WEB_PID_FILE")" >&2
  fi
}

main "$@"
