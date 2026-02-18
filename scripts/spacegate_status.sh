#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_load_env_defaults "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
DOCKER_COMPOSE_FILE="${SPACEGATE_DOCKER_COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"
NGINX_CONF="${SPACEGATE_NGINX_CONF:-/etc/nginx/sites-available/spacegate.conf}"
NGINX_ACCESS_LOG="${SPACEGATE_NGINX_ACCESS_LOG:-/var/log/nginx/access.log}"
PUBLIC_URL="${SPACEGATE_STATUS_PUBLIC_URL:-http://127.0.0.1}"
WINDOW_MIN=15
TAIL_LINES=20000
WATCH_SEC=0
NO_COLOR=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/spacegate_status.sh [--watch SEC] [--window MIN] [--public-url URL] [--no-color]

One-screen Spacegate runtime monitor:
- docker/host process status
- nginx mode and health checks
- build ID / served pointer
- recent nginx usage and error counts

Options:
  --watch SEC      Refresh every SEC seconds.
  --window MIN     Nginx metrics lookback window in minutes (default: 15).
  --public-url URL Base URL for proxied checks (default: http://127.0.0.1).
  --no-color       Disable ANSI color output.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --watch)
      WATCH_SEC="${2:-}"
      shift 2
      ;;
    --window)
      WINDOW_MIN="${2:-}"
      shift 2
      ;;
    --public-url)
      PUBLIC_URL="${2:-}"
      shift 2
      ;;
    --no-color)
      NO_COLOR=1
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

if [[ "${NO_COLOR}" -eq 1 || ! -t 1 ]]; then
  C_RESET=""
  C_DIM=""
  C_CYAN=""
  C_MAGENTA=""
  C_GREEN=""
  C_YELLOW=""
  C_RED=""
  C_BLUE=""
else
  C_RESET=$'\033[0m'
  C_DIM=$'\033[2m'
  C_CYAN=$'\033[38;5;51m'
  C_MAGENTA=$'\033[38;5;201m'
  C_GREEN=$'\033[38;5;82m'
  C_YELLOW=$'\033[38;5;220m'
  C_RED=$'\033[38;5;197m'
  C_BLUE=$'\033[38;5;39m'
fi

cmd_exists() {
  command -v "$1" >/dev/null 2>&1
}

badge() {
  local state="${1:-unknown}"
  case "$state" in
    ok|up|running|200)
      printf '%s%s%s' "$C_GREEN" "OK" "$C_RESET"
      ;;
    warn|degraded|partial)
      printf '%s%s%s' "$C_YELLOW" "WARN" "$C_RESET"
      ;;
    fail|down|stopped|error)
      printf '%s%s%s' "$C_RED" "FAIL" "$C_RESET"
      ;;
    *)
      printf '%s%s%s' "$C_DIM" "N/A" "$C_RESET"
      ;;
  esac
}

http_code() {
  local url="$1"
  curl -sS -o /dev/null -m 3 -w '%{http_code}' "$url" 2>/dev/null || true
}

http_body() {
  local url="$1"
  curl -fsS -m 3 "$url" 2>/dev/null || true
}

port_state() {
  local port="$1"
  if ! cmd_exists ss; then
    echo "unknown"
    return 0
  fi
  if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":${port}\$"; then
    echo "up"
  else
    echo "down"
  fi
}

docker_services_running() {
  if ! cmd_exists docker || [[ ! -f "$DOCKER_COMPOSE_FILE" ]]; then
    return 0
  fi
  docker compose -f "$DOCKER_COMPOSE_FILE" ps --services --status running 2>/dev/null || true
}

docker_stats_lines() {
  if ! cmd_exists docker; then
    return 0
  fi
  docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}' 2>/dev/null | grep -E '^spacegate-(api|web)-' || true
}

pid_running_from_file() {
  local f="$1"
  if [[ -f "$f" ]]; then
    local p
    p="$(cat "$f" 2>/dev/null || true)"
    if [[ -n "$p" ]] && kill -0 "$p" >/dev/null 2>&1; then
      echo "$p"
      return 0
    fi
  fi
  echo ""
}

nginx_is_active() {
  if ! cmd_exists systemctl; then
    echo "unknown"
    return 0
  fi
  if systemctl is-active --quiet nginx 2>/dev/null; then
    echo "up"
  else
    echo "down"
  fi
}

nginx_field() {
  local pattern="$1"
  if [[ ! -f "$NGINX_CONF" ]]; then
    echo ""
    return 0
  fi
  awk -v p="$pattern" '$1 == p {gsub(";", "", $2); print $2; exit}' "$NGINX_CONF" 2>/dev/null || true
}

nginx_api_upstream() {
  if [[ ! -f "$NGINX_CONF" ]]; then
    echo ""
    return 0
  fi
  awk '
    /location \/api\/[[:space:]]*{/ {in_api=1; next}
    in_api && /proxy_pass/ {gsub(";", "", $2); print $2; exit}
    in_api && /}/ {in_api=0}
  ' "$NGINX_CONF" 2>/dev/null || true
}

nginx_web_mode() {
  if [[ ! -f "$NGINX_CONF" ]]; then
    echo "missing"
    return 0
  fi
  local proxy
  proxy="$(awk '
    /location \/[[:space:]]*{/ {in_root=1; next}
    in_root && /proxy_pass/ {gsub(";", "", $2); print $2; exit}
    in_root && /}/ {in_root=0}
  ' "$NGINX_CONF" 2>/dev/null || true)"
  if [[ -n "$proxy" ]]; then
    echo "proxy:${proxy}"
    return 0
  fi
  local root_dir
  root_dir="$(nginx_field root)"
  if [[ -n "$root_dir" ]]; then
    echo "static:${root_dir}"
    return 0
  fi
  echo "unknown"
}

extract_build_id() {
  local json="$1"
  if [[ -z "$json" ]]; then
    echo ""
    return 0
  fi
  python3 - "$json" <<'PY' 2>/dev/null || true
import json
import sys
raw = sys.argv[1]
try:
    data = json.loads(raw)
except Exception:
    print("")
    raise SystemExit(0)
print(data.get("build_id", ""))
PY
}

extract_db_path() {
  local json="$1"
  if [[ -z "$json" ]]; then
    echo ""
    return 0
  fi
  python3 - "$json" <<'PY' 2>/dev/null || true
import json
import sys
raw = sys.argv[1]
try:
    data = json.loads(raw)
except Exception:
    print("")
    raise SystemExit(0)
print(data.get("db_path", ""))
PY
}

nginx_metrics_assignments() {
  python3 - "$NGINX_ACCESS_LOG" "$WINDOW_MIN" "$TAIL_LINES" <<'PY'
import datetime
import re
import sys
from collections import deque
from time import time

log_path = sys.argv[1]
window_min = int(sys.argv[2])
tail_lines = int(sys.argv[3])
window_sec = window_min * 60

out = {
    "LOG_READABLE": "0",
    "TOTAL_REQ": "0",
    "API_REQ": "0",
    "SEARCH_REQ": "0",
    "ERR_4XX": "0",
    "ERR_5XX": "0",
    "UNIQ_IP": "0",
}

try:
    with open(log_path, "r", encoding="utf-8", errors="ignore") as fh:
        lines = deque(fh, maxlen=tail_lines)
except Exception:
    for k, v in out.items():
        print(f"{k}={v}")
    raise SystemExit(0)

out["LOG_READABLE"] = "1"
now = time()
pattern = re.compile(
    r'^(?P<ip>\S+)\s+\S+\s+\S+\s+\[(?P<ts>[^\]]+)\]\s+"[A-Z]+\s+(?P<path>\S+)\s+[^"]+"\s+(?P<code>\d{3})'
)

total = 0
api = 0
search = 0
err4 = 0
err5 = 0
ips = set()

for line in lines:
    m = pattern.match(line)
    if not m:
        continue
    ts = m.group("ts")
    try:
        dt = datetime.datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z")
    except ValueError:
        continue
    if now - dt.timestamp() > window_sec:
        continue
    code = int(m.group("code"))
    path = m.group("path")
    total += 1
    ips.add(m.group("ip"))
    if path.startswith("/api/"):
        api += 1
    if path.startswith("/api/v1/systems/search"):
        search += 1
    if 400 <= code <= 499:
        err4 += 1
    if code >= 500:
        err5 += 1

out["TOTAL_REQ"] = str(total)
out["API_REQ"] = str(api)
out["SEARCH_REQ"] = str(search)
out["ERR_4XX"] = str(err4)
out["ERR_5XX"] = str(err5)
out["UNIQ_IP"] = str(len(ips))

for k, v in out.items():
    print(f"{k}={v}")
PY
}

render() {
  local now_utc
  now_utc="$(date -u +"%Y-%m-%d %H:%M:%SZ")"

  local compose_running
  compose_running="$(docker_services_running)"
  local compose_api="down"
  local compose_web="down"
  if echo "$compose_running" | grep -qx 'api'; then
    compose_api="up"
  fi
  if echo "$compose_running" | grep -qx 'web'; then
    compose_web="up"
  fi

  local host_api_pid host_web_pid
  host_api_pid="$(pid_running_from_file "$LOG_DIR/spacegate_api.pid")"
  host_web_pid="$(pid_running_from_file "$LOG_DIR/spacegate_web.pid")"

  local runtime_mode="unknown"
  if [[ "$compose_api" == "up" || "$compose_web" == "up" ]]; then
    runtime_mode="docker"
  elif [[ -n "$host_api_pid" || -n "$host_web_pid" ]]; then
    runtime_mode="host"
  else
    runtime_mode="down"
  fi

  local p8000 p8081 p5173 nginx_state
  p8000="$(port_state 8000)"
  p8081="$(port_state 8081)"
  p5173="$(port_state 5173)"
  nginx_state="$(nginx_is_active)"

  local api_direct_code api_proxy_code web_proxy_code
  api_direct_code="$(http_code "http://127.0.0.1:8000/api/v1/health")"
  api_proxy_code="$(http_code "${PUBLIC_URL%/}/api/v1/health")"
  web_proxy_code="$(http_code "${PUBLIC_URL%/}/")"

  local health_json build_id db_path
  if [[ "$api_direct_code" == "200" ]]; then
    health_json="$(http_body "http://127.0.0.1:8000/api/v1/health")"
  elif [[ "$api_proxy_code" == "200" ]]; then
    health_json="$(http_body "${PUBLIC_URL%/}/api/v1/health")"
  else
    health_json=""
  fi
  build_id="$(extract_build_id "$health_json")"
  db_path="$(extract_db_path "$health_json")"

  local served_link
  served_link="$(readlink "$STATE_DIR/served/current" 2>/dev/null || true)"

  local server_name listen_port api_upstream web_mode
  server_name="$(nginx_field server_name)"
  listen_port="$(nginx_field listen)"
  api_upstream="$(nginx_api_upstream)"
  web_mode="$(nginx_web_mode)"

  local LOG_READABLE TOTAL_REQ API_REQ SEARCH_REQ ERR_4XX ERR_5XX UNIQ_IP
  LOG_READABLE=0
  TOTAL_REQ=0
  API_REQ=0
  SEARCH_REQ=0
  ERR_4XX=0
  ERR_5XX=0
  UNIQ_IP=0
  while IFS='=' read -r k v; do
    case "$k" in
      LOG_READABLE|TOTAL_REQ|API_REQ|SEARCH_REQ|ERR_4XX|ERR_5XX|UNIQ_IP)
        printf -v "$k" '%s' "$v"
        ;;
    esac
  done < <(nginx_metrics_assignments)

  local disk_line
  disk_line="$(df -h "$STATE_DIR" 2>/dev/null | awk 'NR==2{printf "%s free / %s total (%s used)", $4, $2, $5}' || true)"
  if [[ -z "$disk_line" ]]; then
    disk_line="n/a"
  fi

  local load_line
  load_line="$(uptime 2>/dev/null | sed -n 's/.*load average: //p' || true)"
  if [[ -z "$load_line" ]]; then
    load_line="n/a"
  fi

  local err_state="ok"
  if [[ "$ERR_5XX" != "0" ]]; then
    err_state="fail"
  elif [[ "$ERR_4XX" != "0" ]]; then
    err_state="warn"
  fi

  printf '%s╔══════════════════════════════════════════════════════════════════════════════╗%s\n' "$C_MAGENTA" "$C_RESET"
  printf '%s║%s %-36s %s%s%s %-27s %s║%s\n' "$C_MAGENTA" "$C_RESET" "SPACEGATE STATUS MATRIX" "$C_CYAN" "scan:" "$C_RESET" "$now_utc" "$C_MAGENTA" "$C_RESET"
  printf '%s╚══════════════════════════════════════════════════════════════════════════════╝%s\n' "$C_MAGENTA" "$C_RESET"

  printf '%s[%sRUNTIME%s]%s mode=%s%s%s  docker(api=%s, web=%s)  host(api_pid=%s web_pid=%s)\n' \
    "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" "$C_BLUE" "$runtime_mode" "$C_RESET" \
    "$compose_api" "$compose_web" "${host_api_pid:-none}" "${host_web_pid:-none}"

  printf '  ports: 8000=%s 8081=%s 5173=%s\n' "$(badge "$p8000")" "$(badge "$p8081")" "$(badge "$p5173")"

  printf '%s[%sNGINX%s]%s status=%s server_name=%s listen=%s\n' \
    "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" "$(badge "$nginx_state")" "${server_name:-n/a}" "${listen_port:-n/a}"
  printf '  api_upstream=%s\n' "${api_upstream:-n/a}"
  printf '  web_mode=%s\n' "$web_mode"

  printf '%s[%sHEALTH%s]%s api_direct=%s api_proxy=%s web_proxy=%s\n' \
    "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" \
    "$(badge "$api_direct_code")(${api_direct_code:-000})" \
    "$(badge "$api_proxy_code")(${api_proxy_code:-000})" \
    "$(badge "$web_proxy_code")(${web_proxy_code:-000})"
  printf '  build_id=%s\n' "${build_id:-n/a}"
  printf '  db_path=%s\n' "${db_path:-n/a}"
  printf '  served/current -> %s\n' "${served_link:-n/a}"

  printf '%s[%sUSAGE %sm%s]%s req=%s api=%s search=%s uniq_ip=%s err4=%s err5=%s %s\n' \
    "$C_CYAN" "$C_MAGENTA" "$WINDOW_MIN" "$C_CYAN" "$C_RESET" \
    "$TOTAL_REQ" "$API_REQ" "$SEARCH_REQ" "$UNIQ_IP" "$ERR_4XX" "$ERR_5XX" "$(badge "$err_state")"
  if [[ "$LOG_READABLE" != "1" ]]; then
    printf '  %snginx access log not readable at %s%s\n' "$C_DIM" "$NGINX_ACCESS_LOG" "$C_RESET"
  fi

  printf '%s[%sSYSTEM%s]%s disk=%s  load=%s\n' "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" "$disk_line" "$load_line"

  local dstats
  dstats="$(docker_stats_lines)"
  if [[ -n "$dstats" ]]; then
    printf '%s[%sDOCKER STATS%s]%s\n' "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET"
    while IFS='|' read -r name cpu mem; do
      [[ -z "$name" ]] && continue
      printf '  %-22s cpu=%-8s mem=%s\n' "$name" "$cpu" "$mem"
    done <<<"$dstats"
  fi

  printf '%s\n' "${C_DIM}Tip: use --watch 2 for live mode.${C_RESET}"
}

if [[ "$WATCH_SEC" =~ ^[0-9]+$ ]] && (( WATCH_SEC > 0 )); then
  while true; do
    clear
    render
    sleep "$WATCH_SEC"
  done
else
  render
fi
