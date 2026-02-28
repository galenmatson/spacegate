#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
DOCKER_COMPOSE_FILE="${SPACEGATE_DOCKER_COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"
NGINX_CONF="${SPACEGATE_NGINX_CONF:-/etc/nginx/sites-available/spacegate.conf}"
NGINX_ACCESS_LOG="${SPACEGATE_NGINX_ACCESS_LOG:-/var/log/nginx/access.log}"
PUBLIC_URL="${SPACEGATE_STATUS_PUBLIC_URL:-http://127.0.0.1}"
PUBLIC_DOMAIN="${SPACEGATE_PUBLIC_DOMAIN:-coolstars.org}"
CERT_FILE="${SPACEGATE_TLS_CERT_FILE:-/etc/letsencrypt/live/${PUBLIC_DOMAIN}/fullchain.pem}"
WINDOW_MIN=15
TAIL_LINES=20000
NO_COLOR=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/ops_report.sh [--public-url URL] [--window MIN] [--state-dir DIR] [--no-color]

One-shot operations report for Spacegate:
- docker services and ports
- nginx state and upstreams
- API/web health checks
- TLS certificate expiry
- fail2ban jail summary
- recent nginx request/error metrics
- disk + load snapshot

Options:
  --public-url URL Base URL for proxied checks (default: http://127.0.0.1).
  --window MIN     Nginx metrics lookback in minutes (default: 15).
  --state-dir DIR  Override state dir (default from SPACEGATE_STATE_DIR/SPACEGATE_DATA_DIR or ./data).
  --no-color       Disable ANSI color output.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --public-url)
      PUBLIC_URL="${2:-}"
      shift 2
      ;;
    --window)
      WINDOW_MIN="${2:-}"
      shift 2
      ;;
    --state-dir)
      STATE_DIR="${2:-}"
      shift 2
      ;;
    --no-color)
      NO_COLOR=1
      shift
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
else
  C_RESET=$'\033[0m'
  C_DIM=$'\033[2m'
  C_CYAN=$'\033[38;5;51m'
  C_MAGENTA=$'\033[38;5;201m'
  C_GREEN=$'\033[38;5;82m'
  C_YELLOW=$'\033[38;5;220m'
  C_RED=$'\033[38;5;197m'
fi

cmd_exists() {
  command -v "$1" >/dev/null 2>&1
}

badge() {
  local state="${1:-unknown}"
  case "$state" in
    ok|up|healthy|200)
      printf '%s%s%s' "$C_GREEN" "OK" "$C_RESET"
      ;;
    warn|degraded|301|302)
      printf '%s%s%s' "$C_YELLOW" "WARN" "$C_RESET"
      ;;
    fail|down|error|unhealthy|4*|5*)
      printf '%s%s%s' "$C_RED" "FAIL" "$C_RESET"
      ;;
    *)
      printf '%s%s%s' "$C_DIM" "N/A" "$C_RESET"
      ;;
  esac
}

http_code() {
  local url="$1"
  curl -sS -o /dev/null -m 4 -w '%{http_code}' "$url" 2>/dev/null || true
}

http_body() {
  local url="$1"
  curl -fsS -m 4 "$url" 2>/dev/null || true
}

extract_json_field() {
  local json="$1"
  local field="$2"
  if [[ -z "$json" ]]; then
    echo ""
    return 0
  fi
  python3 - "$json" "$field" <<'PY' 2>/dev/null || true
import json
import sys
raw = sys.argv[1]
field = sys.argv[2]
try:
    data = json.loads(raw)
except Exception:
    print("")
    raise SystemExit(0)
print(data.get(field, ""))
PY
}

nginx_active() {
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

nginx_server_name() {
  if [[ ! -f "$NGINX_CONF" ]]; then
    echo ""
    return 0
  fi
  awk '$1 == "server_name" {sub(/;$/, "", $2); print $2; exit}' "$NGINX_CONF"
}

nginx_listen() {
  if [[ ! -f "$NGINX_CONF" ]]; then
    echo ""
    return 0
  fi
  awk '$1 == "listen" {sub(/;$/, "", $2); print $2; exit}' "$NGINX_CONF"
}

nginx_api_upstream() {
  if [[ ! -f "$NGINX_CONF" ]]; then
    echo ""
    return 0
  fi
  awk '
    /location \/api\/[[:space:]]*{/ {in_api=1; next}
    in_api && /proxy_pass/ {sub(/;$/, "", $2); print $2; exit}
    in_api && /}/ {in_api=0}
  ' "$NGINX_CONF"
}

nginx_web_mode() {
  if [[ ! -f "$NGINX_CONF" ]]; then
    echo "missing"
    return 0
  fi
  local proxy
  proxy="$(awk '
    /location \/[[:space:]]*{/ {in_root=1; next}
    in_root && /proxy_pass/ {sub(/;$/, "", $2); print $2; exit}
    in_root && /}/ {in_root=0}
  ' "$NGINX_CONF")"
  if [[ -n "$proxy" ]]; then
    echo "proxy:${proxy}"
    return 0
  fi
  local root_dir
  root_dir="$(awk '$1 == "root" {sub(/;$/, "", $2); print $2; exit}' "$NGINX_CONF")"
  if [[ -n "$root_dir" ]]; then
    echo "static:${root_dir}"
    return 0
  fi
  echo "unknown"
}

cert_summary() {
  if ! cmd_exists openssl || [[ ! -f "$CERT_FILE" ]]; then
    echo "state=missing"
    return 0
  fi
  local end issuer subject now_ts end_ts days
  end="$(openssl x509 -in "$CERT_FILE" -noout -enddate 2>/dev/null | sed 's/^notAfter=//')"
  issuer="$(openssl x509 -in "$CERT_FILE" -noout -issuer 2>/dev/null | sed 's/^issuer=//')"
  subject="$(openssl x509 -in "$CERT_FILE" -noout -subject 2>/dev/null | sed 's/^subject=//')"
  if [[ -z "$end" ]]; then
    echo "state=error"
    return 0
  fi
  now_ts="$(date +%s)"
  end_ts="$(date -d "$end" +%s 2>/dev/null || echo 0)"
  if [[ "$end_ts" -le 0 ]]; then
    echo "state=error"
    return 0
  fi
  days="$(( (end_ts - now_ts) / 86400 ))"
  echo "state=ok"
  echo "not_after=$end"
  echo "days_left=$days"
  echo "issuer=$issuer"
  echo "subject=$subject"
}

compose_ps() {
  if ! cmd_exists docker || [[ ! -f "$DOCKER_COMPOSE_FILE" ]]; then
    return 0
  fi
  docker compose -f "$DOCKER_COMPOSE_FILE" ps 2>/dev/null || true
}

fail2ban_jail_status() {
  local jail="$1"
  if ! cmd_exists fail2ban-client; then
    echo "state=missing"
    return 0
  fi
  local out
  if out="$(sudo -n fail2ban-client status "$jail" 2>/dev/null || fail2ban-client status "$jail" 2>/dev/null)"; then
    local current total
    current="$(echo "$out" | sed -n 's/.*Currently banned:[[:space:]]*//p' | head -n1 | tr -d '\r')"
    total="$(echo "$out" | sed -n 's/.*Total banned:[[:space:]]*//p' | head -n1 | tr -d '\r')"
    echo "state=ok"
    echo "current=${current:-0}"
    echo "total=${total:-0}"
  else
    echo "state=unavailable"
  fi
}

nginx_usage_metrics() {
  python3 - "$NGINX_ACCESS_LOG" "$WINDOW_MIN" "$TAIL_LINES" <<'PY'
import datetime as dt
import re
import sys
from collections import Counter
from pathlib import Path

path = Path(sys.argv[1])
window = int(sys.argv[2])
tail_lines = int(sys.argv[3])

if not path.exists():
    print("LOG_READABLE=0")
    print("TOTAL=0")
    print("ERR4=0")
    print("ERR5=0")
    print("TOP_ERRORS=")
    raise SystemExit(0)

try:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
except Exception:
    print("LOG_READABLE=0")
    print("TOTAL=0")
    print("ERR4=0")
    print("ERR5=0")
    print("TOP_ERRORS=")
    raise SystemExit(0)

if len(lines) > tail_lines:
    lines = lines[-tail_lines:]

now = dt.datetime.now(dt.timezone.utc)
cutoff = now - dt.timedelta(minutes=window)
pat = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] "(?P<method>[A-Z]+) (?P<path>\S+) [^"]+" (?P<status>\d{3}) '
)

total = 0
err4 = 0
err5 = 0
errors = Counter()

for line in lines:
    m = pat.match(line)
    if not m:
        continue
    try:
        ts = dt.datetime.strptime(m.group("ts"), "%d/%b/%Y:%H:%M:%S %z")
    except Exception:
        continue
    if ts < cutoff:
        continue
    total += 1
    status = int(m.group("status"))
    if 400 <= status <= 499:
        err4 += 1
        errors[f'{m.group("path")} {status}'] += 1
    elif 500 <= status <= 599:
        err5 += 1
        errors[f'{m.group("path")} {status}'] += 1

top_errors = "; ".join(f"{k} x{v}" for k, v in errors.most_common(5))

print("LOG_READABLE=1")
print(f"TOTAL={total}")
print(f"ERR4={err4}")
print(f"ERR5={err5}")
print(f"TOP_ERRORS={top_errors}")
PY
}

now_utc="$(date -u +"%Y-%m-%d %H:%M:%SZ")"
host_name="$(hostname 2>/dev/null || echo unknown)"

api_direct_code="$(http_code "http://127.0.0.1:8000/api/v1/health")"
api_proxy_code="$(http_code "${PUBLIC_URL%/}/api/v1/health")"
web_proxy_code="$(http_code "${PUBLIC_URL%/}/")"

health_json=""
if [[ "$api_direct_code" == "200" ]]; then
  health_json="$(http_body "http://127.0.0.1:8000/api/v1/health")"
elif [[ "$api_proxy_code" == "200" ]]; then
  health_json="$(http_body "${PUBLIC_URL%/}/api/v1/health")"
fi
build_id="$(extract_json_field "$health_json" "build_id")"
db_path="$(extract_json_field "$health_json" "db_path")"

served_link="$(readlink "$STATE_DIR/served/current" 2>/dev/null || true)"
nginx_state="$(nginx_active)"
server_name="$(nginx_server_name)"
listen_port="$(nginx_listen)"
api_upstream="$(nginx_api_upstream)"
web_mode="$(nginx_web_mode)"

CERT_STATE="missing"
CERT_NOT_AFTER=""
CERT_DAYS_LEFT=""
CERT_ISSUER=""
CERT_SUBJECT=""
while IFS='=' read -r k v; do
  case "$k" in
    state) CERT_STATE="$v" ;;
    not_after) CERT_NOT_AFTER="$v" ;;
    days_left) CERT_DAYS_LEFT="$v" ;;
    issuer) CERT_ISSUER="$v" ;;
    subject) CERT_SUBJECT="$v" ;;
  esac
done < <(cert_summary)

F2B_SSH_STATE="unavailable"
F2B_SSH_CUR="0"
F2B_SSH_TOT="0"
while IFS='=' read -r k v; do
  case "$k" in
    state) F2B_SSH_STATE="$v" ;;
    current) F2B_SSH_CUR="$v" ;;
    total) F2B_SSH_TOT="$v" ;;
  esac
done < <(fail2ban_jail_status "sshd")

F2B_PROBE_STATE="unavailable"
F2B_PROBE_CUR="0"
F2B_PROBE_TOT="0"
while IFS='=' read -r k v; do
  case "$k" in
    state) F2B_PROBE_STATE="$v" ;;
    current) F2B_PROBE_CUR="$v" ;;
    total) F2B_PROBE_TOT="$v" ;;
  esac
done < <(fail2ban_jail_status "nginx-spacegate-probe")

LOG_READABLE=0
TOTAL=0
ERR4=0
ERR5=0
TOP_ERRORS=""
while IFS='=' read -r k v; do
  case "$k" in
    LOG_READABLE|TOTAL|ERR4|ERR5|TOP_ERRORS)
      printf -v "$k" '%s' "$v"
      ;;
  esac
done < <(nginx_usage_metrics)

disk_line="$(df -h "$STATE_DIR" 2>/dev/null | awk 'NR==2{printf "%s free / %s total (%s used)", $4, $2, $5}' || true)"
if [[ -z "$disk_line" ]]; then
  disk_line="n/a"
fi

load_line="$(uptime 2>/dev/null | sed -n 's/.*load average: //p' || true)"
if [[ -z "$load_line" ]]; then
  load_line="n/a"
fi

printf '%s╔══════════════════════════════════════════════════════════════════════════════╗%s\n' "$C_MAGENTA" "$C_RESET"
printf '%s║%s %-35s %s%s%s %-27s %s║%s\n' "$C_MAGENTA" "$C_RESET" "SPACEGATE OPS REPORT" "$C_CYAN" "scan:" "$C_RESET" "$now_utc" "$C_MAGENTA" "$C_RESET"
printf '%s╚══════════════════════════════════════════════════════════════════════════════╝%s\n' "$C_MAGENTA" "$C_RESET"
printf 'host=%s  state_dir=%s\n' "$host_name" "$STATE_DIR"

printf '%s[%sDOCKER%s]%s compose=%s\n' "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" "$DOCKER_COMPOSE_FILE"
compose_out="$(compose_ps)"
if [[ -n "$compose_out" ]]; then
  echo "$compose_out" | sed 's/^/  /'
else
  echo "  (docker compose status unavailable)"
fi

printf '%s[%sNGINX%s]%s status=%s server_name=%s listen=%s\n' \
  "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" "$(badge "$nginx_state")" "${server_name:-n/a}" "${listen_port:-n/a}"
printf '  api_upstream=%s\n' "${api_upstream:-n/a}"
printf '  web_mode=%s\n' "$web_mode"

printf '%s[%sHEALTH%s]%s api_direct=%s(%s) api_proxy=%s(%s) web_proxy=%s(%s)\n' \
  "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" \
  "$(badge "$api_direct_code")" "${api_direct_code:-000}" \
  "$(badge "$api_proxy_code")" "${api_proxy_code:-000}" \
  "$(badge "$web_proxy_code")" "${web_proxy_code:-000}"
printf '  build_id=%s\n' "${build_id:-n/a}"
printf '  db_path=%s\n' "${db_path:-n/a}"
printf '  served/current -> %s\n' "${served_link:-n/a}"

cert_badge="N/A"
if [[ "$CERT_STATE" == "ok" ]]; then
  cert_badge="$(badge ok)"
  if [[ "${CERT_DAYS_LEFT:-0}" =~ ^-?[0-9]+$ ]] && (( CERT_DAYS_LEFT < 15 )); then
    cert_badge="$(badge warn)"
  fi
fi
printf '%s[%sTLS%s]%s cert=%s file=%s\n' "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" "$cert_badge" "$CERT_FILE"
if [[ "$CERT_STATE" == "ok" ]]; then
  printf '  not_after=%s  days_left=%s\n' "$CERT_NOT_AFTER" "$CERT_DAYS_LEFT"
  printf '  issuer=%s\n' "${CERT_ISSUER:-n/a}"
  printf '  subject=%s\n' "${CERT_SUBJECT:-n/a}"
fi

printf '%s[%sFAIL2BAN%s]%s sshd=%s(cur=%s total=%s) probe=%s(cur=%s total=%s)\n' \
  "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" \
  "$(badge "$F2B_SSH_STATE")" "$F2B_SSH_CUR" "$F2B_SSH_TOT" \
  "$(badge "$F2B_PROBE_STATE")" "$F2B_PROBE_CUR" "$F2B_PROBE_TOT"

usage_state="ok"
if [[ "$ERR5" != "0" ]]; then
  usage_state="fail"
elif [[ "$ERR4" != "0" ]]; then
  usage_state="warn"
fi
printf '%s[%sNGINX USAGE %sm%s]%s req=%s err4=%s err5=%s %s\n' \
  "$C_CYAN" "$C_MAGENTA" "$WINDOW_MIN" "$C_CYAN" "$C_RESET" "$TOTAL" "$ERR4" "$ERR5" "$(badge "$usage_state")"
if [[ "$LOG_READABLE" == "1" && -n "$TOP_ERRORS" ]]; then
  printf '  top_errors=%s\n' "$TOP_ERRORS"
fi

printf '%s[%sSYSTEM%s]%s disk=%s  load=%s\n' "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" "$disk_line" "$load_line"
