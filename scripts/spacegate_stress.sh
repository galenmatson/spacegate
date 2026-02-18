#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
ARTIFACT_ROOT="${SPACEGATE_STRESS_ARTIFACT_ROOT:-$LOG_DIR/stress}"
DOCKER_COMPOSE_FILE="${SPACEGATE_DOCKER_COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"

PROFILE="mixed"
BASE_URL="${SPACEGATE_STRESS_URL:-http://127.0.0.1}"
DURATION_SEC=""
CONCURRENCY=""
PEAK_CONCURRENCY=""
TIMEOUT_SEC="${SPACEGATE_STRESS_TIMEOUT_SEC:-4}"
ERROR_THRESHOLD_PCT=""
P95_THRESHOLD_MS=""
NO_COLOR=0
TAIL_LINES=20000

usage() {
  cat <<'USAGE'
Usage:
  scripts/spacegate_stress.sh [options]

Profiles:
  smoke         short confidence check
  mixed         balanced read traffic (default)
  search-heavy  mostly /api/v1/systems/search
  sustain       longer stable-load run
  spike         burst traffic with peak concurrency

Options:
  --profile NAME            Profile to run.
  --url URL                 Base URL to test (default: http://127.0.0.1).
  --duration SEC            Override duration.
  --concurrency N           Override worker count.
  --peak-concurrency N      Spike peak worker count.
  --timeout SEC             Per-request timeout (default: 4).
  --error-threshold PCT     Fail if error rate exceeds this percent.
  --p95-threshold-ms MS     Fail if p95 latency exceeds this many ms.
  --no-color                Disable ANSI color output.
  --help                    Show help.

Artifacts:
  data/logs/stress/<timestamp>_<profile>/
    - requests.csv
    - summary.json
    - docker_stats.log (if available)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --url)
      BASE_URL="${2:-}"
      shift 2
      ;;
    --duration)
      DURATION_SEC="${2:-}"
      shift 2
      ;;
    --concurrency)
      CONCURRENCY="${2:-}"
      shift 2
      ;;
    --peak-concurrency)
      PEAK_CONCURRENCY="${2:-}"
      shift 2
      ;;
    --timeout)
      TIMEOUT_SEC="${2:-}"
      shift 2
      ;;
    --error-threshold)
      ERROR_THRESHOLD_PCT="${2:-}"
      shift 2
      ;;
    --p95-threshold-ms)
      P95_THRESHOLD_MS="${2:-}"
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

if [[ "$NO_COLOR" -eq 1 || ! -t 1 ]]; then
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

need_cmd() {
  local c="$1"
  if ! command -v "$c" >/dev/null 2>&1; then
    echo "Missing required command: $c" >&2
    exit 1
  fi
}

need_cmd curl
need_cmd python3

profile_defaults() {
  REQUEST_MODE="mixed"
  case "$PROFILE" in
    smoke)
      DEFAULT_DURATION=30
      DEFAULT_CONCURRENCY=8
      DEFAULT_ERROR_THRESHOLD=3
      DEFAULT_P95_MS=1800
      REQUEST_MODE="smoke"
      ;;
    mixed)
      DEFAULT_DURATION=120
      DEFAULT_CONCURRENCY=24
      DEFAULT_ERROR_THRESHOLD=2
      DEFAULT_P95_MS=1200
      REQUEST_MODE="mixed"
      ;;
    search-heavy)
      DEFAULT_DURATION=180
      DEFAULT_CONCURRENCY=40
      DEFAULT_ERROR_THRESHOLD=2
      DEFAULT_P95_MS=1500
      REQUEST_MODE="search-heavy"
      ;;
    sustain)
      DEFAULT_DURATION=600
      DEFAULT_CONCURRENCY=32
      DEFAULT_ERROR_THRESHOLD=1
      DEFAULT_P95_MS=1300
      REQUEST_MODE="mixed"
      ;;
    spike)
      DEFAULT_DURATION=120
      DEFAULT_CONCURRENCY=24
      DEFAULT_PEAK_CONCURRENCY=120
      DEFAULT_ERROR_THRESHOLD=3
      DEFAULT_P95_MS=2000
      REQUEST_MODE="mixed"
      ;;
    *)
      echo "Unknown profile: $PROFILE" >&2
      exit 1
      ;;
  esac
}

profile_defaults

DURATION_SEC="${DURATION_SEC:-$DEFAULT_DURATION}"
CONCURRENCY="${CONCURRENCY:-$DEFAULT_CONCURRENCY}"
if [[ "$PROFILE" == "spike" ]]; then
  PEAK_CONCURRENCY="${PEAK_CONCURRENCY:-${DEFAULT_PEAK_CONCURRENCY:-120}}"
else
  PEAK_CONCURRENCY="${PEAK_CONCURRENCY:-$CONCURRENCY}"
fi
ERROR_THRESHOLD_PCT="${ERROR_THRESHOLD_PCT:-$DEFAULT_ERROR_THRESHOLD}"
P95_THRESHOLD_MS="${P95_THRESHOLD_MS:-$DEFAULT_P95_MS}"

for n in "$DURATION_SEC" "$CONCURRENCY" "$PEAK_CONCURRENCY" "$TIMEOUT_SEC"; do
  if [[ ! "$n" =~ ^[0-9]+$ ]] || [[ "$n" -le 0 ]]; then
    echo "Numeric options must be positive integers." >&2
    exit 1
  fi
done

if [[ ! "$ERROR_THRESHOLD_PCT" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--error-threshold must be numeric." >&2
  exit 1
fi
if [[ ! "$P95_THRESHOLD_MS" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--p95-threshold-ms must be numeric." >&2
  exit 1
fi

RUN_ID="$(date -u +%Y-%m-%dT%H%M%SZ)_${PROFILE}"
RUN_DIR="$ARTIFACT_ROOT/$RUN_ID"
WORKER_DIR="$RUN_DIR/workers"
REQ_CSV="$RUN_DIR/requests.csv"
SUMMARY_JSON="$RUN_DIR/summary.json"
DOCKER_STATS_FILE="$RUN_DIR/docker_stats.log"
MONITOR_STOP="$RUN_DIR/.monitor_stop"

mkdir -p "$WORKER_DIR"

pick_endpoint() {
  local mode="$1"
  local r
  r=$((RANDOM % 100))
  case "$mode" in
    search-heavy)
      if (( r < 92 )); then
        printf '/api/v1/systems/search?q=a&limit=100'
      elif (( r < 97 )); then
        printf '/api/v1/systems/search?q=alpha&limit=20'
      else
        printf '/api/v1/health'
      fi
      ;;
    smoke)
      if (( r < 30 )); then
        printf '/'
      elif (( r < 70 )); then
        printf '/api/v1/health'
      elif (( r < 90 )); then
        printf '/api/v1/systems/search?q=a&limit=50'
      else
        printf '/api/v1/systems/search?q=alpha&limit=20'
      fi
      ;;
    mixed|*)
      if (( r < 10 )); then
        printf '/'
      elif (( r < 20 )); then
        printf '/api/v1/health'
      elif (( r < 85 )); then
        printf '/api/v1/systems/search?q=a&limit=50'
      else
        printf '/api/v1/systems/search?q=alpha&limit=20'
      fi
      ;;
  esac
}

worker_loop() {
  local wid="$1"
  local phase="$2"
  local duration="$3"
  local mode="$4"
  local out="$WORKER_DIR/worker_${phase}_${wid}.csv"
  local end_ts
  end_ts=$(( $(date +%s) + duration ))
  : >"$out"
  while (( $(date +%s) < end_ts )); do
    local path url code tsec lat_ms ok ts
    path="$(pick_endpoint "$mode")"
    url="${BASE_URL%/}${path}"
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if response="$(curl -sS -o /dev/null -w '%{http_code},%{time_total}' --max-time "$TIMEOUT_SEC" "$url" 2>/dev/null)"; then
      code="${response%,*}"
      tsec="${response#*,}"
    else
      code="000"
      tsec="$TIMEOUT_SEC"
    fi
    lat_ms="$(awk -v t="$tsec" 'BEGIN{printf "%.3f", (t+0)*1000}')"
    ok=0
    if [[ "$code" =~ ^2[0-9][0-9]$ ]]; then
      ok=1
    fi
    printf '%s,%s,%s,%s,%s,%s\n' "$ts" "$phase" "$path" "$code" "$lat_ms" "$ok" >>"$out"
  done
}

run_phase() {
  local phase="$1"
  local duration="$2"
  local workers="$3"
  local mode="$4"
  local -a pids=()

  printf '%s[%sphase%s]%s %-10s duration=%ss workers=%s\n' \
    "$C_CYAN" "$C_MAGENTA" "$C_CYAN" "$C_RESET" "$phase" "$duration" "$workers"

  local i
  for i in $(seq 1 "$workers"); do
    worker_loop "$i" "$phase" "$duration" "$mode" &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
}

start_docker_monitor() {
  if ! command -v docker >/dev/null 2>&1; then
    return 0
  fi
  if [[ ! -f "$DOCKER_COMPOSE_FILE" ]]; then
    return 0
  fi
  : >"$DOCKER_STATS_FILE"
  (
    while [[ ! -f "$MONITOR_STOP" ]]; do
      local ts stats
      ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      stats="$(docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}' 2>/dev/null | grep -E '^spacegate-(api|web)-' || true)"
      if [[ -n "$stats" ]]; then
        while IFS= read -r line; do
          [[ -z "$line" ]] && continue
          printf '%s|%s\n' "$ts" "$line" >>"$DOCKER_STATS_FILE"
        done <<<"$stats"
      fi
      sleep 1
    done
  ) &
  MONITOR_PID=$!
}

stop_docker_monitor() {
  touch "$MONITOR_STOP"
  if [[ -n "${MONITOR_PID:-}" ]]; then
    wait "$MONITOR_PID" 2>/dev/null || true
  fi
}

trap stop_docker_monitor EXIT

printf '%s╔══════════════════════════════════════════════════════════════════════════════╗%s\n' "$C_MAGENTA" "$C_RESET"
printf '%s║%s %-76s %s║%s\n' "$C_MAGENTA" "$C_RESET" "SPACEGATE STRESS GRID // profile=${PROFILE}" "$C_MAGENTA" "$C_RESET"
printf '%s╚══════════════════════════════════════════════════════════════════════════════╝%s\n' "$C_MAGENTA" "$C_RESET"
printf '%sTarget:%s %s\n' "$C_CYAN" "$C_RESET" "$BASE_URL"
printf '%sThresholds:%s error<=%s%%  p95<=%sms\n' "$C_CYAN" "$C_RESET" "$ERROR_THRESHOLD_PCT" "$P95_THRESHOLD_MS"

start_docker_monitor

START_EPOCH="$(date +%s)"

if [[ "$PROFILE" == "spike" ]]; then
  WARM_SEC=$(( DURATION_SEC / 5 ))
  COOL_SEC=$(( DURATION_SEC / 5 ))
  PEAK_SEC=$(( DURATION_SEC - WARM_SEC - COOL_SEC ))
  (( WARM_SEC <= 0 )) && WARM_SEC=1
  (( COOL_SEC <= 0 )) && COOL_SEC=1
  (( PEAK_SEC <= 0 )) && PEAK_SEC=1
  run_phase "warmup" "$WARM_SEC" "$CONCURRENCY" "$REQUEST_MODE"
  run_phase "spike" "$PEAK_SEC" "$PEAK_CONCURRENCY" "$REQUEST_MODE"
  run_phase "cooldown" "$COOL_SEC" "$CONCURRENCY" "$REQUEST_MODE"
else
  run_phase "steady" "$DURATION_SEC" "$CONCURRENCY" "$REQUEST_MODE"
fi

END_EPOCH="$(date +%s)"
stop_docker_monitor

{
  printf 'ts,phase,endpoint,code,latency_ms,ok\n'
  cat "$WORKER_DIR"/*.csv 2>/dev/null || true
} >"$REQ_CSV"

eval "$(
python3 - "$REQ_CSV" "$SUMMARY_JSON" "$ERROR_THRESHOLD_PCT" "$P95_THRESHOLD_MS" "$DOCKER_STATS_FILE" "$START_EPOCH" "$END_EPOCH" <<'PY'
import csv
import json
import math
import re
import sys
from collections import Counter, defaultdict

req_csv = sys.argv[1]
summary_json = sys.argv[2]
error_threshold_pct = float(sys.argv[3])
p95_threshold_ms = float(sys.argv[4])
docker_stats_file = sys.argv[5]
start_epoch = int(sys.argv[6])
end_epoch = int(sys.argv[7])

def pct(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return float(s[0])
    rank = (p / 100.0) * (len(s) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return float(s[lo])
    return float(s[lo] + (s[hi] - s[lo]) * (rank - lo))

def parse_mem_to_mib(text):
    # "116MiB / 30.61GiB" -> 116.0
    left = text.split("/", 1)[0].strip()
    m = re.match(r"([0-9.]+)\s*([KMG]iB|B)", left)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = m.group(2)
    if unit == "B":
        return val / (1024 * 1024)
    if unit == "KiB":
        return val / 1024
    if unit == "MiB":
        return val
    if unit == "GiB":
        return val * 1024
    return 0.0

total = 0
ok = 0
fail = 0
code_counts = Counter()
phase_counts = Counter()
endpoint_counts = Counter()
lat_ok = []

with open(req_csv, "r", encoding="utf-8", newline="") as fh:
    reader = csv.DictReader(fh)
    for row in reader:
        total += 1
        code = row.get("code", "000")
        endpoint = row.get("endpoint", "")
        phase = row.get("phase", "steady")
        latency = float(row.get("latency_ms", "0") or "0")
        ok_flag = row.get("ok", "0") == "1"

        code_counts[code] += 1
        phase_counts[phase] += 1
        endpoint_counts[endpoint] += 1
        if ok_flag:
            ok += 1
            lat_ok.append(latency)
        else:
            fail += 1

elapsed = max(1, end_epoch - start_epoch)
rps = total / elapsed
error_rate_pct = (fail / total * 100.0) if total else 0.0

p50 = pct(lat_ok, 50)
p95 = pct(lat_ok, 95)
p99 = pct(lat_ok, 99)

docker_peak = defaultdict(lambda: {"cpu_pct_max": 0.0, "mem_mib_max": 0.0})
try:
    with open(docker_stats_file, "r", encoding="utf-8") as fh:
        for line in fh:
            parts = line.strip().split("|")
            if len(parts) < 4:
                continue
            name = parts[1]
            cpu_txt = parts[2].strip().replace("%", "")
            mem_txt = parts[3].strip()
            try:
                cpu = float(cpu_txt)
            except ValueError:
                cpu = 0.0
            mem_mib = parse_mem_to_mib(mem_txt)
            docker_peak[name]["cpu_pct_max"] = max(docker_peak[name]["cpu_pct_max"], cpu)
            docker_peak[name]["mem_mib_max"] = max(docker_peak[name]["mem_mib_max"], mem_mib)
except FileNotFoundError:
    pass

has_502 = code_counts.get("502", 0) > 0
has_503 = code_counts.get("503", 0) > 0
gate_error_rate = error_rate_pct <= error_threshold_pct
gate_p95 = p95 <= p95_threshold_ms
gate_gateway = not (has_502 or has_503)
passed = gate_error_rate and gate_p95 and gate_gateway and total > 0

summary = {
    "profile": None,
    "timing": {
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "elapsed_sec": elapsed,
    },
    "requests": {
        "total": total,
        "ok": ok,
        "fail": fail,
        "rps": rps,
        "error_rate_pct": error_rate_pct,
        "code_counts": dict(code_counts),
        "phase_counts": dict(phase_counts),
        "endpoint_counts": dict(endpoint_counts),
    },
    "latency_ms": {
        "p50": p50,
        "p95": p95,
        "p99": p99,
    },
    "gates": {
        "no_502_503": gate_gateway,
        "error_rate_ok": gate_error_rate,
        "p95_ok": gate_p95,
        "passed": passed,
    },
    "thresholds": {
        "error_threshold_pct": error_threshold_pct,
        "p95_threshold_ms": p95_threshold_ms,
    },
    "docker_peak": docker_peak,
}

with open(summary_json, "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2, sort_keys=True)

print(f"TOTAL={total}")
print(f"OK={ok}")
print(f"FAIL={fail}")
print(f"RPS={rps:.2f}")
print(f"ERR_PCT={error_rate_pct:.2f}")
print(f"P50={p50:.1f}")
print(f"P95={p95:.1f}")
print(f"P99={p99:.1f}")
print(f"C502={code_counts.get('502', 0)}")
print(f"C503={code_counts.get('503', 0)}")
print(f"GATE_GATEWAY={1 if gate_gateway else 0}")
print(f"GATE_ERROR={1 if gate_error_rate else 0}")
print(f"GATE_P95={1 if gate_p95 else 0}")
print(f"PASSED={1 if passed else 0}")
PY
)"

gate_badge() {
  local ok="$1"
  if [[ "$ok" == "1" ]]; then
    printf '%sPASS%s' "$C_GREEN" "$C_RESET"
  else
    printf '%sFAIL%s' "$C_RED" "$C_RESET"
  fi
}

printf '\n%sSummary%s\n' "$C_CYAN" "$C_RESET"
printf '  total=%s ok=%s fail=%s rps=%s\n' "$TOTAL" "$OK" "$FAIL" "$RPS"
printf '  latency_ms p50=%s p95=%s p99=%s\n' "$P50" "$P95" "$P99"
printf '  error_rate=%s%%  502=%s 503=%s\n' "$ERR_PCT" "$C502" "$C503"

printf '\n%sGates%s\n' "$C_CYAN" "$C_RESET"
printf '  no_502_503: %s\n' "$(gate_badge "$GATE_GATEWAY")"
printf '  error_rate<=%s%%: %s\n' "$ERROR_THRESHOLD_PCT" "$(gate_badge "$GATE_ERROR")"
printf '  p95<=%sms: %s\n' "$P95_THRESHOLD_MS" "$(gate_badge "$GATE_P95")"
printf '  overall: %s\n' "$(gate_badge "$PASSED")"

printf '\n%sArtifacts%s\n' "$C_CYAN" "$C_RESET"
printf '  %s\n' "$REQ_CSV"
printf '  %s\n' "$SUMMARY_JSON"
if [[ -f "$DOCKER_STATS_FILE" ]]; then
  printf '  %s\n' "$DOCKER_STATS_FILE"
fi

if [[ "$PASSED" != "1" ]]; then
  exit 2
fi
