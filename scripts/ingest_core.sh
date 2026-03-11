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
LOG_FILE="$LOG_DIR/ingest_core.log"

mkdir -p "$LOG_DIR"

log() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$msg" | tee -a "$LOG_FILE"
}

parse_bool_env() {
  local raw="${1:-}"
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    1|true|yes|on) echo "1" ;;
    0|false|no|off|"") echo "0" ;;
    *) echo "0" ;;
  esac
}

format_duration() {
  local total_s="$1"
  local h=$(( total_s / 3600 ))
  local m=$(( (total_s % 3600) / 60 ))
  local s=$(( total_s % 60 ))
  if (( h > 0 )); then
    printf '%dh%02dm%02ds' "$h" "$m" "$s"
  elif (( m > 0 )); then
    printf '%dm%02ds' "$m" "$s"
  else
    printf '%ds' "$s"
  fi
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

emit_heavy_ingest_warning() {
  local enable_gaia_backbone
  enable_gaia_backbone="$(parse_bool_env "${SPACEGATE_ENABLE_GAIA_BACKBONE:-0}")"
  if [[ "$enable_gaia_backbone" != "1" ]]; then
    return
  fi

  local warning_lines=""
  warning_lines="$("$PYTHON_BIN" - "$LOG_FILE" "$STATE_DIR" <<'PY'
import datetime as dt
import json
import os
import statistics
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
state_dir = Path(sys.argv[2])

def parse_ts(line: str):
    token = line.split(" ", 1)[0].strip()
    try:
        return dt.datetime.strptime(token, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

runs = []
current = None
if log_path.exists():
    for raw in log_path.read_text(errors="ignore").splitlines():
        ts = parse_ts(raw)
        if ts is None:
            continue
        msg = raw.split(" ", 1)[1] if " " in raw else ""
        if "Ingest core start" in msg:
            current = {"start": ts, "science": ""}
            continue
        if current is None:
            continue
        if "Science catalogs:" in msg and not current.get("science"):
            current["science"] = msg
            continue
        if "Ingest core complete" in msg:
            end = ts
            if end >= current["start"]:
                current["end"] = end
                runs.append(current)
            current = None

heavy_runs = [r for r in runs if "gaia_classprob=1" in r.get("science", "")]
durations_min = [
    (r["end"] - r["start"]).total_seconds() / 60.0
    for r in heavy_runs
    if r.get("end") and r["end"] >= r["start"]
]

row_count = None
byte_count = None
manifest_path = state_dir / "reports" / "manifests" / "gaia_backbone_manifest.json"
if manifest_path.exists():
    try:
        payload = json.loads(manifest_path.read_text())
        if isinstance(payload, list):
            row_count = int(sum(int(item.get("row_count") or 0) for item in payload))
            byte_count = int(sum(int(item.get("bytes_written") or 0) for item in payload))
    except Exception:
        pass

memory_limit = os.getenv("SPACEGATE_DUCKDB_MEMORY_LIMIT", "24GB (default)")
threads = os.getenv("SPACEGATE_DUCKDB_THREADS", "12 (default)")

print("Resource warning: Gaia-backbone ingest is a heavy operation (high CPU, memory, and disk I/O).")
if row_count:
    if byte_count:
        gib = byte_count / float(1024 ** 3)
        print(f"Input snapshot: {row_count:,} Gaia rows (~{gib:.2f} GiB raw CSV).")
    else:
        print(f"Input snapshot: {row_count:,} Gaia rows.")
if durations_min:
    median = statistics.median(durations_min)
    p90 = sorted(durations_min)[max(0, int(round(0.9 * (len(durations_min) - 1))))]
    latest = durations_min[-1]
    print(
        "Recent benchmark on this host: "
        f"median {median:.1f} min, p90 {p90:.1f} min, last {latest:.1f} min (n={len(durations_min)})."
    )
else:
    print("Recent benchmark: no completed Gaia-backbone ingest runs found in ingest_core.log.")
print(f"Configured DuckDB resources: threads={threads}, memory_limit={memory_limit}.")
print("Planning guidance: expect sustained multi-core load, up to ~20+ GiB temporary disk writes, and long wall-clock runtime.")
print("Modest hardware estimate (about 8 vCPU / 16GB RAM / SSD): commonly 45-90+ minutes for full Gaia-backbone + aliases.")
PY
)"

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    log "$line"
  done <<<"$warning_lines"
}

log "Ingest core begin"
emit_heavy_ingest_warning
"$PYTHON_BIN" - <<'PY' >/dev/null 2>&1 || {
import duckdb
PY
  echo "Error: python module 'duckdb' not found for $PYTHON_BIN" >&2
  echo "Tip: run scripts/setup_spacegate.sh (or install requirements.txt in the root venv)." >&2
  exit 1
}

if [[ -z "${SPACEGATE_KEEP_TMP:-}" ]] && [[ "$(parse_bool_env "${SPACEGATE_ENABLE_GAIA_BACKBONE:-0}")" == "1" ]]; then
  export SPACEGATE_KEEP_TMP=1
  log "SPACEGATE_KEEP_TMP not set; defaulting to 1 for Gaia ingest so failed temp builds remain resumable."
fi

HEARTBEAT_S="${SPACEGATE_INGEST_HEARTBEAT_S:-120}"
if ! [[ "$HEARTBEAT_S" =~ ^[0-9]+$ ]] || (( HEARTBEAT_S < 15 )); then
  HEARTBEAT_S=120
fi

start_epoch="$(date +%s)"
"$PYTHON_BIN" "$ROOT_DIR/scripts/ingest_core.py" "$@" > >(tee -a "$LOG_FILE") 2>&1 &
ingest_pid=$!

while kill -0 "$ingest_pid" >/dev/null 2>&1; do
  sleep "$HEARTBEAT_S"
  if ! kill -0 "$ingest_pid" >/dev/null 2>&1; then
    break
  fi
  now_epoch="$(date +%s)"
  elapsed_s=$(( now_epoch - start_epoch ))
  last_stage="$(awk '!/Ingest heartbeat:/{line=$0} END{print line}' "$LOG_FILE")"
  last_stage="${last_stage#*Z }"
  last_stage="${last_stage//\'/}"
  log "Ingest heartbeat: elapsed=$(format_duration "$elapsed_s") last_stage='${last_stage}'"
done

wait "$ingest_pid"
ingest_status=$?
if (( ingest_status != 0 )); then
  log "Ingest core failed (exit=${ingest_status})."
  if [[ "${SPACEGATE_KEEP_TMP:-0}" == "1" ]]; then
    log "Temp output retained (SPACEGATE_KEEP_TMP=1). Recovery option: scripts/finalize_ingest_tmp.sh --build-id <failed_build_id> after adjusting gates."
  fi
  exit "$ingest_status"
fi

log "Ingest core complete"
echo "Next: scripts/promote_build.sh to activate the new build."
