#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
REPORT_ROOT="$STATE_DIR/reports/benchmarks"
STAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
BENCH_DIR="$REPORT_ROOT/$STAMP"
SUMMARY_CSV="$BENCH_DIR/summary.csv"
SUMMARY_MD="$BENCH_DIR/summary.md"

THREADS_CSV="${SPACEGATE_BENCH_THREADS:-8,10,12}"
MEMORY_CSV="${SPACEGATE_BENCH_MEMORY_LIMITS:-26GB,28GB,30GB}"
PAIR_CSV=""
PREFIX="${SPACEGATE_BENCH_PREFIX:-ingest_bench}"
HEARTBEAT_S="${SPACEGATE_INGEST_HEARTBEAT_S:-120}"
AUTO_FINALIZE_FAILED="${SPACEGATE_BENCH_AUTO_FINALIZE_FAILED:-0}"
STOP_FAH="${SPACEGATE_STOP_FAH_ON_BENCH:-1}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/benchmark_ingest_tuning.sh [options]

Runs ingest benchmarks across thread/memory combinations and records stage timings.

Options:
  --threads CSV         Thread counts (default: 8,10,12)
  --memory CSV          Memory limits (default: 26GB,28GB,30GB)
  --pairs CSV           Explicit pairs, e.g. "8:26GB,10:28GB"
  --prefix TEXT         Build-id prefix (default: ingest_bench)
  --heartbeat SEC       Heartbeat interval seconds (default: 120)
  --auto-finalize 0|1   If ingest fails but tmp build exists, run finalize helper (default: 0)
  --stop-fah 0|1        Attempt to stop Folding@Home jobs first (default: 1)
  -h, --help            Show this help

Output:
  /data/spacegate/data/reports/benchmarks/<timestamp>/
    - summary.csv
    - summary.md
    - <build_id>.log (per run)
USAGE
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

csv_to_array() {
  local csv="$1"
  local -n out_ref="$2"
  out_ref=()
  IFS=',' read -r -a raw <<<"$csv"
  local item=""
  for item in "${raw[@]}"; do
    item="$(trim "$item")"
    [[ -n "$item" ]] && out_ref+=("$item")
  done
}

safe_mem_tag() {
  local mem="$1"
  mem="${mem// /}"
  mem="${mem//[^A-Za-z0-9]/_}"
  printf '%s' "$mem"
}

log() {
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$*"
}

stop_fah_if_requested() {
  [[ "$STOP_FAH" == "1" ]] || return 0
  local fah_procs
  fah_procs="$(pgrep -af -i 'fahclient|fahcore|folding@home|foldingathome' || true)"
  [[ -n "$fah_procs" ]] || return 0

  log "Detected Folding@Home-related processes; attempting shutdown before benchmark."
  printf '%s\n' "$fah_procs"

  if command -v systemctl >/dev/null 2>&1; then
    systemctl stop FAHClient.service >/dev/null 2>&1 || true
    systemctl stop fahclient.service >/dev/null 2>&1 || true
  fi
  pkill -f -i 'fahclient|fahcore|folding@home|foldingathome' >/dev/null 2>&1 || true
}

parse_log_metrics() {
  local run_log="$1"
  python3 - "$run_log" <<'PY'
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
text = log_path.read_text(errors="ignore") if log_path.exists() else ""
lines = text.splitlines()

patterns = {
    "stars_s": r"Stars stage complete in ([0-9.]+)s",
    "systems_s": r"System grouping stage complete in ([0-9.]+)s",
    "planets_s": r"Planets stage complete in ([0-9.]+)s",
    "aliases_s": r"Alias stage complete in ([0-9.]+)s",
    "science_s": r"Science side tables stage complete in ([0-9.]+)s",
    "qc_s": r"QC stage complete in ([0-9.]+)s",
    "parquet_s": r"Parquet export stage complete in ([0-9.]+)s",
    "arm_s": r"Arm stage complete in ([0-9.]+)s",
    "ingest_total_s": r"Ingest core complete in ([0-9.]+)s",
}

values = {k: "" for k in patterns}
for key, pattern in patterns.items():
    for m in re.finditer(pattern, text):
        values[key] = m.group(1)

fail_reason = ""
for line in lines:
    if "QC failed:" in line:
        fail_reason = line.strip()
if not fail_reason:
    for line in reversed(lines):
        if "Out of Memory" in line or "Traceback" in line:
            fail_reason = line.strip()
            break

for key in [
    "stars_s",
    "systems_s",
    "planets_s",
    "aliases_s",
    "science_s",
    "qc_s",
    "parquet_s",
    "arm_s",
    "ingest_total_s",
]:
    print(f"{key}={values[key]}")
print(f"fail_reason={fail_reason.replace(',', ';')}")
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads)
      THREADS_CSV="${2:-}"; shift 2 ;;
    --memory)
      MEMORY_CSV="${2:-}"; shift 2 ;;
    --pairs)
      PAIR_CSV="${2:-}"; shift 2 ;;
    --prefix)
      PREFIX="${2:-}"; shift 2 ;;
    --heartbeat)
      HEARTBEAT_S="${2:-}"; shift 2 ;;
    --auto-finalize)
      AUTO_FINALIZE_FAILED="${2:-}"; shift 2 ;;
    --stop-fah)
      STOP_FAH="${2:-}"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1 ;;
  esac
done

if ! [[ "$HEARTBEAT_S" =~ ^[0-9]+$ ]] || (( HEARTBEAT_S < 15 )); then
  echo "Invalid --heartbeat value: $HEARTBEAT_S (must be integer >= 15)" >&2
  exit 1
fi

mkdir -p "$BENCH_DIR"
cat >"$SUMMARY_CSV" <<'CSV'
run_utc,build_id,threads,memory_limit,exit_code,elapsed_s,stars_s,systems_s,planets_s,aliases_s,science_s,qc_s,parquet_s,arm_s,ingest_total_s,fail_reason,log_path
CSV

stop_fah_if_requested

declare -a combos=()
if [[ -n "$PAIR_CSV" ]]; then
  declare -a pairs_raw=()
  csv_to_array "$PAIR_CSV" pairs_raw
  for pair in "${pairs_raw[@]}"; do
    if [[ "$pair" != *:* ]]; then
      echo "Invalid pair '$pair' (expected thread:memory format)" >&2
      exit 1
    fi
    combos+=("$pair")
  done
else
  declare -a threads=()
  declare -a memories=()
  csv_to_array "$THREADS_CSV" threads
  csv_to_array "$MEMORY_CSV" memories
  for t in "${threads[@]}"; do
    for m in "${memories[@]}"; do
      combos+=("${t}:${m}")
    done
  done
fi

log "Benchmark output: $BENCH_DIR"
log "Benchmark combos: ${combos[*]}"

for combo in "${combos[@]}"; do
  thread="${combo%%:*}"
  memory="${combo#*:}"
  if ! [[ "$thread" =~ ^[0-9]+$ ]] || (( thread < 1 )); then
    echo "Invalid thread count in combo '$combo'" >&2
    exit 1
  fi

  run_ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  build_id="${PREFIX}_${STAMP}_t${thread}_m$(safe_mem_tag "$memory")"
  run_log="$BENCH_DIR/${build_id}.log"

  log "Run start: build_id=$build_id threads=$thread memory_limit=$memory"
  start_epoch="$(date +%s)"
  set +e
  (
    export SPACEGATE_DUCKDB_THREADS="$thread"
    export SPACEGATE_DUCKDB_MEMORY_LIMIT="$memory"
    export SPACEGATE_INGEST_HEARTBEAT_S="$HEARTBEAT_S"
    export SPACEGATE_KEEP_TMP=1
    "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$build_id"
  ) 2>&1 | tee "$run_log"
  rc=${PIPESTATUS[0]}
  set -e
  end_epoch="$(date +%s)"
  elapsed_s=$(( end_epoch - start_epoch ))

  if (( rc != 0 )) && [[ "$AUTO_FINALIZE_FAILED" == "1" ]] && [[ -d "$STATE_DIR/out/${build_id}.tmp" ]]; then
    log "Ingest failed; attempting finalize from temp build for $build_id"
    set +e
    (
      export SPACEGATE_ATHYG_MERGE_HIP_COLLISION_MAX="${SPACEGATE_ATHYG_MERGE_HIP_COLLISION_MAX:-3000}"
      export SPACEGATE_ATHYG_MERGE_HD_COLLISION_MAX="${SPACEGATE_ATHYG_MERGE_HD_COLLISION_MAX:-3000}"
      "$ROOT_DIR/scripts/finalize_ingest_tmp.sh" --build-id "$build_id"
    ) 2>&1 | tee -a "$run_log"
    finalize_rc=${PIPESTATUS[0]}
    set -e
    if (( finalize_rc == 0 )); then
      rc=0
      log "Finalize succeeded for $build_id"
    else
      log "Finalize failed for $build_id (exit=$finalize_rc)"
    fi
  fi

  stars_s=""; systems_s=""; planets_s=""; aliases_s=""; science_s=""
  qc_s=""; parquet_s=""; arm_s=""; ingest_total_s=""; fail_reason=""
  while IFS='=' read -r key value; do
    case "$key" in
      stars_s) stars_s="$value" ;;
      systems_s) systems_s="$value" ;;
      planets_s) planets_s="$value" ;;
      aliases_s) aliases_s="$value" ;;
      science_s) science_s="$value" ;;
      qc_s) qc_s="$value" ;;
      parquet_s) parquet_s="$value" ;;
      arm_s) arm_s="$value" ;;
      ingest_total_s) ingest_total_s="$value" ;;
      fail_reason) fail_reason="$value" ;;
    esac
  done < <(parse_log_metrics "$run_log")

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$run_ts" "$build_id" "$thread" "$memory" "$rc" "$elapsed_s" \
    "$stars_s" "$systems_s" "$planets_s" "$aliases_s" "$science_s" \
    "$qc_s" "$parquet_s" "$arm_s" "$ingest_total_s" "$fail_reason" "$run_log" \
    >>"$SUMMARY_CSV"

  log "Run complete: build_id=$build_id exit=$rc elapsed_s=$elapsed_s"
done

python3 - "$SUMMARY_CSV" "$SUMMARY_MD" <<'PY'
import csv
import sys
from pathlib import Path

csv_path = Path(sys.argv[1])
md_path = Path(sys.argv[2])
rows = list(csv.DictReader(csv_path.open()))

lines = []
lines.append("# Ingest Tuning Benchmark Summary")
lines.append("")
lines.append(f"Source: `{csv_path}`")
lines.append("")
if not rows:
  lines.append("No runs recorded.")
else:
  ok = [r for r in rows if r.get("exit_code") == "0"]
  lines.append(f"Runs: {len(rows)} | Success: {len(ok)} | Failed: {len(rows) - len(ok)}")
  lines.append("")
  lines.append("| build_id | threads | memory | exit | elapsed_s | ingest_total_s |")
  lines.append("|---|---:|---:|---:|---:|---:|")
  for r in rows:
    lines.append(
      f"| `{r['build_id']}` | {r['threads']} | {r['memory_limit']} | {r['exit_code']} | "
      f"{r['elapsed_s'] or ''} | {r['ingest_total_s'] or ''} |"
    )
  if ok:
    best = sorted(ok, key=lambda r: float(r["ingest_total_s"] or r["elapsed_s"] or "inf"))[0]
    lines.append("")
    lines.append(
      f"Best successful run: `{best['build_id']}` "
      f"(threads={best['threads']}, memory={best['memory_limit']}, ingest_total_s={best['ingest_total_s']})"
    )

md_path.write_text("\n".join(lines) + "\n")
PY

log "Benchmark complete."
log "Summary CSV: $SUMMARY_CSV"
log "Summary Markdown: $SUMMARY_MD"
