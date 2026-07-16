#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
OUT_DIR="$STATE_DIR/out"
REPORTS_DIR="$STATE_DIR/reports"
SERVED_LINK="$STATE_DIR/served/current"

KEEP_BUILDS=6
KEEP_REPORTS=12
APPLY=0
PRUNE_TMP=1

usage() {
  cat <<'EOF'
Usage:
  scripts/prune_state_retention.sh [options]

Prunes stale build artifacts under $SPACEGATE_STATE_DIR (or ./data by default).
Default mode is dry-run.

Options:
  --state-dir DIR      Override state dir.
  --keep-builds N      Keep newest N build directories in out/ (default: 6).
  --keep-reports N     Keep newest N build report directories in reports/ (default: 12).
  --no-prune-tmp       Do not prune out/*.tmp directories.
  --apply              Perform deletions.
  -h, --help           Show help.

Notes:
  - The currently served build is always kept.
  - Raw and cooked catalogs are untouched.
  - If some directories are root-owned, run with sufficient permissions.
EOF
}

is_nonnegative_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

is_build_id_dir() {
  local name="${1:-}"
  [[ "$name" =~ ^([0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8})T([0-9]{4}|[0-9]{6})Z_[A-Za-z0-9._-]+$ ]]
}

path_bytes() {
  local p="${1:-}"
  if [[ ! -e "$p" ]]; then
    echo 0
    return 0
  fi
  local out
  if out="$(du -sb "$p" 2>/dev/null | awk '{print $1}')" && [[ -n "$out" ]]; then
    echo "$out"
    return 0
  fi
  out="$(du -sk "$p" 2>/dev/null | awk '{print $1}')"
  if [[ -n "$out" ]]; then
    echo $((out * 1024))
  else
    echo 0
  fi
}

format_bytes() {
  local bytes="${1:-0}"
  local kib=$((1024))
  local mib=$((1024 * 1024))
  local gib=$((1024 * 1024 * 1024))
  if (( bytes >= gib )); then
    awk -v b="$bytes" 'BEGIN { printf "%.2f GiB", b / (1024*1024*1024) }'
  elif (( bytes >= mib )); then
    awk -v b="$bytes" 'BEGIN { printf "%.2f MiB", b / (1024*1024) }'
  elif (( bytes >= kib )); then
    awk -v b="$bytes" 'BEGIN { printf "%.2f KiB", b / 1024 }'
  else
    printf "%d B" "$bytes"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --state-dir)
      STATE_DIR="${2:-}"
      OUT_DIR="$STATE_DIR/out"
      REPORTS_DIR="$STATE_DIR/reports"
      SERVED_LINK="$STATE_DIR/served/current"
      shift 2
      ;;
    --keep-builds)
      KEEP_BUILDS="${2:-}"
      shift 2
      ;;
    --keep-reports)
      KEEP_REPORTS="${2:-}"
      shift 2
      ;;
    --no-prune-tmp)
      PRUNE_TMP=0
      shift
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if ! is_nonnegative_int "$KEEP_BUILDS"; then
  echo "Invalid --keep-builds value: $KEEP_BUILDS" >&2
  exit 2
fi
if ! is_nonnegative_int "$KEEP_REPORTS"; then
  echo "Invalid --keep-reports value: $KEEP_REPORTS" >&2
  exit 2
fi

mkdir -p "$OUT_DIR" "$REPORTS_DIR"

served_build_id=""
if [[ -e "$SERVED_LINK" ]]; then
  resolved="$(readlink -f "$SERVED_LINK" 2>/dev/null || true)"
  if [[ -n "$resolved" ]]; then
    served_build_id="$(basename "$resolved")"
  fi
fi

mapfile -t out_dirs < <(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort -r)
mapfile -t report_dirs < <(find "$REPORTS_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort -r)

declare -a build_dirs=()
declare -a tmp_dirs=()
for name in "${out_dirs[@]}"; do
  if [[ "$name" == *.tmp ]]; then
    tmp_dirs+=("$name")
    continue
  fi
  if is_build_id_dir "$name"; then
    build_dirs+=("$name")
  fi
done

declare -A keep_build_map=()
for ((i=0; i<${#build_dirs[@]} && i<KEEP_BUILDS; i++)); do
  keep_build_map["${build_dirs[$i]}"]=1
done
if [[ -n "$served_build_id" ]]; then
  keep_build_map["$served_build_id"]=1
fi

declare -a remove_build_paths=()
for name in "${build_dirs[@]}"; do
  if [[ -z "${keep_build_map[$name]+x}" ]]; then
    remove_build_paths+=("$OUT_DIR/$name")
  fi
done
if (( PRUNE_TMP == 1 )); then
  for name in "${tmp_dirs[@]}"; do
    remove_build_paths+=("$OUT_DIR/$name")
  done
fi

declare -A keep_report_map=()
declare -a build_report_dirs=()
for name in "${report_dirs[@]}"; do
  if is_build_id_dir "$name"; then
    build_report_dirs+=("$name")
  fi
done
for ((i=0; i<${#build_report_dirs[@]} && i<KEEP_REPORTS; i++)); do
  keep_report_map["${build_report_dirs[$i]}"]=1
done
if [[ -n "$served_build_id" ]]; then
  keep_report_map["$served_build_id"]=1
fi

declare -a remove_report_paths=()
for name in "${build_report_dirs[@]}"; do
  if [[ -z "${keep_report_map[$name]+x}" ]]; then
    remove_report_paths+=("$REPORTS_DIR/$name")
  fi
done

total_reclaim=0
echo "State dir: $STATE_DIR"
echo "Served build: ${served_build_id:-"(none)"}"
echo "Retention: keep_builds=$KEEP_BUILDS keep_reports=$KEEP_REPORTS prune_tmp=$PRUNE_TMP"
echo

print_candidates() {
  local label="$1"
  shift
  local -a paths=("$@")
  echo "$label (${#paths[@]}):"
  if (( ${#paths[@]} == 0 )); then
    echo "  (none)"
    echo
    return 0
  fi
  for p in "${paths[@]}"; do
    local b
    b="$(path_bytes "$p")"
    total_reclaim=$((total_reclaim + b))
    echo "  $p  ($(format_bytes "$b"))"
  done
  echo
}

print_candidates "Build paths to prune" "${remove_build_paths[@]}"
print_candidates "Report paths to prune" "${remove_report_paths[@]}"

echo "Estimated reclaimable: $(format_bytes "$total_reclaim")"

if (( APPLY == 0 )); then
  echo "Dry-run only. Re-run with --apply to delete."
  exit 0
fi

echo "Applying deletions..."
for p in "${remove_build_paths[@]}"; do
  rm -rf -- "$p"
done
for p in "${remove_report_paths[@]}"; do
  rm -rf -- "$p"
done
echo "Retention prune complete."
