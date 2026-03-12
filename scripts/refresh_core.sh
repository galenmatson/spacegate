#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
REPORTS_DIR="$STATE_DIR/reports"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
LOG_FILE="$LOG_DIR/refresh_core.log"

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

usage() {
  cat <<'USAGE'
Usage:
  scripts/refresh_core.sh [--skip-download] [--full] [--download-overwrite]

Behavior:
  - default: run differential download, source-delta scan, impacted-row planning
  - if plan mode is planet_incremental_eligible -> selective cook + incremental planet ingest
  - otherwise -> full cook + full ingest
  - always promotes and verifies resulting build

Options:
  --skip-download       Skip download_core.sh and use current manifests/delta report.
  --full                Force full cook + full ingest regardless of planner mode.
  --download-overwrite  Pass --overwrite to download_core.sh.
USAGE
}

main() {
  local skip_download=0
  local force_full=0
  local download_overwrite=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-download)
        skip_download=1
        shift 1
        ;;
      --full)
        force_full=1
        shift 1
        ;;
      --download-overwrite)
        download_overwrite=1
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

  if [[ "$skip_download" != "1" ]]; then
    log "Refresh core: download stage"
    local -a dl_args=("--core" "--non-interactive")
    if [[ "$download_overwrite" == "1" ]]; then
      dl_args+=("--overwrite")
    fi
    "$ROOT_DIR/scripts/download_core.sh" "${dl_args[@]}"
  else
    log "Refresh core: skip download"
    if ! source_delta_report="$("$PYTHON_BIN" "$ROOT_DIR/scripts/scan_source_deltas.py" --root "$ROOT_DIR" 2>/dev/null)"; then
      log "Warning: failed to refresh source delta report while --skip-download is active."
    fi
  fi

  local impacted_plan
  impacted_plan="$("$PYTHON_BIN" "$ROOT_DIR/scripts/plan_impacted_rows.py" --root "$ROOT_DIR")"
  log "Refresh core: impacted plan -> $impacted_plan"

  local mode
  mode="$("$PYTHON_BIN" - "$impacted_plan" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1])
payload = json.loads(path.read_text())
print(str(payload.get("mode") or "full_rebuild_required"))
PY
)"

  if [[ "$force_full" == "1" ]]; then
    mode="full_rebuild_required"
    log "Refresh core: --full set; forcing full rebuild path"
  fi

  local now git_sha build_id
  now="$(date -u +"%Y%m%dT%H%M%SZ")"
  git_sha="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo nogit)"
  if [[ "$mode" == "planet_incremental_eligible" ]]; then
    build_id="${now}_${git_sha}_incplanet"
    log "Refresh core: incremental mode (build_id=$build_id)"
    "$ROOT_DIR/scripts/cook_delta.sh" "$impacted_plan"
    "$PYTHON_BIN" "$ROOT_DIR/scripts/ingest_incremental_planets.py" \
      --root "$ROOT_DIR" \
      --build-id "$build_id" \
      --impacted-plan "$impacted_plan"
  else
    build_id="${now}_${git_sha}"
    log "Refresh core: full rebuild mode (build_id=$build_id)"
    "$ROOT_DIR/scripts/cook_core.sh"
    "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$build_id"
  fi

  log "Refresh core: promote + verify build $build_id"
  "$ROOT_DIR/scripts/promote_build.sh" "$build_id"
  "$ROOT_DIR/scripts/verify_build.sh" "$build_id"
  log "Refresh core complete (build_id=$build_id)"
}

main "$@"
