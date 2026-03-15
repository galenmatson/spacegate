#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
RAW_DIR="$STATE_DIR/raw"
COOKED_DIR="$STATE_DIR/cooked"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
LOG_FILE="$LOG_DIR/cook_delta.log"
PLAN_PATH_DEFAULT="$STATE_DIR/reports/impacted_rows_plan.json"

NASA_RAW="$RAW_DIR/nasa_exoplanet_archive/pscomppars.csv"
COOKED_NASA_DIR="$COOKED_DIR/nasa_exoplanet_archive"
COOKED_NASA="$COOKED_NASA_DIR/pscomppars_clean.csv"

PYTHON_BIN=""

mkdir -p "$LOG_DIR"

log() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$msg" | tee -a "$LOG_FILE"
}

choose_python() {
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
}

cook_nasa_only() {
  if [[ ! -f "$NASA_RAW" ]]; then
    echo "Error: missing $NASA_RAW" >&2
    exit 1
  fi
  mkdir -p "$COOKED_NASA_DIR"
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local tmp_out="$tmp_dir/pscomppars_clean.csv"
  log "Cook delta: NASA Exoplanet Archive normalize"
  "$PYTHON_BIN" - "$NASA_RAW" "$tmp_out" <<'PY'
import sys
in_path = sys.argv[1]
out_path = sys.argv[2]
bom = b"\xef\xbb\xbf"
with open(in_path, "rb") as f, open(out_path, "wb") as out:
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
  mv "$tmp_out" "$COOKED_NASA"
  rm -rf "$tmp_dir"
}

contains_source() {
  local plan_path="$1"
  local source_name="$2"
  "$PYTHON_BIN" - "$plan_path" "$source_name" <<'PY'
import json
import sys
from pathlib import Path

plan_path = Path(sys.argv[1])
source_name = sys.argv[2]
if not plan_path.exists():
    print("0")
    raise SystemExit(0)
try:
    payload = json.loads(plan_path.read_text())
except Exception:
    print("0")
    raise SystemExit(0)
sources = set()
for key in ("changed_or_new_sources", "changed_sources", "new_sources", "missing_sources"):
    for item in payload.get(key, []) or []:
        if isinstance(item, str):
            sources.add(item.strip())
        elif isinstance(item, dict):
            value = str(item.get("source_name") or "").strip()
            if value:
                sources.add(value)
print("1" if source_name in sources else "0")
PY
}

main() {
  choose_python
  local plan_path="${1:-$PLAN_PATH_DEFAULT}"
  if [[ ! -f "$plan_path" ]]; then
    echo "Error: impacted plan not found: $plan_path" >&2
    echo "Tip: run scripts/plan_impacted_rows.py first." >&2
    exit 1
  fi

  local mode
  mode="$("$PYTHON_BIN" - "$plan_path" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1])
payload = json.loads(path.read_text())
print(str(payload.get("mode") or "full_rebuild_required"))
PY
)"

  log "Cook delta begin (mode=$mode, plan=$plan_path)"
  if [[ "$mode" != "planet_incremental_eligible" ]]; then
    log "Cook delta fallback: full cook required"
    "$ROOT_DIR/scripts/cook_core.sh"
    exit 0
  fi

  local changed_any=0

  if [[ "$(contains_source "$plan_path" "nasa_exoplanet_archive")" == "1" ]]; then
    cook_nasa_only
    changed_any=1
  else
    log "Cook delta: skip NASA (unchanged)"
  fi

  local lifecycle_changed=0
  for src in exoplanet_eu open_exoplanet_catalogue hwc; do
    if [[ "$(contains_source "$plan_path" "$src")" == "1" ]]; then
      lifecycle_changed=1
      break
    fi
  done
  if [[ "$lifecycle_changed" == "1" ]]; then
    log "Cook delta: exoplanet lifecycle support catalogs"
    "$PYTHON_BIN" "$ROOT_DIR/scripts/cook_exoplanet_lifecycle.py"
    changed_any=1
  else
    log "Cook delta: skip exoplanet lifecycle support catalogs (unchanged)"
  fi

  if [[ "$changed_any" == "0" ]]; then
    log "Cook delta: no impacted sources detected; no cook actions executed."
  fi

  if ! "$PYTHON_BIN" "$ROOT_DIR/scripts/update_catalog_pipeline_report.py" --stage cook >/dev/null 2>&1; then
    log "Warning: failed to update catalog pipeline report (cook stage)."
  fi
  log "Cook delta complete"
}

main "$@"
