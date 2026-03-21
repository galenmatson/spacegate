#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
MIN_FREE_GB="${SPACEGATE_REFRESH_MIN_FREE_GB:-80}"
LOCK_PATH="$STATE_DIR/out/.ingest_core.lock"

now_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

fail() {
  echo "$(now_utc) PRECHECK FAIL: $1" >&2
  exit 1
}

warn() {
  echo "$(now_utc) PRECHECK WARN: $1"
}

info() {
  echo "$(now_utc) PRECHECK: $1"
}

require_path() {
  local p="$1"
  [[ -e "$p" ]] || fail "missing required path: $p"
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || fail "missing required command: $cmd"
}

require_cmd python3
require_cmd df
require_cmd awk

require_path "$STATE_DIR"
require_path "$ROOT_DIR/scripts/download_core.sh"
require_path "$ROOT_DIR/scripts/cook_core.sh"
require_path "$ROOT_DIR/scripts/ingest_core.sh"
require_path "$ROOT_DIR/scripts/promote_build.sh"
require_path "$ROOT_DIR/scripts/verify_build.sh"
require_path "$ROOT_DIR/scripts/build_arm.py"

if [[ -f "$LOCK_PATH" ]]; then
  lock_pid="$(awk -F= '/^pid=/{print $2; exit}' "$LOCK_PATH" 2>/dev/null || true)"
  if [[ -n "${lock_pid:-}" ]] && kill -0 "$lock_pid" >/dev/null 2>&1; then
    fail "ingest lock is active at $LOCK_PATH (pid=$lock_pid)"
  fi
  warn "stale ingest lock present at $LOCK_PATH (safe to remove if no ingest running)"
fi

free_bytes="$(df -B1 --output=avail "$STATE_DIR" | awk 'NR==2{print $1}')"
[[ -n "${free_bytes:-}" ]] || fail "could not determine free space for $STATE_DIR"
free_gb="$(python3 - "$free_bytes" <<'PY'
import sys
v=int(sys.argv[1])
print(f"{v/(1024**3):.1f}")
PY
)"
if ! python3 - "$free_bytes" "$MIN_FREE_GB" <<'PY'
import sys
free_bytes=int(sys.argv[1])
min_gb=float(sys.argv[2])
sys.exit(0 if free_bytes >= int(min_gb*(1024**3)) else 1)
PY
then
  fail "insufficient free space at $STATE_DIR (free=${free_gb}GiB, required>=${MIN_FREE_GB}GiB)"
fi

served_current="$STATE_DIR/served/current"
if [[ -L "$served_current" || -d "$served_current" ]]; then
  served_path="$(readlink -f "$served_current" || true)"
  if [[ -n "${served_path:-}" ]]; then
    info "served build: $served_path"
    if [[ ! -f "$served_path/arm.duckdb" ]]; then
      warn "served build has no arm.duckdb (expected before first post-arm refresh)"
    fi
  fi
fi

info "state_dir=$STATE_DIR"
info "disk_free=${free_gb}GiB"
info "gaia_backbone=${SPACEGATE_ENABLE_GAIA_BACKBONE:-0}"
info "gaia_classprob=${SPACEGATE_ENABLE_GAIA_CLASSPROB:-1}"
info "gaia_nss=${SPACEGATE_ENABLE_GAIA_NSS:-1}"
info "msc=${SPACEGATE_ENABLE_MSC:-1}"
info "gaia_delta_mode=${SPACEGATE_GAIA_DELTA_MODE:-resume}"
info "gaia_buckets(backbone/classprob/nss)="\
"${SPACEGATE_GAIA_BACKBONE_BUCKETS:-211}/${SPACEGATE_GAIA_CLASSPROB_BUCKETS:-211}/${SPACEGATE_GAIA_NSS_BUCKETS:-53}"
info "preflight passed; ready for full refresh"
