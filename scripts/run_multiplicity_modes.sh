#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

PYTHON_BIN=""
if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  PYTHON_BIN="python"
fi

RUN_ID="$(date -u +%Y-%m-%dT%H%M%SZ)"
NSS_OFF_BUILD="${RUN_ID}_nss_off"
NSS_ON_BUILD="${RUN_ID}_nss_on"
NSS_ON_WDS_XMATCH_BUILD="${RUN_ID}_nss_on_wds_xmatch"

echo "==> ingest nss_off (NSS=0 MSC=1)"
SPACEGATE_ENABLE_GAIA_NSS=0 SPACEGATE_ENABLE_MSC=1 "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$NSS_OFF_BUILD"

echo "==> ingest nss_on (NSS=1 MSC=1)"
SPACEGATE_ENABLE_GAIA_NSS=1 SPACEGATE_ENABLE_MSC=1 "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$NSS_ON_BUILD"

echo "==> ingest nss_on_wds_xmatch (NSS=1 MSC=1 WDS_GAIA_XMATCH=1)"
SPACEGATE_ENABLE_GAIA_NSS=1 SPACEGATE_ENABLE_MSC=1 SPACEGATE_ENABLE_WDS_GAIA_XMATCH=1 \
  "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$NSS_ON_WDS_XMATCH_BUILD"

echo "==> multiplicity mode report"
"$PYTHON_BIN" "$ROOT_DIR/scripts/multiplicity_mode_report.py" \
  --nss-off "$NSS_OFF_BUILD" \
  --nss-on "$NSS_ON_BUILD" \
  --nss-on-wds-xmatch "$NSS_ON_WDS_XMATCH_BUILD"

echo "Done."
