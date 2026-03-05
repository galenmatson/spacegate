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
BASELINE_BUILD="${RUN_ID}_baseline"
NSS_ONLY_BUILD="${RUN_ID}_nss_only"
MSC_ONLY_BUILD="${RUN_ID}_msc_only"
NSS_MSC_BUILD="${RUN_ID}_nss_msc"

echo "==> ingest baseline (NSS=0 MSC=0)"
SPACEGATE_ENABLE_GAIA_NSS=0 SPACEGATE_ENABLE_MSC=0 "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$BASELINE_BUILD"

echo "==> ingest nss_only (NSS=1 MSC=0)"
SPACEGATE_ENABLE_GAIA_NSS=1 SPACEGATE_ENABLE_MSC=0 "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$NSS_ONLY_BUILD"

echo "==> ingest msc_only (NSS=0 MSC=1)"
SPACEGATE_ENABLE_GAIA_NSS=0 SPACEGATE_ENABLE_MSC=1 "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$MSC_ONLY_BUILD"

echo "==> ingest nss_msc (NSS=1 MSC=1)"
SPACEGATE_ENABLE_GAIA_NSS=1 SPACEGATE_ENABLE_MSC=1 "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$NSS_MSC_BUILD"

echo "==> multiplicity mode report"
"$PYTHON_BIN" "$ROOT_DIR/scripts/multiplicity_mode_report.py" \
  --baseline "$BASELINE_BUILD" \
  --nss-only "$NSS_ONLY_BUILD" \
  --msc-only "$MSC_ONLY_BUILD" \
  --nss-msc "$NSS_MSC_BUILD"

echo "Done."
