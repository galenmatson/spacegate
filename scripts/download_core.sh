#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

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

"$ROOT_DIR/scripts/catalogs.sh" --core "$@"
if [[ "${SPACEGATE_ENABLE_GAIA_NSS:-1}" != "0" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_gaia_nss_core.py" \
    --buckets "${SPACEGATE_GAIA_NSS_BUCKETS:-7}" \
    --min-parallax-mas "${SPACEGATE_GAIA_NSS_MIN_PARALLAX_MAS:-3.26156}" \
    --timeout-s "${SPACEGATE_GAIA_NSS_TIMEOUT_S:-240}" \
    --retries "${SPACEGATE_GAIA_NSS_RETRIES:-4}"
else
  echo "Skip Gaia NSS fetch (SPACEGATE_ENABLE_GAIA_NSS=0)."
fi
if [[ "${SPACEGATE_ENABLE_WDS_GAIA_XMATCH:-0}" == "1" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_wds_gaia_xmatch.py" \
    --dist-max-arcsec "${SPACEGATE_WDS_GAIA_XMATCH_DIST_ARCSEC:-2.0}" \
    --selection "${SPACEGATE_WDS_GAIA_XMATCH_SELECTION:-best}" \
    --max-rec "${SPACEGATE_WDS_GAIA_XMATCH_MAX_REC:-2000000}"
else
  echo "Skip WDS Gaia XMatch fetch (SPACEGATE_ENABLE_WDS_GAIA_XMATCH!=1)."
fi
echo "Download complete."
echo "Next: scripts/cook_core.sh to normalize catalogs."
