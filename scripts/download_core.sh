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

if [[ "${SPACEGATE_ENABLE_GAIA_BACKBONE:-0}" == "1" ]]; then
  if [[ "${SPACEGATE_ENABLE_MSC:-1}" == "0" ]]; then
    echo "Error: MSC is mandatory for default science ingest (SPACEGATE_ENABLE_MSC=0 is not supported)." >&2
    exit 1
  fi
  catalog_args=(
    --catalog nasa_exoplanet_archive
    --catalog wds
    --catalog msc
    --catalog orb6
  )
  if [[ "${SPACEGATE_ENABLE_ECLIPSING_CATALOGS:-1}" != "0" ]]; then
    catalog_args+=(--catalog debcat)
  fi
  if [[ "${SPACEGATE_ENABLE_COMPACT_OBJECT_CATALOGS:-1}" != "0" ]]; then
    catalog_args+=(--catalog atnf --catalog magnetar --catalog white_dwarf)
  fi
  if [[ "${SPACEGATE_ENABLE_SUPERSTELLAR_CATALOGS:-1}" != "0" ]]; then
    catalog_args+=(--catalog clusters --catalog snr)
  fi
  "$ROOT_DIR/scripts/catalogs.sh" \
    "${catalog_args[@]}" \
    "$@"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_gaia_backbone.py" \
    --buckets "${SPACEGATE_GAIA_BACKBONE_BUCKETS:-211}" \
    --min-parallax-mas "${SPACEGATE_GAIA_BACKBONE_MIN_PARALLAX_MAS:-3.26156}" \
    --timeout-s "${SPACEGATE_GAIA_BACKBONE_TIMEOUT_S:-240}" \
    --retries "${SPACEGATE_GAIA_BACKBONE_RETRIES:-4}"
  if [[ "${SPACEGATE_ENABLE_GAIA_CLASSPROB:-1}" != "0" ]]; then
    "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_gaia_classprob_core.py" \
      --buckets "${SPACEGATE_GAIA_CLASSPROB_BUCKETS:-211}" \
      --min-parallax-mas "${SPACEGATE_GAIA_CLASSPROB_MIN_PARALLAX_MAS:-3.26156}" \
      --timeout-s "${SPACEGATE_GAIA_CLASSPROB_TIMEOUT_S:-360}" \
      --retries "${SPACEGATE_GAIA_CLASSPROB_RETRIES:-6}" \
      --max-rec "${SPACEGATE_GAIA_CLASSPROB_MAX_REC:-500000}"
  else
    echo "Skip Gaia classifier fetch (SPACEGATE_ENABLE_GAIA_CLASSPROB=0)."
  fi
else
  if [[ "${SPACEGATE_ENABLE_ECLIPSING_CATALOGS:-1}" != "0" ]]; then
    "$ROOT_DIR/scripts/catalogs.sh" --core "$@"
  else
    "$ROOT_DIR/scripts/catalogs.sh" \
      --catalog athyg \
      --catalog nasa_exoplanet_archive \
      --catalog wds \
      --catalog msc \
      --catalog orb6 \
      "$@"
  fi
fi
if [[ "${SPACEGATE_ENABLE_ECLIPSING_CATALOGS:-1}" != "0" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_kepler_eb_catalog.py"
else
  echo "Skip eclipsing support catalogs (SPACEGATE_ENABLE_ECLIPSING_CATALOGS=0)."
fi
if [[ "${SPACEGATE_ENABLE_GAIA_NSS:-1}" != "0" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_gaia_nss_core.py" \
    --buckets "${SPACEGATE_GAIA_NSS_BUCKETS:-211}" \
    --min-parallax-mas "${SPACEGATE_GAIA_NSS_MIN_PARALLAX_MAS:-3.26156}" \
    --timeout-s "${SPACEGATE_GAIA_NSS_TIMEOUT_S:-360}" \
    --retries "${SPACEGATE_GAIA_NSS_RETRIES:-6}"
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
