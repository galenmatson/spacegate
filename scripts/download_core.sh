#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

GAIA_DELTA_MODE_DEFAULT="${SPACEGATE_GAIA_DELTA_MODE:-resume}"
GAIA_DELTA_MAX_AGE_HOURS_DEFAULT="${SPACEGATE_GAIA_DELTA_MAX_AGE_HOURS:-720}"

GAIA_BACKBONE_DELTA_MODE="${SPACEGATE_GAIA_BACKBONE_DELTA_MODE:-$GAIA_DELTA_MODE_DEFAULT}"
GAIA_BACKBONE_DELTA_MAX_AGE_HOURS="${SPACEGATE_GAIA_BACKBONE_DELTA_MAX_AGE_HOURS:-$GAIA_DELTA_MAX_AGE_HOURS_DEFAULT}"
GAIA_CLASSPROB_DELTA_MODE="${SPACEGATE_GAIA_CLASSPROB_DELTA_MODE:-$GAIA_DELTA_MODE_DEFAULT}"
GAIA_CLASSPROB_DELTA_MAX_AGE_HOURS="${SPACEGATE_GAIA_CLASSPROB_DELTA_MAX_AGE_HOURS:-$GAIA_DELTA_MAX_AGE_HOURS_DEFAULT}"
GAIA_NSS_DELTA_MODE="${SPACEGATE_GAIA_NSS_DELTA_MODE:-$GAIA_DELTA_MODE_DEFAULT}"
GAIA_NSS_DELTA_MAX_AGE_HOURS="${SPACEGATE_GAIA_NSS_DELTA_MAX_AGE_HOURS:-$GAIA_DELTA_MAX_AGE_HOURS_DEFAULT}"
SBX_DELTA_MODE="${SPACEGATE_SBX_DELTA_MODE:-$GAIA_DELTA_MODE_DEFAULT}"
SBX_DELTA_MAX_AGE_HOURS="${SPACEGATE_SBX_DELTA_MAX_AGE_HOURS:-$GAIA_DELTA_MAX_AGE_HOURS_DEFAULT}"

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
  if [[ "${SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS:-0}" == "1" ]]; then
    catalog_args+=(--catalog exoplanet_eu --catalog open_exoplanet_catalogue --catalog hwc)
  fi
  if [[ "${SPACEGATE_ENABLE_ECLIPSING_CATALOGS:-1}" != "0" ]]; then
    catalog_args+=(--catalog debcat)
  fi
  if [[ "${SPACEGATE_ENABLE_COMPACT_OBJECT_CATALOGS:-1}" != "0" ]]; then
    catalog_args+=(--catalog atnf --catalog magnetar --catalog white_dwarf)
  fi
  if [[ "${SPACEGATE_ENABLE_SUPERSTELLAR_CATALOGS:-1}" != "0" ]]; then
    catalog_args+=(--catalog clusters --catalog snr)
  fi
  if [[ "${SPACEGATE_ENABLE_GAIA_UCD:-1}" != "0" ]]; then
    catalog_args+=(--catalog gaia_ucd)
  fi
  if [[ "${SPACEGATE_ENABLE_VSX:-1}" != "0" ]]; then
    catalog_args+=(--catalog vsx)
  fi
  if [[ "${SPACEGATE_ENABLE_ULTRACOOLSHEET:-1}" != "0" ]]; then
    catalog_args+=(--catalog ultracoolsheet)
  fi
  "$ROOT_DIR/scripts/catalogs.sh" \
    "${catalog_args[@]}" \
    "$@"
  echo "Gaia differential mode: backbone=${GAIA_BACKBONE_DELTA_MODE}@${GAIA_BACKBONE_DELTA_MAX_AGE_HOURS}h classprob=${GAIA_CLASSPROB_DELTA_MODE}@${GAIA_CLASSPROB_DELTA_MAX_AGE_HOURS}h nss=${GAIA_NSS_DELTA_MODE}@${GAIA_NSS_DELTA_MAX_AGE_HOURS}h sbx=${SBX_DELTA_MODE}@${SBX_DELTA_MAX_AGE_HOURS}h"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_gaia_backbone.py" \
    --buckets "${SPACEGATE_GAIA_BACKBONE_BUCKETS:-4001}" \
    --min-parallax-mas "${SPACEGATE_GAIA_BACKBONE_MIN_PARALLAX_MAS:-3.26156}" \
    --timeout-s "${SPACEGATE_GAIA_BACKBONE_TIMEOUT_S:-240}" \
    --retries "${SPACEGATE_GAIA_BACKBONE_RETRIES:-4}" \
    --max-rec "${SPACEGATE_GAIA_BACKBONE_MAX_REC:-500000}" \
    --workers "${SPACEGATE_GAIA_BACKBONE_WORKERS:-1}" \
    --delta-mode "${GAIA_BACKBONE_DELTA_MODE}" \
    --delta-max-age-hours "${GAIA_BACKBONE_DELTA_MAX_AGE_HOURS}"
  if [[ "${SPACEGATE_ENABLE_GAIA_CLASSPROB:-1}" != "0" ]]; then
    "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_gaia_classprob_core.py" \
      --buckets "${SPACEGATE_GAIA_CLASSPROB_BUCKETS:-1009}" \
      --min-parallax-mas "${SPACEGATE_GAIA_CLASSPROB_MIN_PARALLAX_MAS:-3.26156}" \
      --timeout-s "${SPACEGATE_GAIA_CLASSPROB_TIMEOUT_S:-360}" \
      --retries "${SPACEGATE_GAIA_CLASSPROB_RETRIES:-6}" \
      --max-rec "${SPACEGATE_GAIA_CLASSPROB_MAX_REC:-500000}" \
      --workers "${SPACEGATE_GAIA_CLASSPROB_WORKERS:-1}" \
      --delta-mode "${GAIA_CLASSPROB_DELTA_MODE}" \
      --delta-max-age-hours "${GAIA_CLASSPROB_DELTA_MAX_AGE_HOURS}"
  else
    echo "Skip Gaia classifier fetch (SPACEGATE_ENABLE_GAIA_CLASSPROB=0)."
  fi
else
  if [[ "${SPACEGATE_ENABLE_ECLIPSING_CATALOGS:-1}" != "0" ]]; then
    "$ROOT_DIR/scripts/catalogs.sh" --core "$@"
  else
    catalog_args=(--catalog athyg --catalog nasa_exoplanet_archive --catalog wds --catalog msc --catalog orb6)
    if [[ "${SPACEGATE_ENABLE_VSX:-1}" != "0" ]]; then
      catalog_args+=(--catalog vsx)
    fi
    if [[ "${SPACEGATE_ENABLE_ULTRACOOLSHEET:-1}" != "0" ]]; then
      catalog_args+=(--catalog ultracoolsheet)
    fi
    if [[ "${SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS:-0}" == "1" ]]; then
      catalog_args+=(--catalog exoplanet_eu --catalog open_exoplanet_catalogue --catalog hwc)
    fi
    "$ROOT_DIR/scripts/catalogs.sh" "${catalog_args[@]}" "$@"
  fi
fi
if [[ "${SPACEGATE_ENABLE_ECLIPSING_CATALOGS:-1}" != "0" ]]; then
  if [[ "${SPACEGATE_ENABLE_KEPLER_EB:-0}" != "0" ]]; then
    "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_kepler_eb_catalog.py"
  else
    echo "Skip Kepler EB fetch (SPACEGATE_ENABLE_KEPLER_EB=0)."
  fi
  if [[ "${SPACEGATE_ENABLE_TESS_EB:-1}" != "0" ]]; then
    "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_tess_eb_catalog.py" \
      --timeout-s "${SPACEGATE_TESS_EB_TIMEOUT_S:-90}" \
      --retries "${SPACEGATE_TESS_EB_RETRIES:-5}" \
      --max-pages "${SPACEGATE_TESS_EB_MAX_PAGES:-0}"
  else
    echo "Skip TESS EB fetch (SPACEGATE_ENABLE_TESS_EB=0)."
  fi
else
  echo "Skip eclipsing support catalogs (SPACEGATE_ENABLE_ECLIPSING_CATALOGS=0)."
fi
if [[ "${SPACEGATE_ENABLE_GAIA_NSS:-1}" != "0" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_gaia_nss_core.py" \
    --buckets "${SPACEGATE_GAIA_NSS_BUCKETS:-53}" \
    --min-parallax-mas "${SPACEGATE_GAIA_NSS_MIN_PARALLAX_MAS:-3.26156}" \
    --timeout-s "${SPACEGATE_GAIA_NSS_TIMEOUT_S:-360}" \
    --retries "${SPACEGATE_GAIA_NSS_RETRIES:-6}" \
    --max-rec "${SPACEGATE_GAIA_NSS_MAX_REC:-500000}" \
    --delta-mode "${GAIA_NSS_DELTA_MODE}" \
    --delta-max-age-hours "${GAIA_NSS_DELTA_MAX_AGE_HOURS}"
else
  echo "Skip Gaia NSS fetch (SPACEGATE_ENABLE_GAIA_NSS=0)."
fi
if [[ "${SPACEGATE_ENABLE_SBX:-1}" != "0" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_sbx_core.py" \
    --buckets "${SPACEGATE_SBX_BUCKETS:-23}" \
    --min-parallax-mas "${SPACEGATE_SBX_MIN_PARALLAX_MAS:-3.26156}" \
    --timeout-s "${SPACEGATE_SBX_TIMEOUT_S:-360}" \
    --retries "${SPACEGATE_SBX_RETRIES:-6}" \
    --delta-mode "${SBX_DELTA_MODE}" \
    --delta-max-age-hours "${SBX_DELTA_MAX_AGE_HOURS}"
else
  echo "Skip SBX fetch (SPACEGATE_ENABLE_SBX=0)."
fi
if [[ "${SPACEGATE_ENABLE_WDS_GAIA_XMATCH:-1}" == "1" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_wds_gaia_xmatch.py" \
    --dist-max-arcsec "${SPACEGATE_WDS_GAIA_XMATCH_DIST_ARCSEC:-2.0}" \
    --selection "${SPACEGATE_WDS_GAIA_XMATCH_SELECTION:-best}" \
    --max-rec "${SPACEGATE_WDS_GAIA_XMATCH_MAX_REC:-2000000}"
else
  echo "Skip WDS Gaia XMatch fetch (SPACEGATE_ENABLE_WDS_GAIA_XMATCH=0)."
fi
if [[ "${SPACEGATE_ENABLE_SOL_AUTHORITY:-1}" != "0" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_sol_authority.py" \
    --start-time "${SPACEGATE_SOL_AUTHORITY_START_TIME:-2016-01-01}" \
    --stop-time "${SPACEGATE_SOL_AUTHORITY_STOP_TIME:-2016-01-02}" \
    --timeout-s "${SPACEGATE_SOL_AUTHORITY_TIMEOUT_S:-120}" \
    --retries "${SPACEGATE_SOL_AUTHORITY_RETRIES:-4}"
else
  echo "Skip Sol authority fetch (SPACEGATE_ENABLE_SOL_AUTHORITY=0)."
fi
if [[ "${SPACEGATE_ENABLE_SOL_ARTIFICIAL:-1}" != "0" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/scripts/fetch_sol_artificial.py" \
    --start-time "${SPACEGATE_SOL_ARTIFICIAL_START_TIME:-2026-01-01}" \
    --stop-time "${SPACEGATE_SOL_ARTIFICIAL_STOP_TIME:-2026-01-02}" \
    --timeout-s "${SPACEGATE_SOL_ARTIFICIAL_TIMEOUT_S:-120}" \
    --retries "${SPACEGATE_SOL_ARTIFICIAL_RETRIES:-4}"
else
  echo "Skip Sol artificial fetch (SPACEGATE_ENABLE_SOL_ARTIFICIAL=0)."
fi
if ! "$PYTHON_BIN" "$ROOT_DIR/scripts/update_catalog_pipeline_report.py" --stage download >/dev/null 2>&1; then
  echo "Warning: failed to update catalog pipeline report (download stage)." >&2
fi
if source_delta_report="$("$PYTHON_BIN" "$ROOT_DIR/scripts/scan_source_deltas.py" --root "$ROOT_DIR" 2>/dev/null)"; then
  if [[ -n "$source_delta_report" ]]; then
    echo "Updated source delta report: $source_delta_report"
  fi
else
  echo "Warning: failed to update source delta report." >&2
fi
echo "Download complete."
echo "Next: scripts/cook_core.sh to normalize catalogs."
