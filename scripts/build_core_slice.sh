#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

usage() {
  cat <<'USAGE'
Usage:
  scripts/build_core_slice.sh [options]

Rebuild a sliced core dataset using slice policy env vars, then promote + verify.

Options:
  --from-cooked                     Reuse existing cooked catalogs (default).
  --full-pipeline                   Run full download->cook->ingest pipeline.
  --overwrite                       Overwrite cached downloads (full-pipeline only).
  --build-id <id>                   Explicit ingest build id.
  --profile-id <id>                 Slice profile id (e.g., core.default).
  --profile-version <ver>           Slice profile version (e.g., v1).
  --source-galaxy-build-id <id>     Galaxy build id this slice derives from.
  --max-distance-ly <float>         Keep stars with dist_ly <= value.
  --min-parallax-over-error <float> Keep stars with parallax_over_error >= value.
  --max-parallax-error-mas <float>  Keep stars with parallax_error_mas <= value.
  --max-ruwe <float>                Keep stars with ruwe <= value.
  --require-spectral-class          Drop stars without spectral_class.
  --require-color-index             Drop stars without color_index.
  --allowed-spectral-classes <csv>  Keep only listed classes (e.g. O,B,A,F,G,K,M,L,T,Y,D,UNKNOWN).
  -h, --help                        Show this help.
USAGE
}

from_cooked=1
overwrite=0
ingest_build_id=""
profile_id=""
profile_version=""
source_galaxy_build_id=""
max_distance_ly=""
min_parallax_over_error=""
max_parallax_error_mas=""
max_ruwe=""
require_spectral_class=0
require_color_index=0
allowed_spectral_classes=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from-cooked)
      from_cooked=1
      shift 1
      ;;
    --full-pipeline)
      from_cooked=0
      shift 1
      ;;
    --overwrite)
      overwrite=1
      shift 1
      ;;
    --build-id)
      ingest_build_id="${2:-}"
      shift 2
      ;;
    --profile-id)
      profile_id="${2:-}"
      shift 2
      ;;
    --profile-version)
      profile_version="${2:-}"
      shift 2
      ;;
    --source-galaxy-build-id)
      source_galaxy_build_id="${2:-}"
      shift 2
      ;;
    --max-distance-ly)
      max_distance_ly="${2:-}"
      shift 2
      ;;
    --min-parallax-over-error)
      min_parallax_over_error="${2:-}"
      shift 2
      ;;
    --max-parallax-error-mas)
      max_parallax_error_mas="${2:-}"
      shift 2
      ;;
    --max-ruwe)
      max_ruwe="${2:-}"
      shift 2
      ;;
    --require-spectral-class)
      require_spectral_class=1
      shift 1
      ;;
    --require-color-index)
      require_color_index=1
      shift 1
      ;;
    --allowed-spectral-classes)
      allowed_spectral_classes="${2:-}"
      shift 2
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

export SPACEGATE_ENABLE_GAIA_BACKBONE="${SPACEGATE_ENABLE_GAIA_BACKBONE:-1}"
export SPACEGATE_SLICE_MAX_DISTANCE_LY="$max_distance_ly"
export SPACEGATE_SLICE_MIN_PARALLAX_OVER_ERROR="$min_parallax_over_error"
export SPACEGATE_SLICE_MAX_PARALLAX_ERROR_MAS="$max_parallax_error_mas"
export SPACEGATE_SLICE_MAX_RUWE="$max_ruwe"
export SPACEGATE_SLICE_REQUIRE_SPECTRAL_CLASS="$require_spectral_class"
export SPACEGATE_SLICE_REQUIRE_COLOR_INDEX="$require_color_index"
export SPACEGATE_SLICE_ALLOWED_SPECTRAL="$allowed_spectral_classes"
export SPACEGATE_SLICE_PROFILE_ID="$profile_id"
export SPACEGATE_SLICE_PROFILE_VERSION="$profile_version"
export SPACEGATE_SOURCE_GALAXY_BUILD_ID="$source_galaxy_build_id"
export SPACEGATE_BUILD_LAYER="core"

echo "Slice policy:"
echo "  SPACEGATE_SLICE_MAX_DISTANCE_LY=${SPACEGATE_SLICE_MAX_DISTANCE_LY:-}"
echo "  SPACEGATE_SLICE_MIN_PARALLAX_OVER_ERROR=${SPACEGATE_SLICE_MIN_PARALLAX_OVER_ERROR:-}"
echo "  SPACEGATE_SLICE_MAX_PARALLAX_ERROR_MAS=${SPACEGATE_SLICE_MAX_PARALLAX_ERROR_MAS:-}"
echo "  SPACEGATE_SLICE_MAX_RUWE=${SPACEGATE_SLICE_MAX_RUWE:-}"
echo "  SPACEGATE_SLICE_REQUIRE_SPECTRAL_CLASS=${SPACEGATE_SLICE_REQUIRE_SPECTRAL_CLASS:-0}"
echo "  SPACEGATE_SLICE_REQUIRE_COLOR_INDEX=${SPACEGATE_SLICE_REQUIRE_COLOR_INDEX:-0}"
echo "  SPACEGATE_SLICE_ALLOWED_SPECTRAL=${SPACEGATE_SLICE_ALLOWED_SPECTRAL:-}"
echo "  SPACEGATE_SLICE_PROFILE_ID=${SPACEGATE_SLICE_PROFILE_ID:-}"
echo "  SPACEGATE_SLICE_PROFILE_VERSION=${SPACEGATE_SLICE_PROFILE_VERSION:-}"
echo "  SPACEGATE_SOURCE_GALAXY_BUILD_ID=${SPACEGATE_SOURCE_GALAXY_BUILD_ID:-}"
echo "  SPACEGATE_BUILD_LAYER=${SPACEGATE_BUILD_LAYER:-core}"

if [[ "$from_cooked" == "1" ]]; then
  echo "==> Rebuild from cooked catalogs"
  if [[ -n "$ingest_build_id" ]]; then
    "$ROOT_DIR/scripts/ingest_core.sh" --build-id "$ingest_build_id"
  else
    "$ROOT_DIR/scripts/ingest_core.sh"
  fi
  "$ROOT_DIR/scripts/promote_build.sh"
  "$ROOT_DIR/scripts/verify_build.sh"
else
  echo "==> Full pipeline rebuild"
  if [[ "$overwrite" == "1" ]]; then
    "$ROOT_DIR/scripts/build_core.sh" --overwrite
  else
    "$ROOT_DIR/scripts/build_core.sh"
  fi
fi

echo "Sliced build complete."
