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
  scripts/build_core.sh [--overwrite] [--full-refresh]

Runs: download -> cook -> ingest -> promote -> verify

Options:
  --overwrite      Replace existing raw downloads if present.
  --full-refresh   Force Gaia recrawl + overwrite support catalogs for a true refresh run.
USAGE
}

main() {
  local -a download_args=("--core" "--non-interactive")
  local full_refresh=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --overwrite)
        download_args+=("--overwrite")
        shift 1
        ;;
      --full-refresh)
        full_refresh=1
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

  if [[ "$full_refresh" == "1" ]]; then
    echo "==> Full refresh mode enabled"
    export SPACEGATE_ENABLE_GAIA_BACKBONE=1
    export SPACEGATE_ENABLE_GAIA_CLASSPROB=1
    export SPACEGATE_ENABLE_GAIA_NSS=1
    export SPACEGATE_ENABLE_MSC=1
    export SPACEGATE_GAIA_BACKBONE_BUCKETS="${SPACEGATE_GAIA_BACKBONE_BUCKETS:-211}"
    export SPACEGATE_GAIA_CLASSPROB_BUCKETS="${SPACEGATE_GAIA_CLASSPROB_BUCKETS:-211}"
    export SPACEGATE_GAIA_NSS_BUCKETS="${SPACEGATE_GAIA_NSS_BUCKETS:-53}"
    export SPACEGATE_GAIA_DELTA_MODE=refresh
    download_args+=("--overwrite")
    "$ROOT_DIR/scripts/preflight_full_refresh.sh"
  fi

  echo "==> Download core catalogs"
  "$ROOT_DIR/scripts/download_core.sh" "${download_args[@]}"

  echo "==> Cook core catalogs"
  "$ROOT_DIR/scripts/cook_core.sh"

  echo "==> Ingest core catalogs"
  "$ROOT_DIR/scripts/ingest_core.sh"

  echo "==> Promote latest build"
  "$ROOT_DIR/scripts/promote_build.sh"

  echo "==> Verify build"
  "$ROOT_DIR/scripts/verify_build.sh"
  echo "Build complete."
  echo "Next: scripts/run_spacegate.sh to start API + web."
}

main "$@"
