#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  scripts/build_core.sh [--overwrite]

Runs: download -> cook -> ingest -> promote -> verify

Options:
  --overwrite   Replace existing raw downloads if present.
USAGE
}

main() {
  local -a download_args=("--core" "--non-interactive")

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --overwrite)
        download_args+=("--overwrite")
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
}

main "$@"
