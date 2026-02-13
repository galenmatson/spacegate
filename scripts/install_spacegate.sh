#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-python3}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/install_spacegate.sh [--overwrite]

Checks system dependencies, sets up venvs, installs deps,
and builds the core database if missing.

Options:
  --overwrite   Re-download catalogs even if present.
USAGE
}

missing=()

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    missing+=("$cmd")
  fi
}

check_deps() {
  require_cmd "$PYTHON_BIN"
  require_cmd git
  require_cmd curl
  require_cmd aria2c
  require_cmd gzip
  require_cmd node
  require_cmd npm

  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Missing required commands: ${missing[*]}" >&2
    echo "Install them and re-run." >&2
    echo "Debian/Ubuntu example:" >&2
    echo "  sudo apt-get update" >&2
    echo "  sudo apt-get install -y python3 python3-venv python3-pip git curl aria2 gzip nodejs npm" >&2
    exit 1
  fi
}

main() {
  local overwrite=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --overwrite)
        overwrite="--overwrite"
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

  check_deps

  echo "==> Setup venvs and dependencies"
  "$ROOT_DIR/scripts/setup_spacegate.sh"

  local core_db="$STATE_DIR/served/current/core.duckdb"
  if [[ ! -f "$core_db" ]]; then
    echo "==> Core database not found; building"
    "$ROOT_DIR/scripts/build_core.sh" $overwrite
  else
    echo "==> Core database exists: $core_db"
  fi

  echo "Install complete. Start with: scripts/run_spacegate.sh"
}

main "$@"
