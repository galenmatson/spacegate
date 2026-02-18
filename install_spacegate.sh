#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_load_env_defaults "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
CACHE_DIR="${SPACEGATE_CACHE_DIR:-$STATE_DIR/cache}"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-python3}"
NODE_MIN_MAJOR="${SPACEGATE_NODE_MIN_MAJOR:-18}"

usage() {
  cat <<'USAGE'
Usage:
  ./install_spacegate.sh [--overwrite] [--skip-web] [--skip-web-build] [--skip-build] [--skip-db-download]

Checks system dependencies, sets up venvs, installs deps,
and installs the core database if missing.

Options:
  --overwrite       Re-download inputs even if present.
  --skip-web        Skip web UI dependency install.
  --skip-web-build  Skip building the web UI bundle.
  --skip-build      Skip building the core database.
  --skip-db-download  Skip download bootstrap and build from catalogs instead.
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
  require_cmd 7z
  require_cmd node
  require_cmd npm

  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Missing required commands: ${missing[*]}" >&2
    echo "Install them and re-run." >&2
    echo "Debian/Ubuntu example (Node.js 20 recommended):" >&2
    echo "  sudo apt-get update" >&2
    echo "  sudo apt-get install -y python3 python3-venv python3-pip git curl aria2 gzip p7zip-full ca-certificates gnupg" >&2
    echo "  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -" >&2
    echo "  sudo apt-get install -y nodejs" >&2
    exit 1
  fi

  local node_major=""
  node_major="$(node -v | sed -E 's/^v([0-9]+).*/\1/')"
  if [[ ! "$node_major" =~ ^[0-9]+$ ]]; then
    echo "Error: unable to parse Node.js version: $(node -v)" >&2
    exit 1
  fi
  if (( node_major < NODE_MIN_MAJOR )); then
    echo "Error: Node.js v$NODE_MIN_MAJOR+ is required (found $(node -v))." >&2
    echo "Debian/Ubuntu upgrade example:" >&2
    echo "  sudo apt-get update" >&2
    echo "  sudo apt-get install -y ca-certificates curl gnupg" >&2
    echo "  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -" >&2
    echo "  sudo apt-get install -y nodejs" >&2
    exit 1
  fi
}

main() {
  local overwrite=""
  local skip_web=0
  local skip_web_build=0
  local skip_build=0
  local skip_db_download=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --overwrite)
        overwrite="--overwrite"
        shift 1
        ;;
      --skip-web)
        skip_web=1
        shift 1
        ;;
      --skip-web-build)
        skip_web_build=1
        shift 1
        ;;
      --skip-build)
        skip_build=1
        shift 1
        ;;
      --skip-db-download)
        skip_db_download=1
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
  echo "==> Using state dir: $STATE_DIR"

  echo "==> Setup venvs and dependencies"
  if [[ $skip_web -eq 1 ]]; then
    "$ROOT_DIR/scripts/setup_spacegate.sh" --skip-web
  else
    if [[ $skip_web_build -eq 1 ]]; then
      "$ROOT_DIR/scripts/setup_spacegate.sh" --skip-web-build
    else
      "$ROOT_DIR/scripts/setup_spacegate.sh"
    fi
  fi

  local core_db="$STATE_DIR/served/current/core.duckdb"
  if [[ $skip_build -eq 1 ]]; then
    echo "Skip build (--skip-build)"
  else
    if [[ ! -f "$core_db" ]]; then
      local bootstrap_enabled="${SPACEGATE_BOOTSTRAP_DB:-1}"
      local bootstrapped=0
      if [[ $skip_db_download -eq 0 && "$bootstrap_enabled" != "0" ]]; then
        echo "==> Core database not found; bootstrapping from Spacegate download"
        if \
          SPACEGATE_STATE_DIR="$STATE_DIR" \
          SPACEGATE_CACHE_DIR="$CACHE_DIR" \
          SPACEGATE_LOG_DIR="$LOG_DIR" \
          "$ROOT_DIR/scripts/bootstrap_core_db.sh" $overwrite; then
          bootstrapped=1
        else
          echo "Warning: bootstrap download failed; falling back to local source build." >&2
        fi
      fi
      if [[ $bootstrapped -eq 0 ]]; then
        echo "==> Building core database from source catalogs"
        SPACEGATE_STATE_DIR="$STATE_DIR" \
        SPACEGATE_CACHE_DIR="$CACHE_DIR" \
        SPACEGATE_LOG_DIR="$LOG_DIR" \
        "$ROOT_DIR/scripts/build_core.sh" $overwrite
      fi
    else
      echo "==> Core database exists: $core_db"
    fi
  fi

  echo "Install complete."
  if [[ $skip_build -eq 1 ]]; then
    echo "Next: scripts/build_core.sh to download and build the core database."
  elif [[ $skip_db_download -eq 1 ]]; then
    echo "Database install mode: local source build (--skip-db-download)."
  else
    echo "Database install mode: bootstrap download (fallback to source build)."
  fi
  echo "Then: scripts/run_spacegate.sh to start API (or add --web-dev for Vite)."
}

main "$@"
