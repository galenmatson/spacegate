#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-python3}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/setup_spacegate.sh

Creates Python virtualenvs and installs dependencies for data tooling,
API, and the web UI (Vite).
USAGE
}

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found." >&2
    exit 1
  fi
}

create_venv() {
  local venv_dir="$1"
  if [[ ! -d "$venv_dir" ]]; then
    if ! "$PYTHON_BIN" -m venv "$venv_dir"; then
      echo "Error: failed to create venv at $venv_dir" >&2
      echo "Tip: on Debian/Ubuntu install python3-venv." >&2
      exit 1
    fi
  fi
}

ensure_pip() {
  local pybin="$1"
  if "$pybin" -m pip --version >/dev/null 2>&1; then
    return 0
  fi
  echo "pip not found for $pybin. Bootstrapping with ensurepip..." >&2
  if ! "$pybin" -m ensurepip --upgrade >/dev/null; then
    echo "Error: ensurepip failed for $pybin" >&2
    echo "Tip: on Debian/Ubuntu install python3-venv or python3-pip." >&2
    exit 1
  fi
}

setup_root_venv() {
  echo "==> Setup root venv (.venv)"
  create_venv "$ROOT_DIR/.venv"
  ensure_pip "$ROOT_DIR/.venv/bin/python"
  "$ROOT_DIR/.venv/bin/python" -m pip install -U pip >/dev/null
  "$ROOT_DIR/.venv/bin/python" -m pip install -r "$ROOT_DIR/requirements.txt"
}

setup_api_venv() {
  echo "==> Setup API venv (services/api/.venv)"
  local api_dir="$ROOT_DIR/services/api"
  create_venv "$api_dir/.venv"
  ensure_pip "$api_dir/.venv/bin/python"
  "$api_dir/.venv/bin/python" -m pip install -U pip >/dev/null
  "$api_dir/.venv/bin/python" -m pip install -r "$api_dir/requirements.txt"
}

setup_web_deps() {
  echo "==> Setup web dependencies (services/web)"
  local web_dir="$ROOT_DIR/services/web"
  require_command npm
  if [[ -f "$web_dir/package-lock.json" ]]; then
    (cd "$web_dir" && npm install)
  else
    (cd "$web_dir" && npm install)
  fi
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_command "$PYTHON_BIN"
  setup_root_venv
  setup_api_venv
  setup_web_deps
  echo "Setup complete. Next: scripts/build_core.sh && scripts/run_spacegate.sh"
}

main "$@"
