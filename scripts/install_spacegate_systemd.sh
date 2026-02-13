#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_TEMPLATE="$ROOT_DIR/scripts/systemd/spacegate-api.service"
UNIT_PATH="/etc/systemd/system/spacegate-api.service"
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
API_PORT="${SPACEGATE_API_PORT:-8000}"
API_USER="${SPACEGATE_API_USER:-${SUDO_USER:-$(id -un)}}"

usage() {
  cat <<'USAGE'
Usage:
  sudo scripts/install_spacegate_systemd.sh

Installs and starts a systemd unit for the Spacegate API.
USAGE
}

require_root() {
  if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    echo "Error: this script must be run with sudo/root." >&2
    exit 1
  fi
}

require_systemd() {
  if ! command -v systemctl >/dev/null 2>&1; then
    echo "Error: systemctl not found (systemd required)." >&2
    exit 1
  fi
}

ensure_template() {
  if [[ ! -f "$UNIT_TEMPLATE" ]]; then
    echo "Error: unit template not found at $UNIT_TEMPLATE" >&2
    exit 1
  fi
}

ensure_uvicorn() {
  local uvicorn_bin="$ROOT_DIR/services/api/.venv/bin/uvicorn"
  if [[ ! -x "$uvicorn_bin" ]]; then
    echo "Error: uvicorn not found at $uvicorn_bin" >&2
    echo "Tip: run scripts/setup_spacegate.sh first." >&2
    exit 1
  fi
}

install_unit() {
  local tmp
  tmp="$(mktemp)"
  sed \
    -e "s|__SPACEGATE_ROOT__|$ROOT_DIR|g" \
    -e "s|__SPACEGATE_STATE_DIR__|$STATE_DIR|g" \
    -e "s|__SPACEGATE_API_PORT__|$API_PORT|g" \
    -e "s|__SPACEGATE_USER__|$API_USER|g" \
    "$UNIT_TEMPLATE" > "$tmp"

  if [[ -f "$UNIT_PATH" ]]; then
    if ! grep -q "Managed by Spacegate setup script" "$UNIT_PATH"; then
      echo "Error: $UNIT_PATH exists but is not managed by Spacegate." >&2
      echo "Refusing to overwrite." >&2
      rm -f "$tmp"
      exit 1
    fi
  fi

  cp "$tmp" "$UNIT_PATH"
  rm -f "$tmp"
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_root
  require_systemd
  ensure_template
  ensure_uvicorn
  install_unit

  systemctl daemon-reload
  systemctl enable spacegate-api.service
  systemctl restart spacegate-api.service

  echo "Spacegate API systemd service installed and started."
  echo "Check status: sudo systemctl status spacegate-api.service"
}

main "$@"
