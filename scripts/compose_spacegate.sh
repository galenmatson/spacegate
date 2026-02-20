#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

DOCKER_COMPOSE_FILE="${SPACEGATE_DOCKER_COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/compose_spacegate.sh <compose args...>

Examples:
  scripts/compose_spacegate.sh up -d --build
  scripts/compose_spacegate.sh down
  scripts/compose_spacegate.sh ps

Behavior:
  - Loads Spacegate env files via scripts/lib/env_loader.sh.
  - Ensures SPACEGATE_DATA_DIR defaults to SPACEGATE_STATE_DIR.
  - Executes docker compose with the configured compose file.
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found." >&2
    exit 1
  fi
}

main() {
  if [[ $# -eq 0 || "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_cmd docker
  export SPACEGATE_DATA_DIR="${SPACEGATE_DATA_DIR:-${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}}"

  exec docker compose -f "$DOCKER_COMPOSE_FILE" "$@"
}

main "$@"
