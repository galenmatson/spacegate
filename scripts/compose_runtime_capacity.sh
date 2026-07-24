#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/env_loader.sh"
spacegate_init_env "$ROOT_DIR"

export SPACEGATE_DATA_DIR="${SPACEGATE_DATA_DIR:-${SPACEGATE_STATE_DIR:-/data/spacegate/state}}"
export SPACEGATE_CONTAINER_UID="${SPACEGATE_CONTAINER_UID:-$(id -u)}"
export SPACEGATE_CONTAINER_GID="${SPACEGATE_CONTAINER_GID:-$(id -g)}"
export SPACEGATE_UMASK="${SPACEGATE_UMASK:-0002}"
export SPACEGATE_API_HOST_PORT="${SPACEGATE_CAPACITY_API_PORT:-18000}"
export SPACEGATE_WEB_BIND="127.0.0.1"
export SPACEGATE_WEB_HOST_PORT="${SPACEGATE_CAPACITY_WEB_PORT:-18081}"
export SPACEGATE_WEB_TLS_BIND="127.0.0.1"
export SPACEGATE_WEB_TLS_HOST_PORT="${SPACEGATE_CAPACITY_WEB_TLS_PORT:-18443}"
export SPACEGATE_WEB_TLS_DIR="${SPACEGATE_CAPACITY_TLS_DIR:-/tmp/spacegate-runtime-capacity-no-tls}"

install -d -m 0755 "$SPACEGATE_WEB_TLS_DIR"

exec docker compose \
  --project-name spacegate-capacity \
  -f "$ROOT_DIR/docker-compose.yml" \
  -f "$ROOT_DIR/config/runtime_capacity/docker-compose.capacity.yml" \
  "$@"
