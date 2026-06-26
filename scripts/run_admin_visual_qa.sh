#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  if declare -F spacegate_init_env >/dev/null 2>&1; then
    spacegate_init_env "$ROOT_DIR"
  fi
fi

MODE="docker"
ADMIN_WEB_DIR="$ROOT_DIR/srv/admin-web"
REPORT_ROOT="${SPACEGATE_ADMIN_VISUAL_REPORT_ROOT:-${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}/reports/admin_visual}"
BASE_URL="${SPACEGATE_ADMIN_VISUAL_BASE_URL:-https://photon.spacegates.org/admin/}"
PLAYWRIGHT_IMAGE="${SPACEGATE_PLAYWRIGHT_IMAGE:-mcr.microsoft.com/playwright:v1.61.1-noble}"
HOST_ALIAS="${SPACEGATE_ADMIN_VISUAL_HOST_ALIAS:-photon.spacegates.org:10.0.0.12}"
DEFAULT_STORAGE_STATE="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}/admin/playwright/admin-storage-state.json"
STORAGE_STATE="${SPACEGATE_ADMIN_STORAGE_STATE:-}"

if [[ -z "$STORAGE_STATE" && -f "$DEFAULT_STORAGE_STATE" ]]; then
  STORAGE_STATE="$DEFAULT_STORAGE_STATE"
fi

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_admin_visual_qa.sh [--docker|--local]

Runs the Admin v2 Playwright visual QA sweep.

Output:
  $SPACEGATE_ADMIN_VISUAL_REPORT_ROOT, defaulting to
  $SPACEGATE_STATE_DIR/reports/admin_visual

Useful environment:
  SPACEGATE_ADMIN_VISUAL_BASE_URL      Admin URL to test
  SPACEGATE_ADMIN_VISUAL_HOST_ALIAS    Optional Docker --add-host entry.
                                      Default: photon.spacegates.org:10.0.0.12
  SPACEGATE_ADMIN_STORAGE_STATE        Optional authenticated Playwright storageState JSON
                                      Defaults to $SPACEGATE_STATE_DIR/admin/playwright/admin-storage-state.json
                                      when that file exists.
  SPACEGATE_ADMIN_VISUAL_REPORT_ROOT   Report output root
  SPACEGATE_ADMIN_VISUAL_RUN_ID        Optional run id
  SPACEGATE_PLAYWRIGHT_IMAGE           Docker image for --docker mode

Authentication:
  Without SPACEGATE_ADMIN_STORAGE_STATE the harness captures the auth gate and
  writes an auth_required report. Provide a saved authenticated storage state to
  capture Admin screens. Use scripts/create_admin_storage_state.sh to create
  the default storage state from an authenticated browser Cookie header.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)
      MODE="docker"
      shift
      ;;
    --local)
      MODE="local"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "$REPORT_ROOT"

if [[ "$MODE" == "local" ]]; then
  cd "$ADMIN_WEB_DIR"
  export SPACEGATE_ADMIN_VISUAL_BASE_URL="$BASE_URL"
  export SPACEGATE_ADMIN_VISUAL_REPORT_ROOT="$REPORT_ROOT"
  export SPACEGATE_ADMIN_STORAGE_STATE="$STORAGE_STATE"
  exec npm run visual:admin
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is required for --docker mode. Use --local after installing Playwright browser dependencies." >&2
  exit 1
fi

storage_args=()
if [[ -n "$STORAGE_STATE" ]]; then
  storage_dir="$(dirname "$STORAGE_STATE")"
  storage_args+=("-v" "$storage_dir:$storage_dir:ro")
fi

host_args=()
if [[ -n "$HOST_ALIAS" ]]; then
  host_args+=("--add-host" "$HOST_ALIAS")
fi

exec docker run --rm \
  --network host \
  --ipc=host \
  "${host_args[@]}" \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e SPACEGATE_ADMIN_VISUAL_BASE_URL="$BASE_URL" \
  -e SPACEGATE_ADMIN_VISUAL_REPORT_ROOT="$REPORT_ROOT" \
  -e SPACEGATE_ADMIN_VISUAL_RUN_ID="${SPACEGATE_ADMIN_VISUAL_RUN_ID:-}" \
  -e SPACEGATE_ADMIN_STORAGE_STATE="$STORAGE_STATE" \
  -v "$ROOT_DIR:/work" \
  -v "$REPORT_ROOT:$REPORT_ROOT" \
  "${storage_args[@]}" \
  -w /work/srv/admin-web \
  "$PLAYWRIGHT_IMAGE" \
  npm run visual:admin
