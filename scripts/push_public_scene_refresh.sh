#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
REMOTE="${SPACEGATE_DEPLOY_REMOTE:-sgdeploy@158.69.198.29}"
REMOTE_APP_DIR="${SPACEGATE_DEPLOY_REMOTE_APP_DIR:-/srv/spacegate/app}"
REMOTE_STATE_DIR="${SPACEGATE_DEPLOY_REMOTE_STATE_DIR:-/srv/spacegate/data}"
SSH_KEY_PATH="${SPACEGATE_DEPLOY_SSH_KEY:-$HOME/.ssh/spacegate_antiproton}"
MANIFEST=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/push_public_scene_refresh.sh --manifest PATH [options]

Transfer and atomically install a build-matched frozen simulation-scene refresh
without retransferring or replacing the scientific, Public Read, or Smart Tag
artifacts.

Options:
  --manifest PATH       Updated full public edge release manifest (required)
  --remote HOST         SSH target
  --remote-app-dir PATH Remote app checkout
  --remote-state PATH   Remote hot Spacegate state root
  --ssh-key PATH        SSH private key
  -h, --help            Show this help
USAGE
}

manifest_value() {
  "$PYTHON_BIN" - "$MANIFEST" "$1" <<'PY'
import json, pathlib, sys
value = json.loads(pathlib.Path(sys.argv[1]).read_text())
for token in sys.argv[2].split("."):
    value = value[token]
print(value)
PY
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --manifest) MANIFEST="$2"; shift 2 ;;
      --remote) REMOTE="$2"; shift 2 ;;
      --remote-app-dir) REMOTE_APP_DIR="$2"; shift 2 ;;
      --remote-state) REMOTE_STATE_DIR="$2"; shift 2 ;;
      --ssh-key) SSH_KEY_PATH="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
    esac
  done
  [[ -n "$MANIFEST" && -f "$MANIFEST" ]] || {
    echo "Error: --manifest must name a release manifest." >&2
    exit 1
  }
  [[ -x "$PYTHON_BIN" ]] || { echo "Error: Python not found: $PYTHON_BIN" >&2; exit 1; }
  [[ -f "$SSH_KEY_PATH" ]] || { echo "Error: SSH key not found: $SSH_KEY_PATH" >&2; exit 1; }

  "$PYTHON_BIN" "$ROOT_DIR/scripts/public_edge_release.py" \
    verify-scene-source --manifest "$MANIFEST"

  local build_id scene_sha scene_source scene_name incoming remote_manifest
  build_id="$(manifest_value build_id)"
  scene_sha="$(manifest_value artifacts.simulation_scenes.sha256)"
  scene_source="$(manifest_value artifacts.simulation_scenes.source_path)"
  scene_name="$(manifest_value artifacts.simulation_scenes.transfer_filename)"
  incoming="$REMOTE_STATE_DIR/incoming/scene-refresh/$build_id/$scene_sha"
  remote_manifest="$incoming/release.json"

  ssh -i "$SSH_KEY_PATH" -o IdentitiesOnly=yes -o BatchMode=yes \
    -o ConnectTimeout=8 "$REMOTE" "mkdir -p '$incoming'"
  rsync -ah --partial --append-verify --info=progress2,stats2 \
    -e "ssh -i $(printf '%q' "$SSH_KEY_PATH") -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=8" \
    "$MANIFEST" "$REMOTE:$remote_manifest"
  rsync -ah --partial --append-verify --info=progress2,stats2 \
    -e "ssh -i $(printf '%q' "$SSH_KEY_PATH") -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=8" \
    "$scene_source" "$REMOTE:$incoming/$scene_name"

  ssh -i "$SSH_KEY_PATH" -o IdentitiesOnly=yes -o BatchMode=yes \
    -o ConnectTimeout=8 "$REMOTE" \
    "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py refresh-scenes --manifest '$remote_manifest' --state-dir '$REMOTE_STATE_DIR' --incoming-dir '$incoming'"
  curl -fsS https://coolstars.org/api/v1/health >/dev/null
  echo "Public scene refresh installed and health check passed: $build_id $scene_sha"
}

main "$@"
