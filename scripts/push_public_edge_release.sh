#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
REMOTE="${SPACEGATE_DEPLOY_REMOTE:-sgdeploy@158.69.198.29}"
REMOTE_APP_DIR="${SPACEGATE_DEPLOY_REMOTE_APP_DIR:-/srv/spacegate/app}"
REMOTE_STATE_DIR="${SPACEGATE_DEPLOY_REMOTE_STATE_DIR:-/srv/spacegate/data}"
REMOTE_COLD_ROOT="${SPACEGATE_DEPLOY_REMOTE_COLD_ROOT:-}"
COLD_VOLUME_ID="${SPACEGATE_DEPLOY_COLD_VOLUME_ID:-}"
SSH_KEY_PATH="${SPACEGATE_DEPLOY_SSH_KEY:-$HOME/.ssh/spacegate_antiproton}"
SSH_COOLDOWN_SECONDS="${SPACEGATE_DEPLOY_SSH_COOLDOWN_SECONDS:-3}"
MANIFEST=""
DRY_RUN=0
INSTALL_HOT=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/push_public_edge_release.sh --manifest PATH [options]

Transfer and stage a verified four-artifact public edge release without
activating it. The scientific archive is extracted and removed before the
public-read projection is transferred, bounding peak disk occupancy.

Options:
  --manifest PATH       Local public edge release manifest (required)
  --remote HOST         SSH target
  --remote-app-dir PATH Remote app checkout
  --remote-state PATH   Remote Spacegate state root
  --remote-cold-root PATH
                        Verified cold volume application root
  --cold-volume-id UUID Expected cold volume identity marker
  --install-hot         Install verified cold stage onto fast state storage
  --ssh-key PATH        SSH private key
  --ssh-cooldown SEC    Pause between SSH connections (default: 3)
  --dry-run             Print and validate without remote writes
USAGE
}

cooldown() {
  if [[ "${CONNECTED:-0}" == "1" && "$SSH_COOLDOWN_SECONDS" != "0" ]]; then
    sleep "$SSH_COOLDOWN_SECONDS"
  fi
  CONNECTED=1
}

ssh_run() {
  cooldown
  ssh -i "$SSH_KEY_PATH" -o IdentitiesOnly=yes -o BatchMode=yes \
    -o ConnectTimeout=8 "$REMOTE" "$@"
}

manifest_value() {
  "$PYTHON_BIN" - "$MANIFEST" "$1" <<'PY'
import json, pathlib, sys
value=json.loads(pathlib.Path(sys.argv[1]).read_text())
for token in sys.argv[2].split("."):
    value=value[token]
print(value)
PY
}

sync_one() {
  local source="$1"
  local destination="$2"
  local -a args=(-ah --partial --append-verify --info=progress2,stats2)
  [[ "$DRY_RUN" == "1" ]] && args+=(-n)
  cooldown
  rsync -e "ssh -i $(printf '%q' "$SSH_KEY_PATH") -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=8" \
    "${args[@]}" "$source" "$REMOTE:$destination"
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --manifest) MANIFEST="$2"; shift 2 ;;
      --remote) REMOTE="$2"; shift 2 ;;
      --remote-app-dir) REMOTE_APP_DIR="$2"; shift 2 ;;
      --remote-state) REMOTE_STATE_DIR="$2"; shift 2 ;;
      --remote-cold-root) REMOTE_COLD_ROOT="$2"; shift 2 ;;
      --cold-volume-id) COLD_VOLUME_ID="$2"; shift 2 ;;
      --install-hot) INSTALL_HOT=1; shift ;;
      --ssh-key) SSH_KEY_PATH="$2"; shift 2 ;;
      --ssh-cooldown) SSH_COOLDOWN_SECONDS="$2"; shift 2 ;;
      --dry-run) DRY_RUN=1; shift ;;
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
  if [[ -n "$REMOTE_COLD_ROOT" || -n "$COLD_VOLUME_ID" ]]; then
    [[ -n "$REMOTE_COLD_ROOT" && -n "$COLD_VOLUME_ID" ]] || {
      echo "Error: --remote-cold-root and --cold-volume-id must be used together." >&2
      exit 1
    }
  fi
  if [[ "$INSTALL_HOT" == "1" && -z "$REMOTE_COLD_ROOT" ]]; then
    echo "Error: --install-hot requires a verified cold stage." >&2
    exit 1
  fi
  "$PYTHON_BIN" "$ROOT_DIR/scripts/public_edge_release.py" verify-source --manifest "$MANIFEST"

  local build_id incoming remote_manifest stage_state capacity_root
  build_id="$(manifest_value build_id)"
  if [[ -n "$REMOTE_COLD_ROOT" ]]; then
    incoming="$REMOTE_COLD_ROOT/incoming/public-edge/$build_id"
    stage_state="$REMOTE_COLD_ROOT/staged/public-edge"
    capacity_root="$REMOTE_COLD_ROOT"
  else
    incoming="$REMOTE_STATE_DIR/incoming/public-edge/$build_id"
    stage_state="$REMOTE_STATE_DIR"
    capacity_root="$REMOTE_STATE_DIR"
  fi
  remote_manifest="$incoming/release.json"
  echo "Release:       $build_id"
  echo "Remote:        $REMOTE"
  echo "Incoming root: $incoming"
  echo "Staged state:  $stage_state"
  echo "Activation:    intentionally not performed"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "Dry run: remote directory creation and staging commands are not executed."
  else
    if [[ -n "$REMOTE_COLD_ROOT" ]]; then
      ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_cold_storage.py verify-volume --cold-root '$REMOTE_COLD_ROOT' --hot-state-dir '$REMOTE_STATE_DIR' --volume-id '$COLD_VOLUME_ID'"
    fi
    ssh_run "mkdir -p '$incoming' '$stage_state'"
  fi
  sync_one "$MANIFEST" "$remote_manifest"

  local core_source core_name public_source public_name scene_source scene_name
  local tags_source tags_name
  local required_free available_free
  required_free="$(manifest_value transfer.minimum_start_free_bytes)"
  available_free="$(ssh_run "df -B1 --output=avail '$capacity_root' | tail -1 | tr -d '[:space:]'")"
  if [[ ! "$available_free" =~ ^[0-9]+$ || "$available_free" -lt "$required_free" ]]; then
    echo "Error: remote staging reserve is insufficient." >&2
    echo "Required free bytes before transfer: $required_free" >&2
    echo "Available free bytes: ${available_free:-unknown}" >&2
    exit 1
  fi
  echo "Remote free-space gate passed: $available_free bytes available."
  ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py verify-runtime-env --manifest '$remote_manifest' --env-file .spacegate.env --env-file .spacegate.local.env"
  echo "Remote measured runtime contract passed."

  core_source="$(manifest_value artifacts.scientific_build.source_path)"
  core_name="$(manifest_value artifacts.scientific_build.transfer_filename)"
  public_source="$(manifest_value artifacts.public_read.source_path)"
  public_name="$(manifest_value artifacts.public_read.transfer_filename)"
  scene_source="$(manifest_value artifacts.simulation_scenes.source_path)"
  scene_name="$(manifest_value artifacts.simulation_scenes.transfer_filename)"
  tags_source="$(manifest_value artifacts.smart_tags.source_path)"
  tags_name="$(manifest_value artifacts.smart_tags.transfer_filename)"

  echo "Transferring and staging scientific build..."
  sync_one "$core_source" "$incoming/$core_name"
  if [[ "$DRY_RUN" != "1" ]]; then
    ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py stage-scientific --manifest '$remote_manifest' --state-dir '$stage_state' --incoming-dir '$incoming' && rm -f '$incoming/$core_name'"
  fi

  echo "Transferring and staging public-read projection..."
  sync_one "$public_source" "$incoming/$public_name"
  if [[ "$DRY_RUN" != "1" ]]; then
    ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py stage-public-read --manifest '$remote_manifest' --state-dir '$stage_state' --incoming-dir '$incoming'"
  fi

  echo "Transferring and staging frozen simulation scenes..."
  sync_one "$scene_source" "$incoming/$scene_name"
  if [[ "$DRY_RUN" != "1" ]]; then
    ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py stage-scenes --manifest '$remote_manifest' --state-dir '$stage_state' --incoming-dir '$incoming'"
  fi

  echo "Transferring and staging Smart Tags..."
  sync_one "$tags_source" "$incoming/$tags_name"
  if [[ "$DRY_RUN" != "1" ]]; then
    ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py stage-smart-tags --manifest '$remote_manifest' --state-dir '$stage_state' --incoming-dir '$incoming'"
    ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py verify-installed --manifest '$remote_manifest' --state-dir '$stage_state'"
    if [[ -n "$REMOTE_COLD_ROOT" ]]; then
      ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py plan-install-from-state --manifest '$remote_manifest' --source-state-dir '$stage_state' --state-dir '$REMOTE_STATE_DIR'"
      if [[ "$INSTALL_HOT" == "1" ]]; then
        ssh_run "cd '$REMOTE_APP_DIR' && python3 scripts/public_edge_release.py install-from-state --manifest '$remote_manifest' --source-state-dir '$stage_state' --state-dir '$REMOTE_STATE_DIR'"
      fi
    fi
  fi
  if [[ -n "$REMOTE_COLD_ROOT" && "$INSTALL_HOT" != "1" ]]; then
    echo "Release verified on cold storage; hot install not requested: $build_id"
  else
    echo "Release staged but not activated: $build_id"
  fi
}

main "$@"
