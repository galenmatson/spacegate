#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

DL_ROOT="${SPACEGATE_DL_ROOT:-/srv/spacegate/dl}"
META_PATH="$DL_ROOT/current.json"
CATALOGS_META_PATH="$DL_ROOT/catalogs/current.json"
REMOTE="${SPACEGATE_PUSH_REMOTE:-spacegate-host}"
REMOTE_DL_ROOT="${SPACEGATE_PUSH_REMOTE_DL_ROOT:-/srv/spacegate/dl}"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-python3}"
SSH_KEY_PATH="${SPACEGATE_PUSH_SSH_KEY:-}"
SSH_COOLDOWN_SECONDS="${SPACEGATE_PUSH_SSH_COOLDOWN_SECONDS:-3}"
RSYNC_COMPRESS="${SPACEGATE_PUSH_RSYNC_COMPRESS:-0}"
SET_CURRENT_LINK=0
INCLUDE_CATALOGS=1
DRY_RUN=0
SSH_CONNECTION_COUNT=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/push_published_db.sh [options]

Push the currently published DB artifacts from local dl root to a remote host.
Reads local current.json to discover the archive + report files. If a local
catalog mirror exists at catalogs/current.json, the active catalog snapshot is
also pushed by default.

Options:
  --remote HOST         SSH target (default: spacegate-host, env SPACEGATE_PUSH_REMOTE)
  --remote-root PATH    Remote dl root (default: /srv/spacegate/dl,
                        env SPACEGATE_PUSH_REMOTE_DL_ROOT)
  --meta PATH           Local metadata file (default: $SPACEGATE_DL_ROOT/current.json)
  --catalogs-meta PATH  Local catalog mirror metadata (default: $SPACEGATE_DL_ROOT/catalogs/current.json)
  --ssh-key PATH        SSH private key path (default: ssh agent / ssh config)
  --ssh-cooldown SEC    Delay between successive SSH connections (default: 3,
                        env SPACEGATE_PUSH_SSH_COOLDOWN_SECONDS)
  --compress            Enable rsync compression (default: disabled; env
                        SPACEGATE_PUSH_RSYNC_COMPRESS=1). Leave disabled for
                        already-compressed DB archives such as .7z.
  --skip-catalogs       Skip pushing the active catalog mirror snapshot
  --set-current-link    Also set remote current symlink to metadata artifact path
  --dry-run             Show what would be transferred without writing remote files
  -h, --help            Show this help
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: missing required command: $cmd" >&2
    exit 1
  fi
}

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log_step() {
  echo "[$(timestamp_utc)] $*"
}

assert_nonnegative_decimal() {
  local value="$1"
  local label="$2"
  if [[ ! "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "Error: $label must be a non-negative number, got: $value" >&2
    exit 1
  fi
}

cooldown_before_ssh_connect() {
  if (( SSH_CONNECTION_COUNT > 0 )) && [[ "$SSH_COOLDOWN_SECONDS" != "0" && "$SSH_COOLDOWN_SECONDS" != "0.0" ]]; then
    echo "Cooling down ${SSH_COOLDOWN_SECONDS}s before next SSH connection..." >&2
    sleep "$SSH_COOLDOWN_SECONDS"
  fi
  SSH_CONNECTION_COUNT=$((SSH_CONNECTION_COUNT + 1))
}

run_ssh() {
  cooldown_before_ssh_connect
  ssh "$@"
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --remote)
        REMOTE="$2"
        shift 2
        ;;
      --remote-root)
        REMOTE_DL_ROOT="$2"
        shift 2
        ;;
      --meta)
        META_PATH="$2"
        shift 2
        ;;
      --catalogs-meta)
        CATALOGS_META_PATH="$2"
        shift 2
        ;;
      --ssh-key)
        SSH_KEY_PATH="$2"
        shift 2
        ;;
      --ssh-cooldown)
        SSH_COOLDOWN_SECONDS="$2"
        shift 2
        ;;
      --compress)
        RSYNC_COMPRESS=1
        shift 1
        ;;
      --skip-catalogs)
        INCLUDE_CATALOGS=0
        shift 1
        ;;
      --set-current-link)
        SET_CURRENT_LINK=1
        shift 1
        ;;
      --dry-run)
        DRY_RUN=1
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

  require_cmd "$PYTHON_BIN"
  require_cmd rsync
  require_cmd ssh
  assert_nonnegative_decimal "$SSH_COOLDOWN_SECONDS" "SSH cooldown"
  if [[ "$RSYNC_COMPRESS" != "0" && "$RSYNC_COMPRESS" != "1" ]]; then
    echo "Error: SPACEGATE_PUSH_RSYNC_COMPRESS must be 0 or 1, got: $RSYNC_COMPRESS" >&2
    exit 1
  fi

  if [[ ! -f "$META_PATH" ]]; then
    echo "Error: metadata not found: $META_PATH" >&2
    exit 1
  fi

  local parse_output
  if ! parse_output="$("$PYTHON_BIN" - "$META_PATH" <<'PY'
import json
import pathlib
import sys

meta_path = pathlib.Path(sys.argv[1]).resolve()
meta = json.loads(meta_path.read_text())

artifact = meta.get("artifact") or meta.get("file") or ""
build_id = meta.get("build_id") or ""
if not artifact:
    raise SystemExit("current.json missing artifact/file field")
if not build_id:
    name = pathlib.PurePosixPath(artifact).name
    for suffix in (".tar.zst", ".tar.gz", ".tgz", ".7z", ".zip", ".tar"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    build_id = name

print(f"artifact={artifact}")
print(f"build_id={build_id}")
print("file=current.json")
print(f"file={artifact}")

reports = meta.get("reports", {})
if isinstance(reports, dict):
    for item in reports.values():
        if isinstance(item, dict):
            path = item.get("path")
            if path:
                print(f"file={path}")
PY
)"; then
    echo "Error: unable to parse metadata: $META_PATH" >&2
    exit 1
  fi

  local artifact=""
  local build_id=""
  local -a rel_files=()

  while IFS= read -r line; do
    case "$line" in
      artifact=*)
        artifact="${line#artifact=}"
        ;;
      build_id=*)
        build_id="${line#build_id=}"
        ;;
      file=*)
        rel_files+=("${line#file=}")
        ;;
    esac
  done <<<"$parse_output"

  if [[ -z "$artifact" || -z "$build_id" ]]; then
    echo "Error: parsed metadata is missing artifact/build_id" >&2
    exit 1
  fi

  local catalogs_snapshot_id=""
  local catalogs_index=""
  local catalogs_enabled=0
  if [[ "$INCLUDE_CATALOGS" == "1" && -f "$CATALOGS_META_PATH" ]]; then
    local catalogs_output
    if ! catalogs_output="$("$PYTHON_BIN" - "$CATALOGS_META_PATH" <<'PY'
import json
import pathlib
import sys

meta_path = pathlib.Path(sys.argv[1]).resolve()
meta = json.loads(meta_path.read_text())
snapshot_id = str(meta.get("snapshot_id") or "").strip()
index_path = str(meta.get("index") or "").strip()
if not snapshot_id:
    raise SystemExit("catalogs/current.json missing snapshot_id")
if not index_path:
    raise SystemExit("catalogs/current.json missing index")
print(f"snapshot_id={snapshot_id}")
print(f"index={index_path}")
PY
)"; then
      echo "Error: unable to parse catalog mirror metadata: $CATALOGS_META_PATH" >&2
      exit 1
    fi
    while IFS= read -r line; do
      case "$line" in
        snapshot_id=*)
          catalogs_snapshot_id="${line#snapshot_id=}"
          ;;
        index=*)
          catalogs_index="${line#index=}"
          ;;
      esac
    done <<<"$catalogs_output"
    if [[ -z "$catalogs_snapshot_id" || -z "$catalogs_index" ]]; then
      echo "Error: parsed catalog mirror metadata is missing snapshot_id/index" >&2
      exit 1
    fi
    catalogs_enabled=1
  fi

  local -a checked_files=()
  local rel
  for rel in "${rel_files[@]}"; do
    local full="$DL_ROOT/$rel"
    if [[ ! -f "$full" ]]; then
      echo "Error: missing local file referenced by metadata: $full" >&2
      exit 1
    fi
    checked_files+=("$rel")
  done

  # Deduplicate while preserving order.
  local -A seen=()
  local -a unique_files=()
  for rel in "${checked_files[@]}"; do
    if [[ -z "${seen[$rel]:-}" ]]; then
      unique_files+=("$rel")
      seen["$rel"]=1
    fi
  done

  local -a rsync_args=( -ah --omit-dir-times --no-implied-dirs --info=progress2,stats2 )
  if [[ "$RSYNC_COMPRESS" == "1" ]]; then
    rsync_args+=( -z )
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    rsync_args=( -ahn --omit-dir-times --no-implied-dirs )
    if [[ "$RSYNC_COMPRESS" == "1" ]]; then
      rsync_args+=( -z )
    fi
  fi

  local -a ssh_opts=()
  if [[ -n "${SSH_KEY_PATH:-}" ]]; then
    if [[ ! -f "$SSH_KEY_PATH" ]]; then
      echo "Error: SSH key path does not exist: $SSH_KEY_PATH" >&2
      exit 1
    fi
    ssh_opts+=(-o IdentitiesOnly=yes -i "$SSH_KEY_PATH")
  fi
  local ssh_rsh="ssh"
  local opt
  for opt in "${ssh_opts[@]}"; do
    ssh_rsh+=" $(printf '%q' "$opt")"
  done

  echo "Local DL root:   $DL_ROOT"
  echo "Remote target:   $REMOTE:$REMOTE_DL_ROOT"
  echo "Build ID:        $build_id"
  echo "Artifact:        $artifact"
  echo "Files to push:   ${#unique_files[@]}"
  if [[ "$catalogs_enabled" == "1" ]]; then
    echo "Catalog mirror:  snapshot $catalogs_snapshot_id"
  else
    echo "Catalog mirror:  skipped"
  fi
  if [[ -n "${SSH_KEY_PATH:-}" ]]; then
    echo "SSH key:         $SSH_KEY_PATH"
  else
    echo "SSH key:         (ssh default auth)"
  fi
  echo "SSH cooldown:    ${SSH_COOLDOWN_SECONDS}s"
  if [[ "$RSYNC_COMPRESS" == "1" ]]; then
    echo "Rsync compress:  enabled"
  else
    echo "Rsync compress:  disabled"
  fi

  log_step "Ensuring remote DB/report directories exist..."
  run_ssh "${ssh_opts[@]}" "$REMOTE" "mkdir -p '$REMOTE_DL_ROOT/db' '$REMOTE_DL_ROOT/reports/$build_id'"

  log_step "Syncing published DB metadata, archive, and reports..."
  (
    cd "$DL_ROOT"
    cooldown_before_ssh_connect
    rsync -e "$ssh_rsh" "${rsync_args[@]}" --relative "${unique_files[@]}" "$REMOTE:$REMOTE_DL_ROOT/"
  )
  log_step "DB/report sync finished."

  if [[ "$catalogs_enabled" == "1" ]]; then
    log_step "Ensuring remote catalog mirror directories exist..."
    run_ssh "${ssh_opts[@]}" "$REMOTE" "mkdir -p '$REMOTE_DL_ROOT/catalogs/snapshots'"
    log_step "Syncing active catalog mirror snapshot $catalogs_snapshot_id..."
    (
      cd "$DL_ROOT"
      cooldown_before_ssh_connect
      rsync -e "$ssh_rsh" "${rsync_args[@]}" --links \
        --relative \
        "catalogs/current.json" \
        "catalogs/current" \
        "catalogs/$catalogs_index" \
        "catalogs/snapshots/$catalogs_snapshot_id/" \
        "$REMOTE:$REMOTE_DL_ROOT/"
    )
    log_step "Catalog mirror sync finished."
  fi

  if [[ "$SET_CURRENT_LINK" == "1" ]]; then
    log_step "Updating remote current symlink..."
    run_ssh "${ssh_opts[@]}" "$REMOTE" "ln -sfn '$artifact' '$REMOTE_DL_ROOT/current'"
    echo "Updated remote symlink: $REMOTE_DL_ROOT/current -> $artifact"
  else
    echo "Skipped remote current symlink update (use --set-current-link to enable)."
  fi

  log_step "Push complete."
}

main "$@"
