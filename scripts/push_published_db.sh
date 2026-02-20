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
REMOTE="${SPACEGATE_PUSH_REMOTE:-antiproton}"
REMOTE_DL_ROOT="${SPACEGATE_PUSH_REMOTE_DL_ROOT:-/srv/spacegate/dl}"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-python3}"
SET_CURRENT_LINK=0
DRY_RUN=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/push_published_db.sh [options]

Push the currently published DB artifacts from local dl root to a remote host.
Reads local current.json to discover the archive + report files.

Options:
  --remote HOST         SSH target (default: antiproton, env SPACEGATE_PUSH_REMOTE)
  --remote-root PATH    Remote dl root (default: /srv/spacegate/dl,
                        env SPACEGATE_PUSH_REMOTE_DL_ROOT)
  --meta PATH           Local metadata file (default: $SPACEGATE_DL_ROOT/current.json)
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

  local rsync_mode="-avh"
  if [[ "$DRY_RUN" == "1" ]]; then
    rsync_mode="-avhn"
  fi

  echo "Local DL root:   $DL_ROOT"
  echo "Remote target:   $REMOTE:$REMOTE_DL_ROOT"
  echo "Build ID:        $build_id"
  echo "Artifact:        $artifact"
  echo "Files to push:   ${#unique_files[@]}"

  ssh "$REMOTE" "mkdir -p '$REMOTE_DL_ROOT/db' '$REMOTE_DL_ROOT/reports/$build_id'"

  (
    cd "$DL_ROOT"
    rsync "$rsync_mode" --relative "${unique_files[@]}" "$REMOTE:$REMOTE_DL_ROOT/"
  )

  if [[ "$SET_CURRENT_LINK" == "1" ]]; then
    ssh "$REMOTE" "ln -sfn '$artifact' '$REMOTE_DL_ROOT/current'"
    echo "Updated remote symlink: $REMOTE_DL_ROOT/current -> $artifact"
  else
    echo "Skipped remote current symlink update (use --set-current-link to enable)."
  fi

  echo "Push complete."
}

main "$@"
