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

APPLY=0
STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
OWNER_SPEC="${SPACEGATE_STATE_OWNER:-}"
MODE_DIR="u+rwX,g+rwX,o-rwx"
SETGID=1

usage() {
  cat <<'USAGE'
Usage:
  scripts/normalize_state_permissions.sh [--apply] [--state-dir PATH] [--owner USER:GROUP] [--no-setgid]

Purpose:
  Normalize ownership and cooperative group permissions for generated Spacegate
  state created by Admin/API jobs or build scripts.

Defaults:
  - Dry-run only unless --apply is provided.
  - Uses SPACEGATE_STATE_DIR/SPACEGATE_DATA_DIR.
  - If run through sudo, defaults ownership to the invoking sudo user/group.
  - Otherwise defaults ownership to the current user/group.
  - Tightens the state root itself non-recursively.
  - Targets generated/admin paths only: admin, backups, cache, logs, out,
    reports, served.
  - Does not touch raw/ or cooked/.

Examples:
  scripts/normalize_state_permissions.sh
  sudo scripts/normalize_state_permissions.sh --apply
  sudo scripts/normalize_state_permissions.sh --apply --owner galen:galen
USAGE
}

default_owner_spec() {
  if [[ -n "${SUDO_UID:-}" && -n "${SUDO_GID:-}" ]]; then
    local user group
    user="$(getent passwd "$SUDO_UID" | cut -d: -f1 || true)"
    group="$(getent group "$SUDO_GID" | cut -d: -f1 || true)"
    if [[ -n "$user" && -n "$group" ]]; then
      printf '%s:%s\n' "$user" "$group"
      return
    fi
  fi
  printf '%s:%s\n' "$(id -un)" "$(id -gn)"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    --state-dir)
      STATE_DIR="${2:-}"
      shift 2
      ;;
    --owner)
      OWNER_SPEC="${2:-}"
      shift 2
      ;;
    --no-setgid)
      SETGID=0
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

if [[ -z "$STATE_DIR" ]]; then
  echo "Error: state dir is empty." >&2
  exit 2
fi

if [[ -z "$OWNER_SPEC" ]]; then
  OWNER_SPEC="$(default_owner_spec)"
fi

STATE_DIR="$(realpath -m "$STATE_DIR")"
if [[ ! -d "$STATE_DIR" ]]; then
  echo "Error: state dir does not exist: $STATE_DIR" >&2
  exit 1
fi

declare -a CANDIDATES=(
  "$STATE_DIR/admin"
  "$STATE_DIR/backups"
  "$STATE_DIR/cache"
  "$STATE_DIR/logs"
  "$STATE_DIR/out"
  "$STATE_DIR/reports"
  "$STATE_DIR/served"
)

declare -a TARGETS=()
for path in "${CANDIDATES[@]}"; do
  [[ -e "$path" ]] || continue
  resolved="$(realpath -m "$path")"
  case "$resolved" in
    "$STATE_DIR/raw"|"$STATE_DIR/raw/"*|"$STATE_DIR/cooked"|"$STATE_DIR/cooked/"*)
      echo "Skipping protected raw/cooked path: $resolved"
      continue
      ;;
    "$STATE_DIR"|"$STATE_DIR/"*)
      TARGETS+=("$path")
      ;;
    *)
      echo "Skipping path outside state dir: $path" >&2
      ;;
  esac
done

echo "Spacegate state permission normalization"
echo "State dir: $STATE_DIR"
echo "Owner:     $OWNER_SPEC"
echo "Mode:      $MODE_DIR"
echo "Setgid:    $SETGID"
echo "Apply:     $APPLY"
echo

if [[ "${#TARGETS[@]}" -eq 0 ]]; then
  echo "No generated/admin state targets found."
fi

printf 'State root:\n'
printf '  %s\n' "$STATE_DIR"
printf 'Targets:\n'
if [[ "${#TARGETS[@]}" -gt 0 ]]; then
  printf '  %s\n' "${TARGETS[@]}"
else
  printf '  (none)\n'
fi

if [[ "$APPLY" -ne 1 ]]; then
  cat <<'DRYRUN'

Dry run only. Re-run with --apply to execute:
  chown OWNER state root, non-recursively
  chmod u+rwX,g+rwX,o-rwx state root, non-recursively
  chmod g+s on state root
  chown -R OWNER each target
  chmod -R u+rwX,g+rwX,o-rwx each target
  chmod g+s on target directories
DRYRUN
  exit 0
fi

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Warning: not running as root; chown may fail for root-owned artifacts." >&2
fi

echo "Normalizing state root: $STATE_DIR"
chown "$OWNER_SPEC" "$STATE_DIR"
chmod "$MODE_DIR" "$STATE_DIR"
if [[ "$SETGID" -eq 1 ]]; then
  chmod g+s "$STATE_DIR"
fi

for path in "${TARGETS[@]}"; do
  echo "Normalizing: $path"
  chown -R "$OWNER_SPEC" "$path"
  chmod -R "$MODE_DIR" "$path"
  if [[ "$SETGID" -eq 1 ]]; then
    find "$path" -type d -exec chmod g+s {} +
  fi
done

echo "Done."
