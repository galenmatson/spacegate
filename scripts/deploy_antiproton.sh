#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

REMOTE="${SPACEGATE_DEPLOY_REMOTE:-sgdeploy@antiproton}"
REMOTE_APP_DIR="${SPACEGATE_DEPLOY_REMOTE_APP_DIR:-/srv/spacegate/app}"
PUBLIC_BASE_URL="${SPACEGATE_DEPLOY_PUBLIC_URL:-https://spacegates.org}"
EXPECT_AUTH="${SPACEGATE_DEPLOY_EXPECT_AUTH:-enabled}" # enabled|disabled|skip
SSH_KEY_PATH="${SPACEGATE_DEPLOY_SSH_KEY:-$HOME/.ssh/spacegate_antiproton}"
BUILD_IMAGES=1
CHECK_PUBLIC=1
AUTO_SCORE_COOLNESS="${SPACEGATE_DEPLOY_AUTO_SCORE_COOLNESS:-1}" # 1|0
SSH_RETRY_ATTEMPTS="${SPACEGATE_DEPLOY_SSH_RETRY_ATTEMPTS:-5}"
SSH_RETRY_DELAY_SECONDS="${SPACEGATE_DEPLOY_SSH_RETRY_DELAY_SECONDS:-2}"
DRY_RUN=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_antiproton.sh [options]

Sync local app code to a remote Spacegate host, restart compose services, and
run post-deploy health/auth checks. Protects remote env secrets by excluding:
  - .spacegate.env
  - .spacegate.local.env

Options:
  --remote HOST          SSH target (default: sgdeploy@antiproton)
  --remote-app-dir PATH  Remote app root (default: /srv/spacegate/app)
  --public-url URL       Public base URL to verify (default: https://spacegates.org)
  --expect-auth MODE     enabled|disabled|skip (default: enabled)
  --ssh-key PATH         SSH private key path (default: ~/.ssh/spacegate_antiproton)
  --no-build             Restart containers without --build
  --skip-auto-score      Skip remote auto-score when coolness outputs are missing
  --skip-public-check    Skip public URL checks
  --dry-run              Show rsync/deploy actions without changing remote files
  -h, --help             Show this help
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: missing required command: $cmd" >&2
    exit 1
  fi
}

assert_expect_auth() {
  case "$EXPECT_AUTH" in
    enabled|disabled|skip) ;;
    *)
      echo "Error: invalid --expect-auth value: $EXPECT_AUTH" >&2
      exit 1
      ;;
  esac
}

assert_auto_score_value() {
  case "$AUTO_SCORE_COOLNESS" in
    1|0) ;;
    *)
      echo "Error: invalid auto-score value: $AUTO_SCORE_COOLNESS" >&2
      exit 1
      ;;
  esac
}

ssh_with_retry() {
  local -a ssh_cmd=()
  while [[ $# -gt 2 ]]; do
    ssh_cmd+=("$1")
    shift
  done
  local remote_host="$1"
  local remote_cmd="$2"
  local attempt rc

  for (( attempt=1; attempt<=SSH_RETRY_ATTEMPTS; attempt++ )); do
    if ssh "${ssh_cmd[@]}" "$remote_host" "$remote_cmd"; then
      rc=0
      return 0
    else
      rc=$?
    fi
    if [[ "$attempt" -ge "$SSH_RETRY_ATTEMPTS" ]]; then
      return "$rc"
    fi
    echo "SSH attempt $attempt/$SSH_RETRY_ATTEMPTS failed; retrying in ${SSH_RETRY_DELAY_SECONDS}s..." >&2
    sleep "$SSH_RETRY_DELAY_SECONDS"
  done
}

remote_has_coolness_scores() {
  local -a ssh_cmd=("$@")
  ssh_with_retry "${ssh_cmd[@]}" "$REMOTE" "
    cd '$REMOTE_APP_DIR' &&
    scripts/compose_spacegate.sh exec -T api python -c \"import duckdb, os, sys; p='/data/served/current/rich.duckdb'; exists=os.path.exists(p); print('missing rich.duckdb at ' + p) if not exists else None; sys.exit(2) if not exists else None; con=duckdb.connect(p, read_only=True); has_table=con.execute(\\\"select count(*) from information_schema.tables where table_schema='main' and table_name='coolness_scores'\\\").fetchone()[0] > 0; print('missing coolness_scores table') if not has_table else None; rows=con.execute('select count(*) from coolness_scores').fetchone()[0] if has_table else -1; print(f'coolness_scores rows={rows}') if has_table else None; sys.exit(0 if has_table else 3)\"
  "
}

check_auth_enabled_json() {
  local payload="$1"
  local expected="$2"
  python3 -c '
import json
import sys

expected = sys.argv[1]
data = json.loads(sys.argv[2])
actual = data.get("auth_enabled")
if expected == "enabled" and actual is not True:
    raise SystemExit(f"auth_enabled mismatch: expected true, got {actual!r}\\nraw={data!r}")
if expected == "disabled" and actual is not False:
    raise SystemExit(f"auth_enabled mismatch: expected false, got {actual!r}\\nraw={data!r}")
print(f"auth_enabled={actual}")
' "$expected" "$payload"
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --remote)
        REMOTE="$2"
        shift 2
        ;;
      --remote-app-dir)
        REMOTE_APP_DIR="$2"
        shift 2
        ;;
      --public-url)
        PUBLIC_BASE_URL="$2"
        shift 2
        ;;
      --expect-auth)
        EXPECT_AUTH="$2"
        shift 2
        ;;
      --ssh-key)
        SSH_KEY_PATH="$2"
        shift 2
        ;;
      --no-build)
        BUILD_IMAGES=0
        shift 1
        ;;
      --skip-auto-score)
        AUTO_SCORE_COOLNESS=0
        shift 1
        ;;
      --skip-public-check)
        CHECK_PUBLIC=0
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

  require_cmd ssh
  require_cmd rsync
  require_cmd curl
  require_cmd python3
  assert_expect_auth
  assert_auto_score_value

  local -a ssh_opts=()
  if [[ -n "${SSH_KEY_PATH:-}" && -f "$SSH_KEY_PATH" ]]; then
    ssh_opts+=(-o IdentitiesOnly=yes -i "$SSH_KEY_PATH")
  fi
  ssh_opts+=(
    -o ConnectTimeout=8
    -o ConnectionAttempts=1
    -o ControlMaster=auto
    -o ControlPersist=60
    -o ControlPath=/tmp/spacegate-deploy-%C
  )
  local ssh_rsh="ssh"
  local opt
  for opt in "${ssh_opts[@]}"; do
    ssh_rsh+=" $(printf '%q' "$opt")"
  done

  local -a rsync_args=(
    -rz
    --delete
    --omit-dir-times
    --no-perms
    --no-owner
    --no-group
    --exclude=.git/
    --exclude=data/
    --exclude=.venv/
    --exclude=__pycache__/
    --exclude=*.pyc
    --exclude=.pytest_cache/
    --exclude=node_modules/
    --exclude=.spacegate.env
    --exclude=.spacegate.local.env
  )

  if [[ "$DRY_RUN" == "1" ]]; then
    rsync_args+=(-n -v)
  fi

  echo "Deploy target:    $REMOTE:$REMOTE_APP_DIR"
  echo "Public URL check: $PUBLIC_BASE_URL"
  echo "Expect auth:      $EXPECT_AUTH"
  echo "Build images:     $BUILD_IMAGES"
  echo "Auto-score miss:  $AUTO_SCORE_COOLNESS"
  echo "Dry run:          $DRY_RUN"
  if [[ "${#ssh_opts[@]}" -gt 0 ]]; then
    echo "SSH key:          $SSH_KEY_PATH"
  else
    echo "SSH key:          (ssh default auth)"
  fi

  if ! ssh_with_retry "${ssh_opts[@]}" "$REMOTE" "test -d '$REMOTE_APP_DIR' && command -v docker >/dev/null && command -v curl >/dev/null"; then
    echo "Error: SSH preflight failed for $REMOTE." >&2
    exit 1
  fi

  echo "Syncing app tree (env files excluded)..."
  rsync -e "$ssh_rsh" "${rsync_args[@]}" "$ROOT_DIR/" "$REMOTE:$REMOTE_APP_DIR/"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "Dry run complete. Skipping remote restart/checks."
    exit 0
  fi

  local compose_cmd
  if [[ "$BUILD_IMAGES" == "1" ]]; then
    compose_cmd="scripts/compose_spacegate.sh up -d --build api web"
  else
    compose_cmd="scripts/compose_spacegate.sh up -d api web"
  fi

  echo "Restarting remote services..."
  if ! ssh_with_retry "${ssh_opts[@]}" "$REMOTE" "cd '$REMOTE_APP_DIR' && $compose_cmd && scripts/compose_spacegate.sh ps"; then
    echo "Error: remote compose restart failed on $REMOTE." >&2
    exit 1
  fi

  if [[ "$AUTO_SCORE_COOLNESS" == "1" ]]; then
    echo "Checking remote coolness outputs..."
    if remote_has_coolness_scores "${ssh_opts[@]}"; then
      echo "Remote coolness outputs are present."
    else
      echo "Remote coolness outputs missing; running score_coolness on current build..."
      if ! ssh_with_retry "${ssh_opts[@]}" "$REMOTE" "cd '$REMOTE_APP_DIR' && scripts/compose_spacegate.sh exec -T api /app/scripts/score_coolness.sh score --latest-out"; then
        echo "Error: remote coolness scoring failed." >&2
        exit 1
      fi
      echo "Re-checking remote coolness outputs..."
      remote_has_coolness_scores "${ssh_opts[@]}"
    fi
  else
    echo "Skipping remote coolness auto-score check."
  fi

  echo "Running remote API checks..."
  local remote_health
  if ! remote_health="$(ssh_with_retry "${ssh_opts[@]}" "$REMOTE" "curl -fsS http://127.0.0.1:8000/api/v1/health")"; then
    echo "Error: failed to fetch remote /health via SSH." >&2
    exit 1
  fi
  echo "Remote /health: $remote_health"

  if [[ "$EXPECT_AUTH" != "skip" ]]; then
    local remote_auth
    if ! remote_auth="$(ssh_with_retry "${ssh_opts[@]}" "$REMOTE" "curl -fsS http://127.0.0.1:8000/api/v1/auth/me")"; then
      echo "Error: failed to fetch remote /auth/me via SSH." >&2
      exit 1
    fi
    echo "Remote /auth/me: $remote_auth"
    check_auth_enabled_json "$remote_auth" "$EXPECT_AUTH"
  fi

  if [[ "$CHECK_PUBLIC" == "1" ]]; then
    echo "Running public URL checks..."
    local public_health
    public_health="$(curl -fsS "$PUBLIC_BASE_URL/api/v1/health")"
    echo "Public /health: $public_health"

    if [[ "$EXPECT_AUTH" != "skip" ]]; then
      local public_auth
      public_auth="$(curl -fsS "$PUBLIC_BASE_URL/api/v1/auth/me")"
      echo "Public /auth/me: $public_auth"
      check_auth_enabled_json "$public_auth" "$EXPECT_AUTH"
    fi
  fi

  echo "Deploy complete."
}

main "$@"
