#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export SPACEGATE_DEPLOY_PUBLIC_URL="${SPACEGATE_DEPLOY_PUBLIC_URL:-https://coolstars.org}"
export SPACEGATE_DEPLOY_EXPECT_AUTH="${SPACEGATE_DEPLOY_EXPECT_AUTH:-enabled}"
export SPACEGATE_DEPLOY_SSH_COOLDOWN_SECONDS="${SPACEGATE_DEPLOY_SSH_COOLDOWN_SECONDS:-3}"

exec "$ROOT_DIR/scripts/deploy_spacegate.sh" "$@"
