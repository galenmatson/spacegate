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

BASE_URL="${SPACEGATE_ADMIN_VISUAL_BASE_URL:-https://photon.spacegates.org/admin/}"
OUTPUT="${SPACEGATE_ADMIN_STORAGE_STATE:-${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}/admin/playwright/admin-storage-state.json}"
COOKIE_FILE=""
SESSION_COOKIE_NAME="${SPACEGATE_SESSION_COOKIE_NAME:-__Host-spacegate_session}"
CSRF_COOKIE_NAME="${SPACEGATE_CSRF_COOKIE_NAME:-__Host-spacegate_csrf}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/create_admin_storage_state.sh [options]

Creates a Playwright storageState JSON from an already authenticated Admin
browser Cookie header. The cookie value is treated as a session secret and is
never printed.

Options:
  --base-url URL              Admin base URL, default SPACEGATE_ADMIN_VISUAL_BASE_URL
                              or https://photon.spacegates.org/admin/
  --output PATH               storageState output path, default
                              $SPACEGATE_STATE_DIR/admin/playwright/admin-storage-state.json
  --cookie-file PATH          Read the Cookie header from a local file instead
                              of hidden terminal input.
  --session-cookie-name NAME  Expected session cookie name.
  --csrf-cookie-name NAME     Expected CSRF cookie name.
  -h, --help                  Show this help.

Input format:
  Paste the value of the browser request header named "Cookie", for example:
  __Host-spacegate_session=...; __Host-spacegate_csrf=...

Do not paste session cookies into chat, tickets, or logs.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url)
      BASE_URL="${2:?missing value for --base-url}"
      shift 2
      ;;
    --output)
      OUTPUT="${2:?missing value for --output}"
      shift 2
      ;;
    --cookie-file)
      COOKIE_FILE="${2:?missing value for --cookie-file}"
      shift 2
      ;;
    --session-cookie-name)
      SESSION_COOKIE_NAME="${2:?missing value for --session-cookie-name}"
      shift 2
      ;;
    --csrf-cookie-name)
      CSRF_COOKIE_NAME="${2:?missing value for --csrf-cookie-name}"
      shift 2
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

if [[ -n "$COOKIE_FILE" ]]; then
  if [[ ! -r "$COOKIE_FILE" ]]; then
    echo "Error: cookie file is not readable: $COOKIE_FILE" >&2
    exit 1
  fi
  COOKIE_HEADER="$(<"$COOKIE_FILE")"
else
  printf 'Paste Admin Cookie header for %s (input hidden): ' "$BASE_URL" >&2
  IFS= read -r -s COOKIE_HEADER
  printf '\n' >&2
fi

if [[ -z "${COOKIE_HEADER//[[:space:]]/}" ]]; then
  echo "Error: Cookie header was empty." >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT")"
umask 077

export SPACEGATE_ADMIN_STORAGE_BASE_URL="$BASE_URL"
export SPACEGATE_ADMIN_STORAGE_OUTPUT="$OUTPUT"
export SPACEGATE_ADMIN_COOKIE_HEADER="$COOKIE_HEADER"
export SPACEGATE_ADMIN_SESSION_COOKIE_NAME="$SESSION_COOKIE_NAME"
export SPACEGATE_ADMIN_CSRF_COOKIE_NAME="$CSRF_COOKIE_NAME"

node <<'NODE'
const fs = require("node:fs");

const baseUrl = process.env.SPACEGATE_ADMIN_STORAGE_BASE_URL || "";
const output = process.env.SPACEGATE_ADMIN_STORAGE_OUTPUT || "";
const rawHeader = process.env.SPACEGATE_ADMIN_COOKIE_HEADER || "";
const sessionCookieName =
  process.env.SPACEGATE_ADMIN_SESSION_COOKIE_NAME || "__Host-spacegate_session";
const csrfCookieName =
  process.env.SPACEGATE_ADMIN_CSRF_COOKIE_NAME || "__Host-spacegate_csrf";

if (!baseUrl || !output || !rawHeader) {
  throw new Error("missing required storageState inputs");
}

const url = new URL(baseUrl);
const header = rawHeader.replace(/^Cookie:\s*/i, "").trim();
const pairs = header
  .split(";")
  .map((part) => part.trim())
  .filter(Boolean);

const seen = new Set();
const cookies = [];
for (const pair of pairs) {
  const index = pair.indexOf("=");
  if (index <= 0) continue;
  const name = pair.slice(0, index).trim();
  const value = pair.slice(index + 1).trim();
  if (!name || seen.has(name)) continue;
  seen.add(name);
  cookies.push({
    name,
    value,
    domain: url.hostname,
    path: "/",
    expires: -1,
    httpOnly: name === sessionCookieName,
    secure: url.protocol === "https:",
    sameSite: "Lax",
  });
}

const names = new Set(cookies.map((cookie) => cookie.name));
if (!names.has(sessionCookieName)) {
  throw new Error(`missing expected session cookie: ${sessionCookieName}`);
}

const state = {
  cookies,
  origins: [],
};

fs.writeFileSync(output, `${JSON.stringify(state, null, 2)}\n`, {
  mode: 0o600,
});
fs.chmodSync(output, 0o600);

console.log(`Wrote Playwright storageState: ${output}`);
console.log(`Cookie count: ${cookies.length}`);
console.log(`Session cookie present: yes`);
console.log(`CSRF cookie present: ${names.has(csrfCookieName) ? "yes" : "no"}`);
NODE
