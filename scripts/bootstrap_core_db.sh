#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
OUT_DIR="$STATE_DIR/out"
CACHE_DIR="${SPACEGATE_CACHE_DIR:-$STATE_DIR/cache}"
DOWNLOAD_DIR="${SPACEGATE_BOOTSTRAP_DOWNLOAD_DIR:-$CACHE_DIR/downloads}"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-python3}"
PUBLIC_BASE_URL="${SPACEGATE_PUBLIC_BASE_URL:-https://coolstars.org}"
META_URL="${SPACEGATE_BOOTSTRAP_META_URL:-${PUBLIC_BASE_URL%/}/dl/current.json}"
BASE_URL="${SPACEGATE_BOOTSTRAP_BASE_URL:-}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/bootstrap_core_db.sh [--overwrite] [--meta-url URL] [--base-url URL]

Downloads the current prebuilt core database archive from Spacegate metadata,
extracts it into $SPACEGATE_STATE_DIR/out/<BUILD_ID>, and promotes it.

Options:
  --overwrite      Re-download archive and replace an existing extracted build.
  --meta-url URL   Metadata URL (default: SPACEGATE_BOOTSTRAP_META_URL or SPACEGATE_PUBLIC_BASE_URL/dl/current.json; default public base https://coolstars.org).
  --base-url URL   Base URL for relative artifact paths in metadata.
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: missing required command: $cmd" >&2
    exit 1
  fi
}

resolve_artifact_url() {
  local base_url="$1"
  local artifact_path="$2"

  "$PYTHON_BIN" - "$base_url" "$artifact_path" <<'PY'
import sys
from urllib.parse import urljoin

base = sys.argv[1]
path = sys.argv[2]
if base and not base.endswith("/"):
    base = base + "/"
print(urljoin(base, path))
PY
}

read_metadata() {
  local json_path="$1"

  "$PYTHON_BIN" - "$json_path" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())

build_id = data.get("build_id", "")
artifact = data.get("artifact") or data.get("file") or ""
bytes_val = data.get("bytes", "")
sha256 = data.get("sha256", "")

if not artifact:
    raise SystemExit("Metadata is missing 'artifact' (or legacy 'file').")

if not build_id:
    name = pathlib.PurePosixPath(artifact).name
    for suffix in (".tar.zst", ".tar.gz", ".tgz", ".7z", ".zip", ".tar"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    build_id = name

if not build_id:
    raise SystemExit("Unable to determine build_id from metadata.")

if bytes_val is None:
    bytes_val = ""

print(build_id)
print(artifact)
print(bytes_val)
print(sha256 or "")
PY
}

extract_archive() {
  local archive_path="$1"
  local target_out_dir="$2"

  case "$archive_path" in
    *.7z)
      require_cmd 7z
      7z x -y "-o$target_out_dir" "$archive_path" >/dev/null
      ;;
    *.tar.zst|*.tzst)
      require_cmd tar
      tar --zstd -xf "$archive_path" -C "$target_out_dir"
      ;;
    *.tar.gz|*.tgz)
      require_cmd tar
      tar -xzf "$archive_path" -C "$target_out_dir"
      ;;
    *.tar)
      require_cmd tar
      tar -xf "$archive_path" -C "$target_out_dir"
      ;;
    *)
      echo "Error: unsupported archive type: $archive_path" >&2
      echo "Supported extensions: .7z, .tar.zst, .tar.gz, .tgz, .tar" >&2
      exit 1
      ;;
  esac
}

main() {
  local overwrite=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --overwrite)
        overwrite=1
        shift 1
        ;;
      --meta-url)
        META_URL="$2"
        shift 2
        ;;
      --base-url)
        BASE_URL="$2"
        shift 2
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
  require_cmd curl
  require_cmd sha256sum
  require_cmd stat

  echo "Using state dir: $STATE_DIR"
  echo "Using cache dir: $CACHE_DIR"
  echo "Using download dir: $DOWNLOAD_DIR"
  if [[ -z "${SPACEGATE_STATE_DIR:-}" && -z "${SPACEGATE_DATA_DIR:-}" ]]; then
    if [[ -d /srv/spacegate/data && "$STATE_DIR" != "/srv/spacegate/data" ]]; then
      echo "Warning: defaulting to $STATE_DIR while /srv/spacegate/data exists." >&2
      echo "Tip: set SPACEGATE_STATE_DIR (or SPACEGATE_DATA_DIR) to your desired state path." >&2
    fi
  fi

  mkdir -p "$DOWNLOAD_DIR" "$OUT_DIR"

  local tmp_meta=""
  local tmp_parse_err=""
  tmp_meta="$(mktemp)"
  tmp_parse_err="$(mktemp)"
  trap 'rm -f "${tmp_meta:-}" "${tmp_parse_err:-}"' EXIT

  echo "Fetching metadata: $META_URL"
  curl -fsSL "$META_URL" -o "$tmp_meta"

  local -a meta=()
  if ! mapfile -t meta < <(read_metadata "$tmp_meta" 2>"$tmp_parse_err"); then
    echo "Error: failed to parse metadata from $META_URL" >&2
    if [[ -s "$tmp_parse_err" ]]; then
      cat "$tmp_parse_err" >&2
    fi
    echo "Tip: validate JSON with: python3 -m json.tool /path/to/current.json" >&2
    echo "Common issue: trailing text after JSON (for example a stray 'EOF')." >&2
    exit 1
  fi
  if [[ ${#meta[@]} -lt 4 ]]; then
    echo "Error: failed to parse metadata from $META_URL" >&2
    if [[ -s "$tmp_parse_err" ]]; then
      cat "$tmp_parse_err" >&2
    fi
    exit 1
  fi

  local build_id="${meta[0]}"
  local artifact_path="${meta[1]}"
  local expected_bytes="${meta[2]}"
  local expected_sha="${meta[3]}"
  local artifact_base="$BASE_URL"
  if [[ -z "$artifact_base" ]]; then
    artifact_base="${META_URL%/*}/"
  fi
  local artifact_url
  artifact_url="$(resolve_artifact_url "$artifact_base" "$artifact_path")"

  local archive_name
  archive_name="$(basename "$artifact_path")"
  local archive_path="$DOWNLOAD_DIR/$archive_name"
  local build_dir="$OUT_DIR/$build_id"

  if [[ -f "$archive_path" && $overwrite -eq 0 ]]; then
    echo "Using existing archive: $archive_path"
  else
    rm -f "$archive_path"
    echo "Downloading artifact: $artifact_url"
    if command -v aria2c >/dev/null 2>&1; then
      if ! aria2c \
        --allow-overwrite=true \
        --auto-file-renaming=false \
        --continue=true \
        --dir "$DOWNLOAD_DIR" \
        --out "$archive_name" \
        "$artifact_url"; then
        echo "Warning: aria2c download failed; retrying with curl." >&2
        curl -fL "$artifact_url" -o "$archive_path"
      fi
    else
      curl -fL "$artifact_url" -o "$archive_path"
    fi
  fi

  if [[ "$expected_bytes" =~ ^[0-9]+$ ]]; then
    local actual_bytes
    actual_bytes="$(stat -c '%s' "$archive_path")"
    if [[ "$actual_bytes" != "$expected_bytes" ]]; then
      echo "Error: size mismatch for $archive_path (expected $expected_bytes, got $actual_bytes)." >&2
      exit 1
    fi
  fi

  if [[ "$expected_sha" =~ ^[0-9a-fA-F]{64}$ ]]; then
    local actual_sha
    actual_sha="$(sha256sum "$archive_path" | awk '{print $1}')"
    if [[ "${actual_sha,,}" != "${expected_sha,,}" ]]; then
      echo "Error: sha256 mismatch for $archive_path" >&2
      echo "Expected: $expected_sha" >&2
      echo "Actual:   $actual_sha" >&2
      exit 1
    fi
  elif [[ -n "$expected_sha" ]]; then
    echo "Warning: metadata sha256 value is not a valid hex digest; skipping hash check." >&2
  fi

  if [[ -d "$build_dir" && $overwrite -eq 1 ]]; then
    rm -rf "$build_dir"
  fi

  if [[ ! -d "$build_dir" ]]; then
    echo "Extracting build into: $OUT_DIR"
    extract_archive "$archive_path" "$OUT_DIR"
  else
    echo "Using existing extracted build: $build_dir"
  fi

  if [[ ! -f "$build_dir/core.duckdb" ]]; then
    echo "Error: extracted build is missing $build_dir/core.duckdb" >&2
    exit 1
  fi

  "$ROOT_DIR/scripts/promote_build.sh" "$build_id"

  if [[ ! -d "$STATE_DIR/reports/$build_id" ]]; then
    echo "Warning: reports for $build_id are not present in $STATE_DIR/reports." >&2
    echo "run_spacegate.sh may require relaxed verification if reports are omitted from published artifacts." >&2
  fi

  echo "Bootstrapped build: $build_id"
  echo "Archive cache: $archive_path"
  echo "Next: scripts/run_spacegate.sh to start API + web."
}

main "$@"
