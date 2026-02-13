#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
RAW_DIR="$STATE_DIR/raw"
COOKED_DIR="$STATE_DIR/cooked"
LOG_DIR="${SPACEGATE_LOG_DIR:-$STATE_DIR/logs}"
PYTHON_BIN=""

ATHYG_PART1="${ATHYG_PART1:-$RAW_DIR/athyg/athyg_v33-1.csv.gz}"
ATHYG_PART2="${ATHYG_PART2:-$RAW_DIR/athyg/athyg_v33-2.csv.gz}"
NASA_RAW="$RAW_DIR/nasa_exoplanet_archive/pscomppars.csv"

COOKED_ATHYG_DIR="$COOKED_DIR/athyg"
COOKED_NASA_DIR="$COOKED_DIR/nasa_exoplanet_archive"
COOKED_ATHYG="$COOKED_ATHYG_DIR/athyg.csv.gz"
COOKED_NASA="$COOKED_NASA_DIR/pscomppars_clean.csv"

LOG_FILE="$LOG_DIR/cook_core.log"

log() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '%s %s\n' "$ts" "$msg" | tee -a "$LOG_FILE"
}

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' not found." >&2
    exit 1
  fi
}

is_lfs_pointer() {
  local path="$1"
  [[ -f "$path" ]] || return 1
  head -c 256 "$path" 2>/dev/null | grep -aq "version https://git-lfs.github.com/spec/v1"
}

ensure_inputs() {
  local missing=0
  if [[ ! -f "$ATHYG_PART1" ]]; then
    echo "Missing: $ATHYG_PART1" >&2
    missing=1
  fi
  if [[ ! -f "$ATHYG_PART2" ]]; then
    echo "Missing: $ATHYG_PART2" >&2
    missing=1
  fi
  if [[ ! -f "$NASA_RAW" ]]; then
    echo "Missing: $NASA_RAW" >&2
    missing=1
  fi
  if [[ $missing -eq 0 ]]; then
    if is_lfs_pointer "$ATHYG_PART1" || is_lfs_pointer "$ATHYG_PART2"; then
      echo "AT-HYG files are Git LFS pointers. Re-run scripts/download_core.sh to fetch the real data." >&2
      exit 1
    fi
  fi
  if [[ $missing -ne 0 ]]; then
    exit 1
  fi
}

cook_athyg() {
  mkdir -p "$COOKED_ATHYG_DIR"

  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local tmp_concat="$tmp_dir/athyg.csv"
  local tmp_out="$tmp_dir/athyg.csv.gz"

  log "Cook: AT-HYG concat"

  local header1_file="$tmp_dir/athyg_header1.txt"
  local header2_file="$tmp_dir/athyg_header2.txt"
  "$PYTHON_BIN" - "$ATHYG_PART1" "$header1_file" <<'PY'
import gzip
import sys

in_path = sys.argv[1]
out_path = sys.argv[2]

with gzip.open(in_path, "rb") as f, open(out_path, "wb") as out:
    out.write(f.readline())
PY
  "$PYTHON_BIN" - "$ATHYG_PART2" "$header2_file" <<'PY'
import gzip
import sys

in_path = sys.argv[1]
out_path = sys.argv[2]

with gzip.open(in_path, "rb") as f, open(out_path, "wb") as out:
    out.write(f.readline())
PY

  gzip -dc "$ATHYG_PART1" > "$tmp_concat"
  if cmp -s "$header1_file" "$header2_file"; then
    gzip -dc "$ATHYG_PART2" | tail -n +2 >> "$tmp_concat"
  else
    gzip -dc "$ATHYG_PART2" >> "$tmp_concat"
  fi

  gzip -n -c "$tmp_concat" > "$tmp_out"
  mv "$tmp_out" "$COOKED_ATHYG"

  rm -rf "$tmp_dir"
}

cook_nasa() {
  mkdir -p "$COOKED_NASA_DIR"

  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local tmp_out="$tmp_dir/pscomppars_clean.csv"

  log "Cook: NASA Exoplanet Archive normalize"

  "$PYTHON_BIN" - "$NASA_RAW" "$tmp_out" <<'PY'
import sys
in_path = sys.argv[1]
out_path = sys.argv[2]

bom = b"\xef\xbb\xbf"

with open(in_path, "rb") as f, open(out_path, "wb") as out:
    first = f.read(3)
    if first != bom:
        f.seek(0)
    prev_cr = False
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        if prev_cr:
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
            else:
                out.write(b"\n")
            prev_cr = False
        if chunk.endswith(b"\r"):
            prev_cr = True
            chunk = chunk[:-1]
        chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        out.write(chunk)
    if prev_cr:
        out.write(b"\n")
PY

  mv "$tmp_out" "$COOKED_NASA"
  rm -rf "$tmp_dir"
}

main() {
  require_command gzip
  if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
    PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "Error: required command 'python3' (or 'python') not found." >&2
    exit 1
  fi

  mkdir -p "$COOKED_DIR" "$LOG_DIR"

  ensure_inputs

  log "Cook core begin"
  cook_athyg
  cook_nasa
  log "Cook core complete"
  echo "Next: scripts/ingest_core.sh to build the core database."
}

main "$@"
