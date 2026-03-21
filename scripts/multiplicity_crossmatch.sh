#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi

PYTHON_BIN=""
if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Error: python3 not found." >&2
  exit 1
fi

"$PYTHON_BIN" - <<'PY' >/dev/null 2>&1 || {
import duckdb
PY
  echo "Error: python module 'duckdb' not found for $PYTHON_BIN" >&2
  echo "Tip: run scripts/setup_spacegate.sh (or install requirements.txt in the root venv)." >&2
  exit 1
}

exec "$PYTHON_BIN" "$ROOT_DIR/scripts/multiplicity_crossmatch.py" "$@"
