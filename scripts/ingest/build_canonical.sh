#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  # shellcheck disable=SC1090
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

usage() {
  cat <<'USAGE'
Usage:
  scripts/ingest/build_canonical.sh --build-id <source_build_id> [--canonical-build-id <canonical_build_id>]

Runs the canonicalization stages over an existing core/arm build and emits
a standalone canonical build.
USAGE
}

main() {
  local build_id=""
  local canonical_build_id=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --build-id)
        build_id="${2:-}"
        shift 2
        ;;
      --canonical-build-id)
        canonical_build_id="${2:-}"
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

  if [[ -z "$build_id" ]]; then
    echo "Error: --build-id is required." >&2
    usage
    exit 1
  fi

  echo "==> Canonical ingest: normalize sources ($build_id)"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/ingest/normalize_sources.py" --build-id "$build_id"

  echo "==> Canonical ingest: build identity graph ($build_id)"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/ingest/build_identity_graph.py" --build-id "$build_id"

  echo "==> Canonical ingest: reduce canonical objects ($build_id)"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/ingest/reduce_canonical_objects.py" --build-id "$build_id"

  echo "==> Canonical ingest: build canonical hierarchy ($build_id)"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/ingest/build_canonical_hierarchy.py" --build-id "$build_id"

  local -a emit_args=("--build-id" "$build_id")
  if [[ -n "$canonical_build_id" ]]; then
    emit_args+=("--canonical-build-id" "$canonical_build_id")
  fi

  echo "==> Canonical ingest: emit canonical build"
  "$PYTHON_BIN" "$ROOT_DIR/scripts/ingest/emit_canonical_build.py" "${emit_args[@]}"
}

main "$@"
