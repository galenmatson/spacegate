#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"$ROOT_DIR/scripts/catalogs.sh" --core "$@"
echo "Download complete."
echo "Next: scripts/cook_core.sh to normalize catalogs."
