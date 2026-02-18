#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_load_env_defaults "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-$ROOT_DIR/data}"
SERVED_DIR="$STATE_DIR/served"
OUT_DIR="$STATE_DIR/out"
REPORTS_DIR="$STATE_DIR/reports"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-}"
REQUIRE_REPORTS="${SPACEGATE_VERIFY_REQUIRE_REPORTS:-0}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/verify_build.sh [BUILD_ID]

If BUILD_ID is not provided, the script verifies $SPACEGATE_STATE_DIR/served/current.
Set SPACEGATE_VERIFY_REQUIRE_REPORTS=1 for strict report validation.
USAGE
}

resolve_path() {
  local path="$1"
  if command -v readlink >/dev/null 2>&1; then
    readlink -f "$path" 2>/dev/null || true
  fi
}

main() {
  if [[ -z "$PYTHON_BIN" ]]; then
    if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
      PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
    else
      PYTHON_BIN="python3"
    fi
  fi

  if ! "$PYTHON_BIN" - <<'PY' >/dev/null 2>&1; then
import duckdb
PY
    echo "Error: python module 'duckdb' not found." >&2
    echo "Tip: activate the project venv or install requirements:" >&2
    echo "  cd $ROOT_DIR" >&2
    echo "  python3 -m venv .venv && source .venv/bin/activate" >&2
    echo "  pip install -r requirements.txt" >&2
    exit 1
  fi

  local build_id="${1:-}"
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  local build_dir=""
  if [[ -n "$build_id" ]]; then
    build_dir="$OUT_DIR/$build_id"
  else
    if [[ ! -e "$SERVED_DIR/current" ]]; then
      echo "Error: $SERVED_DIR/current not found" >&2
      exit 1
    fi
    build_dir="$(resolve_path "$SERVED_DIR/current")"
    if [[ -z "$build_dir" ]]; then
      build_dir="$SERVED_DIR/current"
    fi
    build_id="$(basename "$build_dir")"
  fi

  echo "Verify target: $build_dir"

  if [[ ! -d "$build_dir" ]]; then
    echo "Error: build directory not found: $build_dir" >&2
    exit 1
  fi

  local core_db="$build_dir/core.duckdb"
  local parquet_dir="$build_dir/parquet"
  local reports_dir="$REPORTS_DIR/$build_id"

  if [[ ! -f "$core_db" ]]; then
    echo "Error: missing $core_db" >&2
    exit 1
  fi
  echo "OK: core.duckdb"

  if [[ ! -f "$parquet_dir/stars.parquet" || ! -f "$parquet_dir/systems.parquet" || ! -f "$parquet_dir/planets.parquet" ]]; then
    echo "Error: missing parquet files in $parquet_dir" >&2
    exit 1
  fi
  echo "OK: parquet exports"

  local qc_report="$reports_dir/qc_report.json"
  local match_report="$reports_dir/match_report.json"
  local prov_report="$reports_dir/provenance_report.json"

  local have_reports=1
  if [[ ! -d "$reports_dir" ]]; then
    have_reports=0
  elif [[ ! -f "$qc_report" || ! -f "$match_report" || ! -f "$prov_report" ]]; then
    have_reports=0
  fi

  if [[ $have_reports -eq 0 ]]; then
    if [[ "$REQUIRE_REPORTS" == "1" ]]; then
      echo "Error: missing reports for $build_id in $reports_dir" >&2
      echo "Set SPACEGATE_VERIFY_REQUIRE_REPORTS=0 to allow reportless prebuilt DB verification." >&2
      exit 1
    fi
    echo "Warning: reports missing for $build_id; continuing with relaxed verification." >&2
  else
    echo "OK: QC reports"
    "$PYTHON_BIN" - <<'PY' "$qc_report" "$build_id"
import json
import sys
from pathlib import Path

qc_path = Path(sys.argv[1])
build_id = sys.argv[2]

data = json.loads(qc_path.read_text())

if data.get("build_id") != build_id:
    raise SystemExit(f"QC build_id mismatch: {data.get('build_id')} != {build_id}")

violations = data.get("dist_invariant_violations")
if violations is None:
    raise SystemExit("QC report missing dist_invariant_violations")
if violations != 0:
    raise SystemExit(f"Distance invariant violations: {violations}")

counts = data.get("counts") or {}
if not all(counts.get(k, 0) > 0 for k in ("stars", "systems", "planets")):
    raise SystemExit(f"Invalid counts in QC report: {counts}")

print("OK: qc_report.json")
PY
  fi

  "$PYTHON_BIN" - <<'PY' "$core_db"
import sys
import duckdb

db_path = sys.argv[1]
con = duckdb.connect(db_path, read_only=True)

rows = con.execute(
    """
    select
      s.system_name,
      st.star_name,
      st.spectral_type_raw,
      p.planet_name,
      round(s.dist_ly, 2) as dist_ly,
      coalesce(round(p.mass_earth, 2), round(p.mass_jup, 2)) as mass,
      case
        when p.mass_earth is not null then 'M⊕'
        when p.mass_jup is not null then 'M♃'
        else null
      end as mass_unit,
      coalesce(round(p.radius_earth, 2), round(p.radius_jup, 2)) as radius,
      case
        when p.radius_earth is not null then 'R⊕'
        when p.radius_jup is not null then 'R♃'
        else null
      end as radius_unit,
      p.match_method,
      p.match_confidence
    from planets p
    join stars st on p.star_id = st.star_id
    join systems s on p.system_id = s.system_id
    where p.star_id is not null and p.system_id is not null
    order by random()
    limit 4
    """
).fetchall()

if not rows:
    raise SystemExit("No joined planet/star/system rows found in core.duckdb")

headers = [
    "system",
    "star",
    "spec",
    "planet",
    "dist_ly",
    "mass",
    "m_u",
    "radius",
    "r_u",
    "match",
    "conf",
]

print("OK: sample join")
values_rows = [[("" if v is None else str(v)) for v in row] for row in rows]
widths = []
for idx, header in enumerate(headers):
    max_val = max(len(r[idx]) for r in values_rows) if values_rows else 0
    widths.append(max(len(header), max_val))

def fmt_row(items):
    return "| " + " | ".join(item.ljust(widths[i]) for i, item in enumerate(items)) + " |"

print(fmt_row(headers))
print("| " + " | ".join("-" * w for w in widths) + " |")
for row in values_rows:
    print(fmt_row(row))
PY

  echo "Verified build $build_id"
  echo "Next: scripts/run_spacegate.sh to start API + web."
}

main "$@"
