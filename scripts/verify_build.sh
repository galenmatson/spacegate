#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/scripts/lib/env_loader.sh" ]]; then
  source "$ROOT_DIR/scripts/lib/env_loader.sh"
  spacegate_init_env "$ROOT_DIR"
fi
STATE_DIR="${SPACEGATE_STATE_DIR:-${SPACEGATE_DATA_DIR:-$ROOT_DIR/data}}"
SERVED_DIR="$STATE_DIR/served"
OUT_DIR="$STATE_DIR/out"
REPORTS_DIR="$STATE_DIR/reports"
PYTHON_BIN="${SPACEGATE_PYTHON_BIN:-}"
REQUIRE_REPORTS="${SPACEGATE_VERIFY_REQUIRE_REPORTS:-0}"
VERIFY_MULTIPLICITY_GOLDENS="${SPACEGATE_VERIFY_MULTIPLICITY_GOLDENS:-1}"
VERIFY_DETERMINISTIC_RERUN="${SPACEGATE_VERIFY_DETERMINISTIC_RERUN:-1}"

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
  local duplicate_report="$reports_dir/duplicate_trap_report.json"
  local prov_report="$reports_dir/provenance_report.json"
  local planet_delta_report="$reports_dir/planet_catalog_delta_report.json"
  local planet_reclass_report="$reports_dir/planet_reclassification_report.json"

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

for key in (
    "gaia_backbone_row_count_check_match",
    "gaia_classprob_row_count_check_match",
    "gaia_nss_non_single_row_count_check_match",
    "gaia_nss_two_body_row_count_check_match",
):
    value = data.get(key)
    if value is False:
        raise SystemExit(f"Gaia TAP completeness check failed ({key}=false)")

print("OK: qc_report.json")
PY

    if [[ -f "$duplicate_report" ]]; then
      "$PYTHON_BIN" - <<'PY' "$duplicate_report" "$build_id"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
build_id = sys.argv[2]
data = json.loads(path.read_text())
if data.get("build_id") != build_id:
    raise SystemExit(f"duplicate_trap_report build_id mismatch: {data.get('build_id')} != {build_id}")
near = (data.get("near_pair_totals") or {})
for key in ("candidate_pairs", "likely_duplicate_pairs", "high_confidence_pairs"):
    if key not in near:
        raise SystemExit(f"duplicate_trap_report missing near_pair_totals.{key}")
print("OK: duplicate_trap_report.json")
PY
    elif [[ "$REQUIRE_REPORTS" == "1" ]]; then
      echo "Error: missing duplicate trap report: $duplicate_report" >&2
      exit 1
    else
      echo "Warning: duplicate trap report missing: $duplicate_report" >&2
    fi

    "$PYTHON_BIN" - <<'PY' "$qc_report" "$planet_delta_report" "$planet_reclass_report"
import json
import sys
from pathlib import Path

qc_path = Path(sys.argv[1])
delta_path = Path(sys.argv[2])
reclass_path = Path(sys.argv[3])

qc = json.loads(qc_path.read_text())
lifecycle_enabled = bool(qc.get("exoplanet_lifecycle_catalogs_enabled"))

if not lifecycle_enabled:
    print("OK: exoplanet lifecycle reports not required for this build")
    raise SystemExit(0)

if not delta_path.exists():
    raise SystemExit(f"Missing lifecycle report: {delta_path}")
if not reclass_path.exists():
    raise SystemExit(f"Missing lifecycle report: {reclass_path}")

delta = json.loads(delta_path.read_text())
reclass = json.loads(reclass_path.read_text())
if not delta.get("lifecycle_enabled"):
    raise SystemExit("planet_catalog_delta_report lifecycle_enabled=false but QC indicates lifecycle enabled")
if not reclass.get("lifecycle_enabled"):
    raise SystemExit("planet_reclassification_report lifecycle_enabled=false but QC indicates lifecycle enabled")

stale_rows = int(reclass.get("stale_classifier_rows") or 0)
if stale_rows != 0:
    raise SystemExit(f"Lifecycle stale classifier rows: {stale_rows}")

print("OK: exoplanet lifecycle reports")
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

  "$PYTHON_BIN" - <<'PY' "$core_db"
import sys
import duckdb

db_path = sys.argv[1]
con = duckdb.connect(db_path, read_only=True)

sol = con.execute(
    """
    select system_id
    from systems
    where lower(coalesce(system_name_norm, '')) = 'sol'
       or lower(coalesce(stable_object_key, '')) = 'system:sol'
    order by system_id
    limit 1
    """
).fetchone()
if not sol:
    raise SystemExit("Sol gate failed: missing Sol system row")
sol_system_id = int(sol[0])

sun_count = int(
    con.execute(
        """
        select count(*)::bigint
        from stars
        where system_id = ?
          and lower(coalesce(star_name_norm, '')) = 'sun'
        """,
        [sol_system_id],
    ).fetchone()[0]
    or 0
)
if sun_count < 1:
    raise SystemExit("Sol gate failed: missing Sun star row linked to Sol system")

major_required = {"mercury", "venus", "earth", "mars", "jupiter", "saturn", "uranus", "neptune"}
sol_planets = {
    str(row[0] or "").strip().lower()
    for row in con.execute(
        """
        select planet_name_norm
        from planets
        where system_id = ?
        """,
        [sol_system_id],
    ).fetchall()
}
missing = sorted(name for name in major_required if name not in sol_planets)
if missing:
    raise SystemExit(f"Sol gate failed: missing required major planets: {', '.join(missing)}")

sol_planet_count = int(
    con.execute(
        "select count(*)::bigint from planets where system_id = ?",
        [sol_system_id],
    ).fetchone()[0]
    or 0
)
if sol_planet_count < 8:
    raise SystemExit(f"Sol gate failed: expected >=8 planets linked to Sol; got {sol_planet_count}")

print(f"OK: Sol gate (system_id={sol_system_id}, planets={sol_planet_count}, sun_rows={sun_count})")
PY

  local alias_search_script="$ROOT_DIR/scripts/verify_alias_search.py"
  if [[ ! -x "$alias_search_script" ]]; then
    echo "Error: missing executable $alias_search_script" >&2
    exit 1
  fi
  "$PYTHON_BIN" "$alias_search_script" --core-db "$core_db"

  local arm_db="$build_dir/arm.duckdb"
  if [[ -f "$arm_db" ]]; then
    "$PYTHON_BIN" - <<'PY' "$arm_db" "${SPACEGATE_ENABLE_SOL_ARTIFICIAL:-1}"
import sys
import duckdb

db_path = sys.argv[1]
enable_sol_artificial = str(sys.argv[2]).strip().lower() not in {"0", "false", "no", "off"}
con = duckdb.connect(db_path, read_only=True)

required_tables = {
    "component_entities",
    "system_hierarchy_edges",
    "orbit_edges",
    "orbital_solutions",
    "barycenters",
    "sol_small_body_objects",
}
if enable_sol_artificial:
    required_tables.add("sol_artificial_objects")
present = {
    row[0]
    for row in con.execute(
        "select table_name from information_schema.tables where table_schema='main'"
    ).fetchall()
}
missing_tables = sorted(required_tables - present)
if missing_tables:
    raise SystemExit(f"Sol S2 gate failed: missing arm tables: {', '.join(missing_tables)}")

moon_component_count = int(
    con.execute(
        """
        select count(*)::bigint
        from component_entities
        where component_type = 'moon'
          and source_catalog = 'sol_authority'
        """
    ).fetchone()[0]
    or 0
)
if moon_component_count < 5:
    raise SystemExit(f"Sol S2 gate failed: expected >=5 moon components, got {moon_component_count}")

earth_moon_edge_count = int(
    con.execute(
        """
        select count(*)::bigint
        from system_hierarchy_edges e
        join component_entities parent_ce
          on parent_ce.stable_component_key = e.parent_component_key
        join component_entities child_ce
          on child_ce.stable_component_key = e.child_component_key
        where parent_ce.component_type = 'planet'
          and lower(coalesce(parent_ce.display_name, '')) = 'earth'
          and child_ce.stable_component_key = 'comp:moon:sol:moon'
          and e.source_catalog = 'sol_authority'
        """
    ).fetchone()[0]
    or 0
)
if earth_moon_edge_count < 1:
    raise SystemExit("Sol S2 gate failed: missing Earth->Moon hierarchy edge")

satellite_orbit_count = int(
    con.execute(
        """
        select count(*)::bigint
        from orbit_edges
        where relation_kind = 'satellite'
          and source_catalog = 'sol_authority'
        """
    ).fetchone()[0]
    or 0
)
if satellite_orbit_count < 5:
    raise SystemExit(
        f"Sol S2 gate failed: expected >=5 sol_authority satellite orbit edges, got {satellite_orbit_count}"
    )

barycenter_count = int(
    con.execute(
        """
        select count(*)::bigint
        from barycenters
        where barycenter_key in ('bary:center:sol:earth-moon', 'bary:center:sol:pluto-charon')
        """
    ).fetchone()[0]
    or 0
)
if barycenter_count < 2:
    raise SystemExit(
        "Sol S2 gate failed: missing expected Earth-Moon and Pluto-Charon barycenter rows"
    )

print(
    f"OK: Sol S2 gate (moon_components={moon_component_count}, "
    f"satellite_orbits={satellite_orbit_count}, barycenters={barycenter_count})"
)

small_body_count = int(
    con.execute(
        """
        select count(*)::bigint
        from sol_small_body_objects
        where source_catalog = 'sol_authority'
        """
    ).fetchone()[0]
    or 0
)
if small_body_count < 20:
    raise SystemExit(
        f"Sol S3 gate failed: expected >=20 named minor bodies in sol_small_body_objects, got {small_body_count}"
    )

asteroid_count = int(
    con.execute("select count(*)::bigint from sol_small_body_objects where body_kind = 'asteroid'").fetchone()[0]
    or 0
)
tno_count = int(
    con.execute("select count(*)::bigint from sol_small_body_objects where body_kind = 'tno'").fetchone()[0]
    or 0
)
comet_count = int(
    con.execute("select count(*)::bigint from sol_small_body_objects where body_kind = 'comet'").fetchone()[0]
    or 0
)
if asteroid_count < 5 or tno_count < 3 or comet_count < 1:
    raise SystemExit(
        "Sol S3 gate failed: expected asteroid/tno/comet coverage "
        f"(got asteroids={asteroid_count}, tnos={tno_count}, comets={comet_count})"
    )

small_body_orbit_count = int(
    con.execute(
        """
        select count(*)::bigint
        from orbit_edges
        where relation_kind = 'orbits'
          and source_catalog = 'sol_authority'
        """
    ).fetchone()[0]
    or 0
)
if small_body_orbit_count < small_body_count:
    raise SystemExit(
        "Sol S3 gate failed: missing orbit edges for one or more small bodies "
        f"(objects={small_body_count}, orbit_edges={small_body_orbit_count})"
    )

print(
    f"OK: Sol S3 gate (minor_bodies={small_body_count}, "
    f"asteroids={asteroid_count}, tnos={tno_count}, comets={comet_count})"
)

if enable_sol_artificial:
    artificial_count = int(
        con.execute(
            """
            select count(*)::bigint
            from sol_artificial_objects
            where source_catalog = 'sol_artificial'
            """
        ).fetchone()[0]
        or 0
    )
    if artificial_count < 6:
        raise SystemExit(
            f"Sol S4 gate failed: expected >=6 artificial objects in sol_artificial_objects, got {artificial_count}"
        )

    artificial_orbit_count = int(
        con.execute(
            """
            select count(*)::bigint
            from orbit_edges
            where relation_kind = 'artificial_orbit'
              and source_catalog = 'sol_artificial'
            """
        ).fetchone()[0]
        or 0
    )
    if artificial_orbit_count < artificial_count:
        raise SystemExit(
            "Sol S4 gate failed: missing orbit edges for one or more artificial objects "
            f"(objects={artificial_count}, orbit_edges={artificial_orbit_count})"
        )

    deep_space_probe_count = int(
        con.execute(
            """
            select count(*)::bigint
            from sol_artificial_objects
            where coalesce(artifact_kind, '') = 'deep_space_probe'
            """
        ).fetchone()[0]
        or 0
    )
    if deep_space_probe_count < 3:
        raise SystemExit(
            f"Sol S4 gate failed: expected >=3 deep-space probes, got {deep_space_probe_count}"
        )

    print(
        f"OK: Sol S4 gate (artificial_objects={artificial_count}, "
        f"deep_space_probes={deep_space_probe_count})"
    )
else:
    print("SKIP: Sol S4 gate (SPACEGATE_ENABLE_SOL_ARTIFICIAL=0)")
PY
  else
    echo "Warning: skipping Sol S2 arm gate (missing $arm_db)." >&2
  fi

  if [[ "$VERIFY_MULTIPLICITY_GOLDENS" == "1" ]]; then
    local goldens_script="$ROOT_DIR/scripts/verify_multiplicity_goldens.py"
    if [[ ! -x "$goldens_script" ]]; then
      echo "Error: missing executable $goldens_script" >&2
      exit 1
    fi
    echo "Running multiplicity goldens exam..."
    "$PYTHON_BIN" "$goldens_script" --core-db "$core_db" --arm-db "$arm_db" --require-arm
    echo "OK: multiplicity goldens"
  fi

  if [[ "$VERIFY_DETERMINISTIC_RERUN" == "1" && $have_reports -eq 1 ]]; then
    local deterministic_script="$ROOT_DIR/scripts/verify_deterministic_rerun.py"
    local determinism_report="$reports_dir/determinism_report.json"
    if [[ -f "$deterministic_script" ]]; then
      if [[ -f "$determinism_report" ]]; then
        "$PYTHON_BIN" "$deterministic_script" \
          --state-dir "$STATE_DIR" \
          --build-id "$build_id"
        echo "OK: deterministic rerun check"
      elif [[ "$REQUIRE_REPORTS" == "1" ]]; then
        echo "Error: missing determinism report: $determinism_report" >&2
        exit 1
      else
        echo "Warning: skipping deterministic rerun check (missing $determinism_report)." >&2
      fi
    else
      echo "Warning: missing deterministic rerun checker: $deterministic_script" >&2
    fi
  elif [[ "$VERIFY_DETERMINISTIC_RERUN" == "1" ]]; then
    echo "Warning: skipping deterministic rerun check (reports missing)." >&2
  fi

  echo "Verified build $build_id"
  echo "Next (Docker default): scripts/compose_spacegate.sh up -d --build api web"
  echo "Host mode (no Docker): scripts/run_spacegate.sh"
}

main "$@"
