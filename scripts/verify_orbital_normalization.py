#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

try:
    import duckdb
except ModuleNotFoundError as exc:
    raise SystemExit(
        "python module 'duckdb' not found. Run from the project venv "
        "(for example: /srv/spacegate/app/.venv/bin/python scripts/verify_orbital_normalization.py)"
    ) from exc


def normalize_name(value: str) -> str:
    return " ".join(value.strip().lower().replace("-", " ").split())


def resolve_default_paths(root: Path) -> tuple[Path, Path]:
    env_state = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    state_dir = Path(env_state) if env_state else Path("/data/spacegate/state")
    served = state_dir / "served" / "current"
    return served / "core.duckdb", served / "arm.duckdb"


def find_system_id(core: duckdb.DuckDBPyConnection, aliases: list[str]) -> int | None:
    normalized = [normalize_name(alias) for alias in aliases]
    placeholders = ",".join(["?"] * len(normalized))
    row = core.execute(
        f"""
        select system_id
        from aliases
        where target_type = 'system'
          and alias_norm in ({placeholders})
        order by alias_priority asc nulls last, is_primary desc, system_id asc
        limit 1
        """,
        normalized,
    ).fetchone()
    if row:
        return int(row[0])
    row = core.execute(
        f"""
        select system_id
        from systems
        where system_name_norm in ({placeholders})
        order by system_id asc
        limit 1
        """,
        normalized,
    ).fetchone()
    return int(row[0]) if row else None


def check(condition: bool, failures: list[str], message: str) -> None:
    if not condition:
        failures.append(message)


def check_range(value: Any, low: float, high: float, failures: list[str], message: str) -> None:
    if value is None:
        failures.append(f"{message}: missing value")
        return
    numeric = float(value)
    if not (low <= numeric <= high):
        failures.append(f"{message}: expected {low}..{high}, got {numeric}")


def planet_orbit_rows(
    core: duckdb.DuckDBPyConnection,
    system_id: int,
) -> list[dict[str, Any]]:
    rows = core.execute(
        """
        select
          p.planet_name,
          p.planet_name_norm,
          p.orbital_period_days as core_period_days,
          os.period_days as arm_period_days,
          p.semi_major_axis_au as core_sma_au,
          os.semi_major_axis_au as arm_sma_au,
          os.solution_source_catalog,
          os.normalization_method,
          os.confidence_tier,
          e.primary_component_key,
          e.secondary_component_key
        from planets p
        join arm_db.orbit_edges e
          on e.secondary_component_key = 'comp:planet:' || p.stable_object_key
         and e.relation_kind = 'planetary_orbit'
        left join arm_db.orbital_solutions os
          on os.orbit_edge_id = e.orbit_edge_id
         and os.solution_rank = 1
        where p.system_id = ?
        order by p.planet_name_norm asc
        """,
        [system_id],
    ).fetchall()
    columns = [
        "planet_name",
        "planet_name_norm",
        "core_period_days",
        "arm_period_days",
        "core_sma_au",
        "arm_sma_au",
        "solution_source_catalog",
        "normalization_method",
        "confidence_tier",
        "primary_component_key",
        "secondary_component_key",
    ]
    return [dict(zip(columns, row)) for row in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify ARM planet-orbit normalization benchmarks.")
    root = Path(__file__).resolve().parents[1]
    default_core, default_arm = resolve_default_paths(root)
    parser.add_argument("--core-db", default=str(default_core), help="Path to core.duckdb")
    parser.add_argument("--arm-db", default=str(default_arm), help="Path to arm.duckdb")
    args = parser.parse_args()

    core_path = Path(args.core_db)
    arm_path = Path(args.arm_db)
    if not core_path.exists():
        raise SystemExit(f"Core DB not found: {core_path}")
    if not arm_path.exists():
        raise SystemExit(f"ARM DB not found: {arm_path}")

    failures: list[str] = []
    core = duckdb.connect(str(core_path), read_only=True)
    core.execute(f"attach '{str(arm_path).replace("'", "''")}' as arm_db (read_only)")

    summary: dict[str, Any] = {
        "core_db": str(core_path),
        "arm_db": str(arm_path),
        "status": "pass",
        "checks": {},
    }

    planet_edge_count, planet_solution_count = core.execute(
        """
        select
          count(distinct e.orbit_edge_id)::bigint,
          count(os.orbital_solution_id)::bigint
        from arm_db.orbit_edges e
        left join arm_db.orbital_solutions os on os.orbit_edge_id = e.orbit_edge_id
        where e.relation_kind = 'planetary_orbit'
        """
    ).fetchone()
    max_solution_count = core.execute(
        """
        select coalesce(max(solution_count), 0)::bigint
        from (
          select e.orbit_edge_id, count(os.orbital_solution_id)::bigint as solution_count
          from arm_db.orbit_edges e
          left join arm_db.orbital_solutions os on os.orbit_edge_id = e.orbit_edge_id
          where e.relation_kind = 'planetary_orbit'
          group by e.orbit_edge_id
        )
        """
    ).fetchone()[0]
    rank1_duplicate_count = core.execute(
        """
        select count(*)::bigint
        from (
          select e.orbit_edge_id, count(os.orbital_solution_id)::bigint as rank1_count
          from arm_db.orbit_edges e
          join arm_db.orbital_solutions os on os.orbit_edge_id = e.orbit_edge_id
          where e.relation_kind = 'planetary_orbit'
            and os.solution_rank = 1
          group by e.orbit_edge_id
          having count(os.orbital_solution_id) > 1
        )
        """
    ).fetchone()[0]
    nasa_ps_solution_count = core.execute(
        """
        select count(*)::bigint
        from arm_db.orbital_solutions
        where solution_source_catalog = 'nasa_exoplanet_archive'
          and json_extract_string(fit_quality_json, '$.solver') = 'nasa_ps'
        """
    ).fetchone()[0]
    nasa_ps_source_csv = core.execute(
        """
        select coalesce(max(value), '')
        from arm_db.build_metadata
        where key = 'arm_source_nasa_ps_csv'
        """
    ).fetchone()[0]
    summary["checks"]["planet_orbit_counts"] = {
        "edges": int(planet_edge_count or 0),
        "solutions": int(planet_solution_count or 0),
        "max_solutions_per_edge": int(max_solution_count or 0),
        "duplicate_rank1_edges": int(rank1_duplicate_count or 0),
        "nasa_ps_alternate_solutions": int(nasa_ps_solution_count or 0),
    }
    check(int(planet_edge_count or 0) >= 2500, failures, "expected at least 2500 ARM planet orbit edges")
    check(int(planet_solution_count or 0) >= 2500, failures, "expected at least 2500 ARM planet orbital solutions")
    check(int(rank1_duplicate_count or 0) == 0, failures, "planetary orbit edges must not fan out to duplicate rank-1 solutions")
    if nasa_ps_source_csv:
        check(
            int(nasa_ps_solution_count or 0) > 0,
            failures,
            "NASA ps source is present but no alternate planet orbital solutions were materialized",
        )

    trappist_id = find_system_id(core, ["trappist 1", "trappist-1"])
    check(trappist_id is not None, failures, "TRAPPIST-1 system not found")
    if trappist_id is not None:
        rows = planet_orbit_rows(core, trappist_id)
        periods = [float(row["arm_period_days"]) for row in rows if row.get("arm_period_days") is not None]
        summary["checks"]["trappist_1"] = {
            "system_id": trappist_id,
            "planet_rows": len(rows),
            "period_days": periods,
        }
        check(len(rows) == 7, failures, f"TRAPPIST-1 expected 7 ARM planet orbit rows, got {len(rows)}")
        check(periods == sorted(periods), failures, "TRAPPIST-1 ARM periods should sort in orbital order")
        check(
            all(row.get("normalization_method") == "source_native_planet_orbit" for row in rows),
            failures,
            "TRAPPIST-1 rows should use source_native_planet_orbit normalization",
        )

    cancri_id = find_system_id(core, ["55 cnc", "55 cancri", "copernicus", "hd 75732"])
    check(cancri_id is not None, failures, "55 Cancri system not found")
    if cancri_id is not None:
        rows = planet_orbit_rows(core, cancri_id)
        summary["checks"]["55_cancri"] = {
            "system_id": cancri_id,
            "planet_rows": len(rows),
            "planets": [row["planet_name"] for row in rows],
        }
        check(len(rows) >= 5, failures, f"55 Cancri expected at least 5 ARM planet orbit rows, got {len(rows)}")
        check(
            all(row.get("arm_period_days") is not None for row in rows),
            failures,
            "55 Cancri ARM planet orbit rows should include source periods",
        )

    sol_id = find_system_id(core, ["sol", "sun"])
    check(sol_id is not None, failures, "Sol system not found")
    if sol_id is not None:
        sol_rows = planet_orbit_rows(core, sol_id)
        moon_count = core.execute(
            """
            select count(*)::bigint
            from arm_db.orbit_edges
            where source_catalog = 'sol_authority'
              and relation_kind = 'satellite'
            """
        ).fetchone()[0]
        summary["checks"]["sol"] = {
            "system_id": sol_id,
            "planet_orbit_rows": len(sol_rows),
            "satellite_orbit_edges": int(moon_count or 0),
        }
        check(len(sol_rows) >= 8, failures, f"Sol expected at least 8 ARM planet orbit rows, got {len(sol_rows)}")
        check(int(moon_count or 0) >= 1, failures, "Sol moon/satellite ARM orbit rows missing")
        rows_by_name = {normalize_name(str(row.get("planet_name") or "")): row for row in sol_rows}
        mercury = rows_by_name.get("mercury")
        ceres = rows_by_name.get("ceres")
        check(mercury is not None, failures, "Sol Mercury ARM orbit row missing")
        check(ceres is not None, failures, "Sol Ceres ARM orbit row missing")
        if mercury:
            check_range(mercury.get("arm_sma_au"), 0.36, 0.42, failures, "Sol Mercury ARM semi-major axis")
            check_range(mercury.get("arm_period_days"), 80.0, 95.0, failures, "Sol Mercury ARM period")
        if ceres:
            check_range(ceres.get("arm_sma_au"), 2.5, 3.1, failures, "Sol Ceres ARM semi-major axis")
            check_range(ceres.get("arm_period_days"), 1500.0, 1900.0, failures, "Sol Ceres ARM period")
        vesta = core.execute(
            """
            select os.semi_major_axis_au, os.period_days
            from arm_db.orbit_edges e
            join arm_db.orbital_solutions os on os.orbit_edge_id = e.orbit_edge_id
            where e.secondary_component_key = 'comp:minor_body:sol:vesta'
              and os.source_catalog = 'sol_authority'
            limit 1
            """
        ).fetchone()
        check(vesta is not None, failures, "Sol Vesta ARM small-body orbit row missing")
        if vesta:
            check_range(vesta[0], 2.1, 2.6, failures, "Sol Vesta ARM semi-major axis")
            check_range(vesta[1], 1200.0, 1450.0, failures, "Sol Vesta ARM period")
        if mercury and ceres and mercury.get("arm_sma_au") is not None and ceres.get("arm_sma_au") is not None:
            check(
                abs(float(mercury["arm_sma_au"]) - float(ceres["arm_sma_au"])) > 1.0,
                failures,
                "Sol Ceres ARM orbit must not duplicate Mercury",
            )
        summary["checks"]["sol"].update(
            {
                "mercury_arm_sma_au": mercury.get("arm_sma_au") if mercury else None,
                "ceres_arm_sma_au": ceres.get("arm_sma_au") if ceres else None,
                "vesta_arm_sma_au": vesta[0] if vesta else None,
            }
        )

    castor_period_count = core.execute(
        """
        select count(*)::bigint
        from arm_db.orbital_solutions os
        join arm_db.orbit_edges e on e.orbit_edge_id = os.orbit_edge_id
        where os.solution_source_catalog = 'msc'
          and e.host_component_key like '%07346+3153%'
          and os.period_days is not null
        """
    ).fetchone()[0]
    summary["checks"]["castor_regression"] = {
        "msc_period_solutions": int(castor_period_count or 0),
    }
    check(int(castor_period_count or 0) >= 5, failures, "Castor MSC orbital solutions regressed")

    core.close()
    if failures:
        summary["status"] = "fail"
        summary["failures"] = failures
        print(json.dumps(summary, indent=2))
        return 1
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
