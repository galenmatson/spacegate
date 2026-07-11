#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_BUILD_DIR = Path("/data/spacegate/state/served/current")


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> int:
    row = con.execute(sql, params or []).fetchone()
    return int(row[0] or 0) if row else 0


def rows(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    cur = con.execute(sql, [*(params or []), limit])
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            select count(*)::bigint
            from information_schema.tables
            where table_schema = 'main'
              and table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
    )


def audit(build_dir: Path, *, sample_limit: int = 20, include_hierarchy_mismatch: bool = False) -> dict[str, Any]:
    core_path = build_dir / "core.duckdb"
    arm_path = build_dir / "arm.duckdb"
    if not core_path.exists():
        raise FileNotFoundError(f"core.duckdb not found: {core_path}")
    if not arm_path.exists():
        raise FileNotFoundError(f"arm.duckdb not found: {arm_path}")

    con = duckdb.connect(str(arm_path), read_only=True)
    try:
        required = [
            "msc_orbit_details",
            "msc_system_details",
            "msc_component_details",
            "component_entities",
            "system_hierarchy_edges",
            "orbit_edges",
            "orbital_solutions",
        ]
        missing = [table for table in required if not has_table(con, table)]
        if missing:
            raise RuntimeError(f"arm.duckdb missing required table(s): {', '.join(missing)}")
        con.execute(f"attach {sql_string(str(core_path))} as core_db (read_only)")

        msc_orbit_match_cte = """
        with matched as (
          select distinct m.source_pk
          from msc_orbit_details m
          join orbit_edges oe on oe.source_pk = m.source_pk
          union
          select distinct m.source_pk
          from msc_orbit_details m
          join orbit_edges oe
            on oe.primary_component_key = m.primary_component_key
           and oe.secondary_component_key = m.secondary_component_key
          union
          select distinct m.source_pk
          from msc_orbit_details m
          join orbit_edges oe
            on oe.primary_component_key = m.secondary_component_key
           and oe.secondary_component_key = m.primary_component_key
        )
        """
        msc_system_match_cte = """
        with matched as (
          select distinct m.source_pk
          from msc_system_details m
          join orbit_edges oe on oe.source_pk = m.source_pk
          union
          select distinct m.source_pk
          from msc_system_details m
          join orbit_edges oe
            on oe.primary_component_key = m.primary_component_key
           and oe.secondary_component_key = m.secondary_component_key
          union
          select distinct m.source_pk
          from msc_system_details m
          join orbit_edges oe
            on oe.primary_component_key = m.secondary_component_key
           and oe.secondary_component_key = m.primary_component_key
        )
        """
        msc_orbit_total = scalar(con, "select count(*)::bigint from msc_orbit_details")
        msc_orbit_unmatched = scalar(
            con,
            msc_orbit_match_cte
            + """
            select count(*)::bigint
            from msc_orbit_details m
            left join matched using (source_pk)
            where matched.source_pk is null
            """,
        )
        msc_orbit_with_solution_fields = scalar(
            con,
            """
            select count(*)::bigint
            from msc_orbit_details
            where period_days is not null
               or semi_major_axis_arcsec is not null
               or eccentricity is not null
               or inclination_deg is not null
            """,
        )
        msc_orbit_unmatched_samples = rows(
            con,
            msc_orbit_match_cte
            + """
            select
              m.wds_id,
              m.system_label,
              m.primary_label,
              m.secondary_label,
              m.period_days,
              m.semi_major_axis_arcsec,
              m.eccentricity,
              m.inclination_deg,
              m.note,
              m.primary_component_key,
              m.secondary_component_key,
              m.source_pk
            from msc_orbit_details m
            left join matched using (source_pk)
            where matched.source_pk is null
            order by
              (m.period_days is not null) desc,
              (m.semi_major_axis_arcsec is not null) desc,
              m.wds_id asc,
              m.source_pk asc
            limit ?
            """,
            limit=sample_limit,
        )

        msc_system_orbitlike_total = scalar(
            con,
            """
            select count(*)::bigint
            from msc_system_details
            where period_days is not null or separation_arcsec is not null or separation_mas is not null
            """,
        )
        msc_system_orbitlike_without_edge = scalar(
            con,
            msc_system_match_cte
            + """
            select count(*)::bigint
            from msc_system_details m
            left join matched using (source_pk)
            where (m.period_days is not null or m.separation_arcsec is not null or m.separation_mas is not null)
              and matched.source_pk is null
            """,
        )

        endpoint_sql = """
        with endpoints as (
          select
            wds_id,
            primary_label as component_label,
            primary_component_key as source_endpoint_key,
            mass_primary_msun as mass_msun,
            spectral_type_primary as spectral_type_raw,
            vmag_primary as vmag,
            source_pk
          from msc_system_details
          where primary_component_key is not null
          union all
          select
            wds_id,
            secondary_label as component_label,
            secondary_component_key as source_endpoint_key,
            mass_secondary_msun as mass_msun,
            spectral_type_secondary as spectral_type_raw,
            vmag_secondary as vmag,
            source_pk
          from msc_system_details
          where secondary_component_key is not null
        )
        select * from endpoints
        """
        source_endpoints_total = scalar(con, f"select count(*)::bigint from ({endpoint_sql}) e")
        source_endpoints_with_physical_values = scalar(
            con,
            f"""
            select count(*)::bigint
            from ({endpoint_sql}) e
            where e.mass_msun is not null
               or nullif(trim(e.spectral_type_raw), '') is not null
               or e.vmag is not null
            """,
        )
        source_endpoint_keys_missing_component = scalar(
            con,
            f"""
            select count(*)::bigint
            from ({endpoint_sql}) e
            left join component_entities ce
              on ce.stable_component_key = e.source_endpoint_key
            where ce.stable_component_key is null
            """,
        )
        source_endpoint_keys_label_bridged = scalar(
            con,
            f"""
            select count(*)::bigint
            from ({endpoint_sql}) e
            join msc_component_details mcd
              on mcd.wds_id = e.wds_id
             and lower(trim(mcd.component_label)) = lower(trim(e.component_label))
            where e.source_endpoint_key is not null
              and mcd.stable_component_key is not null
              and e.source_endpoint_key <> mcd.stable_component_key
            """,
        )
        endpoint_bridge_samples = rows(
            con,
            f"""
            select
              e.wds_id,
              e.component_label,
              e.source_endpoint_key,
              mcd.stable_component_key as canonical_component_key,
              e.mass_msun,
              e.spectral_type_raw,
              e.vmag,
              e.source_pk
            from ({endpoint_sql}) e
            join msc_component_details mcd
              on mcd.wds_id = e.wds_id
             and lower(trim(mcd.component_label)) = lower(trim(e.component_label))
            where e.source_endpoint_key <> mcd.stable_component_key
            order by
              (e.mass_msun is not null) desc,
              (nullif(trim(e.spectral_type_raw), '') is not null) desc,
              e.wds_id asc,
              e.component_label asc
            limit ?
            """,
            limit=sample_limit,
        )

        system_count_mismatches = None
        system_count_mismatch_samples: list[dict[str, Any]] = []
        if include_hierarchy_mismatch:
            system_count_mismatches = scalar(
                con,
                """
                with recursive tree(system_id, node_key) as (
                  select
                    core_object_id as system_id,
                    stable_component_key as node_key
                  from component_entities
                  where component_type = 'system'
                    and core_object_type = 'system'
                  union all
                  select
                    tree.system_id,
                    e.child_component_key as node_key
                  from tree
                  join system_hierarchy_edges e
                    on e.parent_component_key = tree.node_key
                ), hierarchy_star_counts as (
                  select
                    tree.system_id,
                    count(*)::bigint as hierarchy_star_count
                  from tree
                  join component_entities child
                    on child.stable_component_key = tree.node_key
                  where child.component_type = 'star'
                    and child.core_object_type = 'star'
                    and child.core_object_id is not null
                  group by tree.system_id
                )
                select count(*)::bigint
                from hierarchy_star_counts h
                join core_db.systems s on s.system_id = h.system_id
                where coalesce(s.star_count, 0) <> h.hierarchy_star_count
                """,
            )
            system_count_mismatch_samples = rows(
                con,
                """
                with recursive tree(system_id, node_key) as (
                  select
                    core_object_id as system_id,
                    stable_component_key as node_key
                  from component_entities
                  where component_type = 'system'
                    and core_object_type = 'system'
                  union all
                  select
                    tree.system_id,
                    e.child_component_key as node_key
                  from tree
                  join system_hierarchy_edges e
                    on e.parent_component_key = tree.node_key
                ), hierarchy_star_counts as (
                  select
                    tree.system_id,
                    count(*)::bigint as hierarchy_star_count
                  from tree
                  join component_entities child
                    on child.stable_component_key = tree.node_key
                  where child.component_type = 'star'
                    and child.core_object_type = 'star'
                    and child.core_object_id is not null
                  group by tree.system_id
                )
                select
                  s.system_id,
                  s.system_name,
                  s.wds_id,
                  s.star_count as core_star_count,
                  h.hierarchy_star_count
                from hierarchy_star_counts h
                join core_db.systems s on s.system_id = h.system_id
                where coalesce(s.star_count, 0) <> h.hierarchy_star_count
                order by abs(coalesce(s.star_count, 0) - h.hierarchy_star_count) desc, s.system_id asc
                limit ?
                """,
                limit=sample_limit,
            )

        return {
            "build_dir": str(build_dir),
            "core_db": str(core_path),
            "arm_db": str(arm_path),
            "summary": {
                "msc_orbit_detail_rows": msc_orbit_total,
                "msc_orbit_detail_rows_with_solution_fields": msc_orbit_with_solution_fields,
                "msc_orbit_detail_rows_without_arm_orbit_edge": msc_orbit_unmatched,
                "msc_system_detail_orbitlike_rows": msc_system_orbitlike_total,
                "msc_system_detail_orbitlike_rows_without_arm_orbit_edge": msc_system_orbitlike_without_edge,
                "msc_system_detail_endpoint_rows": source_endpoints_total,
                "msc_system_detail_endpoint_rows_with_physical_values": source_endpoints_with_physical_values,
                "msc_source_endpoint_keys_missing_component_entity": source_endpoint_keys_missing_component,
                "msc_source_endpoint_keys_with_label_bridge_to_canonical_component": source_endpoint_keys_label_bridged,
                "core_system_star_count_vs_hierarchy_star_count_mismatches": system_count_mismatches,
            },
            "samples": {
                "msc_orbit_detail_rows_without_arm_orbit_edge": msc_orbit_unmatched_samples,
                "msc_source_endpoint_key_bridges": endpoint_bridge_samples,
                "core_system_star_count_vs_hierarchy_star_count_mismatches": system_count_mismatch_samples,
            },
            "interpretation": [
                "MSC detail rows are preserved ARM source evidence.",
                "Rows without ARM orbit edges are not available to the simulation as normalized orbital solutions.",
                "MSC source endpoint keys that bridge by WDS label to canonical components indicate evidence we can use, but need deterministic endpoint reconciliation.",
                "Core star_count versus hierarchy mismatch samples are opt-in because they require a recursive graph scan.",
            ],
        }
    finally:
        con.close()


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit source evidence preserved in ARM but not used by normalized graph/simulation contracts.")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIR, help="Build directory containing core.duckdb and arm.duckdb.")
    parser.add_argument("--sample-limit", type=int, default=20, help="Maximum rows to include per sample section.")
    parser.add_argument("--include-hierarchy-mismatch", action="store_true", help="Also run the heavier recursive hierarchy-vs-core star count scan.")
    parser.add_argument("--json-output", type=Path, default=None, help="Optional path for JSON audit output.")
    args = parser.parse_args()

    report = audit(
        args.build_dir,
        sample_limit=max(1, args.sample_limit),
        include_hierarchy_mismatch=args.include_hierarchy_mismatch,
    )
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
