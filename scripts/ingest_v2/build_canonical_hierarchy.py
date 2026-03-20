#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def state_dir(root: Path) -> Path:
    configured = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if configured:
        return Path(configured)
    shared_state = Path("/data/spacegate/data")
    if shared_state.exists():
        return shared_state
    return root / "data"


def resolve_build_dir(state: Path, build_id: str | None, prefer_latest_out: bool) -> tuple[str, Path]:
    out_dir = state / "out"
    if build_id:
        build_dir = out_dir / build_id
        if not build_dir.is_dir():
            raise SystemExit(f"Build directory not found: {build_dir}")
        return build_id, build_dir

    if prefer_latest_out:
        candidates = sorted(
            [path for path in out_dir.iterdir() if path.is_dir() and not path.name.endswith(".tmp")],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise SystemExit(f"No build directories found in {out_dir}")
        return candidates[0].name, candidates[0]

    served = state / "served" / "current"
    if served.exists():
        build_dir = served.resolve(strict=True)
        return build_dir.name, build_dir

    raise SystemExit("No served/current build found and no build_id was provided.")


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def maybe_set_duckdb_env(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET preserve_insertion_order=false")
    threads_env = (os.getenv("SPACEGATE_DUCKDB_THREADS") or "").strip()
    if threads_env:
        try:
            con.execute(f"SET threads={max(1, int(threads_env))}")
        except Exception:
            pass
    memory_limit_env = (os.getenv("SPACEGATE_DUCKDB_MEMORY_LIMIT") or "").strip()
    if memory_limit_env:
        try:
            con.execute(f"SET memory_limit={sql_literal(memory_limit_env)}")
        except Exception:
            pass


def count_table(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"select count(*) from {table_name}").fetchone()[0])


def build_hierarchy(*, build_id: str, build_dir: Path, reports_dir: Path) -> dict[str, object]:
    reduction_path = build_dir / "ingest_v2" / "canonical_reduction.duckdb"
    arm_path = build_dir / "arm.duckdb"
    missing = [str(p) for p in (reduction_path, arm_path) if not p.exists()]
    if missing:
        raise SystemExit(
            "Missing ingest_v2 prerequisites for hierarchy build: " + ", ".join(missing)
        )

    out_dir = build_dir / "ingest_v2"
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "canonical_hierarchy.duckdb"
    report_path = reports_dir / "canonical_hierarchy_report.json"
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    try:
        maybe_set_duckdb_env(con)
        con.execute(f"ATTACH {sql_literal(str(reduction_path))} AS red (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(str(arm_path))} AS arm (READ_ONLY)")
        con.execute(
            """
            create table build_metadata (
              build_id varchar,
              generated_at varchar,
              source_kind varchar,
              source_path varchar
            )
            """
        )
        con.execute(
            """
            insert into build_metadata values
              (?, ?, ?, ?),
              (?, ?, ?, ?),
              (?, ?, ?, ?)
            """,
            [
                build_id,
                utc_now(),
                "canonical_reduction_db",
                str(reduction_path),
                build_id,
                utc_now(),
                "arm_db",
                str(arm_path),
                build_id,
                utc_now(),
                "canonical_hierarchy_db",
                str(db_path),
            ],
        )

        con.execute(
            """
            create temp table core_to_canonical_system as
            select
              try_cast(replace(node_key, 'src:legacy_core_system:', '') as bigint) as core_system_id,
              canonical_system_key
            from red.canonical_system_sources
            where node_key like 'src:legacy_core_system:%'
            """
        )
        con.execute(
            """
            create temp table core_to_canonical_star as
            select
              try_cast(replace(node_key, 'src:legacy_core_star:', '') as bigint) as core_star_id,
              canonical_star_key
            from red.canonical_star_sources
            where node_key like 'src:legacy_core_star:%'
            """
        )
        con.execute(
            """
            create temp table core_to_canonical_planet as
            select
              try_cast(replace(node_key, 'src:legacy_core_planet:', '') as bigint) as core_planet_id,
              canonical_planet_key
            from red.canonical_planet_sources
            where node_key like 'src:legacy_core_planet:%'
            """
        )

        con.execute(
            """
            create temp table root_star_edges as
            with raw as (
              select
                sys_map.canonical_system_key,
                star_map.canonical_star_key,
                nullif(lower(trim(e.member_role)), '') as member_role,
                max(e.confidence_score) as confidence_score,
                count(*)::bigint as supporting_edge_count
              from arm.system_hierarchy_edges e
              join arm.component_entities parent_ce on parent_ce.stable_component_key = e.parent_component_key
              join arm.component_entities child_ce on child_ce.stable_component_key = e.child_component_key
              join core_to_canonical_system sys_map
                on parent_ce.core_object_type = 'system'
               and parent_ce.core_object_id = sys_map.core_system_id
              join core_to_canonical_star star_map
                on child_ce.core_object_type = 'star'
               and child_ce.core_object_id = star_map.core_star_id
              where e.edge_kind = 'contains'
              group by 1, 2, 3
            ),
            grouped as (
              select
                canonical_system_key,
                canonical_star_key,
                coalesce(
                  min(member_role) filter (where member_role is not null and member_role not in ('planet')),
                  min(member_role)
                ) as member_role,
                max(confidence_score) as confidence_score,
                sum(supporting_edge_count)::bigint as supporting_edge_count
              from raw
              group by 1, 2
            )
            select * from grouped
            """
        )

        con.execute(
            """
            create temp table root_role_map as
            select
              canonical_system_key,
              member_role,
              min(canonical_star_key) as canonical_star_key
            from root_star_edges
            where member_role is not null and member_role not in ('planet')
            group by canonical_system_key, member_role
            having count(distinct canonical_star_key) = 1
            """
        )

        con.execute(
            """
            create temp table root_planet_edges as
            with raw as (
              select
                sys_map.canonical_system_key,
                planet_map.canonical_planet_key,
                max(e.confidence_score) as confidence_score,
                count(*)::bigint as supporting_edge_count
              from arm.system_hierarchy_edges e
              join arm.component_entities parent_ce on parent_ce.stable_component_key = e.parent_component_key
              join arm.component_entities child_ce on child_ce.stable_component_key = e.child_component_key
              join core_to_canonical_system sys_map
                on parent_ce.core_object_type = 'system'
               and parent_ce.core_object_id = sys_map.core_system_id
              join core_to_canonical_planet planet_map
                on child_ce.core_object_type = 'planet'
               and child_ce.core_object_id = planet_map.core_planet_id
              where e.edge_kind = 'contains'
              group by 1, 2
            )
            select * from raw
            """
        )

        con.execute(
            """
            create temp table hosted_planets as
            select
              left_canonical_key as canonical_planet_key,
              right_canonical_key as canonical_star_key,
              max(confidence_score) as confidence_score,
              sum(supporting_edge_count)::bigint as supporting_edge_count
            from red.canonical_relations
            where relation_type = 'planet_hosts_star'
            group by 1, 2
            qualify row_number() over (
              partition by left_canonical_key
              order by max(confidence_score) desc, sum(supporting_edge_count) desc, right_canonical_key
            ) = 1
            """
        )

        con.execute(
            """
            create temp table unresolved_root_planets as
            select
              r.canonical_system_key,
              r.canonical_planet_key,
              r.confidence_score,
              r.supporting_edge_count
            from root_planet_edges r
            left join hosted_planets h on h.canonical_planet_key = r.canonical_planet_key
            where h.canonical_planet_key is null
            """
        )

        con.execute(
            """
            create temp table msc_leaf_nodes as
            with raw as (
              select
                sys_map.canonical_system_key,
                ce.stable_component_key,
                lower(nullif(ce.catalog_component_label, '')) as catalog_component_label,
                nullif(lower(trim(e.member_role)), '') as member_role,
                ce.display_name,
                max(e.confidence_score) as confidence_score,
                min(e.confidence_tier) as confidence_tier
              from arm.system_hierarchy_edges e
              join arm.component_entities parent_ce on parent_ce.stable_component_key = e.parent_component_key
              join arm.component_entities ce on ce.stable_component_key = e.child_component_key
              join red.canonical_system_groups sys_map on sys_map.wds_id = replace(parent_ce.stable_component_key, 'comp:msc_system:wds:', '')
              where parent_ce.stable_component_key like 'comp:msc_system:wds:%'
                and ce.stable_component_key like 'comp:msc:wds:%'
                and e.edge_kind = 'contains'
              group by 1,2,3,4,5
            )
            select
              'canon:leaf:msc:' || replace(stable_component_key, 'comp:msc:wds:', '') as hierarchy_node_key,
              canonical_system_key,
              stable_component_key,
              catalog_component_label,
              member_role,
              display_name,
              confidence_score,
              confidence_tier
            from raw
            """
        )

        con.execute(
            """
            create temp table msc_leaf_edges as
            select
              role_map.canonical_star_key as parent_canonical_star_key,
              leaf.hierarchy_node_key as child_hierarchy_node_key,
              leaf.canonical_system_key,
              leaf.catalog_component_label,
              leaf.member_role,
              leaf.display_name,
              leaf.confidence_score,
              leaf.confidence_tier
            from msc_leaf_nodes leaf
            join root_role_map role_map
              on role_map.canonical_system_key = leaf.canonical_system_key
             and role_map.member_role = leaf.member_role
            """
        )

        con.execute(
            """
            create table hierarchy_nodes as
            with system_nodes as (
              select
                canonical_system_key as hierarchy_node_key,
                'system'::varchar as node_kind,
                canonical_system_key as canonical_key,
                representative_name as display_name,
                wds_id,
                cast(null as varchar) as member_role,
                'canonical_system'::varchar as source_basis
              from red.canonical_system_groups
            ),
            star_nodes as (
              select
                canonical_star_key as hierarchy_node_key,
                'star'::varchar as node_kind,
                canonical_star_key as canonical_key,
                representative_name as display_name,
                cast(null as varchar) as wds_id,
                cast(null as varchar) as member_role,
                'canonical_star'::varchar as source_basis
              from red.canonical_star_groups
              where canonical_star_key in (
                select distinct canonical_star_key from root_star_edges
                union
                select distinct canonical_star_key from hosted_planets
              )
            ),
            planet_nodes as (
              select
                canonical_planet_key as hierarchy_node_key,
                'planet'::varchar as node_kind,
                canonical_planet_key as canonical_key,
                representative_name as display_name,
                cast(null as varchar) as wds_id,
                cast(null as varchar) as member_role,
                'canonical_planet'::varchar as source_basis
              from red.canonical_planet_groups
              where canonical_planet_key in (
                select distinct canonical_planet_key from hosted_planets
                union
                select distinct canonical_planet_key from unresolved_root_planets
              )
            ),
            msc_leaf as (
              select
                hierarchy_node_key,
                'inferred_star_leaf'::varchar as node_kind,
                cast(null as varchar) as canonical_key,
                display_name,
                replace(split_part(stable_component_key, ':', 4), '', '') as wds_id,
                member_role,
                'msc_inferred_leaf'::varchar as source_basis
              from msc_leaf_nodes
            )
            select * from system_nodes
            union all
            select * from star_nodes
            union all
            select * from planet_nodes
            union all
            select * from msc_leaf
            """
        )

        con.execute(
            """
            create table hierarchy_edges as
            with system_to_star as (
              select
                row_number() over (order by canonical_system_key, canonical_star_key)::bigint as hierarchy_edge_id,
                canonical_system_key as parent_node_key,
                canonical_star_key as child_node_key,
                'contains'::varchar as edge_kind,
                member_role,
                'canonical_root_star'::varchar as source_basis,
                confidence_score,
                supporting_edge_count
              from root_star_edges
            ),
            star_to_planet as (
              select
                row_number() over (order by canonical_star_key, canonical_planet_key)::bigint as hierarchy_edge_id,
                canonical_star_key as parent_node_key,
                canonical_planet_key as child_node_key,
                'contains'::varchar as edge_kind,
                'planet'::varchar as member_role,
                'canonical_host_planet'::varchar as source_basis,
                confidence_score,
                supporting_edge_count
              from hosted_planets
            ),
            system_to_planet as (
              select
                row_number() over (order by canonical_system_key, canonical_planet_key)::bigint as hierarchy_edge_id,
                canonical_system_key as parent_node_key,
                canonical_planet_key as child_node_key,
                'contains'::varchar as edge_kind,
                'planet'::varchar as member_role,
                'fallback_root_planet'::varchar as source_basis,
                confidence_score,
                supporting_edge_count
              from unresolved_root_planets
            ),
            star_to_msc_leaf as (
              select
                row_number() over (order by parent_canonical_star_key, child_hierarchy_node_key)::bigint as hierarchy_edge_id,
                parent_canonical_star_key as parent_node_key,
                child_hierarchy_node_key as child_node_key,
                'contains'::varchar as edge_kind,
                catalog_component_label as member_role,
                'msc_role_leaf'::varchar as source_basis,
                confidence_score,
                1::bigint as supporting_edge_count
              from msc_leaf_edges
            )
            select * from system_to_star
            union all
            select * from star_to_planet
            union all
            select * from system_to_planet
            union all
            select * from star_to_msc_leaf
            """
        )

        table_counts = {
            "hierarchy_nodes": count_table(con, "hierarchy_nodes"),
            "hierarchy_edges": count_table(con, "hierarchy_edges"),
        }

        samples = {
            "castor": con.execute(
                """
                with recursive walk as (
                  select
                    hn.hierarchy_node_key,
                    hn.node_kind,
                    hn.display_name,
                    cast(null as varchar) as parent_node_key,
                    0 as depth
                  from hierarchy_nodes hn
                  where hn.hierarchy_node_key = 'canon:system:wds:07346+3153'
                  union all
                  select
                    child.hierarchy_node_key,
                    child.node_kind,
                    child.display_name,
                    e.parent_node_key,
                    walk.depth + 1
                  from walk
                  join hierarchy_edges e on e.parent_node_key = walk.hierarchy_node_key
                  join hierarchy_nodes child on child.hierarchy_node_key = e.child_node_key
                )
                select depth, node_kind, display_name, hierarchy_node_key, parent_node_key
                from walk
                order by depth, hierarchy_node_key
                """
            ).fetchall(),
            "cyg16": con.execute(
                """
                with recursive walk as (
                  select
                    hn.hierarchy_node_key,
                    hn.node_kind,
                    hn.display_name,
                    cast(null as varchar) as parent_node_key,
                    0 as depth
                  from hierarchy_nodes hn
                  where hn.hierarchy_node_key = 'canon:system:wds:19418+5032'
                  union all
                  select
                    child.hierarchy_node_key,
                    child.node_kind,
                    child.display_name,
                    e.parent_node_key,
                    walk.depth + 1
                  from walk
                  join hierarchy_edges e on e.parent_node_key = walk.hierarchy_node_key
                  join hierarchy_nodes child on child.hierarchy_node_key = e.child_node_key
                )
                select depth, node_kind, display_name, hierarchy_node_key, parent_node_key
                from walk
                order by depth, hierarchy_node_key
                """
            ).fetchall(),
        }
    finally:
        con.close()

    reports_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "generated_at": utc_now(),
        "build_id": build_id,
        "canonical_hierarchy_db_path": str(db_path),
        "table_counts": table_counts,
        "samples": {
          key: [
            [str(value) if value is not None else None for value in row]
            for row in rows
          ]
          for key, rows in samples.items()
        },
        "notes": [
            "This bootstrap hierarchy prefers canonical root system -> canonical star -> canonical planet containment.",
            "MSC inferred leaf components are attached beneath top-level stars only when the root member_role mapping is unique.",
        ],
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ingest_v2 canonical hierarchy artifacts for a Spacegate build.")
    parser.add_argument("--build-id", help="Specific build id to analyze.")
    parser.add_argument(
        "--latest-out",
        action="store_true",
        help="Analyze the latest out/<build_id> directory instead of served/current.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    state = state_dir(root)
    build_id, build_dir = resolve_build_dir(state, args.build_id, args.latest_out)
    reports_dir = state / "reports" / build_id
    payload = build_hierarchy(build_id=build_id, build_dir=build_dir, reports_dir=reports_dir)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
