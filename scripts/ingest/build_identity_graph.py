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


def build_identity_graph(*, build_id: str, build_dir: Path, reports_dir: Path) -> dict[str, object]:
    norm_path = build_dir / "ingest" / "normalized_sources.duckdb"
    if not norm_path.exists():
        raise SystemExit(
            f"Missing normalized ingest artifact for build {build_id}: {norm_path}. "
            "Run scripts/ingest/normalize_sources.py first."
        )

    out_dir = build_dir / "ingest"
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "identity_graph.duckdb"
    report_path = reports_dir / "identity_graph_report.json"
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    try:
        maybe_set_duckdb_env(con)
        con.execute(f"ATTACH {sql_literal(str(norm_path))} AS norm (READ_ONLY)")
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
              (?, ?, ?, ?)
            """,
            [
                build_id,
                utc_now(),
                "normalized_sources_db",
                str(norm_path),
                build_id,
                utc_now(),
                "identity_graph_db",
                str(db_path),
            ],
        )

        con.execute("create table graph_nodes as select * from norm.source_nodes")

        con.execute(
            """
            create temp table graph_edges_stage as
            with
            exact_gaia_legacy as (
              select
                least(g.node_key, l.node_key) as left_node_key,
                greatest(g.node_key, l.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_gaia_id'::varchar as match_method,
                1.00::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["gaia_backbone","legacy_core_crosswalk"]'::varchar as source_catalogs_json,
                'gaia_id=' || g.gaia_id::varchar as evidence_summary,
                json_object('gaia_id', g.gaia_id) as evidence_json,
                false as ambiguous
              from norm.gaia_star_sources g
              join norm.legacy_crosswalk_star_sources l
                on g.gaia_id is not null
               and g.gaia_id = l.gaia_id
            ),
            exact_gaia_nasa as (
              select
                least(g.node_key, n.node_key) as left_node_key,
                greatest(g.node_key, n.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_gaia_id'::varchar as match_method,
                1.00::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["gaia_backbone","nasa_exoplanet_archive"]'::varchar as source_catalogs_json,
                'gaia_id=' || g.gaia_id::varchar as evidence_summary,
                json_object('gaia_id', g.gaia_id) as evidence_json,
                false as ambiguous
              from norm.gaia_star_sources g
              join norm.nasa_host_sources n
                on g.gaia_id is not null
               and g.gaia_id = n.gaia_id
            ),
            exact_gaia_sbx as (
              select
                least(g.node_key, s.node_key) as left_node_key,
                greatest(g.node_key, s.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_gaia_id'::varchar as match_method,
                1.00::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["gaia_backbone","sbx"]'::varchar as source_catalogs_json,
                'gaia_id=' || g.gaia_id::varchar as evidence_summary,
                json_object('gaia_id', g.gaia_id) as evidence_json,
                false as ambiguous
              from norm.gaia_star_sources g
              join norm.sbx_star_sources s
                on g.gaia_id is not null
               and g.gaia_id = s.gaia_id
            ),
            legacy_hip_unique as (
              select hip_id, min(node_key) as node_key
              from norm.legacy_crosswalk_star_sources
              where hip_id is not null and hip_id > 0
              group by hip_id
              having count(*) = 1
            ),
            legacy_hd_unique as (
              select hd_id, min(node_key) as node_key
              from norm.legacy_crosswalk_star_sources
              where hd_id is not null and hd_id > 0
              group by hd_id
              having count(*) = 1
            ),
            msc_hip_unique as (
              select hip_id, min(node_key) as node_key
              from norm.msc_component_sources
              where hip_id is not null and hip_id > 0
              group by hip_id
              having count(*) = 1
            ),
            msc_hd_unique as (
              select hd_id, min(node_key) as node_key
              from norm.msc_component_sources
              where hd_id is not null and hd_id > 0
              group by hd_id
              having count(*) = 1
            ),
            nasa_hip_unique as (
              select hip_id, min(node_key) as node_key
              from norm.nasa_host_sources
              where hip_id is not null and hip_id > 0
              group by hip_id
              having count(*) = 1
            ),
            nasa_hd_unique as (
              select hd_id, min(node_key) as node_key
              from norm.nasa_host_sources
              where hd_id is not null and hd_id > 0
              group by hd_id
              having count(*) = 1
            ),
            orb6_hip_unique as (
              select hip_id, min(node_key) as node_key
              from norm.orb6_orbit_sources
              where hip_id is not null and hip_id > 0
              group by hip_id
              having count(*) = 1
            ),
            orb6_hd_unique as (
              select hd_id, min(node_key) as node_key
              from norm.orb6_orbit_sources
              where hd_id is not null and hd_id > 0
              group by hd_id
              having count(*) = 1
            ),
            sbx_hip_unique as (
              select hip_id, min(node_key) as node_key
              from norm.sbx_star_sources
              where hip_id is not null and hip_id > 0
              group by hip_id
              having count(*) = 1
            ),
            sbx_hd_unique as (
              select hd_id, min(node_key) as node_key
              from norm.sbx_star_sources
              where hd_id is not null and hd_id > 0
              group by hd_id
              having count(*) = 1
            ),
            exact_hip_legacy_msc as (
              select
                least(l.node_key, m.node_key) as left_node_key,
                greatest(l.node_key, m.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_hip_id_unique'::varchar as match_method,
                0.98::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk","msc"]'::varchar as source_catalogs_json,
                'hip_id=' || l.hip_id::varchar as evidence_summary,
                json_object('hip_id', l.hip_id) as evidence_json,
                false as ambiguous
              from legacy_hip_unique l
              join msc_hip_unique m using (hip_id)
            ),
            exact_hd_legacy_msc as (
              select
                least(l.node_key, m.node_key) as left_node_key,
                greatest(l.node_key, m.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_hd_id_unique'::varchar as match_method,
                0.96::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk","msc"]'::varchar as source_catalogs_json,
                'hd_id=' || l.hd_id::varchar as evidence_summary,
                json_object('hd_id', l.hd_id) as evidence_json,
                false as ambiguous
              from legacy_hd_unique l
              join msc_hd_unique m using (hd_id)
            ),
            exact_hip_legacy_nasa as (
              select
                least(l.node_key, n.node_key) as left_node_key,
                greatest(l.node_key, n.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_hip_id_unique'::varchar as match_method,
                0.98::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk","nasa_exoplanet_archive"]'::varchar as source_catalogs_json,
                'hip_id=' || l.hip_id::varchar as evidence_summary,
                json_object('hip_id', l.hip_id) as evidence_json,
                false as ambiguous
              from legacy_hip_unique l
              join nasa_hip_unique n using (hip_id)
            ),
            exact_hd_legacy_nasa as (
              select
                least(l.node_key, n.node_key) as left_node_key,
                greatest(l.node_key, n.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_hd_id_unique'::varchar as match_method,
                0.96::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk","nasa_exoplanet_archive"]'::varchar as source_catalogs_json,
                'hd_id=' || l.hd_id::varchar as evidence_summary,
                json_object('hd_id', l.hd_id) as evidence_json,
                false as ambiguous
              from legacy_hd_unique l
              join nasa_hd_unique n using (hd_id)
            ),
            exact_hip_legacy_orb6 as (
              select
                least(l.node_key, o.node_key) as left_node_key,
                greatest(l.node_key, o.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_hip_id_unique'::varchar as match_method,
                0.94::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk","orb6"]'::varchar as source_catalogs_json,
                'hip_id=' || l.hip_id::varchar as evidence_summary,
                json_object('hip_id', l.hip_id) as evidence_json,
                false as ambiguous
              from legacy_hip_unique l
              join orb6_hip_unique o using (hip_id)
            ),
            exact_hd_legacy_orb6 as (
              select
                least(l.node_key, o.node_key) as left_node_key,
                greatest(l.node_key, o.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_hd_id_unique'::varchar as match_method,
                0.92::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk","orb6"]'::varchar as source_catalogs_json,
                'hd_id=' || l.hd_id::varchar as evidence_summary,
                json_object('hd_id', l.hd_id) as evidence_json,
                false as ambiguous
              from legacy_hd_unique l
              join orb6_hd_unique o using (hd_id)
            ),
            exact_hip_legacy_sbx as (
              select
                least(l.node_key, s.node_key) as left_node_key,
                greatest(l.node_key, s.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_hip_id_unique'::varchar as match_method,
                0.96::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk","sbx"]'::varchar as source_catalogs_json,
                'hip_id=' || l.hip_id::varchar as evidence_summary,
                json_object('hip_id', l.hip_id) as evidence_json,
                false as ambiguous
              from legacy_hip_unique l
              join sbx_hip_unique s using (hip_id)
            ),
            exact_hd_legacy_sbx as (
              select
                least(l.node_key, s.node_key) as left_node_key,
                greatest(l.node_key, s.node_key) as right_node_key,
                'same_star'::varchar as relation_type,
                'exact_hd_id_unique'::varchar as match_method,
                0.94::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk","sbx"]'::varchar as source_catalogs_json,
                'hd_id=' || l.hd_id::varchar as evidence_summary,
                json_object('hd_id', l.hd_id) as evidence_json,
                false as ambiguous
              from legacy_hd_unique l
              join sbx_hd_unique s using (hd_id)
            ),
            exact_wds_legacy_system as (
              select
                least(w.node_key, l.node_key) as left_node_key,
                greatest(w.node_key, l.node_key) as right_node_key,
                'same_system'::varchar as relation_type,
                'exact_wds_id'::varchar as match_method,
                0.99::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["wds","legacy_core_crosswalk"]'::varchar as source_catalogs_json,
                'wds_id=' || w.wds_id as evidence_summary,
                json_object('wds_id', w.wds_id) as evidence_json,
                false as ambiguous
              from norm.wds_system_sources w
              join norm.legacy_crosswalk_system_sources l
                on w.wds_id is not null
               and w.wds_id = l.wds_id
            ),
            exact_wds_msc_system as (
              select
                least(w.node_key, m.node_key) as left_node_key,
                greatest(w.node_key, m.node_key) as right_node_key,
                'same_system'::varchar as relation_type,
                'exact_wds_id'::varchar as match_method,
                1.00::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["wds","msc"]'::varchar as source_catalogs_json,
                'wds_id=' || w.wds_id as evidence_summary,
                json_object('wds_id', w.wds_id) as evidence_json,
                false as ambiguous
              from norm.wds_system_sources w
              join norm.msc_system_sources m
                on w.wds_id is not null
               and w.wds_id = m.wds_id
            ),
            exact_wds_orb6_system as (
              select
                least(w.node_key, o.system_node_key) as left_node_key,
                greatest(w.node_key, o.system_node_key) as right_node_key,
                'same_system'::varchar as relation_type,
                'exact_wds_id'::varchar as match_method,
                0.97::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["wds","orb6"]'::varchar as source_catalogs_json,
                'wds_id=' || w.wds_id as evidence_summary,
                json_object('wds_id', w.wds_id) as evidence_json,
                false as ambiguous
              from norm.wds_system_sources w
              join norm.orb6_orbit_sources o
                on w.wds_id is not null
               and w.wds_id = o.wds_id
               and o.system_node_key is not null
              group by 1,2,3,4,5,6,7,8,9,10
            ),
            msc_component_of_system as (
              select
                node_key as left_node_key,
                system_node_key as right_node_key,
                'component_of'::varchar as relation_type,
                'exact_wds_component_membership'::varchar as match_method,
                1.00::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["msc"]'::varchar as source_catalogs_json,
                'msc component belongs to msc system'::varchar as evidence_summary,
                json_object('wds_id', wds_id, 'component_label', component_label) as evidence_json,
                false as ambiguous
              from norm.msc_component_sources
            ),
            nasa_planet_hosts_nasa_star as (
              select
                node_key as left_node_key,
                host_node_key as right_node_key,
                'planet_hosts_star'::varchar as relation_type,
                'exact_nasa_hostid'::varchar as match_method,
                1.00::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["nasa_exoplanet_archive"]'::varchar as source_catalogs_json,
                'nasa hostid=' || replace(host_node_key, 'src:nasa_host:', '') as evidence_summary,
                json_object('host_node_key', host_node_key) as evidence_json,
                false as ambiguous
              from norm.nasa_planet_sources
              where host_node_key is not null
            ),
            exact_nasa_planet_legacy as (
              select
                least(n.node_key, l.node_key) as left_node_key,
                greatest(n.node_key, l.node_key) as right_node_key,
                'same_planet'::varchar as relation_type,
                'exact_planet_name_norm_nasa_lineage'::varchar as match_method,
                0.97::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["nasa_exoplanet_archive","legacy_core_crosswalk"]'::varchar as source_catalogs_json,
                'planet_name_norm=' || n.planet_name_norm as evidence_summary,
                json_object('planet_name_norm', n.planet_name_norm) as evidence_json,
                false as ambiguous
              from norm.nasa_planet_sources n
              join norm.legacy_crosswalk_planet_sources l
                on l.upstream_source_catalog = 'nasa_exoplanet_archive'
               and n.planet_name_norm is not null
               and n.planet_name_norm = l.planet_name_norm
            ),
            legacy_planet_hosts_legacy_star as (
              select
                node_key as left_node_key,
                host_node_key as right_node_key,
                'planet_hosts_star'::varchar as relation_type,
                'legacy_core_star_id'::varchar as match_method,
                0.99::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["legacy_core_crosswalk"]'::varchar as source_catalogs_json,
                'legacy star host link'::varchar as evidence_summary,
                json_object('host_node_key', host_node_key) as evidence_json,
                false as ambiguous
              from norm.legacy_crosswalk_planet_sources
              where host_node_key is not null
            ),
            orb6_orbit_of_wds_system as (
              select
                node_key as left_node_key,
                system_node_key as right_node_key,
                'orbit_of_system'::varchar as relation_type,
                'exact_wds_id'::varchar as match_method,
                0.98::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["orb6","wds"]'::varchar as source_catalogs_json,
                'wds_id=' || wds_id as evidence_summary,
                json_object('wds_id', wds_id, 'reference_code', reference_code) as evidence_json,
                false as ambiguous
              from norm.orb6_orbit_sources
              where system_node_key is not null
            ),
            sol_parent_edges as (
              select
                node_key as left_node_key,
                case
                  when lower(parent_object_name) = 'sun' then 'src:sol:1'
                  else 'src:sol_name:' || lower(parent_object_name)
                end as right_node_key,
                'component_of'::varchar as relation_type,
                'sol_parent_name'::varchar as match_method,
                0.95::double as confidence_score,
                'high'::varchar as confidence_tier,
                '["sol_authority"]'::varchar as source_catalogs_json,
                'parent_object_name=' || parent_object_name as evidence_summary,
                json_object('parent_object_name', parent_object_name) as evidence_json,
                false as ambiguous
              from norm.sol_object_sources
              where parent_object_name is not null
            )
            select * from exact_gaia_legacy
            union all select * from exact_gaia_nasa
            union all select * from exact_gaia_sbx
            union all select * from exact_hip_legacy_msc
            union all select * from exact_hd_legacy_msc
            union all select * from exact_hip_legacy_nasa
            union all select * from exact_hd_legacy_nasa
            union all select * from exact_hip_legacy_orb6
            union all select * from exact_hd_legacy_orb6
            union all select * from exact_hip_legacy_sbx
            union all select * from exact_hd_legacy_sbx
            union all select * from exact_wds_legacy_system
            union all select * from exact_wds_msc_system
            union all select * from exact_wds_orb6_system
            union all select * from msc_component_of_system
            union all select * from nasa_planet_hosts_nasa_star
            union all select * from exact_nasa_planet_legacy
            union all select * from legacy_planet_hosts_legacy_star
            union all select * from orb6_orbit_of_wds_system
            union all select * from sol_parent_edges
            """
        )

        con.execute(
            """
            create table graph_edges as
            with deduped as (
              select
                *,
                row_number() over (
                  partition by left_node_key, right_node_key, relation_type, match_method
                  order by confidence_score desc, evidence_summary asc
                ) as row_num
              from graph_edges_stage
            )
            select
              row_number() over (
                order by relation_type, match_method, left_node_key, right_node_key
              )::bigint as edge_id,
              left_node_key,
              right_node_key,
              relation_type,
              match_method,
              confidence_score,
              confidence_tier,
              source_catalogs_json,
              evidence_summary,
              evidence_json,
              ambiguous
            from deduped
            where row_num = 1
            """
        )

        table_counts = {
            "graph_nodes": count_table(con, "graph_nodes"),
            "graph_edges": count_table(con, "graph_edges"),
        }
        relation_counts = con.execute(
            """
            select relation_type, match_method, count(*)::bigint as edge_count
            from graph_edges
            group by relation_type, match_method
            order by relation_type, match_method
            """
        ).fetchall()
    finally:
        con.close()

    reports_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "generated_at": utc_now(),
        "build_id": build_id,
        "identity_graph_db_path": str(db_path),
        "table_counts": table_counts,
        "relation_counts": [
            {"relation_type": rel, "match_method": method, "edge_count": int(count)}
            for rel, method, count in relation_counts
        ],
        "notes": [
            "legacy_core_crosswalk-backed edges are transitional bootstrap evidence for ingest",
            "canonical reduction should eventually replace those edges with source-native crosswalk and resolver outputs",
        ],
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ingest identity graph artifacts for a Spacegate build.")
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
    payload = build_identity_graph(build_id=build_id, build_dir=build_dir, reports_dir=reports_dir)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
