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


def build_reduction(*, build_id: str, build_dir: Path, reports_dir: Path) -> dict[str, object]:
    norm_path = build_dir / "ingest" / "normalized_sources.duckdb"
    graph_path = build_dir / "ingest" / "identity_graph.duckdb"
    missing = [str(p) for p in (norm_path, graph_path) if not p.exists()]
    if missing:
        raise SystemExit(
            "Missing ingest prerequisites for canonical reduction: " + ", ".join(missing)
        )

    out_dir = build_dir / "ingest"
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "canonical_reduction.duckdb"
    report_path = reports_dir / "canonical_reduction_report.json"
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    try:
        maybe_set_duckdb_env(con)
        con.execute(f"ATTACH {sql_literal(str(norm_path))} AS norm (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(str(graph_path))} AS graph (READ_ONLY)")
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
                "normalized_sources_db",
                str(norm_path),
                build_id,
                utc_now(),
                "identity_graph_db",
                str(graph_path),
                build_id,
                utc_now(),
                "canonical_reduction_db",
                str(db_path),
            ],
        )

        con.execute(
            """
            create temp table legacy_resolved_star_identities as
            with hip_gaia as (
              select hip_id, min(gaia_id) as resolved_gaia_id
              from norm.legacy_crosswalk_star_sources
              where hip_id is not null and hip_id > 0 and gaia_id is not null
              group by hip_id
              having count(distinct gaia_id) = 1
            ),
            hd_gaia as (
              select hd_id, min(gaia_id) as resolved_gaia_id
              from norm.legacy_crosswalk_star_sources
              where hd_id is not null and hd_id > 0 and gaia_id is not null
              group by hd_id
              having count(distinct gaia_id) = 1
            )
            select
              l.node_key as legacy_node_key,
              l.display_name,
              l.gaia_id as direct_gaia_id,
              coalesce(l.gaia_id, hg.resolved_gaia_id, dg.resolved_gaia_id) as resolved_gaia_id,
              l.hip_id,
              l.hd_id,
              l.wds_id
            from norm.legacy_crosswalk_star_sources l
            left join hip_gaia hg on hg.hip_id = l.hip_id
            left join hd_gaia dg on dg.hd_id = l.hd_id
            """
        )

        con.execute(
            """
            create temp table star_to_legacy_match as
            with graph_matches as (
              select
                case
                  when e.left_node_key like 'src:legacy_core_star:%' then e.right_node_key
                  else e.left_node_key
                end as node_key,
                case
                  when e.left_node_key like 'src:legacy_core_star:%' then e.left_node_key
                  else e.right_node_key
                end as legacy_node_key,
                e.match_method,
                e.confidence_score
              from graph.graph_edges e
              where e.relation_type = 'same_star'
                and (
                  e.left_node_key like 'src:legacy_core_star:%'
                  or e.right_node_key like 'src:legacy_core_star:%'
                )
                and not (
                  e.left_node_key like 'src:legacy_core_star:%'
                  and e.right_node_key like 'src:legacy_core_star:%'
                )
            ),
            direct_identifier_matches as (
              select
                m.node_key,
                l.legacy_node_key,
                'direct_legacy_hip'::varchar as match_method,
                0.96::double as confidence_score
              from norm.msc_component_sources m
              join legacy_resolved_star_identities l
                on m.hip_id is not null
               and m.hip_id > 0
               and m.hip_id = l.hip_id
              union all
              select
                m.node_key,
                l.legacy_node_key,
                'direct_legacy_hd'::varchar as match_method,
                0.94::double as confidence_score
              from norm.msc_component_sources m
              join legacy_resolved_star_identities l
                on m.hd_id is not null
               and m.hd_id > 0
               and m.hd_id = l.hd_id
              union all
              select
                n.node_key,
                l.legacy_node_key,
                'direct_legacy_hip'::varchar as match_method,
                0.96::double as confidence_score
              from norm.nasa_host_sources n
              join legacy_resolved_star_identities l
                on n.hip_id is not null
               and n.hip_id > 0
               and n.hip_id = l.hip_id
              union all
              select
                n.node_key,
                l.legacy_node_key,
                'direct_legacy_hd'::varchar as match_method,
                0.94::double as confidence_score
              from norm.nasa_host_sources n
              join legacy_resolved_star_identities l
                on n.hd_id is not null
               and n.hd_id > 0
               and n.hd_id = l.hd_id
              union all
              select
                s.node_key,
                l.legacy_node_key,
                'direct_legacy_hip'::varchar as match_method,
                0.95::double as confidence_score
              from norm.sbx_star_sources s
              join legacy_resolved_star_identities l
                on s.hip_id is not null
               and s.hip_id > 0
               and s.hip_id = l.hip_id
              union all
              select
                s.node_key,
                l.legacy_node_key,
                'direct_legacy_hd'::varchar as match_method,
                0.93::double as confidence_score
              from norm.sbx_star_sources s
              join legacy_resolved_star_identities l
                on s.hd_id is not null
               and s.hd_id > 0
               and s.hd_id = l.hd_id
            )
            select * from graph_matches
            union all
            select * from direct_identifier_matches
            """
        )

        con.execute(
            """
            create temp table star_legacy_match_summary as
            select
              m.node_key,
              count(distinct m.legacy_node_key)::bigint as matched_legacy_node_count,
              count(distinct lr.resolved_gaia_id) filter (where lr.resolved_gaia_id is not null) as matched_legacy_resolved_gaia_count,
              min(lr.resolved_gaia_id) filter (where lr.resolved_gaia_id is not null) as matched_legacy_gaia_id,
              count(distinct lr.hip_id) filter (where lr.hip_id is not null) as matched_legacy_hip_count,
              min(lr.hip_id) filter (where lr.hip_id is not null) as matched_legacy_hip_id,
              count(distinct lr.hd_id) filter (where lr.hd_id is not null) as matched_legacy_hd_count,
              min(lr.hd_id) filter (where lr.hd_id is not null) as matched_legacy_hd_id
            from star_to_legacy_match m
            join legacy_resolved_star_identities lr on lr.legacy_node_key = m.legacy_node_key
            group by m.node_key
            """
        )

        con.execute(
            """
            create table canonical_star_sources as
            with star_source_base as (
              select
                node_key,
                source_catalog,
                display_name,
                gaia_id,
                cast(null as bigint) as hip_id,
                cast(null as bigint) as hd_id
              from norm.gaia_star_sources
              union all
              select
                node_key,
                source_catalog,
                display_name,
                gaia_id,
                nullif(hip_id, 0) as hip_id,
                nullif(hd_id, 0) as hd_id
              from norm.nasa_host_sources
              union all
              select
                node_key,
                source_catalog,
                display_name,
                cast(null as bigint) as gaia_id,
                nullif(hip_id, 0) as hip_id,
                nullif(hd_id, 0) as hd_id
              from norm.msc_component_sources
              union all
              select
                node_key,
                source_catalog,
                display_name,
                gaia_id,
                nullif(hip_id, 0) as hip_id,
                nullif(hd_id, 0) as hd_id
              from norm.sbx_star_sources
              union all
              select
                l.legacy_node_key as node_key,
                'legacy_core_crosswalk'::varchar as source_catalog,
                l.display_name,
                l.direct_gaia_id as gaia_id,
                nullif(l.hip_id, 0) as hip_id,
                nullif(l.hd_id, 0) as hd_id
              from legacy_resolved_star_identities l
            ),
            resolved as (
              select
                b.*,
                l.resolved_gaia_id as legacy_self_resolved_gaia_id,
                m.matched_legacy_node_count,
                m.matched_legacy_resolved_gaia_count,
                m.matched_legacy_gaia_id,
                m.matched_legacy_hip_id,
                m.matched_legacy_hd_id,
                coalesce(
                  case when b.source_catalog = 'legacy_core_crosswalk' then l.resolved_gaia_id end,
                  b.gaia_id,
                  case when coalesce(m.matched_legacy_resolved_gaia_count, 0) = 1 then m.matched_legacy_gaia_id end
                ) as resolved_gaia_id
              from star_source_base b
              left join legacy_resolved_star_identities l on l.legacy_node_key = b.node_key
              left join star_legacy_match_summary m on m.node_key = b.node_key
            )
            select
              row_number() over (order by node_key)::bigint as canonical_source_id,
              node_key,
              source_catalog,
              display_name,
              resolved_gaia_id,
              hip_id,
              hd_id,
              matched_legacy_node_count,
              matched_legacy_resolved_gaia_count,
              case
                when resolved_gaia_id is not null then 'canon:star:gaia:' || resolved_gaia_id::varchar
                when hip_id is not null then 'canon:star:hip:' || hip_id::varchar
                when hd_id is not null then 'canon:star:hd:' || hd_id::varchar
                else 'canon:star:node:' || node_key
              end as canonical_star_key,
              case
                when source_catalog = 'legacy_core_crosswalk' and gaia_id is null and legacy_self_resolved_gaia_id is not null then 'legacy_identifier_to_gaia'
                when gaia_id is not null then 'direct_gaia'
                when coalesce(matched_legacy_resolved_gaia_count, 0) = 1 then 'via_legacy_gaia'
                when hip_id is not null then 'direct_hip'
                when hd_id is not null then 'direct_hd'
                else 'fallback_node'
              end as resolution_method,
              case
                when resolved_gaia_id is not null then 1.00
                when hip_id is not null then 0.96
                when hd_id is not null then 0.93
                else 0.50
              end as resolution_confidence
            from resolved
            """
        )

        con.execute(
            """
            create table canonical_star_groups as
            select
              canonical_star_key,
              min(display_name) as representative_name,
              min(resolved_gaia_id) as resolved_gaia_id,
              min(hip_id) as hip_id,
              min(hd_id) as hd_id,
              count(*)::bigint as source_node_count,
              (count(*) filter (where source_catalog = 'legacy_core_crosswalk'))::bigint as legacy_source_count,
              count(distinct source_catalog)::bigint as source_catalog_count
            from canonical_star_sources
            group by canonical_star_key
            """
        )

        con.execute(
            """
            create table star_quarantine_conflicts as
            with legacy_duplicate_bundle as (
              select
                'legacy_duplicate_bundle'::varchar as issue_type,
                canonical_star_key as subject_key,
                'high'::varchar as severity,
                (count(*) filter (where source_catalog = 'legacy_core_crosswalk'))::bigint as affected_node_count,
                json_object(
                  'legacy_source_count', count(*) filter (where source_catalog = 'legacy_core_crosswalk'),
                  'source_catalogs', string_agg(distinct source_catalog, ',')
                ) as details_json
              from canonical_star_sources
              group by canonical_star_key
              having count(*) filter (where source_catalog = 'legacy_core_crosswalk') > 1
            ),
            hip_multi_canonical as (
              select
                'hip_maps_to_multiple_canonical_stars'::varchar as issue_type,
                'hip:' || hip_id::varchar as subject_key,
                'high'::varchar as severity,
                count(distinct canonical_star_key)::bigint as affected_node_count,
                json_object(
                  'hip_id', hip_id,
                  'canonical_key_count', count(distinct canonical_star_key)
                ) as details_json
              from canonical_star_sources
              where hip_id is not null and hip_id > 0
              group by hip_id
              having count(distinct canonical_star_key) > 1
            ),
            hd_multi_canonical as (
              select
                'hd_maps_to_multiple_canonical_stars'::varchar as issue_type,
                'hd:' || hd_id::varchar as subject_key,
                'high'::varchar as severity,
                count(distinct canonical_star_key)::bigint as affected_node_count,
                json_object(
                  'hd_id', hd_id,
                  'canonical_key_count', count(distinct canonical_star_key)
                ) as details_json
              from canonical_star_sources
              where hd_id is not null and hd_id > 0
              group by hd_id
              having count(distinct canonical_star_key) > 1
            ),
            legacy_multi_match as (
              select
                'source_matches_multiple_legacy_gaia_groups'::varchar as issue_type,
                node_key as subject_key,
                'medium'::varchar as severity,
                matched_legacy_resolved_gaia_count::bigint as affected_node_count,
                json_object(
                  'matched_legacy_node_count', matched_legacy_node_count,
                  'matched_legacy_resolved_gaia_count', matched_legacy_resolved_gaia_count
                ) as details_json
              from canonical_star_sources
              where coalesce(matched_legacy_resolved_gaia_count, 0) > 1
            )
            select row_number() over (order by severity desc, issue_type, subject_key)::bigint as quarantine_id, *
            from (
              select * from legacy_duplicate_bundle
              union all
              select * from hip_multi_canonical
              union all
              select * from hd_multi_canonical
              union all
              select * from legacy_multi_match
            )
            """
        )

        con.execute(
            """
            create table canonical_system_sources as
            with system_source_base as (
              select node_key, source_catalog, display_name, wds_id, source_pk
              from norm.msc_system_sources
              union all
              select node_key, source_catalog, display_name, wds_id, source_pk
              from norm.wds_system_sources
              union all
              select node_key, source_catalog, display_name, wds_id, source_pk
              from norm.legacy_crosswalk_system_sources
            )
            select
              row_number() over (order by node_key)::bigint as canonical_source_id,
              node_key,
              source_catalog,
              display_name,
              wds_id,
              case
                when wds_id is not null then 'canon:system:wds:' || wds_id
                when source_catalog = 'legacy_core_crosswalk' then 'canon:system:legacy:' || source_pk::varchar
                else 'canon:system:node:' || node_key
              end as canonical_system_key,
              case
                when wds_id is not null then 'exact_wds_id'
                when source_catalog = 'legacy_core_crosswalk' then 'legacy_system_id'
                else 'fallback_node'
              end as resolution_method,
              case
                when wds_id is not null then 1.00
                when source_catalog = 'legacy_core_crosswalk' then 0.90
                else 0.50
              end as resolution_confidence
            from system_source_base
            """
        )

        con.execute(
            """
            create table canonical_system_groups as
            select
              canonical_system_key,
              min(display_name) as representative_name,
              min(wds_id) as wds_id,
              count(*)::bigint as source_node_count,
              (count(*) filter (where source_catalog = 'legacy_core_crosswalk'))::bigint as legacy_source_count,
              count(distinct source_catalog)::bigint as source_catalog_count
            from canonical_system_sources
            group by canonical_system_key
            """
        )

        con.execute(
            """
            create table system_quarantine_conflicts as
            select
              row_number() over (order by canonical_system_key)::bigint as quarantine_id,
              'legacy_duplicate_bundle'::varchar as issue_type,
              canonical_system_key as subject_key,
              'medium'::varchar as severity,
              legacy_source_count::bigint as affected_node_count,
              json_object('legacy_source_count', legacy_source_count) as details_json
            from canonical_system_groups
            where legacy_source_count > 1
            """
        )

        con.execute(
            """
            create table canonical_planet_sources as
            with planet_source_base as (
              select
                node_key,
                source_catalog,
                display_name,
                planet_name_norm,
                source_pk as nasa_source_pk,
                host_node_key,
                cast(null as varchar) as stable_object_key,
                cast(null as varchar) as upstream_source_catalog
              from norm.nasa_planet_sources
              union all
              select
                node_key,
                source_catalog,
                display_name,
                planet_name_norm,
                upstream_source_pk as nasa_source_pk,
                host_node_key,
                stable_object_key,
                upstream_source_catalog
              from norm.legacy_crosswalk_planet_sources
            )
            select
              row_number() over (order by p.node_key)::bigint as canonical_source_id,
              p.node_key,
              p.source_catalog,
              p.display_name,
              p.planet_name_norm,
              p.nasa_source_pk,
              p.stable_object_key,
              p.host_node_key,
              s.canonical_star_key as canonical_host_star_key,
              case
                when p.planet_name_norm is not null
                 and (
                   p.source_catalog = 'nasa_exoplanet_archive'
                   or p.upstream_source_catalog = 'nasa_exoplanet_archive'
                 )
                then 'canon:planet:nasa_name:' || p.planet_name_norm
                when p.nasa_source_pk is not null then 'canon:planet:nasa_source:' || p.nasa_source_pk
                when p.stable_object_key is not null then 'canon:planet:stable:' || p.stable_object_key
                else 'canon:planet:node:' || p.node_key
              end as canonical_planet_key,
              case
                when p.planet_name_norm is not null
                 and (
                   p.source_catalog = 'nasa_exoplanet_archive'
                   or p.upstream_source_catalog = 'nasa_exoplanet_archive'
                 )
                then 'planet_name_norm_nasa_lineage'
                when p.nasa_source_pk is not null then 'nasa_source_pk_fallback'
                when p.stable_object_key is not null then 'legacy_stable_object_key'
                else 'fallback_node'
              end as resolution_method,
              case
                when p.planet_name_norm is not null
                 and (
                   p.source_catalog = 'nasa_exoplanet_archive'
                   or p.upstream_source_catalog = 'nasa_exoplanet_archive'
                 )
                then 0.97
                when p.nasa_source_pk is not null then 0.90
                when p.stable_object_key is not null then 0.90
                else 0.50
              end as resolution_confidence
            from planet_source_base p
            left join canonical_star_sources s on s.node_key = p.host_node_key
            """
        )

        con.execute(
            """
            create table canonical_planet_groups as
            select
              canonical_planet_key,
              min(display_name) as representative_name,
              min(planet_name_norm) as planet_name_norm,
              min(nasa_source_pk) as nasa_source_pk,
              min(stable_object_key) as stable_object_key,
              min(canonical_host_star_key) as canonical_host_star_key,
              count(*)::bigint as source_node_count,
              (count(*) filter (where source_catalog = 'legacy_core_crosswalk'))::bigint as legacy_source_count,
              count(distinct source_catalog)::bigint as source_catalog_count
            from canonical_planet_sources
            group by canonical_planet_key
            """
        )

        con.execute(
            """
            create table planet_quarantine_conflicts as
            with legacy_duplicate_bundle as (
              select
                'legacy_duplicate_bundle'::varchar as issue_type,
                canonical_planet_key as subject_key,
                'high'::varchar as severity,
                legacy_source_count::bigint as affected_node_count,
                json_object(
                  'legacy_source_count', legacy_source_count,
                  'stable_object_key', stable_object_key,
                  'nasa_source_pk', nasa_source_pk
                ) as details_json
              from canonical_planet_groups
              where legacy_source_count > 1
            ),
            missing_host_mapping as (
              select
                'missing_canonical_host_star'::varchar as issue_type,
                node_key as subject_key,
                'medium'::varchar as severity,
                1::bigint as affected_node_count,
                json_object(
                  'host_node_key', host_node_key,
                  'source_catalog', source_catalog
                ) as details_json
              from canonical_planet_sources
              where host_node_key is not null and canonical_host_star_key is null
            )
            select row_number() over (order by severity desc, issue_type, subject_key)::bigint as quarantine_id, *
            from (
              select * from legacy_duplicate_bundle
              union all
              select * from missing_host_mapping
            )
            """
        )

        con.execute(
            """
            create table canonical_relations as
            with star_component_rel as (
              select
                row_number() over (order by cs.canonical_star_key, sy.canonical_system_key, e.match_method)::bigint as relation_id,
                'star'::varchar as left_entity_kind,
                cs.canonical_star_key as left_canonical_key,
                'system'::varchar as right_entity_kind,
                sy.canonical_system_key as right_canonical_key,
                e.relation_type,
                e.match_method,
                max(e.confidence_score) as confidence_score,
                count(*)::bigint as supporting_edge_count
              from graph.graph_edges e
              join canonical_star_sources cs on cs.node_key = e.left_node_key
              join canonical_system_sources sy on sy.node_key = e.right_node_key
              where e.relation_type = 'component_of'
              group by 2,3,4,5,6,7
            ),
            planet_host_rel as (
              select
                row_number() over (order by cp.canonical_planet_key, cs.canonical_star_key, e.match_method)::bigint as relation_id,
                'planet'::varchar as left_entity_kind,
                cp.canonical_planet_key as left_canonical_key,
                'star'::varchar as right_entity_kind,
                cs.canonical_star_key as right_canonical_key,
                e.relation_type,
                e.match_method,
                max(e.confidence_score) as confidence_score,
                count(*)::bigint as supporting_edge_count
              from graph.graph_edges e
              join canonical_planet_sources cp on cp.node_key = e.left_node_key
              join canonical_star_sources cs on cs.node_key = e.right_node_key
              where e.relation_type = 'planet_hosts_star'
              group by 2,3,4,5,6,7
            )
            select * from star_component_rel
            union all
            select * from planet_host_rel
            """
        )

        table_counts = {
            "canonical_star_sources": count_table(con, "canonical_star_sources"),
            "canonical_star_groups": count_table(con, "canonical_star_groups"),
            "star_quarantine_conflicts": count_table(con, "star_quarantine_conflicts"),
            "canonical_system_sources": count_table(con, "canonical_system_sources"),
            "canonical_system_groups": count_table(con, "canonical_system_groups"),
            "system_quarantine_conflicts": count_table(con, "system_quarantine_conflicts"),
            "canonical_planet_sources": count_table(con, "canonical_planet_sources"),
            "canonical_planet_groups": count_table(con, "canonical_planet_groups"),
            "planet_quarantine_conflicts": count_table(con, "planet_quarantine_conflicts"),
            "canonical_relations": count_table(con, "canonical_relations"),
        }

        interesting_samples = {
            "star_quarantine_top": con.execute(
                """
                select issue_type, subject_key, severity, affected_node_count
                from star_quarantine_conflicts
                order by affected_node_count desc, subject_key
                limit 10
                """
            ).fetchall(),
            "planet_quarantine_top": con.execute(
                """
                select issue_type, subject_key, severity, affected_node_count
                from planet_quarantine_conflicts
                order by affected_node_count desc, subject_key
                limit 10
                """
            ).fetchall(),
            "sample_16cyg_planets": con.execute(
                """
                select canonical_planet_key, source_catalog, node_key, stable_object_key, canonical_host_star_key
                from canonical_planet_sources
                where lower(display_name) like '16 cyg b b%'
                order by source_catalog, node_key
                """
            ).fetchall(),
        }
    finally:
        con.close()

    reports_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "generated_at": utc_now(),
        "build_id": build_id,
        "canonical_reduction_db_path": str(db_path),
        "table_counts": table_counts,
        "samples": {
            key: [
                [str(value) if value is not None else None for value in row]
                for row in rows
            ]
            for key, rows in interesting_samples.items()
        },
        "notes": [
            "This is a bootstrap reducer built on exact graph evidence plus the transitional legacy_core_crosswalk bridge.",
            "Planet duplicate bundles from the current build are intentionally surfaced in quarantine instead of being silently collapsed away.",
        ],
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ingest canonical reduction artifacts for a Spacegate build.")
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
    payload = build_reduction(build_id=build_id, build_dir=build_dir, reports_dir=reports_dir)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
