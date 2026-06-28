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
    reduction_path = build_dir / "ingest" / "canonical_reduction.duckdb"
    arm_path = build_dir / "arm.duckdb"
    missing = [str(p) for p in (reduction_path, arm_path) if not p.exists()]
    if missing:
        raise SystemExit(
            "Missing ingest prerequisites for hierarchy build: " + ", ".join(missing)
        )

    out_dir = build_dir / "ingest"
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
            create temp table explicit_root_role_map as
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
            create temp table msc_role_candidates as
            select
              sys_map.canonical_system_key,
              lower(substr(nullif(ce.catalog_component_label, ''), 1, 1)) as member_role
            from arm.component_entities ce
            join red.canonical_system_groups sys_map
              on sys_map.wds_id = split_part(ce.stable_component_key, ':', 4)
            where ce.stable_component_key like 'comp:msc:wds:%'
              and length(coalesce(ce.catalog_component_label, '')) >= 2
              and lower(substr(nullif(ce.catalog_component_label, ''), 1, 1)) between 'a' and 'z'
            group by 1, 2
            """
        )

        con.execute(
            """
            create temp table inferred_root_role_map as
            with unlabeled_root_star as (
              select
                canonical_system_key,
                min(canonical_star_key) as canonical_star_key
              from root_star_edges
              where member_role is null
              group by canonical_system_key
              having count(distinct canonical_star_key) = 1
            ),
            missing_role as (
              select
                candidate.canonical_system_key,
                min(candidate.member_role) as member_role
              from msc_role_candidates candidate
              join unlabeled_root_star unlabeled
                on unlabeled.canonical_system_key = candidate.canonical_system_key
              left join explicit_root_role_map explicit
                on explicit.canonical_system_key = candidate.canonical_system_key
               and explicit.member_role = candidate.member_role
              where explicit.member_role is null
                and length(candidate.member_role) = 1
                and candidate.member_role between 'a' and 'z'
              group by 1
              having count(distinct candidate.member_role) = 1
            )
            select
              unlabeled.canonical_system_key,
              missing.member_role,
              unlabeled.canonical_star_key
            from unlabeled_root_star unlabeled
            join missing_role missing
              on missing.canonical_system_key = unlabeled.canonical_system_key
            """
        )

        con.execute(
            """
            create temp table root_role_map as
            select * from explicit_root_role_map
            union all
            select * from inferred_root_role_map
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
            with recursive msc_walk as (
              select
                sys_map.canonical_system_key,
                e.parent_component_key,
                e.child_component_key,
                greatest(coalesce(e.depth_hint, 1), 1)::int as depth,
                coalesce(e.confidence_score, 0.72)::double as path_confidence_score
              from arm.system_hierarchy_edges e
              join arm.component_entities parent_ce on parent_ce.stable_component_key = e.parent_component_key
              join red.canonical_system_groups sys_map on sys_map.wds_id = replace(parent_ce.stable_component_key, 'comp:msc_system:wds:', '')
              where parent_ce.stable_component_key like 'comp:msc_system:wds:%'
                and e.edge_kind = 'contains'
              union all
              select
                walk.canonical_system_key,
                e.parent_component_key,
                e.child_component_key,
                walk.depth + 1 as depth,
                least(walk.path_confidence_score, coalesce(e.confidence_score, 0.72))::double as path_confidence_score
              from msc_walk walk
              join arm.system_hierarchy_edges e on e.parent_component_key = walk.child_component_key
              where e.edge_kind = 'contains'
                and walk.depth < 8
            ),
            path_leaf as (
              select
                walk.canonical_system_key,
                ce.stable_component_key,
                lower(nullif(ce.catalog_component_label, '')) as catalog_component_label,
                lower(substr(nullif(ce.catalog_component_label, ''), 1, 1)) as member_role,
                ce.display_name,
                max(walk.path_confidence_score) as confidence_score,
                'medium'::varchar as confidence_tier
              from msc_walk walk
              join arm.component_entities ce on ce.stable_component_key = walk.child_component_key
              where ce.stable_component_key like 'comp:msc:wds:%'
                and length(coalesce(ce.catalog_component_label, '')) >= 2
              group by 1, 2, 3, 4, 5
            ),
            component_leaf as (
              select
                sys_map.canonical_system_key,
                ce.stable_component_key,
                lower(nullif(ce.catalog_component_label, '')) as catalog_component_label,
                lower(substr(nullif(ce.catalog_component_label, ''), 1, 1)) as member_role,
                ce.display_name,
                0.72::double as confidence_score,
                'low'::varchar as confidence_tier
              from arm.component_entities ce
              join red.canonical_system_groups sys_map
                on sys_map.wds_id = split_part(ce.stable_component_key, ':', 4)
              where ce.stable_component_key like 'comp:msc:wds:%'
                and length(coalesce(ce.catalog_component_label, '')) >= 2
            ),
            raw as (
              select * from path_leaf
              union all
              select * from component_leaf
            )
            select
              'canon:leaf:msc:' || replace(stable_component_key, 'comp:msc:wds:', '') as hierarchy_node_key,
              canonical_system_key,
              stable_component_key,
              catalog_component_label,
              member_role,
              display_name,
              max(confidence_score) as confidence_score,
              case
                when max(confidence_score) >= 0.80 then 'medium'
                else 'low'
              end::varchar as confidence_tier
            from raw
            where member_role between 'a' and 'z'
            group by 1, 2, 3, 4, 5, 6
            """
        )

        con.execute(
            """
            create temp table msc_role_leaf_counts as
            select
              canonical_system_key,
              member_role,
              count(*)::bigint as leaf_count
            from msc_leaf_nodes
            where member_role is not null
            group by 1, 2
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
            join msc_role_leaf_counts leaf_counts
              on leaf_counts.canonical_system_key = leaf.canonical_system_key
             and leaf_counts.member_role = leaf.member_role
            where leaf_counts.leaf_count >= 2
            """
        )

        con.execute(
            """
            create temp table unresolved_role_nodes as
            with wds_pair_roles as (
              select
                wds_id,
                lower(substr(component_label, 1, 1)) as role_a,
                lower(substr(component_label, 2, 1)) as role_b,
                lower(component_label) as component_label,
                max(coalesce(obs_count, 0))::bigint as max_obs_count,
                max(coalesce(last_year, 0))::bigint as last_observed,
                string_agg(distinct source_pk, ',' order by source_pk) as source_pks
              from arm.wds_component_observations
              where length(coalesce(component_label, '')) = 2
                and lower(component_label) ~ '^[a-z][a-z]$'
              group by 1, 2, 3, 4
            ),
            supported_missing_roles as (
              select
                leaf_counts.canonical_system_key,
                leaf_counts.member_role,
                leaf_counts.leaf_count,
                max(leaf.confidence_score) as msc_confidence_score,
                string_agg(leaf.catalog_component_label, ',' order by leaf.catalog_component_label) as msc_leaf_labels,
                string_agg(distinct pair.component_label, ',' order by pair.component_label) as wds_pair_labels,
                max(pair.max_obs_count)::bigint as max_wds_obs_count,
                max(pair.last_observed)::bigint as last_wds_observed,
                string_agg(distinct pair.source_pks, ',' order by pair.source_pks) as wds_source_pks
              from msc_role_leaf_counts leaf_counts
              join msc_leaf_nodes leaf
                on leaf.canonical_system_key = leaf_counts.canonical_system_key
               and leaf.member_role = leaf_counts.member_role
              join red.canonical_system_groups sys
                on sys.canonical_system_key = leaf_counts.canonical_system_key
              join wds_pair_roles pair
                on pair.wds_id = sys.wds_id
               and (
                    (pair.role_a = leaf_counts.member_role and exists (
                       select 1
                       from root_role_map sibling
                       where sibling.canonical_system_key = leaf_counts.canonical_system_key
                         and sibling.member_role = pair.role_b
                    ))
                 or (pair.role_b = leaf_counts.member_role and exists (
                       select 1
                       from root_role_map sibling
                       where sibling.canonical_system_key = leaf_counts.canonical_system_key
                         and sibling.member_role = pair.role_a
                    ))
               )
              left join root_role_map resolved
                on resolved.canonical_system_key = leaf_counts.canonical_system_key
               and resolved.member_role = leaf_counts.member_role
              where leaf_counts.leaf_count >= 2
                and resolved.member_role is null
                and pair.max_obs_count >= 2
              group by 1, 2, 3
            )
            select
              'canon:unresolved_role:' || replace(sys.wds_id, ':', '_') || ':' || missing.member_role as hierarchy_node_key,
              missing.canonical_system_key,
              sys.wds_id,
              missing.member_role,
              coalesce(nullif(sys.representative_name, ''), 'WDS ' || sys.wds_id) || ' ' || upper(missing.member_role) as display_name,
              least(0.80, max(missing.msc_confidence_score))::double as confidence_score,
              'medium'::varchar as confidence_tier,
              missing.leaf_count,
              missing.msc_leaf_labels,
              missing.wds_pair_labels,
              missing.max_wds_obs_count,
              missing.last_wds_observed,
              missing.wds_source_pks
            from supported_missing_roles missing
            join red.canonical_system_groups sys
              on sys.canonical_system_key = missing.canonical_system_key
            group by
              missing.canonical_system_key,
              sys.wds_id,
              sys.representative_name,
              missing.member_role,
              missing.leaf_count,
              missing.msc_leaf_labels,
              missing.wds_pair_labels,
              missing.max_wds_obs_count,
              missing.last_wds_observed,
              missing.wds_source_pks
            """
        )

        con.execute(
            """
            create temp table unresolved_role_leaf_edges as
            select
              unresolved.hierarchy_node_key as parent_unresolved_node_key,
              leaf.hierarchy_node_key as child_hierarchy_node_key,
              leaf.catalog_component_label,
              least(unresolved.confidence_score, leaf.confidence_score)::double as confidence_score
            from unresolved_role_nodes unresolved
            join msc_leaf_nodes leaf
              on leaf.canonical_system_key = unresolved.canonical_system_key
             and leaf.member_role = unresolved.member_role
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
            ),
            unresolved_role as (
              select
                hierarchy_node_key,
                'unresolved_component'::varchar as node_kind,
                cast(null as varchar) as canonical_key,
                display_name,
                wds_id,
                member_role,
                'wds_msc_implied_role'::varchar as source_basis
              from unresolved_role_nodes
            )
            select * from system_nodes
            union all
            select * from star_nodes
            union all
            select * from planet_nodes
            union all
            select * from msc_leaf
            union all
            select * from unresolved_role
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
            ),
            system_to_unresolved_role as (
              select
                row_number() over (order by canonical_system_key, hierarchy_node_key)::bigint as hierarchy_edge_id,
                canonical_system_key as parent_node_key,
                hierarchy_node_key as child_node_key,
                'contains'::varchar as edge_kind,
                member_role,
                'wds_msc_implied_role'::varchar as source_basis,
                confidence_score,
                (leaf_count + 1)::bigint as supporting_edge_count
              from unresolved_role_nodes
            ),
            unresolved_role_to_msc_leaf as (
              select
                row_number() over (order by parent_unresolved_node_key, child_hierarchy_node_key)::bigint as hierarchy_edge_id,
                parent_unresolved_node_key as parent_node_key,
                child_hierarchy_node_key as child_node_key,
                'contains'::varchar as edge_kind,
                catalog_component_label as member_role,
                'wds_msc_implied_role_leaf'::varchar as source_basis,
                confidence_score,
                1::bigint as supporting_edge_count
              from unresolved_role_leaf_edges
            )
            select * from system_to_star
            union all
            select * from star_to_planet
            union all
            select * from system_to_planet
            union all
            select * from star_to_msc_leaf
            union all
            select * from system_to_unresolved_role
            union all
            select * from unresolved_role_to_msc_leaf
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
            "Missing one-letter root roles may be represented as unresolved components only when WDS pair evidence and MSC multi-leaf evidence agree.",
            "Singleton MSC subdivisions are suppressed in the canonical hierarchy to avoid overfitting sparse role evidence.",
        ],
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ingest canonical hierarchy artifacts for a Spacegate build.")
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
