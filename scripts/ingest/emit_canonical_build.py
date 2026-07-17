#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_token_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def git_sha(root: Path) -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=root,
                text=True,
            ).strip()
            or "unknown"
        )
    except Exception:
        return "unknown"


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


def normalize_expr(expr: str) -> str:
    return (
        "trim(regexp_replace(regexp_replace(lower(coalesce("
        + expr
        + ", '')), '[^0-9a-z]+', ' ', 'g'), '\\\\s+', ' ', 'g'))"
    )


def count_table(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"select count(*) from {table_name}").fetchone()[0])


def remap_object_identifiers(con: duckdb.DuckDBPyConnection) -> None:
    """Retarget source object identifiers after canonical entity reduction."""
    for target_type, mapping_table, old_column, new_column in (
        ("system", "original_system_to_preview", "original_system_id", "system_id"),
        ("star", "original_star_to_preview", "original_star_id", "star_id"),
        ("planet", "original_planet_to_preview", "original_planet_id", "planet_id"),
    ):
        con.execute(
            f"""
            update object_identifiers oi
            set target_id = mapping.{new_column}
            from {mapping_table} mapping
            where oi.target_type = {sql_literal(target_type)}
              and oi.target_id = mapping.{old_column}
            """
        )

    con.execute(
        """
        delete from object_identifiers
        where
          (target_type = 'system' and target_id not in (select system_id from systems))
          or (target_type = 'star' and target_id not in (select star_id from stars))
          or (target_type = 'planet' and target_id not in (select planet_id from planets))
          or target_type not in ('system', 'star', 'planet')
        """
    )
    con.execute(
        """
        create or replace temp table object_identifier_dedup as
        select
          identifier_id,
          row_number() over (
            partition by target_type, target_id, namespace, id_value_norm
            order by
              coalesce(is_canonical, false) desc,
              resolution_confidence desc nulls last,
              coalesce(source_catalog, '') asc,
              coalesce(source_pk, 0) asc,
              identifier_id asc
          ) as rn
        from object_identifiers
        """
    )
    con.execute(
        """
        delete from object_identifiers
        where identifier_id in (
          select identifier_id from object_identifier_dedup where rn > 1
        )
        """
    )
    con.execute(
        """
        create or replace temp table object_identifier_reseed as
        select
          row_number() over (
            order by target_type, target_id, namespace, id_value_norm, identifier_id
          )::bigint as new_identifier_id,
          identifier_id
        from object_identifiers
        """
    )
    con.execute(
        """
        update object_identifiers oi
        set identifier_id = reseed.new_identifier_id
        from object_identifier_reseed reseed
        where oi.identifier_id = reseed.identifier_id
        """
    )


def table_fingerprint(con: duckdb.DuckDBPyConnection, table_name: str, key_column: str) -> dict[str, object]:
    row = con.execute(
        f"""
        select
          count(*)::bigint as row_count,
          bit_xor(hash({key_column}))::ubigint as xor_hash,
          min(hash({key_column}))::ubigint as min_hash,
          max(hash({key_column}))::ubigint as max_hash
        from {table_name}
        """
    ).fetchone()
    return {
        "row_count": int(row[0] or 0),
        "xor_hash_hex": f"{int(row[1] or 0):016x}",
        "min_hash_uint64": int(row[2] or 0),
        "max_hash_uint64": int(row[3] or 0),
    }


def emit_canonical_build(
    *,
    root: Path,
    state: Path,
    source_build_id: str,
    source_build_dir: Path,
    canonical_build_id: str,
    hierarchy_source: Path | None = None,
) -> dict[str, object]:
    source_core = source_build_dir / "core.duckdb"
    source_arm = source_build_dir / "arm.duckdb"
    source_hierarchy = hierarchy_source or source_build_dir / "ingest" / "canonical_hierarchy.duckdb"
    source_reduction = source_build_dir / "ingest" / "canonical_reduction.duckdb"
    missing = [
        str(path)
        for path in (source_core, source_arm, source_hierarchy, source_reduction)
        if not path.exists()
    ]
    if missing:
        raise SystemExit("Missing canonical build prerequisites: " + ", ".join(missing))

    out_dir = state / "out"
    canonical_dir = out_dir / canonical_build_id
    tmp_dir = out_dir / f"{canonical_build_id}.tmp"
    if canonical_dir.exists():
        raise SystemExit(f"Canonical build already exists: {canonical_dir}")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=False)

    canonical_core = tmp_dir / "core.duckdb"
    canonical_hierarchy = tmp_dir / "canonical_hierarchy.duckdb"
    canonical_arm = tmp_dir / "arm.duckdb"
    canonical_arm_report = tmp_dir / "arm_report.json"
    parquet_dir = tmp_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(source_core, canonical_core)
    shutil.copy2(source_hierarchy, canonical_hierarchy)

    con = duckdb.connect(str(canonical_core))
    report: dict[str, object] | None = None
    determinism_report: dict[str, object] | None = None
    try:
        maybe_set_duckdb_env(con)
        con.execute(f"ATTACH {sql_literal(str(source_core))} AS src_core (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(str(source_reduction))} AS red (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(str(canonical_hierarchy))} AS hier (READ_ONLY)")

        con.execute(
            """
            create or replace temp table rep_systems as
            select
              canonical_system_key,
              try_cast(replace(node_key, 'src:legacy_core_system:', '') as bigint) as original_system_id
            from red.canonical_system_sources
            where node_key like 'src:legacy_core_system:%'
            qualify row_number() over (
              partition by canonical_system_key
              order by resolution_confidence desc, original_system_id asc
            ) = 1
            """
        )
        con.execute(
            """
            create or replace temp table rep_stars as
            select
              canonical_star_key,
              try_cast(replace(node_key, 'src:legacy_core_star:', '') as bigint) as original_star_id
            from red.canonical_star_sources
            where node_key like 'src:legacy_core_star:%'
            qualify row_number() over (
              partition by canonical_star_key
              order by resolution_confidence desc, original_star_id asc
            ) = 1
            """
        )
        con.execute(
            """
            create or replace temp table rep_planets as
            select
              canonical_planet_key,
              try_cast(replace(node_key, 'src:legacy_core_planet:', '') as bigint) as original_planet_id
            from red.canonical_planet_sources
            where node_key like 'src:legacy_core_planet:%'
            qualify row_number() over (
              partition by canonical_planet_key
              order by resolution_confidence desc, original_planet_id asc
            ) = 1
            """
        )

        con.execute(
            """
            create or replace temp table preview_system_map as
            with ranked as (
              select
                rs.*,
                row_number() over (
                  partition by original_system_id order by canonical_system_key
                ) as id_rank
              from rep_systems rs
              where rs.original_system_id is not null
            ), numbered as (
              select
                *,
                sum(case when id_rank > 1 then 1 else 0 end) over (
                  order by canonical_system_key rows unbounded preceding
                ) as overflow_id
              from ranked
            ), base as (
              select coalesce(max(system_id), 0)::bigint as max_id from src_core.systems
            )
            select
              canonical_system_key,
              case when id_rank = 1 then original_system_id else base.max_id + overflow_id end as system_id
            from numbered cross join base
            """
        )
        con.execute(
            """
            create or replace temp table preview_star_map as
            with ranked as (
              select
                rs.*,
                row_number() over (
                  partition by original_star_id order by canonical_star_key
                ) as id_rank
              from rep_stars rs
              where rs.original_star_id is not null
            ), numbered as (
              select
                *,
                sum(case when id_rank > 1 then 1 else 0 end) over (
                  order by canonical_star_key rows unbounded preceding
                ) as overflow_id
              from ranked
            ), base as (
              select coalesce(max(star_id), 0)::bigint as max_id from src_core.stars
            )
            select
              canonical_star_key,
              case when id_rank = 1 then original_star_id else base.max_id + overflow_id end as star_id
            from numbered cross join base
            """
        )
        con.execute(
            """
            create or replace temp table preview_planet_map as
            with ranked as (
              select
                rp.*,
                row_number() over (
                  partition by original_planet_id order by canonical_planet_key
                ) as id_rank
              from rep_planets rp
              where rp.original_planet_id is not null
            ), numbered as (
              select
                *,
                sum(case when id_rank > 1 then 1 else 0 end) over (
                  order by canonical_planet_key rows unbounded preceding
                ) as overflow_id
              from ranked
            ), base as (
              select coalesce(max(planet_id), 0)::bigint as max_id from src_core.planets
            )
            select
              canonical_planet_key,
              case when id_rank = 1 then original_planet_id else base.max_id + overflow_id end as planet_id
            from numbered cross join base
            """
        )
        con.execute(
            """
            create or replace temp table source_system_to_preview as
            select
              try_cast(replace(css.node_key, 'src:legacy_core_system:', '') as bigint) as original_system_id,
              psm.system_id,
              css.canonical_system_key
            from red.canonical_system_sources css
            join preview_system_map psm on psm.canonical_system_key = css.canonical_system_key
            where css.node_key like 'src:legacy_core_system:%'
              and try_cast(replace(css.node_key, 'src:legacy_core_system:', '') as bigint) is not null
            """
        )
        con.execute(
            """
            create or replace temp table source_star_to_preview as
            select
              try_cast(replace(css.node_key, 'src:legacy_core_star:', '') as bigint) as original_star_id,
              psm.star_id,
              css.canonical_star_key
            from red.canonical_star_sources css
            join preview_star_map psm on psm.canonical_star_key = css.canonical_star_key
            where css.node_key like 'src:legacy_core_star:%'
              and try_cast(replace(css.node_key, 'src:legacy_core_star:', '') as bigint) is not null
            """
        )

        con.execute(
            """
            create or replace temp table preview_star_system as
            select
              psm.system_id,
              pstm.star_id,
              h.parent_node_key as canonical_system_key,
              h.child_node_key as canonical_star_key
            from hier.hierarchy_edges h
            join preview_system_map psm on psm.canonical_system_key = h.parent_node_key
            join preview_star_map pstm on pstm.canonical_star_key = h.child_node_key
            where h.edge_kind = 'contains'
            qualify row_number() over (
              partition by pstm.star_id
              order by psm.system_id, h.parent_node_key
            ) = 1
            """
        )
        con.execute(
            """
            create or replace temp table preview_planet_host as
            select
              pstm.star_id,
              ppm.planet_id,
              h.parent_node_key as canonical_star_key,
              h.child_node_key as canonical_planet_key
            from hier.hierarchy_edges h
            join preview_star_map pstm on pstm.canonical_star_key = h.parent_node_key
            join preview_planet_map ppm on ppm.canonical_planet_key = h.child_node_key
            where h.edge_kind = 'contains'
            """
        )
        con.execute(
            """
            create or replace temp table preview_planet_system as
            with hosted as (
              select
                pss.system_id,
                pph.star_id,
                pph.planet_id,
                pph.canonical_planet_key
              from preview_planet_host pph
              join preview_star_system pss on pss.canonical_star_key = pph.canonical_star_key
            ),
            root_fallback as (
              select
                psm.system_id,
                cast(null as bigint) as star_id,
                ppm.planet_id,
                h.child_node_key as canonical_planet_key
              from hier.hierarchy_edges h
              join preview_system_map psm on psm.canonical_system_key = h.parent_node_key
              join preview_planet_map ppm on ppm.canonical_planet_key = h.child_node_key
              where h.edge_kind = 'contains'
                and not exists (
                  select 1
                  from preview_planet_host hosted
                  where hosted.canonical_planet_key = h.child_node_key
                )
            ),
            source_fallback as (
              select
                coalesce(host_psys.system_id, psys.system_id, osp.system_id) as system_id,
                coalesce(host_star.star_id, pstar.star_id) as star_id,
                ppm.planet_id,
                ppm.canonical_planet_key
              from preview_planet_map ppm
              join red.canonical_planet_groups cpg on cpg.canonical_planet_key = ppm.canonical_planet_key
              join src_core.planets p on p.planet_id = ppm.planet_id
              left join preview_planet_host ph on ph.canonical_planet_key = ppm.canonical_planet_key
              left join preview_star_map host_star on host_star.canonical_star_key = cpg.canonical_host_star_key
              left join preview_star_system host_psys on host_psys.star_id = host_star.star_id
              left join source_system_to_preview osp on osp.original_system_id = p.system_id
              left join source_star_to_preview pstar on pstar.original_star_id = p.star_id
              left join preview_star_system psys on psys.star_id = pstar.star_id
              where ph.canonical_planet_key is null
                and coalesce(host_psys.system_id, psys.system_id, osp.system_id) is not null
            ),
            combined as (
              select * from hosted
              union all
              select * from root_fallback
              union all
              select * from source_fallback
            )
            select system_id, star_id, planet_id, canonical_planet_key
            from combined
            qualify row_number() over (
              partition by canonical_planet_key
              order by
                case when star_id is not null then 0 else 1 end asc,
                system_id asc,
                coalesce(star_id, 9223372036854775807) asc,
                planet_id asc
            ) = 1
            """
        )
        con.execute(
            """
            create or replace temp table preview_leaf_rollup as
            with direct_children as (
              select
                e.parent_node_key as canonical_system_key,
                e.child_node_key,
                n.node_kind
              from hier.hierarchy_edges e
              join hier.hierarchy_nodes n on n.hierarchy_node_key = e.child_node_key
              where e.edge_kind = 'contains'
                and n.node_kind in ('star', 'unresolved_component')
            ),
            leaf_counts as (
              select
                e.parent_node_key as canonical_star_key,
                count(*) filter (
                  where coalesce(n.component_type, 'star') not in ('brown_dwarf', 'substellar')
                )::bigint as leaf_star_count
              from hier.hierarchy_edges e
              join hier.hierarchy_nodes n on n.hierarchy_node_key = e.child_node_key
              where e.edge_kind = 'contains'
                and n.node_kind = 'inferred_star_leaf'
              group by 1
            )
            select
              dc.canonical_system_key,
              sum(case when coalesce(lc.leaf_star_count, 0) > 0 then lc.leaf_star_count else 1 end)::bigint as effective_star_count
            from direct_children dc
            left join leaf_counts lc on lc.canonical_star_key = dc.child_node_key
            group by 1
            """
        )

        con.execute("begin transaction")
        try:
            con.execute("drop table if exists systems")
            con.execute(
                f"""
                create table systems as
                select
                  s.* replace (
                    psm.system_id as system_id,
                    csg.canonical_system_key as stable_object_key,
                    coalesce(nullif(csg.representative_name, ''), s.system_name) as system_name,
                    {normalize_expr("coalesce(nullif(csg.representative_name, ''), s.system_name)")} as system_name_norm,
                    coalesce(nullif(csg.wds_id, ''), s.wds_id) as wds_id,
                    'canonical_build_v2' as grouping_basis,
                    cast(1.00 as decimal(3,2)) as grouping_confidence,
                    'high' as grouping_confidence_tier,
                    cast(0 as bigint) as star_count,
                    cast(0 as bigint) as planet_count,
                    cast(0 as bigint) as star_teff_count,
                    cast(null as double) as min_star_teff_k,
                    cast(null as double) as max_star_teff_k,
                    cast('[]' as varchar) as spectral_classes_json,
                    cast(0 as bigint) as spectral_class_mask
                  )
                from src_core.systems s
                join preview_system_map psm on psm.system_id = s.system_id
                join red.canonical_system_groups csg on csg.canonical_system_key = psm.canonical_system_key
                """
            )

            con.execute("drop table if exists stars")
            con.execute(
                f"""
                create table stars as
                select
                  s.* replace (
                    pss.star_id as star_id,
                    pss.system_id as system_id,
                    csg.canonical_star_key as stable_object_key,
                    coalesce(nullif(csg.representative_name, ''), s.star_name) as star_name,
                    {normalize_expr("coalesce(nullif(csg.representative_name, ''), s.star_name)")} as star_name_norm
                  )
                from src_core.stars s
                join preview_star_map psm on psm.star_id = s.star_id
                join preview_star_system pss on pss.star_id = psm.star_id
                join red.canonical_star_groups csg on csg.canonical_star_key = psm.canonical_star_key
                """
            )

            con.execute("drop table if exists planets")
            con.execute(
                f"""
                create table planets as
                select
                  p.* replace (
                    ppm.planet_id as planet_id,
                    pps.system_id as system_id,
                    pps.star_id as star_id,
                    cpg.canonical_planet_key as stable_object_key,
                    coalesce(nullif(cpg.representative_name, ''), p.planet_name) as planet_name,
                    {normalize_expr("coalesce(nullif(cpg.representative_name, ''), p.planet_name)")} as planet_name_norm
                  )
                from src_core.planets p
                join preview_planet_map ppm on ppm.planet_id = p.planet_id
                left join preview_planet_system pps on pps.planet_id = ppm.planet_id
                join red.canonical_planet_groups cpg on cpg.canonical_planet_key = ppm.canonical_planet_key
                """
            )

            con.execute(
                """
                update systems s
                set star_count = counts.star_count
                from (
                  select
                    psm.system_id,
                    coalesce(plr.effective_star_count, count(*)::bigint) as star_count
                  from preview_star_system psm
                  left join preview_leaf_rollup plr on plr.canonical_system_key = psm.canonical_system_key
                  group by 1, plr.effective_star_count
                ) counts
                where s.system_id = counts.system_id
                """
            )
            con.execute(
                """
                update systems s
                set planet_count = counts.planet_count
                from (
                  select system_id, count(*)::bigint as planet_count
                  from planets
                  group by 1
                ) counts
                where s.system_id = counts.system_id
                """
            )
            con.execute(
                """
                update systems s
                set
                  star_teff_count = stats.star_teff_count,
                  min_star_teff_k = stats.min_star_teff_k,
                  max_star_teff_k = stats.max_star_teff_k,
                  spectral_classes_json = stats.spectral_classes_json,
                  spectral_class_mask = stats.spectral_class_mask
                from (
                  select
                    system_id,
                    count(*) filter (where teff_k is not null)::bigint as star_teff_count,
                    min(teff_k) as min_star_teff_k,
                    max(teff_k) as max_star_teff_k,
                    coalesce(
                      to_json(
                        list(distinct spectral_class order by spectral_class)
                        filter (where nullif(spectral_class, '') is not null)
                      )::varchar,
                      '[]'
                    ) as spectral_classes_json,
                    sum(distinct case upper(coalesce(spectral_class, ''))
                      when 'O' then 1
                      when 'B' then 2
                      when 'A' then 4
                      when 'F' then 8
                      when 'G' then 16
                      when 'K' then 32
                      when 'M' then 64
                      when 'L' then 128
                      when 'T' then 256
                      when 'Y' then 512
                      when 'D' then 1024
                      else 0
                    end)::bigint as spectral_class_mask
                  from stars
                  group by 1
                ) stats
                where s.system_id = stats.system_id
                """
            )

            con.execute(
                """
                create or replace temp table original_system_to_preview as
                select distinct original_system_id, system_id
                from source_system_to_preview
                """
            )
            con.execute(
                """
                create or replace temp table original_star_to_preview as
                select distinct original_star_id, star_id
                from source_star_to_preview
                """
            )
            con.execute(
                """
                create or replace temp table original_planet_to_preview as
                select original_planet_id, planet_id
                from rep_planets
                join preview_planet_map using (canonical_planet_key)
                """
            )
            con.execute(
                """
                create or replace temp table preview_star_to_system as
                select star_id, system_id from stars
                """
            )
            con.execute(
                """
                create or replace temp table preview_planet_to_system as
                select planet_id, system_id from planets
                """
            )

            con.execute(
                """
                update aliases a
                set
                  target_id = osp.system_id,
                  system_id = osp.system_id,
                  star_id = null
                from original_system_to_preview osp
                where a.target_type = 'system'
                  and a.target_id = osp.original_system_id
                """
            )
            con.execute(
                """
                update aliases a
                set
                  target_id = ost.star_id,
                  star_id = ost.star_id,
                  system_id = pts.system_id
                from original_star_to_preview ost
                join preview_star_to_system pts on pts.star_id = ost.star_id
                where a.target_type = 'star'
                  and a.target_id = ost.original_star_id
                """
            )
            con.execute(
                """
                update aliases a
                set
                  target_id = opp.planet_id,
                  system_id = ptp.system_id,
                  star_id = null
                from original_planet_to_preview opp
                join preview_planet_to_system ptp on ptp.planet_id = opp.planet_id
                where a.target_type = 'planet'
                  and a.target_id = opp.original_planet_id
                """
            )
            con.execute(
                """
                delete from aliases
                where
                  (target_type = 'system' and target_id not in (select system_id from systems))
                  or (target_type = 'star' and target_id not in (select star_id from stars))
                  or (target_type = 'planet' and target_id not in (select planet_id from planets))
                  or (
                    target_type not in ('system', 'star', 'planet')
                    and coalesce(system_id, star_id) is null
                  )
                """
            )
            con.execute(
                """
                create or replace temp table alias_dedup as
                select
                  *,
                  row_number() over (
                    partition by target_type, target_id, alias_norm
                    order by coalesce(alias_priority, 999999) asc, alias_raw asc, alias_id asc
                  ) as rn
                from aliases
                where nullif(alias_norm, '') is not null
                """
            )
            con.execute("delete from aliases where alias_id in (select alias_id from alias_dedup where rn > 1)")
            con.execute(
                """
                create or replace temp table alias_reseed as
                select
                  row_number() over (
                    order by target_type, target_id, coalesce(alias_priority, 999999), alias_norm, alias_raw, alias_id
                  )::bigint as new_alias_id,
                  alias_id
                from aliases
                """
            )
            con.execute(
                """
                update aliases a
                set alias_id = r.new_alias_id
                from alias_reseed r
                where a.alias_id = r.alias_id
                """
            )

            con.execute("drop table if exists system_search_terms")
            con.execute(
                """
                create table system_search_terms as
                with canonical as (
                  select
                    system_id,
                    'system'::varchar as target_type,
                    system_id as target_id,
                    null::bigint as star_id,
                    null::bigint as alias_id,
                    system_name as term_raw,
                    system_name_norm as term_norm,
                    'canonical_name'::varchar as term_kind,
                    0::integer as term_priority,
                    true as is_primary,
                    source_catalog,
                    source_version,
                    source_pk
                  from systems
                  where system_name_norm is not null
                    and trim(system_name_norm) <> ''
                ), alias_terms as (
                  select
                    system_id,
                    target_type,
                    target_id,
                    star_id,
                    alias_id,
                    alias_raw as term_raw,
                    alias_norm as term_norm,
                    alias_kind as term_kind,
                    coalesce(alias_priority, 999)::integer as term_priority,
                    false as is_primary,
                    source_catalog,
                    source_version,
                    source_pk
                  from aliases
                  where system_id is not null
                    and alias_norm is not null
                    and trim(alias_norm) <> ''
                ), member_star_terms as (
                  select
                    system_id,
                    'star'::varchar as target_type,
                    star_id as target_id,
                    star_id,
                    null::bigint as alias_id,
                    star_name as term_raw,
                    star_name_norm as term_norm,
                    'member_star_name'::varchar as term_kind,
                    500::integer as term_priority,
                    false as is_primary,
                    source_catalog,
                    source_version,
                    source_pk
                  from stars
                  where star_name_norm is not null
                    and trim(star_name_norm) <> ''
                    and star_name_norm not like 'gaia dr3 %'
                    and star_name_norm not like 'gaia %'
                ), merged as (
                  select * from canonical
                  union all
                  select * from alias_terms
                  union all
                  select * from member_star_terms
                ), dedup as (
                  select
                    *,
                    row_number() over (
                      partition by system_id, term_norm
                      order by
                        term_priority asc,
                        case when term_kind = 'canonical_name' then 0 else 1 end asc,
                        length(coalesce(term_raw, '')) asc,
                        coalesce(term_raw, '') asc,
                        coalesce(source_catalog, '') asc,
                        coalesce(source_pk, 0) asc
                    ) as rn
                  from merged
                  where term_raw is not null
                    and trim(term_raw) <> ''
                )
                select
                  row_number() over (
                    order by system_id asc, term_priority asc, term_kind asc, term_norm asc, term_raw asc
                  )::bigint as search_term_id,
                  system_id,
                  target_type,
                  target_id,
                  star_id,
                  alias_id,
                  term_raw,
                  term_norm,
                  term_kind,
                  term_priority,
                  is_primary,
                  source_catalog,
                  source_version,
                  source_pk
                from dedup
                where rn = 1
                """
            )

            remap_object_identifiers(con)

            con.execute(
                """
                delete from build_metadata
                where key in (
                  'build_id',
                  'bootstrap_source_build_id',
                  'canonical_mode',
                  'canonical_hierarchy_enabled',
                  'canonical_build_generated_at',
                  'canonical_transform_git_sha'
                )
                """
            )
            con.execute(
                """
                insert into build_metadata values
                  ('build_id', ?),
                  ('bootstrap_source_build_id', ?),
                  ('canonical_mode', 'ingest_canonical_build'),
                  ('canonical_hierarchy_enabled', '1'),
                  ('canonical_build_generated_at', ?),
                  ('canonical_transform_git_sha', ?)
                """,
                [canonical_build_id, source_build_id, utc_now(), git_sha(root)],
            )

            con.execute(
                f"""
                copy (select * from systems order by system_id asc)
                to {sql_literal(str(parquet_dir / 'systems.parquet'))}
                (format parquet, compression zstd)
                """
            )
            con.execute(
                f"""
                copy (select * from stars order by star_id asc)
                to {sql_literal(str(parquet_dir / 'stars.parquet'))}
                (format parquet, compression zstd)
                """
            )
            con.execute(
                f"""
                copy (select * from planets order by planet_id asc)
                to {sql_literal(str(parquet_dir / 'planets.parquet'))}
                (format parquet, compression zstd)
                """
            )
            con.execute(
                f"""
                copy (select * from system_search_terms order by search_term_id asc)
                to {sql_literal(str(parquet_dir / 'system_search_terms.parquet'))}
                (format parquet, compression zstd)
                """
            )
            con.execute(
                f"""
                copy (select * from aliases order by alias_id asc)
                to {sql_literal(str(parquet_dir / 'aliases.parquet'))}
                (format parquet, compression zstd)
                """
            )
            con.execute(
                f"""
                copy (select * from object_identifiers order by identifier_id asc)
                to {sql_literal(str(parquet_dir / 'object_identifiers.parquet'))}
                (format parquet, compression zstd)
                """
            )
            con.execute(
                f"""
                copy (select * from identifier_quarantine order by quarantine_id asc)
                to {sql_literal(str(parquet_dir / 'identifier_quarantine.parquet'))}
                (format parquet, compression zstd)
                """
            )
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise

        report = {
            "generated_at": utc_now(),
            "canonical_build_id": canonical_build_id,
            "source_build_id": source_build_id,
            "source_build_dir": str(source_build_dir),
            "canonical_hierarchy_source": str(source_hierarchy),
            "canonical_build_dir": str(canonical_dir),
            "canonical_build_mode": "ingest_canonical_build",
            "table_counts": {
                "systems": count_table(con, "systems"),
                "stars": count_table(con, "stars"),
                "planets": count_table(con, "planets"),
                "aliases": count_table(con, "aliases"),
                "system_search_terms": count_table(con, "system_search_terms"),
                "object_identifiers": count_table(con, "object_identifiers"),
            },
            "samples": {
                "castor": con.execute(
                    """
                    select system_id, stable_object_key, system_name, star_count, planet_count
                    from systems
                    where stable_object_key = 'canon:system:wds:07346+3153'
                    """
                ).fetchall(),
                "cyg16": con.execute(
                    """
                    select system_id, stable_object_key, system_name, star_count, planet_count
                    from systems
                    where stable_object_key = 'canon:system:wds:19418+5032'
                    """
                ).fetchall(),
            },
            "notes": [
                "Canonical build keeps representative legacy row ids where possible, but replaces stable_object_key with ingest canonical keys.",
                "Canonical hierarchy lives beside the core build and is preferred by the API when present.",
                "Canonical builds materialize compact system_search_terms so the API exercises the fast search path.",
                "Member-star names are included in search terms when they are not Gaia-style placeholders, so lookups like AR Cas resolve without relying on fallback scans.",
                "Auxiliary tables not needed for search/detail remain copied from the source build; only key browse/detail tables are canonicalized here.",
            ],
        }
        determinism_report = {
            "build_id": canonical_build_id,
            "bootstrap_source_build_id": source_build_id,
            "generated_at": utc_now(),
            "source_inputs_fingerprint": f"canonical:{source_build_id}",
            "transform_version": "ingest_canonical_build",
            "transform_fingerprint": git_sha(root),
            "build_layer": "core",
            "slice_profile_id": "",
            "slice_profile_version": "",
            "table_fingerprints": {
                "systems": table_fingerprint(con, "systems", "stable_object_key"),
                "stars": table_fingerprint(con, "stars", "stable_object_key"),
                "planets": table_fingerprint(con, "planets", "stable_object_key"),
            },
        }
    finally:
        con.close()

    if report is None:
        raise RuntimeError("canonical build failed before report generation")

    subprocess.check_call(
        [
            sys.executable,
            str(root / "scripts" / "build_arm.py"),
            "--core-db",
            str(canonical_core),
            "--arm-db",
            str(canonical_arm),
            "--state-dir",
            str(state),
            "--build-id",
            canonical_build_id,
            "--ingested-at",
            str(report["generated_at"]),
            "--transform-version",
            "ingest_canonical_build",
            "--report-path",
            str(canonical_arm_report),
        ],
        cwd=str(root),
    )

    subprocess.check_call(
        [
            sys.executable,
            str(root / "scripts" / "materialize_stellar_leaf_classifications.py"),
            "--core-db", str(canonical_core),
            "--arm-db", str(canonical_arm),
            "--hierarchy-db", str(canonical_hierarchy),
            "--build-id", canonical_build_id,
            "--report-path", str(state / "reports" / canonical_build_id / "stellar_leaf_classification_report.json"),
        ],
        cwd=str(root),
    )

    reports_dir = state / "reports" / canonical_build_id
    reports_dir.mkdir(parents=True, exist_ok=True)
    source_reports_dir = state / "reports" / source_build_id
    if source_reports_dir.is_dir():
        for source_report in source_reports_dir.iterdir():
            if not source_report.is_file():
                continue
            dest_report = reports_dir / source_report.name
            if source_report.suffix == ".json":
                try:
                    payload = json.loads(source_report.read_text(encoding="utf-8"))
                except Exception:
                    shutil.copy2(source_report, dest_report)
                    continue
                if isinstance(payload, dict):
                    payload.setdefault("bootstrap_source_build_id", source_build_id)
                    payload.setdefault("canonical_build_id", canonical_build_id)
                    if payload.get("build_id") == source_build_id:
                        payload["build_id"] = canonical_build_id
                    if source_report.name == "qc_report.json":
                        counts = payload.get("counts")
                        if isinstance(counts, dict):
                            for key, value in (report.get("table_counts") or {}).items():
                                if key in counts:
                                    counts[key] = value
                    dest_report.write_text(
                        json.dumps(payload, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                else:
                    shutil.copy2(source_report, dest_report)
            else:
                shutil.copy2(source_report, dest_report)
    if canonical_arm_report.exists():
        shutil.copy2(canonical_arm_report, reports_dir / "arm_report.json")
    report_path = reports_dir / "canonical_build_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if determinism_report is not None:
        (reports_dir / "determinism_report.json").write_text(
            json.dumps(determinism_report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    tmp_dir.rename(canonical_dir)
    report["canonical_build_dir"] = str(canonical_dir)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Emit a full ingest canonical build.")
    parser.add_argument("--build-id", help="Source build id to transform into a canonical build.")
    parser.add_argument(
        "--latest-out",
        action="store_true",
        help="Use the newest out/<build_id> directory instead of served/current.",
    )
    parser.add_argument(
        "--canonical-build-id",
        help="Optional explicit canonical build id. Default: <timestamp>_<gitsha>_canonical",
    )
    parser.add_argument(
        "--hierarchy-db",
        type=Path,
        help="Optional independently generated canonical hierarchy artifact. The source build remains read-only.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    state = state_dir(root)
    source_build_id, source_build_dir = resolve_build_dir(state, args.build_id, args.latest_out)
    explicit_build_id = args.canonical_build_id
    canonical_build_id = (
        str(explicit_build_id).strip()
        if explicit_build_id
        else f"{build_token_now()}_{git_sha(root)}_canonical"
    )
    payload = emit_canonical_build(
        root=root,
        state=state,
        source_build_id=source_build_id,
        source_build_dir=source_build_dir,
        canonical_build_id=canonical_build_id,
        hierarchy_source=args.hierarchy_db.resolve() if args.hierarchy_db else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
