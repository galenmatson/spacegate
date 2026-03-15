#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any

import duckdb

MSC_VERSION_FALLBACK = "2024-01-01"
VSX_VERSION_FALLBACK = "vsx_dat"
VSX_URL_FALLBACK = "ftp://cdsarc.u-strasbg.fr/pub/cats/B/vsx/vsx.dat"
ULTRACOOLSHEET_VERSION_FALLBACK = "UltracoolSheet_Main"
ULTRACOOLSHEET_URL_FALLBACK = (
    "https://docs.google.com/spreadsheets/d/1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8/"
    "gviz/tq?tqx=out:csv&sheet=Main"
)


def log(message: str) -> None:
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{timestamp} {message}", flush=True)


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def parse_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    text = value.strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def load_manifest_entry(manifest_path: Path, source_name: str) -> dict[str, Any]:
    if not manifest_path.exists():
        return {}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries: list[dict[str, Any]]
    if isinstance(payload, list):
        entries = [entry for entry in payload if isinstance(entry, dict)]
    elif isinstance(payload, dict):
        entries = [payload]
    else:
        return {}
    for entry in entries:
        if entry.get("source_name") == source_name:
            return entry
    return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Spacegate arm.duckdb from core.duckdb + cooked catalogs.")
    parser.add_argument("--core-db", required=True, help="Path to core.duckdb")
    parser.add_argument("--arm-db", required=True, help="Path to output arm.duckdb")
    parser.add_argument("--state-dir", required=True, help="Spacegate state dir")
    parser.add_argument("--build-id", required=True, help="Build id")
    parser.add_argument("--ingested-at", required=True, help="Ingest timestamp (UTC)")
    parser.add_argument("--transform-version", required=True, help="Transform/version SHA")
    parser.add_argument("--report-path", default=None, help="Optional arm_report.json output path")
    args = parser.parse_args()

    core_db = Path(args.core_db).resolve()
    arm_db = Path(args.arm_db).resolve()
    state_dir = Path(args.state_dir).resolve()
    cooked_msc = state_dir / "cooked" / "msc" / "msc_components.csv"
    cooked_vsx = state_dir / "cooked" / "vsx" / "vsx_variability.csv"
    cooked_ultracoolsheet = state_dir / "cooked" / "ultracoolsheet" / "ultracoolsheet_objects.csv"
    manifest_dir = state_dir / "reports" / "manifests"
    msc_manifest_path = manifest_dir / "msc_manifest.json"
    vsx_manifest_path = manifest_dir / "vsx_manifest.json"
    ultracoolsheet_manifest_path = manifest_dir / "ultracoolsheet_manifest.json"
    core_manifest_path = manifest_dir / "core_manifest.json"
    enable_vsx = parse_bool_env("SPACEGATE_ENABLE_VSX", True)
    enable_ultracoolsheet = parse_bool_env("SPACEGATE_ENABLE_ULTRACOOLSHEET", True)

    if not core_db.exists():
        raise SystemExit(f"Core DB not found: {core_db}")

    arm_db.parent.mkdir(parents=True, exist_ok=True)
    if arm_db.exists():
        arm_db.unlink()

    msc_manifest = load_manifest_entry(msc_manifest_path, "newmsc_20240101")
    if not msc_manifest:
        msc_manifest = load_manifest_entry(core_manifest_path, "msc")
    vsx_manifest = load_manifest_entry(vsx_manifest_path, "vsx_dat")
    ultracoolsheet_manifest = load_manifest_entry(
        ultracoolsheet_manifest_path, "UltracoolSheet_Main"
    )
    msc_version = str(msc_manifest.get("source_version") or MSC_VERSION_FALLBACK)
    msc_checksum = str(msc_manifest.get("sha256") or "")
    msc_retrieved = str(msc_manifest.get("retrieved_at") or "")
    vsx_version = str(vsx_manifest.get("source_version") or VSX_VERSION_FALLBACK)
    vsx_checksum = str(vsx_manifest.get("sha256") or "")
    vsx_retrieved = str(vsx_manifest.get("retrieved_at") or "")
    vsx_url = str(vsx_manifest.get("url") or VSX_URL_FALLBACK)
    ultracoolsheet_version = str(
        ultracoolsheet_manifest.get("source_version") or ULTRACOOLSHEET_VERSION_FALLBACK
    )
    ultracoolsheet_checksum = str(ultracoolsheet_manifest.get("sha256") or "")
    ultracoolsheet_retrieved = str(ultracoolsheet_manifest.get("retrieved_at") or "")
    ultracoolsheet_url = str(ultracoolsheet_manifest.get("url") or ULTRACOOLSHEET_URL_FALLBACK)

    log(f"Arm build start (build_id={args.build_id}, core_db={core_db}, arm_db={arm_db})")
    con = duckdb.connect(str(arm_db))
    con.execute(f"ATTACH {sql_literal(str(core_db))} AS core (READ_ONLY)")
    con.execute("SET preserve_insertion_order=false")
    threads_env = (os.getenv("SPACEGATE_DUCKDB_THREADS") or "").strip()
    if threads_env:
        try:
            threads = max(1, int(threads_env))
            con.execute(f"SET threads TO {threads}")
            log(f"Arm build: DuckDB threads set to {threads}")
        except Exception:
            log(f"Arm build: ignored invalid SPACEGATE_DUCKDB_THREADS={threads_env!r}")
    memory_limit_env = (os.getenv("SPACEGATE_DUCKDB_MEMORY_LIMIT") or "").strip()
    if memory_limit_env:
        try:
            con.execute(f"SET memory_limit={sql_literal(memory_limit_env)}")
            log(f"Arm build: DuckDB memory_limit set to {memory_limit_env}")
        except Exception:
            log(f"Arm build: ignored invalid SPACEGATE_DUCKDB_MEMORY_LIMIT={memory_limit_env!r}")

    if cooked_msc.exists():
        con.execute(
            f"""
            create or replace temp view msc_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_msc))},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
            """
        )
    else:
        con.execute(
            """
            create or replace temp view msc_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              wds_id, ra_deg, dec_deg, parallax_mas, parallax_ref, pm_ra_mas_yr, pm_dec_mas_yr,
              radial_velocity_kms, component, sep_arcsec, spectral_type_raw, hip_id, hd_id, bmag, vmag, imag,
              jmag, hmag, kmag, ncomp, grade, other_identifiers, subsystem_count, orbit_count
            )
            where false
            """
        )

    if enable_vsx and cooked_vsx.exists():
        con.execute(
            f"""
            create or replace temp view vsx_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_vsx))},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
            """
        )
    else:
        con.execute(
            """
            create or replace temp view vsx_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              vsx_oid, name, variability_flag, ra_deg, dec_deg, variability_type_raw, variability_family, max_mag,
              max_passband, min_is_amplitude_flag, min_mag_or_amplitude, min_passband, epoch_hjd, period_days,
              spectral_type, gaia_source_id, gaia_release
            )
            where false
            """
        )

    if enable_ultracoolsheet and cooked_ultracoolsheet.exists():
        con.execute(
            f"""
            create or replace temp view ultracoolsheet_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_ultracoolsheet))},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
            """
        )
    else:
        con.execute(
            """
            create or replace temp view ultracoolsheet_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              source_row_num, object_name, name_simbadable, gaia_dr3_source_id, gaia_dr2_source_id, ra_j2000_deg,
              dec_j2000_deg, plx_mas, pmra_mas_yr, pmdec_mas_yr, rv_kms, dist_pc, dist_source, spectral_type_opt,
              spectral_type_ir, spectral_numeric, gravity_opt, gravity_ir, age_category, youth_evidence,
              banyan_hypothesis_young, banyan_prob_young, is_exoplanet_host_flag, multiple_unresolved_flag,
              multiple_resolved_flag, has_higher_mass_companion_flag, ref_discovery, source_url
            )
            where false
            """
        )

    con.execute("create table build_metadata as select * from core.build_metadata")
    con.execute(
        f"""
        insert into build_metadata values
          ('arm_build_id', {sql_literal(args.build_id)}),
          ('arm_generated_at', {sql_literal(dt.datetime.now(dt.UTC).isoformat(timespec='seconds').replace('+00:00', 'Z'))}),
          ('arm_transform_version', {sql_literal(args.transform_version)}),
          ('arm_source_core_db', {sql_literal(str(core_db))}),
          ('arm_source_msc_csv', {sql_literal(str(cooked_msc) if cooked_msc.exists() else '')}),
          ('arm_source_msc_version', {sql_literal(msc_version)}),
          ('arm_source_vsx_csv', {sql_literal(str(cooked_vsx) if cooked_vsx.exists() else '')}),
          ('arm_source_vsx_version', {sql_literal(vsx_version)}),
          ('arm_source_vsx_enabled', {sql_literal("1" if enable_vsx else "0")}),
          ('arm_source_ultracoolsheet_csv', {sql_literal(str(cooked_ultracoolsheet) if cooked_ultracoolsheet.exists() else '')}),
          ('arm_source_ultracoolsheet_version', {sql_literal(ultracoolsheet_version)}),
          ('arm_source_ultracoolsheet_enabled', {sql_literal("1" if enable_ultracoolsheet else "0")})
        """
    )

    stage_started = time.monotonic()
    log("Arm stage: creating msc_system_roots")
    con.execute(
        """
        create temp table msc_system_roots as
        with core_wds_names as (
          select wds_id, min(system_name) as system_name
          from core.systems
          where wds_id is not null and trim(wds_id) <> ''
          group by wds_id
        ), parsed as (
          select
            nullif(wds_id, '') as wds_id,
            nullif(other_identifiers, '') as other_identifiers,
            cast(nullif(subsystem_count, '') as bigint) as subsystem_count
          from msc_raw
          where nullif(wds_id, '') is not null
        ), grouped as (
          select
            p.wds_id,
            max(coalesce(p.subsystem_count, 0)) as subsystem_count,
            max(
              case
                when p.other_identifiers is not null and lower(p.other_identifiers) like '%castor%' then 'Castor'
                else null
              end
            ) as hardcoded_name
          from parsed p
          group by p.wds_id
        )
        select
          g.wds_id,
          coalesce(g.hardcoded_name, c.system_name, 'WDS ' || g.wds_id) as system_display_name,
          least(greatest(g.subsystem_count, 0), 52) as subsystem_count
        from grouped g
        left join core_wds_names c on c.wds_id = g.wds_id
        where g.subsystem_count >= 2
        """
    )
    log(f"Arm stage complete: msc_system_roots ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating msc_inferred_leaves")
    con.execute(
        """
        create temp table msc_inferred_leaves as
        with expanded as (
          select
            r.wds_id,
            r.system_display_name,
            seq.i as ordinal
          from msc_system_roots r
          join range(1, 53) as seq(i) on seq.i <= r.subsystem_count
        )
        select
          wds_id,
          system_display_name,
          ordinal,
          lower(chr(64 + cast(ceil(ordinal / 2.0) as integer)) || case when ordinal % 2 = 1 then 'a' else 'b' end) as component_label,
          lower(chr(64 + cast(ceil(ordinal / 2.0) as integer))) as component_stem
        from expanded
        """
    )
    log(f"Arm stage complete: msc_inferred_leaves ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating component_entities")
    con.execute(
        f"""
        create table component_entities as
        with core_system_components as (
          select
            'comp:system:' || s.stable_object_key as stable_component_key,
            'system'::varchar as component_type,
            'system'::varchar as core_object_type,
            s.system_id::bigint as core_object_id,
            s.system_name as display_name,
            cast(null as varchar) as catalog_component_label,
            s.ra_deg::double as ra_deg,
            s.dec_deg::double as dec_deg,
            case when s.dist_ly is not null then s.dist_ly / {3.26156} else null end as dist_pc,
            s.source_catalog as source_catalog,
            s.source_version as source_version,
            cast(s.source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            s.retrieval_checksum as retrieval_checksum,
            s.retrieved_at as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from core.systems s
        ), core_star_components as (
          select
            'comp:star:' || s.stable_object_key as stable_component_key,
            coalesce(s.object_type, 'star')::varchar as component_type,
            'star'::varchar as core_object_type,
            s.star_id::bigint as core_object_id,
            s.star_name as display_name,
            nullif(lower(regexp_replace(coalesce(s.component, ''), '[^0-9A-Za-z]+', '', 'g')), '') as catalog_component_label,
            s.ra_deg::double as ra_deg,
            s.dec_deg::double as dec_deg,
            case when s.dist_ly is not null then s.dist_ly / {3.26156} else null end as dist_pc,
            s.source_catalog as source_catalog,
            s.source_version as source_version,
            cast(s.source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            s.retrieval_checksum as retrieval_checksum,
            s.retrieved_at as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from core.stars s
        ), msc_system_components as (
          select
            'comp:msc_system:wds:' || r.wds_id as stable_component_key,
            'system'::varchar as component_type,
            cast(null as varchar) as core_object_type,
            cast(null as bigint) as core_object_id,
            r.system_display_name as display_name,
            cast(null as varchar) as catalog_component_label,
            cast(null as double) as ra_deg,
            cast(null as double) as dec_deg,
            cast(null as double) as dist_pc,
            'msc'::varchar as source_catalog,
            {sql_literal(msc_version)} as source_version,
            r.wds_id as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(msc_checksum)} as retrieval_checksum,
            {sql_literal(msc_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from msc_system_roots r
        ), msc_leaf_components as (
          select
            'comp:msc:wds:' || l.wds_id || ':' || l.component_label as stable_component_key,
            'star'::varchar as component_type,
            cast(null as varchar) as core_object_type,
            cast(null as bigint) as core_object_id,
            l.system_display_name || ' ' || upper(l.component_label) as display_name,
            l.component_label as catalog_component_label,
            cast(null as double) as ra_deg,
            cast(null as double) as dec_deg,
            cast(null as double) as dist_pc,
            'msc'::varchar as source_catalog,
            {sql_literal(msc_version)} as source_version,
            l.wds_id || ':' || l.component_label as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(msc_checksum)} as retrieval_checksum,
            {sql_literal(msc_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from msc_inferred_leaves l
        ), unioned as (
          select * from core_system_components
          union all
          select * from core_star_components
          union all
          select * from msc_system_components
          union all
          select * from msc_leaf_components
        )
        select
          row_number() over ()::bigint as component_entity_id,
          stable_component_key,
          component_type,
          core_object_type,
          core_object_id,
          display_name,
          catalog_component_label,
          ra_deg,
          dec_deg,
          dist_pc,
          source_catalog,
          source_version,
          source_pk,
          source_row_hash,
          retrieval_checksum,
          retrieved_at,
          ingested_at,
          transform_version
        from unioned
        """
    )
    log(f"Arm stage complete: component_entities ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating system_hierarchy_edges")
    con.execute(
        f"""
        create table system_hierarchy_edges as
        with core_edges as (
          select
            'comp:system:' || sys.stable_object_key as parent_component_key,
            'comp:star:' || st.stable_object_key as child_component_key,
            'contains'::varchar as edge_kind,
            nullif(lower(regexp_replace(coalesce(st.component, ''), '[^0-9A-Za-z]+', '', 'g')), '') as member_role,
            cast(null as varchar) as catalog_relation_label,
            1::int as depth_hint,
            coalesce(sys.grouping_confidence, st.multiplicity_match_confidence, 0.82)::double as confidence_score,
            case
              when coalesce(sys.grouping_confidence, st.multiplicity_match_confidence, 0.82) >= 0.95 then 'high'
              when coalesce(sys.grouping_confidence, st.multiplicity_match_confidence, 0.82) >= 0.80 then 'medium'
              when coalesce(sys.grouping_confidence, st.multiplicity_match_confidence, 0.82) >= 0.60 then 'low'
              else 'illustrative'
            end as confidence_tier,
            coalesce(st.multiplicity_source_catalogs_json, '["core"]') as evidence_catalogs_json,
            json_object('system_id', sys.system_id, 'star_id', st.star_id) as evidence_ids_json,
            st.source_catalog as source_catalog,
            st.source_version as source_version,
            cast(st.source_pk as varchar) as source_pk,
            st.source_row_hash as source_row_hash,
            st.retrieval_checksum as retrieval_checksum,
            st.retrieved_at as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from core.stars st
          join core.systems sys on sys.system_id = st.system_id
        ), msc_edges as (
          select
            'comp:msc_system:wds:' || l.wds_id as parent_component_key,
            'comp:msc:wds:' || l.wds_id || ':' || l.component_label as child_component_key,
            'contains'::varchar as edge_kind,
            l.component_stem as member_role,
            'msc_inferred_subsystem'::varchar as catalog_relation_label,
            1::int as depth_hint,
            0.84::double as confidence_score,
            'medium'::varchar as confidence_tier,
            '["msc"]'::varchar as evidence_catalogs_json,
            json_object('wds_id', l.wds_id, 'component_label', l.component_label) as evidence_ids_json,
            'msc'::varchar as source_catalog,
            {sql_literal(msc_version)} as source_version,
            l.wds_id || ':' || l.component_label as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(msc_checksum)} as retrieval_checksum,
            {sql_literal(msc_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from msc_inferred_leaves l
        ), unioned as (
          select * from core_edges
          union all
          select * from msc_edges
        )
        select
          row_number() over ()::bigint as hierarchy_edge_id,
          parent_component_key,
          child_component_key,
          edge_kind,
          member_role,
          catalog_relation_label,
          depth_hint,
          confidence_score,
          confidence_tier,
          evidence_catalogs_json,
          evidence_ids_json,
          source_catalog,
          source_version,
          source_pk,
          source_row_hash,
          retrieval_checksum,
          retrieved_at,
          ingested_at,
          transform_version
        from unioned
        where parent_component_key <> child_component_key
        """
    )
    log(f"Arm stage complete: system_hierarchy_edges ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating orbit_edges")
    con.execute(
        f"""
        create table orbit_edges as
        with core_labeled as (
          select
            st.system_id,
            sys.stable_object_key as system_key,
            'comp:star:' || st.stable_object_key as component_key,
            lower(regexp_replace(coalesce(st.component, ''), '[^0-9A-Za-z]+', '', 'g')) as component_label,
            coalesce(sys.grouping_confidence, st.multiplicity_match_confidence, 0.82)::double as confidence_score,
            st.source_catalog as source_catalog,
            st.source_version as source_version,
            cast(st.source_pk as varchar) as source_pk,
            st.source_row_hash as source_row_hash,
            st.retrieval_checksum as retrieval_checksum,
            st.retrieved_at as retrieved_at
          from core.stars st
          join core.systems sys on sys.system_id = st.system_id
          where st.component is not null
        ), core_pairs as (
          select
            'comp:system:' || a.system_key as host_component_key,
            a.component_key as primary_component_key,
            b.component_key as secondary_component_key,
            'binary'::varchar as relation_kind,
            'bary:center:system:' || a.system_key || ':' || left(a.component_label, 1) as barycenter_key,
            cast(null as bigint) as preferred_solution_id,
            greatest(a.confidence_score, b.confidence_score) as confidence_score,
            case
              when greatest(a.confidence_score, b.confidence_score) >= 0.95 then 'high'
              when greatest(a.confidence_score, b.confidence_score) >= 0.80 then 'medium'
              when greatest(a.confidence_score, b.confidence_score) >= 0.60 then 'low'
              else 'illustrative'
            end as confidence_tier,
            '["core"]'::varchar as evidence_catalogs_json,
            json_object('system_id', a.system_id, 'component_stem', left(a.component_label, 1)) as evidence_ids_json,
            coalesce(a.source_catalog, b.source_catalog, 'core') as source_catalog,
            coalesce(a.source_version, b.source_version, {sql_literal(args.transform_version)}) as source_version,
            coalesce(a.source_pk, b.source_pk, a.system_key) as source_pk,
            coalesce(a.source_row_hash, b.source_row_hash) as source_row_hash,
            coalesce(a.retrieval_checksum, b.retrieval_checksum, '') as retrieval_checksum,
            coalesce(a.retrieved_at, b.retrieved_at, '') as retrieved_at
          from core_labeled a
          join core_labeled b
            on a.system_id = b.system_id
           and length(a.component_label) = 2
           and length(b.component_label) = 2
           and right(a.component_label, 1) = 'a'
           and right(b.component_label, 1) = 'b'
           and left(a.component_label, 1) = left(b.component_label, 1)
           and a.component_key < b.component_key
        ), msc_pairs as (
          select
            'comp:msc_system:wds:' || a.wds_id as host_component_key,
            'comp:msc:wds:' || a.wds_id || ':' || a.component_label as primary_component_key,
            'comp:msc:wds:' || b.wds_id || ':' || b.component_label as secondary_component_key,
            'binary'::varchar as relation_kind,
            'bary:center:msc:' || a.wds_id || ':' || a.component_stem as barycenter_key,
            cast(null as bigint) as preferred_solution_id,
            0.84::double as confidence_score,
            'medium'::varchar as confidence_tier,
            '["msc"]'::varchar as evidence_catalogs_json,
            json_object('wds_id', a.wds_id, 'component_stem', a.component_stem) as evidence_ids_json,
            'msc'::varchar as source_catalog,
            {sql_literal(msc_version)} as source_version,
            a.wds_id || ':' || a.component_stem || 'ab' as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(msc_checksum)} as retrieval_checksum,
            {sql_literal(msc_retrieved)} as retrieved_at
          from msc_inferred_leaves a
          join msc_inferred_leaves b
            on a.wds_id = b.wds_id
           and a.component_stem = b.component_stem
           and right(a.component_label, 1) = 'a'
           and right(b.component_label, 1) = 'b'
           and a.component_label < b.component_label
        ), unioned as (
          select * from core_pairs
          union all
          select * from msc_pairs
        ), deduped as (
          select *
          from (
            select *,
              row_number() over (
                partition by host_component_key, primary_component_key, secondary_component_key, relation_kind
                order by confidence_score desc, source_catalog asc, source_pk asc
              ) as rn
            from unioned
          ) q
          where rn = 1
        )
        select
          row_number() over (order by host_component_key, primary_component_key, secondary_component_key)::bigint as orbit_edge_id,
          host_component_key,
          primary_component_key,
          secondary_component_key,
          relation_kind,
          barycenter_key,
          preferred_solution_id,
          confidence_score,
          confidence_tier,
          evidence_catalogs_json,
          evidence_ids_json,
          source_catalog,
          source_version,
          source_pk,
          source_row_hash,
          retrieval_checksum,
          retrieved_at,
          {sql_literal(args.ingested_at)} as ingested_at,
          {sql_literal(args.transform_version)} as transform_version
        from deduped
        """
    )
    log(f"Arm stage complete: orbit_edges ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating orbital_solutions")
    con.execute(
        """
        create table orbital_solutions as
        select *
        from (
          values
            (
              cast(null as bigint), cast(null as bigint), cast(null as varchar), cast(null as integer),
              cast(null as double), cast(null as double), cast(null as double), cast(null as double),
              cast(null as double), cast(null as double), cast(null as double), cast(null as double),
              cast(null as double), cast(null as double), cast(null as double), cast(null as double),
              cast(null as double), cast(null as double), cast(null as double), cast(null as double),
              cast(null as double), cast(null as double), cast(null as varchar), cast(null as varchar),
              cast(null as double), cast(null as varchar), cast(null as varchar), cast(null as varchar),
              cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
              cast(null as varchar), cast(null as varchar)
            )
        ) as t(
          orbital_solution_id, orbit_edge_id, solution_source_catalog, solution_rank,
          reference_epoch_jyear, reference_epoch_mjd, period_days, semi_major_axis_au,
          semi_major_axis_arcsec, eccentricity, inclination_deg, longitude_ascending_node_deg,
          argument_periastron_deg, time_periastron_jd, mean_anomaly_deg, mass_ratio_q,
          primary_mass_msun, secondary_mass_msun, rv_semiamplitude_primary_kms, rv_semiamplitude_secondary_kms,
          confidence_score, fit_quality_json, normalization_method, confidence_tier,
          source_catalog, source_version, source_pk, source_row_hash, retrieval_checksum,
          retrieved_at, ingested_at, transform_version
        )
        where false
        """
    )
    log(f"Arm stage complete: orbital_solutions ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating barycenters")
    con.execute(
        """
        create table barycenters as
        select *
        from (
          values
            (
              cast(null as bigint), cast(null as varchar), cast(null as varchar), cast(null as double),
              cast(null as double), cast(null as double), cast(null as double), cast(null as double),
              cast(null as double), cast(null as varchar), cast(null as varchar), cast(null as varchar),
              cast(null as varchar), cast(null as double), cast(null as varchar), cast(null as varchar),
              cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
              cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
            )
        ) as t(
          barycenter_id, barycenter_key, host_component_key, x_helio_pc, y_helio_pc, z_helio_pc,
          vx_helio_kms, vy_helio_kms, vz_helio_kms, mass_basis, mass_estimation_method, mass_input_json,
          reference_epoch_jyear, confidence_score, confidence_tier, source_catalog, source_version, source_pk,
          source_row_hash, retrieval_checksum, retrieved_at, ingested_at, transform_version
        )
        where false
        """
    )
    log(f"Arm stage complete: barycenters ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating animation_readiness")
    con.execute(
        """
        create table animation_readiness as
        select *
        from (
          values
            (
              cast(null as bigint), cast(null as varchar), cast(null as varchar), cast(null as bigint),
              cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as boolean),
              cast(null as varchar), cast(null as varchar), cast(null as varchar)
            )
        ) as t(
          animation_readiness_id, stable_object_key, component_key, orbit_edge_id, readiness_level,
          missing_parameters_json, inferred_parameters_json, disallowed_fabrication, notes_json,
          computed_at, transform_version
        )
        where false
        """
    )
    log(f"Arm stage complete: animation_readiness ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating system_neighbors")
    con.execute(
        """
        create table system_neighbors as
        select *
        from (
          values
            (
              cast(null as bigint), cast(null as varchar), cast(null as varchar), cast(null as double),
              cast(null as integer), cast(null as varchar), cast(null as double), cast(null as varchar),
              cast(null as varchar)
            )
        ) as t(
          neighbor_id, source_system_key, neighbor_system_key, distance_ly, rank, method,
          confidence_score, computed_at, transform_version
        )
        where false
        """
    )
    log(f"Arm stage complete: system_neighbors ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating vsx_variability")
    con.execute(
        f"""
        create table vsx_variability as
        with vsx_typed as (
          select
            cast(nullif(vsx_oid, '') as bigint) as vsx_oid,
            nullif(name, '') as vsx_name,
            cast(nullif(variability_flag, '') as integer) as variability_flag,
            cast(nullif(ra_deg, '') as double) as ra_deg,
            cast(nullif(dec_deg, '') as double) as dec_deg,
            nullif(variability_type_raw, '') as variability_type_raw,
            nullif(variability_family, '') as variability_family,
            cast(nullif(max_mag, '') as double) as max_mag,
            nullif(max_passband, '') as max_passband,
            nullif(min_is_amplitude_flag, '') as min_is_amplitude_flag,
            cast(nullif(min_mag_or_amplitude, '') as double) as min_mag_or_amplitude,
            nullif(min_passband, '') as min_passband,
            cast(nullif(epoch_hjd, '') as double) as epoch_hjd,
            cast(nullif(period_days, '') as double) as period_days,
            nullif(spectral_type, '') as spectral_type,
            cast(nullif(gaia_source_id, '') as bigint) as gaia_source_id,
            nullif(gaia_release, '') as gaia_release
          from vsx_raw
          where cast(nullif(gaia_source_id, '') as bigint) is not null
        ), matched as (
          select
            s.stable_object_key,
            s.star_id,
            s.gaia_id,
            v.*
          from vsx_typed v
          join core.stars s on s.gaia_id = v.gaia_source_id
        )
        select
          row_number() over (
            order by stable_object_key, coalesce(vsx_oid, 0), coalesce(vsx_name, '')
          )::bigint as vsx_variability_id,
          stable_object_key,
          star_id,
          gaia_id,
          vsx_oid,
          vsx_name,
          variability_flag,
          case
            when variability_flag = 0 then 'variable'
            when variability_flag = 1 then 'suspected'
            when variability_flag = 2 then 'constant_or_nonexisting'
            when variability_flag = 3 then 'possible_duplicate'
            else 'unknown'
          end as variability_flag_label,
          variability_type_raw,
          coalesce(variability_family, 'unknown') as variability_family,
          max_mag,
          max_passband,
          min_is_amplitude_flag,
          min_mag_or_amplitude,
          min_passband,
          case
            when upper(coalesce(min_is_amplitude_flag, '')) = 'Y'
              then min_mag_or_amplitude
            when min_mag_or_amplitude is not null and max_mag is not null
              then min_mag_or_amplitude - max_mag
            else null
          end as amplitude_mag,
          epoch_hjd,
          period_days,
          spectral_type,
          case
            when variability_flag = 0 then 0.95
            when variability_flag = 1 then 0.75
            when variability_flag = 2 then 0.40
            when variability_flag = 3 then 0.30
            else 0.20
          end as confidence_score,
          case
            when variability_flag = 0 then 'high'
            when variability_flag = 1 then 'medium'
            when variability_flag in (2, 3) then 'low'
            else 'illustrative'
          end as confidence_tier,
          coalesce(variability_flag, 9) in (0, 1) as is_default_usable,
          case
            when coalesce(variability_flag, 9) in (0, 1) and (
              coalesce(
                case
                  when upper(coalesce(min_is_amplitude_flag, '')) = 'Y'
                    then min_mag_or_amplitude
                  when min_mag_or_amplitude is not null and max_mag is not null
                    then min_mag_or_amplitude - max_mag
                  else null
                end,
                0.0
              ) >= 1.0
              or (
                coalesce(variability_family, 'unknown') = 'eruptive'
                and coalesce(
                  case
                    when upper(coalesce(min_is_amplitude_flag, '')) = 'Y'
                      then min_mag_or_amplitude
                    when min_mag_or_amplitude is not null and max_mag is not null
                      then min_mag_or_amplitude - max_mag
                    else null
                  end,
                  0.0
                ) >= 0.5
              )
            ) then true
            else false
          end as is_high_variability,
          'vsx' as source_catalog,
          {sql_literal(vsx_version)} as source_version,
          {sql_literal(vsx_url)} as source_url,
          cast(vsx_oid as varchar) as source_pk,
          cast(null as varchar) as source_row_hash,
          {sql_literal(vsx_checksum)} as retrieval_checksum,
          {sql_literal(vsx_retrieved)} as retrieved_at,
          {sql_literal(args.ingested_at)} as ingested_at,
          {sql_literal(args.transform_version)} as transform_version
        from matched
        """
    )
    log(f"Arm stage complete: vsx_variability ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating variability_summary")
    con.execute(
        """
        create table variability_summary as
        with ranked as (
          select
            v.*,
            count(*) over (partition by stable_object_key) as vsx_match_count,
            max(case when is_high_variability then 1 else 0 end) over (partition by stable_object_key) as any_high_variability_int,
            row_number() over (
              partition by stable_object_key
              order by
                case coalesce(variability_flag, 9)
                  when 0 then 0
                  when 1 then 1
                  when 2 then 2
                  when 3 then 3
                  else 9
                end asc,
                coalesce(amplitude_mag, -1.0) desc,
                coalesce(period_days, 1e99) asc,
                coalesce(vsx_oid, 0) asc
            ) as rn
          from vsx_variability v
        )
        select
          row_number() over (order by stable_object_key)::bigint as variability_summary_id,
          stable_object_key,
          star_id,
          gaia_id,
          vsx_match_count,
          variability_flag as primary_variability_flag,
          variability_flag_label as primary_variability_flag_label,
          variability_type_raw as primary_variability_type_raw,
          variability_family as primary_variability_family,
          amplitude_mag as primary_amplitude_mag,
          period_days as primary_period_days,
          epoch_hjd as primary_epoch_hjd,
          is_default_usable as primary_is_default_usable,
          any_high_variability_int = 1 as any_high_variability,
          confidence_score,
          confidence_tier,
          source_catalog,
          source_version,
          source_pk,
          source_row_hash,
          retrieval_checksum,
          retrieved_at,
          ingested_at,
          transform_version
        from ranked
        where rn = 1
        """
    )
    log(f"Arm stage complete: variability_summary ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating ultracoolsheet_objects")
    con.execute(
        f"""
        create table ultracoolsheet_objects as
        with typed as (
          select
            cast(nullif(source_row_num, '') as bigint) as source_row_num,
            nullif(object_name, '') as object_name,
            nullif(name_simbadable, '') as name_simbadable,
            cast(nullif(gaia_dr3_source_id, '') as bigint) as gaia_dr3_source_id,
            cast(nullif(gaia_dr2_source_id, '') as bigint) as gaia_dr2_source_id,
            cast(nullif(ra_j2000_deg, '') as double) as ra_j2000_deg,
            cast(nullif(dec_j2000_deg, '') as double) as dec_j2000_deg,
            cast(nullif(plx_mas, '') as double) as plx_mas,
            cast(nullif(pmra_mas_yr, '') as double) as pmra_mas_yr,
            cast(nullif(pmdec_mas_yr, '') as double) as pmdec_mas_yr,
            cast(nullif(rv_kms, '') as double) as rv_kms,
            cast(nullif(dist_pc, '') as double) as dist_pc,
            nullif(dist_source, '') as dist_source,
            nullif(spectral_type_opt, '') as spectral_type_opt,
            nullif(spectral_type_ir, '') as spectral_type_ir,
            cast(nullif(spectral_numeric, '') as double) as spectral_numeric,
            nullif(gravity_opt, '') as gravity_opt,
            nullif(gravity_ir, '') as gravity_ir,
            nullif(age_category, '') as age_category,
            nullif(youth_evidence, '') as youth_evidence,
            nullif(banyan_hypothesis_young, '') as banyan_hypothesis_young,
            cast(nullif(banyan_prob_young, '') as double) as banyan_prob_young,
            upper(coalesce(nullif(is_exoplanet_host_flag, ''), 'N')) in ('Y', 'YES', 'TRUE', '1') as is_exoplanet_host,
            upper(coalesce(nullif(multiple_unresolved_flag, ''), 'N')) in ('Y', 'YES', 'TRUE', '1') as has_unresolved_multiplicity,
            upper(coalesce(nullif(multiple_resolved_flag, ''), 'N')) in ('Y', 'YES', 'TRUE', '1') as has_resolved_multiplicity,
            upper(coalesce(nullif(has_higher_mass_companion_flag, ''), 'N')) in ('Y', 'YES', 'TRUE', '1') as has_higher_mass_companion,
            nullif(ref_discovery, '') as ref_discovery,
            nullif(source_url, '') as source_url
          from ultracoolsheet_raw
        ), matched as (
          select
            t.*,
            s.stable_object_key,
            s.star_id,
            s.gaia_id as matched_gaia_id,
            case
              when t.gaia_dr3_source_id is not null and s.gaia_id = t.gaia_dr3_source_id then 1.0
              when t.gaia_dr2_source_id is not null and s.gaia_id = t.gaia_dr2_source_id then 0.92
              else 0.0
            end as match_confidence
          from typed t
          left join core.stars s
            on s.gaia_id = coalesce(t.gaia_dr3_source_id, t.gaia_dr2_source_id)
        )
        select
          row_number() over (order by coalesce(stable_object_key, ''), source_row_num)::bigint as ultracoolsheet_object_id,
          stable_object_key,
          star_id,
          matched_gaia_id as gaia_id,
          gaia_dr3_source_id,
          gaia_dr2_source_id,
          object_name,
          name_simbadable,
          ra_j2000_deg as ra_deg,
          dec_j2000_deg as dec_deg,
          plx_mas,
          pmra_mas_yr,
          pmdec_mas_yr,
          rv_kms,
          dist_pc,
          dist_source,
          spectral_type_opt,
          spectral_type_ir,
          spectral_numeric,
          gravity_opt,
          gravity_ir,
          age_category,
          youth_evidence,
          banyan_hypothesis_young,
          banyan_prob_young,
          is_exoplanet_host,
          has_unresolved_multiplicity,
          has_resolved_multiplicity,
          has_higher_mass_companion,
          match_confidence,
          case
            when match_confidence >= 0.99 then 'high'
            when match_confidence >= 0.90 then 'medium'
            when match_confidence > 0 then 'low'
            else 'illustrative'
          end as confidence_tier,
          ref_discovery,
          coalesce(source_url, {sql_literal(ultracoolsheet_url)}) as source_url,
          'ultracoolsheet' as source_catalog,
          {sql_literal(ultracoolsheet_version)} as source_version,
          cast(source_row_num as varchar) as source_pk,
          cast(null as varchar) as source_row_hash,
          {sql_literal(ultracoolsheet_checksum)} as retrieval_checksum,
          {sql_literal(ultracoolsheet_retrieved)} as retrieved_at,
          {sql_literal(args.ingested_at)} as ingested_at,
          {sql_literal(args.transform_version)} as transform_version
        from matched
        """
    )
    log(f"Arm stage complete: ultracoolsheet_objects ({time.monotonic() - stage_started:.1f}s)")

    component_count = int(con.execute("select count(*) from component_entities").fetchone()[0] or 0)
    hierarchy_count = int(con.execute("select count(*) from system_hierarchy_edges").fetchone()[0] or 0)
    orbit_count = int(con.execute("select count(*) from orbit_edges").fetchone()[0] or 0)
    vsx_variability_count = int(con.execute("select count(*) from vsx_variability").fetchone()[0] or 0)
    variability_summary_count = int(con.execute("select count(*) from variability_summary").fetchone()[0] or 0)
    vsx_high_variability_count = int(
        con.execute(
            "select count(*) from variability_summary where any_high_variability"
        ).fetchone()[0]
        or 0
    )
    ultracoolsheet_count = int(con.execute("select count(*) from ultracoolsheet_objects").fetchone()[0] or 0)
    ultracoolsheet_matched_count = int(
        con.execute(
            "select count(*) from ultracoolsheet_objects where stable_object_key is not null"
        ).fetchone()[0]
        or 0
    )
    inferred_leaf_count = int(con.execute("select count(*) from msc_inferred_leaves").fetchone()[0] or 0)
    inferred_root_count = int(con.execute("select count(*) from msc_system_roots").fetchone()[0] or 0)
    castor_leaf_count = int(
        con.execute(
            """
            select count(*)
            from component_entities
            where lower(coalesce(display_name, '')) like 'castor %'
              and lower(coalesce(catalog_component_label, '')) in ('aa','ab','ba','bb','ca','cb')
            """
        ).fetchone()[0]
        or 0
    )
    castor_pair_count = int(
        con.execute(
            """
            select count(*)
            from orbit_edges
            where (
              lower(primary_component_key) = 'comp:msc:wds:07346+3153:aa'
              and lower(secondary_component_key) = 'comp:msc:wds:07346+3153:ab'
            ) or (
              lower(primary_component_key) = 'comp:msc:wds:07346+3153:ba'
              and lower(secondary_component_key) = 'comp:msc:wds:07346+3153:bb'
            ) or (
              lower(primary_component_key) = 'comp:msc:wds:07346+3153:ca'
              and lower(secondary_component_key) = 'comp:msc:wds:07346+3153:cb'
            )
            """
        ).fetchone()[0]
        or 0
    )

    report = {
        "build_id": args.build_id,
        "generated_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "core_db_path": str(core_db),
        "arm_db_path": str(arm_db),
        "msc_csv_path": str(cooked_msc) if cooked_msc.exists() else None,
        "counts": {
            "component_entities": component_count,
            "system_hierarchy_edges": hierarchy_count,
            "orbit_edges": orbit_count,
            "vsx_variability_rows": vsx_variability_count,
            "variability_summary_rows": variability_summary_count,
            "variability_summary_high_variability_rows": vsx_high_variability_count,
            "ultracoolsheet_rows": ultracoolsheet_count,
            "ultracoolsheet_matched_rows": ultracoolsheet_matched_count,
            "msc_inferred_system_roots": inferred_root_count,
            "msc_inferred_leaf_components": inferred_leaf_count,
            "castor_expected_leaf_matches": castor_leaf_count,
            "castor_expected_pair_matches": castor_pair_count,
        },
        "notes": [
            "Arm graph includes core system->star containment edges.",
            "MSC subsystem_count inference synthesizes lettered leaf labels (Aa/Ab...) for unresolved hierarchy depth.",
            "Orbit edges currently include core two-letter component pairs and inferred MSC leaf pairs.",
            "VSX variability is stored as arm overlay rows keyed by core stable_object_key via Gaia-ID exact joins.",
            "UltracoolSheet rows are stored in arm and linked to core stars when Gaia IDs align.",
        ],
    }

    con.close()

    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    log(
        "Arm build complete "
        f"(components={component_count:,}, hierarchy_edges={hierarchy_count:,}, orbit_edges={orbit_count:,}, "
        f"vsx_rows={vsx_variability_count:,}, ultracoolsheet_rows={ultracoolsheet_count:,}, "
        f"msc_inferred_leaves={inferred_leaf_count:,})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
