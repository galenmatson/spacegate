from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

TIC_SOURCE_URL = "https://mast.stsci.edu/api/v0/invoke"
TOI_SOURCE_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
TESS_TRANSFORM_VERSION = "tess_identity_evidence_v1"
TIC_ALIAS_PRIORITY = 24
TOI_ALIAS_PRIORITY = 23


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _read_manifest(manifest_path: Path) -> dict[str, dict[str, Any]]:
    if not manifest_path.exists():
        return {}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = payload if isinstance(payload, list) else [payload]
    return {
        str(entry.get("source_name")): entry
        for entry in entries
        if isinstance(entry, dict) and entry.get("source_name")
    }


def _require_inputs(cooked_dir: Path) -> dict[str, Path]:
    paths = {
        "targets": cooked_dir / "target_tic_ids.csv",
        "tic": cooked_dir / "targeted_tic.csv",
        "neighbours": cooked_dir / "gaia_dr2_neighbourhood.csv",
        "gaia_dr3": cooked_dir / "gaia_dr3_targets.csv",
        "external": cooked_dir / "gaia_external_crossmatches.csv",
        "toi": cooked_dir / "toi.csv",
        "toi_history": cooked_dir / "toi_disposition_history.csv",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing cooked TESS evidence inputs: " + ", ".join(missing))
    return paths


def create_input_views(con: duckdb.DuckDBPyConnection, cooked_dir: Path) -> dict[str, Path]:
    paths = _require_inputs(cooked_dir)
    con.execute(
        f"create or replace temp view tess_target_input as select * from read_csv({sql_literal(str(paths['targets']))}, header=true, all_varchar=true)"
    )
    con.execute(
        f"create or replace temp view tess_tic_input as select * from read_csv({sql_literal(str(paths['tic']))}, header=true, all_varchar=true)"
    )
    con.execute(
        f"create or replace temp view tess_dr2_neighbourhood_input as select * from read_csv({sql_literal(str(paths['neighbours']))}, header=true, all_varchar=true)"
    )
    con.execute(
        f"create or replace temp view tess_gaia_dr3_input as select * from read_csv({sql_literal(str(paths['gaia_dr3']))}, header=true, all_varchar=true)"
    )
    con.execute(
        f"create or replace temp view tess_gaia_external_input as select * from read_csv({sql_literal(str(paths['external']))}, header=true, all_varchar=true)"
    )
    con.execute(
        f"create or replace temp view tess_toi_input as select * from read_csv({sql_literal(str(paths['toi']))}, header=true, all_varchar=true)"
    )
    con.execute(
        f"create or replace temp view tess_toi_history_input as select * from read_csv({sql_literal(str(paths['toi_history']))}, header=true, all_varchar=true)"
    )
    return paths


def create_resolution_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace temp table tess_target_typed as
        select
          try_cast(t.tic_id as bigint) as tic_id,
          nullif(t.source_families, '') as source_families,
          nullif(i.tic_version, '') as tic_version,
          try_cast(nullif(i.gaia_dr2_id, '') as bigint) as gaia_dr2_id,
          try_cast(nullif(i.hip_id, '') as bigint) as hip_id,
          nullif(i.tyc_id, '') as tyc_id,
          nullif(i.twomass_id, '') as twomass_id,
          try_cast(nullif(i.ra_deg, '') as double) as ra_deg,
          try_cast(nullif(i.dec_deg, '') as double) as dec_deg,
          try_cast(nullif(i.pm_ra_mas_yr, '') as double) as pm_ra_mas_yr,
          try_cast(nullif(i.pm_dec_mas_yr, '') as double) as pm_dec_mas_yr,
          try_cast(nullif(i.distance_pc, '') as double) as distance_pc,
          upper(coalesce(nullif(i.disposition, ''), '')) as tic_disposition,
          nullif(i.duplicate_id, '') as duplicate_id,
          i.source_row_hash,
          i.tic_id is not null as source_present
        from tess_target_input t
        left join tess_tic_input i using (tic_id)
        """
    )
    con.execute(
        """
        create or replace temp table tess_resolution_candidates as
        select
          t.tic_id, oi.target_id as star_id, 1 as method_priority,
          'existing_accepted_tic'::varchar as resolution_method,
          1.0::double as resolution_confidence,
          json_object('identifier_id', oi.identifier_id) as evidence_json
        from tess_target_typed t
        join object_identifiers oi
          on oi.namespace = 'tic'
         and try_cast(oi.id_value_raw as bigint) = t.tic_id
         and oi.target_type = 'star'
        """
    )
    con.execute(
        """
        insert into tess_resolution_candidates
        select
          t.tic_id, s.star_id, 2,
          'tic_gaia_dr2_neighbourhood_dr3',
          case
            when try_cast(n.angular_distance_arcsec as double) <= 0.1 then 0.99
            when try_cast(n.angular_distance_arcsec as double) <= 0.5 then 0.98
            else 0.97
          end,
          json_object(
            'gaia_dr2_id', t.gaia_dr2_id, 'gaia_dr3_id', s.gaia_id,
            'angular_distance_arcsec', try_cast(n.angular_distance_arcsec as double),
            'magnitude_difference', try_cast(n.magnitude_difference as double),
            'number_of_neighbours', try_cast(n.number_of_neighbours as integer),
            'proper_motion_propagation', nullif(n.proper_motion_propagation, '')
          )
        from tess_target_typed t
        join tess_dr2_neighbourhood_input n
          on try_cast(n.dr2_source_id as bigint) = t.gaia_dr2_id
        join stars s on s.gaia_id = try_cast(n.dr3_source_id as bigint)
        where not exists (select 1 from tess_resolution_candidates c where c.tic_id = t.tic_id)
          and t.tic_disposition not in ('SPLIT', 'DUPLICATE', 'ARTIFACT')
          and t.duplicate_id is null
        """
    )
    con.execute(
        """
        insert into tess_resolution_candidates
        select
          t.tic_id, s.star_id, 3,
          'tic_' || x.namespace || '_gaia_dr3_best_neighbour',
          case when try_cast(x.number_of_neighbours as integer) = 1 then 0.96 else 0.93 end,
          json_object(
            'namespace', x.namespace, 'external_id', x.external_id,
            'gaia_dr3_id', s.gaia_id,
            'angular_distance_arcsec', try_cast(x.angular_distance_arcsec as double),
            'number_of_neighbours', try_cast(x.number_of_neighbours as integer),
            'xm_flag', nullif(x.xm_flag, '')
          )
        from tess_target_typed t
        join tess_gaia_external_input x on (
          (x.namespace = 'hip' and try_cast(x.external_id as bigint) = t.hip_id)
          or (x.namespace = 'tyc' and lower(regexp_replace(x.external_id, '[^0-9A-Za-z]+', '', 'g')) = lower(regexp_replace(t.tyc_id, '[^0-9A-Za-z]+', '', 'g')))
          or (x.namespace = 'twomass' and lower(x.external_id) = lower(t.twomass_id))
        )
        join stars s on s.gaia_id = try_cast(x.dr3_source_id as bigint)
        where not exists (select 1 from tess_resolution_candidates c where c.tic_id = t.tic_id)
          and t.tic_disposition not in ('SPLIT', 'DUPLICATE', 'ARTIFACT')
          and t.duplicate_id is null
        """
    )
    con.execute(
        """
        insert into tess_resolution_candidates
        select t.tic_id, s.star_id, 3, 'tic_hip_exact', 0.96,
          json_object('hip_id', t.hip_id)
        from tess_target_typed t
        join stars s on s.hip_id = t.hip_id
        where t.hip_id is not null
          and not exists (select 1 from tess_resolution_candidates c where c.tic_id = t.tic_id)
          and t.tic_disposition not in ('SPLIT', 'DUPLICATE', 'ARTIFACT')
          and t.duplicate_id is null
        """
    )
    con.execute(
        """
        insert into tess_resolution_candidates
        select t.tic_id, oi.target_id, 3, 'tic_tyc_exact', 0.95,
          json_object('tyc_id', t.tyc_id)
        from tess_target_typed t
        join object_identifiers oi
          on oi.namespace = 'tyc'
         and oi.target_type = 'star'
         and oi.id_value_norm = lower(trim(regexp_replace(regexp_replace(t.tyc_id, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g')))
        where nullif(t.tyc_id, '') is not null
          and not exists (select 1 from tess_resolution_candidates c where c.tic_id = t.tic_id)
          and t.tic_disposition not in ('SPLIT', 'DUPLICATE', 'ARTIFACT')
          and t.duplicate_id is null
        """
    )
    con.execute(
        """
        create or replace temp table tess_positional_targets as
        select
          t.*,
          cast(floor(t.ra_deg * 1000) as bigint) + dra.bin_delta as ra_bin,
          cast(floor(t.dec_deg * 1000) as bigint) + ddec.bin_delta as dec_bin
        from tess_target_typed t
        cross join (values (-1), (0), (1)) dra(bin_delta)
        cross join (values (-1), (0), (1)) ddec(bin_delta)
        where t.ra_deg is not null and t.dec_deg is not null
          and not exists (select 1 from tess_resolution_candidates c where c.tic_id = t.tic_id)
          and t.tic_disposition not in ('SPLIT', 'DUPLICATE', 'ARTIFACT')
          and t.duplicate_id is null
        """
    )
    con.execute(
        """
        insert into tess_resolution_candidates
        select
          t.tic_id, s.star_id, 4, 'tic_position_pm_strict', 0.90,
          json_object(
            'angular_distance_arcsec', 3600.0 * degrees(acos(least(1.0, greatest(-1.0,
              sin(radians(t.dec_deg)) * sin(radians(s.dec_deg)) +
              cos(radians(t.dec_deg)) * cos(radians(s.dec_deg)) * cos(radians(t.ra_deg - s.ra_deg))
            )))),
            'pm_ra_delta_mas_yr', abs(t.pm_ra_mas_yr - s.pm_ra_mas_yr),
            'pm_dec_delta_mas_yr', abs(t.pm_dec_mas_yr - s.pm_dec_mas_yr)
          )
        from tess_positional_targets t
        join stars s
          on cast(floor(s.ra_deg * 1000) as bigint) = t.ra_bin
         and cast(floor(s.dec_deg * 1000) as bigint) = t.dec_bin
        where 3600.0 * degrees(acos(least(1.0, greatest(-1.0,
            sin(radians(t.dec_deg)) * sin(radians(s.dec_deg)) +
            cos(radians(t.dec_deg)) * cos(radians(s.dec_deg)) * cos(radians(t.ra_deg - s.ra_deg))
          )))) <= 2.0
          and (t.pm_ra_mas_yr is null or s.pm_ra_mas_yr is null or abs(t.pm_ra_mas_yr - s.pm_ra_mas_yr) <= 10.0)
          and (t.pm_dec_mas_yr is null or s.pm_dec_mas_yr is null or abs(t.pm_dec_mas_yr - s.pm_dec_mas_yr) <= 10.0)
        """
    )
    con.execute(
        """
        create or replace temp table tess_identity_resolution as
        with target_tic as (
          select * from tess_target_typed
        ), best_priority as (
          select tic_id, min(method_priority) as method_priority
          from tess_resolution_candidates
          group by tic_id
        ), best_candidates as (
          select c.*
          from tess_resolution_candidates c
          join best_priority b using (tic_id, method_priority)
        ), best_summary as (
          select
            tic_id,
            count(distinct star_id) as candidate_star_count,
            min(star_id) as resolved_star_id,
            arg_min(resolution_method, star_id) as resolution_method,
            max(resolution_confidence) as resolution_confidence,
            to_json(list(json_object(
              'star_id', star_id, 'method', resolution_method,
              'confidence', resolution_confidence, 'evidence', evidence_json
            ) order by star_id))::varchar as candidates_json
          from best_candidates
          group by tic_id
        ), neighbour_summary as (
          select
            try_cast(dr2_source_id as bigint) as gaia_dr2_id,
            count(*) as neighbourhood_row_count,
            count(distinct try_cast(dr3_source_id as bigint)) as dr3_candidate_count
          from tess_dr2_neighbourhood_input
          group by 1
        ), gaia_links as (
          select
            t.tic_id, try_cast(n.dr3_source_id as bigint) as dr3_source_id
          from tess_target_typed t
          join tess_dr2_neighbourhood_input n
            on try_cast(n.dr2_source_id as bigint) = t.gaia_dr2_id
          union
          select
            t.tic_id, try_cast(x.dr3_source_id as bigint) as dr3_source_id
          from tess_target_typed t
          join tess_gaia_external_input x on (
            (x.namespace = 'hip' and try_cast(x.external_id as bigint) = t.hip_id)
            or (x.namespace = 'tyc' and lower(regexp_replace(x.external_id, '[^0-9A-Za-z]+', '', 'g')) = lower(regexp_replace(t.tyc_id, '[^0-9A-Za-z]+', '', 'g')))
            or (x.namespace = 'twomass' and lower(x.external_id) = lower(t.twomass_id))
          )
        ), gaia_scope_summary as (
          select
            l.tic_id,
            count(distinct try_cast(g.source_id as bigint)) as gaia_dr3_source_count,
            count(distinct try_cast(g.source_id as bigint)) filter (
              where try_cast(g.parallax_mas as double) >= 3.26156
            ) as gaia_dr3_in_scope_count,
            max(try_cast(g.parallax_mas as double)) as gaia_dr3_max_parallax_mas
          from gaia_links l
          join tess_gaia_dr3_input g
            on try_cast(g.source_id as bigint) = l.dr3_source_id
          group by 1
        )
        select
          t.tic_id,
          t.source_families,
          t.tic_version,
          case
            when not t.source_present then 'source_missing'
            when t.tic_disposition in ('SPLIT', 'DUPLICATE', 'ARTIFACT') or t.duplicate_id is not null then 'excluded'
            when coalesce(b.candidate_star_count, 0) = 1 then 'accepted'
            when coalesce(b.candidate_star_count, 0) > 1 then 'ambiguous'
            else 'missing'
          end as resolution_status,
          case
            when not t.source_present then 'tic_source_missing'
            when t.tic_disposition in ('SPLIT', 'DUPLICATE', 'ARTIFACT') then 'tic_' || lower(t.tic_disposition)
            when t.duplicate_id is not null then 'tic_duplicate_id'
            when coalesce(b.candidate_star_count, 0) = 1 then b.resolution_method
            when coalesce(b.candidate_star_count, 0) > 1 then 'best_precedence_multiple_stars'
            when coalesce(g.gaia_dr3_in_scope_count, 0) > 0 then 'gaia_dr3_not_in_core'
            when coalesce(g.gaia_dr3_source_count, 0) > 0 then 'outside_1000ly_scope'
            when t.distance_pc > 306.601 then 'outside_1000ly_scope'
            when t.gaia_dr2_id is not null then 'gaia_dr2_unmapped_or_absent'
            else 'insufficient_identity_evidence'
          end as resolution_reason,
          case when coalesce(b.candidate_star_count, 0) = 1 then b.resolved_star_id else null end as star_id,
          case when coalesce(b.candidate_star_count, 0) = 1 then s.system_id else null end as system_id,
          case when coalesce(b.candidate_star_count, 0) = 1 then b.resolution_confidence else null end as resolution_confidence,
          t.gaia_dr2_id, t.hip_id, t.tyc_id, t.twomass_id, t.ra_deg, t.dec_deg,
          t.distance_pc, t.tic_disposition, t.duplicate_id,
          coalesce(b.candidate_star_count, 0) as candidate_star_count,
          coalesce(n.neighbourhood_row_count, 0) as neighbourhood_row_count,
          coalesce(n.dr3_candidate_count, 0) as dr3_candidate_count,
          coalesce(g.gaia_dr3_source_count, 0) as gaia_dr3_source_count,
          coalesce(g.gaia_dr3_in_scope_count, 0) as gaia_dr3_in_scope_count,
          g.gaia_dr3_max_parallax_mas,
          coalesce(b.candidates_json, '[]') as candidates_json,
          t.source_row_hash
        from target_tic t
        left join best_summary b using (tic_id)
        left join stars s on s.star_id = b.resolved_star_id
        left join neighbour_summary n using (gaia_dr2_id)
        left join gaia_scope_summary g using (tic_id)
        """
    )


def create_toi_link_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create or replace temp table tess_toi_links as
        with toi_typed as (
          select
            t.*,
            try_cast(t.tic_id as bigint) as tic_id_typed,
            try_cast(nullif(t.orbital_period_days, '') as double) as period_days_typed
          from tess_toi_input t
        ), host_links as (
          select
            t.*, r.star_id, r.system_id, r.resolution_status as host_resolution_status,
            r.resolution_reason as host_resolution_reason,
            r.resolution_confidence as host_resolution_confidence
          from toi_typed t
          left join tess_identity_resolution r on r.tic_id = t.tic_id_typed
        ), planet_candidates as (
          select
            h.source_key, p.planet_id,
            abs(p.orbital_period_days - h.period_days_typed) as period_delta_days,
            count(*) over (partition by h.source_key) as candidate_count,
            row_number() over (
              partition by h.source_key
              order by abs(p.orbital_period_days - h.period_days_typed), p.planet_id
            ) as candidate_rank
          from host_links h
          join planets p
            on p.system_id = h.system_id
           and (p.star_id = h.star_id or p.star_id is null or h.star_id is null)
          where h.disposition in ('CP', 'KP')
            and h.period_days_typed is not null
            and p.orbital_period_days is not null
            and abs(p.orbital_period_days - h.period_days_typed) <= greatest(0.01, h.period_days_typed * 0.001)
        ), planet_best as (
          select source_key, planet_id, period_delta_days, candidate_count
          from planet_candidates
          where candidate_rank = 1 and candidate_count = 1
        )
        select
          h.*,
          p.planet_id,
          p.period_delta_days as planet_period_delta_days,
          case
            when h.disposition in ('CP', 'KP') and p.planet_id is not null then 'host_and_period_unique'
            when h.disposition in ('CP', 'KP') then 'confirmed_planet_unmatched'
            else 'not_applicable'
          end as planet_link_method
        from host_links h
        left join planet_best p using (source_key)
        """
    )


def materialize_core(
    con: duckdb.DuckDBPyConnection,
    *,
    cooked_dir: Path,
    manifest_path: Path,
    report_path: Path | None = None,
    append_search_terms: bool = False,
) -> dict[str, Any]:
    create_input_views(con, cooked_dir)
    create_resolution_tables(con)
    create_toi_link_table(con)
    manifest = _read_manifest(manifest_path)
    tic_manifest = manifest.get("mast_tic_targeted", {})
    toi_manifest = manifest.get("nasa_toi", {})
    retrieved_at = str(tic_manifest.get("retrieved_at") or "")
    tic_version = str(tic_manifest.get("source_version") or "tic_v8_targeted")
    toi_version = str(toi_manifest.get("source_version") or "nasa_toi_tap_snapshot")

    con.execute(
        f"""
        insert into identifier_quarantine (
          quarantine_id, source_catalog, source_version, source_pk, gaia_id,
          hip_id, hd_id, reason, details_json, created_at
        )
        with base as (select coalesce(max(quarantine_id), 0)::bigint as base_id from identifier_quarantine),
        pending as (
          select
            r.*,
            row_number() over (order by r.tic_id) as rn
          from tess_identity_resolution r
          where r.resolution_status in ('ambiguous', 'excluded')
            and not exists (
              select 1 from identifier_quarantine q
              where q.source_catalog = 'mast_tic' and q.source_pk = r.tic_id
            )
        )
        select
          base.base_id + pending.rn, 'mast_tic', {sql_literal(tic_version)}, pending.tic_id,
          null::bigint, pending.hip_id, null::bigint,
          pending.resolution_reason,
          json_object(
            'tic_id', pending.tic_id, 'gaia_dr2_id', pending.gaia_dr2_id,
            'status', pending.resolution_status,
            'tic_disposition', pending.tic_disposition, 'duplicate_id', pending.duplicate_id,
            'candidate_star_count', pending.candidate_star_count,
            'candidates', pending.candidates_json, 'source_row_hash', pending.source_row_hash
          )::varchar,
          {sql_literal(retrieved_at)}
        from pending cross join base
        """
    )
    con.execute(
        f"""
        insert into object_identifiers (
          identifier_id, target_type, target_id, namespace, id_value_raw, id_value_norm,
          is_canonical, resolution_method, resolution_confidence, source_catalog,
          source_version, source_pk, evidence_json
        )
        with base as (select coalesce(max(identifier_id), 0)::bigint as base_id from object_identifiers),
        pending as (
          select
            r.*,
            row_number() over (order by r.star_id, r.tic_id) as rn
          from tess_identity_resolution r
          where r.resolution_status = 'accepted'
            and not exists (
              select 1 from object_identifiers oi
              where oi.namespace = 'tic' and try_cast(oi.id_value_raw as bigint) = r.tic_id
            )
        )
        select
          base.base_id + pending.rn, 'star', pending.star_id, 'tic', pending.tic_id::varchar,
          pending.tic_id::varchar, false, pending.resolution_reason,
          pending.resolution_confidence, 'mast_tic', {sql_literal(tic_version)}, pending.tic_id,
          json_object(
            'tic_id', pending.tic_id, 'gaia_dr2_id', pending.gaia_dr2_id,
            'source_families', pending.source_families, 'candidates', pending.candidates_json,
            'source_row_hash', pending.source_row_hash
          )::varchar
        from pending cross join base
        """
    )
    collision_count = int(con.execute(
        """
        select count(*) from (
          select id_value_norm from object_identifiers where namespace = 'tic'
          group by id_value_norm having count(distinct target_id) > 1
        )
        """
    ).fetchone()[0])
    if collision_count:
        raise RuntimeError(f"TIC identifier collision gate failed: {collision_count} IDs map to multiple stars")

    con.execute(
        f"""
        insert into aliases (
          alias_id, target_type, target_id, system_id, star_id, alias_raw, alias_norm,
          alias_kind, alias_priority, is_primary, source_catalog, source_version, source_pk
        )
        with base as (select coalesce(max(alias_id), 0)::bigint as base_id from aliases),
        pending as (
          select r.*, row_number() over (order by r.star_id, r.tic_id) as rn
          from tess_identity_resolution r
          where r.resolution_status = 'accepted'
            and not exists (
              select 1 from aliases a
              where a.target_type = 'star' and a.target_id = r.star_id
                and a.alias_norm = 'tic ' || r.tic_id::varchar
            )
        )
        select
          base.base_id + pending.rn, 'star', pending.star_id, pending.system_id,
          pending.star_id, 'TIC ' || pending.tic_id::varchar, 'tic ' || pending.tic_id::varchar,
          'tic_id', {TIC_ALIAS_PRIORITY}, false, 'mast_tic', {sql_literal(tic_version)}, pending.tic_id
        from pending cross join base
        """
    )
    con.execute(
        f"""
        insert into aliases (
          alias_id, target_type, target_id, system_id, star_id, alias_raw, alias_norm,
          alias_kind, alias_priority, is_primary, source_catalog, source_version, source_pk
        )
        with base as (select coalesce(max(alias_id), 0)::bigint as base_id from aliases),
        candidates as (
          select
            case when planet_id is not null then 'planet' else 'star' end as target_type,
            coalesce(planet_id, star_id) as target_id,
            system_id,
            case when planet_id is null then star_id else null end as star_id,
            source_key as alias_raw,
            lower(trim(regexp_replace(regexp_replace(source_key, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) as alias_norm,
            source_key,
            row_number() over (order by source_key) as rn
          from tess_toi_links
          where host_resolution_status = 'accepted'
            and coalesce(planet_id, star_id) is not null
        ), pending as (
          select c.*
          from candidates c
          where not exists (
            select 1 from aliases a
            where a.target_type = c.target_type and a.target_id = c.target_id
              and a.alias_norm = c.alias_norm
          )
        )
        select
          base.base_id + row_number() over (order by pending.source_key),
          pending.target_type, pending.target_id, pending.system_id, pending.star_id,
          pending.alias_raw, pending.alias_norm, 'toi_id', {TOI_ALIAS_PRIORITY}, false,
          'nasa_toi', {sql_literal(toi_version)},
          cast(round(try_cast(replace(pending.source_key, 'TOI-', '') as double) * 100) as bigint)
        from pending cross join base
        """
    )

    if append_search_terms:
        con.execute(
            """
            insert into system_search_terms
            with base as (select coalesce(max(search_term_id), 0)::bigint as base_id from system_search_terms),
            pending as (
              select a.*
              from aliases a
              where a.alias_kind in ('tic_id', 'toi_id') and a.system_id is not null
                and not exists (
                  select 1 from system_search_terms s
                  where s.system_id = a.system_id and s.term_norm = a.alias_norm
                )
            )
            select
              base.base_id + row_number() over (order by pending.system_id, pending.alias_priority, pending.alias_norm),
              pending.system_id, pending.target_type, pending.target_id, pending.star_id,
              pending.alias_id, pending.alias_raw, pending.alias_norm, pending.alias_kind,
              pending.alias_priority, false, pending.source_catalog, pending.source_version, pending.source_pk
            from pending cross join base
            """
        )

    status_rows = con.execute(
        "select resolution_status, resolution_reason, count(*) from tess_identity_resolution group by 1,2 order by 1,2"
    ).fetchall()
    report = {
        "transform_version": TESS_TRANSFORM_VERSION,
        "counts": {
            "target_tic_ids": int(con.execute("select count(*) from tess_identity_resolution").fetchone()[0]),
            "accepted": int(con.execute("select count(*) from tess_identity_resolution where resolution_status='accepted'").fetchone()[0]),
            "missing": int(con.execute("select count(*) from tess_identity_resolution where resolution_status='missing'").fetchone()[0]),
            "excluded": int(con.execute("select count(*) from tess_identity_resolution where resolution_status='excluded'").fetchone()[0]),
            "ambiguous": int(con.execute("select count(*) from tess_identity_resolution where resolution_status='ambiguous'").fetchone()[0]),
            "source_missing": int(con.execute("select count(*) from tess_identity_resolution where resolution_status='source_missing'").fetchone()[0]),
            "tic_identifier_collisions": collision_count,
            "toi_rows": int(con.execute("select count(*) from tess_toi_links").fetchone()[0]),
            "confirmed_known_planet_links": int(con.execute("select count(*) from tess_toi_links where disposition in ('CP','KP') and planet_id is not null").fetchone()[0]),
        },
        "status_reason_counts": [
            {"status": row[0], "reason": row[1], "count": int(row[2])} for row in status_rows
        ],
    }
    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        csv_path = report_path.with_name("tess_identity_resolution.csv")
        con.execute(f"copy (select * from tess_identity_resolution order by tic_id) to {sql_literal(str(csv_path))} (header, delimiter ',')")
        missing_path = report_path.with_name("tess_missing_object_audit.csv")
        con.execute(
            f"copy (select * from tess_identity_resolution where resolution_status <> 'accepted' order by tic_id) to {sql_literal(str(missing_path))} (header, delimiter ',')"
        )
    return report


def materialize_arm(
    con: duckdb.DuckDBPyConnection,
    *,
    cooked_dir: Path,
    manifest_path: Path,
    ingested_at: str,
) -> dict[str, int]:
    create_input_views(con, cooked_dir)
    # Build resolution against the attached core schema without changing it.
    con.execute("create or replace temp view stars as select * from core.stars")
    con.execute("create or replace temp view planets as select * from core.planets")
    con.execute("create or replace temp view object_identifiers as select * from core.object_identifiers")
    create_resolution_tables(con)
    create_toi_link_table(con)
    manifest = _read_manifest(manifest_path)
    tic_manifest = manifest.get("mast_tic_targeted", {})
    toi_manifest = manifest.get("nasa_toi", {})
    tic_version = str(tic_manifest.get("source_version") or "tic_v8_targeted")
    toi_version = str(toi_manifest.get("source_version") or "nasa_toi_tap_snapshot")
    tic_checksum = str(tic_manifest.get("sha256") or "")
    toi_checksum = str(toi_manifest.get("sha256") or "")
    tic_retrieved = str(tic_manifest.get("retrieved_at") or "")
    toi_retrieved = str(toi_manifest.get("retrieved_at") or "")

    con.execute(
        f"""
        create table tess_target_identity as
        select
          row_number() over (order by tic_id)::bigint as tess_identity_id,
          *,
          {sql_literal(tic_version)} as source_version,
          {sql_literal(TIC_SOURCE_URL)} as source_url,
          {sql_literal(tic_checksum)} as retrieval_checksum,
          {sql_literal(tic_retrieved)} as retrieved_at,
          {sql_literal(ingested_at)} as ingested_at,
          {sql_literal(TESS_TRANSFORM_VERSION)} as transform_version
        from tess_identity_resolution
        """
    )
    con.execute(
        """
        create table tess_missing_object_audit as
        select
          row_number() over (order by tic_id)::bigint as audit_id,
          tic_id, source_families, resolution_status, resolution_reason,
          case
            when resolution_status = 'excluded' then 'tic_artifact_split_join_or_duplicate'
            when resolution_status = 'ambiguous' then 'ambiguous_identity'
            when resolution_status = 'source_missing' then 'source_missing'
            when resolution_reason = 'outside_1000ly_scope' then 'outside_distance_scope'
            when resolution_reason = 'gaia_dr3_not_in_core' then 'valid_gaia_dr3_excluded_or_absent'
            when resolution_reason = 'gaia_dr2_unmapped_or_absent' then 'gaia_dr2_only_or_unmapped'
            else 'insufficient_evidence'
          end as gap_class,
          gaia_dr2_id, hip_id, tyc_id, twomass_id, ra_deg, dec_deg, distance_pc,
          tic_disposition, duplicate_id, candidate_star_count,
          neighbourhood_row_count, dr3_candidate_count, candidates_json, source_row_hash
          , gaia_dr3_source_count, gaia_dr3_in_scope_count, gaia_dr3_max_parallax_mas
        from tess_identity_resolution
        where resolution_status <> 'accepted'
        """
    )
    con.execute(
        f"""
        create table toi_current_evidence as
        select
          row_number() over (order by try_cast(toi as double), source_key)::bigint as toi_evidence_id,
          source_key, try_cast(tic_id as bigint) as tic_id, toi, toi_display, toi_prefix,
          ctoi_alias, try_cast(nullif(planet_number, '') as integer) as planet_number,
          disposition, star_id, system_id, planet_id,
          host_resolution_status, host_resolution_reason, host_resolution_confidence,
          planet_link_method, planet_period_delta_days,
          try_cast(nullif(ra_deg, '') as double) as ra_deg,
          try_cast(nullif(dec_deg, '') as double) as dec_deg,
          try_cast(nullif(pm_ra_mas_yr, '') as double) as pm_ra_mas_yr,
          try_cast(nullif(pm_dec_mas_yr, '') as double) as pm_dec_mas_yr,
          try_cast(nullif(tmag, '') as double) as tmag,
          try_cast(nullif(transit_epoch_bjd, '') as double) as transit_epoch_bjd,
          try_cast(nullif(transit_epoch_err_plus, '') as double) as transit_epoch_err_plus,
          try_cast(nullif(transit_epoch_err_minus, '') as double) as transit_epoch_err_minus,
          try_cast(nullif(orbital_period_days, '') as double) as orbital_period_days,
          try_cast(nullif(orbital_period_err_plus, '') as double) as orbital_period_err_plus,
          try_cast(nullif(orbital_period_err_minus, '') as double) as orbital_period_err_minus,
          try_cast(nullif(transit_duration_hours, '') as double) as transit_duration_hours,
          try_cast(nullif(transit_duration_err_plus, '') as double) as transit_duration_err_plus,
          try_cast(nullif(transit_duration_err_minus, '') as double) as transit_duration_err_minus,
          try_cast(nullif(transit_depth_ppm, '') as double) as transit_depth_ppm,
          try_cast(nullif(transit_depth_err_plus, '') as double) as transit_depth_err_plus,
          try_cast(nullif(transit_depth_err_minus, '') as double) as transit_depth_err_minus,
          try_cast(nullif(planet_radius_earth, '') as double) as planet_radius_earth,
          try_cast(nullif(planet_radius_err_plus, '') as double) as planet_radius_err_plus,
          try_cast(nullif(planet_radius_err_minus, '') as double) as planet_radius_err_minus,
          try_cast(nullif(insolation_earth, '') as double) as insolation_earth,
          try_cast(nullif(insolation_err_plus, '') as double) as insolation_err_plus,
          try_cast(nullif(insolation_err_minus, '') as double) as insolation_err_minus,
          try_cast(nullif(equilibrium_temp_k, '') as double) as equilibrium_temp_k,
          try_cast(nullif(stellar_distance_pc, '') as double) as stellar_distance_pc,
          try_cast(nullif(stellar_teff_k, '') as double) as stellar_teff_k,
          try_cast(nullif(stellar_logg_cgs, '') as double) as stellar_logg_cgs,
          try_cast(nullif(stellar_radius_solar, '') as double) as stellar_radius_solar,
          nullif(sectors, '') as sectors, nullif(toi_created, '') as toi_created,
          nullif(row_updated_at, '') as row_updated_at, nullif(release_date, '') as release_date,
          source_row_hash, 'nasa_toi' as source_catalog,
          {sql_literal(toi_version)} as source_version,
          {sql_literal(TOI_SOURCE_URL)} as source_url,
          {sql_literal(toi_checksum)} as retrieval_checksum,
          {sql_literal(toi_retrieved)} as retrieved_at,
          {sql_literal(ingested_at)} as ingested_at,
          {sql_literal(TESS_TRANSFORM_VERSION)} as transform_version
        from tess_toi_links
        """
    )
    con.execute(
        f"""
        create table toi_disposition_history as
        select
          row_number() over (order by source_key, effective_at, disposition)::bigint as history_id,
          source_key, try_cast(tic_id as bigint) as tic_id, toi_display, disposition,
          nullif(effective_at, '') as effective_at, nullif(release_date, '') as release_date,
          source_row_hash, nullif(first_observed_at, '') as first_observed_at,
          nullif(last_observed_at, '') as last_observed_at,
          'nasa_toi' as source_catalog, {sql_literal(toi_version)} as source_version,
          {sql_literal(TOI_SOURCE_URL)} as source_url,
          {sql_literal(toi_checksum)} as retrieval_checksum,
          {sql_literal(toi_retrieved)} as retrieved_at,
          {sql_literal(ingested_at)} as ingested_at,
          {sql_literal(TESS_TRANSFORM_VERSION)} as transform_version
        from tess_toi_history_input
        """
    )
    return {
        "tess_target_identity_rows": int(con.execute("select count(*) from tess_target_identity").fetchone()[0]),
        "tess_missing_object_audit_rows": int(con.execute("select count(*) from tess_missing_object_audit").fetchone()[0]),
        "toi_current_evidence_rows": int(con.execute("select count(*) from toi_current_evidence").fetchone()[0]),
        "toi_confirmed_known_planet_links": int(con.execute("select count(*) from toi_current_evidence where disposition in ('CP','KP') and planet_id is not null").fetchone()[0]),
        "toi_candidate_rows": int(con.execute("select count(*) from toi_current_evidence where disposition in ('PC','APC')").fetchone()[0]),
        "toi_negative_evidence_rows": int(con.execute("select count(*) from toi_current_evidence where disposition in ('FP','FA')").fetchone()[0]),
    }
