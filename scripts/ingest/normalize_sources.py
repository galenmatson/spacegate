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


def normalized_name_sql(column: str) -> str:
    return (
        "case when nullif(trim("
        + column
        + "), '') is null then null else "
        + "lower(trim(regexp_replace(regexp_replace("
        + column
        + ", '[^0-9A-Za-z]+', ' ', 'g'), '\\\\s+', ' ', 'g'))) end"
    )


def boolish_sql(column: str) -> str:
    return (
        "case "
        f"when lower(trim(coalesce({column}, ''))) in ('1','true','t','yes','y','on') then true "
        f"when lower(trim(coalesce({column}, ''))) in ('0','false','f','no','n','off') then false "
        "else null end"
    )


def count_table(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"select count(*) from {table_name}").fetchone()[0])


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


def create_gaia_sources(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    non_single_star_bool = boolish_sql("non_single_star")
    duplicated_source_bool = boolish_sql("duplicated_source")
    con.execute(
        f"""
        create table gaia_star_sources as
        select
          row_number() over (order by source_id::bigint)::bigint as source_row_id,
          'src:gaia:' || source_id as node_key,
          'gaia_backbone'::varchar as source_catalog,
          'star'::varchar as entity_type,
          source_id::bigint as source_pk,
          'Gaia DR3 ' || source_id as display_name,
          source_id::bigint as gaia_id,
          nullif(ref_epoch, '')::double as ref_epoch,
          nullif(ra_deg, '')::double as ra_deg,
          nullif(dec_deg, '')::double as dec_deg,
          nullif(parallax_mas, '')::double as parallax_mas,
          nullif(parallax_error_mas, '')::double as parallax_error_mas,
          nullif(parallax_over_error, '')::double as parallax_over_error,
          nullif(pm_ra_mas_yr, '')::double as pm_ra_mas_yr,
          nullif(pm_dec_mas_yr, '')::double as pm_dec_mas_yr,
          nullif(radial_velocity_kms, '')::double as radial_velocity_kms,
          nullif(ruwe, '')::double as ruwe,
          nullif(phot_g_mag, '')::double as phot_g_mag,
          nullif(phot_bp_mag, '')::double as phot_bp_mag,
          nullif(phot_rp_mag, '')::double as phot_rp_mag,
          nullif(bp_rp, '')::double as bp_rp,
          nullif(teff_gspphot, '')::double as teff_gspphot,
          nullif(logg_gspphot, '')::double as logg_gspphot,
          nullif(mh_gspphot, '')::double as mh_gspphot,
          nullif(distance_gspphot, '')::double as distance_gspphot,
          coalesce({non_single_star_bool}, false) as non_single_star,
          coalesce({duplicated_source_bool}, false) as duplicated_source
        from read_csv_auto(
          {sql_literal(str(csv_path))},
          delim=',',
          quote='\"',
          escape='\"',
          header=true,
          strict_mode=false,
          null_padding=true,
          all_varchar=true
        )
        where nullif(source_id, '') is not null
        """
    )


def create_nasa_sources(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    host_name_norm = normalized_name_sql("hostname")
    planet_name_norm = normalized_name_sql("pl_name")
    con.execute(
        f"""
        create table nasa_host_sources as
        with base as (
          select
            nullif(hostid, '') as host_source_pk,
            nullif(objectid, '') as object_source_pk,
            nullif(hostname, '') as host_name,
            {host_name_norm} as host_name_norm,
            cast(nullif(regexp_extract(coalesce(gaia_dr3_id, gaia_dr2_id, ''), '(\\d{{10,}})\\s*$', 1), '') as bigint) as gaia_id,
            cast(nullif(regexp_extract(hip_name, '(\\d+)', 1), '') as bigint) as hip_id,
            cast(nullif(regexp_extract(hd_name, '(\\d+)', 1), '') as bigint) as hd_id,
            nullif(st_spectype, '') as spectral_type_raw,
            nullif(st_teff, '')::double as st_teff,
            nullif(st_mass, '')::double as st_mass,
            nullif(st_rad, '')::double as st_rad,
            nullif(st_lum, '')::double as st_lum,
            nullif(st_logg, '')::double as st_logg,
            nullif(st_met, '')::double as st_met,
            nullif(st_age, '')::double as st_age,
            nullif(st_rotp, '')::double as st_rotp,
            nullif(st_dens, '')::double as st_dens,
            nullif(sy_snum, '')::bigint as sy_snum,
            nullif(sy_pnum, '')::bigint as sy_pnum,
            nullif(sy_mnum, '')::bigint as sy_mnum
          from read_csv_auto(
            {sql_literal(str(csv_path))},
            delim=',',
            quote='\"',
            escape='\"',
            header=true,
            strict_mode=false,
            null_padding=true,
            all_varchar=true
          )
          where nullif(hostid, '') is not null
        ), ranked as (
          select
            *,
            row_number() over (
              partition by host_source_pk
              order by
                case when gaia_id is not null then 0 else 1 end,
                case when st_teff is not null then 0 else 1 end,
                object_source_pk asc
            ) as row_num
          from base
        )
        select
          row_number() over (order by host_source_pk)::bigint as source_row_id,
          'src:nasa_host:' || host_source_pk as node_key,
          'nasa_exoplanet_archive'::varchar as source_catalog,
          'star'::varchar as entity_type,
          host_source_pk as source_pk,
          host_name as display_name,
          host_name,
          host_name_norm,
          gaia_id,
          hip_id,
          hd_id,
          spectral_type_raw,
          st_teff,
          st_mass,
          st_rad,
          st_lum,
          st_logg,
          st_met,
          st_age,
          st_rotp,
          st_dens,
          sy_snum,
          sy_pnum,
          sy_mnum
        from ranked
        where row_num = 1
        """
    )
    con.execute(
        f"""
        create table nasa_planet_sources as
        with raw as (
          select
            nullif(objectid, '') as source_pk,
            nullif(hostid, '') as host_source_pk,
            nullif(pl_name, '') as planet_name,
            {planet_name_norm} as planet_name_norm,
            nullif(hostname, '') as host_name,
            {host_name_norm} as host_name_norm,
            nullif(disc_year, '')::int as disc_year,
            nullif(discoverymethod, '') as discovery_method,
            nullif(disc_facility, '') as discovery_facility,
            nullif(pl_orbper, '')::double as orbital_period_days,
            nullif(pl_orbsmax, '')::double as semi_major_axis_au,
            nullif(pl_orbeccen, '')::double as eccentricity,
            nullif(pl_orbincl, '')::double as inclination_deg,
            nullif(pl_radj, '')::double as radius_jup,
            nullif(pl_rade, '')::double as radius_earth,
            nullif(pl_masse, '')::double as mass_earth,
            nullif(pl_massj, '')::double as mass_jup,
            nullif(pl_eqt, '')::double as eq_temp_k,
            nullif(pl_insol, '')::double as insol_earth
          from read_csv_auto(
            {sql_literal(str(csv_path))},
            delim=',',
            quote='\"',
            escape='\"',
            header=true,
            strict_mode=false,
            null_padding=true,
            all_varchar=true
          )
          where nullif(objectid, '') is not null
        )
        select
          row_number() over (order by source_pk)::bigint as source_row_id,
          'src:nasa_planet:' || source_pk as node_key,
          'nasa_exoplanet_archive'::varchar as source_catalog,
          'planet'::varchar as entity_type,
          source_pk,
          planet_name as display_name,
          planet_name,
          planet_name_norm,
          host_name,
          host_name_norm,
          case when host_source_pk is null then null else 'src:nasa_host:' || host_source_pk end as host_node_key,
          disc_year,
          discovery_method,
          discovery_facility,
          orbital_period_days,
          semi_major_axis_au,
          eccentricity,
          inclination_deg,
          radius_jup,
          radius_earth,
          mass_earth,
          mass_jup,
          eq_temp_k,
          insol_earth
        from raw
        """
    )


def create_msc_sources(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    con.execute(
        f"""
        create table msc_component_sources as
        select
          row_number() over (order by wds_id, component)::bigint as source_row_id,
          'src:msc_component:' || wds_id || ':' || component as node_key,
          'src:msc_system:' || wds_id as system_node_key,
          'msc'::varchar as source_catalog,
          'star'::varchar as entity_type,
          row_number() over (order by wds_id, component)::bigint as source_pk,
          coalesce(nullif(preferred_name, ''), 'WDS ' || wds_id || ' ' || component) as display_name,
          nullif(wds_id, '') as wds_id,
          nullif(component, '') as component_label,
          nullif(ra_deg, '')::double as ra_deg,
          nullif(dec_deg, '')::double as dec_deg,
          nullif(parallax_mas, '')::double as parallax_mas,
          nullif(pm_ra_mas_yr, '')::double as pm_ra_mas_yr,
          nullif(pm_dec_mas_yr, '')::double as pm_dec_mas_yr,
          nullif(radial_velocity_kms, '')::double as radial_velocity_kms,
          cast(nullif(hip_id, '') as bigint) as hip_id,
          cast(nullif(hd_id, '') as bigint) as hd_id,
          nullif(spectral_type_raw, '') as spectral_type_raw,
          nullif(sep_arcsec, '')::double as sep_arcsec,
          nullif(vmag, '')::double as vmag,
          nullif(ncomp, '')::bigint as ncomp,
          nullif(subsystem_count, '')::bigint as subsystem_count,
          nullif(orbit_count, '')::bigint as orbit_count
        from read_csv_auto(
          {sql_literal(str(csv_path))},
          delim=',',
          quote='\"',
          escape='\"',
          header=true,
          strict_mode=false,
          null_padding=true,
          all_varchar=true
        )
        where nullif(wds_id, '') is not null and nullif(component, '') is not null
        """
    )
    con.execute(
        """
        create table msc_system_sources as
        select
          row_number() over (order by wds_id)::bigint as source_row_id,
          'src:msc_system:' || wds_id as node_key,
          'msc'::varchar as source_catalog,
          'system'::varchar as entity_type,
          row_number() over (order by wds_id)::bigint as source_pk,
          'WDS ' || wds_id as display_name,
          wds_id,
          count(*)::bigint as component_count,
          max(subsystem_count)::bigint as subsystem_count,
          max(orbit_count)::bigint as orbit_count,
          min(ra_deg) as representative_ra_deg,
          min(dec_deg) as representative_dec_deg
        from msc_component_sources
        group by wds_id
        """
    )


def create_wds_sources(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    con.execute(
        f"""
        create table wds_system_sources as
        with raw as (
          select
            nullif(wds_id, '') as wds_id,
            nullif(discoverer, '') as discoverer,
            nullif(component, '') as component_label,
            nullif(obs_count, '')::bigint as obs_count,
            nullif(ra_deg, '')::double as ra_deg,
            nullif(dec_deg, '')::double as dec_deg
          from read_csv_auto(
            {sql_literal(str(csv_path))},
            delim=',',
            quote='\"',
            escape='\"',
            header=true,
            strict_mode=false,
            null_padding=true,
            all_varchar=true
          )
          where nullif(wds_id, '') is not null
        )
        select
          row_number() over (order by wds_id)::bigint as source_row_id,
          'src:wds_system:' || wds_id as node_key,
          'wds'::varchar as source_catalog,
          'system'::varchar as entity_type,
          row_number() over (order by wds_id)::bigint as source_pk,
          'WDS ' || wds_id as display_name,
          wds_id,
          min(discoverer) as discoverer,
          count(*)::bigint as observation_row_count,
          count(distinct component_label)::bigint as component_group_count,
          sum(coalesce(obs_count, 0))::bigint as observation_count,
          min(ra_deg) as representative_ra_deg,
          min(dec_deg) as representative_dec_deg
        from raw
        group by wds_id
        """
    )


def create_orb6_sources(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    con.execute(
        f"""
        create table orb6_orbit_sources as
        select
          row_number() over (order by coalesce(wds_id, ''), coalesce(reference_code, ''), coalesce(discoverer, ''))::bigint as source_row_id,
          'src:orb6_orbit:' ||
            coalesce(nullif(wds_id, ''), 'none') || ':' ||
            coalesce(nullif(reference_code, ''), 'none') || ':' ||
            row_number() over (order by coalesce(wds_id, ''), coalesce(reference_code, ''), coalesce(discoverer, ''))::varchar as node_key,
          case when nullif(wds_id, '') is null then null else 'src:wds_system:' || wds_id end as system_node_key,
          'orb6'::varchar as source_catalog,
          'orbit'::varchar as entity_type,
          row_number() over (order by coalesce(wds_id, ''), coalesce(reference_code, ''), coalesce(discoverer, ''))::bigint as source_pk,
          coalesce(nullif(discoverer, ''), 'ORB6 orbit') as display_name,
          nullif(wds_id, '') as wds_id,
          cast(nullif(hip_id, '') as bigint) as hip_id,
          cast(nullif(hd_id, '') as bigint) as hd_id,
          nullif(discoverer, '') as discoverer,
          nullif(reference_code, '') as reference_code,
          nullif(period_value, '')::double as period_value,
          nullif(period_unit, '') as period_unit,
          nullif(eccentricity, '')::double as eccentricity,
          nullif(inclination_deg, '')::double as inclination_deg,
          nullif(grade, '') as orbit_grade,
          nullif(ra_deg, '')::double as ra_deg,
          nullif(dec_deg, '')::double as dec_deg
        from read_csv_auto(
          {sql_literal(str(csv_path))},
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


def create_sbx_sources(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    in_triple_bool = boolish_sql("in_triple")
    con.execute(
        f"""
        create table sbx_star_sources as
        select
          row_number() over (order by sn::bigint)::bigint as source_row_id,
          'src:sbx:' || sn as node_key,
          'sbx'::varchar as source_catalog,
          'star'::varchar as entity_type,
          sn::bigint as source_pk,
          coalesce(
            case when nullif(gaia_id, '') is not null then 'Gaia DR3 ' || gaia_id end,
            case when nullif(hip_id, '') is not null then 'HIP ' || hip_id end,
            case when nullif(hd_id, '') is not null then 'HD ' || hd_id end,
            'SBX ' || sn
          ) as display_name,
          cast(nullif(gaia_id, '') as bigint) as gaia_id,
          cast(nullif(hip_id, '') as bigint) as hip_id,
          cast(nullif(hd_id, '') as bigint) as hd_id,
          nullif(wds_id, '') as wds_id,
          nullif(spectral_type_raw, '') as spectral_type_raw,
          nullif(family, '') as sbx_family,
          nullif(parent, '') as sbx_parent,
          nullif(child1, '') as sbx_child1,
          nullif(child2, '') as sbx_child2,
          coalesce({in_triple_bool}, false) as in_triple,
          nullif(orbit_count, '')::bigint as orbit_count,
          nullif(ra_deg, '')::double as ra_deg,
          nullif(dec_deg, '')::double as dec_deg
        from read_csv_auto(
          {sql_literal(str(csv_path))},
          delim=',',
          quote='\"',
          escape='\"',
          header=true,
          strict_mode=false,
          null_padding=true,
          all_varchar=true
        )
        where nullif(sn, '') is not null
        """
    )


def create_sol_sources(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    con.execute(
        f"""
        create table sol_object_sources as
        select
          row_number() over (order by source_pk::bigint)::bigint as source_row_id,
          'src:sol:' || source_pk as node_key,
          case when nullif(parent_object_name, '') is null then null else 'src:sol_name:' || lower(parent_object_name) end as parent_name_key,
          'sol_authority'::varchar as source_catalog,
          'solar_system_object'::varchar as entity_type,
          source_pk::bigint as source_pk,
          nullif(object_name, '') as display_name,
          nullif(object_name, '') as object_name,
          {normalized_name_sql("object_name")} as object_name_norm,
          nullif(parent_object_name, '') as parent_object_name,
          nullif(object_class, '') as object_class,
          nullif(object_kind, '') as object_kind,
          nullif(semi_major_axis_au, '')::double as semi_major_axis_au,
          nullif(orbital_period_days, '')::double as orbital_period_days,
          nullif(radius_km, '')::double as radius_km,
          nullif(mass_kg, '')::double as mass_kg
        from read_csv_auto(
          {sql_literal(str(csv_path))},
          delim=',',
          quote='\"',
          escape='\"',
          header=true,
          strict_mode=false,
          null_padding=true,
          all_varchar=true
        )
        where nullif(source_pk, '') is not null
        """
    )


def create_legacy_crosswalk_sources(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table legacy_crosswalk_star_sources as
        select
          row_number() over (order by star_id)::bigint as source_row_id,
          'src:legacy_core_star:' || star_id::varchar as node_key,
          case when system_id is null then null else 'src:legacy_core_system:' || system_id::varchar end as system_node_key,
          'legacy_core_crosswalk'::varchar as source_catalog,
          'star'::varchar as entity_type,
          star_id as source_pk,
          star_name as display_name,
          star_name,
          star_name_norm,
          stable_object_key,
          source_catalog as upstream_source_catalog,
          source_pk::varchar as upstream_source_pk,
          gaia_id,
          hip_id,
          hd_id,
          wds_id,
          component,
          ra_deg,
          dec_deg,
          dist_ly,
          multiplicity_match_confidence,
          multiplicity_match_method
        from core.stars
        """
    )
    con.execute(
        """
        create table legacy_crosswalk_system_sources as
        select
          row_number() over (order by system_id)::bigint as source_row_id,
          'src:legacy_core_system:' || system_id::varchar as node_key,
          'legacy_core_crosswalk'::varchar as source_catalog,
          'system'::varchar as entity_type,
          system_id as source_pk,
          system_name as display_name,
          system_name,
          system_name_norm,
          stable_object_key,
          source_catalog as upstream_source_catalog,
          source_pk::varchar as upstream_source_pk,
          wds_id,
          gaia_id,
          hip_id,
          hd_id,
          star_count,
          planet_count,
          ra_deg,
          dec_deg,
          dist_ly
        from core.systems
        """
    )
    con.execute(
        """
        create table legacy_crosswalk_planet_sources as
        select
          row_number() over (order by planet_id)::bigint as source_row_id,
          'src:legacy_core_planet:' || planet_id::varchar as node_key,
          case when star_id is null then null else 'src:legacy_core_star:' || star_id::varchar end as host_node_key,
          case when system_id is null then null else 'src:legacy_core_system:' || system_id::varchar end as system_node_key,
          'legacy_core_crosswalk'::varchar as source_catalog,
          'planet'::varchar as entity_type,
          planet_id::varchar as source_pk,
          planet_name as display_name,
          planet_name,
          planet_name_norm,
          stable_object_key,
          source_catalog as upstream_source_catalog,
          source_pk::varchar as upstream_source_pk,
          system_id,
          star_id
        from core.planets
        """
    )


def create_source_nodes(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table source_nodes as
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          cast(null as varchar) as system_node_key,
          gaia_id,
          cast(null as bigint) as hip_id,
          cast(null as bigint) as hd_id,
          cast(null as varchar) as wds_id
        from gaia_star_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          cast(null as varchar) as system_node_key,
          gaia_id,
          hip_id,
          hd_id,
          cast(null as varchar) as wds_id
        from nasa_host_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          host_node_key,
          cast(null as varchar) as system_node_key,
          cast(null as bigint) as gaia_id,
          cast(null as bigint) as hip_id,
          cast(null as bigint) as hd_id,
          cast(null as varchar) as wds_id
        from nasa_planet_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          system_node_key,
          cast(null as bigint) as gaia_id,
          hip_id,
          hd_id,
          wds_id
        from msc_component_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          cast(null as varchar) as system_node_key,
          cast(null as bigint) as gaia_id,
          cast(null as bigint) as hip_id,
          cast(null as bigint) as hd_id,
          wds_id
        from msc_system_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          cast(null as varchar) as system_node_key,
          cast(null as bigint) as gaia_id,
          cast(null as bigint) as hip_id,
          cast(null as bigint) as hd_id,
          wds_id
        from wds_system_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          system_node_key,
          cast(null as bigint) as gaia_id,
          hip_id,
          hd_id,
          wds_id
        from orb6_orbit_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          cast(null as varchar) as system_node_key,
          gaia_id,
          hip_id,
          hd_id,
          wds_id
        from sbx_star_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          parent_name_key as parent_node_key,
          cast(null as varchar) as host_node_key,
          cast(null as varchar) as system_node_key,
          cast(null as bigint) as gaia_id,
          cast(null as bigint) as hip_id,
          cast(null as bigint) as hd_id,
          cast(null as varchar) as wds_id
        from sol_object_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          system_node_key,
          gaia_id,
          hip_id,
          hd_id,
          wds_id
        from legacy_crosswalk_star_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          cast(null as varchar) as host_node_key,
          cast(null as varchar) as system_node_key,
          gaia_id,
          hip_id,
          hd_id,
          wds_id
        from legacy_crosswalk_system_sources
        union all
        select
          node_key,
          source_catalog,
          entity_type,
          source_pk::varchar as source_pk,
          display_name,
          cast(null as varchar) as parent_node_key,
          host_node_key,
          system_node_key,
          cast(null as bigint) as gaia_id,
          cast(null as bigint) as hip_id,
          cast(null as bigint) as hd_id,
          cast(null as varchar) as wds_id
        from legacy_crosswalk_planet_sources
        """
    )


def build_normalized_sources(*, build_id: str, build_dir: Path, state: Path, reports_dir: Path) -> dict[str, object]:
    core_path = build_dir / "core.duckdb"
    if not core_path.exists():
        raise SystemExit(f"Missing core.duckdb for build {build_id}: {core_path}")

    cooked_paths = {
        "gaia": state / "cooked" / "gaia_backbone" / "gaia_dr3_backbone.csv",
        "nasa": state / "cooked" / "nasa_exoplanet_archive" / "pscomppars_clean.csv",
        "msc": state / "cooked" / "msc" / "msc_components.csv",
        "wds": state / "cooked" / "wds" / "wds_summary.csv",
        "orb6": state / "cooked" / "orb6" / "orb6_orbits.csv",
        "sbx": state / "cooked" / "sbx" / "sbx_catalog.csv",
        "sol": state / "cooked" / "sol_authority" / "sol_system_objects.csv",
    }
    missing = [name for name, path in cooked_paths.items() if not path.exists()]
    if missing:
        raise SystemExit(f"Missing cooked source files for ingest normalization: {', '.join(sorted(missing))}")

    out_dir = build_dir / "ingest"
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "normalized_sources.duckdb"
    report_path = reports_dir / "normalized_sources_report.json"
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    try:
        maybe_set_duckdb_env(con)
        con.execute(f"ATTACH {sql_literal(str(core_path))} AS core (READ_ONLY)")
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
            insert into build_metadata values (?, ?, ?, ?), (?, ?, ?, ?)
            """,
            [
                build_id,
                utc_now(),
                "core_build",
                str(core_path),
                build_id,
                utc_now(),
                "state_dir",
                str(state),
            ],
        )
        for source_kind, path in cooked_paths.items():
            con.execute(
                "insert into build_metadata values (?, ?, ?, ?)",
                [build_id, utc_now(), source_kind, str(path)],
            )

        create_gaia_sources(con, cooked_paths["gaia"])
        create_nasa_sources(con, cooked_paths["nasa"])
        create_msc_sources(con, cooked_paths["msc"])
        create_wds_sources(con, cooked_paths["wds"])
        create_orb6_sources(con, cooked_paths["orb6"])
        create_sbx_sources(con, cooked_paths["sbx"])
        create_sol_sources(con, cooked_paths["sol"])
        create_legacy_crosswalk_sources(con)
        create_source_nodes(con)

        table_counts = {
            "gaia_star_sources": count_table(con, "gaia_star_sources"),
            "nasa_host_sources": count_table(con, "nasa_host_sources"),
            "nasa_planet_sources": count_table(con, "nasa_planet_sources"),
            "msc_component_sources": count_table(con, "msc_component_sources"),
            "msc_system_sources": count_table(con, "msc_system_sources"),
            "wds_system_sources": count_table(con, "wds_system_sources"),
            "orb6_orbit_sources": count_table(con, "orb6_orbit_sources"),
            "sbx_star_sources": count_table(con, "sbx_star_sources"),
            "sol_object_sources": count_table(con, "sol_object_sources"),
            "legacy_crosswalk_star_sources": count_table(con, "legacy_crosswalk_star_sources"),
            "legacy_crosswalk_system_sources": count_table(con, "legacy_crosswalk_system_sources"),
            "legacy_crosswalk_planet_sources": count_table(con, "legacy_crosswalk_planet_sources"),
            "source_nodes": count_table(con, "source_nodes"),
        }
    finally:
        con.close()

    reports_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "generated_at": utc_now(),
        "build_id": build_id,
        "normalized_db_path": str(db_path),
        "tables": table_counts,
        "notes": [
            "legacy_core_crosswalk is a transitional bootstrap bridge from the current deterministic build",
            "ingest canonical reduction should replace this bridge with source-native crosswalk rules over time",
        ],
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ingest normalized source artifacts for a Spacegate build.")
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
    payload = build_normalized_sources(
        build_id=build_id,
        build_dir=build_dir,
        state=state,
        reports_dir=reports_dir,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
