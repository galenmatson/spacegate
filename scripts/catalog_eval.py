#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
from dataclasses import dataclass
from pathlib import Path

import duckdb


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def iso_utc(value: dt.datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def normalize_name_sql(expr: str) -> str:
    return (
        "case when {expr} is null or trim({expr}) = '' then null else "
        "lower(trim(regexp_replace(regexp_replace({expr}, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) end"
    ).format(expr=expr)


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if value and value[0] not in "\"'":
            value = value.split("#", 1)[0].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ[key] = value


def init_env(root: Path) -> None:
    for env_path in (
        Path("/etc/spacegate/spacegate.env"),
        root / ".spacegate.env",
        root / ".spacegate.local.env",
    ):
        load_env_file(env_path)


@dataclass(frozen=True)
class OverlapField:
    field_name: str
    core_column: str
    label: str


@dataclass(frozen=True)
class CatalogSpec:
    name: str
    entity_type: str
    path_parts: tuple[str, ...]
    source_sql: str
    normalized_sql: str
    sample_columns: tuple[str, ...]
    coverage_columns: tuple[str, ...]
    overlap_fields: tuple[OverlapField, ...]

    def resolve_path(self, state_dir: Path) -> Path:
        return state_dir.joinpath(*self.path_parts)


CATALOG_RELIABILITY_SCORES: dict[str, float] = {
    "gaia_dr3_sample": 0.98,
    "gaia_dr3_non_single_sample": 0.98,
    "gaia_dr3_nss_two_body_sample": 0.98,
    "athyg": 0.55,
    "nasa_exoplanet_archive": 0.92,
    "wds": 0.95,
    "msc": 0.90,
    "orb6": 0.93,
    "sbx_sample": 0.94,
    "debcat": 0.90,
    "kepler_eb": 0.89,
    "tess_eb": 0.90,
}

CATALOG_REQUIRES_CROSSMATCH: set[str] = {"wds", "msc", "orb6"}

CATALOG_TIER_OVERRIDE: dict[str, str] = {
    "athyg": "situational",
}


ATHYG_NORMALIZED_SQL = f"""
select
  'athyg' as catalog_name,
  'star' as entity_type,
  coalesce(
    nullif(id, ''),
    nullif(gaia, ''),
    nullif(hip, ''),
    nullif(hd, ''),
    md5(
      coalesce(nullif(proper, ''), '') || '|' ||
      coalesce(nullif(ra, ''), '') || '|' ||
      coalesce(nullif(dec, ''), '') || '|' ||
      coalesce(nullif(dist, ''), '')
    )
  ) as sample_key,
  nullif(id, '')::bigint as source_pk,
  nullif(gaia, '')::bigint as gaia_id,
  nullif(hip, '')::bigint as hip_id,
  nullif(hd, '')::bigint as hd_id,
  nullif(hr, '')::bigint as hr_id,
  nullif(gl, '') as gl_id,
  nullif(tyc, '') as tyc_id,
  nullif(hyg, '')::bigint as hyg_id,
  nullif(proper, '') as proper_name,
  nullif(bayer, '') as bayer,
  nullif(flam, '') as flam,
  nullif(con, '') as constellation,
  coalesce(
    nullif(proper, ''),
    case when nullif(bayer, '') is not null and nullif(con, '') is not null then nullif(bayer, '') || ' ' || nullif(con, '') end,
    case when nullif(flam, '') is not null and nullif(con, '') is not null then nullif(flam, '') || ' ' || nullif(con, '') end,
    case when nullif(hip, '') is not null then 'HIP ' || nullif(hip, '') end,
    case when nullif(hd, '') is not null then 'HD ' || nullif(hd, '') end,
    case when nullif(gaia, '') is not null then 'Gaia DR3 ' || nullif(gaia, '') end
  ) as object_name,
  {normalize_name_sql("coalesce(nullif(proper, ''), case when nullif(bayer, '') is not null and nullif(con, '') is not null then nullif(bayer, '') || ' ' || nullif(con, '') end, case when nullif(flam, '') is not null and nullif(con, '') is not null then nullif(flam, '') || ' ' || nullif(con, '') end, case when nullif(hip, '') is not null then 'HIP ' || nullif(hip, '') end, case when nullif(hd, '') is not null then 'HD ' || nullif(hd, '') end, case when nullif(gaia, '') is not null then 'Gaia DR3 ' || nullif(gaia, '') end)")} as object_name_norm,
  nullif(ra, '')::double as ra_deg,
  nullif(dec, '')::double as dec_deg,
  nullif(dist, '')::double as dist_pc,
  nullif(x0, '')::double as x_helio_pc,
  nullif(y0, '')::double as y_helio_pc,
  nullif(z0, '')::double as z_helio_pc,
  nullif(pm_ra, '')::double as pm_ra_mas_yr,
  nullif(pm_dec, '')::double as pm_dec_mas_yr,
  nullif(rv, '')::double as radial_velocity_kms,
  nullif(mag, '')::double as vmag,
  nullif(absmag, '')::double as absmag,
  nullif(ci, '')::double as color_index,
  nullif(spect, '') as spectral_type_raw,
  nullif(pos_src, '') as pos_src,
  nullif(dist_src, '') as dist_src,
  nullif(pm_src, '') as pm_src,
  nullif(rv_src, '') as rv_src,
  nullif(spect_src, '') as spect_src
from catalog_source
"""


NASA_NORMALIZED_SQL = f"""
select
  'nasa_exoplanet_archive' as catalog_name,
  'planet' as entity_type,
  coalesce(nullif(pl_name, ''), nullif(objectid, ''), md5(coalesce(nullif(hostname, ''), ''))) as sample_key,
  nullif(objectid, '')::bigint as source_pk,
  nullif(pl_name, '') as object_name,
  {normalize_name_sql("nullif(pl_name, '')")} as object_name_norm,
  nullif(hostname, '') as host_name_raw,
  {normalize_name_sql("nullif(hostname, '')")} as host_name_norm,
  cast(nullif(regexp_extract(coalesce(nullif(gaia_dr3_id, ''), nullif(gaia_dr2_id, ''), ''), '(\\d{{10,}})\\s*$', 1), '') as bigint) as host_gaia_id,
  cast(nullif(regexp_extract(coalesce(nullif(hip_name, ''), ''), '(\\d+)', 1), '') as bigint) as host_hip_id,
  cast(nullif(regexp_extract(coalesce(nullif(hd_name, ''), ''), '(\\d+)', 1), '') as bigint) as host_hd_id,
  nullif(discoverymethod, '') as discovery_method,
  nullif(disc_year, '')::int as disc_year,
  nullif(pl_orbper, '')::double as orbital_period_days,
  nullif(pl_orbsmax, '')::double as semi_major_axis_au,
  nullif(pl_orbeccen, '')::double as eccentricity,
  nullif(pl_orbincl, '')::double as inclination_deg,
  nullif(pl_rade, '')::double as radius_earth,
  nullif(pl_radj, '')::double as radius_jup,
  nullif(pl_masse, '')::double as mass_earth,
  nullif(pl_massj, '')::double as mass_jup,
  nullif(pl_eqt, '')::double as eq_temp_k,
  nullif(pl_insol, '')::double as insol_earth,
  nullif(sy_dist, '')::double as host_dist_pc
from catalog_source
"""


CATALOGS: dict[str, CatalogSpec] = {
    "gaia_dr3_non_single_sample": CatalogSpec(
        name="gaia_dr3_non_single_sample",
        entity_type="star",
        path_parts=("cooked", "gaia_dr3_non_single_sample", "gaia_dr3_non_single_sample.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql=f"""
            select
              'gaia_dr3_non_single_sample' as catalog_name,
              'star' as entity_type,
              source_id as sample_key,
              cast(nullif(source_id, '') as bigint) as source_pk,
              cast(nullif(source_id, '') as bigint) as gaia_id,
              null::bigint as hip_id,
              null::bigint as hd_id,
              'Gaia DR3 ' || source_id as object_name,
              {normalize_name_sql("'Gaia DR3 ' || source_id")} as object_name_norm,
              cast(nullif(ra, '') as double) as ra_deg,
              cast(nullif(dec, '') as double) as dec_deg,
              case when cast(nullif(parallax, '') as double) > 0 then 1000.0 / cast(nullif(parallax, '') as double) else null end as dist_pc,
              null::double as x_helio_pc,
              null::double as y_helio_pc,
              null::double as z_helio_pc,
              cast(nullif(pmra, '') as double) as pm_ra_mas_yr,
              cast(nullif(pmdec, '') as double) as pm_dec_mas_yr,
              cast(nullif(radial_velocity, '') as double) as radial_velocity_kms,
              cast(nullif(phot_g_mean_mag, '') as double) as vmag,
              null::double as absmag,
              cast(nullif(bp_rp, '') as double) as color_index,
              null::varchar as spectral_type_raw,
              cast(nullif(parallax, '') as double) as parallax_mas,
              cast(nullif(parallax_error, '') as double) as parallax_error_mas,
              cast(nullif(teff_gspphot, '') as double) as teff_gspphot,
              cast(nullif(logg_gspphot, '') as double) as logg_gspphot,
              cast(nullif(mh_gspphot, '') as double) as mh_gspphot,
              cast(nullif(non_single_star, '') as int) as non_single_star,
              nullif(sample_origin, '') as sample_origin
            from catalog_source
        """,
        sample_columns=(
            "source_pk",
            "gaia_id",
            "object_name",
            "ra_deg",
            "dec_deg",
            "dist_pc",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "vmag",
            "color_index",
            "parallax_mas",
            "parallax_error_mas",
            "teff_gspphot",
            "logg_gspphot",
            "mh_gspphot",
            "non_single_star",
            "sample_origin",
        ),
        coverage_columns=(
            "gaia_id",
            "ra_deg",
            "dec_deg",
            "dist_pc",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "vmag",
            "color_index",
            "parallax_mas",
            "teff_gspphot",
            "logg_gspphot",
            "mh_gspphot",
            "non_single_star",
        ),
        overlap_fields=(OverlapField("gaia_id", "gaia_id", "gaia_id"),),
    ),
    "gaia_dr3_nss_two_body_sample": CatalogSpec(
        name="gaia_dr3_nss_two_body_sample",
        entity_type="multiple_star",
        path_parts=("cooked", "gaia_dr3_nss_two_body_sample", "gaia_dr3_nss_two_body_sample.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql="""
            select
              'gaia_dr3_nss_two_body_sample' as catalog_name,
              'multiple_star' as entity_type,
              source_id as sample_key,
              cast(nullif(source_id, '') as bigint) as source_pk,
              cast(nullif(source_id, '') as bigint) as gaia_id,
              nullif(nss_solution_type, '') as nss_solution_type,
              cast(nullif(ra, '') as double) as ra_deg,
              cast(nullif(dec, '') as double) as dec_deg,
              try_cast(nullif(parallax, '') as double) as parallax_mas,
              cast(nullif(pmra, '') as double) as pm_ra_mas_yr,
              cast(nullif(pmdec, '') as double) as pm_dec_mas_yr,
              try_cast(nullif(period, '') as double) as period_days,
              try_cast(nullif(eccentricity, '') as double) as eccentricity,
              try_cast(nullif(center_of_mass_velocity, '') as double) as center_of_mass_velocity,
              try_cast(nullif(semi_amplitude_primary, '') as double) as semi_amplitude_primary,
              try_cast(nullif(mass_ratio, '') as double) as mass_ratio,
              try_cast(nullif(inclination, '') as double) as inclination_deg,
              nullif(flags, '') as flags,
              try_cast(nullif(significance, '') as double) as significance,
              nullif(sample_origin, '') as sample_origin
            from catalog_source
        """,
        sample_columns=(
            "source_pk",
            "gaia_id",
            "nss_solution_type",
            "ra_deg",
            "dec_deg",
            "parallax_mas",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "period_days",
            "eccentricity",
            "center_of_mass_velocity",
            "semi_amplitude_primary",
            "mass_ratio",
            "inclination_deg",
            "flags",
            "significance",
            "sample_origin",
        ),
        coverage_columns=(
            "gaia_id",
            "nss_solution_type",
            "ra_deg",
            "dec_deg",
            "parallax_mas",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "period_days",
            "eccentricity",
            "center_of_mass_velocity",
            "semi_amplitude_primary",
            "mass_ratio",
            "inclination_deg",
            "flags",
            "significance",
        ),
        overlap_fields=(OverlapField("gaia_id", "gaia_id", "gaia_id"),),
    ),
    "gaia_dr3_sample": CatalogSpec(
        name="gaia_dr3_sample",
        entity_type="star",
        path_parts=("cooked", "gaia_dr3_sample", "gaia_dr3_sample.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql=f"""
            select
              'gaia_dr3_sample' as catalog_name,
              'star' as entity_type,
              source_id as sample_key,
              cast(nullif(source_id, '') as bigint) as source_pk,
              cast(nullif(source_id, '') as bigint) as gaia_id,
              null::bigint as hip_id,
              null::bigint as hd_id,
              'Gaia DR3 ' || source_id as object_name,
              {normalize_name_sql("'Gaia DR3 ' || source_id")} as object_name_norm,
              cast(nullif(ra, '') as double) as ra_deg,
              cast(nullif(dec, '') as double) as dec_deg,
              case when cast(nullif(parallax, '') as double) > 0 then 1000.0 / cast(nullif(parallax, '') as double) else null end as dist_pc,
              null::double as x_helio_pc,
              null::double as y_helio_pc,
              null::double as z_helio_pc,
              cast(nullif(pmra, '') as double) as pm_ra_mas_yr,
              cast(nullif(pmdec, '') as double) as pm_dec_mas_yr,
              cast(nullif(radial_velocity, '') as double) as radial_velocity_kms,
              cast(nullif(phot_g_mean_mag, '') as double) as vmag,
              null::double as absmag,
              cast(nullif(bp_rp, '') as double) as color_index,
              null::varchar as spectral_type_raw,
              cast(nullif(parallax, '') as double) as parallax_mas,
              cast(nullif(parallax_error, '') as double) as parallax_error_mas,
              cast(nullif(teff_gspphot, '') as double) as teff_gspphot,
              cast(nullif(logg_gspphot, '') as double) as logg_gspphot,
              cast(nullif(mh_gspphot, '') as double) as mh_gspphot,
              cast(nullif(non_single_star, '') as int) as non_single_star,
              nullif(sample_origin, '') as sample_origin
            from catalog_source
        """,
        sample_columns=(
            "source_pk",
            "gaia_id",
            "object_name",
            "object_name_norm",
            "ra_deg",
            "dec_deg",
            "dist_pc",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "vmag",
            "color_index",
            "parallax_mas",
            "parallax_error_mas",
            "teff_gspphot",
            "logg_gspphot",
            "mh_gspphot",
            "non_single_star",
            "sample_origin",
        ),
        coverage_columns=(
            "gaia_id",
            "ra_deg",
            "dec_deg",
            "dist_pc",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "vmag",
            "color_index",
            "parallax_mas",
            "parallax_error_mas",
            "teff_gspphot",
            "logg_gspphot",
            "mh_gspphot",
            "non_single_star",
        ),
        overlap_fields=(OverlapField("gaia_id", "gaia_id", "gaia_id"),),
    ),
    "athyg": CatalogSpec(
        name="athyg",
        entity_type="star",
        path_parts=("cooked", "athyg", "athyg.csv.gz"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              compression='gzip',
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql=ATHYG_NORMALIZED_SQL,
        sample_columns=(
            "source_pk",
            "gaia_id",
            "hip_id",
            "hd_id",
            "object_name",
            "object_name_norm",
            "ra_deg",
            "dec_deg",
            "dist_pc",
            "x_helio_pc",
            "y_helio_pc",
            "z_helio_pc",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "vmag",
            "absmag",
            "color_index",
            "spectral_type_raw",
            "pos_src",
            "dist_src",
            "pm_src",
            "rv_src",
            "spect_src",
        ),
        coverage_columns=(
            "gaia_id",
            "hip_id",
            "hd_id",
            "object_name",
            "object_name_norm",
            "ra_deg",
            "dec_deg",
            "dist_pc",
            "x_helio_pc",
            "y_helio_pc",
            "z_helio_pc",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "vmag",
            "spectral_type_raw",
        ),
        overlap_fields=(
            OverlapField("gaia_id", "gaia_id", "gaia_id"),
            OverlapField("hip_id", "hip_id", "hip_id"),
            OverlapField("hd_id", "hd_id", "hd_id"),
            OverlapField("object_name_norm", "star_name_norm", "name_norm"),
        ),
    ),
    "nasa_exoplanet_archive": CatalogSpec(
        name="nasa_exoplanet_archive",
        entity_type="planet",
        path_parts=("cooked", "nasa_exoplanet_archive", "pscomppars_clean.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql=NASA_NORMALIZED_SQL,
        sample_columns=(
            "source_pk",
            "object_name",
            "object_name_norm",
            "host_name_raw",
            "host_name_norm",
            "host_gaia_id",
            "host_hip_id",
            "host_hd_id",
            "discovery_method",
            "disc_year",
            "orbital_period_days",
            "semi_major_axis_au",
            "eccentricity",
            "inclination_deg",
            "radius_earth",
            "radius_jup",
            "mass_earth",
            "mass_jup",
            "eq_temp_k",
            "insol_earth",
            "host_dist_pc",
        ),
        coverage_columns=(
            "object_name",
            "host_name_raw",
            "host_name_norm",
            "host_gaia_id",
            "host_hip_id",
            "host_hd_id",
            "orbital_period_days",
            "semi_major_axis_au",
            "eccentricity",
            "radius_earth",
            "mass_earth",
            "eq_temp_k",
            "insol_earth",
            "host_dist_pc",
        ),
        overlap_fields=(
            OverlapField("host_gaia_id", "gaia_id", "host_gaia_id"),
            OverlapField("host_hip_id", "hip_id", "host_hip_id"),
            OverlapField("host_hd_id", "hd_id", "host_hd_id"),
            OverlapField("host_name_norm", "star_name_norm", "host_name_norm"),
        ),
    ),
    "debcat": CatalogSpec(
        name="debcat",
        entity_type="multiple_star",
        path_parts=("cooked", "debcat", "debcat_binaries.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql=f"""
            select
              'debcat' as catalog_name,
              'multiple_star' as entity_type,
              coalesce(
                {normalize_name_sql("nullif(system_name, '')")},
                md5(coalesce(nullif(system_name, ''), '') || '|' || coalesce(nullif(period_days, ''), ''))
              ) as sample_key,
              null::bigint as source_pk,
              nullif(system_name, '') as system_name,
              {normalize_name_sql("nullif(system_name, '')")} as system_name_norm,
              nullif(spectral_type_primary, '') as spectral_type_primary,
              nullif(spectral_type_secondary, '') as spectral_type_secondary,
              try_cast(nullif(period_days, '') as double) as period_days,
              try_cast(nullif(vmag, '') as double) as vmag,
              try_cast(nullif(b_minus_v, '') as double) as b_minus_v,
              try_cast(nullif(mass_primary_msun, '') as double) as mass_primary_msun,
              try_cast(nullif(mass_secondary_msun, '') as double) as mass_secondary_msun,
              try_cast(nullif(radius_primary_rsun, '') as double) as radius_primary_rsun,
              try_cast(nullif(radius_secondary_rsun, '') as double) as radius_secondary_rsun,
              try_cast(nullif(teff_primary_k, '') as double) as teff_primary_k,
              try_cast(nullif(teff_secondary_k, '') as double) as teff_secondary_k,
              try_cast(nullif(metallicity_dex, '') as double) as metallicity_dex
            from catalog_source
        """,
        sample_columns=(
            "system_name",
            "system_name_norm",
            "spectral_type_primary",
            "spectral_type_secondary",
            "period_days",
            "vmag",
            "b_minus_v",
            "mass_primary_msun",
            "mass_secondary_msun",
            "radius_primary_rsun",
            "radius_secondary_rsun",
            "teff_primary_k",
            "teff_secondary_k",
            "metallicity_dex",
        ),
        coverage_columns=(
            "system_name",
            "period_days",
            "vmag",
            "b_minus_v",
            "mass_primary_msun",
            "mass_secondary_msun",
            "radius_primary_rsun",
            "radius_secondary_rsun",
            "teff_primary_k",
            "teff_secondary_k",
            "metallicity_dex",
            "spectral_type_primary",
        ),
        overlap_fields=(OverlapField("system_name_norm", "star_name_norm", "system_name_norm"),),
    ),
    "kepler_eb": CatalogSpec(
        name="kepler_eb",
        entity_type="multiple_star",
        path_parts=("cooked", "kepler_eb", "kepler_eb_catalog.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql="""
            select
              'kepler_eb' as catalog_name,
              'multiple_star' as entity_type,
              cast(nullif(kic_id, '') as varchar) as sample_key,
              try_cast(nullif(kic_id, '') as bigint) as source_pk,
              try_cast(nullif(kic_id, '') as bigint) as kic_id,
              try_cast(nullif(period_days, '') as double) as period_days,
              try_cast(nullif(period_error_days, '') as double) as period_error_days,
              try_cast(nullif(bjd0, '') as double) as bjd0,
              try_cast(nullif(bjd0_error, '') as double) as bjd0_error,
              try_cast(nullif(morphology, '') as double) as morphology,
              try_cast(nullif(glon_deg, '') as double) as glon_deg,
              try_cast(nullif(glat_deg, '') as double) as glat_deg,
              try_cast(nullif(kmag, '') as double) as kmag,
              try_cast(nullif(teff_k, '') as double) as teff_k,
              case
                when lower(nullif(has_short_cadence, '')) in ('1', 'true', 't', 'yes', 'y') then true
                when lower(nullif(has_short_cadence, '')) in ('0', 'false', 'f', 'no', 'n') then false
                else null
              end as has_short_cadence
            from catalog_source
        """,
        sample_columns=(
            "kic_id",
            "period_days",
            "period_error_days",
            "bjd0",
            "bjd0_error",
            "morphology",
            "glon_deg",
            "glat_deg",
            "kmag",
            "teff_k",
            "has_short_cadence",
        ),
        coverage_columns=(
            "kic_id",
            "period_days",
            "period_error_days",
            "bjd0",
            "bjd0_error",
            "morphology",
            "glon_deg",
            "glat_deg",
            "kmag",
            "teff_k",
            "has_short_cadence",
        ),
        overlap_fields=(OverlapField("kic_id", "kic_id", "kic_id"),),
    ),
    "tess_eb": CatalogSpec(
        name="tess_eb",
        entity_type="multiple_star",
        path_parts=("cooked", "tess_eb", "tess_eb_catalog.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql="""
            select
              'tess_eb' as catalog_name,
              'multiple_star' as entity_type,
              cast(nullif(tic_id, '') as varchar) as sample_key,
              try_cast(nullif(tic_id, '') as bigint) as source_pk,
              try_cast(nullif(tic_id, '') as bigint) as tic_id,
              nullif(sectors, '') as sectors,
              try_cast(nullif(ra_deg, '') as double) as ra_deg,
              try_cast(nullif(dec_deg, '') as double) as dec_deg,
              try_cast(nullif(glon_deg, '') as double) as glon_deg,
              try_cast(nullif(glat_deg, '') as double) as glat_deg,
              try_cast(nullif(pm_ra_mas_yr, '') as double) as pm_ra_mas_yr,
              try_cast(nullif(pm_dec_mas_yr, '') as double) as pm_dec_mas_yr,
              try_cast(nullif(tmag, '') as double) as tmag,
              try_cast(nullif(teff_k, '') as double) as teff_k,
              try_cast(nullif(logg_cgs, '') as double) as logg_cgs,
              try_cast(nullif(metallicity_dex, '') as double) as metallicity_dex,
              try_cast(nullif(bjd0, '') as double) as bjd0,
              try_cast(nullif(bjd0_error, '') as double) as bjd0_error,
              try_cast(nullif(period_days, '') as double) as period_days,
              try_cast(nullif(period_error_days, '') as double) as period_error_days,
              try_cast(nullif(morphology, '') as double) as morphology,
              nullif(source, '') as source,
              nullif(flags, '') as flags
            from catalog_source
        """,
        sample_columns=(
            "tic_id",
            "sectors",
            "period_days",
            "period_error_days",
            "bjd0",
            "bjd0_error",
            "morphology",
            "ra_deg",
            "dec_deg",
            "glon_deg",
            "glat_deg",
            "tmag",
            "teff_k",
            "logg_cgs",
            "metallicity_dex",
            "source",
            "flags",
        ),
        coverage_columns=(
            "tic_id",
            "period_days",
            "period_error_days",
            "bjd0",
            "bjd0_error",
            "morphology",
            "ra_deg",
            "dec_deg",
            "glon_deg",
            "glat_deg",
            "tmag",
            "teff_k",
            "logg_cgs",
            "metallicity_dex",
            "source",
            "flags",
        ),
        overlap_fields=(),
    ),
    "wds": CatalogSpec(
        name="wds",
        entity_type="multiple_star",
        path_parts=("cooked", "wds", "wds_summary.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql="""
            select
              'wds' as catalog_name,
              'multiple_star' as entity_type,
              coalesce(wds_id, precise_coordinate) as sample_key,
              null::bigint as source_pk,
              wds_id,
              discoverer,
              component,
              dm_designation,
              note,
              precise_coordinate,
              try_cast(nullif(ra_deg, '') as double) as ra_deg,
              try_cast(nullif(dec_deg, '') as double) as dec_deg,
              try_cast(nullif(mag_primary, '') as double) as mag_primary,
              try_cast(nullif(mag_secondary, '') as double) as mag_secondary,
              nullif(spectral_type_raw, '') as spectral_type_raw,
              try_cast(nullif(pm_primary_ra, '') as double) as pm_primary_ra,
              try_cast(nullif(pm_primary_dec, '') as double) as pm_primary_dec,
              try_cast(nullif(pm_secondary_ra, '') as double) as pm_secondary_ra,
              try_cast(nullif(pm_secondary_dec, '') as double) as pm_secondary_dec,
              try_cast(nullif(first_year, '') as int) as first_year,
              try_cast(nullif(last_year, '') as int) as last_year,
              try_cast(nullif(obs_count, '') as int) as obs_count,
              try_cast(nullif(theta_first_deg, '') as double) as theta_first_deg,
              try_cast(nullif(theta_last_deg, '') as double) as theta_last_deg,
              try_cast(nullif(rho_first_arcsec, '') as double) as rho_first_arcsec,
              try_cast(nullif(rho_last_arcsec, '') as double) as rho_last_arcsec
            from catalog_source
        """,
        sample_columns=(
            "wds_id",
            "discoverer",
            "component",
            "dm_designation",
            "note",
            "precise_coordinate",
            "ra_deg",
            "dec_deg",
            "mag_primary",
            "mag_secondary",
            "spectral_type_raw",
            "pm_primary_ra",
            "pm_primary_dec",
            "pm_secondary_ra",
            "pm_secondary_dec",
            "first_year",
            "last_year",
            "obs_count",
            "theta_first_deg",
            "theta_last_deg",
            "rho_first_arcsec",
            "rho_last_arcsec",
        ),
        coverage_columns=(
            "wds_id",
            "discoverer",
            "component",
            "precise_coordinate",
            "ra_deg",
            "dec_deg",
            "mag_primary",
            "mag_secondary",
            "spectral_type_raw",
            "pm_primary_ra",
            "pm_primary_dec",
            "first_year",
            "last_year",
            "obs_count",
            "rho_first_arcsec",
            "rho_last_arcsec",
        ),
        overlap_fields=(),
    ),
    "msc": CatalogSpec(
        name="msc",
        entity_type="multiple_star",
        path_parts=("cooked", "msc", "msc_components.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql="""
            select
              'msc' as catalog_name,
              'multiple_star' as entity_type,
              coalesce(wds_id || ':' || component, wds_id) as sample_key,
              null::bigint as source_pk,
              nullif(wds_id, '') as wds_id,
              nullif(component, '') as component,
              try_cast(nullif(ra_deg, '') as double) as ra_deg,
              try_cast(nullif(dec_deg, '') as double) as dec_deg,
              try_cast(nullif(parallax_mas, '') as double) as parallax_mas,
              nullif(parallax_ref, '') as parallax_ref,
              try_cast(nullif(pm_ra_mas_yr, '') as double) as pm_ra_mas_yr,
              try_cast(nullif(pm_dec_mas_yr, '') as double) as pm_dec_mas_yr,
              try_cast(nullif(radial_velocity_kms, '') as double) as radial_velocity_kms,
              try_cast(nullif(sep_arcsec, '') as double) as sep_arcsec,
              nullif(spectral_type_raw, '') as spectral_type_raw,
              try_cast(nullif(hip_id, '') as bigint) as hip_id,
              try_cast(nullif(hd_id, '') as bigint) as hd_id,
              try_cast(nullif(vmag, '') as double) as vmag,
              try_cast(nullif(ncomp, '') as int) as ncomp,
              try_cast(nullif(grade, '') as int) as grade,
              nullif(other_identifiers, '') as other_identifiers,
              try_cast(nullif(subsystem_count, '') as int) as subsystem_count,
              try_cast(nullif(orbit_count, '') as int) as orbit_count
            from catalog_source
        """,
        sample_columns=(
            "wds_id",
            "component",
            "ra_deg",
            "dec_deg",
            "parallax_mas",
            "parallax_ref",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "sep_arcsec",
            "spectral_type_raw",
            "hip_id",
            "hd_id",
            "vmag",
            "ncomp",
            "grade",
            "subsystem_count",
            "orbit_count",
        ),
        coverage_columns=(
            "wds_id",
            "ra_deg",
            "dec_deg",
            "parallax_mas",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "radial_velocity_kms",
            "spectral_type_raw",
            "hip_id",
            "hd_id",
            "vmag",
            "ncomp",
            "grade",
            "subsystem_count",
            "orbit_count",
        ),
        overlap_fields=(
            OverlapField("hip_id", "hip_id", "hip_id"),
            OverlapField("hd_id", "hd_id", "hd_id"),
        ),
    ),
    "orb6": CatalogSpec(
        name="orb6",
        entity_type="multiple_star",
        path_parts=("cooked", "orb6", "orb6_orbits.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql="""
            select
              'orb6' as catalog_name,
              'multiple_star' as entity_type,
              coalesce(wds_id, discoverer) as sample_key,
              null::bigint as source_pk,
              nullif(wds_id, '') as wds_id,
              nullif(discoverer, '') as discoverer,
              try_cast(nullif(ads_id, '') as bigint) as ads_id,
              try_cast(nullif(hd_id, '') as bigint) as hd_id,
              try_cast(nullif(hip_id, '') as bigint) as hip_id,
              try_cast(nullif(ra_deg, '') as double) as ra_deg,
              try_cast(nullif(dec_deg, '') as double) as dec_deg,
              try_cast(nullif(mag_primary, '') as double) as mag_primary,
              try_cast(nullif(mag_secondary, '') as double) as mag_secondary,
              try_cast(nullif(period_value, '') as double) as period_value,
              nullif(period_unit, '') as period_unit,
              try_cast(nullif(period_error, '') as double) as period_error,
              try_cast(nullif(semi_major_axis_arcsec, '') as double) as semi_major_axis_arcsec,
              try_cast(nullif(inclination_deg, '') as double) as inclination_deg,
              try_cast(nullif(node_deg, '') as double) as node_deg,
              try_cast(nullif(periastron_epoch, '') as double) as periastron_epoch,
              nullif(epoch_unit, '') as epoch_unit,
              try_cast(nullif(eccentricity, '') as double) as eccentricity,
              nullif(reference_code, '') as reference_code,
              try_cast(nullif(last_observed_year, '') as int) as last_observed_year,
              try_cast(nullif(grade, '') as int) as grade
            from catalog_source
        """,
        sample_columns=(
            "wds_id",
            "discoverer",
            "ads_id",
            "hd_id",
            "hip_id",
            "ra_deg",
            "dec_deg",
            "mag_primary",
            "mag_secondary",
            "period_value",
            "period_unit",
            "period_error",
            "semi_major_axis_arcsec",
            "inclination_deg",
            "node_deg",
            "periastron_epoch",
            "epoch_unit",
            "eccentricity",
            "last_observed_year",
            "grade",
            "reference_code",
        ),
        coverage_columns=(
            "wds_id",
            "hd_id",
            "hip_id",
            "ra_deg",
            "dec_deg",
            "mag_primary",
            "mag_secondary",
            "period_value",
            "semi_major_axis_arcsec",
            "inclination_deg",
            "eccentricity",
            "last_observed_year",
            "grade",
            "reference_code",
        ),
        overlap_fields=(
            OverlapField("hip_id", "hip_id", "hip_id"),
            OverlapField("hd_id", "hd_id", "hd_id"),
        ),
    ),
    "sbx_sample": CatalogSpec(
        name="sbx_sample",
        entity_type="multiple_star",
        path_parts=("cooked", "sbx_sample", "sbx_sample.csv"),
        source_sql="""
            select * from read_csv_auto(
              {path},
              delim=',',
              quote='\"',
              escape='\"',
              header=true,
              strict_mode=false,
              null_padding=true,
              all_varchar=true
            )
        """,
        normalized_sql="""
            select
              'sbx_sample' as catalog_name,
              'multiple_star' as entity_type,
              cast(nullif(sn, '') as varchar) as sample_key,
              try_cast(nullif(sn, '') as bigint) as source_pk,
              try_cast(nullif(sn, '') as bigint) as sn,
              try_cast(nullif(gaia_id, '') as bigint) as gaia_id,
              try_cast(nullif(hip_id, '') as bigint) as hip_id,
              try_cast(nullif(hd_id, '') as bigint) as hd_id,
              nullif(wds_id, '') as wds_id,
              try_cast(nullif(ads_id, '') as bigint) as ads_id,
              try_cast(nullif(ra_deg, '') as double) as ra_deg,
              try_cast(nullif(dec_deg, '') as double) as dec_deg,
              try_cast(nullif(parallax_mas, '') as double) as parallax_mas,
              try_cast(nullif(pm_ra_mas_yr, '') as double) as pm_ra_mas_yr,
              try_cast(nullif(pm_dec_mas_yr, '') as double) as pm_dec_mas_yr,
              try_cast(nullif(mag_primary, '') as double) as mag_primary,
              try_cast(nullif(position_epoch, '') as double) as position_epoch,
              nullif(position_source, '') as position_source,
              nullif(spectral_type_raw, '') as spectral_type_raw,
              nullif(family, '') as family,
              nullif(parent, '') as parent,
              nullif(child1, '') as child1,
              nullif(child2, '') as child2,
              try_cast(nullif(in_triple, '') as int) as in_triple,
              try_cast(nullif(orbit_count, '') as int) as orbit_count,
              nullif(sample_origin, '') as sample_origin
            from catalog_source
        """,
        sample_columns=(
            "source_pk",
            "gaia_id",
            "hip_id",
            "hd_id",
            "wds_id",
            "ads_id",
            "ra_deg",
            "dec_deg",
            "parallax_mas",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "mag_primary",
            "position_epoch",
            "position_source",
            "spectral_type_raw",
            "family",
            "parent",
            "child1",
            "child2",
            "in_triple",
            "orbit_count",
            "sample_origin",
        ),
        coverage_columns=(
            "gaia_id",
            "hip_id",
            "hd_id",
            "wds_id",
            "ra_deg",
            "dec_deg",
            "parallax_mas",
            "pm_ra_mas_yr",
            "pm_dec_mas_yr",
            "mag_primary",
            "position_epoch",
            "spectral_type_raw",
            "family",
            "in_triple",
            "orbit_count",
        ),
        overlap_fields=(
            OverlapField("gaia_id", "gaia_id", "gaia_id"),
            OverlapField("hip_id", "hip_id", "hip_id"),
            OverlapField("hd_id", "hd_id", "hd_id"),
            OverlapField("wds_id", "wds_id", "wds_id"),
        ),
    ),
}


def default_state_dir(root: Path) -> Path:
    state_dir = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if state_dir:
        return Path(state_dir)
    return root / "data"


def load_catalog_view(con: duckdb.DuckDBPyConnection, spec: CatalogSpec, path: Path) -> str:
    source_sql = spec.source_sql.format(path=sql_quote(str(path)))
    con.execute(f"create or replace temp view catalog_source as {source_sql}")
    table_name = f"catalog_{spec.name}"
    con.execute(f"create or replace temp table {table_name} as {spec.normalized_sql}")
    return table_name


def prepare_core_overlap_tables(con: duckdb.DuckDBPyConnection, core_db_attached: bool) -> None:
    if not core_db_attached:
        return
    con.execute(
        """
        create or replace temp table core_key_gaia as
        select distinct gaia_id as key_value
        from core.stars
        where gaia_id is not null
        """
    )
    con.execute(
        """
        create or replace temp table core_key_hip as
        select distinct hip_id as key_value
        from core.stars
        where hip_id is not null
        """
    )
    con.execute(
        """
        create or replace temp table core_key_hd as
        select distinct hd_id as key_value
        from core.stars
        where hd_id is not null
        """
    )
    con.execute(
        """
        create or replace temp table core_key_name as
        select distinct star_name_norm as key_value
        from core.stars
        where star_name_norm is not null and trim(star_name_norm) <> ''
        """
    )
    con.execute(
        """
        create or replace temp table core_key_wds as
        select distinct wds_id as key_value
        from core.stars
        where wds_id is not null and trim(wds_id) <> ''
        """
    )
    has_object_identifiers = (
        con.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = 'core' and table_name = 'object_identifiers'
            """
        ).fetchone()[0]
        > 0
    )
    if has_object_identifiers:
        con.execute(
            """
            create or replace temp table core_key_kic as
            select distinct try_cast(id_value_norm as bigint) as key_value
            from core.object_identifiers
            where lower(namespace) = 'kic'
              and id_value_norm is not null
              and trim(id_value_norm) <> ''
              and try_cast(id_value_norm as bigint) is not null
            """
        )
    else:
        con.execute(
            """
            create or replace temp table core_key_kic as
            select cast(null as bigint) as key_value
            where false
            """
        )


def core_overlap_table(core_column: str) -> str:
    if core_column == "gaia_id":
        return "core_key_gaia"
    if core_column == "hip_id":
        return "core_key_hip"
    if core_column == "hd_id":
        return "core_key_hd"
    if core_column == "star_name_norm":
        return "core_key_name"
    if core_column == "wds_id":
        return "core_key_wds"
    if core_column == "kic_id":
        return "core_key_kic"
    raise ValueError(f"Unsupported core overlap column: {core_column}")


def build_overlap_query(spec: CatalogSpec, view_name: str, limit: int, seed: str) -> str:
    if not spec.overlap_fields:
        return ""
    joins = []
    flag_columns = []
    score_terms = []
    for index, field in enumerate(spec.overlap_fields):
        alias = f"match_{field.label}"
        join_alias = f"j{index}"
        joins.append(
            f"left join {core_overlap_table(field.core_column)} {join_alias} "
            f"on src.{field.field_name} = {join_alias}.key_value"
        )
        flag_columns.append(
            f"case when src.{field.field_name} is not null and {join_alias}.key_value is not null then 1 else 0 end as {alias}"
        )
        score_terms.append(alias)

    score_sql = " + ".join(score_terms)
    select_cols = ", ".join(["src." + col for col in spec.sample_columns])
    match_cols = ", ".join([f"flags.match_{field.label}" for field in spec.overlap_fields])
    return f"""
        with flags as (
          select
            src.sample_key,
            {", ".join(flag_columns)}
          from {view_name} src
          {' '.join(joins)}
        ), ranked as (
          select
            src.catalog_name,
            src.entity_type,
            src.sample_key,
            {select_cols},
            {match_cols},
            ({score_sql}) as overlap_score
          from {view_name} src
          join flags using (sample_key)
          where ({score_sql}) > 0
        )
        select *
        from ranked
        order by overlap_score desc, md5(coalesce(sample_key, '') || '|' || {sql_quote(seed)}) asc
        limit {limit}
    """


def write_csv(con: duckdb.DuckDBPyConnection, query: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy ({query}) to {sql_quote(str(dest))} (header, delimiter ',')")


def coverage_summary(con: duckdb.DuckDBPyConnection, view_name: str, columns: tuple[str, ...]) -> dict:
    total_rows = con.execute(f"select count(*) from {view_name}").fetchone()[0]
    coverage = {}
    for column in columns:
        non_null = con.execute(
            f"select count(*) from {view_name} where {column} is not null"
        ).fetchone()[0]
        coverage[column] = {
            "non_null_rows": int(non_null),
            "coverage_pct": round((non_null / total_rows * 100.0), 2) if total_rows else 0.0,
        }
    return {"row_count": int(total_rows), "columns": coverage}


def overlap_summary(
    con: duckdb.DuckDBPyConnection, spec: CatalogSpec, view_name: str, core_db_attached: bool
) -> dict:
    if not core_db_attached or not spec.overlap_fields:
        return {"core_db_attached": core_db_attached, "matched_rows": 0, "fields": {}}

    summary = {"core_db_attached": True, "matched_rows": 0, "fields": {}}
    match_predicates = []
    for index, field in enumerate(spec.overlap_fields):
        join_alias = f"j{index}"
        predicate = f"src.{field.field_name} is not null and {join_alias}.key_value is not null"
        count = con.execute(
            f"""
            select count(*)
            from {view_name} src
            left join {core_overlap_table(field.core_column)} {join_alias}
              on src.{field.field_name} = {join_alias}.key_value
            where {predicate}
            """
        ).fetchone()[0]
        summary["fields"][field.label] = int(count)
        match_predicates.append(f"({predicate})")
    if match_predicates:
        summary["matched_rows"] = int(
            con.execute(
                f"""
                select count(*)
                from {view_name} src
                {' '.join([
                    f"left join {core_overlap_table(field.core_column)} j{idx} on src.{field.field_name} = j{idx}.key_value"
                    for idx, field in enumerate(spec.overlap_fields)
                ])}
                where {' or '.join(match_predicates)}
                """
            ).fetchone()[0]
        )
    return summary


def coverage_avg_pct(coverage: dict) -> float:
    columns = coverage.get("columns", {})
    if not columns:
        return 0.0
    values = [float(stats.get("coverage_pct", 0.0)) for stats in columns.values()]
    return round(sum(values) / len(values), 2)


def assess_catalog(spec: CatalogSpec, coverage: dict, overlap: dict) -> dict:
    row_count = int(coverage.get("row_count", 0))
    matched_rows = int(overlap.get("matched_rows", 0))
    avg_coverage_pct = coverage_avg_pct(coverage)
    overlap_pct = round((matched_rows / row_count) * 100.0, 2) if row_count else 0.0
    estimated_novel_rows = max(row_count - matched_rows, 0)
    novelty_pct = round((estimated_novel_rows / row_count) * 100.0, 2) if row_count else 0.0

    reliability = float(CATALOG_RELIABILITY_SCORES.get(spec.name, 0.75))
    coverage_score = avg_coverage_pct / 100.0
    overlap_score = overlap_pct / 100.0
    composite = round(
        (
            0.50 * coverage_score
            + 0.30 * overlap_score
            + 0.20 * reliability
        )
        * 100.0,
        2,
    )

    if row_count == 0:
        tier = "meh"
        rationale = "no rows available"
    elif spec.name in CATALOG_REQUIRES_CROSSMATCH:
        tier = "needs_crossmatch"
        rationale = "coordinate-led catalog; use multiplicity crossmatch report before policy decisions"
    elif overlap_pct >= 20.0 and avg_coverage_pct >= 65.0:
        tier = "indispensable"
        rationale = "high overlap and high field coverage"
    elif overlap_pct >= 8.0 and avg_coverage_pct >= 55.0:
        tier = "strong"
        rationale = "usable overlap with solid field coverage"
    elif avg_coverage_pct >= 45.0:
        tier = "situational"
        rationale = "good intrinsic data but currently low linkage"
    else:
        tier = "meh"
        rationale = "limited linkage and limited effective coverage"

    override_tier = CATALOG_TIER_OVERRIDE.get(spec.name)
    if override_tier:
        tier = override_tier
        rationale = "policy override"

    return {
        "avg_coverage_pct": avg_coverage_pct,
        "overlap_pct": overlap_pct,
        "estimated_novel_rows": int(estimated_novel_rows),
        "novelty_pct": novelty_pct,
        "reliability_score": round(reliability * 100.0, 1),
        "composite_score": composite,
        "tier": tier,
        "tier_rationale": rationale,
    }


def pick_catalogs(catalog_names: list[str], state_dir: Path) -> list[CatalogSpec]:
    if catalog_names:
        specs = []
        for name in catalog_names:
            if name not in CATALOGS:
                raise SystemExit(f"Unknown catalog: {name}")
            specs.append(CATALOGS[name])
        return specs

    available = []
    for spec in CATALOGS.values():
        if spec.resolve_path(state_dir).exists():
            available.append(spec)
    if not available:
        raise SystemExit("No built-in catalog sample sources are available locally.")
    return available


def render_markdown(run_id: str, catalog_reports: list[dict]) -> str:
    tier_rank = {
        "indispensable": 4,
        "strong": 3,
        "situational": 2,
        "meh": 1,
        "needs_crossmatch": 0,
    }
    ranked_reports = sorted(
        catalog_reports,
        key=lambda report: (
            tier_rank.get(report["assessment"]["tier"], -1),
            report["assessment"]["composite_score"],
        ),
        reverse=True,
    )
    lines = [
        "# Catalog Evaluation Summary",
        "",
        f"- Run ID: `{run_id}`",
        f"- Generated at: `{iso_utc(utc_now())}`",
        "",
        "## Catalog Contribution Ranking",
        "",
        "| Catalog | Tier | Composite | Avg Coverage | Overlap | Estimated Novel Rows | Reliability |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for report in ranked_reports:
        assessment = report["assessment"]
        lines.append(
            "| {catalog} | {tier} | {composite:.2f} | {coverage:.2f}% | {overlap:.2f}% | {novel:,} | {reliability:.1f}% |".format(
                catalog=report["catalog"],
                tier=assessment["tier"],
                composite=assessment["composite_score"],
                coverage=assessment["avg_coverage_pct"],
                overlap=assessment["overlap_pct"],
                novel=assessment["estimated_novel_rows"],
                reliability=assessment["reliability_score"],
            )
        )

    lines.extend(
        [
            "",
            "Legend:",
            "- `indispensable`: keep by default",
            "- `strong`: high-value support source",
            "- `situational`: useful but currently limited linkage/coverage",
            "- `meh`: low present value",
            "- `needs_crossmatch`: coordinate-led; decide after multiplicity crossmatch",
            "",
        ]
    )

    for report in ranked_reports:
        assessment = report["assessment"]
        lines.extend(
            [
                f"## {report['catalog']}",
                "",
                f"- Entity type: `{report['entity_type']}`",
                f"- Source path: `{report['source_path']}`",
                f"- Rows scanned: `{report['coverage']['row_count']}`",
                f"- Core-overlap rows: `{report['overlap']['matched_rows']}`",
                f"- Tier: `{assessment['tier']}` ({assessment['tier_rationale']})",
                f"- Composite score: `{assessment['composite_score']}`",
                f"- Avg coverage: `{assessment['avg_coverage_pct']}%`",
                f"- Overlap: `{assessment['overlap_pct']}%`",
                f"- Estimated novel rows: `{assessment['estimated_novel_rows']}` ({assessment['novelty_pct']}%)",
                f"- Reliability score: `{assessment['reliability_score']}%`",
                "",
                "### Field Coverage",
                "",
            ]
        )
        for column, stats in report["coverage"]["columns"].items():
            lines.append(
                f"- `{column}`: {stats['non_null_rows']} rows ({stats['coverage_pct']}%)"
            )
        if report["overlap"]["fields"]:
            lines.extend(["", "### Core Overlap Keys", ""])
            for key, count in report["overlap"]["fields"].items():
                lines.append(f"- `{key}`: {count} rows")
        lines.extend(
            [
                "",
                "### Sample Files",
                "",
                f"- Random sample: `{report['random_sample_path']}`",
                f"- Overlap sample: `{report['overlap_sample_path']}`",
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sample and summarize candidate catalogs for v1.2 precedence work."
    )
    parser.add_argument(
        "--catalog",
        action="append",
        default=[],
        help="Catalog name to evaluate. Defaults to all locally available built-ins.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Rows to emit for deterministic random and overlap samples (default: 100).",
    )
    parser.add_argument(
        "--seed",
        default="spacegate-v1-2",
        help="Deterministic sample seed.",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional explicit output directory. Defaults under $SPACEGATE_STATE_DIR/reports/catalog_eval/<run_id>/.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    init_env(root)
    state_dir = default_state_dir(root)
    run_id = utc_now().strftime("%Y-%m-%dT%H%M%SZ")
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else state_dir / "reports" / "catalog_eval" / run_id
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    core_db_path = state_dir / "served" / "current" / "core.duckdb"
    con = duckdb.connect(database=":memory:")
    core_db_attached = core_db_path.exists()
    if core_db_attached:
        con.execute(f"attach {sql_quote(str(core_db_path))} as core")
        prepare_core_overlap_tables(con, core_db_attached)

    reports = []
    for spec in pick_catalogs(args.catalog, state_dir):
        source_path = spec.resolve_path(state_dir)
        if not source_path.exists():
            raise SystemExit(f"Missing source for {spec.name}: {source_path}")

        view_name = load_catalog_view(con, spec, source_path)
        random_query = f"""
            select
              catalog_name,
              entity_type,
              sample_key,
              {", ".join(spec.sample_columns)}
            from {view_name}
            order by md5(coalesce(sample_key, '') || '|' || {sql_quote(args.seed)}) asc
            limit {args.sample_size}
        """
        overlap_query = build_overlap_query(spec, view_name, args.sample_size, args.seed)
        random_sample_path = output_dir / f"{spec.name}_random_sample.csv"
        overlap_sample_path = output_dir / f"{spec.name}_overlap_sample.csv"

        write_csv(con, random_query, random_sample_path)
        if core_db_attached and overlap_query:
            write_csv(con, overlap_query, overlap_sample_path)
        else:
            overlap_sample_path.write_text("core overlap unavailable\n")

        coverage = coverage_summary(con, view_name, spec.coverage_columns)
        overlap = overlap_summary(con, spec, view_name, core_db_attached)
        assessment = assess_catalog(spec, coverage, overlap)
        report = {
            "catalog": spec.name,
            "entity_type": spec.entity_type,
            "source_path": str(source_path),
            "coverage": coverage,
            "overlap": overlap,
            "assessment": assessment,
            "random_sample_path": str(random_sample_path),
            "overlap_sample_path": str(overlap_sample_path),
        }
        reports.append(report)
        (output_dir / f"{spec.name}_summary.json").write_text(
            json.dumps(report, indent=2) + "\n"
        )
        con.execute(f"drop table {view_name}")
        con.execute("drop view catalog_source")

    tier_rank = {
        "indispensable": 4,
        "strong": 3,
        "situational": 2,
        "meh": 1,
        "needs_crossmatch": 0,
    }
    ranking = sorted(
        [
            {
                "catalog": report["catalog"],
                "tier": report["assessment"]["tier"],
                "composite_score": report["assessment"]["composite_score"],
                "avg_coverage_pct": report["assessment"]["avg_coverage_pct"],
                "overlap_pct": report["assessment"]["overlap_pct"],
                "estimated_novel_rows": report["assessment"]["estimated_novel_rows"],
            }
            for report in reports
        ],
        key=lambda row: (tier_rank.get(row["tier"], -1), row["composite_score"]),
        reverse=True,
    )
    summary = {"run_id": run_id, "catalogs": reports, "ranking": ranking}
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (output_dir / "summary.md").write_text(render_markdown(run_id, reports))
    print(str(output_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
