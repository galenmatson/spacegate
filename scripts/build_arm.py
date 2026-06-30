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

MSC_VERSION_FALLBACK = "2026-06-19"
WDS_VERSION_FALLBACK = "wdsweb_summ2"
ORB6_VERSION_FALLBACK = "orb6orbits"
VSX_VERSION_FALLBACK = "vsx_dat"
VSX_URL_FALLBACK = "https://cdsarc.cds.unistra.fr/ftp/B/vsx/vsx.dat"
ULTRACOOLSHEET_VERSION_FALLBACK = "UltracoolSheet_Main"
ULTRACOOLSHEET_URL_FALLBACK = (
    "https://docs.google.com/spreadsheets/d/1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8/"
    "gviz/tq?tqx=out:csv&sheet=Main"
)
SOL_AUTHORITY_VERSION_FALLBACK = "horizons_s1"
SOL_AUTHORITY_URL_FALLBACK = "https://ssd.jpl.nasa.gov/api/horizons.api"
SOL_ARTIFICIAL_VERSION_FALLBACK = "horizons_s4"
SOL_ARTIFICIAL_URL_FALLBACK = "https://ssd.jpl.nasa.gov/api/horizons.api"
GAIA_BACKBONE_VERSION_FALLBACK = "gaia_dr3_backbone"
GAIA_BACKBONE_URL_FALLBACK = "https://gea.esac.esa.int/archive/"
GAIA_CLASSPROB_VERSION_FALLBACK = "gaia_dr3_astrophysical_classprob"
GAIA_CLASSPROB_URL_FALLBACK = "https://gea.esac.esa.int/archive/"
GAIA_NSS_VERSION_FALLBACK = "gaia_dr3_nss_two_body_orbit"
GAIA_NSS_URL_FALLBACK = "https://gea.esac.esa.int/archive/"
NASA_PSCOMPPARS_VERSION_FALLBACK = "pscomppars"
NASA_PSCOMPPARS_URL_FALLBACK = "https://exoplanetarchive.ipac.caltech.edu"
DERIVED_PHYSICAL_PARAMETERS_VERSION = "derived_physical_parameters_v1"


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
    cooked_msc_systems = state_dir / "cooked" / "msc" / "msc_systems.csv"
    cooked_msc_orbits = state_dir / "cooked" / "msc" / "msc_orbits.csv"
    cooked_wds = state_dir / "cooked" / "wds" / "wds_summary.csv"
    cooked_orb6 = state_dir / "cooked" / "orb6" / "orb6_orbits.csv"
    cooked_vsx = state_dir / "cooked" / "vsx" / "vsx_variability.csv"
    cooked_ultracoolsheet = state_dir / "cooked" / "ultracoolsheet" / "ultracoolsheet_objects.csv"
    cooked_sol_authority = state_dir / "cooked" / "sol_authority" / "sol_system_objects.csv"
    cooked_sol_artificial = state_dir / "cooked" / "sol_artificial" / "sol_artificial_objects.csv"
    cooked_gaia_backbone = state_dir / "cooked" / "gaia_backbone" / "gaia_dr3_backbone.csv"
    cooked_gaia_classprob = (
        state_dir / "cooked" / "gaia_classprob" / "gaia_dr3_astrophysical_classprob.csv"
    )
    cooked_gaia_nss = state_dir / "cooked" / "gaia_nss" / "gaia_dr3_nss_two_body_orbit.csv"
    cooked_nasa_pscomppars = (
        state_dir / "cooked" / "nasa_exoplanet_archive" / "pscomppars_clean.csv"
    )
    manifest_dir = state_dir / "reports" / "manifests"
    msc_manifest_path = manifest_dir / "msc_manifest.json"
    wds_manifest_path = manifest_dir / "wds_manifest.json"
    orb6_manifest_path = manifest_dir / "orb6_manifest.json"
    vsx_manifest_path = manifest_dir / "vsx_manifest.json"
    ultracoolsheet_manifest_path = manifest_dir / "ultracoolsheet_manifest.json"
    sol_authority_manifest_path = manifest_dir / "sol_authority_manifest.json"
    sol_artificial_manifest_path = manifest_dir / "sol_artificial_manifest.json"
    gaia_backbone_manifest_path = manifest_dir / "gaia_backbone_manifest.json"
    gaia_classprob_manifest_path = manifest_dir / "gaia_classprob_manifest.json"
    gaia_nss_manifest_path = manifest_dir / "gaia_nss_manifest.json"
    core_manifest_path = manifest_dir / "core_manifest.json"
    enable_vsx = parse_bool_env("SPACEGATE_ENABLE_VSX", True)
    enable_ultracoolsheet = parse_bool_env("SPACEGATE_ENABLE_ULTRACOOLSHEET", True)
    enable_sol_authority = parse_bool_env("SPACEGATE_ENABLE_SOL_AUTHORITY", True)
    enable_sol_artificial = parse_bool_env("SPACEGATE_ENABLE_SOL_ARTIFICIAL", True)

    if not core_db.exists():
        raise SystemExit(f"Core DB not found: {core_db}")

    arm_db.parent.mkdir(parents=True, exist_ok=True)
    if arm_db.exists():
        arm_db.unlink()

    msc_manifest = load_manifest_entry(msc_manifest_path, "newmsc_20260619")
    if not msc_manifest:
        msc_manifest = load_manifest_entry(core_manifest_path, "msc")
    wds_manifest = load_manifest_entry(wds_manifest_path, "wdsweb_summ2")
    orb6_manifest = load_manifest_entry(orb6_manifest_path, "orb6orbits")
    vsx_manifest = load_manifest_entry(vsx_manifest_path, "vsx_dat")
    ultracoolsheet_manifest = load_manifest_entry(
        ultracoolsheet_manifest_path, "UltracoolSheet_Main"
    )
    sol_authority_manifest = load_manifest_entry(sol_authority_manifest_path, "sol_system_objects")
    sol_artificial_manifest = load_manifest_entry(
        sol_artificial_manifest_path, "sol_artificial_objects"
    )
    gaia_backbone_manifest = load_manifest_entry(gaia_backbone_manifest_path, "gaia_dr3_backbone")
    gaia_classprob_manifest = load_manifest_entry(
        gaia_classprob_manifest_path, "gaia_dr3_astrophysical_classprob"
    )
    gaia_nss_manifest = load_manifest_entry(
        gaia_nss_manifest_path, "gaia_dr3_nss_two_body_orbit"
    )
    nasa_pscomppars_manifest = load_manifest_entry(core_manifest_path, "pscomppars")
    msc_version = str(msc_manifest.get("source_version") or MSC_VERSION_FALLBACK)
    msc_checksum = str(msc_manifest.get("sha256") or "")
    msc_retrieved = str(msc_manifest.get("retrieved_at") or "")
    wds_version = str(wds_manifest.get("source_version") or WDS_VERSION_FALLBACK)
    wds_checksum = str(wds_manifest.get("sha256") or "")
    wds_retrieved = str(wds_manifest.get("retrieved_at") or "")
    orb6_version = str(orb6_manifest.get("source_version") or ORB6_VERSION_FALLBACK)
    orb6_checksum = str(orb6_manifest.get("sha256") or "")
    orb6_retrieved = str(orb6_manifest.get("retrieved_at") or "")
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
    sol_authority_version = str(
        sol_authority_manifest.get("source_version") or SOL_AUTHORITY_VERSION_FALLBACK
    )
    sol_authority_checksum = str(sol_authority_manifest.get("sha256") or "")
    sol_authority_retrieved = str(sol_authority_manifest.get("retrieved_at") or "")
    sol_authority_url = str(sol_authority_manifest.get("url") or SOL_AUTHORITY_URL_FALLBACK)
    sol_artificial_version = str(
        sol_artificial_manifest.get("source_version") or SOL_ARTIFICIAL_VERSION_FALLBACK
    )
    sol_artificial_checksum = str(sol_artificial_manifest.get("sha256") or "")
    sol_artificial_retrieved = str(sol_artificial_manifest.get("retrieved_at") or "")
    sol_artificial_url = str(sol_artificial_manifest.get("url") or SOL_ARTIFICIAL_URL_FALLBACK)
    gaia_backbone_version = str(
        gaia_backbone_manifest.get("source_version") or GAIA_BACKBONE_VERSION_FALLBACK
    )
    gaia_backbone_checksum = str(gaia_backbone_manifest.get("sha256") or "")
    gaia_backbone_retrieved = str(gaia_backbone_manifest.get("retrieved_at") or "")
    gaia_backbone_url = str(gaia_backbone_manifest.get("url") or GAIA_BACKBONE_URL_FALLBACK)
    gaia_classprob_version = str(
        gaia_classprob_manifest.get("source_version") or GAIA_CLASSPROB_VERSION_FALLBACK
    )
    gaia_classprob_checksum = str(gaia_classprob_manifest.get("sha256") or "")
    gaia_classprob_retrieved = str(gaia_classprob_manifest.get("retrieved_at") or "")
    gaia_classprob_url = str(
        gaia_classprob_manifest.get("url") or GAIA_CLASSPROB_URL_FALLBACK
    )
    gaia_nss_version = str(gaia_nss_manifest.get("source_version") or GAIA_NSS_VERSION_FALLBACK)
    gaia_nss_checksum = str(gaia_nss_manifest.get("sha256") or "")
    gaia_nss_retrieved = str(gaia_nss_manifest.get("retrieved_at") or "")
    gaia_nss_url = str(gaia_nss_manifest.get("url") or GAIA_NSS_URL_FALLBACK)
    nasa_pscomppars_version = str(
        nasa_pscomppars_manifest.get("source_version") or NASA_PSCOMPPARS_VERSION_FALLBACK
    )
    nasa_pscomppars_checksum = str(nasa_pscomppars_manifest.get("sha256") or "")
    nasa_pscomppars_retrieved = str(nasa_pscomppars_manifest.get("retrieved_at") or "")
    nasa_pscomppars_url = str(nasa_pscomppars_manifest.get("url") or NASA_PSCOMPPARS_URL_FALLBACK)

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

    if cooked_msc_systems.exists():
        con.execute(
            f"""
            create or replace temp view msc_systems_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_msc_systems))},
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
            create or replace temp view msc_systems_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              wds_id, primary_label, secondary_label, parent_label, system_type, period_value, period_unit,
              separation_value, separation_unit, position_angle_deg, vmag_primary, spectral_type_primary,
              vmag_secondary, spectral_type_secondary, mass_primary_msun, mass_code_primary,
              mass_secondary_msun, mass_code_secondary, comment, source_line_number, raw_row
            )
            where false
            """
        )

    if cooked_msc_orbits.exists():
        con.execute(
            f"""
            create or replace temp view msc_orbits_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_msc_orbits))},
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
            create or replace temp view msc_orbits_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              wds_id, system_label, primary_label, secondary_label, period_value, periastron_epoch,
              eccentricity, semi_major_axis_arcsec, node_deg, longitude_periastron_deg, inclination_deg,
              semi_amplitude_primary_kms, semi_amplitude_secondary_kms, center_of_mass_velocity_kms,
              node_flag, period_unit, note, source_line_number, raw_row
            )
            where false
            """
        )

    if cooked_wds.exists():
        con.execute(
            f"""
            create or replace temp view wds_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_wds))},
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
            create or replace temp view wds_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              wds_id, discoverer, component, first_year, last_year, obs_count, theta_first_deg, theta_last_deg,
              rho_first_arcsec, rho_last_arcsec, mag_primary, mag_secondary, spectral_type_raw, pm_primary_ra,
              pm_primary_dec, pm_secondary_ra, pm_secondary_dec, dm_designation, note, precise_coordinate,
              ra_deg, dec_deg
            )
            where false
            """
        )

    if cooked_orb6.exists():
        con.execute(
            f"""
            create or replace temp view orb6_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_orb6))},
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
            create or replace temp view orb6_raw as
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
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              wds_id, discoverer, ads_id, hd_id, hip_id, ra_deg, dec_deg, mag_primary, mag_secondary,
              period_value, period_unit, period_error, semi_major_axis_arcsec, axis_qualifier, axis_error,
              inclination_deg, inclination_error, node_deg, node_error, periastron_epoch, epoch_unit,
              eccentricity, eccentricity_error, long_periastron_deg, long_periastron_error, equinox,
              last_observed_year, grade, notes_flag, reference_code, png_file
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

    if enable_sol_authority and cooked_sol_authority.exists():
        con.execute(
            f"""
            create or replace temp view sol_authority_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_sol_authority))},
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
            create or replace temp view sol_authority_raw as
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
              source_pk, object_name, object_class, object_kind, object_class_aliases_json, parent_object_name,
              horizons_command, epoch_tdb_jd, eccentricity, inclination_deg, semi_major_axis_au,
              orbital_period_days, radius_km, mass_kg, horizons_query_url, retrieved_at, source_row_hash
            )
            where false
            """
        )

    if enable_sol_artificial and cooked_sol_artificial.exists():
        con.execute(
            f"""
            create or replace temp view sol_artificial_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_sol_artificial))},
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
            create or replace temp view sol_artificial_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              source_pk, object_name, object_class, object_kind, parent_object_name, horizons_command, center_code,
              epoch_tdb_jd, eccentricity, inclination_deg, semi_major_axis_au, orbital_period_days, radius_km,
              mass_kg, freshness_window_days, target_body_name, horizons_query_url, retrieved_at, source_row_hash
            )
            where false
            """
        )

    if cooked_gaia_backbone.exists():
        con.execute(
            f"""
            create or replace temp view gaia_backbone_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_gaia_backbone))},
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
            create or replace temp view gaia_backbone_raw as
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
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              source_id, ref_epoch, ra_deg, dec_deg, parallax_mas, parallax_error_mas, pm_ra_mas_yr,
              pm_dec_mas_yr, radial_velocity_kms, radial_velocity_error_kms, phot_g_mag, phot_bp_mag,
              phot_rp_mag, bp_rp, bp_g, g_rp, ra_error_mas, dec_error_mas, pm_ra_error_mas_yr,
              pm_dec_error_mas_yr, teff_gspphot, teff_gspphot_lower, teff_gspphot_upper, logg_gspphot,
              logg_gspphot_lower, logg_gspphot_upper, mh_gspphot, mh_gspphot_lower, mh_gspphot_upper,
              distance_gspphot, distance_gspphot_lower, distance_gspphot_upper, non_single_star,
              has_xp_continuous, has_xp_sampled, has_rvs, visibility_periods_used, astrometric_params_solved,
              duplicated_source, source_row_hash, retrieval_checksum
            )
            where false
            """
        )

    if cooked_gaia_classprob.exists():
        con.execute(
            f"""
            create or replace temp view gaia_classprob_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_gaia_classprob))},
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
            create or replace temp view gaia_classprob_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              source_id, classprob_dsc_combmod_whitedwarf, classprob_dsc_specmod_whitedwarf,
              classprob_dsc_combmod_star, classprob_dsc_specmod_star, classprob_dsc_combmod_binarystar,
              classprob_dsc_specmod_binarystar, classprob_dsc_combmod_galaxy, classprob_dsc_specmod_galaxy,
              classprob_dsc_combmod_quasar, classprob_dsc_specmod_quasar
            )
            where false
            """
        )

    if cooked_gaia_nss.exists():
        con.execute(
            f"""
            create or replace temp view gaia_nss_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_gaia_nss))},
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
            create or replace temp view gaia_nss_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              source_id, nss_solution_type, ra_deg, dec_deg, parallax_mas, parallax_error_mas,
              pm_ra_mas_yr, pm_dec_mas_yr, period_days, eccentricity, center_of_mass_velocity_kms,
              semi_amplitude_primary_kms, mass_ratio, inclination_deg, flags, significance
            )
            where false
            """
        )

    if cooked_nasa_pscomppars.exists():
        con.execute(
            f"""
            create or replace temp view nasa_pscomppars_raw as
            select *
            from read_csv_auto(
              {sql_literal(str(cooked_nasa_pscomppars))},
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
            create or replace temp view nasa_pscomppars_raw as
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
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              objectid, pl_name, hostname, gaia_dr3_id, gaia_dr2_id, hip_name, hd_name, sy_snum, sy_pnum,
              sy_mnum, st_teff, st_tefferr1, st_tefferr2, st_mass, st_masserr1, st_masserr2, st_rad,
              st_raderr1, st_raderr2, st_lum, st_lumerr1, st_lumerr2, st_logg, st_loggerr1, st_loggerr2,
              st_met, st_meterr1, st_meterr2, st_age, st_ageerr1, st_ageerr2, st_rotp, st_dens,
              st_denserr1, st_denserr2, st_radv, st_radverr1, st_radverr2, st_spectype
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
          ('arm_source_wds_csv', {sql_literal(str(cooked_wds) if cooked_wds.exists() else '')}),
          ('arm_source_wds_version', {sql_literal(wds_version)}),
          ('arm_source_orb6_csv', {sql_literal(str(cooked_orb6) if cooked_orb6.exists() else '')}),
          ('arm_source_orb6_version', {sql_literal(orb6_version)}),
          ('arm_source_vsx_csv', {sql_literal(str(cooked_vsx) if cooked_vsx.exists() else '')}),
          ('arm_source_vsx_version', {sql_literal(vsx_version)}),
          ('arm_source_vsx_enabled', {sql_literal("1" if enable_vsx else "0")}),
          ('arm_source_ultracoolsheet_csv', {sql_literal(str(cooked_ultracoolsheet) if cooked_ultracoolsheet.exists() else '')}),
          ('arm_source_ultracoolsheet_version', {sql_literal(ultracoolsheet_version)}),
          ('arm_source_ultracoolsheet_enabled', {sql_literal("1" if enable_ultracoolsheet else "0")}),
          ('arm_source_sol_authority_csv', {sql_literal(str(cooked_sol_authority) if cooked_sol_authority.exists() else '')}),
          ('arm_source_sol_authority_version', {sql_literal(sol_authority_version)}),
          ('arm_source_sol_authority_enabled', {sql_literal("1" if enable_sol_authority else "0")}),
          ('arm_source_sol_artificial_csv', {sql_literal(str(cooked_sol_artificial) if cooked_sol_artificial.exists() else '')}),
          ('arm_source_sol_artificial_version', {sql_literal(sol_artificial_version)}),
          ('arm_source_sol_artificial_enabled', {sql_literal("1" if enable_sol_artificial else "0")}),
          ('arm_source_gaia_backbone_csv', {sql_literal(str(cooked_gaia_backbone) if cooked_gaia_backbone.exists() else '')}),
          ('arm_source_gaia_backbone_version', {sql_literal(gaia_backbone_version)}),
          ('arm_source_gaia_classprob_csv', {sql_literal(str(cooked_gaia_classprob) if cooked_gaia_classprob.exists() else '')}),
          ('arm_source_gaia_classprob_version', {sql_literal(gaia_classprob_version)}),
          ('arm_source_gaia_nss_csv', {sql_literal(str(cooked_gaia_nss) if cooked_gaia_nss.exists() else '')}),
          ('arm_source_gaia_nss_version', {sql_literal(gaia_nss_version)}),
          ('arm_source_nasa_pscomppars_csv', {sql_literal(str(cooked_nasa_pscomppars) if cooked_nasa_pscomppars.exists() else '')}),
          ('arm_source_nasa_pscomppars_version', {sql_literal(nasa_pscomppars_version)})
        """
    )

    stage_started = time.monotonic()
    log("Arm stage: copying exoplanet lifecycle audit tables from core")
    con.execute(
        """
        create table planet_catalog_observations as
        select * from core.planet_catalog_observations
        """
    )
    con.execute(
        """
        create table planet_status_history as
        select * from core.planet_status_history
        """
    )
    con.execute(
        """
        create table planet_reclassification_audit as
        select * from core.planet_reclassification_audit
        """
    )
    log(f"Arm stage complete: lifecycle audit copy ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating stellar_parameters")
    con.execute(
        f"""
        create table stellar_parameters as
        with gaia_backbone_typed as (
          select
            cast(nullif(source_id, '') as bigint) as gaia_id,
            nullif(teff_gspphot, '')::double as teff_k,
            nullif(teff_gspphot_lower, '')::double as teff_lo_k,
            nullif(teff_gspphot_upper, '')::double as teff_hi_k,
            nullif(logg_gspphot, '')::double as logg_cgs,
            nullif(logg_gspphot_lower, '')::double as logg_lo_cgs,
            nullif(logg_gspphot_upper, '')::double as logg_hi_cgs,
            nullif(mh_gspphot, '')::double as metallicity_feh,
            nullif(mh_gspphot_lower, '')::double as metallicity_lo_feh,
            nullif(mh_gspphot_upper, '')::double as metallicity_hi_feh,
            nullif(distance_gspphot, '')::double as distance_pc,
            nullif(distance_gspphot_lower, '')::double as distance_lo_pc,
            nullif(distance_gspphot_upper, '')::double as distance_hi_pc,
            nullif(radial_velocity_kms, '')::double as radial_velocity_kms,
            nullif(radial_velocity_error_kms, '')::double as radial_velocity_error_kms,
            nullif(phot_g_mag, '')::double as phot_g_mag,
            nullif(phot_bp_mag, '')::double as phot_bp_mag,
            nullif(phot_rp_mag, '')::double as phot_rp_mag,
            nullif(bp_rp, '')::double as bp_rp,
            nullif(bp_g, '')::double as bp_g,
            nullif(g_rp, '')::double as g_rp,
            nullif(ra_error_mas, '')::double as ra_error_mas,
            nullif(dec_error_mas, '')::double as dec_error_mas,
            nullif(pm_ra_error_mas_yr, '')::double as pm_ra_error_mas_yr,
            nullif(pm_dec_error_mas_yr, '')::double as pm_dec_error_mas_yr,
            cast(nullif(visibility_periods_used, '') as bigint) as visibility_periods_used,
            cast(nullif(astrometric_params_solved, '') as bigint) as astrometric_params_solved,
            case lower(trim(coalesce(non_single_star, '')))
              when '1' then true
              when 'true' then true
              when 't' then true
              when 'yes' then true
              when '0' then false
              when 'false' then false
              when 'f' then false
              when 'no' then false
              else null
            end as non_single_star,
            case lower(trim(coalesce(has_xp_continuous, '')))
              when '1' then true
              when 'true' then true
              when 't' then true
              when 'yes' then true
              when '0' then false
              when 'false' then false
              when 'f' then false
              when 'no' then false
              else null
            end as has_xp_continuous,
            case lower(trim(coalesce(has_xp_sampled, '')))
              when '1' then true
              when 'true' then true
              when 't' then true
              when 'yes' then true
              when '0' then false
              when 'false' then false
              when 'f' then false
              when 'no' then false
              else null
            end as has_xp_sampled,
            case lower(trim(coalesce(has_rvs, '')))
              when '1' then true
              when 'true' then true
              when 't' then true
              when 'yes' then true
              when '0' then false
              when 'false' then false
              when 'f' then false
              when 'no' then false
              else null
            end as has_rvs,
            case lower(trim(coalesce(duplicated_source, '')))
              when '1' then true
              when 'true' then true
              when 't' then true
              when 'yes' then true
              when '0' then false
              when 'false' then false
              when 'f' then false
              when 'no' then false
              else null
            end as duplicated_source
          from gaia_backbone_raw
          where cast(nullif(source_id, '') as bigint) is not null
        ), gaia_classprob_typed as (
          select
            cast(nullif(source_id, '') as bigint) as gaia_id,
            nullif(classprob_dsc_combmod_whitedwarf, '')::double as classprob_whitedwarf_combmod,
            nullif(classprob_dsc_specmod_whitedwarf, '')::double as classprob_whitedwarf_specmod,
            greatest(
              nullif(classprob_dsc_combmod_star, '')::double,
              nullif(classprob_dsc_specmod_star, '')::double
            ) as classprob_star,
            greatest(
              nullif(classprob_dsc_combmod_binarystar, '')::double,
              nullif(classprob_dsc_specmod_binarystar, '')::double
            ) as classprob_binarystar,
            greatest(
              nullif(classprob_dsc_combmod_galaxy, '')::double,
              nullif(classprob_dsc_specmod_galaxy, '')::double
            ) as classprob_galaxy,
            greatest(
              nullif(classprob_dsc_combmod_quasar, '')::double,
              nullif(classprob_dsc_specmod_quasar, '')::double
            ) as classprob_quasar
          from gaia_classprob_raw
          where cast(nullif(source_id, '') as bigint) is not null
        ), gaia_rows as (
          select
            st.star_id,
            st.system_id,
            st.stable_object_key,
            'gaia_dr3_backbone'::varchar as parameter_source,
            g.teff_k,
            g.teff_lo_k,
            g.teff_hi_k,
            g.logg_cgs,
            g.logg_lo_cgs,
            g.logg_hi_cgs,
            g.metallicity_feh,
            g.metallicity_lo_feh,
            g.metallicity_hi_feh,
            g.distance_pc,
            g.distance_lo_pc,
            g.distance_hi_pc,
            cast(null as double) as radius_rsun,
            cast(null as double) as radius_err_plus_rsun,
            cast(null as double) as radius_err_minus_rsun,
            cast(null as double) as mass_msun,
            cast(null as double) as mass_err_plus_msun,
            cast(null as double) as mass_err_minus_msun,
            cast(null as double) as luminosity_log10_lsun,
            cast(null as double) as luminosity_err_plus_log10_lsun,
            cast(null as double) as luminosity_err_minus_log10_lsun,
            cast(null as double) as density_g_cm3,
            cast(null as double) as density_err_plus_g_cm3,
            cast(null as double) as density_err_minus_g_cm3,
            cast(null as double) as age_gyr,
            cast(null as double) as age_err_plus_gyr,
            cast(null as double) as age_err_minus_gyr,
            cast(null as double) as rotation_period_days,
            g.radial_velocity_kms,
            g.radial_velocity_error_kms,
            g.phot_g_mag,
            g.phot_bp_mag,
            g.phot_rp_mag,
            g.bp_rp,
            g.bp_g,
            g.g_rp,
            g.ra_error_mas,
            g.dec_error_mas,
            g.pm_ra_error_mas_yr,
            g.pm_dec_error_mas_yr,
            g.visibility_periods_used,
            g.astrometric_params_solved,
            g.non_single_star,
            g.duplicated_source,
            g.has_xp_continuous,
            g.has_xp_sampled,
            g.has_rvs,
            st.spectral_type_raw,
            c.classprob_star,
            c.classprob_binarystar,
            c.classprob_galaxy,
            c.classprob_quasar,
            c.classprob_whitedwarf_combmod,
            c.classprob_whitedwarf_specmod,
            json_object(
              'catalog_ids', st.catalog_ids_json,
              'wd_catalog_teff_k', st.wd_catalog_teff_k,
              'wd_catalog_logg_cgs', st.wd_catalog_logg_cgs,
              'wd_catalog_mass_msun', st.wd_catalog_mass_msun
            ) as context_json,
            'gaia_dr3'::varchar as source_catalog,
            {sql_literal(gaia_backbone_version)}::varchar as source_version,
            cast(st.gaia_id as varchar) as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(gaia_backbone_checksum)}::varchar as retrieval_checksum,
            {sql_literal(gaia_backbone_retrieved)}::varchar as retrieved_at,
            {sql_literal(args.ingested_at)}::varchar as ingested_at,
            {sql_literal(args.transform_version)}::varchar as transform_version,
            {sql_literal(gaia_backbone_url)}::varchar as source_url
          from core.stars st
          join gaia_backbone_typed g on g.gaia_id = st.gaia_id
          left join gaia_classprob_typed c on c.gaia_id = st.gaia_id
          where
            g.teff_k is not null
            or g.logg_cgs is not null
            or g.metallicity_feh is not null
            or g.distance_pc is not null
            or g.bp_rp is not null
            or g.phot_bp_mag is not null
            or g.phot_rp_mag is not null
            or st.wd_catalog_teff_k is not null
        ), nasa_base as (
          select
            cast(nullif(objectid, '') as bigint) as source_pk,
            nullif(pl_name, '') as planet_name_raw,
            case
              when nullif(pl_name, '') is not null then
                lower(
                  trim(
                    regexp_replace(
                      regexp_replace(pl_name, '[^0-9A-Za-z]+', ' ', 'g'),
                      '\\s+',
                      ' ',
                      'g'
                    )
                  )
                )
              else null
            end as planet_name_norm,
            nullif(hostname, '') as host_name_raw,
            cast(nullif(sy_snum, '') as bigint) as system_star_count,
            cast(nullif(sy_pnum, '') as bigint) as system_planet_count,
            cast(nullif(sy_mnum, '') as bigint) as system_multiplicity_count,
            nullif(st_teff, '')::double as teff_k,
            nullif(st_tefferr1, '')::double as teff_err_plus_k,
            nullif(st_tefferr2, '')::double as teff_err_minus_k,
            nullif(st_mass, '')::double as mass_msun,
            nullif(st_masserr1, '')::double as mass_err_plus_msun,
            nullif(st_masserr2, '')::double as mass_err_minus_msun,
            nullif(st_rad, '')::double as radius_rsun,
            nullif(st_raderr1, '')::double as radius_err_plus_rsun,
            nullif(st_raderr2, '')::double as radius_err_minus_rsun,
            nullif(st_lum, '')::double as luminosity_log10_lsun,
            nullif(st_lumerr1, '')::double as luminosity_err_plus_log10_lsun,
            nullif(st_lumerr2, '')::double as luminosity_err_minus_log10_lsun,
            nullif(st_logg, '')::double as logg_cgs,
            nullif(st_loggerr1, '')::double as logg_err_plus_cgs,
            nullif(st_loggerr2, '')::double as logg_err_minus_cgs,
            nullif(st_met, '')::double as metallicity_feh,
            nullif(st_meterr1, '')::double as metallicity_err_plus_feh,
            nullif(st_meterr2, '')::double as metallicity_err_minus_feh,
            nullif(st_age, '')::double as age_gyr,
            nullif(st_ageerr1, '')::double as age_err_plus_gyr,
            nullif(st_ageerr2, '')::double as age_err_minus_gyr,
            nullif(st_rotp, '')::double as rotation_period_days,
            nullif(st_dens, '')::double as density_g_cm3,
            nullif(st_denserr1, '')::double as density_err_plus_g_cm3,
            nullif(st_denserr2, '')::double as density_err_minus_g_cm3,
            nullif(st_radv, '')::double as radial_velocity_kms,
            greatest(
              abs(nullif(st_radverr1, '')::double),
              abs(nullif(st_radverr2, '')::double)
            ) as radial_velocity_error_kms,
            nullif(st_spectype, '') as spectral_type_raw
          from nasa_pscomppars_raw
          where cast(nullif(objectid, '') as bigint) is not null
        ), nasa_planet_match as (
          select
            planet_name_norm,
            max_by(star_id, coalesce(match_confidence, 0.0)) as star_id,
            max_by(system_id, coalesce(match_confidence, 0.0)) as system_id
          from core.planets
          where source_catalog = 'nasa_exoplanet_archive'
            and planet_name_norm is not null
            and star_id is not null
          group by planet_name_norm
        ), nasa_matches as (
          select
            n.*,
            p.star_id,
            p.system_id
          from nasa_base n
          left join nasa_planet_match p on p.planet_name_norm = n.planet_name_norm
        ), nasa_ranked as (
          select
            n.star_id,
            n.system_id,
            st.stable_object_key,
            'nasa_pscomppars_host'::varchar as parameter_source,
            n.teff_k,
            case when n.teff_k is not null then n.teff_k + n.teff_err_minus_k else null end as teff_lo_k,
            case when n.teff_k is not null then n.teff_k + n.teff_err_plus_k else null end as teff_hi_k,
            n.logg_cgs,
            case when n.logg_cgs is not null then n.logg_cgs + n.logg_err_minus_cgs else null end as logg_lo_cgs,
            case when n.logg_cgs is not null then n.logg_cgs + n.logg_err_plus_cgs else null end as logg_hi_cgs,
            n.metallicity_feh,
            case when n.metallicity_feh is not null then n.metallicity_feh + n.metallicity_err_minus_feh else null end as metallicity_lo_feh,
            case when n.metallicity_feh is not null then n.metallicity_feh + n.metallicity_err_plus_feh else null end as metallicity_hi_feh,
            cast(null as double) as distance_pc,
            cast(null as double) as distance_lo_pc,
            cast(null as double) as distance_hi_pc,
            n.radius_rsun,
            n.radius_err_plus_rsun,
            n.radius_err_minus_rsun,
            n.mass_msun,
            n.mass_err_plus_msun,
            n.mass_err_minus_msun,
            n.luminosity_log10_lsun,
            n.luminosity_err_plus_log10_lsun,
            n.luminosity_err_minus_log10_lsun,
            n.density_g_cm3,
            n.density_err_plus_g_cm3,
            n.density_err_minus_g_cm3,
            n.age_gyr,
            n.age_err_plus_gyr,
            n.age_err_minus_gyr,
            n.rotation_period_days,
            n.radial_velocity_kms,
            n.radial_velocity_error_kms,
            cast(null as double) as phot_g_mag,
            cast(null as double) as phot_bp_mag,
            cast(null as double) as phot_rp_mag,
            cast(null as double) as bp_rp,
            cast(null as double) as bp_g,
            cast(null as double) as g_rp,
            cast(null as double) as ra_error_mas,
            cast(null as double) as dec_error_mas,
            cast(null as double) as pm_ra_error_mas_yr,
            cast(null as double) as pm_dec_error_mas_yr,
            cast(null as bigint) as visibility_periods_used,
            cast(null as bigint) as astrometric_params_solved,
            cast(null as boolean) as non_single_star,
            cast(null as boolean) as duplicated_source,
            cast(null as boolean) as has_xp_continuous,
            cast(null as boolean) as has_xp_sampled,
            cast(null as boolean) as has_rvs,
            n.spectral_type_raw,
            cast(null as double) as classprob_star,
            cast(null as double) as classprob_binarystar,
            cast(null as double) as classprob_galaxy,
            cast(null as double) as classprob_quasar,
            cast(null as double) as classprob_whitedwarf_combmod,
            cast(null as double) as classprob_whitedwarf_specmod,
            json_object(
              'host_name_raw', n.host_name_raw,
              'system_star_count', n.system_star_count,
              'system_planet_count', n.system_planet_count,
              'system_multiplicity_count', n.system_multiplicity_count
            ) as context_json,
            'nasa_exoplanet_archive'::varchar as source_catalog,
            {sql_literal(nasa_pscomppars_version)}::varchar as source_version,
            cast(n.source_pk as varchar) as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(nasa_pscomppars_checksum)}::varchar as retrieval_checksum,
            {sql_literal(nasa_pscomppars_retrieved)}::varchar as retrieved_at,
            {sql_literal(args.ingested_at)}::varchar as ingested_at,
            {sql_literal(args.transform_version)}::varchar as transform_version,
            {sql_literal(nasa_pscomppars_url)}::varchar as source_url,
            row_number() over (
              partition by n.star_id
              order by
                (
                  case when n.teff_k is not null then 1 else 0 end +
                  case when n.mass_msun is not null then 1 else 0 end +
                  case when n.radius_rsun is not null then 1 else 0 end +
                  case when n.luminosity_log10_lsun is not null then 1 else 0 end +
                  case when n.logg_cgs is not null then 1 else 0 end +
                  case when n.metallicity_feh is not null then 1 else 0 end +
                  case when n.age_gyr is not null then 1 else 0 end +
                  case when n.density_g_cm3 is not null then 1 else 0 end
                ) desc,
                n.source_pk asc
            ) as rn
          from nasa_matches n
          join core.stars st on st.star_id = n.star_id
          where
            n.star_id is not null
            and (
              n.teff_k is not null
              or n.mass_msun is not null
              or n.radius_rsun is not null
              or n.luminosity_log10_lsun is not null
              or n.logg_cgs is not null
              or n.metallicity_feh is not null
              or n.age_gyr is not null
              or n.rotation_period_days is not null
              or n.density_g_cm3 is not null
            )
        ), nasa_rows as (
          select
            star_id,
            system_id,
            stable_object_key,
            parameter_source,
            teff_k,
            teff_lo_k,
            teff_hi_k,
            logg_cgs,
            logg_lo_cgs,
            logg_hi_cgs,
            metallicity_feh,
            metallicity_lo_feh,
            metallicity_hi_feh,
            distance_pc,
            distance_lo_pc,
            distance_hi_pc,
            radius_rsun,
            radius_err_plus_rsun,
            radius_err_minus_rsun,
            mass_msun,
            mass_err_plus_msun,
            mass_err_minus_msun,
            luminosity_log10_lsun,
            luminosity_err_plus_log10_lsun,
            luminosity_err_minus_log10_lsun,
            density_g_cm3,
            density_err_plus_g_cm3,
            density_err_minus_g_cm3,
            age_gyr,
            age_err_plus_gyr,
            age_err_minus_gyr,
            rotation_period_days,
            radial_velocity_kms,
            radial_velocity_error_kms,
            phot_g_mag,
            phot_bp_mag,
            phot_rp_mag,
            bp_rp,
            bp_g,
            g_rp,
            ra_error_mas,
            dec_error_mas,
            pm_ra_error_mas_yr,
            pm_dec_error_mas_yr,
            visibility_periods_used,
            astrometric_params_solved,
            non_single_star,
            duplicated_source,
            has_xp_continuous,
            has_xp_sampled,
            has_rvs,
            spectral_type_raw,
            classprob_star,
            classprob_binarystar,
            classprob_galaxy,
            classprob_quasar,
            classprob_whitedwarf_combmod,
            classprob_whitedwarf_specmod,
            context_json,
            source_catalog,
            source_version,
            source_pk,
            source_row_hash,
            retrieval_checksum,
            retrieved_at,
            ingested_at,
            transform_version,
            source_url
          from nasa_ranked
          where rn = 1
        ), unioned as (
          select * from gaia_rows
          union all
          select * from nasa_rows
        )
        select
          row_number() over (order by star_id, parameter_source, source_catalog, source_pk)::bigint as stellar_parameter_id,
          star_id,
          system_id,
          stable_object_key,
          parameter_source,
          teff_k,
          teff_lo_k,
          teff_hi_k,
          logg_cgs,
          logg_lo_cgs,
          logg_hi_cgs,
          metallicity_feh,
          metallicity_lo_feh,
          metallicity_hi_feh,
          distance_pc,
          distance_lo_pc,
          distance_hi_pc,
          radius_rsun,
          radius_err_plus_rsun,
          radius_err_minus_rsun,
          mass_msun,
          mass_err_plus_msun,
          mass_err_minus_msun,
          luminosity_log10_lsun,
          luminosity_err_plus_log10_lsun,
          luminosity_err_minus_log10_lsun,
          density_g_cm3,
          density_err_plus_g_cm3,
          density_err_minus_g_cm3,
          age_gyr,
          age_err_plus_gyr,
          age_err_minus_gyr,
          rotation_period_days,
          radial_velocity_kms,
          radial_velocity_error_kms,
          phot_g_mag,
          phot_bp_mag,
          phot_rp_mag,
          bp_rp,
          bp_g,
          g_rp,
          ra_error_mas,
          dec_error_mas,
          pm_ra_error_mas_yr,
          pm_dec_error_mas_yr,
          visibility_periods_used,
          astrometric_params_solved,
          non_single_star,
          duplicated_source,
          has_xp_continuous,
          has_xp_sampled,
          has_rvs,
          spectral_type_raw,
          classprob_star,
          classprob_binarystar,
          classprob_galaxy,
          classprob_quasar,
          classprob_whitedwarf_combmod,
          classprob_whitedwarf_specmod,
          context_json,
          source_catalog,
          source_version,
          source_url,
          source_pk,
          source_row_hash,
          retrieval_checksum,
          retrieved_at,
          ingested_at,
          transform_version
        from unioned
        """
    )
    log(f"Arm stage complete: stellar_parameters ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating derived_physical_parameters")
    con.execute(
        f"""
        create table derived_physical_parameters as
        with source_luminosity_by_star as (
          select
            star_id,
            max(power(10.0, luminosity_log10_lsun)) as luminosity_lsun
          from stellar_parameters
          where luminosity_log10_lsun is not null
          group by star_id
        ), source_mass_by_star as (
          select
            star_id,
            mass_msun,
            stellar_parameter_id,
            parameter_source,
            source_catalog,
            source_version,
            source_pk,
            source_row_hash,
            retrieval_checksum,
            retrieved_at
          from (
            select
              *,
              row_number() over (
                partition by star_id
                order by
                  case parameter_source
                    when 'nasa_pscomppars_host' then 0
                    when 'gaia_dr3_backbone' then 1
                    else 9
                  end,
                  stellar_parameter_id
              ) as rn
            from stellar_parameters
            where mass_msun is not null
              and mass_msun > 0.0
          )
          where rn = 1
        ), stellar_luminosity_candidates as (
          select
            'star'::varchar as object_type,
            sp.system_id,
            sp.star_id,
            cast(null as bigint) as planet_id,
            sp.stable_object_key,
            cast(null as varchar) as stable_component_key,
            'luminosity_lsun'::varchar as parameter_key,
            (sp.radius_rsun * sp.radius_rsun * power(sp.teff_k / 5772.0, 4.0))::double as value,
            'Lsun'::varchar as unit,
            cast(null as double) as value_lo,
            cast(null as double) as value_hi,
            cast(null as double) as sigma,
            'stefan_boltzmann_from_radius_teff'::varchar as derivation_method,
            {sql_literal(DERIVED_PHYSICAL_PARAMETERS_VERSION)}::varchar as derivation_version,
            json_object(
              'stellar_parameter_id', sp.stellar_parameter_id,
              'parameter_source', sp.parameter_source,
              'teff_k', sp.teff_k,
              'radius_rsun', sp.radius_rsun,
              'solar_teff_reference_k', 5772.0
            )::varchar as input_parameters_json,
            json_object(
              'formula', 'L/Lsun = (R/Rsun)^2 * (Teff/5772K)^4',
              'requires_source_teff', true,
              'requires_source_radius', true
            )::varchar as assumptions_json,
            false as lossy_transform,
            false as superseded_by_source,
            'normal'::varchar as replacement_priority,
            0.86::double as confidence_score,
            'medium'::varchar as confidence_tier,
            'candidate'::varchar as review_status,
            'spacegate_derived'::varchar as source_catalog,
            {sql_literal(DERIVED_PHYSICAL_PARAMETERS_VERSION)}::varchar as source_version,
            concat('star:', sp.star_id::varchar, ':luminosity_lsun:', sp.stellar_parameter_id::varchar)::varchar as source_pk,
            sp.source_row_hash,
            sp.retrieval_checksum,
            sp.retrieved_at,
            sp.ingested_at,
            {sql_literal(args.transform_version)}::varchar as transform_version
          from stellar_parameters sp
          left join source_luminosity_by_star src on src.star_id = sp.star_id
          where sp.teff_k is not null
            and sp.teff_k > 0.0
            and sp.radius_rsun is not null
            and sp.radius_rsun > 0.0
            and src.star_id is null
          qualify row_number() over (
            partition by sp.star_id
            order by
              case sp.parameter_source
                when 'nasa_pscomppars_host' then 0
                when 'gaia_dr3_backbone' then 1
                else 9
              end,
              sp.stellar_parameter_id
          ) = 1
        ), luminosity_for_planets as (
          select
            star_id,
            luminosity_lsun,
            'source'::varchar as luminosity_status,
            'source stellar luminosity_log10_lsun'::varchar as luminosity_basis,
            cast(null as bigint) as derived_parameter_id
          from source_luminosity_by_star
          union all
          select
            star_id,
            value as luminosity_lsun,
            'derived'::varchar as luminosity_status,
            'arm.derived_physical_parameters:stefan_boltzmann_from_radius_teff'::varchar as luminosity_basis,
            cast(null as bigint) as derived_parameter_id
          from stellar_luminosity_candidates
        ), planet_sma_candidates as (
          select
            'planet'::varchar as object_type,
            p.system_id,
            cast(null as bigint) as star_id,
            p.planet_id,
            p.stable_object_key,
            cast(null as varchar) as stable_component_key,
            'semi_major_axis_au'::varchar as parameter_key,
            power(sm.mass_msun * power(p.orbital_period_days / 365.25, 2.0), 1.0 / 3.0)::double as value,
            'au'::varchar as unit,
            cast(null as double) as value_lo,
            cast(null as double) as value_hi,
            cast(null as double) as sigma,
            'kepler_from_period_host_mass'::varchar as derivation_method,
            {sql_literal(DERIVED_PHYSICAL_PARAMETERS_VERSION)}::varchar as derivation_version,
            json_object(
              'planet_id', p.planet_id,
              'star_id', p.star_id,
              'orbital_period_days', p.orbital_period_days,
              'host_mass_msun', sm.mass_msun,
              'host_mass_stellar_parameter_id', sm.stellar_parameter_id,
              'host_mass_parameter_source', sm.parameter_source
            )::varchar as input_parameters_json,
            json_object(
              'formula', 'a_au = (Mstar_msun * period_years^2)^(1/3)',
              'planet_mass_ignored', true,
              'period_year_days', 365.25
            )::varchar as assumptions_json,
            true as lossy_transform,
            false as superseded_by_source,
            'high'::varchar as replacement_priority,
            0.78::double as confidence_score,
            'medium'::varchar as confidence_tier,
            'candidate'::varchar as review_status,
            'spacegate_derived'::varchar as source_catalog,
            {sql_literal(DERIVED_PHYSICAL_PARAMETERS_VERSION)}::varchar as source_version,
            concat('planet:', p.planet_id::varchar, ':semi_major_axis_au')::varchar as source_pk,
            p.source_row_hash,
            coalesce(p.retrieval_checksum, sm.retrieval_checksum) as retrieval_checksum,
            coalesce(p.retrieved_at, sm.retrieved_at) as retrieved_at,
            {sql_literal(args.ingested_at)}::varchar as ingested_at,
            {sql_literal(args.transform_version)}::varchar as transform_version
          from core.planets p
          join source_mass_by_star sm on sm.star_id = p.star_id
          where p.semi_major_axis_au is null
            and p.orbital_period_days is not null
            and p.orbital_period_days > 0.0
        ), sma_for_planets as (
          select
            planet_id,
            semi_major_axis_au,
            'source'::varchar as sma_status,
            'source planet semi_major_axis_au'::varchar as sma_basis
          from core.planets
          where semi_major_axis_au is not null
            and semi_major_axis_au > 0.0
          union all
          select
            planet_id,
            value as semi_major_axis_au,
            'derived'::varchar as sma_status,
            'arm.derived_physical_parameters:kepler_from_period_host_mass'::varchar as sma_basis
          from planet_sma_candidates
        ), planet_insolation_candidates as (
          select
            'planet'::varchar as object_type,
            p.system_id,
            cast(null as bigint) as star_id,
            p.planet_id,
            p.stable_object_key,
            cast(null as varchar) as stable_component_key,
            'insol_earth'::varchar as parameter_key,
            (lfp.luminosity_lsun / (sfp.semi_major_axis_au * sfp.semi_major_axis_au))::double as value,
            'Earth=1'::varchar as unit,
            cast(null as double) as value_lo,
            cast(null as double) as value_hi,
            cast(null as double) as sigma,
            'insolation_from_luminosity_sma'::varchar as derivation_method,
            {sql_literal(DERIVED_PHYSICAL_PARAMETERS_VERSION)}::varchar as derivation_version,
            json_object(
              'planet_id', p.planet_id,
              'star_id', p.star_id,
              'luminosity_lsun', lfp.luminosity_lsun,
              'luminosity_status', lfp.luminosity_status,
              'luminosity_basis', lfp.luminosity_basis,
              'semi_major_axis_au', sfp.semi_major_axis_au,
              'semi_major_axis_status', sfp.sma_status,
              'semi_major_axis_basis', sfp.sma_basis
            )::varchar as input_parameters_json,
            json_object(
              'formula', 'S/Searth = L/Lsun / a_au^2',
              'single_host_star', true
            )::varchar as assumptions_json,
            false as lossy_transform,
            false as superseded_by_source,
            'high'::varchar as replacement_priority,
            case
              when lfp.luminosity_status = 'source' and sfp.sma_status = 'source' then 0.90
              when lfp.luminosity_status = 'source' or sfp.sma_status = 'source' then 0.82
              else 0.72
            end::double as confidence_score,
            case
              when lfp.luminosity_status = 'source' and sfp.sma_status = 'source' then 'high'
              else 'medium'
            end::varchar as confidence_tier,
            'candidate'::varchar as review_status,
            'spacegate_derived'::varchar as source_catalog,
            {sql_literal(DERIVED_PHYSICAL_PARAMETERS_VERSION)}::varchar as source_version,
            concat('planet:', p.planet_id::varchar, ':insol_earth')::varchar as source_pk,
            p.source_row_hash,
            p.retrieval_checksum,
            p.retrieved_at,
            {sql_literal(args.ingested_at)}::varchar as ingested_at,
            {sql_literal(args.transform_version)}::varchar as transform_version
          from core.planets p
          join luminosity_for_planets lfp on lfp.star_id = p.star_id
          join sma_for_planets sfp on sfp.planet_id = p.planet_id
          where p.insol_earth is null
            and sfp.semi_major_axis_au > 0.0
        ), insolation_for_planets as (
          select
            planet_id,
            insol_earth,
            'source'::varchar as insolation_status,
            'source planet insol_earth'::varchar as insolation_basis
          from core.planets
          where insol_earth is not null
            and insol_earth > 0.0
          union all
          select
            planet_id,
            value as insol_earth,
            'derived'::varchar as insolation_status,
            'arm.derived_physical_parameters:insolation_from_luminosity_sma'::varchar as insolation_basis
          from planet_insolation_candidates
        ), planet_eq_temp_candidates as (
          select
            'planet'::varchar as object_type,
            p.system_id,
            cast(null as bigint) as star_id,
            p.planet_id,
            p.stable_object_key,
            cast(null as varchar) as stable_component_key,
            'eq_temp_k'::varchar as parameter_key,
            (278.5 * power(ifp.insol_earth, 0.25))::double as value,
            'K'::varchar as unit,
            cast(null as double) as value_lo,
            cast(null as double) as value_hi,
            cast(null as double) as sigma,
            'equilibrium_temp_from_insolation'::varchar as derivation_method,
            {sql_literal(DERIVED_PHYSICAL_PARAMETERS_VERSION)}::varchar as derivation_version,
            json_object(
              'planet_id', p.planet_id,
              'insol_earth', ifp.insol_earth,
              'insolation_status', ifp.insolation_status,
              'insolation_basis', ifp.insolation_basis,
              'earth_normalized_constant_k', 278.5
            )::varchar as input_parameters_json,
            json_object(
              'formula', 'Teq_K = 278.5 * (S/Searth)^0.25',
              'atmosphere_ignored', true,
              'albedo_not_individually_modeled', true
            )::varchar as assumptions_json,
            true as lossy_transform,
            false as superseded_by_source,
            'normal'::varchar as replacement_priority,
            case when ifp.insolation_status = 'source' then 0.84 else 0.74 end::double as confidence_score,
            case when ifp.insolation_status = 'source' then 'medium' else 'low' end::varchar as confidence_tier,
            'candidate'::varchar as review_status,
            'spacegate_derived'::varchar as source_catalog,
            {sql_literal(DERIVED_PHYSICAL_PARAMETERS_VERSION)}::varchar as source_version,
            concat('planet:', p.planet_id::varchar, ':eq_temp_k')::varchar as source_pk,
            p.source_row_hash,
            p.retrieval_checksum,
            p.retrieved_at,
            {sql_literal(args.ingested_at)}::varchar as ingested_at,
            {sql_literal(args.transform_version)}::varchar as transform_version
          from core.planets p
          join insolation_for_planets ifp on ifp.planet_id = p.planet_id
          where p.eq_temp_k is null
            and ifp.insol_earth > 0.0
        ), unioned as (
          select * from stellar_luminosity_candidates
          union all
          select * from planet_sma_candidates
          union all
          select * from planet_insolation_candidates
          union all
          select * from planet_eq_temp_candidates
        )
        select
          row_number() over (order by object_type, coalesce(system_id, -1), coalesce(star_id, -1), coalesce(planet_id, -1), parameter_key)::bigint as derived_parameter_id,
          {sql_literal(args.build_id)}::varchar as build_id,
          object_type,
          system_id,
          star_id,
          planet_id,
          stable_object_key,
          stable_component_key,
          parameter_key,
          value,
          unit,
          value_lo,
          value_hi,
          sigma,
          derivation_method,
          derivation_version,
          input_parameters_json,
          assumptions_json,
          lossy_transform,
          superseded_by_source,
          replacement_priority,
          confidence_score,
          confidence_tier,
          review_status,
          source_catalog,
          source_version,
          source_pk,
          sha256(concat_ws('|', source_pk, parameter_key, coalesce(cast(value as varchar), ''), input_parameters_json, assumptions_json)) as source_row_hash,
          retrieval_checksum,
          retrieved_at,
          ingested_at,
          transform_version
        from unioned
        where value is not null
        """
    )
    log(f"Arm stage complete: derived_physical_parameters ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating gaia_nss_best")
    con.execute(
        """
        create temp table gaia_nss_best as
        with typed as (
          select
            cast(nullif(source_id, '') as bigint) as gaia_id,
            nullif(nss_solution_type, '') as nss_solution_type,
            nullif(period_days, '')::double as period_days,
            nullif(eccentricity, '')::double as eccentricity,
            nullif(center_of_mass_velocity_kms, '')::double as center_of_mass_velocity_kms,
            nullif(semi_amplitude_primary_kms, '')::double as semi_amplitude_primary_kms,
            nullif(mass_ratio, '')::double as mass_ratio,
            nullif(inclination_deg, '')::double as inclination_deg,
            nullif(flags, '')::bigint as flags,
            nullif(significance, '')::double as significance,
            nullif(ra_deg, '')::double as ra_deg,
            nullif(dec_deg, '')::double as dec_deg,
            nullif(parallax_mas, '')::double as parallax_mas,
            nullif(parallax_error_mas, '')::double as parallax_error_mas,
            nullif(pm_ra_mas_yr, '')::double as pm_ra_mas_yr,
            nullif(pm_dec_mas_yr, '')::double as pm_dec_mas_yr
          from gaia_nss_raw
          where cast(nullif(source_id, '') as bigint) is not null
        )
        select
          gaia_id,
          nss_solution_type,
          period_days,
          eccentricity,
          center_of_mass_velocity_kms,
          semi_amplitude_primary_kms,
          mass_ratio,
          inclination_deg,
          flags,
          significance,
          ra_deg,
          dec_deg,
          parallax_mas,
          parallax_error_mas,
          pm_ra_mas_yr,
          pm_dec_mas_yr
        from (
          select
            *,
            row_number() over (
              partition by gaia_id
              order by
                coalesce(significance, -1.0) desc,
                case
                  when lower(coalesce(nss_solution_type, '')) like '%validated%' then 0
                  when lower(coalesce(nss_solution_type, '')) like '%orbital%' then 1
                  else 2
                end,
                coalesce(period_days, 1.0e18) asc
            ) as rn
          from typed
        ) ranked
        where rn = 1
        """
    )
    log(f"Arm stage complete: gaia_nss_best ({time.monotonic() - stage_started:.1f}s)")

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
    log("Arm stage: creating msc_source_rows")
    con.execute(
        """
        create temp table msc_source_system_rows as
        select
          nullif(wds_id, '') as wds_id,
          nullif(primary_label, '') as primary_label_raw,
          nullif(secondary_label, '') as secondary_label_raw,
          nullif(parent_label, '') as parent_label_raw,
          lower(regexp_replace(coalesce(primary_label, ''), '[^0-9A-Za-z]+', '', 'g')) as primary_label,
          lower(regexp_replace(coalesce(secondary_label, ''), '[^0-9A-Za-z]+', '', 'g')) as secondary_label,
          case
            when lower(trim(coalesce(parent_label, ''))) in ('', '*', 't') then lower(trim(coalesce(parent_label, '')))
            else lower(regexp_replace(coalesce(parent_label, ''), '[^0-9A-Za-z]+', '', 'g'))
          end as parent_label,
          nullif(system_type, '') as system_type,
          try_cast(nullif(period_value, '') as double) as period_value,
          nullif(period_unit, '') as period_unit,
          try_cast(nullif(separation_value, '') as double) as separation_value,
          nullif(separation_unit, '') as separation_unit,
          try_cast(nullif(position_angle_deg, '') as double) as position_angle_deg,
          try_cast(nullif(vmag_primary, '') as double) as vmag_primary,
          nullif(spectral_type_primary, '') as spectral_type_primary,
          try_cast(nullif(vmag_secondary, '') as double) as vmag_secondary,
          nullif(spectral_type_secondary, '') as spectral_type_secondary,
          try_cast(nullif(mass_primary_msun, '') as double) as mass_primary_msun,
          nullif(mass_code_primary, '') as mass_code_primary,
          try_cast(nullif(mass_secondary_msun, '') as double) as mass_secondary_msun,
          nullif(mass_code_secondary, '') as mass_code_secondary,
          nullif(comment, '') as comment,
          try_cast(nullif(source_line_number, '') as bigint) as source_line_number,
          nullif(raw_row, '') as raw_row
        from msc_systems_raw
        where nullif(wds_id, '') is not null
          and nullif(primary_label, '') is not null
          and nullif(secondary_label, '') is not null
        """
    )
    con.execute(
        """
        create temp table msc_source_orbit_rows as
        select
          nullif(wds_id, '') as wds_id,
          nullif(system_label, '') as system_label,
          nullif(primary_label, '') as primary_label_raw,
          nullif(secondary_label, '') as secondary_label_raw,
          lower(regexp_replace(coalesce(primary_label, ''), '[^0-9A-Za-z]+', '', 'g')) as primary_label,
          lower(regexp_replace(coalesce(secondary_label, ''), '[^0-9A-Za-z]+', '', 'g')) as secondary_label,
          try_cast(nullif(period_value, '') as double) as period_value,
          try_cast(nullif(periastron_epoch, '') as double) as periastron_epoch,
          try_cast(nullif(eccentricity, '') as double) as eccentricity,
          try_cast(nullif(semi_major_axis_arcsec, '') as double) as semi_major_axis_arcsec,
          try_cast(nullif(node_deg, '') as double) as node_deg,
          try_cast(nullif(longitude_periastron_deg, '') as double) as longitude_periastron_deg,
          try_cast(nullif(inclination_deg, '') as double) as inclination_deg,
          try_cast(nullif(semi_amplitude_primary_kms, '') as double) as semi_amplitude_primary_kms,
          try_cast(nullif(semi_amplitude_secondary_kms, '') as double) as semi_amplitude_secondary_kms,
          try_cast(nullif(center_of_mass_velocity_kms, '') as double) as center_of_mass_velocity_kms,
          nullif(node_flag, '') as node_flag,
          nullif(period_unit, '') as period_unit,
          nullif(note, '') as note,
          try_cast(nullif(source_line_number, '') as bigint) as source_line_number,
          nullif(raw_row, '') as raw_row
        from msc_orbits_raw
        where nullif(wds_id, '') is not null
          and nullif(primary_label, '') is not null
          and nullif(secondary_label, '') is not null
        """
    )
    con.execute(
        """
        create temp table msc_source_group_labels as
        select distinct wds_id, parent_label as component_label
        from msc_source_system_rows
        where parent_label not in ('', '*', 't')
        """
    )
    con.execute(
        """
        create temp table msc_source_group_raw_labels as
        select distinct
          wds_id,
          parent_label as component_label,
          parent_label_raw as component_label_raw
        from msc_source_system_rows
        where parent_label not in ('', '*', 't')
          and parent_label_raw is not null
        """
    )
    con.execute(
        """
        create temp table msc_source_leaf_labels as
        with endpoints as (
          select
            wds_id,
            primary_label as component_label,
            primary_label_raw as component_label_raw
          from msc_source_system_rows
          where primary_label is not null and primary_label <> ''
          union all
          select
            wds_id,
            secondary_label as component_label,
            secondary_label_raw as component_label_raw
          from msc_source_system_rows
          where secondary_label is not null and secondary_label <> ''
          union all
          select
            wds_id,
            primary_label as component_label,
            primary_label_raw as component_label_raw
          from msc_source_orbit_rows
          where primary_label is not null and primary_label <> ''
          union all
          select
            wds_id,
            secondary_label as component_label,
            secondary_label_raw as component_label_raw
          from msc_source_orbit_rows
          where secondary_label is not null and secondary_label <> ''
        ), typed as (
          select distinct
            e.wds_id,
            e.component_label,
            case
              when g.component_label is null then true
              when coalesce(e.component_label_raw, '') <> upper(coalesce(e.component_label_raw, ''))
               and not exists (
                 select 1
                 from msc_source_group_raw_labels gr
                 where gr.wds_id = e.wds_id
                   and gr.component_label = e.component_label
                   and gr.component_label_raw = e.component_label_raw
               ) then true
              else false
            end as is_leaf_endpoint
          from endpoints e
          left join msc_source_group_labels g
            on g.wds_id = e.wds_id
           and g.component_label = e.component_label
        )
        select
          t.wds_id,
          coalesce(r.system_display_name, 'WDS ' || t.wds_id) as system_display_name,
          row_number() over (partition by t.wds_id order by t.component_label)::bigint as ordinal,
          t.component_label,
          regexp_replace(t.component_label, '[ab]$', '') as component_stem
        from typed t
        left join msc_system_roots r on r.wds_id = t.wds_id
        where t.is_leaf_endpoint
        """
    )
    con.execute(
        """
        create temp table msc_leaf_labels as
        select
          wds_id,
          system_display_name,
          ordinal,
          component_label,
          component_stem
        from msc_source_leaf_labels
        union all
        select
          l.wds_id,
          l.system_display_name,
          l.ordinal,
          l.component_label,
          l.component_stem
        from msc_inferred_leaves l
        where not exists (
          select 1
          from msc_source_leaf_labels s
          where s.wds_id = l.wds_id
        )
        """
    )
    log(f"Arm stage complete: msc_source_rows ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating sol_authority_typed")
    con.execute(
        """
        create temp table sol_authority_typed as
        with typed as (
          select
            cast(nullif(source_pk, '') as bigint) as source_pk,
            nullif(trim(object_name), '') as object_name,
            lower(
              trim(
                regexp_replace(
                  regexp_replace(coalesce(object_name, ''), '[^0-9A-Za-z]+', ' ', 'g'),
                  '\\s+',
                  ' ',
                  'g'
                )
              )
            ) as object_name_norm,
            lower(trim(coalesce(object_class, ''))) as object_class_norm,
            lower(trim(coalesce(object_kind, object_class, ''))) as object_kind_norm,
            nullif(trim(parent_object_name), '') as parent_object_name,
            lower(trim(coalesce(parent_object_name, ''))) as parent_object_name_norm,
            nullif(trim(orbital_period_days), '')::double as orbital_period_days,
            nullif(trim(semi_major_axis_au), '')::double as semi_major_axis_au,
            nullif(trim(eccentricity), '')::double as eccentricity,
            nullif(trim(inclination_deg), '')::double as inclination_deg,
            nullif(trim(epoch_tdb_jd), '')::double as epoch_tdb_jd,
            nullif(trim(radius_km), '')::double as radius_km,
            nullif(trim(mass_kg), '')::double as mass_kg,
            nullif(trim(horizons_query_url), '') as source_url,
            nullif(trim(source_row_hash), '') as source_row_hash
          from sol_authority_raw
        )
        select *
        from typed
        where source_pk is not null
          and object_name is not null
          and object_name_norm <> ''
        """
    )
    log(f"Arm stage complete: sol_authority_typed ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating sol_moon_orbits")
    con.execute(
        """
        create temp table sol_moon_orbits as
        with sol_system as (
          select system_id, stable_object_key as system_key
          from core.systems
          where lower(coalesce(system_name_norm, '')) = 'sol'
             or lower(coalesce(stable_object_key, '')) = 'system:sol'
          order by system_id
          limit 1
        ), sol_planets as (
          select
            p.planet_id,
            p.system_id,
            p.stable_object_key as planet_key,
            lower(trim(coalesce(p.planet_name_norm, ''))) as planet_name_norm
          from core.planets p
          join sol_system s on s.system_id = p.system_id
        )
        select
          m.source_pk as moon_source_pk,
          m.object_name as moon_name,
          m.object_name_norm as moon_name_norm,
          m.parent_object_name as parent_name,
          m.parent_object_name_norm as parent_name_norm,
          m.object_kind_norm as moon_kind_norm,
          m.orbital_period_days,
          m.semi_major_axis_au,
          m.eccentricity,
          m.inclination_deg,
          m.epoch_tdb_jd,
          m.mass_kg as moon_mass_kg,
          m.radius_km as moon_radius_km,
          m.source_row_hash,
          m.source_url,
          p.planet_id as parent_planet_id,
          p.planet_key as parent_planet_key,
          p.planet_name_norm as parent_planet_name_norm
        from sol_authority_typed m
        join sol_planets p
          on m.parent_object_name_norm = p.planet_name_norm
        where m.object_class_norm = 'moon'
        """
    )
    log(f"Arm stage complete: sol_moon_orbits ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating sol_small_body_orbits")
    con.execute(
        """
        create temp table sol_small_body_orbits as
        with sol_system as (
          select system_id, stable_object_key as system_key
          from core.systems
          where lower(coalesce(system_name_norm, '')) = 'sol'
             or lower(coalesce(stable_object_key, '')) = 'system:sol'
          order by system_id
          limit 1
        )
        select
          m.source_pk as body_source_pk,
          m.object_name as body_name,
          m.object_name_norm as body_name_norm,
          m.object_kind_norm as body_kind_norm,
          coalesce(nullif(m.parent_object_name, ''), 'Sun') as parent_name,
          coalesce(nullif(m.parent_object_name_norm, ''), 'sun') as parent_name_norm,
          m.orbital_period_days,
          m.semi_major_axis_au,
          m.eccentricity,
          m.inclination_deg,
          m.epoch_tdb_jd,
          m.mass_kg as body_mass_kg,
          m.radius_km as body_radius_km,
          m.source_row_hash,
          m.source_url,
          s.system_id as sol_system_id,
          s.system_key as sol_system_key
        from sol_authority_typed m
        join sol_system s on true
        where m.object_class_norm = 'minor_body'
          and coalesce(nullif(m.parent_object_name_norm, ''), 'sun') = 'sun'
        """
    )
    log(f"Arm stage complete: sol_small_body_orbits ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating sol_artificial_typed")
    con.execute(
        """
        create temp table sol_artificial_typed as
        with typed_raw as (
          select
            cast(nullif(source_pk, '') as bigint) as source_pk,
            nullif(trim(object_name), '') as object_name,
            lower(
              trim(
                regexp_replace(
                  regexp_replace(coalesce(object_name, ''), '[^0-9A-Za-z]+', ' ', 'g'),
                  '\\s+',
                  ' ',
                  'g'
                )
              )
            ) as object_name_norm,
            lower(trim(coalesce(object_kind, 'artificial'))) as object_kind_norm,
            nullif(trim(parent_object_name), '') as parent_object_name,
            lower(trim(coalesce(parent_object_name, ''))) as parent_object_name_norm,
            nullif(trim(center_code), '') as center_code,
            nullif(trim(orbital_period_days), '')::double as orbital_period_days_raw,
            nullif(trim(semi_major_axis_au), '')::double as semi_major_axis_au_raw,
            nullif(trim(eccentricity), '')::double as eccentricity_raw,
            nullif(trim(inclination_deg), '')::double as inclination_deg,
            nullif(trim(epoch_tdb_jd), '')::double as epoch_tdb_jd,
            nullif(trim(radius_km), '')::double as radius_km,
            nullif(trim(mass_kg), '')::double as mass_kg,
            cast(coalesce(nullif(trim(freshness_window_days), ''), '45') as int) as freshness_window_days,
            nullif(trim(target_body_name), '') as target_body_name,
            nullif(trim(horizons_query_url), '') as source_url,
            nullif(trim(source_row_hash), '') as source_row_hash
          from sol_artificial_raw
        ), typed as (
          select
            source_pk,
            object_name,
            object_name_norm,
            object_kind_norm,
            parent_object_name,
            parent_object_name_norm,
            center_code,
            case
              when orbital_period_days_raw is null then null
              when not isfinite(orbital_period_days_raw) then null
              when abs(orbital_period_days_raw) >= 1e20 then null
              when orbital_period_days_raw <= 0.0 then null
              when coalesce(eccentricity_raw, 0.0) >= 1.0 then null
              when coalesce(semi_major_axis_au_raw, 1.0) <= 0.0 then null
              else orbital_period_days_raw
            end as orbital_period_days,
            case
              when semi_major_axis_au_raw is null then null
              when not isfinite(semi_major_axis_au_raw) then null
              when abs(semi_major_axis_au_raw) >= 1e9 then null
              else semi_major_axis_au_raw
            end as semi_major_axis_au,
            case
              when eccentricity_raw is null then null
              when not isfinite(eccentricity_raw) then null
              when abs(eccentricity_raw) >= 1e6 then null
              else eccentricity_raw
            end as eccentricity,
            inclination_deg,
            epoch_tdb_jd,
            radius_km,
            mass_kg,
            freshness_window_days,
            target_body_name,
            source_url,
            source_row_hash
          from typed_raw
        )
        select *
        from typed
        where source_pk is not null
          and object_name is not null
          and object_name_norm <> ''
        """
    )
    log(f"Arm stage complete: sol_artificial_typed ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating sol_artificial_orbits")
    con.execute(
        """
        create temp table sol_artificial_orbits as
        with sol_system as (
          select system_id, stable_object_key as system_key
          from core.systems
          where lower(coalesce(system_name_norm, '')) = 'sol'
             or lower(coalesce(stable_object_key, '')) = 'system:sol'
          order by system_id
          limit 1
        ), sol_planets as (
          select
            p.planet_id,
            p.stable_object_key as planet_key,
            lower(trim(coalesce(p.planet_name_norm, ''))) as planet_name_norm
          from core.planets p
          join sol_system s on s.system_id = p.system_id
        )
        select
          a.source_pk as artifact_source_pk,
          a.object_name as artifact_name,
          a.object_name_norm as artifact_name_norm,
          a.object_kind_norm as artifact_kind_norm,
          coalesce(nullif(a.parent_object_name, ''), 'Sun') as parent_name,
          coalesce(nullif(a.parent_object_name_norm, ''), 'sun') as parent_name_norm,
          a.center_code,
          a.orbital_period_days,
          a.semi_major_axis_au,
          a.eccentricity,
          a.inclination_deg,
          a.epoch_tdb_jd,
          a.mass_kg as artifact_mass_kg,
          a.radius_km as artifact_radius_km,
          greatest(coalesce(a.freshness_window_days, 45), 1)::int as freshness_window_days,
          a.target_body_name,
          a.source_row_hash,
          a.source_url,
          case
            when coalesce(nullif(a.parent_object_name_norm, ''), 'sun') = 'sun' then 'comp:system:system:sol'
            when p.planet_key is not null then 'comp:planet:' || p.planet_key
            else 'comp:system:system:sol'
          end as parent_component_key,
          case
            when coalesce(nullif(a.parent_object_name_norm, ''), 'sun') = 'sun' then 'comp:system:system:sol'
            when p.planet_key is not null then 'comp:planet:' || p.planet_key
            else 'comp:system:system:sol'
          end as host_component_key,
          case
            when coalesce(nullif(a.parent_object_name_norm, ''), 'sun') = 'sun' then 'comp:star:star:sol:sun'
            when p.planet_key is not null then 'comp:planet:' || p.planet_key
            else 'comp:star:star:sol:sun'
          end as primary_component_key
        from sol_artificial_typed a
        left join sol_planets p
          on p.planet_name_norm = coalesce(nullif(a.parent_object_name_norm, ''), 'sun')
        """
    )
    log(f"Arm stage complete: sol_artificial_orbits ({time.monotonic() - stage_started:.1f}s)")

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
        ), core_planet_components as (
          select
            'comp:planet:' || p.stable_object_key as stable_component_key,
            case
              when lower(coalesce(p.planet_size_mass_class, '')) = 'subplanet' then 'subplanet'
              else 'planet'
            end::varchar as component_type,
            'planet'::varchar as core_object_type,
            p.planet_id::bigint as core_object_id,
            p.planet_name as display_name,
            cast(null as varchar) as catalog_component_label,
            cast(null as double) as ra_deg,
            cast(null as double) as dec_deg,
            cast(null as double) as dist_pc,
            p.source_catalog as source_catalog,
            p.source_version as source_version,
            cast(p.source_pk as varchar) as source_pk,
            p.source_row_hash as source_row_hash,
            p.retrieval_checksum as retrieval_checksum,
            p.retrieved_at as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from core.planets p
        ), gaia_nss_companion_components as (
          select
            'comp:gaia_nss_companion:' || st.stable_object_key as stable_component_key,
            'star'::varchar as component_type,
            cast(null as varchar) as core_object_type,
            cast(null as bigint) as core_object_id,
            coalesce(nullif(st.star_name, ''), 'Gaia DR3 ' || st.gaia_id::varchar) || ' companion' as display_name,
            cast(null as varchar) as catalog_component_label,
            st.ra_deg::double as ra_deg,
            st.dec_deg::double as dec_deg,
            case when st.dist_ly is not null then st.dist_ly / {3.26156} else null end as dist_pc,
            'gaia_nss'::varchar as source_catalog,
            {sql_literal(gaia_nss_version)} as source_version,
            cast(st.gaia_id as varchar) as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(gaia_nss_checksum)} as retrieval_checksum,
            {sql_literal(gaia_nss_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from core.stars st
          join gaia_nss_best n on n.gaia_id = st.gaia_id
        ), sol_moon_components as (
          select
            'comp:moon:sol:' || s.moon_name_norm as stable_component_key,
            'moon'::varchar as component_type,
            cast(null as varchar) as core_object_type,
            cast(null as bigint) as core_object_id,
            s.moon_name as display_name,
            cast(null as varchar) as catalog_component_label,
            cast(null as double) as ra_deg,
            cast(null as double) as dec_deg,
            cast(null as double) as dist_pc,
            'sol_authority'::varchar as source_catalog,
            {sql_literal(sol_authority_version)} as source_version,
            cast(s.moon_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_authority_checksum)} as retrieval_checksum,
            {sql_literal(sol_authority_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from sol_moon_orbits s
        ), sol_small_body_components as (
          select
            'comp:minor_body:sol:' || s.body_name_norm as stable_component_key,
            'minor_body'::varchar as component_type,
            cast(null as varchar) as core_object_type,
            cast(null as bigint) as core_object_id,
            s.body_name as display_name,
            cast(null as varchar) as catalog_component_label,
            cast(null as double) as ra_deg,
            cast(null as double) as dec_deg,
            cast(null as double) as dist_pc,
            'sol_authority'::varchar as source_catalog,
            {sql_literal(sol_authority_version)} as source_version,
            cast(s.body_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_authority_checksum)} as retrieval_checksum,
            {sql_literal(sol_authority_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from sol_small_body_orbits s
        ), sol_artificial_components as (
          select
            'comp:artifact:sol:' || s.artifact_name_norm as stable_component_key,
            'artificial'::varchar as component_type,
            cast(null as varchar) as core_object_type,
            cast(null as bigint) as core_object_id,
            s.artifact_name as display_name,
            cast(null as varchar) as catalog_component_label,
            cast(null as double) as ra_deg,
            cast(null as double) as dec_deg,
            cast(null as double) as dist_pc,
            'sol_artificial'::varchar as source_catalog,
            {sql_literal(sol_artificial_version)} as source_version,
            cast(s.artifact_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_artificial_checksum)} as retrieval_checksum,
            {sql_literal(sol_artificial_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from sol_artificial_orbits s
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
        ), msc_group_components as (
          select
            'comp:msc_group:wds:' || g.wds_id || ':' || g.component_label as stable_component_key,
            'subsystem'::varchar as component_type,
            cast(null as varchar) as core_object_type,
            cast(null as bigint) as core_object_id,
            coalesce(r.system_display_name, 'WDS ' || g.wds_id) || ' ' || upper(g.component_label) as display_name,
            g.component_label as catalog_component_label,
            cast(null as double) as ra_deg,
            cast(null as double) as dec_deg,
            cast(null as double) as dist_pc,
            'msc'::varchar as source_catalog,
            {sql_literal(msc_version)} as source_version,
            g.wds_id || ':' || g.component_label as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(msc_checksum)} as retrieval_checksum,
            {sql_literal(msc_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from msc_source_group_labels g
          left join msc_system_roots r on r.wds_id = g.wds_id
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
          from msc_leaf_labels l
        ), unioned as (
          select * from core_system_components
          union all
          select * from core_star_components
          union all
          select * from core_planet_components
          union all
          select * from gaia_nss_companion_components
          union all
          select * from sol_moon_components
          union all
          select * from sol_small_body_components
          union all
          select * from sol_artificial_components
          union all
          select * from msc_system_components
          union all
          select * from msc_group_components
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
    log("Arm stage: creating msc_system_details")
    con.execute(
        f"""
        create table msc_system_details as
        with keyed as (
          select
            r.*,
            case
              when r.parent_label in ('', '*', 't') then 'comp:msc_system:wds:' || r.wds_id
              else 'comp:msc_group:wds:' || r.wds_id || ':' || r.parent_label
            end as parent_component_key,
            case
              when lp.component_label is not null and coalesce(r.primary_label_raw, '') <> upper(coalesce(r.primary_label_raw, '')) then 'comp:msc:wds:' || r.wds_id || ':' || r.primary_label
              when gp.component_label is not null then 'comp:msc_group:wds:' || r.wds_id || ':' || r.primary_label
              when lp.component_label is not null then 'comp:msc:wds:' || r.wds_id || ':' || r.primary_label
              else cast(null as varchar)
            end as primary_component_key,
            case
              when ls.component_label is not null and coalesce(r.secondary_label_raw, '') <> upper(coalesce(r.secondary_label_raw, '')) then 'comp:msc:wds:' || r.wds_id || ':' || r.secondary_label
              when gs.component_label is not null then 'comp:msc_group:wds:' || r.wds_id || ':' || r.secondary_label
              when ls.component_label is not null then 'comp:msc:wds:' || r.wds_id || ':' || r.secondary_label
              else cast(null as varchar)
            end as secondary_component_key
          from msc_source_system_rows r
          left join msc_source_group_labels gp
            on gp.wds_id = r.wds_id and gp.component_label = r.primary_label
          left join msc_source_group_labels gs
            on gs.wds_id = r.wds_id and gs.component_label = r.secondary_label
          left join msc_leaf_labels lp
            on lp.wds_id = r.wds_id and lp.component_label = r.primary_label
          left join msc_leaf_labels ls
            on ls.wds_id = r.wds_id and ls.component_label = r.secondary_label
        )
        select
          row_number() over (order by wds_id, coalesce(source_line_number, 0), primary_label, secondary_label)::bigint as msc_system_detail_id,
          wds_id,
          primary_label,
          secondary_label,
          parent_label,
          parent_component_key,
          primary_component_key,
          secondary_component_key,
          system_type,
          period_value,
          period_unit,
          case lower(coalesce(period_unit, ''))
            when 'd' then nullif(period_value, 0)
            when 'y' then nullif(period_value, 0) * 365.25
            when 'k' then nullif(period_value, 0) * 365250.0
            when 'm' then nullif(period_value, 0) * 36525000.0
            else null
          end as period_days,
          separation_value,
          separation_unit,
          case when separation_unit = '"' then nullif(separation_value, 0) else null end as separation_arcsec,
          case when lower(coalesce(separation_unit, '')) = 'm' then nullif(separation_value, 0) else null end as separation_mas,
          position_angle_deg,
          vmag_primary,
          spectral_type_primary,
          vmag_secondary,
          spectral_type_secondary,
          mass_primary_msun,
          mass_code_primary,
          mass_secondary_msun,
          mass_code_secondary,
          comment,
          source_line_number,
          raw_row,
          'msc'::varchar as source_catalog,
          {sql_literal(msc_version)}::varchar as source_version,
          wds_id || ':sys:' || coalesce(cast(source_line_number as varchar), primary_label || '-' || secondary_label) as source_pk,
          cast(null as varchar) as source_row_hash,
          {sql_literal(msc_checksum)}::varchar as retrieval_checksum,
          {sql_literal(msc_retrieved)}::varchar as retrieved_at,
          {sql_literal(args.ingested_at)}::varchar as ingested_at,
          {sql_literal(args.transform_version)}::varchar as transform_version
        from keyed
        """
    )
    log(f"Arm stage complete: msc_system_details ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating msc_orbit_details")
    con.execute(
        f"""
        create table msc_orbit_details as
        with keyed as (
          select
            r.*,
            coalesce(sys.parent_component_key, 'comp:msc_system:wds:' || r.wds_id) as host_component_key,
            case
              when lp.component_label is not null and coalesce(r.primary_label_raw, '') <> upper(coalesce(r.primary_label_raw, '')) then 'comp:msc:wds:' || r.wds_id || ':' || r.primary_label
              when gp.component_label is not null then 'comp:msc_group:wds:' || r.wds_id || ':' || r.primary_label
              when lp.component_label is not null then 'comp:msc:wds:' || r.wds_id || ':' || r.primary_label
              else cast(null as varchar)
            end as primary_component_key,
            case
              when ls.component_label is not null and coalesce(r.secondary_label_raw, '') <> upper(coalesce(r.secondary_label_raw, '')) then 'comp:msc:wds:' || r.wds_id || ':' || r.secondary_label
              when gs.component_label is not null then 'comp:msc_group:wds:' || r.wds_id || ':' || r.secondary_label
              when ls.component_label is not null then 'comp:msc:wds:' || r.wds_id || ':' || r.secondary_label
              else cast(null as varchar)
            end as secondary_component_key
          from msc_source_orbit_rows r
          left join msc_system_details sys
            on sys.wds_id = r.wds_id
           and sys.primary_label = r.primary_label
           and sys.secondary_label = r.secondary_label
          left join msc_source_group_labels gp
            on gp.wds_id = r.wds_id and gp.component_label = r.primary_label
          left join msc_source_group_labels gs
            on gs.wds_id = r.wds_id and gs.component_label = r.secondary_label
          left join msc_leaf_labels lp
            on lp.wds_id = r.wds_id and lp.component_label = r.primary_label
          left join msc_leaf_labels ls
            on ls.wds_id = r.wds_id and ls.component_label = r.secondary_label
        )
        select
          row_number() over (order by wds_id, coalesce(source_line_number, 0), system_label)::bigint as msc_orbit_detail_id,
          wds_id,
          system_label,
          primary_label,
          secondary_label,
          host_component_key,
          primary_component_key,
          secondary_component_key,
          period_value,
          period_unit,
          case lower(coalesce(period_unit, ''))
            when 'd' then nullif(period_value, 0)
            when 'y' then nullif(period_value, 0) * 365.25
            else null
          end as period_days,
          periastron_epoch,
          eccentricity,
          semi_major_axis_arcsec,
          node_deg,
          longitude_periastron_deg,
          inclination_deg,
          semi_amplitude_primary_kms,
          semi_amplitude_secondary_kms,
          center_of_mass_velocity_kms,
          node_flag,
          note,
          source_line_number,
          raw_row,
          'msc'::varchar as source_catalog,
          {sql_literal(msc_version)}::varchar as source_version,
          wds_id || ':orb:' || coalesce(cast(source_line_number as varchar), system_label) as source_pk,
          cast(null as varchar) as source_row_hash,
          {sql_literal(msc_checksum)}::varchar as retrieval_checksum,
          {sql_literal(msc_retrieved)}::varchar as retrieved_at,
          {sql_literal(args.ingested_at)}::varchar as ingested_at,
          {sql_literal(args.transform_version)}::varchar as transform_version
        from keyed
        """
    )
    log(f"Arm stage complete: msc_orbit_details ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating msc_component_details")
    con.execute(
        f"""
        create table msc_component_details as
        with core_component_match as (
          select
            st.system_id,
            st.star_id,
            st.stable_object_key,
            'comp:star:' || st.stable_object_key as stable_component_key,
            st.wds_id,
            lower(regexp_replace(coalesce(st.component, ''), '[^0-9A-Za-z]+', '', 'g')) as component_label
          from core.stars st
          where st.wds_id is not null
            and trim(coalesce(st.component, '')) <> ''
        ), typed as (
          select
            nullif(wds_id, '') as wds_id,
            lower(regexp_replace(coalesce(component, ''), '[^0-9A-Za-z]+', '', 'g')) as component_label,
            nullif(preferred_name, '') as preferred_name,
            nullif(sep_arcsec, '')::double as sep_arcsec,
            nullif(spectral_type_raw, '') as spectral_type_raw,
            nullif(parallax_mas, '')::double as parallax_mas,
            nullif(pm_ra_mas_yr, '')::double as pm_ra_mas_yr,
            nullif(pm_dec_mas_yr, '')::double as pm_dec_mas_yr,
            nullif(radial_velocity_kms, '')::double as radial_velocity_kms,
            nullif(bmag, '')::double as bmag,
            nullif(vmag, '')::double as vmag,
            nullif(imag, '')::double as imag,
            nullif(jmag, '')::double as jmag,
            nullif(hmag, '')::double as hmag,
            nullif(kmag, '')::double as kmag,
            nullif(grade, '') as grade,
            nullif(other_identifiers, '') as other_identifiers,
            cast(nullif(subsystem_count, '') as bigint) as subsystem_count,
            cast(nullif(orbit_count, '') as bigint) as orbit_count
          from msc_raw
          where nullif(wds_id, '') is not null
        )
        select
          row_number() over (order by t.wds_id, t.component_label, coalesce(m.star_id, -1))::bigint as msc_component_detail_id,
          s.system_id,
          m.star_id,
          s.stable_object_key,
          coalesce(m.stable_component_key, 'comp:msc:wds:' || t.wds_id || ':' || t.component_label) as stable_component_key,
          t.wds_id,
          t.component_label,
          t.preferred_name,
          t.sep_arcsec,
          t.spectral_type_raw,
          t.parallax_mas,
          t.pm_ra_mas_yr,
          t.pm_dec_mas_yr,
          t.radial_velocity_kms,
          t.bmag,
          t.vmag,
          t.imag,
          t.jmag,
          t.hmag,
          t.kmag,
          t.grade,
          t.other_identifiers,
          t.subsystem_count,
          t.orbit_count,
          'msc'::varchar as source_catalog,
          {sql_literal(msc_version)}::varchar as source_version,
          t.wds_id || ':' || t.component_label as source_pk,
          cast(null as varchar) as source_row_hash,
          {sql_literal(msc_checksum)}::varchar as retrieval_checksum,
          {sql_literal(msc_retrieved)}::varchar as retrieved_at,
          {sql_literal(args.ingested_at)}::varchar as ingested_at,
          {sql_literal(args.transform_version)}::varchar as transform_version
        from typed t
        left join core_component_match m
          on m.wds_id = t.wds_id
         and m.component_label = t.component_label
        left join core.systems s on s.wds_id = t.wds_id
        """
    )
    log(f"Arm stage complete: msc_component_details ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating wds_component_observations")
    con.execute(
        f"""
        create table wds_component_observations as
        with core_component_match as (
          select
            st.system_id,
            st.star_id,
            st.stable_object_key,
            'comp:star:' || st.stable_object_key as stable_component_key,
            st.wds_id,
            lower(regexp_replace(coalesce(st.component, ''), '[^0-9A-Za-z]+', '', 'g')) as component_label
          from core.stars st
          where st.wds_id is not null
            and trim(coalesce(st.component, '')) <> ''
        ), typed as (
          select
            nullif(wds_id, '') as wds_id,
            nullif(discoverer, '') as discoverer,
            lower(regexp_replace(coalesce(component, ''), '[^0-9A-Za-z]+', '', 'g')) as component_label,
            try_cast(nullif(first_year, '') as bigint) as first_year,
            try_cast(nullif(last_year, '') as bigint) as last_year,
            try_cast(nullif(obs_count, '') as bigint) as obs_count,
            try_cast(nullif(theta_first_deg, '') as double) as theta_first_deg,
            try_cast(nullif(theta_last_deg, '') as double) as theta_last_deg,
            try_cast(nullif(rho_first_arcsec, '') as double) as rho_first_arcsec,
            try_cast(nullif(rho_last_arcsec, '') as double) as rho_last_arcsec,
            try_cast(nullif(mag_primary, '') as double) as mag_primary,
            try_cast(nullif(mag_secondary, '') as double) as mag_secondary,
            nullif(spectral_type_raw, '') as spectral_type_raw,
            try_cast(nullif(pm_primary_ra, '') as double) as pm_primary_ra,
            try_cast(nullif(pm_primary_dec, '') as double) as pm_primary_dec,
            try_cast(nullif(pm_secondary_ra, '') as double) as pm_secondary_ra,
            try_cast(nullif(pm_secondary_dec, '') as double) as pm_secondary_dec,
            nullif(dm_designation, '') as dm_designation,
            nullif(note, '') as note,
            nullif(precise_coordinate, '') as precise_coordinate,
            try_cast(nullif(ra_deg, '') as double) as ra_deg,
            try_cast(nullif(dec_deg, '') as double) as dec_deg
          from wds_raw
          where nullif(wds_id, '') is not null
        )
        select
          row_number() over (order by t.wds_id, t.component_label, coalesce(m.star_id, -1))::bigint as wds_component_observation_id,
          s.system_id,
          m.star_id,
          s.stable_object_key,
          coalesce(m.stable_component_key, 'comp:wds:' || t.wds_id || ':' || t.component_label) as stable_component_key,
          t.wds_id,
          t.discoverer,
          t.component_label,
          t.first_year,
          t.last_year,
          t.obs_count,
          t.theta_first_deg,
          t.theta_last_deg,
          t.rho_first_arcsec,
          t.rho_last_arcsec,
          t.mag_primary,
          t.mag_secondary,
          t.spectral_type_raw,
          t.pm_primary_ra,
          t.pm_primary_dec,
          t.pm_secondary_ra,
          t.pm_secondary_dec,
          t.dm_designation,
          t.note,
          t.precise_coordinate,
          t.ra_deg,
          t.dec_deg,
          'wds'::varchar as source_catalog,
          {sql_literal(wds_version)}::varchar as source_version,
          t.wds_id || ':' || t.component_label as source_pk,
          cast(null as varchar) as source_row_hash,
          {sql_literal(wds_checksum)}::varchar as retrieval_checksum,
          {sql_literal(wds_retrieved)}::varchar as retrieved_at,
          {sql_literal(args.ingested_at)}::varchar as ingested_at,
          {sql_literal(args.transform_version)}::varchar as transform_version
        from typed t
        left join core_component_match m
          on m.wds_id = t.wds_id
         and m.component_label = t.component_label
        left join core.systems s on s.wds_id = t.wds_id
        """
    )
    log(f"Arm stage complete: wds_component_observations ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating system_hierarchy_edges")
    con.execute(
        f"""
        create table system_hierarchy_edges as
        with core_star_edges as (
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
        ), core_planet_edges as (
          select
            'comp:system:' || sys.stable_object_key as parent_component_key,
            'comp:planet:' || p.stable_object_key as child_component_key,
            'contains'::varchar as edge_kind,
            'planet'::varchar as member_role,
            'core_system_planet'::varchar as catalog_relation_label,
            1::int as depth_hint,
            coalesce(p.match_confidence, 0.90)::double as confidence_score,
            case
              when coalesce(p.match_confidence, 0.90) >= 0.95 then 'high'
              when coalesce(p.match_confidence, 0.90) >= 0.80 then 'medium'
              when coalesce(p.match_confidence, 0.90) >= 0.60 then 'low'
              else 'illustrative'
            end as confidence_tier,
            '["core","planet_catalog"]'::varchar as evidence_catalogs_json,
            json_object('system_id', sys.system_id, 'planet_id', p.planet_id) as evidence_ids_json,
            p.source_catalog as source_catalog,
            p.source_version as source_version,
            cast(p.source_pk as varchar) as source_pk,
            p.source_row_hash as source_row_hash,
            p.retrieval_checksum as retrieval_checksum,
            p.retrieved_at as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from core.planets p
          join core.systems sys on sys.system_id = p.system_id
        ), sol_moon_edges as (
          select
            'comp:planet:' || s.parent_planet_key as parent_component_key,
            'comp:moon:sol:' || s.moon_name_norm as child_component_key,
            'contains'::varchar as edge_kind,
            'satellite'::varchar as member_role,
            'sol_authority_moon_parent'::varchar as catalog_relation_label,
            2::int as depth_hint,
            0.995::double as confidence_score,
            'high'::varchar as confidence_tier,
            '["sol_authority"]'::varchar as evidence_catalogs_json,
            json_object(
              'moon_source_pk', s.moon_source_pk,
              'moon_name', s.moon_name,
              'parent_name', s.parent_name
            ) as evidence_ids_json,
            'sol_authority'::varchar as source_catalog,
            {sql_literal(sol_authority_version)} as source_version,
            cast(s.moon_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_authority_checksum)} as retrieval_checksum,
            {sql_literal(sol_authority_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from sol_moon_orbits s
        ), sol_small_body_edges as (
          select
            'comp:system:system:sol'::varchar as parent_component_key,
            'comp:minor_body:sol:' || s.body_name_norm as child_component_key,
            'contains'::varchar as edge_kind,
            'minor_body'::varchar as member_role,
            'sol_authority_minor_body'::varchar as catalog_relation_label,
            1::int as depth_hint,
            0.99::double as confidence_score,
            'high'::varchar as confidence_tier,
            '["sol_authority"]'::varchar as evidence_catalogs_json,
            json_object(
              'body_source_pk', s.body_source_pk,
              'body_name', s.body_name,
              'body_kind', s.body_kind_norm
            ) as evidence_ids_json,
            'sol_authority'::varchar as source_catalog,
            {sql_literal(sol_authority_version)} as source_version,
            cast(s.body_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_authority_checksum)} as retrieval_checksum,
            {sql_literal(sol_authority_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from sol_small_body_orbits s
        ), sol_artificial_edges as (
          select
            s.parent_component_key as parent_component_key,
            'comp:artifact:sol:' || s.artifact_name_norm as child_component_key,
            'contains'::varchar as edge_kind,
            'artificial'::varchar as member_role,
            'sol_artificial_object'::varchar as catalog_relation_label,
            case
              when s.parent_component_key = 'comp:system:system:sol' then 1
              else 2
            end::int as depth_hint,
            0.98::double as confidence_score,
            'high'::varchar as confidence_tier,
            '["sol_artificial"]'::varchar as evidence_catalogs_json,
            json_object(
              'artifact_source_pk', s.artifact_source_pk,
              'artifact_name', s.artifact_name,
              'artifact_kind', s.artifact_kind_norm,
              'parent_name', s.parent_name
            ) as evidence_ids_json,
            'sol_artificial'::varchar as source_catalog,
            {sql_literal(sol_artificial_version)} as source_version,
            cast(s.artifact_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_artificial_checksum)} as retrieval_checksum,
            {sql_literal(sol_artificial_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from sol_artificial_orbits s
        ), msc_edge_endpoints as (
          select
            d.parent_component_key,
            d.primary_component_key as child_component_key,
            'contains'::varchar as edge_kind,
            d.primary_label as member_role,
            d.*
          from msc_system_details d
          where d.primary_component_key is not null
          union all
          select
            d.parent_component_key,
            d.secondary_component_key as child_component_key,
            'contains'::varchar as edge_kind,
            d.secondary_label as member_role,
            d.*
          from msc_system_details d
          where d.secondary_component_key is not null
        ), msc_edges as (
          select
            e.parent_component_key,
            e.child_component_key,
            e.edge_kind,
            e.member_role,
            'msc_source_subsystem'::varchar as catalog_relation_label,
            case
              when e.parent_label in ('', '*', 't') then 1
              else 2
            end::int as depth_hint,
            0.90::double as confidence_score,
            'medium'::varchar as confidence_tier,
            '["msc"]'::varchar as evidence_catalogs_json,
            json_object(
              'wds_id', e.wds_id,
              'primary_label', e.primary_label,
              'secondary_label', e.secondary_label,
              'parent_label', e.parent_label,
              'system_type', e.system_type,
              'source_line_number', e.source_line_number
            ) as evidence_ids_json,
            'msc'::varchar as source_catalog,
            {sql_literal(msc_version)} as source_version,
            e.source_pk as source_pk,
            e.source_row_hash as source_row_hash,
            {sql_literal(msc_checksum)} as retrieval_checksum,
            {sql_literal(msc_retrieved)} as retrieved_at,
            {sql_literal(args.ingested_at)} as ingested_at,
            {sql_literal(args.transform_version)} as transform_version
          from msc_edge_endpoints e
          join component_entities parent_ce on parent_ce.stable_component_key = e.parent_component_key
          join component_entities child_ce on child_ce.stable_component_key = e.child_component_key
        ), unioned as (
          select * from core_star_edges
          union all
          select * from core_planet_edges
          union all
          select * from sol_moon_edges
          union all
          select * from sol_small_body_edges
          union all
          select * from sol_artificial_edges
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
            d.parent_component_key as host_component_key,
            d.primary_component_key,
            d.secondary_component_key,
            case
              when p.component_type = 'subsystem' or s.component_type = 'subsystem' then 'hierarchical_pair'
              else 'binary'
            end::varchar as relation_kind,
            'bary:center:msc:' || d.wds_id || ':' || d.primary_label || '-' || d.secondary_label as barycenter_key,
            cast(null as bigint) as preferred_solution_id,
            0.90::double as confidence_score,
            'medium'::varchar as confidence_tier,
            '["msc"]'::varchar as evidence_catalogs_json,
            json_object(
              'wds_id', d.wds_id,
              'primary_label', d.primary_label,
              'secondary_label', d.secondary_label,
              'parent_label', d.parent_label,
              'system_type', d.system_type,
              'source_line_number', d.source_line_number
            ) as evidence_ids_json,
            'msc'::varchar as source_catalog,
            {sql_literal(msc_version)} as source_version,
            d.source_pk as source_pk,
            d.source_row_hash as source_row_hash,
            {sql_literal(msc_checksum)} as retrieval_checksum,
            {sql_literal(msc_retrieved)} as retrieved_at
          from msc_system_details d
          join component_entities host on host.stable_component_key = d.parent_component_key
          join component_entities p on p.stable_component_key = d.primary_component_key
          join component_entities s on s.stable_component_key = d.secondary_component_key
          where d.primary_component_key is not null
            and d.secondary_component_key is not null
        ), gaia_nss_pairs as (
          select
            'comp:system:' || sys.stable_object_key as host_component_key,
            'comp:star:' || st.stable_object_key as primary_component_key,
            'comp:gaia_nss_companion:' || st.stable_object_key as secondary_component_key,
            'binary'::varchar as relation_kind,
            'bary:center:gaia_nss:' || st.stable_object_key as barycenter_key,
            cast(null as bigint) as preferred_solution_id,
            case
              when coalesce(n.significance, 0.0) >= 100.0 then 0.98
              when coalesce(n.significance, 0.0) >= 50.0 then 0.95
              when coalesce(n.significance, 0.0) >= 20.0 then 0.90
              when coalesce(n.significance, 0.0) >= 10.0 then 0.82
              else 0.72
            end::double as confidence_score,
            case
              when coalesce(n.significance, 0.0) >= 50.0 then 'high'
              when coalesce(n.significance, 0.0) >= 20.0 then 'medium'
              when coalesce(n.significance, 0.0) >= 10.0 then 'low'
              else 'illustrative'
            end as confidence_tier,
            '["gaia_nss"]'::varchar as evidence_catalogs_json,
            json_object(
              'gaia_id', st.gaia_id,
              'nss_solution_type', n.nss_solution_type,
              'significance', n.significance
            ) as evidence_ids_json,
            'gaia_nss'::varchar as source_catalog,
            {sql_literal(gaia_nss_version)} as source_version,
            cast(st.gaia_id as varchar) as source_pk,
            cast(null as varchar) as source_row_hash,
            {sql_literal(gaia_nss_checksum)} as retrieval_checksum,
            {sql_literal(gaia_nss_retrieved)} as retrieved_at
          from gaia_nss_best n
          join core.stars st on st.gaia_id = n.gaia_id
          join core.systems sys on sys.system_id = st.system_id
        ), planet_orbit_edges as (
          select
            'comp:system:' || sys.stable_object_key as host_component_key,
            case
              when host.stable_object_key is not null then 'comp:star:' || host.stable_object_key
              else 'comp:system:' || sys.stable_object_key
            end as primary_component_key,
            'comp:planet:' || p.stable_object_key as secondary_component_key,
            'planetary_orbit'::varchar as relation_kind,
            cast(null as varchar) as barycenter_key,
            cast(null as bigint) as preferred_solution_id,
            case
              when p.source_catalog = 'sol_authority' then 0.995
              when coalesce(p.match_confidence, 0.0) >= 0.95 then 0.94
              when coalesce(p.match_confidence, 0.0) >= 0.80 then 0.88
              when coalesce(p.match_confidence, 0.0) > 0.0 then 0.72
              else 0.60
            end::double as confidence_score,
            case
              when p.source_catalog = 'sol_authority' then 'high'
              when coalesce(p.match_confidence, 0.0) >= 0.80 then 'medium'
              when coalesce(p.match_confidence, 0.0) > 0.0 then 'low'
              else 'illustrative'
            end::varchar as confidence_tier,
            json_array(coalesce(p.source_catalog, 'core_planet_inventory'))::varchar as evidence_catalogs_json,
            json_object(
              'system_id', p.system_id,
              'star_id', p.star_id,
              'planet_id', p.planet_id,
              'planet_stable_object_key', p.stable_object_key
            ) as evidence_ids_json,
            coalesce(p.source_catalog, 'core_planet_inventory') as source_catalog,
            coalesce(p.source_version, {sql_literal(args.transform_version)}) as source_version,
            cast(coalesce(p.stable_object_key, cast(p.source_pk as varchar)) as varchar) as source_pk,
            p.source_row_hash as source_row_hash,
            coalesce(p.retrieval_checksum, '') as retrieval_checksum,
            coalesce(p.retrieved_at, '') as retrieved_at
          from core.planets p
          join core.systems sys on sys.system_id = p.system_id
          left join core.stars host on host.star_id = p.star_id
          where p.stable_object_key is not null
        ), sol_satellite_pairs as (
          select
            'comp:planet:' || s.parent_planet_key as host_component_key,
            'comp:planet:' || s.parent_planet_key as primary_component_key,
            'comp:moon:sol:' || s.moon_name_norm as secondary_component_key,
            'satellite'::varchar as relation_kind,
            case
              when s.parent_planet_name_norm = 'earth' and s.moon_name_norm = 'moon' then 'bary:center:sol:earth-moon'
              when s.parent_planet_name_norm = 'pluto' and s.moon_name_norm = 'charon' then 'bary:center:sol:pluto-charon'
              else null
            end as barycenter_key,
            cast(null as bigint) as preferred_solution_id,
            0.995::double as confidence_score,
            'high'::varchar as confidence_tier,
            '["sol_authority"]'::varchar as evidence_catalogs_json,
            json_object(
              'moon_source_pk', s.moon_source_pk,
              'moon_name', s.moon_name,
              'parent_name', s.parent_name
            ) as evidence_ids_json,
            'sol_authority'::varchar as source_catalog,
            {sql_literal(sol_authority_version)} as source_version,
            cast(s.moon_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_authority_checksum)} as retrieval_checksum,
            {sql_literal(sol_authority_retrieved)} as retrieved_at
          from sol_moon_orbits s
        ), sol_small_body_orbit_edges as (
          select
            'comp:system:system:sol'::varchar as host_component_key,
            'comp:star:star:sol:sun'::varchar as primary_component_key,
            'comp:minor_body:sol:' || s.body_name_norm as secondary_component_key,
            'orbits'::varchar as relation_kind,
            cast(null as varchar) as barycenter_key,
            cast(null as bigint) as preferred_solution_id,
            0.99::double as confidence_score,
            'high'::varchar as confidence_tier,
            '["sol_authority"]'::varchar as evidence_catalogs_json,
            json_object(
              'body_source_pk', s.body_source_pk,
              'body_name', s.body_name,
              'body_kind', s.body_kind_norm
            ) as evidence_ids_json,
            'sol_authority'::varchar as source_catalog,
            {sql_literal(sol_authority_version)} as source_version,
            cast(s.body_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_authority_checksum)} as retrieval_checksum,
            {sql_literal(sol_authority_retrieved)} as retrieved_at
          from sol_small_body_orbits s
        ), sol_artificial_orbit_edges as (
          select
            s.host_component_key as host_component_key,
            s.primary_component_key as primary_component_key,
            'comp:artifact:sol:' || s.artifact_name_norm as secondary_component_key,
            'artificial_orbit'::varchar as relation_kind,
            cast(null as varchar) as barycenter_key,
            cast(null as bigint) as preferred_solution_id,
            0.98::double as confidence_score,
            'high'::varchar as confidence_tier,
            '["sol_artificial"]'::varchar as evidence_catalogs_json,
            json_object(
              'artifact_source_pk', s.artifact_source_pk,
              'artifact_name', s.artifact_name,
              'artifact_kind', s.artifact_kind_norm,
              'center_code', s.center_code
            ) as evidence_ids_json,
            'sol_artificial'::varchar as source_catalog,
            {sql_literal(sol_artificial_version)} as source_version,
            cast(s.artifact_source_pk as varchar) as source_pk,
            s.source_row_hash as source_row_hash,
            {sql_literal(sol_artificial_checksum)} as retrieval_checksum,
            {sql_literal(sol_artificial_retrieved)} as retrieved_at
          from sol_artificial_orbits s
        ), unioned as (
          select * from core_pairs
          union all
          select * from msc_pairs
          union all
          select * from gaia_nss_pairs
          union all
          select * from planet_orbit_edges
          union all
          select * from sol_satellite_pairs
          union all
          select * from sol_small_body_orbit_edges
          union all
          select * from sol_artificial_orbit_edges
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
        f"""
        create table orbital_solutions as
        with sol_satellite_rows as (
          select
            e.orbit_edge_id,
            s.moon_source_pk as source_pk,
            s.epoch_tdb_jd,
            s.orbital_period_days,
            s.semi_major_axis_au,
            s.eccentricity,
            s.inclination_deg,
            s.source_row_hash,
            'horizons_elements_s2'::varchar as solver,
            'host_centered'::varchar as frame,
            0.995::double as confidence_score,
            'sol_authority'::varchar as source_catalog,
            {sql_literal(sol_authority_version)}::varchar as source_version,
            {sql_literal(sol_authority_checksum)}::varchar as retrieval_checksum,
            {sql_literal(sol_authority_retrieved)}::varchar as retrieved_at,
            cast(null as double) as center_of_mass_velocity_kms,
            cast(null as double) as semi_amplitude_primary_kms,
            cast(null as double) as mass_ratio,
            cast(null as bigint) as flags,
            cast(null as double) as significance,
            1::int as solution_rank,
            cast(null as double) as semi_major_axis_arcsec,
            cast(null as double) as node_deg,
            cast(null as double) as long_periastron_deg,
            cast(null as double) as time_periastron_jd,
            cast(null as double) as reference_epoch_jyear,
            cast(null as double) as reference_epoch_mjd,
            cast(null as double) as period_value,
            cast(null as varchar) as period_unit,
            cast(null as double) as period_error,
            cast(null as varchar) as axis_qualifier,
            cast(null as double) as axis_error,
            cast(null as double) as inclination_error,
            cast(null as double) as node_error,
            cast(null as double) as periastron_epoch,
            cast(null as varchar) as epoch_unit,
            cast(null as double) as eccentricity_error,
            cast(null as double) as long_periastron_error,
            cast(null as varchar) as discoverer,
            cast(null as varchar) as grade,
            cast(null as varchar) as notes_flag,
            cast(null as varchar) as reference_code,
            cast(null as varchar) as png_file,
            cast(null as double) as last_observed_year
          from sol_moon_orbits s
          join orbit_edges e
            on e.relation_kind = 'satellite'
           and e.source_catalog = 'sol_authority'
           and cast(e.source_pk as bigint) = s.moon_source_pk
        ), sol_small_body_rows as (
          select
            e.orbit_edge_id,
            s.body_source_pk as source_pk,
            s.epoch_tdb_jd,
            s.orbital_period_days,
            s.semi_major_axis_au,
            s.eccentricity,
            s.inclination_deg,
            s.source_row_hash,
            'horizons_elements_s3'::varchar as solver,
            'heliocentric'::varchar as frame,
            0.995::double as confidence_score,
            'sol_authority'::varchar as source_catalog,
            {sql_literal(sol_authority_version)}::varchar as source_version,
            {sql_literal(sol_authority_checksum)}::varchar as retrieval_checksum,
            {sql_literal(sol_authority_retrieved)}::varchar as retrieved_at,
            cast(null as double) as center_of_mass_velocity_kms,
            cast(null as double) as semi_amplitude_primary_kms,
            cast(null as double) as mass_ratio,
            cast(null as bigint) as flags,
            cast(null as double) as significance,
            1::int as solution_rank,
            cast(null as double) as semi_major_axis_arcsec,
            cast(null as double) as node_deg,
            cast(null as double) as long_periastron_deg,
            cast(null as double) as time_periastron_jd,
            cast(null as double) as reference_epoch_jyear,
            cast(null as double) as reference_epoch_mjd,
            cast(null as double) as period_value,
            cast(null as varchar) as period_unit,
            cast(null as double) as period_error,
            cast(null as varchar) as axis_qualifier,
            cast(null as double) as axis_error,
            cast(null as double) as inclination_error,
            cast(null as double) as node_error,
            cast(null as double) as periastron_epoch,
            cast(null as varchar) as epoch_unit,
            cast(null as double) as eccentricity_error,
            cast(null as double) as long_periastron_error,
            cast(null as varchar) as discoverer,
            cast(null as varchar) as grade,
            cast(null as varchar) as notes_flag,
            cast(null as varchar) as reference_code,
            cast(null as varchar) as png_file,
            cast(null as double) as last_observed_year
          from sol_small_body_orbits s
          join orbit_edges e
            on e.relation_kind = 'orbits'
           and e.source_catalog = 'sol_authority'
           and cast(e.source_pk as bigint) = s.body_source_pk
        ), sol_artificial_rows as (
          select
            e.orbit_edge_id,
            s.artifact_source_pk as source_pk,
            s.epoch_tdb_jd,
            s.orbital_period_days,
            s.semi_major_axis_au,
            s.eccentricity,
            s.inclination_deg,
            s.source_row_hash,
            'horizons_elements_s4'::varchar as solver,
            case
              when s.host_component_key = 'comp:system:system:sol' then 'heliocentric'
              else 'host_centered'
            end::varchar as frame,
            0.985::double as confidence_score,
            'sol_artificial'::varchar as source_catalog,
            {sql_literal(sol_artificial_version)}::varchar as source_version,
            {sql_literal(sol_artificial_checksum)}::varchar as retrieval_checksum,
            {sql_literal(sol_artificial_retrieved)}::varchar as retrieved_at,
            cast(null as double) as center_of_mass_velocity_kms,
            cast(null as double) as semi_amplitude_primary_kms,
            cast(null as double) as mass_ratio,
            cast(null as bigint) as flags,
            cast(null as double) as significance,
            1::int as solution_rank,
            cast(null as double) as semi_major_axis_arcsec,
            cast(null as double) as node_deg,
            cast(null as double) as long_periastron_deg,
            cast(null as double) as time_periastron_jd,
            cast(null as double) as reference_epoch_jyear,
            cast(null as double) as reference_epoch_mjd,
            cast(null as double) as period_value,
            cast(null as varchar) as period_unit,
            cast(null as double) as period_error,
            cast(null as varchar) as axis_qualifier,
            cast(null as double) as axis_error,
            cast(null as double) as inclination_error,
            cast(null as double) as node_error,
            cast(null as double) as periastron_epoch,
            cast(null as varchar) as epoch_unit,
            cast(null as double) as eccentricity_error,
            cast(null as double) as long_periastron_error,
            cast(null as varchar) as discoverer,
            cast(null as varchar) as grade,
            cast(null as varchar) as notes_flag,
            cast(null as varchar) as reference_code,
            cast(null as varchar) as png_file,
            cast(null as double) as last_observed_year
          from sol_artificial_orbits s
          join orbit_edges e
            on e.relation_kind = 'artificial_orbit'
           and e.source_catalog = 'sol_artificial'
           and cast(e.source_pk as bigint) = s.artifact_source_pk
        ), gaia_nss_rows as (
          select
            e.orbit_edge_id,
            n.gaia_id as source_pk,
            cast(null as double) as epoch_tdb_jd,
            n.period_days as orbital_period_days,
            cast(null as double) as semi_major_axis_au,
            n.eccentricity,
            n.inclination_deg,
            cast(null as varchar) as source_row_hash,
            n.nss_solution_type as solver,
            'gaia_barycentric'::varchar as frame,
            case
              when coalesce(n.significance, 0.0) >= 100.0 then 0.98
              when coalesce(n.significance, 0.0) >= 50.0 then 0.95
              when coalesce(n.significance, 0.0) >= 20.0 then 0.90
              when coalesce(n.significance, 0.0) >= 10.0 then 0.82
              else 0.72
            end::double as confidence_score,
            'gaia_nss'::varchar as source_catalog,
            {sql_literal(gaia_nss_version)}::varchar as source_version,
            {sql_literal(gaia_nss_checksum)}::varchar as retrieval_checksum,
            {sql_literal(gaia_nss_retrieved)}::varchar as retrieved_at,
            n.center_of_mass_velocity_kms,
            n.semi_amplitude_primary_kms,
            n.mass_ratio,
            n.flags,
            n.significance,
            row_number() over (
              partition by n.gaia_id
              order by coalesce(n.significance, -1.0) desc, coalesce(n.period_days, 1.0e18) asc
            ) as solution_rank,
            cast(null as double) as semi_major_axis_arcsec,
            cast(null as double) as node_deg,
            cast(null as double) as long_periastron_deg,
            cast(null as double) as time_periastron_jd,
            cast(null as double) as reference_epoch_jyear,
            cast(null as double) as reference_epoch_mjd,
            cast(null as double) as period_value,
            cast(null as varchar) as period_unit,
            cast(null as double) as period_error,
            cast(null as varchar) as axis_qualifier,
            cast(null as double) as axis_error,
            cast(null as double) as inclination_error,
            cast(null as double) as node_error,
            cast(null as double) as periastron_epoch,
            cast(null as varchar) as epoch_unit,
            cast(null as double) as eccentricity_error,
            cast(null as double) as long_periastron_error,
            cast(null as varchar) as discoverer,
            cast(null as varchar) as grade,
            cast(null as varchar) as notes_flag,
            cast(null as varchar) as reference_code,
            cast(null as varchar) as png_file,
            cast(null as double) as last_observed_year
          from (
            select
              cast(nullif(source_id, '') as bigint) as gaia_id,
              nullif(nss_solution_type, '') as nss_solution_type,
              nullif(period_days, '')::double as period_days,
              nullif(eccentricity, '')::double as eccentricity,
              nullif(center_of_mass_velocity_kms, '')::double as center_of_mass_velocity_kms,
              nullif(semi_amplitude_primary_kms, '')::double as semi_amplitude_primary_kms,
              nullif(mass_ratio, '')::double as mass_ratio,
              nullif(inclination_deg, '')::double as inclination_deg,
              nullif(flags, '')::bigint as flags,
              nullif(significance, '')::double as significance
            from gaia_nss_raw
            where cast(nullif(source_id, '') as bigint) is not null
          ) n
          join orbit_edges e
            on e.source_catalog = 'gaia_nss'
           and cast(e.source_pk as bigint) = n.gaia_id
        ), msc_orbit_rows as (
          select
            e.orbit_edge_id,
            o.source_pk,
            cast(null as double) as epoch_tdb_jd,
            o.period_days as orbital_period_days,
            cast(null as double) as semi_major_axis_au,
            nullif(o.eccentricity, 0) as eccentricity,
            nullif(o.inclination_deg, 0) as inclination_deg,
            o.source_row_hash,
            'msc'::varchar as solver,
            'source_native_hierarchical_multiple'::varchar as frame,
            0.90::double as confidence_score,
            'msc'::varchar as source_catalog,
            {sql_literal(msc_version)}::varchar as source_version,
            {sql_literal(msc_checksum)}::varchar as retrieval_checksum,
            {sql_literal(msc_retrieved)}::varchar as retrieved_at,
            o.center_of_mass_velocity_kms,
            o.semi_amplitude_primary_kms,
            cast(null as double) as mass_ratio,
            cast(null as bigint) as flags,
            cast(null as double) as significance,
            row_number() over (
              partition by e.orbit_edge_id
              order by
                case when o.period_days is not null then 0 else 1 end,
                case when nullif(o.semi_major_axis_arcsec, 0) is not null then 0 else 1 end,
                coalesce(o.source_line_number, 999999)
            )::int as solution_rank,
            nullif(o.semi_major_axis_arcsec, 0) as semi_major_axis_arcsec,
            nullif(o.node_deg, 0) as node_deg,
            nullif(o.longitude_periastron_deg, 0) as long_periastron_deg,
            cast(null as double) as time_periastron_jd,
            case
              when o.periastron_epoch is not null and o.periastron_epoch < 3000 then o.periastron_epoch
              else null
            end as reference_epoch_jyear,
            case
              when o.periastron_epoch is not null and o.periastron_epoch >= 3000 then o.periastron_epoch
              else null
            end as reference_epoch_mjd,
            o.period_value,
            o.period_unit,
            cast(null as double) as period_error,
            cast(null as varchar) as axis_qualifier,
            cast(null as double) as axis_error,
            cast(null as double) as inclination_error,
            cast(null as double) as node_error,
            o.periastron_epoch,
            cast(null as varchar) as epoch_unit,
            cast(null as double) as eccentricity_error,
            cast(null as double) as long_periastron_error,
            cast(null as varchar) as discoverer,
            cast(null as varchar) as grade,
            cast(null as varchar) as notes_flag,
            o.note as reference_code,
            cast(null as varchar) as png_file,
            cast(null as double) as last_observed_year
          from msc_orbit_details o
          join orbit_edges e
            on e.source_catalog = 'msc'
           and e.primary_component_key = o.primary_component_key
           and e.secondary_component_key = o.secondary_component_key
        ), orb6_system_edge_match as (
          select
            s.wds_id,
            case when count(*) = 1 then min(e.orbit_edge_id) else null end as orbit_edge_id,
            min(s.system_id) as system_id,
            min(s.stable_object_key) as system_key
          from core.systems s
          join orbit_edges e
            on e.host_component_key = 'comp:system:' || s.stable_object_key
           and e.relation_kind = 'binary'
          where s.wds_id is not null
          group by s.wds_id
        ), orb6_rows as (
          select
            m.orbit_edge_id,
            coalesce(o.ads_id, o.hip_id, o.hd_id, o.wds_id) as source_pk,
            cast(null as double) as epoch_tdb_jd,
            case lower(coalesce(o.period_unit, ''))
              when 'd' then o.period_value
              when 'y' then o.period_value * 365.25
              when 'c' then o.period_value * 36525.0
              when 'h' then o.period_value / 24.0
              else null
            end as orbital_period_days,
            cast(null as double) as semi_major_axis_au,
            o.eccentricity,
            o.inclination_deg,
            cast(null as varchar) as source_row_hash,
            'orb6'::varchar as solver,
            'relative_visual_binary'::varchar as frame,
            case
              when cast(nullif(o.grade, '') as int) <= 1 then 0.99
              when cast(nullif(o.grade, '') as int) = 2 then 0.95
              when cast(nullif(o.grade, '') as int) = 3 then 0.88
              when cast(nullif(o.grade, '') as int) = 4 then 0.75
              when cast(nullif(o.grade, '') as int) >= 5 then 0.62
              else 0.70
            end::double as confidence_score,
            'orb6'::varchar as source_catalog,
            {sql_literal(orb6_version)}::varchar as source_version,
            {sql_literal(orb6_checksum)}::varchar as retrieval_checksum,
            {sql_literal(orb6_retrieved)}::varchar as retrieved_at,
            cast(null as double) as center_of_mass_velocity_kms,
            cast(null as double) as semi_amplitude_primary_kms,
            cast(null as double) as mass_ratio,
            cast(null as bigint) as flags,
            cast(null as double) as significance,
            1::int as solution_rank,
            o.semi_major_axis_arcsec,
            o.node_deg,
            o.long_periastron_deg,
            case
              when lower(coalesce(o.epoch_unit, '')) = 'j' then o.periastron_epoch
              else null
            end as time_periastron_jd,
            case
              when lower(coalesce(o.epoch_unit, '')) = 'y' then o.periastron_epoch
              else null
            end as reference_epoch_jyear,
            case
              when lower(coalesce(o.epoch_unit, '')) = 'd' then o.periastron_epoch
              else null
            end as reference_epoch_mjd,
            o.period_value,
            o.period_unit,
            o.period_error,
            o.axis_qualifier,
            o.axis_error,
            o.inclination_error,
            o.node_error,
            o.periastron_epoch,
            o.epoch_unit,
            o.eccentricity_error,
            o.long_periastron_error,
            o.discoverer,
            o.grade,
            o.notes_flag,
            o.reference_code,
            o.png_file,
            o.last_observed_year
          from (
            select
              nullif(wds_id, '') as wds_id,
              nullif(discoverer, '') as discoverer,
              nullif(ads_id, '') as ads_id,
              nullif(hd_id, '') as hd_id,
              nullif(hip_id, '') as hip_id,
              try_cast(nullif(period_value, '') as double) as period_value,
              nullif(period_unit, '') as period_unit,
              try_cast(nullif(period_error, '') as double) as period_error,
              try_cast(nullif(semi_major_axis_arcsec, '') as double) as semi_major_axis_arcsec,
              nullif(axis_qualifier, '') as axis_qualifier,
              try_cast(nullif(axis_error, '') as double) as axis_error,
              try_cast(nullif(inclination_deg, '') as double) as inclination_deg,
              try_cast(nullif(inclination_error, '') as double) as inclination_error,
              try_cast(nullif(node_deg, '') as double) as node_deg,
              try_cast(nullif(node_error, '') as double) as node_error,
              try_cast(nullif(periastron_epoch, '') as double) as periastron_epoch,
              nullif(epoch_unit, '') as epoch_unit,
              try_cast(nullif(eccentricity, '') as double) as eccentricity,
              try_cast(nullif(eccentricity_error, '') as double) as eccentricity_error,
              try_cast(nullif(long_periastron_deg, '') as double) as long_periastron_deg,
              try_cast(nullif(long_periastron_error, '') as double) as long_periastron_error,
              try_cast(nullif(last_observed_year, '') as double) as last_observed_year,
              nullif(grade, '') as grade,
              nullif(notes_flag, '') as notes_flag,
              nullif(reference_code, '') as reference_code,
              nullif(png_file, '') as png_file
            from orb6_raw
            where nullif(wds_id, '') is not null
          ) o
          join orb6_system_edge_match m on m.wds_id = o.wds_id
          where m.orbit_edge_id is not null
        ), nasa_planet_orbit_source as (
          select *
          from (
            select
              n.*,
              lower(
                trim(
                  regexp_replace(
                    regexp_replace(coalesce(n.pl_name, ''), '[^0-9A-Za-z]+', ' ', 'g'),
                    '\\s+',
                    ' ',
                    'g'
                  )
                )
              ) as planet_name_norm,
              row_number() over (
                partition by lower(
                  trim(
                    regexp_replace(
                      regexp_replace(coalesce(n.pl_name, ''), '[^0-9A-Za-z]+', ' ', 'g'),
                      '\\s+',
                      ' ',
                      'g'
                    )
                  )
                )
                order by try_cast(nullif(n.objectid, '') as bigint) asc nulls last
              ) as rn
            from nasa_pscomppars_raw n
            where nullif(n.pl_name, '') is not null
          ) q
          where rn = 1
        ), planet_orbit_rows as (
          select
            e.orbit_edge_id,
            coalesce(nullif(n.objectid, ''), p.stable_object_key, cast(p.source_pk as varchar)) as source_pk,
            cast(null as double) as epoch_tdb_jd,
            coalesce(try_cast(nullif(n.pl_orbper, '') as double), p.orbital_period_days) as orbital_period_days,
            coalesce(try_cast(nullif(n.pl_orbsmax, '') as double), p.semi_major_axis_au) as semi_major_axis_au,
            coalesce(try_cast(nullif(n.pl_orbeccen, '') as double), p.eccentricity) as eccentricity,
            coalesce(try_cast(nullif(n.pl_orbincl, '') as double), p.inclination_deg) as inclination_deg,
            p.source_row_hash,
            case
              when p.source_catalog = 'nasa_exoplanet_archive' then 'nasa_pscomppars'
              when p.source_catalog = 'sol_authority' then 'horizons_elements_planet_inventory'
              else coalesce(p.source_catalog, 'core_planet_inventory')
            end::varchar as solver,
            case
              when p.source_catalog = 'sol_authority' then 'heliocentric'
              else 'host_centered'
            end::varchar as frame,
            case
              when p.source_catalog = 'sol_authority' then 0.995
              when coalesce(p.match_confidence, 0.0) >= 0.95 then 0.94
              when coalesce(p.match_confidence, 0.0) >= 0.80 then 0.88
              when coalesce(p.match_confidence, 0.0) > 0.0 then 0.72
              else 0.60
            end::double as confidence_score,
            coalesce(p.source_catalog, 'core_planet_inventory') as source_catalog,
            coalesce(p.source_version, {sql_literal(nasa_pscomppars_version)}) as source_version,
            coalesce(p.retrieval_checksum, {sql_literal(nasa_pscomppars_checksum)}) as retrieval_checksum,
            coalesce(p.retrieved_at, {sql_literal(nasa_pscomppars_retrieved)}) as retrieved_at,
            cast(null as double) as center_of_mass_velocity_kms,
            try_cast(nullif(n.pl_rvamp, '') as double) as semi_amplitude_primary_kms,
            cast(null as double) as mass_ratio,
            cast(null as bigint) as flags,
            cast(null as double) as significance,
            1::int as solution_rank,
            cast(null as double) as semi_major_axis_arcsec,
            cast(null as double) as node_deg,
            try_cast(nullif(n.pl_orblper, '') as double) as long_periastron_deg,
            try_cast(nullif(n.pl_orbtper, '') as double) as time_periastron_jd,
            cast(null as double) as reference_epoch_jyear,
            cast(null as double) as reference_epoch_mjd,
            coalesce(try_cast(nullif(n.pl_orbper, '') as double), p.orbital_period_days) as period_value,
            'd'::varchar as period_unit,
            greatest(
              abs(try_cast(nullif(n.pl_orbpererr1, '') as double)),
              abs(try_cast(nullif(n.pl_orbpererr2, '') as double))
            ) as period_error,
            cast(null as varchar) as axis_qualifier,
            greatest(
              abs(try_cast(nullif(n.pl_orbsmaxerr1, '') as double)),
              abs(try_cast(nullif(n.pl_orbsmaxerr2, '') as double))
            ) as axis_error,
            greatest(
              abs(try_cast(nullif(n.pl_orbinclerr1, '') as double)),
              abs(try_cast(nullif(n.pl_orbinclerr2, '') as double))
            ) as inclination_error,
            cast(null as double) as node_error,
            try_cast(nullif(n.pl_orbtper, '') as double) as periastron_epoch,
            nullif(n.pl_orbtper_systemref, '') as epoch_unit,
            greatest(
              abs(try_cast(nullif(n.pl_orbeccenerr1, '') as double)),
              abs(try_cast(nullif(n.pl_orbeccenerr2, '') as double))
            ) as eccentricity_error,
            greatest(
              abs(try_cast(nullif(n.pl_orblpererr1, '') as double)),
              abs(try_cast(nullif(n.pl_orblpererr2, '') as double))
            ) as long_periastron_error,
            cast(null as varchar) as discoverer,
            cast(null as varchar) as grade,
            nullif(n.ttv_flag, '') as notes_flag,
            coalesce(
              nullif(n.pl_orbper_reflink, ''),
              nullif(n.pl_orbsmax_reflink, ''),
              nullif(n.pl_orbeccen_reflink, ''),
              nullif(n.pl_orbincl_reflink, '')
            ) as reference_code,
            cast(null as varchar) as png_file,
            cast(null as double) as last_observed_year
          from core.planets p
          join orbit_edges e
            on e.relation_kind = 'planetary_orbit'
           and e.secondary_component_key = 'comp:planet:' || p.stable_object_key
          left join nasa_planet_orbit_source n
            on p.source_catalog = 'nasa_exoplanet_archive'
           and n.planet_name_norm = p.planet_name_norm
          where coalesce(try_cast(nullif(n.pl_orbper, '') as double), p.orbital_period_days) is not null
             or coalesce(try_cast(nullif(n.pl_orbsmax, '') as double), p.semi_major_axis_au) is not null
             or coalesce(try_cast(nullif(n.pl_orbeccen, '') as double), p.eccentricity) is not null
             or coalesce(try_cast(nullif(n.pl_orbincl, '') as double), p.inclination_deg) is not null
        ), sol_rows as (
          select * from sol_satellite_rows
          union all
          select * from sol_small_body_rows
          union all
          select * from sol_artificial_rows
          union all
          select * from gaia_nss_rows
          union all
          select * from msc_orbit_rows
          union all
          select * from orb6_rows
          union all
          select * from planet_orbit_rows
        )
        select
          row_number() over (
            order by orbit_edge_id, solution_rank nulls first, source_catalog, cast(source_pk as varchar)
          )::bigint as orbital_solution_id,
          orbit_edge_id,
          source_catalog as solution_source_catalog,
          coalesce(solution_rank, 1)::int as solution_rank,
          coalesce(
            reference_epoch_jyear,
            case
              when epoch_tdb_jd is not null then 2000.0 + ((epoch_tdb_jd - 2451545.0) / 365.25)
              else null
            end
          ) as reference_epoch_jyear,
          reference_epoch_mjd as reference_epoch_mjd,
          orbital_period_days as period_days,
          semi_major_axis_au,
          semi_major_axis_arcsec as semi_major_axis_arcsec,
          eccentricity,
          inclination_deg,
          node_deg as longitude_ascending_node_deg,
          long_periastron_deg as argument_periastron_deg,
          time_periastron_jd as time_periastron_jd,
          cast(null as double) as mean_anomaly_deg,
          mass_ratio as mass_ratio_q,
          cast(null as double) as primary_mass_msun,
          cast(null as double) as secondary_mass_msun,
          semi_amplitude_primary_kms as rv_semiamplitude_primary_kms,
          cast(null as double) as rv_semiamplitude_secondary_kms,
          confidence_score,
          json_object(
            'solver', solver,
            'frame', frame,
            'center_of_mass_velocity_kms', center_of_mass_velocity_kms,
            'flags', flags,
            'significance', significance,
            'period_value', period_value,
            'period_unit', period_unit,
            'period_error', period_error,
            'axis_qualifier', axis_qualifier,
            'axis_error', axis_error,
            'inclination_error', inclination_error,
            'node_error', node_error,
            'periastron_epoch', periastron_epoch,
            'epoch_unit', epoch_unit,
            'eccentricity_error', eccentricity_error,
            'long_periastron_error', long_periastron_error,
            'discoverer', discoverer,
            'grade', grade,
            'notes_flag', notes_flag,
            'reference_code', reference_code,
            'png_file', png_file,
            'last_observed_year', last_observed_year
          ) as fit_quality_json,
          case
            when solver = 'nasa_pscomppars' then 'source_native_planet_orbit'
            when solver = 'horizons_elements_planet_inventory' then 'source_native_planet_orbit'
            else 'source_native'
          end::varchar as normalization_method,
          case
            when confidence_score >= 0.95 then 'high'
            when confidence_score >= 0.80 then 'medium'
            when confidence_score >= 0.60 then 'low'
            else 'illustrative'
          end as confidence_tier,
          source_catalog as source_catalog,
          source_version as source_version,
          cast(source_pk as varchar) as source_pk,
          source_row_hash as source_row_hash,
          retrieval_checksum,
          retrieved_at,
          {sql_literal(args.ingested_at)}::varchar as ingested_at,
          {sql_literal(args.transform_version)}::varchar as transform_version
        from sol_rows
        """
    )
    log(f"Arm stage complete: orbital_solutions ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating barycenters")
    con.execute(
        f"""
        create table barycenters as
        with parent_mass as (
          select
            lower(trim(coalesce(object_name, ''))) as object_name_norm,
            nullif(trim(mass_kg), '')::double as mass_kg
          from sol_authority_raw
          where lower(trim(coalesce(object_class, ''))) in ('planet', 'subplanet', 'dwarf_planet')
        ), candidates as (
          select
            e.barycenter_key,
            e.host_component_key,
            s.parent_name_norm,
            s.moon_name_norm,
            s.epoch_tdb_jd,
            pm.mass_kg as parent_mass_kg,
            s.moon_mass_kg,
            s.moon_source_pk,
            s.source_row_hash
          from sol_moon_orbits s
          join orbit_edges e
            on e.relation_kind = 'satellite'
           and e.source_catalog = 'sol_authority'
           and cast(e.source_pk as bigint) = s.moon_source_pk
          left join parent_mass pm on pm.object_name_norm = s.parent_name_norm
          where e.barycenter_key is not null
        )
        select
          row_number() over (order by barycenter_key)::bigint as barycenter_id,
          barycenter_key,
          host_component_key,
          cast(null as double) as x_helio_pc,
          cast(null as double) as y_helio_pc,
          cast(null as double) as z_helio_pc,
          cast(null as double) as vx_helio_kms,
          cast(null as double) as vy_helio_kms,
          cast(null as double) as vz_helio_kms,
          case
            when parent_mass_kg is not null and moon_mass_kg is not null then 'measured'
            else 'catalog_ratio'
          end::varchar as mass_basis,
          'sol_authority_mass_ratio'::varchar as mass_estimation_method,
          json_object(
            'parent', parent_name_norm,
            'moon', moon_name_norm,
            'parent_mass_kg', parent_mass_kg,
            'moon_mass_kg', moon_mass_kg,
            'moon_mass_fraction',
              case
                when parent_mass_kg is not null and moon_mass_kg is not null and (parent_mass_kg + moon_mass_kg) > 0
                then moon_mass_kg / (parent_mass_kg + moon_mass_kg)
                else null
              end
          ) as mass_input_json,
          case
            when epoch_tdb_jd is not null then 2000.0 + ((epoch_tdb_jd - 2451545.0) / 365.25)
            else null
          end as reference_epoch_jyear,
          0.99::double as confidence_score,
          'high'::varchar as confidence_tier,
          'sol_authority'::varchar as source_catalog,
          {sql_literal(sol_authority_version)}::varchar as source_version,
          cast(moon_source_pk as varchar) as source_pk,
          source_row_hash as source_row_hash,
          {sql_literal(sol_authority_checksum)}::varchar as retrieval_checksum,
          {sql_literal(sol_authority_retrieved)}::varchar as retrieved_at,
          {sql_literal(args.ingested_at)}::varchar as ingested_at,
          {sql_literal(args.transform_version)}::varchar as transform_version
        from candidates
        """
    )
    log(f"Arm stage complete: barycenters ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating sol_small_body_objects")
    con.execute(
        f"""
        create table sol_small_body_objects as
        with typed as (
          select
            s.body_source_pk,
            s.body_name,
            s.body_name_norm,
            s.body_kind_norm,
            s.parent_name,
            s.parent_name_norm,
            s.orbital_period_days,
            s.semi_major_axis_au,
            s.eccentricity,
            s.inclination_deg,
            s.epoch_tdb_jd,
            s.body_mass_kg,
            s.body_radius_km,
            s.source_row_hash,
            s.source_url
          from sol_small_body_orbits s
        )
        select
          row_number() over (order by body_source_pk)::bigint as sol_small_body_id,
          'comp:minor_body:sol:' || body_name_norm as stable_component_key,
          body_name,
          body_name_norm,
          coalesce(body_kind_norm, 'unknown') as body_kind,
          'comp:system:system:sol'::varchar as host_component_key,
          'comp:star:star:sol:sun'::varchar as primary_component_key,
          'comp:minor_body:sol:' || body_name_norm as secondary_component_key,
          parent_name,
          parent_name_norm,
          orbital_period_days,
          semi_major_axis_au,
          eccentricity,
          inclination_deg,
          epoch_tdb_jd,
          body_mass_kg,
          body_radius_km,
          case
            when coalesce(body_kind_norm, '') = 'comet' then 45
            when coalesce(body_kind_norm, '') = 'asteroid' then 365
            when coalesce(body_kind_norm, '') = 'tno' then 730
            else 365
          end::int as freshness_window_days,
          greatest(
            datediff(
              'day',
              cast({sql_literal(sol_authority_retrieved)} as timestamp),
              cast({sql_literal(args.ingested_at)} as timestamp)
            ),
            0
          )::int as staleness_days,
          (
            greatest(
              datediff(
                'day',
                cast({sql_literal(sol_authority_retrieved)} as timestamp),
                cast({sql_literal(args.ingested_at)} as timestamp)
              ),
              0
            )::int >
            case
              when coalesce(body_kind_norm, '') = 'comet' then 45
              when coalesce(body_kind_norm, '') = 'asteroid' then 365
              when coalesce(body_kind_norm, '') = 'tno' then 730
              else 365
            end::int
          ) as is_stale,
          0.99::double as confidence_score,
          'high'::varchar as confidence_tier,
          'sol_authority'::varchar as source_catalog,
          {sql_literal(sol_authority_version)}::varchar as source_version,
          cast(body_source_pk as varchar) as source_pk,
          source_row_hash,
          {sql_literal(sol_authority_checksum)}::varchar as retrieval_checksum,
          {sql_literal(sol_authority_retrieved)}::varchar as retrieved_at,
          {sql_literal(args.ingested_at)}::varchar as ingested_at,
          {sql_literal(args.transform_version)}::varchar as transform_version,
          coalesce(source_url, {sql_literal(sol_authority_url)}) as source_url
        from typed
        """
    )
    log(f"Arm stage complete: sol_small_body_objects ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating sol_artificial_objects")
    con.execute(
        f"""
        create table sol_artificial_objects as
        with typed as (
          select
            s.artifact_source_pk,
            s.artifact_name,
            s.artifact_name_norm,
            s.artifact_kind_norm,
            s.parent_name,
            s.parent_name_norm,
            s.center_code,
            s.orbital_period_days,
            s.semi_major_axis_au,
            s.eccentricity,
            s.inclination_deg,
            s.epoch_tdb_jd,
            s.artifact_mass_kg,
            s.artifact_radius_km,
            s.freshness_window_days,
            s.target_body_name,
            s.host_component_key,
            s.primary_component_key,
            s.source_row_hash,
            s.source_url
          from sol_artificial_orbits s
        )
        select
          row_number() over (order by artifact_source_pk)::bigint as sol_artificial_id,
          'comp:artifact:sol:' || artifact_name_norm as stable_component_key,
          artifact_name,
          artifact_name_norm,
          coalesce(artifact_kind_norm, 'artificial') as artifact_kind,
          host_component_key,
          primary_component_key,
          'comp:artifact:sol:' || artifact_name_norm as secondary_component_key,
          parent_name,
          parent_name_norm,
          center_code,
          target_body_name,
          orbital_period_days,
          semi_major_axis_au,
          eccentricity,
          inclination_deg,
          epoch_tdb_jd,
          artifact_mass_kg,
          artifact_radius_km,
          freshness_window_days,
          greatest(
            datediff(
              'day',
              cast({sql_literal(sol_artificial_retrieved)} as timestamp),
              cast({sql_literal(args.ingested_at)} as timestamp)
            ),
            0
          )::int as staleness_days,
          (
            greatest(
              datediff(
                'day',
                cast({sql_literal(sol_artificial_retrieved)} as timestamp),
                cast({sql_literal(args.ingested_at)} as timestamp)
              ),
              0
            )::int > freshness_window_days
          ) as is_stale,
          0.985::double as confidence_score,
          'high'::varchar as confidence_tier,
          'sol_artificial'::varchar as source_catalog,
          {sql_literal(sol_artificial_version)}::varchar as source_version,
          cast(artifact_source_pk as varchar) as source_pk,
          source_row_hash,
          {sql_literal(sol_artificial_checksum)}::varchar as retrieval_checksum,
          {sql_literal(sol_artificial_retrieved)}::varchar as retrieved_at,
          {sql_literal(args.ingested_at)}::varchar as ingested_at,
          {sql_literal(args.transform_version)}::varchar as transform_version,
          coalesce(source_url, {sql_literal(sol_artificial_url)}) as source_url
        from typed
        """
    )
    log(f"Arm stage complete: sol_artificial_objects ({time.monotonic() - stage_started:.1f}s)")

    stage_started = time.monotonic()
    log("Arm stage: creating animation_readiness")
    con.execute(
        f"""
        create table animation_readiness as
        with sol_system as (
          select stable_object_key as system_key
          from core.systems
          where lower(coalesce(system_name_norm, '')) = 'sol'
             or lower(coalesce(stable_object_key, '')) = 'system:sol'
          order by system_id
          limit 1
        ), sol_dynamic as (
          select
            e.orbit_edge_id,
            e.secondary_component_key as component_key,
            s.system_key as stable_object_key,
            o.period_days,
            o.semi_major_axis_au,
            o.eccentricity,
            o.inclination_deg
          from orbit_edges e
          join sol_system s on true
          left join orbital_solutions o on o.orbit_edge_id = e.orbit_edge_id
          where e.relation_kind in ('satellite', 'orbits', 'artificial_orbit')
            and e.source_catalog in ('sol_authority', 'sol_artificial')
        )
        select
          row_number() over (order by orbit_edge_id)::bigint as animation_readiness_id,
          stable_object_key,
          component_key,
          orbit_edge_id,
          case
            when period_days is not null
             and semi_major_axis_au is not null
             and eccentricity is not null
             and inclination_deg is not null then 'full'
            else 'partial'
          end as readiness_level,
          json_object(
            'period_days_missing', period_days is null,
            'semi_major_axis_au_missing', semi_major_axis_au is null,
            'eccentricity_missing', eccentricity is null,
            'inclination_deg_missing', inclination_deg is null
          ) as missing_parameters_json,
          '[]'::varchar as inferred_parameters_json,
          true as disallowed_fabrication,
          json_object(
            'program',
            case
              when component_key like 'comp:artifact:sol:%' then 'sol_artificial_s4'
              else 'sol_authority_s2s3'
            end
          ) as notes_json,
          {sql_literal(args.ingested_at)}::varchar as computed_at,
          {sql_literal(args.transform_version)}::varchar as transform_version
        from sol_dynamic
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
    planet_orbit_edge_count = int(
        con.execute(
            """
            select count(*)
            from orbit_edges
            where relation_kind = 'planetary_orbit'
            """
        ).fetchone()[0]
        or 0
    )
    planet_orbital_solution_count = int(
        con.execute(
            """
            select count(*)
            from orbital_solutions o
            join orbit_edges e on e.orbit_edge_id = o.orbit_edge_id
            where e.relation_kind = 'planetary_orbit'
            """
        ).fetchone()[0]
        or 0
    )
    msc_component_detail_count = int(
        con.execute("select count(*) from msc_component_details").fetchone()[0] or 0
    )
    msc_system_detail_count = int(
        con.execute("select count(*) from msc_system_details").fetchone()[0] or 0
    )
    msc_orbit_detail_count = int(
        con.execute("select count(*) from msc_orbit_details").fetchone()[0] or 0
    )
    wds_component_observation_count = int(
        con.execute("select count(*) from wds_component_observations").fetchone()[0] or 0
    )
    stellar_parameter_count = int(
        con.execute("select count(*) from stellar_parameters").fetchone()[0] or 0
    )
    derived_parameter_count = int(
        con.execute("select count(*) from derived_physical_parameters").fetchone()[0] or 0
    )
    derived_parameter_by_key_rows = con.execute(
        """
        select parameter_key, count(*)::bigint
        from derived_physical_parameters
        group by parameter_key
        order by parameter_key
        """
    ).fetchall()
    derived_parameter_counts_by_key = {
        str(parameter_key): int(count or 0)
        for parameter_key, count in derived_parameter_by_key_rows
    }
    gaia_stellar_parameter_count = int(
        con.execute(
            """
            select count(*)
            from stellar_parameters
            where source_catalog = 'gaia_dr3'
            """
        ).fetchone()[0]
        or 0
    )
    nasa_stellar_parameter_count = int(
        con.execute(
            """
            select count(*)
            from stellar_parameters
            where source_catalog = 'nasa_exoplanet_archive'
            """
        ).fetchone()[0]
        or 0
    )
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
    lifecycle_observation_count = int(
        con.execute("select count(*) from planet_catalog_observations").fetchone()[0] or 0
    )
    lifecycle_status_history_count = int(
        con.execute("select count(*) from planet_status_history").fetchone()[0] or 0
    )
    lifecycle_reclass_count = int(
        con.execute("select count(*) from planet_reclassification_audit").fetchone()[0] or 0
    )
    inferred_leaf_count = int(con.execute("select count(*) from msc_inferred_leaves").fetchone()[0] or 0)
    source_leaf_count = int(con.execute("select count(*) from msc_source_leaf_labels").fetchone()[0] or 0)
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
    sol_moon_component_count = int(
        con.execute(
            """
            select count(*)
            from component_entities
            where source_catalog = 'sol_authority'
              and component_type = 'moon'
            """
        ).fetchone()[0]
        or 0
    )
    sol_moon_hierarchy_edge_count = int(
        con.execute(
            """
            select count(*)
            from system_hierarchy_edges
            where source_catalog = 'sol_authority'
              and member_role = 'satellite'
            """
        ).fetchone()[0]
        or 0
    )
    sol_satellite_orbit_edge_count = int(
        con.execute(
            """
            select count(*)
            from orbit_edges
            where source_catalog = 'sol_authority'
              and relation_kind = 'satellite'
            """
        ).fetchone()[0]
        or 0
    )
    sol_satellite_solution_count = int(
        con.execute(
            """
            select count(*)
            from orbital_solutions o
            join orbit_edges e on e.orbit_edge_id = o.orbit_edge_id
            where o.source_catalog = 'sol_authority'
              and e.relation_kind = 'satellite'
            """
        ).fetchone()[0]
        or 0
    )
    sol_small_body_component_count = int(
        con.execute(
            """
            select count(*)
            from component_entities
            where source_catalog = 'sol_authority'
              and component_type = 'minor_body'
            """
        ).fetchone()[0]
        or 0
    )
    sol_small_body_hierarchy_edge_count = int(
        con.execute(
            """
            select count(*)
            from system_hierarchy_edges
            where source_catalog = 'sol_authority'
              and member_role = 'minor_body'
            """
        ).fetchone()[0]
        or 0
    )
    sol_small_body_orbit_edge_count = int(
        con.execute(
            """
            select count(*)
            from orbit_edges
            where source_catalog = 'sol_authority'
              and relation_kind = 'orbits'
            """
        ).fetchone()[0]
        or 0
    )
    sol_small_body_solution_count = int(
        con.execute(
            """
            select count(*)
            from orbital_solutions o
            join orbit_edges e on e.orbit_edge_id = o.orbit_edge_id
            where o.source_catalog = 'sol_authority'
              and e.relation_kind = 'orbits'
            """
        ).fetchone()[0]
        or 0
    )
    gaia_nss_companion_count = int(
        con.execute(
            """
            select count(*)
            from component_entities
            where source_catalog = 'gaia_nss'
            """
        ).fetchone()[0]
        or 0
    )
    gaia_nss_orbit_edge_count = int(
        con.execute(
            """
            select count(*)
            from orbit_edges
            where source_catalog = 'gaia_nss'
            """
        ).fetchone()[0]
        or 0
    )
    gaia_nss_solution_count = int(
        con.execute(
            """
            select count(*)
            from orbital_solutions
            where source_catalog = 'gaia_nss'
            """
        ).fetchone()[0]
        or 0
    )
    orb6_solution_count = int(
        con.execute(
            """
            select count(*)
            from orbital_solutions
            where source_catalog = 'orb6'
            """
        ).fetchone()[0]
        or 0
    )
    msc_solution_count = int(
        con.execute(
            """
            select count(*)
            from orbital_solutions
            where source_catalog = 'msc'
            """
        ).fetchone()[0]
        or 0
    )
    sol_small_body_table_count = int(
        con.execute("select count(*) from sol_small_body_objects").fetchone()[0] or 0
    )
    sol_small_body_asteroid_count = int(
        con.execute(
            "select count(*) from sol_small_body_objects where body_kind = 'asteroid'"
        ).fetchone()[0]
        or 0
    )
    sol_small_body_tno_count = int(
        con.execute("select count(*) from sol_small_body_objects where body_kind = 'tno'").fetchone()[0]
        or 0
    )
    sol_small_body_comet_count = int(
        con.execute("select count(*) from sol_small_body_objects where body_kind = 'comet'").fetchone()[0]
        or 0
    )
    sol_small_body_stale_count = int(
        con.execute("select count(*) from sol_small_body_objects where is_stale").fetchone()[0] or 0
    )
    sol_barycenter_count = int(
        con.execute(
            """
            select count(*)
            from barycenters
            where source_catalog = 'sol_authority'
            """
        ).fetchone()[0]
        or 0
    )
    sol_animation_readiness_count = int(
        con.execute(
            """
            select count(*)
            from animation_readiness
            where stable_object_key = 'system:sol'
            """
        ).fetchone()[0]
        or 0
    )
    sol_artificial_component_count = int(
        con.execute(
            """
            select count(*)
            from component_entities
            where source_catalog = 'sol_artificial'
              and component_type = 'artificial'
            """
        ).fetchone()[0]
        or 0
    )
    sol_artificial_hierarchy_edge_count = int(
        con.execute(
            """
            select count(*)
            from system_hierarchy_edges
            where source_catalog = 'sol_artificial'
              and member_role = 'artificial'
            """
        ).fetchone()[0]
        or 0
    )
    sol_artificial_orbit_edge_count = int(
        con.execute(
            """
            select count(*)
            from orbit_edges
            where source_catalog = 'sol_artificial'
              and relation_kind = 'artificial_orbit'
            """
        ).fetchone()[0]
        or 0
    )
    sol_artificial_solution_count = int(
        con.execute(
            """
            select count(*)
            from orbital_solutions o
            join orbit_edges e on e.orbit_edge_id = o.orbit_edge_id
            where o.source_catalog = 'sol_artificial'
              and e.relation_kind = 'artificial_orbit'
            """
        ).fetchone()[0]
        or 0
    )
    sol_artificial_table_count = int(
        con.execute("select count(*) from sol_artificial_objects").fetchone()[0] or 0
    )
    sol_artificial_stale_count = int(
        con.execute("select count(*) from sol_artificial_objects where is_stale").fetchone()[0] or 0
    )

    report = {
        "build_id": args.build_id,
        "generated_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "core_db_path": str(core_db),
        "arm_db_path": str(arm_db),
        "msc_csv_path": str(cooked_msc) if cooked_msc.exists() else None,
        "msc_systems_csv_path": str(cooked_msc_systems) if cooked_msc_systems.exists() else None,
        "msc_orbits_csv_path": str(cooked_msc_orbits) if cooked_msc_orbits.exists() else None,
        "counts": {
            "component_entities": component_count,
            "system_hierarchy_edges": hierarchy_count,
            "orbit_edges": orbit_count,
            "planet_orbit_edges": planet_orbit_edge_count,
            "planet_orbital_solutions": planet_orbital_solution_count,
            "msc_component_details": msc_component_detail_count,
            "msc_system_details": msc_system_detail_count,
            "msc_orbit_details": msc_orbit_detail_count,
            "wds_component_observations": wds_component_observation_count,
            "stellar_parameters_rows": stellar_parameter_count,
            "derived_physical_parameters_rows": derived_parameter_count,
            "derived_physical_parameters_by_key": derived_parameter_counts_by_key,
            "stellar_parameters_gaia_rows": gaia_stellar_parameter_count,
            "stellar_parameters_nasa_rows": nasa_stellar_parameter_count,
            "vsx_variability_rows": vsx_variability_count,
            "variability_summary_rows": variability_summary_count,
            "variability_summary_high_variability_rows": vsx_high_variability_count,
            "ultracoolsheet_rows": ultracoolsheet_count,
            "ultracoolsheet_matched_rows": ultracoolsheet_matched_count,
            "planet_catalog_observations_rows": lifecycle_observation_count,
            "planet_status_history_rows": lifecycle_status_history_count,
            "planet_reclassification_audit_rows": lifecycle_reclass_count,
            "msc_inferred_system_roots": inferred_root_count,
            "msc_inferred_leaf_components": inferred_leaf_count,
            "msc_source_leaf_components": source_leaf_count,
            "castor_expected_leaf_matches": castor_leaf_count,
            "castor_expected_pair_matches": castor_pair_count,
            "sol_moon_components": sol_moon_component_count,
            "sol_moon_hierarchy_edges": sol_moon_hierarchy_edge_count,
            "sol_satellite_orbit_edges": sol_satellite_orbit_edge_count,
            "sol_satellite_orbital_solutions": sol_satellite_solution_count,
            "sol_small_body_components": sol_small_body_component_count,
            "sol_small_body_hierarchy_edges": sol_small_body_hierarchy_edge_count,
            "sol_small_body_orbit_edges": sol_small_body_orbit_edge_count,
            "sol_small_body_orbital_solutions": sol_small_body_solution_count,
            "sol_small_body_rows": sol_small_body_table_count,
            "sol_small_body_asteroids": sol_small_body_asteroid_count,
            "sol_small_body_tnos": sol_small_body_tno_count,
            "sol_small_body_comets": sol_small_body_comet_count,
            "sol_small_body_stale_rows": sol_small_body_stale_count,
            "sol_barycenters": sol_barycenter_count,
            "sol_animation_readiness_rows": sol_animation_readiness_count,
            "gaia_nss_companion_components": gaia_nss_companion_count,
            "gaia_nss_orbit_edges": gaia_nss_orbit_edge_count,
            "gaia_nss_orbital_solutions": gaia_nss_solution_count,
            "msc_orbital_solutions": msc_solution_count,
            "orb6_orbital_solutions": orb6_solution_count,
            "sol_artificial_components": sol_artificial_component_count,
            "sol_artificial_hierarchy_edges": sol_artificial_hierarchy_edge_count,
            "sol_artificial_orbit_edges": sol_artificial_orbit_edge_count,
            "sol_artificial_orbital_solutions": sol_artificial_solution_count,
            "sol_artificial_rows": sol_artificial_table_count,
            "sol_artificial_stale_rows": sol_artificial_stale_count,
        },
        "notes": [
            "Arm graph includes core system->star containment edges.",
            "MSC subsystem_count inference still seeds unresolved lettered leaf labels where source rows support them.",
            "MSC sys.tsv rows materialize source-native hierarchy and orbit edges where supported endpoint keys exist.",
            "MSC orb.tsv rows materialize source-native orbital_solutions linked to matching MSC orbit edges.",
            "Arm stellar_parameters materializes source-native Gaia DR3 and NASA host-star values for narration/filtering workflows.",
            "Arm derived_physical_parameters materializes deterministic source-input physical candidates; source-native values supersede these rows.",
            "Gaia NSS rows materialize inferred unresolved companions plus source-native orbital summaries in arm.orbital_solutions.",
            "WDS and MSC catalog detail tables preserve observation history, component photometry, hierarchy, orbital rows, grades, and notes outside core hot paths.",
            "ORB6 rows are folded into arm.orbital_solutions when they can be mapped safely to a unique binary edge for a WDS-linked system.",
            "VSX variability is stored as arm overlay rows keyed by core stable_object_key via Gaia-ID exact joins.",
            "UltracoolSheet rows are stored in arm and linked to core stars when Gaia IDs align.",
            "Exoplanet lifecycle audit tables are mirrored from core into arm for lineage/diff workflows.",
            "Planetary orbit edges and source-native pscomppars/Sol orbital summaries are materialized in arm.orbit_edges and arm.orbital_solutions; core.planets orbit scalars remain promoted hot-path summaries during the migration.",
            "Sol S2 moon hierarchy/orbit rows are sourced from JPL Horizons sol_authority and remain outside core hot paths.",
            "Sol S3 named minor bodies are materialized in arm with deterministic staleness metadata by body family.",
            "Sol S4 artificial stations/probes/orbiters are materialized in arm with parent linkage and staleness metadata.",
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
        f"msc_details={msc_component_detail_count:,}, msc_systems={msc_system_detail_count:,}, "
        f"msc_orbit_rows={msc_orbit_detail_count:,}, wds_obs={wds_component_observation_count:,}, "
        f"stellar_parameters={stellar_parameter_count:,}, derived_parameters={derived_parameter_count:,}, "
        f"planet_orbits={planet_orbital_solution_count:,}, "
        f"gaia_nss_orbits={gaia_nss_solution_count:,}, msc_orbits={msc_solution_count:,}, "
        f"orb6_orbits={orb6_solution_count:,}, "
        f"vsx_rows={vsx_variability_count:,}, ultracoolsheet_rows={ultracoolsheet_count:,}, "
        f"lifecycle_obs={lifecycle_observation_count:,}, lifecycle_reclass={lifecycle_reclass_count:,}, "
        f"msc_source_leaves={source_leaf_count:,}, msc_inferred_leaves={inferred_leaf_count:,}, "
        f"sol_moons={sol_moon_component_count:,}, "
        f"sol_satellite_orbits={sol_satellite_orbit_edge_count:,}, sol_small_bodies={sol_small_body_table_count:,}, "
        f"sol_artificial={sol_artificial_table_count:,}, sol_barycenters={sol_barycenter_count:,})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
