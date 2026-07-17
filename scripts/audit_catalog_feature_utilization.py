#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_BUILD_DIR = Path("/data/spacegate/state/served/current")
DEFAULT_STATE_DIR = Path("/data/spacegate/state")


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def records(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cur = con.execute(sql)
    names = [column[0] for column in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def one(con: duckdb.DuckDBPyConnection, sql: str) -> dict[str, Any]:
    values = records(con, sql)
    return values[0] if values else {}


def csv_counts(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    fields: list[str],
) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "path": str(path)}
    expressions = ["count(*)::bigint as source_rows"]
    expressions.extend(
        f"count(*) filter (where nullif(cast({field} as varchar), '') is not null)::bigint as {field}"
        for field in fields
    )
    result = one(
        con,
        f"select {', '.join(expressions)} "
        f"from read_csv_auto({sql_string(str(path))}, all_varchar=true, delim=',', "
        "header=true, quote='\"', escape='\"', strict_mode=false, null_padding=true)",
    )
    return {
        "exists": True,
        "path": str(path),
        "size_bytes": path.stat().st_size,
        **result,
    }


def audit(build_dir: Path, state_dir: Path) -> dict[str, Any]:
    core_path = (build_dir / "core.duckdb").resolve()
    arm_path = (build_dir / "arm.duckdb").resolve()
    if not core_path.exists() or not arm_path.exists():
        raise FileNotFoundError(f"Expected core.duckdb and arm.duckdb under {build_dir}")

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"attach {sql_string(str(core_path))} as core_db (read_only)")
        con.execute(f"attach {sql_string(str(arm_path))} as arm_db (read_only)")

        core_inventory = one(
            con,
            """
            select
              (select count(*) from core_db.main.systems)::bigint as systems,
              (select count(*) from core_db.main.stars)::bigint as stars,
              (select count(*) from core_db.main.planets)::bigint as planets,
              (select count(*) from core_db.main.eclipsing_binaries)::bigint as eclipsing_binaries,
              (select count(*) from core_db.main.compact_objects)::bigint as compact_objects,
              (select count(*) from core_db.main.open_clusters)::bigint as open_clusters,
              (select count(*) from core_db.main.open_cluster_memberships)::bigint as open_cluster_memberships,
              (select count(*) from core_db.main.superstellar_objects)::bigint as superstellar_objects
            """,
        )

        stellar_parameters = records(
            con,
            """
            select
              parameter_source,
              count(*)::bigint as rows,
              count(distinct star_id)::bigint as stars,
              count(*) filter (where teff_k is not null)::bigint as teff,
              count(*) filter (where logg_cgs is not null)::bigint as logg,
              count(*) filter (where metallicity_feh is not null)::bigint as metallicity,
              count(*) filter (where radius_rsun is not null)::bigint as radius,
              count(*) filter (where mass_msun is not null)::bigint as mass,
              count(*) filter (where luminosity_log10_lsun is not null)::bigint as luminosity,
              count(*) filter (where age_gyr is not null)::bigint as age,
              count(*) filter (where rotation_period_days is not null)::bigint as rotation
            from arm_db.main.stellar_parameters
            group by parameter_source
            order by rows desc
            """,
        )

        hz_eligibility = records(
            con,
            """
            with source_physics as (
              select distinct star_id
              from arm_db.main.stellar_parameters
              where luminosity_log10_lsun is not null
                 or (radius_rsun is not null and coalesce(teff_k, 0) > 0)
            ), classified as (
              select
                s.*,
                upper(coalesce(s.spectral_class, '')) as sc,
                upper(coalesce(s.luminosity_class, '')) as lc,
                lower(coalesce(s.spectral_type_raw, '')) as raw
              from core_db.main.stars s
            ), categorized as (
              select
                star_id,
                case
                  when star_id in (select star_id from source_physics)
                    then 'source_or_radius_teff'
                  when sc in ('O', 'B', 'A', 'F', 'G', 'K', 'M')
                   and lc in ('', 'V')
                   and not regexp_matches(
                     raw,
                     'giant|supergiant|white dwarf|neutron|black hole|pulsar|magnetar|cataclysmic'
                   )
                   and not regexp_matches(
                     upper(coalesce(spectral_type_raw, '')),
                     '(^|[^A-Z])(III|II|IV|IAB|IB|IA|I)([^A-Z]|$)'
                   )
                    then 'main_sequence_prior'
                  when sc in ('L', 'T', 'Y')
                    then 'no_hz_ultracool_or_brown_dwarf'
                  when sc in ('D', 'WD', 'NS', 'BH')
                    or regexp_matches(
                      raw,
                      'white dwarf|neutron|black hole|pulsar|magnetar|cataclysmic'
                    )
                    then 'no_hz_remnant'
                  when lc not in ('', 'V') or regexp_matches(raw, 'giant|supergiant|subgiant')
                    then 'no_hz_evolved_without_physics'
                  else 'no_hz_unclassified_or_unsupported'
                end as category
              from classified
            )
            select category, count(*)::bigint as stars
            from categorized
            group by category
            order by stars desc
            """,
        )

        planet_projection = one(
            con,
            """
            select
              count(*)::bigint as planets,
              count(*) filter (where radius_earth is not null or radius_jup is not null)::bigint as radius,
              count(*) filter (where mass_earth is not null or mass_jup is not null)::bigint as mass,
              count(*) filter (where eq_temp_k is not null)::bigint as equilibrium_temperature,
              count(*) filter (where insol_earth is not null)::bigint as insolation,
              count(*) filter (where semi_major_axis_au is not null)::bigint as semi_major_axis,
              count(*) filter (where orbital_period_days is not null)::bigint as period,
              count(*) filter (where eccentricity is not null)::bigint as eccentricity,
              count(*) filter (where inclination_deg is not null)::bigint as inclination
            from core_db.main.planets
            where coalesce(is_tombstoned, false) = false
            """,
        )

        normalized_orbits = records(
            con,
            """
            select
              solution_source_catalog as source_catalog,
              count(*)::bigint as solutions,
              count(*) filter (where period_days is not null)::bigint as period,
              count(*) filter (
                where semi_major_axis_au is not null or semi_major_axis_arcsec is not null
              )::bigint as semi_major_axis,
              count(*) filter (where eccentricity is not null)::bigint as eccentricity,
              count(*) filter (where inclination_deg is not null)::bigint as inclination
            from arm_db.main.orbital_solutions
            group by solution_source_catalog
            order by solutions desc
            """,
        )

        orb6_path = state_dir / "cooked/orb6/orb6_orbits.csv"
        orb6_binding: dict[str, Any] = {}
        if orb6_path.exists():
            orb6_binding = one(
                con,
                f"""
                with source as (
                  select nullif(wds_id, '') as wds_id
                  from read_csv_auto(
                    {sql_string(str(orb6_path))}, all_varchar=true, delim=',', header=true,
                    quote='"', escape='"', strict_mode=false, null_padding=true
                  )
                  where nullif(wds_id, '') is not null
                ), systems as (
                  select wds_id, system_id, stable_object_key
                  from core_db.main.systems
                  where wds_id is not null
                ), edges as (
                  select systems.wds_id, systems.system_id, count(*)::bigint as edge_count
                  from systems
                  join arm_db.main.orbit_edges e
                    on e.host_component_key = 'comp:system:' || systems.stable_object_key
                   and e.relation_kind = 'binary'
                  group by systems.wds_id, systems.system_id
                )
                select
                  count(*)::bigint as source_rows,
                  count(distinct source.wds_id)::bigint as source_wds_scopes,
                  count(*) filter (where systems.system_id is not null)::bigint as rows_with_core_system,
                  count(*) filter (where edges.edge_count = 1)::bigint as rows_with_unique_binary_edge,
                  count(*) filter (where systems.system_id is null)::bigint as rows_outside_or_unlinked
                from source
                left join systems using (wds_id)
                left join edges using (wds_id, system_id)
                """,
            )

        cooked = state_dir / "cooked"
        source_snapshots = {
            "nasa_pscomppars": csv_counts(
                con,
                cooked / "nasa_exoplanet_archive/pscomppars_clean.csv",
                [
                    "pl_rade",
                    "pl_masse",
                    "pl_bmasse",
                    "pl_bmassprov",
                    "pl_dens",
                    "pl_eqt",
                    "pl_insol",
                    "pl_trandep",
                    "st_lum",
                    "st_rad",
                    "st_mass",
                    "st_age",
                    "st_rotp",
                ],
            ),
            "debcat": csv_counts(
                con,
                cooked / "debcat/debcat_binaries.csv",
                [
                    "mass_primary_msun",
                    "radius_primary_rsun",
                    "teff_primary_k",
                    "lum_primary_lsun",
                    "metallicity_dex",
                ],
            ),
            "tess_eb": csv_counts(
                con,
                cooked / "tess_eb/tess_eb_catalog.csv",
                ["sectors", "tmag", "teff_k", "logg_cgs", "metallicity_dex", "period_days", "morphology", "source", "flags"],
            ),
            "atnf": csv_counts(
                con,
                cooked / "atnf/pulsars.csv",
                ["distance_pc", "type_raw", "assoc_raw", "period_s", "period_derivative", "spin_frequency_hz", "spin_frequency_derivative_hz_s"],
            ),
            "magnetar": csv_counts(
                con,
                cooked / "magnetar/magnetars.csv",
                ["distance_pc", "period_s", "period_dot", "assoc_raw", "activity_raw", "bands_raw"],
            ),
            "open_clusters": csv_counts(
                con,
                cooked / "clusters/open_clusters.csv",
                ["age_log_yr", "av_mag", "distance_modulus_mag", "pm_ra_sigma_mas_yr", "parallax_sigma_mas", "x_gal_pc", "y_gal_pc", "z_gal_pc", "rgc_pc"],
            ),
            "open_cluster_members": csv_counts(
                con,
                cooked / "clusters/open_cluster_members.csv",
                ["gaia_dr2_source_id", "membership_probability"],
            ),
            "snr": csv_counts(
                con,
                cooked / "snr/green_snr.csv",
                ["size_major_arcmin", "morphology_type", "flux_1ghz_jy_raw", "spectral_index_raw", "other_names"],
            ),
            "orb6": csv_counts(
                con,
                orb6_path,
                ["period_value", "semi_major_axis_arcsec", "inclination_deg", "eccentricity", "grade", "reference_code"],
            ),
            "gaia_nss_two_body_orbit": csv_counts(
                con,
                cooked / "gaia_nss/gaia_dr3_nss_two_body_orbit.csv",
                ["period_days", "eccentricity", "center_of_mass_velocity_kms", "semi_amplitude_primary_kms", "mass_ratio", "inclination_deg", "flags", "significance"],
            ),
            "white_dwarf": csv_counts(
                con,
                cooked / "white_dwarf/gaiaedr3_white_dwarf.csv",
                ["pwd", "teff_best_k", "logg_best_cgs", "mass_best_msun", "teff_h_k", "chisq_h", "teff_he_k", "chisq_he"],
            ),
        }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "build_dir": str(build_dir),
            "resolved_core_db": str(core_path),
            "resolved_arm_db": str(arm_path),
            "state_dir": str(state_dir),
            "core_inventory": core_inventory,
            "stellar_parameter_projection": stellar_parameters,
            "simulation_hz_eligibility_approximation": hz_eligibility,
            "planet_projection": planet_projection,
            "normalized_orbits": normalized_orbits,
            "orb6_binding": orb6_binding,
            "cooked_source_non_null_counts": source_snapshots,
            "notes": [
                "Counts describe the selected served build and current cooked snapshots; consult build manifests before treating them as a single immutable lineage.",
                "HZ categories mirror the simulation's conservative luminosity policy approximately; per-scene component evidence can change individual outcomes.",
                "A populated cooked source field is evidence availability, not permission to promote it into CORE.",
            ],
        }
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure how current catalog fields reach Spacegate features and shared evidence tables."
    )
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIR)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()

    report = audit(args.build_dir, args.state_dir)
    payload = json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(payload, encoding="utf-8")
    print(payload, end="")


if __name__ == "__main__":
    main()
