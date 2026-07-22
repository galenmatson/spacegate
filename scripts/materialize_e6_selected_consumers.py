#!/usr/bin/env python3
"""Materialize shared E6 stellar parameter and display-class consumer surfaces."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import resource
import time
from typing import Any

import duckdb

from materialize_stellar_leaf_classifications import spectral_class_sql


PROJECTION_VERSION = "e6_selected_consumer_projection_v1"
VALID_CLASSES = (
    "O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "WR", "WD",
    "NS", "PULSAR", "MAGNETAR", "BLACK HOLE", "UNKNOWN",
)


class PhaseRecorder:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    @contextmanager
    def phase(self, name: str):
        started_wall = time.perf_counter()
        started_usage = resource.getrusage(resource.RUSAGE_SELF)
        try:
            yield
        finally:
            ended_usage = resource.getrusage(resource.RUSAGE_SELF)
            self.rows.append(
                {
                    "phase": name,
                    "wall_seconds": round(time.perf_counter() - started_wall, 6),
                    "cpu_seconds": round(
                        (ended_usage.ru_utime + ended_usage.ru_stime)
                        - (started_usage.ru_utime + started_usage.ru_stime),
                        6,
                    ),
                    "peak_rss_kib": int(ended_usage.ru_maxrss),
                }
            )


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def required_tables(con: duckdb.DuckDBPyConnection) -> None:
    required = {
        "e6_selected_stellar_astrometry",
        "e6_selected_stellar_physics",
        "e6_selected_stellar_photometry",
        "e6_selected_stellar_classification",
    }
    actual = {
        str(row[0])
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main'"
        ).fetchall()
    }
    missing = sorted(required - actual)
    if missing:
        raise ValueError(f"missing E6 selected consumer inputs: {missing}")


def materialize(*, core_db: Path, arm_db: Path, build_id: str) -> dict[str, Any]:
    total_started = time.perf_counter()
    phases = PhaseRecorder()
    con = duckdb.connect(str(arm_db))
    try:
        with phases.phase("attach_and_validate_inputs"):
            con.execute(f"ATTACH {sql_literal(core_db.resolve())} AS core (READ_ONLY)")
            required_tables(con)
            con.execute("DROP VIEW IF EXISTS e6_selected_stellar_parameters")
            con.execute(
                "DROP TABLE IF EXISTS "
                "e6_selected_stellar_parameter_subject_supplement"
            )
            con.execute("DROP TABLE IF EXISTS e6_selected_stellar_display_classifications")

        with phases.phase("stellar_parameter_compatibility_view"):
            con.execute(
                """
                CREATE TABLE e6_selected_stellar_parameter_subject_supplement AS
                SELECT s.star_id,s.system_id,s.stable_object_key
                FROM core.stars s
                WHERE NOT EXISTS (
                  SELECT 1 FROM e6_selected_stellar_astrometry a
                  WHERE a.star_id=s.star_id
                )
                ORDER BY s.star_id
                """
            )
            con.execute(
                f"""
            CREATE VIEW e6_selected_stellar_parameters AS
            WITH subjects AS (
              SELECT star_id,system_id,stable_object_key
              FROM e6_selected_stellar_astrometry
              UNION ALL
              SELECT star_id,system_id,stable_object_key
              FROM e6_selected_stellar_parameter_subject_supplement
            )
            SELECT
              s.star_id::BIGINT AS stellar_parameter_id,
              s.star_id,s.system_id,s.stable_object_key,
              'evidence_lake_v2_selected_facts'::VARCHAR AS parameter_source,
              p.teff_k,p.teff_k_lower AS teff_lo_k,p.teff_k_upper AS teff_hi_k,
              p.logg_cgs,p.logg_cgs_lower AS logg_lo_cgs,p.logg_cgs_upper AS logg_hi_cgs,
              p.metallicity_m_h AS metallicity_feh,
              p.metallicity_m_h_lower AS metallicity_lo_feh,
              p.metallicity_m_h_upper AS metallicity_hi_feh,
              coalesce(p.distance_geometric_pc,p.distance_photogeometric_pc) AS distance_pc,
              coalesce(p.distance_geometric_pc_lower,p.distance_photogeometric_pc_lower) AS distance_lo_pc,
              coalesce(p.distance_geometric_pc_upper,p.distance_photogeometric_pc_upper) AS distance_hi_pc,
              p.radius_rsun,NULL::DOUBLE AS radius_err_plus_rsun,
              NULL::DOUBLE AS radius_err_minus_rsun,
              p.mass_msun,NULL::DOUBLE AS mass_err_plus_msun,
              NULL::DOUBLE AS mass_err_minus_msun,
              CASE WHEN p.luminosity_lsun>0 THEN log10(p.luminosity_lsun) END AS luminosity_log10_lsun,
              NULL::DOUBLE AS luminosity_err_plus_log10_lsun,
              NULL::DOUBLE AS luminosity_err_minus_log10_lsun,
              NULL::DOUBLE AS density_g_cm3,NULL::DOUBLE AS density_err_plus_g_cm3,
              NULL::DOUBLE AS density_err_minus_g_cm3,
              p.age_gyr,NULL::DOUBLE AS age_err_plus_gyr,NULL::DOUBLE AS age_err_minus_gyr,
              p.rotation_period_days,a.radial_velocity_km_s AS radial_velocity_kms,
              NULL::DOUBLE AS radial_velocity_error_kms,
              phot.gaia_g_mag AS phot_g_mag,phot.gaia_bp_mag AS phot_bp_mag,
              phot.gaia_rp_mag AS phot_rp_mag,phot.gaia_bp_rp_mag AS bp_rp,
              NULL::DOUBLE AS bp_g,NULL::DOUBLE AS g_rp,
              a.ra_error_mas,a.dec_error_mas,NULL::DOUBLE AS pm_ra_error_mas_yr,
              NULL::DOUBLE AS pm_dec_error_mas_yr,NULL::BIGINT AS visibility_periods_used,
              NULL::BIGINT AS astrometric_params_solved,
              CASE WHEN try_cast(a.gaia_non_single_star_status AS INTEGER)>0 THEN TRUE
                   WHEN a.gaia_non_single_star_status IS NOT NULL THEN FALSE END AS non_single_star,
              NULL::BOOLEAN AS duplicated_source,NULL::BOOLEAN AS has_xp_continuous,
              NULL::BOOLEAN AS has_xp_sampled,NULL::BOOLEAN AS has_rvs,
              coalesce(c.spectral_type_optical,c.spectral_type_infrared,c.spectral_type_simbad)
                AS spectral_type_raw,
              NULL::DOUBLE AS classprob_star,NULL::DOUBLE AS classprob_binarystar,
              NULL::DOUBLE AS classprob_galaxy,NULL::DOUBLE AS classprob_quasar,
              NULL::DOUBLE AS classprob_whitedwarf_combmod,
              NULL::DOUBLE AS classprob_whitedwarf_specmod,
              json_object(
                'projection_version',{sql_literal(PROJECTION_VERSION)},
                'teff_fact_id',p.teff_k_fact_id,'mass_fact_id',p.mass_msun_fact_id,
                'radius_fact_id',p.radius_rsun_fact_id,
                'luminosity_fact_id',p.luminosity_lsun_fact_id,
                'distance_geometric_fact_id',p.distance_geometric_pc_fact_id,
                'distance_photogeometric_fact_id',p.distance_photogeometric_pc_fact_id,
                'spectral_type_optical_fact_id',c.spectral_type_optical_fact_id,
                'spectral_type_infrared_fact_id',c.spectral_type_infrared_fact_id,
                'spectral_type_simbad_fact_id',c.spectral_type_simbad_fact_id
              ) AS context_json,
              'evidence_lake_v2'::VARCHAR AS source_catalog,
              {sql_literal(build_id)}::VARCHAR AS source_version,
              NULL::VARCHAR AS source_url,s.stable_object_key::VARCHAR AS source_pk,
              sha256(concat_ws('|',s.stable_object_key,{sql_literal(PROJECTION_VERSION)},
                coalesce(p.teff_k_fact_id,''),coalesce(p.mass_msun_fact_id,''),
                coalesce(p.radius_rsun_fact_id,''),coalesce(p.luminosity_lsun_fact_id,'')))
                AS source_row_hash,
              NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
              NULL::VARCHAR AS ingested_at,{sql_literal(PROJECTION_VERSION)}::VARCHAR AS transform_version
            FROM subjects s
            LEFT JOIN e6_selected_stellar_astrometry a USING (star_id)
            LEFT JOIN e6_selected_stellar_physics p USING (star_id)
            LEFT JOIN e6_selected_stellar_photometry phot USING (star_id)
            LEFT JOIN e6_selected_stellar_classification c USING (star_id)
                """
            )

        optical_class = spectral_class_sql("c.spectral_type_optical", "NULL", "'star'")
        infrared_class = spectral_class_sql("c.spectral_type_infrared", "NULL", "'star'")
        simbad_class = spectral_class_sql("c.spectral_type_simbad", "NULL", "'star'")
        core_class = spectral_class_sql("s.spectral_type_raw", "s.spectral_class", "s.object_type")
        with phases.phase("stellar_display_classification_projection"):
            con.execute(
                f"""
            CREATE TABLE e6_selected_stellar_display_classifications AS
            WITH candidates AS (
              SELECT s.star_id,s.system_id,s.stable_object_key,0 evidence_rank,
                CASE lower(coalesce(s.object_type,''))
                  WHEN 'black_hole' THEN 'BLACK HOLE' WHEN 'magnetar' THEN 'MAGNETAR'
                  WHEN 'pulsar' THEN 'PULSAR' WHEN 'neutron_star' THEN 'NS'
                  WHEN 'white_dwarf' THEN 'WD' END::VARCHAR classification_value,
                'source'::VARCHAR classification_status,
                'canonical_object_type'::VARCHAR evidence_basis,
                NULL::VARCHAR selected_fact_id,s.object_type::VARCHAR source_value,
                1.0::DOUBLE confidence_score
              FROM core.stars s
              WHERE lower(coalesce(s.object_type,'')) IN
                ('black_hole','magnetar','pulsar','neutron_star','white_dwarf')
              UNION ALL
              SELECT s.star_id,s.system_id,s.stable_object_key,10,
                {optical_class}::VARCHAR,'source','selected_spectral_type_optical',
                c.spectral_type_optical_fact_id,c.spectral_type_optical,0.98
              FROM core.stars s JOIN e6_selected_stellar_classification c USING (star_id)
              WHERE {optical_class} IS NOT NULL
              UNION ALL
              SELECT s.star_id,s.system_id,s.stable_object_key,11,
                {infrared_class}::VARCHAR,'source','selected_spectral_type_infrared',
                c.spectral_type_infrared_fact_id,c.spectral_type_infrared,0.96
              FROM core.stars s JOIN e6_selected_stellar_classification c USING (star_id)
              WHERE {infrared_class} IS NOT NULL
              UNION ALL
              SELECT s.star_id,s.system_id,s.stable_object_key,12,
                {simbad_class}::VARCHAR,'source','selected_spectral_type_simbad',
                c.spectral_type_simbad_fact_id,c.spectral_type_simbad,0.94
              FROM core.stars s JOIN e6_selected_stellar_classification c USING (star_id)
              WHERE {simbad_class} IS NOT NULL
              UNION ALL
              SELECT s.star_id,s.system_id,s.stable_object_key,20,
                {core_class}::VARCHAR,'source','stability_core_source_class_fallback',
                NULL::VARCHAR,coalesce(s.spectral_type_raw,s.spectral_class),0.75
              FROM core.stars s
              WHERE {core_class} IS NOT NULL AND coalesce(s.source_catalog,'')<>'gaia_dr3'
              UNION ALL
              SELECT s.star_id,s.system_id,s.stable_object_key,30,
                CASE WHEN p.teff_k>=30000 THEN 'O' WHEN p.teff_k>=10000 THEN 'B'
                     WHEN p.teff_k>=7500 THEN 'A' WHEN p.teff_k>=6000 THEN 'F'
                     WHEN p.teff_k>=5200 THEN 'G' WHEN p.teff_k>=3700 THEN 'K'
                     WHEN p.teff_k>=2400 THEN 'M' WHEN p.teff_k IS NOT NULL THEN 'L' END,
                'derived','selected_teff_visual_class_prior',p.teff_k_fact_id,
                cast(p.teff_k AS VARCHAR),0.62
              FROM core.stars s JOIN e6_selected_stellar_physics p USING (star_id)
              WHERE p.teff_k IS NOT NULL
              UNION ALL
              SELECT s.star_id,s.system_id,s.stable_object_key,35,
                CASE WHEN phot.gaia_bp_rp_mag < -0.20 THEN 'O'
                     WHEN phot.gaia_bp_rp_mag < 0.00 THEN 'B'
                     WHEN phot.gaia_bp_rp_mag < 0.30 THEN 'A'
                     WHEN phot.gaia_bp_rp_mag < 0.58 THEN 'F'
                     WHEN phot.gaia_bp_rp_mag < 0.81 THEN 'G'
                     WHEN phot.gaia_bp_rp_mag < 1.40 THEN 'K'
                     WHEN phot.gaia_bp_rp_mag < 2.40 THEN 'M' ELSE 'L' END,
                'assumed','selected_bp_rp_visual_class_prior',
                phot.gaia_bp_rp_mag_fact_id,cast(phot.gaia_bp_rp_mag AS VARCHAR),0.40
              FROM core.stars s JOIN e6_selected_stellar_photometry phot USING (star_id)
              WHERE phot.gaia_bp_rp_mag IS NOT NULL
              UNION ALL
              SELECT s.star_id,s.system_id,s.stable_object_key,40,
                CASE WHEN p.mass_msun<0.08 THEN 'L' WHEN p.mass_msun<0.65 THEN 'M'
                     WHEN p.mass_msun<0.85 THEN 'K' WHEN p.mass_msun<1.04 THEN 'G'
                     WHEN p.mass_msun<1.40 THEN 'F' WHEN p.mass_msun<2.10 THEN 'A'
                     WHEN p.mass_msun<16.0 THEN 'B' ELSE 'O' END,
                'assumed','selected_mass_main_sequence_prior',p.mass_msun_fact_id,
                cast(p.mass_msun AS VARCHAR),0.35
              FROM core.stars s JOIN e6_selected_stellar_physics p USING (star_id)
              WHERE p.mass_msun>0
            ), valid AS (
              SELECT * FROM candidates WHERE classification_value IN {VALID_CLASSES[:-1]}
            ), ranked AS (
              SELECT *,row_number() OVER (
                PARTITION BY star_id ORDER BY evidence_rank,confidence_score DESC,
                  selected_fact_id NULLS LAST,classification_value
              ) choice_rank
              FROM valid
            ), conflicts AS (
              SELECT star_id,count(DISTINCT classification_value)::INTEGER distinct_candidate_class_count,
                to_json(list(DISTINCT classification_value ORDER BY classification_value))::VARCHAR
                  candidate_classes_json
              FROM valid GROUP BY star_id
            ), direct_conflicts AS (
              SELECT star_id,
                count(DISTINCT classification_value)::INTEGER distinct_direct_class_count,
                to_json(list(DISTINCT classification_value ORDER BY classification_value))::VARCHAR
                  direct_classes_json
              FROM valid WHERE evidence_rank<=20 GROUP BY star_id
            )
            SELECT
              row_number() OVER (ORDER BY s.star_id)::BIGINT AS selected_display_classification_id,
              {sql_literal(build_id)}::VARCHAR AS build_id,s.star_id,s.system_id,s.stable_object_key,
              coalesce(r.classification_value,'UNKNOWN')::VARCHAR classification_value,
              coalesce(r.classification_status,'missing')::VARCHAR classification_status,
              coalesce(r.evidence_basis,'no_selected_or_fallback_classification')::VARCHAR evidence_basis,
              r.selected_fact_id,r.source_value,coalesce(r.confidence_score,0.0)::DOUBLE confidence_score,
              CASE
                WHEN r.selected_fact_id IS NOT NULL THEN 'selected_fact'
                WHEN r.evidence_basis IN ('canonical_object_type','stability_core_source_class_fallback')
                  THEN 'canonical_core_star'
                ELSE 'missing'
              END::VARCHAR AS lineage_kind,
              coalesce(r.selected_fact_id,
                CASE WHEN r.evidence_basis IN
                  ('canonical_object_type','stability_core_source_class_fallback')
                  THEN 'core:star:' || s.star_id::VARCHAR END
              )::VARCHAR AS lineage_id,
              coalesce(cf.distinct_candidate_class_count,0)::INTEGER distinct_candidate_class_count,
              coalesce(cf.candidate_classes_json,'[]')::VARCHAR candidate_classes_json,
              coalesce(dc.distinct_direct_class_count,0)::INTEGER distinct_direct_class_count,
              coalesce(dc.direct_classes_json,'[]')::VARCHAR direct_classes_json,
              (coalesce(dc.distinct_direct_class_count,0)>1)::BOOLEAN has_classification_conflict,
              (coalesce(cf.distinct_candidate_class_count,0)>1)::BOOLEAN has_alternative_disagreement,
              {sql_literal(PROJECTION_VERSION)}::VARCHAR projection_version
            FROM core.stars s
            LEFT JOIN ranked r ON r.star_id=s.star_id AND r.choice_rank=1
            LEFT JOIN conflicts cf ON cf.star_id=s.star_id
            LEFT JOIN direct_conflicts dc ON dc.star_id=s.star_id
            ORDER BY s.star_id
                """
            )
        with phases.phase("stellar_display_classification_indexes"):
            con.execute(
                "CREATE UNIQUE INDEX e6_selected_stellar_display_star_uq "
                "ON e6_selected_stellar_display_classifications(star_id)"
            )
            con.execute(
                "CREATE INDEX e6_selected_stellar_display_system_idx "
                "ON e6_selected_stellar_display_classifications(system_id)"
            )

        with phases.phase("selected_consumer_verification"):
            star_count = int(con.execute("SELECT count(*) FROM core.stars").fetchone()[0])
            parameter_count = int(
                con.execute("SELECT count(*) FROM e6_selected_stellar_parameters").fetchone()[0]
            )
            class_count = int(
                con.execute(
                    "SELECT count(*) FROM e6_selected_stellar_display_classifications"
                ).fetchone()[0]
            )
            duplicate_classes = int(
                con.execute(
                    "SELECT count(*) FROM (SELECT star_id FROM "
                    "e6_selected_stellar_display_classifications GROUP BY 1 HAVING count(*)<>1)"
                ).fetchone()[0]
            )
            invalid_classes = int(
                con.execute(
                    f"SELECT count(*) FROM e6_selected_stellar_display_classifications "
                    f"WHERE classification_value NOT IN {VALID_CLASSES}"
                ).fetchone()[0]
            )
            missing_selected_lineage = int(
                con.execute(
                    "SELECT count(*) FROM e6_selected_stellar_display_classifications "
                    "WHERE evidence_basis LIKE 'selected_%' AND selected_fact_id IS NULL"
                ).fetchone()[0]
            )
            missing_nonmissing_lineage = int(
                con.execute(
                    "SELECT count(*) FROM e6_selected_stellar_display_classifications "
                    "WHERE classification_value<>'UNKNOWN' AND lineage_id IS NULL"
                ).fetchone()[0]
            )
            by_basis = {
                str(basis): int(count)
                for basis, count in con.execute(
                    "SELECT evidence_basis,count(*) FROM "
                    "e6_selected_stellar_display_classifications GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            direct_conflict_count = int(
                con.execute(
                    "SELECT count(*) FROM e6_selected_stellar_display_classifications "
                    "WHERE has_classification_conflict"
                ).fetchone()[0]
            )
            alternative_disagreement_count = int(
                con.execute(
                    "SELECT count(*) FROM e6_selected_stellar_display_classifications "
                    "WHERE has_alternative_disagreement"
                ).fetchone()[0]
            )
        failures = {
            "parameter_inventory_delta": abs(parameter_count - star_count),
            "classification_inventory_delta": abs(class_count - star_count),
            "duplicate_classifications": duplicate_classes,
            "invalid_classifications": invalid_classes,
            "missing_selected_fact_lineage": missing_selected_lineage,
            "missing_nonmissing_lineage": missing_nonmissing_lineage,
        }
        report = {
            "schema_version": "spacegate.e6_selected_consumer_report.v1",
            "status": "pass" if not any(failures.values()) else "fail",
            "build_id": build_id,
            "projection_version": PROJECTION_VERSION,
            "stellar_parameter_rows": parameter_count,
            "stellar_classification_rows": class_count,
            "classification_by_basis": by_basis,
            "classification_conflicts": direct_conflict_count,
            "classification_alternative_disagreements": alternative_disagreement_count,
            "checks": failures,
        }
        with phases.phase("checkpoint"):
            con.execute("CHECKPOINT")
        report["phase_timings"] = phases.rows
        report["total_wall_seconds"] = round(time.perf_counter() - total_started, 6)
        report["peak_rss_kib"] = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        if report["status"] != "pass":
            raise ValueError(f"E6 selected consumer projection failed: {failures}")
        return report
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--core-db", type=Path, required=True)
    parser.add_argument("--arm-db", type=Path, required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = materialize(core_db=args.core_db, arm_db=args.arm_db, build_id=args.build_id)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
