#!/usr/bin/env python3
"""Audit GSP-Phot distance selection before a full E5 rebuild."""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb


def attach(con: duckdb.DuckDBPyConnection, alias: str, path: Path) -> None:
    escaped = str(path.resolve(strict=True)).replace("'", "''")
    con.execute(f"ATTACH '{escaped}' AS {alias} (READ_ONLY)")


def timed_query(
    con: duckdb.DuckDBPyConnection, sql: str
) -> tuple[tuple[object, ...], float]:
    started = time.monotonic()
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError("audit query returned no row")
    return row, round(time.monotonic() - started, 6)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--e4-database", type=Path, required=True)
    parser.add_argument("--e5-database", type=Path, required=True)
    parser.add_argument("--e6-arm", type=Path, required=True)
    parser.add_argument("--stability-arm", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=16)
    args = parser.parse_args()

    con = duckdb.connect(":memory:")
    con.execute(f"SET threads={max(1, args.threads)}")
    attach(con, "e4", args.e4_database)
    attach(con, "e5", args.e5_database)
    attach(con, "e6", args.e6_arm)
    attach(con, "stability", args.stability_arm)

    valid_predicate = """
      quantity_key='gspphot_model_distance'
      AND normalized_value > 0
      AND uncertainty_lower > 0
      AND uncertainty_upper >= uncertainty_lower
      AND method='gaia_dr3_gspphot'
      AND model='Aeneas_MCMC_BP_RP_spectrophotometry'
    """
    phases: dict[str, float] = {}

    valid, phases["valid_source_evidence"] = timed_query(
        con,
        f"""
        SELECT count(*), count(DISTINCT source_record_id), count(DISTINCT evidence_id)
        FROM e4.astrometry_distance_evidence WHERE {valid_predicate}
        """,
    )
    accepted, phases["accepted_bindings"] = timed_query(
        con,
        f"""
        WITH valid AS (
          SELECT DISTINCT source_record_id
          FROM e4.astrometry_distance_evidence WHERE {valid_predicate}
        ), bindings AS (
          SELECT DISTINCT source_record_id, stable_object_key
          FROM e5.evidence_object_bindings
          WHERE source_id='gaia.dr3.astrophysical_parameters'
            AND binding_subject_kind='source_record'
            AND binding_status='accepted'
        )
        SELECT count(*), count(DISTINCT bindings.stable_object_key)
        FROM valid JOIN bindings USING (source_record_id)
        """,
    )
    precedence, phases["distance_precedence"] = timed_query(
        con,
        f"""
        WITH valid AS (
          SELECT DISTINCT source_record_id
          FROM e4.astrometry_distance_evidence WHERE {valid_predicate}
        ), gsp AS (
          SELECT DISTINCT bindings.stable_object_key
          FROM valid
          JOIN e5.evidence_object_bindings bindings USING (source_record_id)
          WHERE bindings.source_id='gaia.dr3.astrophysical_parameters'
            AND bindings.binding_subject_kind='source_record'
            AND bindings.binding_status='accepted'
        ), bj AS (
          SELECT stable_object_key,
                 bool_or(quantity_key='distance_geometric_pc') has_geometric,
                 bool_or(quantity_key='distance_photogeometric_pc') has_photogeometric
          FROM e5.selected_facts
          WHERE quantity_key IN ('distance_geometric_pc', 'distance_photogeometric_pc')
          GROUP BY stable_object_key
        )
        SELECT count(*),
               count(*) FILTER (WHERE coalesce(has_geometric, FALSE)),
               count(*) FILTER (WHERE NOT coalesce(has_geometric, FALSE)
                                  AND coalesce(has_photogeometric, FALSE)),
               count(*) FILTER (WHERE NOT coalesce(has_geometric, FALSE)
                                  AND NOT coalesce(has_photogeometric, FALSE))
        FROM gsp LEFT JOIN bj USING (stable_object_key)
        """,
    )
    recovery, phases["legacy_tail_recovery"] = timed_query(
        con,
        f"""
        WITH valid AS (
          SELECT source_record_id, normalized_value, uncertainty_lower, uncertainty_upper,
                 row_number() OVER (PARTITION BY source_record_id ORDER BY evidence_id) row_num
          FROM e4.astrometry_distance_evidence WHERE {valid_predicate}
        ), bindings AS (
          SELECT DISTINCT source_record_id, stable_object_key
          FROM e5.evidence_object_bindings
          WHERE source_id='gaia.dr3.astrophysical_parameters'
            AND binding_subject_kind='source_record'
            AND binding_status='accepted'
        ), gsp AS (
          SELECT bindings.stable_object_key, valid.normalized_value
          FROM valid JOIN bindings USING (source_record_id) WHERE row_num=1
        ), tail AS (
          SELECT old.stable_object_key, old.distance_pc
          FROM stability.stellar_parameters old
          LEFT JOIN e6.e6_selected_stellar_parameters candidate USING (stable_object_key)
          WHERE old.distance_pc IS NOT NULL AND candidate.distance_pc IS NULL
        )
        SELECT count(*), count(gsp.stable_object_key),
               count(*) - count(gsp.stable_object_key),
               max(abs(tail.distance_pc - gsp.normalized_value))
        FROM tail LEFT JOIN gsp USING (stable_object_key)
        """,
    )
    parallax, phases["legacy_tail_parallax_quality"] = timed_query(
        con,
        """
        WITH tail AS (
          SELECT old.stable_object_key
          FROM stability.stellar_parameters old
          LEFT JOIN e6.e6_selected_stellar_parameters candidate USING (stable_object_key)
          WHERE old.distance_pc IS NOT NULL AND candidate.distance_pc IS NULL
        ), selected_parallax AS (
          SELECT stable_object_key, normalized_value,
                 normalized_value / greatest(
                   normalized_value - value_lower,
                   value_upper - normalized_value
                 ) AS signal_to_noise
          FROM e5.selected_facts WHERE quantity_key='parallax_mas'
        )
        SELECT count(*), count(selected_parallax.stable_object_key),
               min(signal_to_noise), median(signal_to_noise), max(signal_to_noise),
               count(*) FILTER (WHERE signal_to_noise >= 10)
        FROM tail LEFT JOIN selected_parallax USING (stable_object_key)
        """,
    )
    con.close()

    pass_checks = {
        "source_evidence_ids_unique": valid[0] == valid[2],
        "source_records_unique": valid[0] == valid[1],
        "accepted_bindings_unique": accepted[0] == accepted[1],
        "legacy_tail_fully_recovered": recovery[0] == recovery[1] and recovery[2] == 0,
        "legacy_values_match_gspphot": recovery[3] is not None and float(recovery[3]) < 0.001,
        "no_high_snr_inverse_parallax_tail": parallax[5] == 0,
    }
    report = {
        "schema_version": "spacegate.e5_gspphot_distance_preflight.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "pass" if all(pass_checks.values()) else "fail",
        "inputs": {
            "e4_database": str(args.e4_database.resolve()),
            "e5_database": str(args.e5_database.resolve()),
            "e6_arm": str(args.e6_arm.resolve()),
            "stability_arm": str(args.stability_arm.resolve()),
        },
        "valid_gspphot_evidence": {
            "rows": valid[0],
            "source_records": valid[1],
            "evidence_ids": valid[2],
        },
        "accepted_gspphot_bindings": {
            "rows": accepted[0],
            "stable_objects": accepted[1],
        },
        "consumer_precedence": {
            "gspphot_objects": precedence[0],
            "with_geometric_bailer_jones": precedence[1],
            "with_only_photogeometric_bailer_jones": precedence[2],
            "gspphot_fallback_objects": precedence[3],
            "order": [
                "distance_geometric_pc",
                "distance_photogeometric_pc",
                "distance_gspphot_pc",
            ],
        },
        "legacy_tail": {
            "rows": recovery[0],
            "recovered": recovery[1],
            "unrecovered": recovery[2],
            "maximum_absolute_pc_delta": recovery[3],
            "parallax_rows": parallax[1],
            "parallax_signal_to_noise_min": parallax[2],
            "parallax_signal_to_noise_median": parallax[3],
            "parallax_signal_to_noise_max": parallax[4],
            "parallax_signal_to_noise_at_least_10": parallax[5],
        },
        "checks": pass_checks,
        "phase_wall_seconds": phases,
        "scientific_decision": (
            "Select official Gaia GSP-Phot posterior model distance as a distinct "
            "source-model fact after Bailer-Jones posteriors; do not derive inverse-parallax "
            "distances for this low-signal-to-noise tail."
        ),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
