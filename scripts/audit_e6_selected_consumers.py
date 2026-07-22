#!/usr/bin/env python3
"""Audit E6 selected consumers against a stability-reference ARM database."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import resource
import time
from typing import Any

import duckdb


PARAMETER_QUANTITIES = (
    "teff_k",
    "mass_msun",
    "radius_rsun",
    "luminosity_log10_lsun",
    "distance_pc",
)


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _parameter_delta(con: duckdb.DuckDBPyConnection, quantity: str) -> dict[str, int]:
    row = con.execute(
        f"""
        WITH legacy AS (
          SELECT star_id,{quantity} AS value
          FROM reference.stellar_parameters
          QUALIFY row_number() OVER (
            PARTITION BY star_id
            ORDER BY
              CASE parameter_source
                WHEN 'nasa_pscomppars_host' THEN 0
                WHEN 'gaia_dr3_backbone' THEN 1
                ELSE 9
              END,
              (
                CASE WHEN mass_msun IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN radius_rsun IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN luminosity_log10_lsun IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN teff_k IS NOT NULL THEN 1 ELSE 0 END
              ) DESC,
              stellar_parameter_id
          )=1
        ), compared AS (
          SELECT c.star_id,c.{quantity} AS candidate_value,l.value AS reference_value
          FROM e6_selected_stellar_parameters c
          LEFT JOIN legacy l USING (star_id)
        )
        SELECT
          count(*) FILTER (WHERE candidate_value IS NOT NULL),
          count(*) FILTER (WHERE reference_value IS NOT NULL),
          count(*) FILTER (WHERE candidate_value IS NOT NULL AND reference_value IS NULL),
          count(*) FILTER (WHERE candidate_value IS NULL AND reference_value IS NOT NULL),
          count(*) FILTER (
            WHERE candidate_value IS NOT NULL AND reference_value IS NOT NULL
              AND abs(candidate_value-reference_value) >
                greatest(1e-9,abs(reference_value)*1e-9)
          )
        FROM compared
        """
    ).fetchone()
    return {
        "candidate_nonnull": int(row[0]),
        "reference_nonnull": int(row[1]),
        "filled": int(row[2]),
        "lost": int(row[3]),
        "changed": int(row[4]),
    }


def audit(*, candidate_arm: Path, reference_arm: Path, build_id: str) -> dict[str, Any]:
    started = time.perf_counter()
    con = duckdb.connect(str(candidate_arm), read_only=True)
    try:
        con.execute(f"ATTACH {sql_literal(reference_arm.resolve())} AS reference (READ_ONLY)")
        candidate_rows = int(
            con.execute("SELECT count(*) FROM stellar_leaf_display_classifications").fetchone()[0]
        )
        reference_rows = int(
            con.execute(
                "SELECT count(*) FROM reference.stellar_leaf_display_classifications"
            ).fetchone()[0]
        )
        transitions = con.execute(
            """
            SELECT
              count(*) FILTER (WHERE r.classification_value<>c.classification_value),
              count(*) FILTER (
                WHERE r.classification_value='UNKNOWN' AND c.classification_value<>'UNKNOWN'
              ),
              count(*) FILTER (
                WHERE r.classification_value<>'UNKNOWN' AND c.classification_value='UNKNOWN'
              )
            FROM reference.stellar_leaf_display_classifications r
            JOIN stellar_leaf_display_classifications c USING (hierarchy_node_key)
            """
        ).fetchone()
        delta_matrix = [
            {"reference_class": str(old), "candidate_class": str(new), "rows": int(count)}
            for old, new, count in con.execute(
                """
                SELECT r.classification_value,c.classification_value,count(*) AS row_count
                FROM reference.stellar_leaf_display_classifications r
                JOIN stellar_leaf_display_classifications c USING (hierarchy_node_key)
                WHERE r.classification_value<>c.classification_value
                GROUP BY 1,2 ORDER BY row_count DESC,1,2
                """
            ).fetchall()
        ]
        classification_by_basis = {
            str(basis): int(count)
            for basis, count in con.execute(
                "SELECT evidence_basis,count(*) FROM stellar_leaf_display_classifications "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        checks = {
            "leaf_inventory_delta": abs(candidate_rows - reference_rows),
            "duplicate_leaf_keys": int(
                con.execute(
                    "SELECT count(*) FROM (SELECT hierarchy_node_key FROM "
                    "stellar_leaf_display_classifications GROUP BY 1 HAVING count(*)<>1)"
                ).fetchone()[0]
            ),
            "known_to_unknown_regressions": int(transitions[2]),
            "missing_canonical_classification_lineage": int(
                con.execute(
                    "SELECT count(*) FROM e6_selected_stellar_display_classifications "
                    "WHERE classification_value<>'UNKNOWN' AND lineage_id IS NULL"
                ).fetchone()[0]
            ),
            "missing_leaf_selected_fact_lineage": int(
                con.execute(
                    "SELECT count(*) FROM stellar_leaf_display_classifications "
                    "WHERE evidence_basis LIKE 'selected_%' AND selected_fact_id IS NULL"
                ).fetchone()[0]
            ),
        }
        report = {
            "schema_version": "spacegate.e6_selected_consumer_ab.v1",
            "status": "pass" if not any(checks.values()) else "fail",
            "build_id": build_id,
            "candidate_arm": str(candidate_arm.resolve()),
            "reference_arm": str(reference_arm.resolve()),
            "leaf_rows": {"candidate": candidate_rows, "reference": reference_rows},
            "classification_transitions": {
                "changed": int(transitions[0]),
                "unknown_to_known": int(transitions[1]),
                "known_to_unknown": int(transitions[2]),
            },
            "classification_delta_matrix": delta_matrix,
            "classification_by_basis": classification_by_basis,
            "component_evidence_usage": {
                "spectral_type": classification_by_basis.get(
                    "e6_msc_component_spectral_type_v1", 0
                ),
                "mass_prior": classification_by_basis.get(
                    "e6_msc_component_mass_main_sequence_prior_v1", 0
                ),
            },
            "parameter_deltas": {
                quantity: _parameter_delta(con, quantity)
                for quantity in PARAMETER_QUANTITIES
            },
            "checks": checks,
            "wall_seconds": round(time.perf_counter() - started, 6),
            "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        }
        if report["status"] != "pass":
            raise ValueError(f"E6 selected consumer A/B failed: {checks}")
        return report
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-arm", type=Path, required=True)
    parser.add_argument("--reference-arm", type=Path, required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit(
        candidate_arm=args.candidate_arm,
        reference_arm=args.reference_arm,
        build_id=args.build_id,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
