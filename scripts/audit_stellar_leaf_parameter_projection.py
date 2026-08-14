#!/usr/bin/env python3
"""Audit exact accounting for legacy MSC mass-derived leaf classifications."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


LEGACY_MSC_MASS_BASIS = "selected_msc_component_mass_main_sequence_prior"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def _grouped(con: duckdb.DuckDBPyConnection, sql: str) -> dict[str, int]:
    return {str(key): int(count) for key, count in con.execute(sql).fetchall()}


def audit(baseline_arm: Path, candidate_arm: Path, expected: int) -> dict[str, Any]:
    con = duckdb.connect(str(candidate_arm), read_only=True)
    try:
        con.execute(f"ATTACH '{_sql_path(baseline_arm)}' AS baseline (READ_ONLY)")
        con.execute(
            f"""
            CREATE TEMP VIEW legacy_msc_mass_leaves AS
            SELECT hierarchy_node_key
            FROM baseline.stellar_leaf_display_classifications
            WHERE evidence_basis='{LEGACY_MSC_MASS_BASIS}'
            """
        )
        total = _scalar(con, "SELECT count(*) FROM legacy_msc_mass_leaves")
        distinct_total = _scalar(
            con, "SELECT count(DISTINCT hierarchy_node_key) FROM legacy_msc_mass_leaves"
        )
        evidence_accounted = _scalar(
            con,
            """
            SELECT count(*) FROM legacy_msc_mass_leaves legacy
            WHERE EXISTS (
              SELECT 1 FROM stellar_leaf_parameter_evidence evidence
              WHERE evidence.hierarchy_node_key=legacy.hierarchy_node_key
                AND evidence.source_id='multiplicity.msc'
                AND evidence.quantity_key='mass_msun'
            )
            """,
        )
        binding_accounted = _scalar(
            con,
            """
            SELECT count(*) FROM legacy_msc_mass_leaves legacy
            WHERE EXISTS (
                SELECT 1 FROM stellar_leaf_parameter_binding_outcomes binding
              WHERE binding.hierarchy_node_key=legacy.hierarchy_node_key
                AND binding.source_id='multiplicity.msc'
                AND binding.quantity_key='mass'
            )
            """,
        )
        selections = _grouped(
            con,
            """
            SELECT coalesce(selected.selection_status,'absent'),count(*)
            FROM legacy_msc_mass_leaves legacy
            LEFT JOIN stellar_leaf_selected_parameters selected
              ON selected.hierarchy_node_key=legacy.hierarchy_node_key
             AND selected.quantity_key='mass_msun'
            GROUP BY 1 ORDER BY 1
            """,
        )
        display_outcomes = _grouped(
            con,
            """
            SELECT concat(display.classification_status,'|',display.evidence_basis),count(*)
            FROM legacy_msc_mass_leaves legacy
            JOIN stellar_leaf_display_classifications display
              USING(hierarchy_node_key)
            GROUP BY 1 ORDER BY 1
            """,
        )
        evidence_disposition = _grouped(
            con,
            """
            SELECT concat(
              coalesce(evidence.msc_mass_code,'missing'),'|',
              evidence.mass_method_class,'|',evidence.applicability_decision
            ),count(*)
            FROM legacy_msc_mass_leaves legacy
            JOIN stellar_leaf_parameter_evidence evidence
              ON evidence.hierarchy_node_key=legacy.hierarchy_node_key
             AND evidence.source_id='multiplicity.msc'
             AND evidence.quantity_key='mass_msun'
            GROUP BY 1 ORDER BY 1
            """,
        )
        missing_examples = [
            {"hierarchy_node_key": row[0], "selection_status": row[1]}
            for row in con.execute(
                """
                SELECT legacy.hierarchy_node_key,
                  coalesce(selected.selection_status,'absent')
                FROM legacy_msc_mass_leaves legacy
                LEFT JOIN stellar_leaf_selected_parameters selected
                  ON selected.hierarchy_node_key=legacy.hierarchy_node_key
                 AND selected.quantity_key='mass_msun'
                WHERE coalesce(selected.selection_status,'absent')!='accepted'
                ORDER BY 1 LIMIT 25
                """
            ).fetchall()
        ]
        checks = {
            "expected_inventory": total == expected,
            "unique_inventory": distinct_total == total,
            "all_bindings_accounted": binding_accounted == total,
            "all_evidence_accounted": evidence_accounted == total,
            "all_display_outcomes_accounted": sum(display_outcomes.values()) == total,
            "all_selection_outcomes_accounted": sum(selections.values()) == total,
        }
        return {
            "schema_version": "spacegate.stellar_leaf_parameter_projection_audit.v1",
            "generated_at_utc": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "baseline_arm": {
                "path": str(baseline_arm),
                "sha256": _sha256(baseline_arm),
            },
            "candidate_arm": {
                "path": str(candidate_arm),
                "sha256": _sha256(candidate_arm),
            },
            "legacy_basis": LEGACY_MSC_MASS_BASIS,
            "expected_inventory": expected,
            "legacy_leaf_count": total,
            "legacy_distinct_leaf_count": distinct_total,
            "binding_accounted_leaf_count": binding_accounted,
            "evidence_accounted_leaf_count": evidence_accounted,
            "selection_outcomes": selections,
            "display_outcomes": display_outcomes,
            "evidence_disposition": evidence_disposition,
            "nonaccepted_examples": missing_examples,
            "checks": checks,
            "status": "pass" if all(checks.values()) else "fail",
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-arm", required=True, type=Path)
    parser.add_argument("--candidate-arm", required=True, type=Path)
    parser.add_argument("--expected", type=int, default=8429)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = audit(
        args.baseline_arm.resolve(), args.candidate_arm.resolve(), args.expected
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "legacy_leaf_count": report["legacy_leaf_count"],
                "selection_outcomes": report["selection_outcomes"],
                "display_outcomes": report["display_outcomes"],
            },
            indent=2,
        )
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
