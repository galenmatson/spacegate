#!/usr/bin/env python3
"""Audit E7 runtime stellar classifications against the served stability build."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import resource
import time
from typing import Any

import duckdb


MSC_LEGACY_BASES = (
    "mass_main_sequence_prior_v1",
    "msc_exact_leaf_spectral_type_v1",
    "source_component_spectral_type_v1",
)
MSC_LEGACY_CATALOGS = ("msc", "sb9", "debcat")


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def rows_as_dicts(
    con: duckdb.DuckDBPyConnection, query: str, parameters: list[Any] | None = None
) -> list[dict[str, Any]]:
    cursor = con.execute(query, parameters or [])
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def audit(*, candidate_arm: Path, reference_arm: Path, build_id: str) -> dict[str, Any]:
    started = time.perf_counter()
    con = duckdb.connect(str(candidate_arm), read_only=True)
    try:
        con.execute(
            f"ATTACH {sql_literal(reference_arm.resolve())} AS reference (READ_ONLY)"
        )
        con.execute(
            """
            CREATE TEMP VIEW classification_ab AS
            SELECT
              coalesce(c.hierarchy_node_key,r.hierarchy_node_key) AS hierarchy_node_key,
              r.classification_value AS reference_class,
              r.classification_status AS reference_status,
              r.evidence_basis AS reference_basis,
              r.source_catalog AS reference_catalog,
              r.source_version AS reference_version,
              r.source_pk AS reference_source_pk,
              r.display_name AS reference_display_name,
              c.classification_value AS candidate_class,
              c.classification_status AS candidate_status,
              c.evidence_basis AS candidate_basis,
              c.source_catalog AS candidate_catalog,
              c.source_version AS candidate_version,
              c.source_pk AS candidate_source_pk,
              c.hierarchy_source_basis AS candidate_hierarchy_basis,
              c.node_kind AS candidate_node_kind
            FROM reference.stellar_leaf_display_classifications r
            FULL OUTER JOIN stellar_leaf_display_classifications c USING(hierarchy_node_key)
            """
        )
        con.execute(
            f"""
            CREATE TEMP VIEW known_to_unknown_accounting AS
            WITH regressions AS (
              SELECT *,split_part(hierarchy_node_key,':',4) AS wds_id,
                     split_part(hierarchy_node_key,':',5) AS legacy_label
              FROM classification_ab
              WHERE reference_class<>'UNKNOWN' AND candidate_class='UNKNOWN'
            ), collision_support AS (
              SELECT r.hierarchy_node_key,
                     count(*) AS binding_rows,
                     count(DISTINCT b.component_label_raw) AS case_distinct_labels,
                     count(*) FILTER (
                       WHERE b.runtime_binding_status='ambiguous'
                         AND b.runtime_binding_reason='case_significant_source_collision'
                     ) AS supported_rows
              FROM regressions r
              JOIN msc_runtime_leaf_bindings b
                ON b.wds_id_raw=r.wds_id
               AND lower(b.component_label_raw)=r.legacy_label
              GROUP BY 1
            )
            SELECT r.*,
              CASE
                WHEN r.reference_basis IN {MSC_LEGACY_BASES}
                 AND r.reference_catalog IN {MSC_LEGACY_CATALOGS}
                 AND s.binding_rows>=2
                 AND s.case_distinct_labels>=2
                 AND s.supported_rows=s.binding_rows
                  THEN 'deferred_case_significant_component_scope_collision'
                WHEN r.reference_basis='core_leaf_source_class_v1'
                 AND r.reference_catalog='ultracoolsheet'
                 AND r.reference_class IN ('M','L','T','Y')
                  THEN 'deferred_unmigrated_ultracool_classification'
                ELSE 'unaccounted'
              END AS accounting_status,
              coalesce(s.binding_rows,0) AS supporting_binding_rows,
              coalesce(s.case_distinct_labels,0) AS supporting_case_distinct_labels
            FROM regressions r
            LEFT JOIN collision_support s USING(hierarchy_node_key)
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW candidate_only_accounting AS
            SELECT *,
              CASE
                WHEN candidate_hierarchy_basis='msc_inferred_leaf'
                 AND candidate_node_kind='inferred_star_leaf'
                 AND candidate_class='L'
                 AND candidate_status='assumed'
                 AND candidate_basis='selected_msc_component_mass_main_sequence_prior'
                 AND candidate_catalog='multiplicity.msc'
                  THEN 'accepted_clean_msc_inferred_mass_prior_leaf'
                WHEN candidate_hierarchy_basis='msc_inferred_leaf'
                 AND candidate_node_kind='inferred_star_leaf'
                 AND candidate_class='UNKNOWN'
                 AND candidate_status='missing'
                 AND candidate_basis='no_selected_leaf_classification'
                  THEN 'accepted_clean_msc_inferred_unclassified_leaf'
                ELSE 'unaccounted'
              END AS accounting_status
            FROM classification_ab
            WHERE reference_class IS NULL
            """
        )

        scalar = lambda query: int(con.execute(query).fetchone()[0])
        transition = con.execute(
            """
            SELECT
              count(*) FILTER (
                WHERE reference_class IS NOT NULL AND candidate_class IS NOT NULL
                  AND reference_class<>candidate_class
              ),
              count(*) FILTER (
                WHERE reference_class='UNKNOWN' AND candidate_class<>'UNKNOWN'
              ),
              count(*) FILTER (
                WHERE reference_class<>'UNKNOWN' AND candidate_class='UNKNOWN'
              )
            FROM classification_ab
            """
        ).fetchone()
        checks = {
            "duplicate_candidate_leaf_keys": scalar(
                "SELECT count(*) FROM (SELECT hierarchy_node_key FROM "
                "stellar_leaf_display_classifications GROUP BY 1 HAVING count(*)<>1)"
            ),
            "reference_only_leaves": scalar(
                "SELECT count(*) FROM classification_ab WHERE candidate_class IS NULL"
            ),
            "unaccounted_candidate_only_leaves": scalar(
                "SELECT count(*) FROM candidate_only_accounting "
                "WHERE accounting_status='unaccounted'"
            ),
            "unaccounted_known_to_unknown": scalar(
                "SELECT count(*) FROM known_to_unknown_accounting "
                "WHERE accounting_status='unaccounted'"
            ),
            "gaia_white_dwarf_known_to_unknown": scalar(
                "SELECT count(*) FROM known_to_unknown_accounting "
                "WHERE reference_catalog LIKE 'gaia%' AND reference_class='WD'"
            ),
            "nonmissing_candidate_without_lineage": scalar(
                "SELECT count(*) FROM stellar_leaf_display_classifications "
                "WHERE classification_status<>'missing' AND "
                "(evidence_basis IS NULL OR source_catalog IS NULL OR source_pk IS NULL)"
            ),
        }
        report = {
            "schema_version": "spacegate.e7_runtime_classification_ab.v1",
            "status": "pass" if not any(checks.values()) else "fail",
            "build_id": build_id,
            "candidate_arm": str(candidate_arm.resolve()),
            "reference_arm": str(reference_arm.resolve()),
            "policy": {
                "inventory_changes_require_clean_hierarchy_lineage": True,
                "known_to_unknown_requires_explicit_deferral_evidence": True,
                "gaia_white_dwarf_regressions_allowed": False,
                "named_object_rules": False,
            },
            "leaf_rows": {
                "candidate": scalar(
                    "SELECT count(*) FROM stellar_leaf_display_classifications"
                ),
                "reference": scalar(
                    "SELECT count(*) FROM reference.stellar_leaf_display_classifications"
                ),
                "candidate_only": scalar("SELECT count(*) FROM candidate_only_accounting"),
                "reference_only": checks["reference_only_leaves"],
            },
            "classification_transitions": {
                "changed": int(transition[0]),
                "unknown_to_known": int(transition[1]),
                "known_to_unknown": int(transition[2]),
            },
            "candidate_only_accounting": rows_as_dicts(
                con,
                "SELECT accounting_status,candidate_class,candidate_status,candidate_basis,"
                "candidate_catalog,count(*) AS rows FROM candidate_only_accounting "
                "GROUP BY ALL ORDER BY rows DESC,accounting_status",
            ),
            "known_to_unknown_accounting": rows_as_dicts(
                con,
                "SELECT accounting_status,reference_basis,reference_catalog,reference_class,"
                "count(*) AS rows FROM known_to_unknown_accounting "
                "GROUP BY ALL ORDER BY rows DESC,accounting_status",
            ),
            "classification_delta_matrix": rows_as_dicts(
                con,
                "SELECT reference_class,candidate_class,count(*) AS rows FROM classification_ab "
                "WHERE reference_class IS NOT NULL AND candidate_class IS NOT NULL "
                "AND reference_class<>candidate_class GROUP BY ALL "
                "ORDER BY rows DESC,reference_class,candidate_class",
            ),
            "candidate_classification_by_basis": rows_as_dicts(
                con,
                "SELECT classification_status,evidence_basis,source_catalog,count(*) AS rows "
                "FROM stellar_leaf_display_classifications GROUP BY ALL "
                "ORDER BY rows DESC,evidence_basis",
            ),
            "deferred_known_to_unknown_rows": rows_as_dicts(
                con,
                "SELECT hierarchy_node_key,reference_display_name,reference_class,"
                "reference_basis,reference_catalog,reference_source_pk,accounting_status,"
                "supporting_binding_rows,supporting_case_distinct_labels "
                "FROM known_to_unknown_accounting ORDER BY accounting_status,hierarchy_node_key",
            ),
            "checks": checks,
            "wall_seconds": round(time.perf_counter() - started, 6),
            "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        }
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
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
