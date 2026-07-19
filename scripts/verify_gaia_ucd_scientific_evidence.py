#!/usr/bin/env python3
"""Audit the Gaia DR3 ultracool-dwarf association evidence checkpoint."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "ultracool.gaia_dr3_sample"
EXPECTED_SOURCE_RECORDS = {"table4": 7_630, "table4_readme": 93}
EXPECTED_MEMBERSHIPS = {
    "banyan_sigma_best_hypothesis": 2_840,
    "hmac_unsupervised_cluster_assignment": 6_259,
}


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def mismatch_count(actual: dict[str, int], expected: dict[str, int]) -> int:
    return sum(abs(actual.get(key, 0) - count) for key, count in expected.items()) + sum(
        count for key, count in actual.items() if key not in expected
    )


def audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    source_counts = {
        str(table): int(count)
        for table, count in con.execute(
            "select source_table,count(*) from source_records group by source_table"
        ).fetchall()
    }
    membership_counts = {
        str(method): int(count)
        for method, count in con.execute(
            "select method,count(*) from cluster_membership_evidence group by method"
        ).fetchall()
    }
    checks = {
        "unexpected_source_record_counts": mismatch_count(
            source_counts, EXPECTED_SOURCE_RECORDS
        ),
        "collapsed_or_duplicate_source_rows": scalar(
            con, "select count(*) from source_records where source_duplicate_count<>1"
        ),
        "unexpected_gaia_identity_count": abs(
            scalar(
                con,
                "select count(*) from identifier_claim_evidence "
                "where namespace='gaia_dr3_source_id' and claim_scope='star'",
            )
            - 7_630
        ),
        "unexpected_non_gaia_identifier_claim": scalar(
            con,
            "select count(*) from identifier_claim_evidence "
            "where namespace<>'gaia_dr3_source_id'",
        ),
        "unexpected_membership_counts": mismatch_count(
            membership_counts, EXPECTED_MEMBERSHIPS
        ),
        "source_placeholder_promoted": scalar(
            con,
            "select count(*) from cluster_membership_evidence "
            "where cluster_identity_raw='--' or member_identity_raw='--'",
        ),
        "banyan_probability_missing_or_out_of_range": scalar(
            con,
            "select count(*) from cluster_membership_evidence "
            "where method='banyan_sigma_best_hypothesis' "
            "and (membership_probability is null or membership_probability not between 0 and 1)",
        ),
        "hmac_probability_fabricated": scalar(
            con,
            "select count(*) from cluster_membership_evidence "
            "where method='hmac_unsupervised_cluster_assignment' "
            "and membership_probability is not null",
        ),
        "unexpected_hmac_label_domain": scalar(
            con,
            "select count(*) from cluster_membership_evidence "
            "where method='hmac_unsupervised_cluster_assignment' "
            "and try_cast(cluster_identity_raw as integer) not between 1 and 93",
        ),
        "missing_or_incorrect_source_citation": abs(
            scalar(
                con,
                "select count(*) from evidence_citations ec "
                "join cluster_membership_evidence e "
                "on ec.evidence_table='cluster_membership_evidence' "
                "and ec.evidence_id=e.evidence_id "
                "join citations c using(citation_id) "
                "where c.bibcode='2023A&A...669A.139S'",
            )
            - 9_099
        ),
        "unaccounted_or_pending_fields": scalar(
            con,
            "select count(*) from source_field_dispositions "
            "where mapping_status<>'materialized'",
        ),
        "unexpected_field_count": abs(
            scalar(con, "select count(*) from source_field_dispositions") - 8
        ),
        "premature_spectral_classification": scalar(
            con, "select count(*) from stellar_classification_evidence"
        ),
        "premature_relation_or_orbit_promotion": scalar(
            con,
            "select (select count(*) from relation_claim_evidence) "
            "+ (select count(*) from orbital_solution_evidence)",
        ),
        "source_records_without_binding_outcome": scalar(
            con,
            "select count(*) from source_records r "
            "left join object_binding_outcomes b using(source_record_id) "
            "where b.source_record_id is null",
        ),
        "premature_non_unresolved_bindings": scalar(
            con,
            "select count(*) from object_binding_outcomes "
            "where binding_status<>'unresolved'",
        ),
    }
    summaries = {
        "source_records_by_table": rows(
            con,
            "select source_table,count(*) row_count from source_records "
            "group by source_table order by source_table",
        ),
        "memberships_by_method": rows(
            con,
            "select method,count(*) evidence_count,count(membership_probability) "
            "probability_count,min(membership_probability) minimum_probability,"
            "max(membership_probability) maximum_probability "
            "from cluster_membership_evidence group by method order by method",
        ),
        "bindings": rows(
            con,
            "select binding_status,binding_scope,count(*) outcome_count "
            "from object_binding_outcomes group by all order by binding_scope",
        ),
    }
    return {
        "schema_version": "spacegate.gaia_ucd_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--report",
        type=Path,
        default=(
            DEFAULT_STATE
            / "reports"
            / "evidence_lake_v2"
            / "e4_gaia_ucd_scope_audit.json"
        ),
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    with duckdb.connect(str(database), read_only=True) as con:
        report = audit(con)
    report["build_id"] = str(manifest["build_id"])
    report["database"] = str(database)
    write_json(args.report, report)
    print(
        f"Gaia UCD scientific evidence audit {report['status']}: "
        f"{report['build_id']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
