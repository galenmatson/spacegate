#!/usr/bin/env python3
"""Audit official Gaia DR3 external best-neighbour evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "gaia.dr3.external_crossmatches"
RELEASE_ID = "gaia_dr3_1250ly_external_crossmatches_v1"
ADAPTER_VERSION = "gaia_dr3_external_best_neighbour_scientific_evidence_v1"
TOTAL_ROWS = 24_045_693
FIELD_OCCURRENCES = 62
REFERENCE = (
    "https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/"
    "chap_datamodel/sec_dm_cross-matches/"
)
EXPECTED_RELATIONS = {
    "gaia_dr3_to_2mass": 11_896_596,
    "gaia_dr3_to_allwise": 11_201_608,
    "gaia_dr3_to_hipparcos2": 69_285,
    "gaia_dr3_to_rave_dr6": 126_154,
    "gaia_dr3_to_tycho2": 752_050,
}


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def grouped(con: duckdb.DuckDBPyConnection, query: str) -> dict[str, int]:
    return {str(key): int(value) for key, value in con.execute(query).fetchall()}


def audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    relations = grouped(
        con,
        "select relation_scope,count(*) from relation_claim_evidence group by 1",
    )
    bindings = grouped(
        con,
        "select binding_scope,count(*) from object_binding_outcomes group by 1",
    )
    checks = {
        "unexpected_source_metadata": abs(
            scalar(con, "select count(*) from evidence_sources") - 1
        )
        + scalar(
            con,
            "select count(*) from evidence_sources where "
            f"source_id<>'{SOURCE_ID}' or release_id<>'{RELEASE_ID}' or "
            f"adapter_version<>'{ADAPTER_VERSION}'",
        ),
        "unexpected_source_records": abs(
            scalar(con, "select count(*) from source_records") - TOTAL_ROWS
        )
        + scalar(
            con,
            "select count(*) from source_records where "
            "object_scope<>'gaia_external_best_neighbour_relation' or "
            "source_duplicate_count<>1",
        ),
        "unexpected_identifier_claims": abs(
            scalar(con, "select count(*) from identifier_claim_evidence")
            - 2 * TOTAL_ROWS
        ),
        "identifier_normalization_rejections": scalar(
            con, "select count(*) from identifier_normalization_rejections"
        ),
        "unexpected_relations": (
            sum(abs(relations.get(key, 0) - value) for key, value in EXPECTED_RELATIONS.items())
            + sum(value for key, value in relations.items() if key not in EXPECTED_RELATIONS)
        ),
        "wrong_relation_semantics": scalar(
            con,
            "select count(*) from relation_claim_evidence where "
            "relation_kind<>'official_gaia_external_best_neighbour' or "
            "evidence_polarity<>'candidate' or probability is not null or "
            "confidence_statistic_key<>'angular_separation' or "
            "confidence_statistic_value is null or reference_raw<>" + repr(REFERENCE),
        ),
        "unexpected_bindings": sum(
            abs(bindings.get(scope, 0) - TOTAL_ROWS)
            for scope in (
                "gaia_external_best_neighbour_relation",
                "observation_target",
                "star",
            )
        ) + sum(
            value
            for scope, value in bindings.items()
            if scope not in {
                "gaia_external_best_neighbour_relation",
                "observation_target",
                "star",
            }
        ),
        "non_unresolved_bindings": scalar(
            con,
            "select count(*) from object_binding_outcomes "
            "where binding_status<>'unresolved'",
        ),
        "unexpected_field_accounting": abs(
            scalar(con, "select count(*) from source_field_dispositions")
            - FIELD_OCCURRENCES
        )
        + scalar(
            con,
            "select count(*) from source_field_dispositions "
            "where mapping_status<>'materialized' or disposition='exclude'",
        ),
        "unexpected_citations": abs(scalar(con, "select count(*) from citations") - 1)
        + abs(
            scalar(con, "select count(*) from evidence_citations") - TOTAL_ROWS
        ),
    }
    return {
        "schema_version": "spacegate.gaia_external_crossmatch_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": {"relations": relations, "bindings": bindings},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_STATE / "reports/evidence_lake_v2/e4_gaia_external_crossmatch_audit.json",
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
        f"Gaia external-crossmatch audit {report['status']}: "
        f"{sum(report['checks'].values())} discrepancies"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
