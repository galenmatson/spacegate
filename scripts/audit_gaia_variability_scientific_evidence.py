#!/usr/bin/env python3
"""Audit the Gaia variability coherent E4 scientific-evidence projection."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from audit_gaia_variability_typed_source import (  # noqa: E402
    EXPECTED_FIELDS,
    ROTATION_TABLES,
    SUMMARY_TABLES,
)
from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json  # noqa: E402
from evidence_parameter_sets import MASKED_VECTOR_ENCODING  # noqa: E402


SOURCE_ID = "gaia.dr3.variability"
PARAMETER_TABLE = "variability_activity_rotation_parameter_sets"
EXPECTED_ROWS = {
    "gaia_dr3_variability_summary_v2": 536_777,
    "gaia_dr3_rotation_modulation_v2": 43_208,
    "gaia_dr3_variability_summary_uncertain_distance_supplement_v1": 11_261,
    "gaia_dr3_rotation_modulation_uncertain_distance_supplement_v1": 951,
}
DEFAULT_REPORT = DEFAULT_STATE / "reports" / "evidence_lake_v2" / (
    "e4_gaia_variability_scientific_evidence_audit.json"
)


def rows_by_table(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, int]:
    if relation == "source_records":
        query = (
            "select source_table,count(*) from source_records "
            "where source_id=? group by 1 order by 1"
        )
    else:
        query = (
            f"select r.source_table,count(*) from {relation} e "
            "join source_records r using(source_record_id) "
            "where r.source_id=? group by 1 order by 1"
        )
    return {
        str(table): int(count)
        for table, count in con.execute(query, [SOURCE_ID]).fetchall()
    }


def audit(con: duckdb.DuckDBPyConnection, manifest: dict[str, Any]) -> dict[str, Any]:
    source_records = rows_by_table(con, "source_records")
    parameter_sets = rows_by_table(con, PARAMETER_TABLE)
    schema_rows = con.execute(
        "select source_table,destination,parameter_set_kind,schema_json "
        "from coherent_parameter_set_schemas where source_id=? order by source_table",
        [SOURCE_ID],
    ).fetchall()
    schema_summaries = []
    wrong_schema_field_counts: dict[str, int] = {}
    wrong_vector_counts: dict[str, int] = {}
    wrong_destinations: list[str] = []
    for source_table, destination, kind, raw_schema in schema_rows:
        schema = json.loads(str(raw_schema))
        fields = list(schema.get("fields") or [])
        vectors = [
            field
            for field in fields
            if field.get("encoding") == MASKED_VECTOR_ENCODING
        ]
        expected_fields = EXPECTED_FIELDS[str(source_table)] - 2
        expected_vectors = 52 if source_table in ROTATION_TABLES else 0
        if len(fields) != expected_fields:
            wrong_schema_field_counts[str(source_table)] = len(fields)
        if len(vectors) != expected_vectors:
            wrong_vector_counts[str(source_table)] = len(vectors)
        if destination != PARAMETER_TABLE:
            wrong_destinations.append(str(source_table))
        schema_summaries.append(
            {
                "source_table": str(source_table),
                "parameter_set_kind": str(kind),
                "value_fields": len(fields),
                "masked_vector_fields": len(vectors),
            }
        )
    mapping_counts = {
        str(status): int(count)
        for status, count in con.execute(
            "select mapping_status,count(*) from source_field_dispositions "
            "where source_id=? group by 1 order by 1",
            [SOURCE_ID],
        ).fetchall()
    }
    citation_count = int(
        con.execute("select count(*) from citations where source_id=?", [SOURCE_ID]).fetchone()[0]
    )
    citation_links = int(
        con.execute(
            "select count(*) from evidence_citations where evidence_table=?",
            [PARAMETER_TABLE],
        ).fetchone()[0]
    )
    identifier_claims = int(
        con.execute(
            "select count(*) from identifier_claim_evidence i "
            "join source_records r using(source_record_id) where r.source_id=?",
            [SOURCE_ID],
        ).fetchone()[0]
    )
    checks: dict[str, Any] = {
        "source_record_count_deltas": {
            table: source_records.get(table, 0) - expected
            for table, expected in EXPECTED_ROWS.items()
            if source_records.get(table, 0) != expected
        },
        "parameter_set_count_deltas": {
            table: parameter_sets.get(table, 0) - expected
            for table, expected in EXPECTED_ROWS.items()
            if parameter_sets.get(table, 0) != expected
        },
        "unexpected_source_record_tables": sorted(set(source_records) - set(EXPECTED_ROWS)),
        "unexpected_parameter_set_tables": sorted(set(parameter_sets) - set(EXPECTED_ROWS)),
        "schema_count_delta": len(schema_rows) - len(EXPECTED_ROWS),
        "wrong_schema_field_counts": wrong_schema_field_counts,
        "wrong_masked_vector_counts": wrong_vector_counts,
        "wrong_schema_destinations": wrong_destinations,
        "nonmaterialized_field_occurrences": sum(
            count for status, count in mapping_counts.items() if status != "materialized"
        ),
        "materialized_field_count_delta": mapping_counts.get("materialized", 0)
        - sum(EXPECTED_FIELDS.values()),
        "identifier_claim_count_delta": identifier_claims - sum(EXPECTED_ROWS.values()),
        "citation_count_delta": citation_count - 2,
        "citation_link_count_delta": citation_links - sum(EXPECTED_ROWS.values()),
    }
    failed = any(bool(value) for value in checks.values())
    return {
        "schema_version": "spacegate.gaia_variability_scientific_evidence_audit.v1",
        "status": "fail" if failed else "pass",
        "build_id": manifest["build_id"],
        "source_id": SOURCE_ID,
        "checks": checks,
        "summaries": {
            "source_records_by_table": source_records,
            "parameter_sets_by_table": parameter_sets,
            "schemas": schema_summaries,
            "mapping_status_counts": mapping_counts,
            "identifier_claims": identifier_claims,
            "citations": citation_count,
            "citation_links": citation_links,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    with duckdb.connect(str(database), read_only=True) as con:
        report = audit(con, manifest)
    write_json(args.report, report)
    print(
        f"Gaia variability scientific evidence audit {report['status']}: "
        f"build={manifest['build_id']} rows={sum(EXPECTED_ROWS.values()):,}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
