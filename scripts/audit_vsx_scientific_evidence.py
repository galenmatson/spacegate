#!/usr/bin/env python3
"""Audit the pinned VSX E4 scientific-evidence projection."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json  # noqa: E402


SOURCE_ID = "classification.vsx"
RELEASE_ID = "rolling_snapshot_20260721"
PARAMETER_TABLE = "variability_activity_rotation_parameter_sets"
EXPECTED_SOURCE_ROWS = {
    "vsx_dat": 10_304_607,
    "vsx_readme": 715,
    "vsx_references": 830_415,
}
EXPECTED_IDENTIFIER_CLAIMS = {
    ("vsx_dat", "gaia_dr3_source_id"): 8_016_792,
    ("vsx_dat", "variable_star_name"): 10_304_607,
    ("vsx_dat", "vsx_oid"): 10_304_607,
    ("vsx_references", "vsx_oid"): 830_415,
}
EXPECTED_ASTROMETRY = {
    "declination": 10_304_607,
    "right_ascension": 10_304_607,
}
EXPECTED_CLASSIFICATIONS = 5_152_350
EXPECTED_PARAMETER_SETS = 10_304_607
EXPECTED_SCHEMA_FIELDS = 16
EXPECTED_CITATIONS = 12_372
EXPECTED_CANONICAL_BIBCODES = 12_362
EXPECTED_NONCANONICAL_CITATIONS = 9
EXPECTED_NONCANONICAL_LINKS = 56
EXPECTED_LINKS = {
    ("astrometry_distance_evidence", "source_reference"): 20_609_214,
    ("identifier_claim_evidence", "vsx_object_bibliography"): 830_415,
    ("stellar_classification_evidence", "source_reference"): 5_152_350,
    (PARAMETER_TABLE, "source_reference"): 10_304_607,
}
EXPECTED_BINDING_OUTCOMES = {
    ("unresolved", "source_document_line"): 715,
    ("unresolved", "source_reference_link"): 830_415,
    ("unresolved", "star"): 8_016_792,
    ("unresolved", "variable_star_or_component"): 11_135_022,
}
EXPECTED_HISTORICAL_OIDS = 1_833
EXPECTED_HISTORICAL_LINKS = 2_080
EXPECTED_FIELD_MAPPING = {"excluded": 3, "materialized": 26}
DEFAULT_REPORT = DEFAULT_STATE / "reports" / "evidence_lake_v2" / (
    "e4_vsx_scientific_evidence_audit.json"
)


def rows_by_source_table(
    con: duckdb.DuckDBPyConnection,
    relation: str,
) -> dict[str, int]:
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
        str(source_table): int(count)
        for source_table, count in con.execute(query, [SOURCE_ID]).fetchall()
    }


def count_map(
    con: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[Any],
) -> dict[tuple[str, str], int]:
    return {
        (str(first), str(second)): int(count)
        for first, second, count in con.execute(query, parameters).fetchall()
    }


def deltas(
    actual: dict[Any, int],
    expected: dict[Any, int],
) -> dict[str, int]:
    keys = sorted(set(actual) | set(expected), key=str)
    return {
        "|".join(key) if isinstance(key, tuple) else str(key): (
            actual.get(key, 0) - expected.get(key, 0)
        )
        for key in keys
        if actual.get(key, 0) != expected.get(key, 0)
    }


def audit(con: duckdb.DuckDBPyConnection, manifest: dict[str, Any]) -> dict[str, Any]:
    source_rows = rows_by_source_table(con, "source_records")
    parameter_rows = rows_by_source_table(con, PARAMETER_TABLE)
    identifier_claims = count_map(
        con,
        "select r.source_table,i.namespace,count(*) "
        "from identifier_claim_evidence i "
        "join source_records r using(source_record_id) "
        "where r.source_id=? group by 1,2 order by 1,2",
        [SOURCE_ID],
    )
    astrometry = {
        str(quantity): int(count)
        for quantity, count in con.execute(
            "select e.quantity_key,count(*) "
            "from astrometry_distance_evidence e "
            "join source_records r using(source_record_id) "
            "where r.source_id=? group by 1 order by 1",
            [SOURCE_ID],
        ).fetchall()
    }
    classification_count = int(
        con.execute(
            "select count(*) from stellar_classification_evidence e "
            "join source_records r using(source_record_id) where r.source_id=?",
            [SOURCE_ID],
        ).fetchone()[0]
    )
    schema_rows = con.execute(
        "select destination,parameter_set_kind,schema_json "
        "from coherent_parameter_set_schemas "
        "where source_id=? and source_table='vsx_dat'",
        [SOURCE_ID],
    ).fetchall()
    schema_fields = (
        len(json.loads(str(schema_rows[0][2])).get("fields") or [])
        if len(schema_rows) == 1
        else 0
    )
    field_mapping = {
        str(status): int(count)
        for status, count in con.execute(
            "select mapping_status,count(*) from source_field_dispositions "
            "where source_id=? group by 1 order by 1",
            [SOURCE_ID],
        ).fetchall()
    }
    citation_count, canonical_bibcodes, noncanonical_citations = con.execute(
        "select count(*), "
        "count(*) filter(where bibcode is not null), "
        "count(*) filter(where bibcode is null and "
        "json_extract_string(parsed_json, '$.bibcode_validation.status')="
        "'preserved_noncanonical') "
        "from citations where source_id=?",
        [SOURCE_ID],
    ).fetchone()
    evidence_links = count_map(
        con,
        "select evidence_table,citation_role,count(*) "
        "from evidence_citations group by 1,2 order by 1,2",
        [],
    )
    binding_outcomes = count_map(
        con,
        "select binding_status,binding_scope,count(*) "
        "from object_binding_outcomes group by 1,2 order by 1,2",
        [],
    )
    noncanonical_links = int(
        con.execute(
            "select count(*) from evidence_citations e "
            "join citations c using(citation_id) "
            "where c.source_id=? and "
            "json_extract_string(c.parsed_json, '$.bibcode_validation.status')="
            "'preserved_noncanonical'",
            [SOURCE_ID],
        ).fetchone()[0]
    )
    historical_oids, historical_links = con.execute(
        "with current_oids as ("
        "  select distinct i.identifier_normalized "
        "  from identifier_claim_evidence i "
        "  join source_records r using(source_record_id) "
        "  where r.source_id=? and r.source_table='vsx_dat' "
        "    and i.namespace='vsx_oid'"
        "), historical as ("
        "  select i.identifier_normalized "
        "  from identifier_claim_evidence i "
        "  join source_records r using(source_record_id) "
        "  where r.source_id=? and r.source_table='vsx_references' "
        "    and i.namespace='vsx_oid' "
        "    and i.identifier_normalized not in "
        "      (select identifier_normalized from current_oids)"
        ") select count(distinct identifier_normalized),count(*) from historical",
        [SOURCE_ID, SOURCE_ID],
    ).fetchone()
    rejection_count = int(
        con.execute(
            "select count(*) from identifier_normalization_rejections n "
            "join source_records r using(source_record_id) where r.source_id=?",
            [SOURCE_ID],
        ).fetchone()[0]
    )
    release_rows = con.execute(
        "select release_id from evidence_sources where source_id=?",
        [SOURCE_ID],
    ).fetchall()
    checks: dict[str, Any] = {
        "release_mismatch": [str(row[0]) for row in release_rows]
        if release_rows != [(RELEASE_ID,)]
        else [],
        "source_record_count_deltas": deltas(source_rows, EXPECTED_SOURCE_ROWS),
        "identifier_claim_count_deltas": deltas(
            identifier_claims, EXPECTED_IDENTIFIER_CLAIMS
        ),
        "astrometry_count_deltas": deltas(astrometry, EXPECTED_ASTROMETRY),
        "classification_count_delta": (
            classification_count - EXPECTED_CLASSIFICATIONS
        ),
        "parameter_set_count_deltas": deltas(
            parameter_rows, {"vsx_dat": EXPECTED_PARAMETER_SETS}
        ),
        "schema_count_delta": len(schema_rows) - 1,
        "schema_destination_mismatch": (
            str(schema_rows[0][0])
            if len(schema_rows) == 1 and schema_rows[0][0] != PARAMETER_TABLE
            else ""
        ),
        "schema_kind_mismatch": (
            str(schema_rows[0][1])
            if len(schema_rows) == 1
            and schema_rows[0][1] != "vsx_variability_catalog_record"
            else ""
        ),
        "schema_field_count_delta": schema_fields - EXPECTED_SCHEMA_FIELDS,
        "field_mapping_count_deltas": deltas(field_mapping, EXPECTED_FIELD_MAPPING),
        "citation_count_delta": int(citation_count) - EXPECTED_CITATIONS,
        "canonical_bibcode_count_delta": (
            int(canonical_bibcodes) - EXPECTED_CANONICAL_BIBCODES
        ),
        "noncanonical_citation_count_delta": (
            int(noncanonical_citations) - EXPECTED_NONCANONICAL_CITATIONS
        ),
        "noncanonical_bibliography_link_count_delta": (
            noncanonical_links - EXPECTED_NONCANONICAL_LINKS
        ),
        "evidence_citation_count_deltas": deltas(evidence_links, EXPECTED_LINKS),
        "binding_outcome_count_deltas": deltas(
            binding_outcomes, EXPECTED_BINDING_OUTCOMES
        ),
        "historical_oid_count_delta": int(historical_oids) - EXPECTED_HISTORICAL_OIDS,
        "historical_link_count_delta": int(historical_links) - EXPECTED_HISTORICAL_LINKS,
        "identifier_normalization_rejections": rejection_count,
    }
    failed = any(bool(value) for value in checks.values())
    return {
        "schema_version": "spacegate.vsx_scientific_evidence_audit.v1",
        "status": "fail" if failed else "pass",
        "build_id": manifest["build_id"],
        "source_id": SOURCE_ID,
        "release_id": RELEASE_ID,
        "checks": checks,
        "summaries": {
            "source_records_by_table": source_rows,
            "identifier_claims": {
                "|".join(key): value for key, value in identifier_claims.items()
            },
            "astrometry": astrometry,
            "stellar_classifications": classification_count,
            "parameter_sets_by_table": parameter_rows,
            "coherent_schema_fields": schema_fields,
            "field_mapping_statuses": field_mapping,
            "citations": int(citation_count),
            "canonical_bibcodes": int(canonical_bibcodes),
            "noncanonical_citations": int(noncanonical_citations),
            "noncanonical_bibliography_links": noncanonical_links,
            "evidence_citations": {
                "|".join(key): value for key, value in evidence_links.items()
            },
            "binding_outcomes": {
                "|".join(key): value for key, value in binding_outcomes.items()
            },
            "historical_reference_oids_absent_from_current_catalog": int(
                historical_oids
            ),
            "historical_reference_links_preserved": int(historical_links),
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
        f"VSX scientific evidence audit {report['status']}: "
        f"build={manifest['build_id']} rows={sum(EXPECTED_SOURCE_ROWS.values()):,}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
