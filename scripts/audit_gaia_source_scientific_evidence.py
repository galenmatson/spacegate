#!/usr/bin/env python3
"""Audit the complete Gaia DR3 source E4 scientific-evidence adapter."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json  # noqa: E402


SOURCE_ID = "gaia.dr3.gaia_source"
RELEASE_ID = "gaia_dr3_1250ly_envelope_v2"
HARD_TABLE = "gaia_dr3_source_envelope_v2"
SUPPLEMENT_TABLE = "gaia_dr3_source_uncertain_distance_supplement_v1"
EXPECTED_ROWS = {HARD_TABLE: 31_987_126, SUPPLEMENT_TABLE: 189_145}
EXPECTED_DOMAIN_FIELDS = {
    "astrometric_solution": 65,
    "photometric_solution": 24,
    "radial_velocity_solution": 22,
    "classification_and_membership": 8,
    "observation_product_index": 6,
}
ROLE_TO_SCHEMA_DOMAIN = {
    "astrometric_solution": "astrometry",
    "photometric_solution": "photometry",
    "radial_velocity_solution": "radial_velocity",
    "classification_and_membership": "classification_and_membership",
    "observation_product_index": "observation_product_availability",
}
EXPECTED_COVERAGE = {
    "radial_velocity": 2_929_216,
    "has_xp_continuous": 5_778_039,
    "has_rvs": 206_781,
    "has_epoch_photometry": 548_038,
}
DEFAULT_SOURCE_AUDIT = (
    DEFAULT_STATE
    / "reports"
    / "evidence_lake_v2"
    / "e4_gaia_source_typed_source_audit.json"
)
DEFAULT_REPORT = (
    DEFAULT_STATE
    / "reports"
    / "evidence_lake_v2"
    / "e4_gaia_source_scientific_evidence_audit.json"
)


def grouped_counts(
    con: duckdb.DuckDBPyConnection, query: str
) -> dict[tuple[str, ...], int]:
    return {
        tuple(str(value) for value in row[:-1]): int(row[-1])
        for row in con.execute(query).fetchall()
    }


def count_deltas(
    actual: dict[tuple[str, ...], int], expected: dict[tuple[str, ...], int]
) -> dict[str, int]:
    return {
        "|".join(key): actual.get(key, 0) - expected.get(key, 0)
        for key in sorted(set(actual) | set(expected))
        if actual.get(key, 0) != expected.get(key, 0)
    }


def schema_fields(schema_raw: str) -> list[dict[str, Any]]:
    return list(json.loads(schema_raw).get("fields") or [])


def aggregate_coverage(
    con: duckdb.DuckDBPyConnection,
    schemas: list[tuple[str, str, str]],
) -> dict[str, int]:
    totals = {field: 0 for field in EXPECTED_COVERAGE}
    for schema_id, source_table, schema_raw in schemas:
        positions = {
            str(field["name"]): int(field["position"])
            for field in schema_fields(schema_raw)
        }
        missing = sorted(set(EXPECTED_COVERAGE) - set(positions))
        if missing:
            raise ValueError(
                f"Gaia source coherent schema lacks coverage fields for "
                f"{source_table}: {missing}"
            )
        expressions = []
        for field, position in positions.items():
            if field not in EXPECTED_COVERAGE:
                continue
            value = f"json_extract(values_json, '$[{position}]')"
            predicate = (
                f"try_cast({value} as boolean)"
                if field.startswith("has_")
                else f"try_cast({value} as double) is not null"
            )
            expressions.append(
                f"count(*) filter (where {predicate})::bigint as \"{field}\""
            )
        row = con.execute(
            "select " + ",".join(expressions)
            + " from stellar_source_parameter_sets where parameter_schema_id=?",
            [schema_id],
        ).fetchone()
        for index, field in enumerate(
            field for field in positions if field in EXPECTED_COVERAGE
        ):
            totals[field] += int(row[index])
    return totals


def audit(
    con: duckdb.DuckDBPyConnection,
    manifest: dict[str, Any],
    source_audit: dict[str, Any],
) -> dict[str, Any]:
    source_rows = grouped_counts(
        con,
        "select source_table,count(*) from source_records "
        f"where source_id='{SOURCE_ID}' group by 1 order by 1",
    )
    expected_rows = {(table,): count for table, count in EXPECTED_ROWS.items()}
    dispositions = grouped_counts(
        con,
        "select source_table,mapping_status,count(*) "
        "from source_field_dispositions "
        f"where source_id='{SOURCE_ID}' group by 1,2 order by 1,2",
    )
    expected_dispositions = {
        (table, "materialized"): 127 for table in EXPECTED_ROWS
    } | {(table, "excluded"): 25 for table in EXPECTED_ROWS}
    identifiers = grouped_counts(
        con,
        "select r.source_table,i.namespace,count(*) "
        "from identifier_claim_evidence i join source_records r using(source_record_id) "
        f"where r.source_id='{SOURCE_ID}' group by 1,2 order by 1,2",
    )
    expected_identifiers = {
        (table, "gaia_dr3_source_id"): count
        for table, count in EXPECTED_ROWS.items()
    }
    parameter_sets = grouped_counts(
        con,
        "select r.source_table,count(*) from stellar_source_parameter_sets p "
        "join source_records r using(source_record_id) "
        f"where r.source_id='{SOURCE_ID}' group by 1 order by 1",
    )
    bindings = grouped_counts(
        con,
        "select r.source_table,b.binding_status,b.binding_scope,count(*) "
        "from object_binding_outcomes b join source_records r using(source_record_id) "
        f"where r.source_id='{SOURCE_ID}' group by 1,2,3 order by 1,2,3",
    )
    expected_bindings = {
        (table, "unresolved", "star"): count
        for table, count in EXPECTED_ROWS.items()
    }
    schemas = [
        (str(schema_id), str(source_table), str(schema_raw))
        for schema_id, source_table, schema_raw in con.execute(
            "select parameter_schema_id,source_table,schema_json "
            "from coherent_parameter_set_schemas "
            "where source_id=? and destination='stellar_source_parameter_sets' "
            "order by source_table",
            [SOURCE_ID],
        ).fetchall()
    ]
    expected_fields = {
        field
        for role, fields in source_audit["summaries"]["field_roles"].items()
        if role in EXPECTED_DOMAIN_FIELDS
        for field in fields
    }
    schema_field_deltas: dict[str, dict[str, list[str]]] = {}
    schema_domain_count_deltas: dict[str, dict[str, int]] = {}
    schema_metadata_defects = 0
    for _schema_id, source_table, schema_raw in schemas:
        fields = schema_fields(schema_raw)
        names = {str(field["name"]) for field in fields}
        if names != expected_fields:
            schema_field_deltas[source_table] = {
                "missing": sorted(expected_fields - names),
                "unexpected": sorted(names - expected_fields),
            }
        actual_domains = Counter(str(field.get("scientific_domain")) for field in fields)
        expected_domains = {
            ROLE_TO_SCHEMA_DOMAIN[role]: count
            for role, count in EXPECTED_DOMAIN_FIELDS.items()
        }
        domain_deltas = {
            domain: actual_domains.get(domain, 0) - expected
            for domain, expected in expected_domains.items()
            if actual_domains.get(domain, 0) != expected
        }
        domain_deltas.update(
            {
                domain: count
                for domain, count in actual_domains.items()
                if domain not in expected_domains
            }
        )
        if domain_deltas:
            schema_domain_count_deltas[source_table] = domain_deltas
        schema_metadata_defects += sum(
            int(field.get("datatype") in (None, "")) for field in fields
        )
    coverage = aggregate_coverage(con, schemas) if len(schemas) == 2 else {}
    coverage_deltas = {
        field: coverage.get(field, 0) - expected
        for field, expected in EXPECTED_COVERAGE.items()
        if coverage.get(field, 0) != expected
    }
    context_defects = int(
        con.execute(
            "select count(*) from source_records where source_id=? and "
            "(json_keys(source_context_json)<>['solution_id'] or "
            "nullif(json_extract_string(source_context_json,'$.solution_id'),'') is null)",
            [SOURCE_ID],
        ).fetchone()[0]
    )
    excluded_projection_defects = int(
        con.execute(
            "select count(*) from source_field_dispositions where source_id=? "
            "and (source_field like '%_gspphot%' or "
            "source_field in ('has_mcmc_gspphot','libname_gspphot')) "
            "and mapping_status<>'excluded'",
            [SOURCE_ID],
        ).fetchone()[0]
    )
    citation_count = int(
        con.execute("select count(*) from citations where source_id=?", [SOURCE_ID]).fetchone()[0]
    )
    citation_link_count = int(
        con.execute(
            "select count(*) from evidence_citations "
            "where evidence_table='stellar_source_parameter_sets'"
        ).fetchone()[0]
    )
    total_rows = sum(EXPECTED_ROWS.values())
    checks: dict[str, Any] = {
        "source_release_mismatch": int(
            con.execute(
                "select abs(count(*)-1) + count(*) filter (where release_id<>?) "
                "from evidence_sources where source_id=?",
                [RELEASE_ID, SOURCE_ID],
            ).fetchone()[0]
        ),
        "source_record_count_deltas": count_deltas(source_rows, expected_rows),
        "field_disposition_count_deltas": count_deltas(
            dispositions, expected_dispositions
        ),
        "identifier_count_deltas": count_deltas(identifiers, expected_identifiers),
        "parameter_set_count_deltas": count_deltas(parameter_sets, expected_rows),
        "binding_count_deltas": count_deltas(bindings, expected_bindings),
        "schema_count_delta": len(schemas) - 2,
        "schema_field_deltas": schema_field_deltas,
        "schema_domain_count_deltas": schema_domain_count_deltas,
        "schema_metadata_defects": schema_metadata_defects,
        "coverage_count_deltas": coverage_deltas,
        "source_context_defects": context_defects,
        "excluded_projection_defects": excluded_projection_defects,
        "citation_count_delta": citation_count - 1,
        "citation_link_count_delta": citation_link_count - total_rows,
        "identifier_normalization_rejections": int(
            con.execute(
                "select count(*) from identifier_normalization_rejections"
            ).fetchone()[0]
        ),
    }
    failed = any(bool(value) for value in checks.values())
    return {
        "schema_version": "spacegate.gaia_source_scientific_evidence_audit.v1",
        "status": "fail" if failed else "pass",
        "build_id": manifest["build_id"],
        "source_id": SOURCE_ID,
        "release_id": RELEASE_ID,
        "checks": checks,
        "summaries": {
            "source_records": {"|".join(k): v for k, v in source_rows.items()},
            "field_dispositions": {"|".join(k): v for k, v in dispositions.items()},
            "identifier_claims": {"|".join(k): v for k, v in identifiers.items()},
            "parameter_sets": {"|".join(k): v for k, v in parameter_sets.items()},
            "bindings": {"|".join(k): v for k, v in bindings.items()},
            "schema_domain_field_counts": EXPECTED_DOMAIN_FIELDS,
            "schema_value_field_count": len(expected_fields),
            "coverage": coverage,
            "citations": citation_count,
            "evidence_citation_links": citation_link_count,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--source-audit", type=Path, default=DEFAULT_SOURCE_AUDIT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    source_audit = load_json(args.source_audit)
    if source_audit.get("status") != "pass":
        raise ValueError("Gaia typed-source audit must pass before E4 audit")
    database = args.manifest.parent / str(manifest["database"])
    with duckdb.connect(str(database), read_only=True) as con:
        con.execute("set threads=4")
        con.execute("set memory_limit='16GB'")
        report = audit(con, manifest, source_audit)
    write_json(args.report, report)
    print(
        f"Gaia source scientific evidence audit {report['status']}: "
        f"build={manifest['build_id']} rows={sum(EXPECTED_ROWS.values()):,}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
