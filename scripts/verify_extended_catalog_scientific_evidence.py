#!/usr/bin/env python3
"""Audit OpenNGC and constituent nebula-catalog scientific evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "extended.openngc_and_nebulae"
CATALOG_RECORDS = {
    "barnard_vii_220a": 349,
    "cederblad_vii_231": 330,
    "lbn_vii_9": 1125,
    "ldn_vii_7a": 1791,
    "magakian_2003": 913,
    "openngc_addendum": 64,
    "openngc_ngc": 13969,
    "sharpless_vii_20": 313,
    "vdb_vii_21": 158,
}
DOCUMENT_RECORDS = {
    "barnard_vii_220a_readme": 93,
    "cederblad_vii_231_readme": 245,
    "lbn_vii_9_readme": 99,
    "ldn_vii_7a_readme": 123,
    "magakian_2003_readme": 129,
    "sharpless_vii_20_readme": 74,
    "vdb_vii_21_readme": 93,
}
EXPECTED_NAMESPACES = {
    "barnard_designation": 582,
    "cederblad_component_designation": 149,
    "cederblad_designation": 181,
    "ic_designation": 460,
    "lbn_designation": 1125,
    "ldn_designation": 1787,
    "magakian_2003_entry": 913,
    "messier_designation": 110,
    "nebula_source_designation": 388,
    "ngc_designation": 908,
    "openngc_designation": 14033,
    "sharpless_designation": 313,
    "vdb_designation": 158,
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
            "select source_table,count(*) from source_records where source_id=? "
            "group by source_table",
            [SOURCE_ID],
        ).fetchall()
    }
    namespace_counts = {
        str(namespace): int(count)
        for namespace, count in con.execute(
            """
            select namespace,count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id=? group by namespace
            """,
            [SOURCE_ID],
        ).fetchall()
    }
    checks = {
        "unexpected_source_record_counts": mismatch_count(
            source_counts, {**CATALOG_RECORDS, **DOCUMENT_RECORDS}
        ),
        "unexpected_identifier_namespace_counts": mismatch_count(
            namespace_counts, EXPECTED_NAMESPACES
        ),
        "unaccounted_or_pending_fields": scalar(
            con,
            f"""
            select count(*) from source_field_dispositions
            where source_id='{SOURCE_ID}' and mapping_status<>'materialized'
            """,
        ),
        "catalog_records_without_extended_evidence": scalar(
            con,
            f"""
            select count(*) from source_records r
            left join extended_object_evidence e using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and r.source_table not like '%_readme'
              and e.source_record_id is null
            """,
        ),
        "document_lines_promoted_as_objects": scalar(
            con,
            f"""
            select count(*) from extended_object_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and r.source_table like '%_readme'
            """,
        ),
        "source_records_without_binding_outcome": scalar(
            con,
            f"""
            select count(*) from source_records r
            left join object_binding_outcomes b using(source_record_id)
            where r.source_id='{SOURCE_ID}' and b.source_record_id is null
            """,
        ),
        "premature_non_unresolved_bindings": scalar(
            con,
            f"""
            select count(*) from object_binding_outcomes b
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and b.binding_status<>'unresolved'
            """,
        ),
        "referenced_evidence_without_citation": scalar(
            con,
            f"""
            select count(*) from extended_object_evidence e
            join source_records r using(source_record_id)
            left join evidence_citations c
              on c.evidence_table='extended_object_evidence'
             and c.evidence_id=e.evidence_id
            where r.source_id='{SOURCE_ID}'
              and nullif(trim(e.reference_raw),'') is not null
              and c.evidence_id is null
            """,
        ),
        "cederblad_components_claiming_base_identity": scalar(
            con,
            f"""
            select count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and r.source_table='cederblad_vii_231'
              and nullif(r.logical_key_json->>'m_Ced','') is not null
              and i.namespace='cederblad_designation'
            """,
        ),
        "heuristically_split_openngc_alias_claims": scalar(
            con,
            f"""
            select count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and r.source_table in ('openngc_ngc','openngc_addendum')
              and (i.quality_json->>'source_field') in (
                'Identifiers','Common names','Cstar Names'
              )
            """,
        ),
        "blank_identifier_claims": scalar(
            con,
            f"""
            select count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and (
              nullif(trim(i.identifier_raw),'') is null
              or nullif(trim(i.identifier_normalized),'') is null
            )
            """,
        ),
        "relation_or_orbit_promotion": scalar(
            con,
            f"""
            select count(*) from (
              select source_record_id from relation_claim_evidence
              union all select source_record_id from orbital_solution_evidence
            ) e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            """,
        ),
    }
    summaries = {
        "source_records_by_table": rows(
            con,
            f"""
            select source_table,count(*) row_count from source_records
            where source_id='{SOURCE_ID}' group by source_table order by source_table
            """,
        ),
        "identifier_claims_by_namespace": rows(
            con,
            f"""
            select namespace,count(*) claim_count,
              count(distinct identifier_normalized) distinct_value_count
            from identifier_claim_evidence i join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' group by namespace order by namespace
            """,
        ),
        "extended_kinds": rows(
            con,
            f"""
            select extended_kind,count(*) evidence_count
            from extended_object_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' group by extended_kind order by extended_kind
            """,
        ),
        "citation_counts": rows(
            con,
            f"""
            select count(*) citation_count from citations where source_id='{SOURCE_ID}'
            """,
        )[0],
        "bindings_by_status_and_scope": rows(
            con,
            f"""
            select binding_status,binding_scope,count(*) outcome_count
            from object_binding_outcomes b join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' group by binding_status,binding_scope
            order by binding_status,binding_scope
            """,
        ),
    }
    return {
        "schema_version": "spacegate.extended_catalog_scientific_evidence_audit.v1",
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
            / "e4_extended_catalog_scope_audit.json"
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
        f"Extended-catalog scientific evidence audit {report['status']}: "
        f"{report['build_id']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
