#!/usr/bin/env python3
"""Audit MSC scientific evidence, component scope, and source-row accounting."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "multiplicity.msc"
EXPECTED_SOURCE_RECORDS = {
    "msc_archive_members": 5,
    "msc_comp": 13_151,
    "msc_notes": 9_577,
    "msc_orb": 4_728,
    "msc_readme": 209,
    "msc_sys": 15_748,
}
EXPECTED_RELATION_POLARITIES = {
    "ambiguous": 883,
    "negative": 360,
    "positive": 14_505,
}
EXPECTED_ORBITS = {"msc_orb": 4_728, "msc_sys": 14_638}


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
    relation_polarities = {
        str(polarity): int(count)
        for polarity, count in con.execute(
            """
            select evidence_polarity,count(*)
            from relation_claim_evidence e
            join source_records r using(source_record_id)
            where r.source_id=? group by evidence_polarity
            """,
            [SOURCE_ID],
        ).fetchall()
    }
    orbit_counts = {
        str(table): int(count)
        for table, count in con.execute(
            """
            select r.source_table,count(*)
            from orbital_solution_evidence e
            join source_records r using(source_record_id)
            where r.source_id=? group by r.source_table
            """,
            [SOURCE_ID],
        ).fetchall()
    }
    checks = {
        "unexpected_source_record_counts": mismatch_count(
            source_counts, EXPECTED_SOURCE_RECORDS
        ),
        "collapsed_or_duplicate_source_rows": scalar(
            con,
            f"""
            select count(*) from source_records
            where source_id='{SOURCE_ID}' and source_duplicate_count<>1
            """,
        ),
        "unexpected_relation_polarities": mismatch_count(
            relation_polarities, EXPECTED_RELATION_POLARITIES
        ),
        "unexpected_orbit_counts": mismatch_count(orbit_counts, EXPECTED_ORBITS),
        "unaccounted_or_pending_fields": scalar(
            con,
            f"""
            select count(*) from source_field_dispositions
            where source_id='{SOURCE_ID}' and mapping_status<>'materialized'
            """,
        ),
        "unexpected_field_count": abs(
            scalar(
                con,
                f"select count(*) from source_field_dispositions where source_id='{SOURCE_ID}'",
            )
            - 73
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
        "system_rows_without_exactly_one_relation": scalar(
            con,
            f"""
            select count(*) from (
              select r.source_record_id,count(e.evidence_id) claim_count
              from source_records r
              left join relation_claim_evidence e using(source_record_id)
              where r.source_id='{SOURCE_ID}' and r.source_table='msc_sys'
              group by r.source_record_id
            ) where claim_count<>1
            """,
        ),
        "relation_outside_system_rows": scalar(
            con,
            f"""
            select count(*) from relation_claim_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and r.source_table<>'msc_sys'
            """,
        ),
        "relation_endpoint_without_wds_scope": scalar(
            con,
            f"""
            select count(*) from relation_claim_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and (
              split_part(e.left_identity_raw, ':', 1)<>
                json_extract_string(r.logical_key_json, '$.WDS')
              or split_part(e.right_identity_raw, ':', 1)<>
                json_extract_string(r.logical_key_json, '$.WDS')
              or strpos(e.left_identity_raw, ':')=0
              or strpos(e.right_identity_raw, ':')=0
            )
            """,
        ),
        "relation_polarity_disagrees_with_source_status": scalar(
            con,
            f"""
            select count(*) from relation_claim_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and e.evidence_polarity <>
              case
                when strpos(json_extract_string(e.quality_json, '$.Type'), 'X')>0
                  then 'negative'
                when strpos(json_extract_string(e.quality_json, '$.Type'), '?')>0
                  or starts_with(json_extract_string(e.quality_json, '$.Type'), 'c')
                  then 'ambiguous'
                else 'positive'
              end
            """,
        ),
        "special_parent_marker_promoted_as_identifier": scalar(
            con,
            f"""
            select count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and i.namespace in ('msc_component','msc_component_label')
              and (i.identifier_raw in ('*','t')
                or i.identifier_raw like '%:*' or i.identifier_raw like '%:t')
            """,
        ),
        "component_identifier_without_wds_scope": scalar(
            con,
            f"""
            select count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and i.namespace='msc_component'
              and split_part(i.identifier_raw, ':', 1)<>
                json_extract_string(r.logical_key_json, '$.WDS')
            """,
        ),
        "heuristically_split_identifier_list": scalar(
            con,
            f"""
            select count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and json_extract_string(i.quality_json, '$.source_field')='Id'
            """,
        ),
        "orbit_rows_without_solution": scalar(
            con,
            f"""
            select count(*) from source_records r
            left join orbital_solution_evidence e using(source_record_id)
            where r.source_id='{SOURCE_ID}' and r.source_table='msc_orb'
              and e.source_record_id is null
            """,
        ),
        "orbit_pair_parsed_or_prematurely_linked": scalar(
            con,
            f"""
            select count(*) from orbital_solution_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and r.source_table='msc_orb'
              and e.relation_claim_id is not null
            """,
        ),
        "classification_without_source_component_scope": scalar(
            con,
            f"""
            select count(*) from stellar_classification_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and (
              nullif(trim(e.component_scope),'') is null
              or strpos(e.component_scope, ':')=0
              or split_part(e.component_scope, ':', 1)<>
                json_extract_string(r.logical_key_json, '$.WDS')
            )
            """,
        ),
        "zero_unknown_classification_leak": scalar(
            con,
            f"""
            select count(*) from stellar_classification_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and trim(e.classification_raw)='0'
            """,
        ),
        "zero_unknown_measurement_leak": scalar(
            con,
            f"""
            select count(*) from (
              select e.source_record_id,e.value_raw
              from astrometry_distance_evidence e
              union all
              select e.source_record_id,e.value_raw
              from photometry_extinction_evidence e
              union all
              select e.source_record_id,e.value_raw
              from stellar_parameter_evidence e
            ) e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and coalesce(try_cast(e.value_raw as double), 1)=0
            """,
        ),
        "note_reference_without_citation_link": abs(
            scalar(
                con,
                f"""
                select count(*) from evidence_citations c
                join identifier_claim_evidence i
                  on c.evidence_table='identifier_claim_evidence'
                 and c.evidence_id=i.evidence_id
                join source_records r using(source_record_id)
                where r.source_id='{SOURCE_ID}'
                  and c.citation_role='msc_system_note_reference'
                """,
            )
            - 3_482
        ),
    }
    summaries = {
        "source_records_by_table": rows(
            con,
            f"""
            select source_table,count(*) row_count,
              sum(source_duplicate_count) input_row_count
            from source_records where source_id='{SOURCE_ID}'
            group by source_table order by source_table
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
        "relations_by_polarity": rows(
            con,
            f"""
            select evidence_polarity,count(*) claim_count
            from relation_claim_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' group by evidence_polarity
            order by evidence_polarity
            """,
        ),
        "domain_counts": rows(
            con,
            f"""
            select 'astrometry_distance_evidence' evidence_table,count(*) evidence_count
            from astrometry_distance_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            union all
            select 'orbital_solution_evidence',count(*)
            from orbital_solution_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            union all
            select 'photometry_extinction_evidence',count(*)
            from photometry_extinction_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            union all
            select 'relation_claim_evidence',count(*)
            from relation_claim_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            union all
            select 'stellar_classification_evidence',count(*)
            from stellar_classification_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            union all
            select 'stellar_parameter_evidence',count(*)
            from stellar_parameter_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' order by 1
            """,
        ),
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
        "schema_version": "spacegate.msc_scientific_evidence_audit.v1",
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
            / "e4_msc_scope_audit.json"
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
    print(f"MSC scientific evidence audit {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
