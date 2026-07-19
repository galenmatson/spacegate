#!/usr/bin/env python3
"""Audit the GCVS/NSV scientific-evidence adapter and scope boundaries."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "classification.gcvs"


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    checks = {
        "unaccounted_or_pending_fields": scalar(
            con,
            f"""
            select count(*) from source_field_dispositions
            where source_id='{SOURCE_ID}' and mapping_status<>'materialized'
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
        "duplicate_citation_reference_keys": scalar(
            con,
            f"""
            select count(*) from (
              select source_reference_key from citations
              where source_id='{SOURCE_ID}'
              group by source_reference_key having count(*)<>1
            )
            """,
        ),
        "referenced_evidence_without_citation": scalar(
            con,
            f"""
            with referenced as (
              select 'astrometry_distance_evidence' evidence_table,evidence_id
              from astrometry_distance_evidence e join source_records r using(source_record_id)
              where r.source_id='{SOURCE_ID}' and nullif(trim(e.reference_raw),'') is not null
              union all
              select 'stellar_classification_evidence',evidence_id
              from stellar_classification_evidence e join source_records r using(source_record_id)
              where r.source_id='{SOURCE_ID}' and nullif(trim(e.reference_raw),'') is not null
              union all
              select 'variability_activity_rotation_evidence',evidence_id
              from variability_activity_rotation_evidence e join source_records r using(source_record_id)
              where r.source_id='{SOURCE_ID}' and nullif(trim(e.reference_raw),'') is not null
            )
            select count(*) from referenced e
            left join evidence_citations c using(evidence_table,evidence_id)
            where c.evidence_id is null
            """,
        ),
        "invalid_normalized_coordinates": scalar(
            con,
            f"""
            select count(*) from astrometry_distance_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and e.quantity_key in ('right_ascension','declination')
              and (e.bound_semantics<>'measurement' or e.normalized_value is null)
            """,
        ),
        "coordinates_outside_physical_range": scalar(
            con,
            f"""
            select count(*) from astrometry_distance_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and (
              (e.quantity_key='right_ascension' and (e.normalized_value<0 or e.normalized_value>=360))
              or (e.quantity_key='declination' and abs(e.normalized_value)>90)
            )
            """,
        ),
        "lexical_variability_values_with_numeric_normalization": scalar(
            con,
            f"""
            select count(*) from variability_activity_rotation_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and e.normalized_value is not null
              and e.evidence_kind in (
                'variability_classification','catalog_object_status',
                'observation_document_reference','suspected_variable_status',
                'identity_quality','catalog_identity_transition'
              )
            """,
        ),
        "variability_class_leaked_into_stellar_scheme": scalar(
            con,
            f"""
            select count(*) from stellar_classification_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and (
              e.classification_scheme<>'spectral_type'
              or e.method not in (
                'gcvs_compiled_spectral_classification',
                'nsv_compiled_spectral_classification',
                'nsv_henry_draper_spectral_classification'
              )
            )
            """,
        ),
        "suffixed_nsv_records_claiming_base_identity": scalar(
            con,
            f"""
            select count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and r.source_table='gcvs_suspected_variables'
              and nullif(r.logical_key_json->>'m_NSV','') is not null
              and i.namespace in ('nsv_numeric_key','nsv_designation')
              and i.identifier_normalized=(
                'NSV ' || (r.logical_key_json->>'NSV')
              )
            """,
        ),
        "component_gcvs_records_claiming_unsuffixed_numeric_key": scalar(
            con,
            f"""
            select count(*) from identifier_claim_evidence i
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and r.source_table='gcvs_catalog'
              and nullif(r.logical_key_json->>'m_VarNum','') is not null
              and i.namespace='gcvs_numeric_key'
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
            select namespace,count(*) claim_count,count(distinct identifier_normalized) distinct_value_count
            from identifier_claim_evidence i join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' group by namespace order by namespace
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
        "coordinate_evidence": rows(
            con,
            f"""
            select r.source_table,e.quantity_key,e.frame_raw,count(*) evidence_count,
              count(*) filter(where e.quality_json->>'embedded_degree_sign'='true') embedded_sign_count
            from astrometry_distance_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and e.quantity_key in ('right_ascension','declination')
            group by r.source_table,e.quantity_key,e.frame_raw order by 1,2,3
            """,
        ),
        "domain_counts": rows(
            con,
            f"""
            select 'astrometry_distance_evidence' evidence_table,count(*) evidence_count
            from astrometry_distance_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            union all
            select 'stellar_classification_evidence',count(*)
            from stellar_classification_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            union all
            select 'variability_activity_rotation_evidence',count(*)
            from variability_activity_rotation_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            order by 1
            """,
        ),
        "citation_catalog": rows(
            con,
            f"""
            select count(*) citation_count,
              count(*) filter(where parsed_json->'source_context'->>'aggregation'=
                'repeated_reference_key_lines_v1') aggregated_reference_count,
              max(try_cast(parsed_json->'source_context'->>'line_count' as integer)) max_reference_lines
            from citations where source_id='{SOURCE_ID}'
            """,
        )[0],
    }
    return {
        "schema_version": "spacegate.gcvs_scientific_evidence_audit.v1",
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
        default=DEFAULT_STATE / "reports" / "evidence_lake_v2" / "e4_gcvs_scope_audit.json",
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    with duckdb.connect(str(database), read_only=True) as con:
        report = audit(con)
    report["build_id"] = str(manifest["build_id"])
    report["database"] = str(database)
    write_json(args.report, report)
    print(f"GCVS scientific evidence audit {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
