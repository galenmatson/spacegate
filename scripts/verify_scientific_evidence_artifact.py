#!/usr/bin/env python3
"""Audit a compiled scientific-evidence DuckDB artifact."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import (
    DEFAULT_STATE,
    EVIDENCE_REFERENCE_TABLES,
    load_json,
    sql_string,
    write_json,
)


def scalar_count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def audit_evidence(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    checks = {
        "blank_identifier_claims": scalar_count(
            con,
            "select count(*) from identifier_claim_evidence "
            "where nullif(trim(identifier_normalized), '') is null",
        ),
        "malformed_identifier_normalization_rejections": scalar_count(
            con,
            "select count(*) from identifier_normalization_rejections where "
            "nullif(trim(identifier_raw), '') is null or "
            "nullif(trim(source_field), '') is null or "
            "nullif(trim(requested_namespace), '') is null or "
            "nullif(trim(normalization), '') is null or "
            "nullif(trim(reason), '') is null",
        ),
        "gaia_zero_sentinel_identifiers": scalar_count(
            con,
            "select count(*) from identifier_claim_evidence "
            "where namespace in ('gaia_dr2_source_id','gaia_edr3_source_id',"
            "'gaia_dr3_source_id') and identifier_normalized='0'",
        ),
        "relation_endpoints_without_identifier_claims": scalar_count(
            con,
            """
            with endpoints as (
              select evidence_id, source_record_id, 'left' endpoint_side,
                left_identity_namespace endpoint_namespace,
                left_identity_raw endpoint_raw,
                left_component_scope endpoint_scope
              from relation_claim_evidence
              union all
              select evidence_id, source_record_id, 'right',
                right_identity_namespace, right_identity_raw,
                right_component_scope
              from relation_claim_evidence
            )
            select count(distinct e.evidence_id)
            from endpoints e
            left join identifier_claim_evidence i
              on i.source_record_id=e.source_record_id
             and i.namespace=e.endpoint_namespace
             and i.identifier_normalized=e.endpoint_raw
             and i.component_scope is not distinct from e.endpoint_scope
            where i.evidence_id is null
            """,
        ),
        "relation_endpoints_without_binding_scopes": scalar_count(
            con,
            """
            with endpoints as (
              select evidence_id, source_record_id,
                left_identity_namespace endpoint_namespace,
                left_identity_raw endpoint_raw,
                left_component_scope endpoint_scope
              from relation_claim_evidence
              union all
              select evidence_id, source_record_id,
                right_identity_namespace, right_identity_raw,
                right_component_scope
              from relation_claim_evidence
            ), endpoint_bindings as (
              select e.evidence_id, e.endpoint_namespace, e.endpoint_raw,
                count(b.binding_outcome_id) binding_count
              from endpoints e
              left join identifier_claim_evidence i
                on i.source_record_id=e.source_record_id
               and i.namespace=e.endpoint_namespace
               and i.identifier_normalized=e.endpoint_raw
               and i.component_scope is not distinct from e.endpoint_scope
              left join object_binding_outcomes b
                on b.source_record_id=e.source_record_id
               and b.binding_scope=i.claim_scope
               and b.component_scope is not distinct from i.component_scope
              group by e.evidence_id, e.endpoint_namespace, e.endpoint_raw
            )
            select count(distinct evidence_id) from endpoint_bindings
            where binding_count=0
            """,
        ),
        "strict_probabilities_outside_unit_interval": scalar_count(
            con,
            "select count(*) from relation_claim_evidence "
            "where probability is not null and (probability<0 or probability>1)",
        ),
        "strict_probabilities_without_semantics": scalar_count(
            con,
            "select count(*) from relation_claim_evidence "
            "where probability is not null "
            "and nullif(trim(probability_semantics), '') is null",
        ),
        "confidence_statistics_without_semantics": scalar_count(
            con,
            "select count(*) from relation_claim_evidence "
            "where confidence_statistic_value is not null and ("
            "nullif(trim(confidence_statistic_key), '') is null or "
            "nullif(trim(confidence_statistic_value_raw), '') is null or "
            "nullif(trim(confidence_statistic_semantics), '') is null)",
        ),
        "approximate_statistics_promoted_to_strict_probability": scalar_count(
            con,
            "select count(*) from relation_claim_evidence "
            "where probability is not null and "
            "lower(coalesce(confidence_statistic_semantics, '')) like '%not strictly%probability%'",
        ),
        "relation_evidence_without_citations": scalar_count(
            con,
            """
            select count(*)
            from relation_claim_evidence r
            left join evidence_citations c
              on c.evidence_table='relation_claim_evidence'
             and c.evidence_id=r.evidence_id
            where nullif(trim(r.reference_raw), '') is not null
              and c.evidence_id is null
            """,
        ),
        "empty_orbital_solution_parameter_sets": scalar_count(
            con,
            "select count(*) from orbital_solution_evidence "
            "where parameter_set_raw is null or parameter_set_raw::varchar='{}'",
        ),
        "empty_astrometry_measurement_bundles": scalar_count(
            con,
            "select count(*) from astrometry_distance_evidence_bundles "
            "where len(measurements)=0",
        ),
        "duplicate_astrometry_bundle_measurement_ids": scalar_count(
            con,
            "select count(*)-count(distinct measurement.evidence_id) "
            "from astrometry_distance_evidence_bundles b, "
            "unnest(b.measurements) as nested(measurement)",
        ),
        "blank_astrometry_bundle_measurement_ids": scalar_count(
            con,
            "select count(*) from astrometry_distance_evidence_bundles b, "
            "unnest(b.measurements) as nested(measurement) "
            "where nullif(trim(measurement.evidence_id), '') is null",
        ),
        "orphan_orbital_solution_relations": scalar_count(
            con,
            "select count(*) from orbital_solution_evidence o "
            "left join relation_claim_evidence r "
            "on r.evidence_id=o.relation_claim_id "
            "where o.relation_claim_id is not null and r.evidence_id is null",
        ),
        "orbital_solution_evidence_without_citations": scalar_count(
            con,
            """
            select count(*)
            from orbital_solution_evidence o
            left join evidence_citations c
              on c.evidence_table='orbital_solution_evidence'
             and c.evidence_id=o.evidence_id
            where nullif(trim(o.reference_raw), '') is not null
              and c.evidence_id is null
            """,
        ),
        "empty_extended_object_geometry": scalar_count(
            con,
            "select count(*) from extended_object_evidence "
            "where geometry_raw is null or geometry_raw::varchar='{}'",
        ),
        "empty_compact_object_parameter_sets": scalar_count(
            con,
            "select count(*) from compact_object_evidence "
            "where parameter_set_raw is null or parameter_set_raw::varchar='{}'",
        ),
        "orphan_planet_parameter_evidence": scalar_count(
            con,
            "select count(*) from planet_parameter_evidence e "
            "left join planet_parameter_sets s using (parameter_set_id) "
            "where s.parameter_set_id is null",
        ),
        "orphan_stellar_parameter_evidence": scalar_count(
            con,
            "select count(*) from stellar_parameter_evidence e "
            "left join stellar_parameter_sets s using (parameter_set_id) "
            "where s.parameter_set_id is null",
        ),
        "stellar_parameter_component_scope_mismatch": scalar_count(
            con,
            "select count(*) from stellar_parameter_evidence e "
            "join stellar_parameter_sets s using (parameter_set_id) "
            "where e.component_scope is distinct from s.component_scope",
        ),
        "component_scoped_evidence_without_binding_scope": scalar_count(
            con,
            """
            with scoped as (
              select source_record_id, component_scope
              from stellar_parameter_evidence
              where component_scope is not null
              union
              select source_record_id, component_scope
              from stellar_classification_evidence
              where component_scope is not null
            )
            select count(*)
            from scoped s
            left join object_binding_outcomes b
              on b.source_record_id=s.source_record_id
             and b.binding_scope='stellar_component'
             and b.component_scope=s.component_scope
            where b.binding_outcome_id is null
            """,
        ),
        "empty_planet_parameter_sets": scalar_count(
            con,
            "select count(*) from planet_parameter_sets s "
            "left join planet_parameter_evidence e using (parameter_set_id) "
            "where e.parameter_set_id is null",
        ),
        "empty_stellar_parameter_sets": scalar_count(
            con,
            "select count(*) from stellar_parameter_sets s "
            "left join stellar_parameter_evidence e using (parameter_set_id) "
            "where e.parameter_set_id is null",
        ),
    }
    uncertainty_tables = [
        "stellar_parameter_evidence",
        "astrometry_distance_evidence",
        "photometry_extinction_evidence",
        "variability_activity_rotation_evidence",
        "planet_parameter_evidence",
        "transit_observation_evidence",
        "radial_velocity_evidence",
    ]
    checks["negative_uncertainty_magnitudes"] = sum(
        scalar_count(
            con,
            f"select count(*) from {table} where uncertainty_lower<0 or uncertainty_upper<0",
        )
        for table in uncertainty_tables
    ) + scalar_count(
        con,
        "select count(*) from astrometry_distance_evidence_bundles b, "
        "unnest(b.measurements) as nested(measurement) "
        "where measurement.uncertainty_lower<0 or measurement.uncertainty_upper<0",
    )
    checks["referenced_evidence_without_citations"] = sum(
        scalar_count(
            con,
            f"select count(*) from {table} e "
            "left join evidence_citations c "
            f"on c.evidence_table='{table}' and c.evidence_id=e.evidence_id "
            "where nullif(trim(e.reference_raw), '') is not null "
            "and c.evidence_id is null",
        )
        for table in sorted(EVIDENCE_REFERENCE_TABLES)
    ) + scalar_count(
        con,
        "select count(*) from astrometry_distance_evidence_bundles b, "
        "unnest(b.measurements) as nested(measurement) "
        "left join evidence_citations c "
        "on c.evidence_table='astrometry_distance_evidence_bundles' "
        "and c.evidence_id=measurement.evidence_id "
        "where nullif(trim(measurement.reference_raw), '') is not null "
        "and c.evidence_id is null",
    )

    source_record_tables = [
        str(row[0])
        for row in con.execute(
            "select table_name from information_schema.columns "
            "where table_schema='main' and column_name='source_record_id' "
            "and table_name<>'source_records' order by table_name"
        ).fetchall()
    ]
    orphan_counts = {
        table: scalar_count(
            con,
            f"select count(*) from {table} e left join source_records r "
            "using (source_record_id) where r.source_record_id is null",
        )
        for table in source_record_tables
    }
    checks["orphan_source_record_references"] = sum(orphan_counts.values())
    relation_summary = {
        "rows": scalar_count(con, "select count(*) from relation_claim_evidence"),
        "strict_probabilities": scalar_count(
            con,
            "select count(*) from relation_claim_evidence where probability is not null",
        ),
        "confidence_statistics": scalar_count(
            con,
            "select count(*) from relation_claim_evidence "
            "where confidence_statistic_value is not null",
        ),
        "confidence_statistic_above_one": scalar_count(
            con,
            "select count(*) from relation_claim_evidence "
            "where confidence_statistic_value>1",
        ),
    }
    return {
        "schema_version": "spacegate.scientific_evidence_artifact_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "orphan_source_record_counts_by_table": orphan_counts,
        "relation_summary": relation_summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument(
        "--temp-directory",
        type=Path,
        default=(
            Path(os.environ["SPACEGATE_E4_TEMP_DIRECTORY"])
            if os.environ.get("SPACEGATE_E4_TEMP_DIRECTORY")
            else None
        ),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_STATE
        / "reports"
        / "evidence_lake_v2"
        / "e4_scientific_evidence_artifact_audit.json",
    )
    args = parser.parse_args()
    if bool(args.database) == bool(args.manifest):
        parser.error("provide exactly one of --database or --manifest")
    if args.manifest:
        manifest = load_json(args.manifest)
        database = args.manifest.parent / str(manifest["database"])
        build_id = str(manifest["build_id"])
    else:
        database = args.database
        build_id = database.parent.name
    audit_temporary = None
    if args.temp_directory is not None:
        args.temp_directory.mkdir(parents=True, exist_ok=True)
        audit_temporary = Path(
            tempfile.mkdtemp(prefix=f"artifact-audit-{build_id}.", dir=args.temp_directory)
        )
    try:
        con = duckdb.connect(str(database), read_only=True)
        if audit_temporary is not None:
            con.execute(f"set temp_directory={sql_string(str(audit_temporary))}")
        con.execute(f"set memory_limit={sql_string(str(args.memory_limit))}")
        con.execute(f"set threads={max(1, args.threads)}")
        report = audit_evidence(con)
    finally:
        if "con" in locals():
            con.close()
        if audit_temporary is not None:
            shutil.rmtree(audit_temporary, ignore_errors=True)
    report["build_id"] = build_id
    report["database"] = str(database)
    report["threads"] = max(1, args.threads)
    report["memory_limit"] = str(args.memory_limit)
    report["temporary_storage_policy"] = (
        "external_operator_scratch" if args.temp_directory is not None else "duckdb_default"
    )
    report["temporary_storage_removed"] = (
        audit_temporary is None or not audit_temporary.exists()
    )
    write_json(args.report, report)
    print(f"scientific evidence artifact audit {report['status']}: {build_id}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
