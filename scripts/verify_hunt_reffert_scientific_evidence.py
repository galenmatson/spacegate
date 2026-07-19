#!/usr/bin/env python3
"""Audit bounded Hunt/Reffert cluster and membership evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "clusters.hunt_reffert_2024"
EXPECTED_SOURCE_RECORDS = {
    "hunt_reffert_2024_clusters": 465,
    "hunt_reffert_2024_crossmatch": 451,
    "hunt_reffert_2024_members": 51017,
}


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    source_counts = {
        str(table): int(count)
        for table, count in con.execute(
            """
            select source_table,count(*)
            from source_records where source_id=?
            group by source_table
            """,
            [SOURCE_ID],
        ).fetchall()
    }
    checks = {
        "unexpected_source_record_counts": sum(
            abs(source_counts.get(table, 0) - expected)
            for table, expected in EXPECTED_SOURCE_RECORDS.items()
        )
        + sum(
            count
            for table, count in source_counts.items()
            if table not in EXPECTED_SOURCE_RECORDS
        ),
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
        "selected_clusters_outside_distance_overlap": scalar(
            con,
            f"""
            select count(*) from cluster_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and r.source_table='hunt_reffert_2024_clusters'
              and try_cast(e.parameter_set_raw->>'dist16' as double)>383.245
            """,
        ),
        "cluster_records_without_context": scalar(
            con,
            f"""
            select count(*) from source_records r
            left join cluster_evidence e using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and r.source_table in (
                'hunt_reffert_2024_clusters','hunt_reffert_2024_crossmatch'
              ) and e.source_record_id is null
            """,
        ),
        "member_records_without_membership": scalar(
            con,
            f"""
            select count(*) from source_records r
            left join cluster_membership_evidence e using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and r.source_table='hunt_reffert_2024_members'
              and e.source_record_id is null
            """,
        ),
        "invalid_membership_probability": scalar(
            con,
            f"""
            select count(*) from cluster_membership_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and (
              e.membership_probability is null
              or e.membership_probability<0 or e.membership_probability>1
            )
            """,
        ),
        "membership_without_selected_cluster": scalar(
            con,
            f"""
            select count(*) from cluster_membership_evidence m
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and not exists (
              select 1 from cluster_evidence c
              join source_records cr using(source_record_id)
              where cr.source_id='{SOURCE_ID}'
                and cr.source_table='hunt_reffert_2024_clusters'
                and c.cluster_identity_raw=m.cluster_identity_raw
            )
            """,
        ),
        "crossmatch_without_selected_cluster": scalar(
            con,
            f"""
            select count(*) from cluster_evidence x
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
              and r.source_table='hunt_reffert_2024_crossmatch'
              and not exists (
                select 1 from cluster_evidence c
                join source_records cr using(source_record_id)
                where cr.source_id='{SOURCE_ID}'
                  and cr.source_table='hunt_reffert_2024_clusters'
                  and c.cluster_identity_raw=x.cluster_identity_raw
              )
            """,
        ),
        "membership_without_matching_gaia_claim": scalar(
            con,
            f"""
            select count(*) from cluster_membership_evidence m
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and not exists (
              select 1 from identifier_claim_evidence i
              where i.source_record_id=m.source_record_id
                and i.namespace='gaia_dr3_source_id'
                and i.identifier_normalized=m.member_identity_raw
                and i.component_scope='member'
            )
            """,
        ),
        "membership_without_matching_cluster_claim": scalar(
            con,
            f"""
            select count(*) from cluster_membership_evidence m
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' and not exists (
              select 1 from identifier_claim_evidence i
              where i.source_record_id=m.source_record_id
                and i.namespace='hunt_reffert_cluster_id'
                and i.identifier_normalized=m.cluster_identity_raw
            )
            """,
        ),
        "referenced_evidence_without_citation": scalar(
            con,
            f"""
            with referenced as (
              select 'cluster_evidence' evidence_table,e.evidence_id
              from cluster_evidence e join source_records r using(source_record_id)
              where r.source_id='{SOURCE_ID}' and nullif(trim(e.reference_raw),'') is not null
              union all
              select 'cluster_membership_evidence',e.evidence_id
              from cluster_membership_evidence e join source_records r using(source_record_id)
              where r.source_id='{SOURCE_ID}' and nullif(trim(e.reference_raw),'') is not null
            )
            select count(*) from referenced e
            left join evidence_citations c using(evidence_table,evidence_id)
            where c.evidence_id is null
            """,
        ),
        "relation_or_orbit_promotion": scalar(
            con,
            f"""
            select count(*) from (
              select e.source_record_id from relation_claim_evidence e
              union all
              select e.source_record_id from orbital_solution_evidence e
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
        "cluster_domain_counts": rows(
            con,
            f"""
            select 'cluster_evidence' evidence_table,count(*) evidence_count
            from cluster_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
            union all
            select 'cluster_membership_evidence',count(*)
            from cluster_membership_evidence e join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}' order by 1
            """,
        ),
        "membership_probability": rows(
            con,
            f"""
            select min(membership_probability) minimum,
              avg(membership_probability) mean,
              max(membership_probability) maximum
            from cluster_membership_evidence e
            join source_records r using(source_record_id)
            where r.source_id='{SOURCE_ID}'
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
        "schema_version": "spacegate.hunt_reffert_scientific_evidence_audit.v1",
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
            / "e4_hunt_reffert_scope_audit.json"
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
        f"Hunt/Reffert scientific evidence audit {report['status']}: "
        f"{report['build_id']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
