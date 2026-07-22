#!/usr/bin/env python3
"""Audit Cantat-Gaudin cluster and DR2 membership evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "clusters.cantat_gaudin_2020"
EXPECTED_SOURCE_RECORDS = {
    "cantat_gaudin_2020_table1": 2017,
    "cantat_gaudin_2020_members": 234128,
    "cantat_gaudin_2020_readme": 172,
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
            "select source_table,count(*) from source_records where source_id=? "
            "group by source_table",
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
            f"select count(*) from source_field_dispositions "
            f"where source_id='{SOURCE_ID}' and mapping_status<>'materialized'",
        ),
        "source_records_without_binding_outcome": scalar(
            con,
            f"select count(*) from source_records r left join object_binding_outcomes b "
            f"using(source_record_id) where r.source_id='{SOURCE_ID}' "
            "and b.source_record_id is null",
        ),
        "premature_non_unresolved_bindings": scalar(
            con,
            f"select count(*) from object_binding_outcomes b join source_records r "
            f"using(source_record_id) where r.source_id='{SOURCE_ID}' "
            "and b.binding_status<>'unresolved'",
        ),
        "cluster_records_without_context": scalar(
            con,
            f"select count(*) from source_records r left join cluster_evidence e "
            f"using(source_record_id) where r.source_id='{SOURCE_ID}' "
            "and r.source_table='cantat_gaudin_2020_table1' "
            "and e.source_record_id is null",
        ),
        "member_records_without_membership": scalar(
            con,
            f"select count(*) from source_records r left join cluster_membership_evidence e "
            f"using(source_record_id) where r.source_id='{SOURCE_ID}' "
            "and r.source_table='cantat_gaudin_2020_members' "
            "and e.source_record_id is null",
        ),
        "invalid_membership_probability": scalar(
            con,
            f"select count(*) from cluster_membership_evidence e join source_records r "
            f"using(source_record_id) where r.source_id='{SOURCE_ID}' and "
            "(e.membership_probability is null or e.membership_probability<0 "
            "or e.membership_probability>1)",
        ),
        "membership_without_cluster_context": scalar(
            con,
            f"select count(*) from cluster_membership_evidence m join source_records r "
            f"using(source_record_id) where r.source_id='{SOURCE_ID}' and not exists ("
            "select 1 from cluster_evidence c join source_records cr using(source_record_id) "
            f"where cr.source_id='{SOURCE_ID}' and "
            "cr.source_table='cantat_gaudin_2020_table1' "
            "and c.cluster_identity_raw=m.cluster_identity_raw)",
        ),
        "membership_without_gaia_dr2_claim": scalar(
            con,
            f"select count(*) from cluster_membership_evidence m join source_records r "
            f"using(source_record_id) where r.source_id='{SOURCE_ID}' and not exists ("
            "select 1 from identifier_claim_evidence i "
            "where i.source_record_id=m.source_record_id "
            "and i.namespace='gaia_dr2_source_id' "
            "and i.identifier_normalized=m.member_identity_raw "
            "and i.component_scope='member')",
        ),
        "referenced_evidence_without_citation": scalar(
            con,
            f"with referenced as (select 'cluster_evidence' evidence_table,e.evidence_id "
            "from cluster_evidence e join source_records r using(source_record_id) "
            f"where r.source_id='{SOURCE_ID}' union all select "
            "'cluster_membership_evidence',e.evidence_id from cluster_membership_evidence e "
            f"join source_records r using(source_record_id) where r.source_id='{SOURCE_ID}') "
            "select count(*) from referenced e left join evidence_citations c "
            "using(evidence_table,evidence_id) where c.evidence_id is null",
        ),
        "relation_or_orbit_promotion": scalar(
            con,
            f"select count(*) from (select source_record_id from relation_claim_evidence "
            "union all select source_record_id from orbital_solution_evidence) e "
            f"join source_records r using(source_record_id) where r.source_id='{SOURCE_ID}'",
        ),
    }
    summaries = {
        "source_records_by_table": [
            {"source_table": table, "row_count": count}
            for table, count in sorted(source_counts.items())
        ],
        "cluster_domain_counts": rows(
            con,
            f"select 'cluster_evidence' evidence_table,count(*) evidence_count "
            "from cluster_evidence e join source_records r using(source_record_id) "
            f"where r.source_id='{SOURCE_ID}' union all select "
            "'cluster_membership_evidence',count(*) from cluster_membership_evidence e "
            f"join source_records r using(source_record_id) where r.source_id='{SOURCE_ID}' "
            "order by 1",
        ),
        "membership_probability": rows(
            con,
            f"select min(membership_probability) minimum,avg(membership_probability) mean,"
            "max(membership_probability) maximum from cluster_membership_evidence e "
            f"join source_records r using(source_record_id) where r.source_id='{SOURCE_ID}'",
        )[0],
        "pleiades_cluster_context": rows(
            con,
            f"select e.cluster_identity_raw,e.parameter_set_raw->>'DistPc' distance_pc,"
            "e.parameter_set_raw->>'RAdeg' ra_deg,e.parameter_set_raw->>'DEdeg' dec_deg "
            "from cluster_evidence e join source_records r using(source_record_id) "
            f"where r.source_id='{SOURCE_ID}' and e.cluster_identity_raw='Melotte_22'",
        ),
    }
    return {
        "schema_version": "spacegate.cantat_gaudin_scientific_evidence_audit.v1",
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
        default=DEFAULT_STATE
        / "reports/evidence_lake_v2/e4_cantat_gaudin_scope_audit.json",
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    with duckdb.connect(str(database), read_only=True) as con:
        report = audit(con)
    report["build_id"] = str(manifest["build_id"])
    report["database"] = str(database)
    write_json(args.report, report)
    print(f"Cantat-Gaudin scientific evidence audit {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
