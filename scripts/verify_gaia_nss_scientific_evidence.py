#!/usr/bin/env python3
"""Audit Gaia DR3 NSS hard-envelope and uncertainty-supplement evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "gaia.dr3.non_single_star"
RELEASE_ID = "gaia_dr3_1250ly_nss_v2"
ADAPTER_VERSION = "gaia_dr3_nss_scientific_evidence_v2"
HARD_TABLE = "gaia_dr3_nss_two_body_orbit_full_v2"
SUPPLEMENT_TABLE = "gaia_dr3_nss_two_body_orbit_uncertain_distance_supplement_v1"
HARD_ROWS = 85_724
SUPPLEMENT_ROWS = 1_351
TOTAL_ROWS = HARD_ROWS + SUPPLEMENT_ROWS
FIELDS_PER_TABLE = 77
PARAMETER_FIELDS = 56
QUALITY_FIELDS = 18
REFERENCE = (
    "https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/"
    "chap_datamodel/sec_dm_non--single_stars/ssec_dm_nss_two_body_orbit.html"
)


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    expected_tables = f"values ('{HARD_TABLE}', {HARD_ROWS}), ('{SUPPLEMENT_TABLE}', {SUPPLEMENT_ROWS})"
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
        "unexpected_source_table_counts": scalar(
            con,
            "with expected(source_table,row_count) as ("
            + expected_tables
            + "), actual as (select source_table,count(*) row_count "
            "from source_records group by source_table) "
            "select count(*) from (select * from expected except select * from actual "
            "union all select * from actual except select * from expected)",
        ),
        "unexpected_source_record_count": abs(
            scalar(con, "select count(*) from source_records") - TOTAL_ROWS
        ),
        "wrong_scope_or_duplicate_source_rows": scalar(
            con,
            "select count(*) from source_records where "
            "object_scope<>'nss_orbit_solution' or source_duplicate_count<>1",
        ),
        "nonempty_source_context": scalar(
            con, "select count(*) from source_records where source_context_json<>'{}'"
        ),
        "unexpected_identifier_counts": abs(
            scalar(
                con,
                "select count(*) from identifier_claim_evidence "
                "where namespace='gaia_dr3_source_id' and claim_scope='star'",
            )
            - TOTAL_ROWS
        )
        + abs(
            scalar(
                con,
                "select count(*) from identifier_claim_evidence "
                "where namespace='gaia_dr3_nss_solution_id' "
                "and claim_scope='orbit_solution'",
            )
            - TOTAL_ROWS
        )
        + abs(
            scalar(con, "select count(*) from identifier_claim_evidence")
            - 2 * TOTAL_ROWS
        ),
        "identifier_normalization_rejections": scalar(
            con, "select count(*) from identifier_normalization_rejections"
        ),
        "duplicate_or_null_identifiers": scalar(
            con,
            "select count(*) from identifier_claim_evidence where "
            "identifier_normalized is null or identifier_normalized='0'",
        ),
        "unexpected_binding_outcomes": abs(
            scalar(con, "select count(*) from object_binding_outcomes")
            - 3 * TOTAL_ROWS
        )
        + scalar(
            con,
            "select count(*) from object_binding_outcomes where "
            "binding_status<>'unresolved' or binding_scope not in "
            "('nss_orbit_solution','orbit_solution','star')",
        ),
        "unexpected_orbit_count": abs(
            scalar(con, "select count(*) from orbital_solution_evidence") - TOTAL_ROWS
        ),
        "duplicate_orbit_solution_keys": scalar(
            con,
            "select count(*)-count(distinct solution_key) "
            "from orbital_solution_evidence",
        ),
        "premature_relation_links": scalar(
            con,
            "select count(*) from orbital_solution_evidence "
            "where relation_claim_id is not null",
        ),
        "wrong_orbit_lineage": scalar(
            con,
            "select count(*) from orbital_solution_evidence where "
            "frame_raw<>'ICRS J2016.0' or "
            "method<>'gaia_dr3_nss_two_body_orbit' or "
            f"reference_raw<>'{REFERENCE}' or "
            "normalization_version<>'gaia_dr3_source_native_lexical_v1' or "
            "model is null or trim(model)=''",
        ),
        "incomplete_parameter_or_quality_shape": scalar(
            con,
            "select count(*) from orbital_solution_evidence where "
            f"len(json_keys(parameter_set_raw))<>{PARAMETER_FIELDS} or "
            f"len(json_keys(quality_json))<>{QUALITY_FIELDS}",
        ),
        "unexpected_field_accounting": abs(
            scalar(con, "select count(*) from source_field_dispositions")
            - 2 * FIELDS_PER_TABLE
        )
        + scalar(
            con,
            "select count(*) from source_field_dispositions where "
            "mapping_status<>'materialized' or disposition='exclude'",
        ),
        "unexpected_citations": abs(
            scalar(con, "select count(*) from citations") - 1
        )
        + scalar(
            con,
            "select count(*) from citations where "
            f"source_id<>'{SOURCE_ID}' or citation_text_raw<>'{REFERENCE}'",
        ),
        "unexpected_evidence_citations": abs(
            scalar(con, "select count(*) from evidence_citations") - TOTAL_ROWS
        ),
        "premature_other_domain_evidence": scalar(
            con,
            "select (select count(*) from stellar_parameter_evidence) "
            "+ (select count(*) from stellar_classification_evidence) "
            "+ (select count(*) from astrometry_distance_evidence) "
            "+ (select count(*) from astrometry_distance_evidence_bundles) "
            "+ (select count(*) from photometry_extinction_evidence) "
            "+ (select count(*) from variability_activity_rotation_evidence) "
            "+ (select count(*) from relation_claim_evidence) "
            "+ (select count(*) from cluster_evidence) "
            "+ (select count(*) from planet_parameter_evidence) "
            "+ (select count(*) from compact_object_evidence) "
            "+ (select count(*) from extended_object_evidence)",
        ),
    }
    return {
        "schema_version": "spacegate.gaia_nss_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": {
            "source_tables": rows(
                con,
                "select source_table,count(*) row_count from source_records "
                "group by source_table order by source_table",
            ),
            "models": rows(
                con,
                "select model,count(*) evidence_count from orbital_solution_evidence "
                "group by model order by model",
            ),
            "bindings": rows(
                con,
                "select binding_scope,binding_status,count(*) outcome_count "
                "from object_binding_outcomes group by all order by all",
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--report",
        type=Path,
        default=(
            DEFAULT_STATE
            / "reports/evidence_lake_v2/e4_gaia_nss_scientific_evidence_audit.json"
        ),
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    con = duckdb.connect(str(database), read_only=True)
    try:
        report = audit(con)
    finally:
        con.close()
    report["build_id"] = str(manifest["build_id"])
    report["database"] = str(database)
    write_json(args.report, report)
    print(
        f"Gaia NSS scientific evidence audit {report['status']}: "
        f"{sum(report['checks'].values())} discrepancies"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
