#!/usr/bin/env python3
"""Audit Gaia DR3 astrophysical-parameter scientific evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "gaia.dr3.astrophysical_parameters"
RELEASE_ID = "gaia_dr3_1250ly_parameter_sets_v2"
ADAPTER_VERSION = "gaia_dr3_astrophysical_parameters_scientific_evidence_v1"
RAW_SNAPSHOT_ID = "1f13c88951b996b95e702913"
TYPED_SNAPSHOT_ID = "3da71b75938d286c44c5a5e0"
GAIA_SOLUTION_ID = "1636148068921376768"
FIELD_OCCURRENCES = 482
EXPECTED_TABLE_ROWS = {
    "gaia_dr3_ap_activity_specialized_uncertain_distance_supplement_v1": 139_530,
    "gaia_dr3_ap_activity_specialized_v2": 6_128_293,
    "gaia_dr3_ap_classifier_uncertain_distance_supplement_v1": 186_875,
    "gaia_dr3_ap_classifier_v2": 28_664_791,
    "gaia_dr3_ap_multiple_oa_uncertain_distance_supplement_v1": 165_130,
    "gaia_dr3_ap_multiple_oa_v2": 7_862_084,
    "gaia_dr3_ap_photometry_flame_uncertain_distance_supplement_v1": 152_407,
    "gaia_dr3_ap_photometry_flame_v2": 6_802_649,
    "gaia_dr3_ap_spectroscopy_uncertain_distance_supplement_v1": 16_530,
    "gaia_dr3_ap_spectroscopy_v2": 1_046_136,
}
TOTAL_ROWS = sum(EXPECTED_TABLE_ROWS.values())


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def expected_table_values() -> str:
    return ",".join(
        f"('{table_name}',{row_count})"
        for table_name, row_count in sorted(EXPECTED_TABLE_ROWS.items())
    )


def audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    checks = {
        "unexpected_source_metadata": abs(
            scalar(con, "select count(*) from evidence_sources") - 1
        )
        + scalar(
            con,
            "select count(*) from evidence_sources where "
            f"source_id<>'{SOURCE_ID}' or release_id<>'{RELEASE_ID}' or "
            f"adapter_version<>'{ADAPTER_VERSION}' or "
            f"raw_snapshot_id<>'{RAW_SNAPSHOT_ID}' or "
            f"typed_snapshot_id<>'{TYPED_SNAPSHOT_ID}'",
        ),
        "unexpected_source_table_counts": scalar(
            con,
            "with expected(source_table,row_count) as (values "
            + expected_table_values()
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
            "object_scope<>'star' or source_duplicate_count<>1",
        ),
        "wrong_logical_identity_shape": scalar(
            con,
            "select count(*) from source_records where "
            "len(json_keys(logical_key_json))<>1 or "
            "json_extract_string(logical_key_json,'$.source_id') is null or "
            "json_extract(logical_key_json,'$.solution_id') is not null",
        ),
        "missing_or_wrong_solution_lineage": scalar(
            con,
            "select count(*) from source_records where "
            "json_extract_string(source_context_json,'$.solution_id') "
            f"is distinct from '{GAIA_SOLUTION_ID}'",
        ),
        "unexpected_identifier_claims": abs(
            scalar(con, "select count(*) from identifier_claim_evidence") - TOTAL_ROWS
        )
        + scalar(
            con,
            "select count(*) from identifier_claim_evidence where "
            "namespace<>'gaia_dr3_source_id' or claim_scope<>'star' or "
            "component_scope is not null or identifier_normalized is null or "
            "identifier_normalized='0'",
        ),
        "solution_id_promoted_to_identity": scalar(
            con,
            "select count(*) from identifier_claim_evidence where "
            "namespace ilike '%solution%'",
        ),
        "identifier_normalization_rejections": scalar(
            con, "select count(*) from identifier_normalization_rejections"
        ),
        "missing_or_duplicate_star_bindings": scalar(
            con,
            "select count(*) from (select r.source_record_id "
            "from source_records r left join object_binding_outcomes b "
            "on b.source_record_id=r.source_record_id and b.binding_scope='star' "
            "and b.component_scope is null group by r.source_record_id "
            "having count(b.binding_outcome_id)<>1)",
        ),
        "unexpected_binding_status_or_scope": scalar(
            con,
            "select count(*) from object_binding_outcomes where "
            "binding_status<>'unresolved' or binding_scope not in ('star','stellar_component')",
        ),
        "component_binding_mismatch": scalar(
            con,
            "with expected as (select distinct source_record_id,component_scope "
            "from stellar_parameter_evidence where component_scope is not null "
            "union select distinct source_record_id,component_scope "
            "from stellar_classification_evidence where component_scope is not null), "
            "actual as (select source_record_id,component_scope "
            "from object_binding_outcomes where binding_scope='stellar_component') "
            "select count(*) from (select * from expected except select * from actual "
            "union all select * from actual except select * from expected)",
        ),
        "unexpected_field_accounting": abs(
            scalar(con, "select count(*) from source_field_dispositions")
            - FIELD_OCCURRENCES
        )
        + scalar(
            con,
            "select count(*) from source_field_dispositions where "
            "mapping_status<>'materialized' or disposition='exclude'",
        ),
        "malformed_probability_bundles": scalar(
            con,
            "select count(*) from stellar_classification_evidence where "
            "classification_raw='probability_bundle' and (probability is not null or "
            "classification_normalized is not null or "
            "json_extract(quality_json,'$.models') is null or "
            "json_extract_string(quality_json,'$.probability_semantics')<>"
            "'source_published_strict_probability_vectors')",
        ),
        "missing_probability_bundle_families": scalar(
            con,
            "with expected(source_table) as (values "
            "('gaia_dr3_ap_classifier_v2'),"
            "('gaia_dr3_ap_classifier_uncertain_distance_supplement_v1'),"
            "('gaia_dr3_ap_activity_specialized_v2'),"
            "('gaia_dr3_ap_activity_specialized_uncertain_distance_supplement_v1')), "
            "actual as (select distinct r.source_table "
            "from stellar_classification_evidence e join source_records r "
            "using(source_record_id) where e.classification_raw='probability_bundle') "
            "select count(*) from expected where source_table not in "
            "(select source_table from actual)",
        ),
        "unexpected_probability_bundle_models": scalar(
            con,
            "select count(*) from stellar_classification_evidence e "
            "join source_records r using(source_record_id) "
            "where classification_raw='probability_bundle' and "
            "((r.source_table like 'gaia_dr3_ap_classifier%' and ("
            "json_extract(quality_json,'$.models.DSC_combmod') is null or "
            "json_extract(quality_json,'$.models.DSC_specmod') is null or "
            "json_extract(quality_json,'$.models.DSC_allosmod') is null or "
            "json_extract(quality_json,'$.models.ESP_ELS') is null)) or "
            "(r.source_table like 'gaia_dr3_ap_activity_specialized%' and ("
            "len(json_keys(json_extract(quality_json,'$.models')))<>1 or "
            "json_extract(quality_json,'$.models.ESP_ELS') is null)))",
        ),
        "invalid_interval_endpoints": scalar(
            con,
            "select count(*) from stellar_parameter_evidence where "
            "json_extract_string(quality_json,'$.uncertainty_field_semantics')="
            "'interval_endpoints' and normalized_value is not null and "
            "((uncertainty_lower is not null and uncertainty_lower>normalized_value) or "
            "(uncertainty_upper is not null and uncertainty_upper<normalized_value) or "
            "(uncertainty_lower is not null and uncertainty_upper is not null "
            "and uncertainty_lower>uncertainty_upper))",
        )
        + scalar(
            con,
            "select count(*) from astrometry_distance_evidence where "
            "json_extract_string(quality_json,'$.uncertainty_field_semantics')="
            "'interval_endpoints' and normalized_value is not null and "
            "((uncertainty_lower is not null and uncertainty_lower>normalized_value) or "
            "(uncertainty_upper is not null and uncertainty_upper<normalized_value) or "
            "(uncertainty_lower is not null and uncertainty_upper is not null "
            "and uncertainty_lower>uncertainty_upper))",
        )
        + scalar(
            con,
            "select count(*) from photometry_extinction_evidence where "
            "json_extract_string(quality_json,'$.uncertainty_field_semantics')="
            "'interval_endpoints' and normalized_value is not null and "
            "((uncertainty_lower is not null and uncertainty_lower>normalized_value) or "
            "(uncertainty_upper is not null and uncertainty_upper<normalized_value) or "
            "(uncertainty_lower is not null and uncertainty_upper is not null "
            "and uncertainty_lower>uncertainty_upper))",
        ),
        "missing_parameter_set_lineage": scalar(
            con,
            "select count(*) from stellar_parameter_sets where "
            "method is null or trim(method)='' or "
            "json_extract_string(quality_json,'$.parameter_set_kind') is null",
        ),
        "orphan_parameter_evidence": scalar(
            con,
            "select count(*) from stellar_parameter_evidence e "
            "left join stellar_parameter_sets s using(parameter_set_id) "
            "where s.parameter_set_id is null",
        ),
        "missing_expected_domain_evidence": scalar(
            con,
            "select (case when (select count(*) from stellar_parameter_evidence)=0 "
            "then 1 else 0 end) + "
            "(case when (select count(*) from stellar_classification_evidence)=0 "
            "then 1 else 0 end) + "
            "(case when (select count(*) from astrometry_distance_evidence)=0 "
            "then 1 else 0 end) + "
            "(case when (select count(*) from photometry_extinction_evidence)=0 "
            "then 1 else 0 end) + "
            "(case when (select count(*) from variability_activity_rotation_evidence)=0 "
            "then 1 else 0 end)",
        ),
        "missing_citations": scalar(con, "select case when count(*)=0 then 1 else 0 end from citations")
        + scalar(
            con,
            "select count(*) from citations where source_id<>" + repr(SOURCE_ID),
        ),
        "premature_unrelated_domain_evidence": scalar(
            con,
            "select (select count(*) from spectra_product_index) "
            "+ (select count(*) from relation_claim_evidence) "
            "+ (select count(*) from orbital_solution_evidence) "
            "+ (select count(*) from cluster_evidence) "
            "+ (select count(*) from planet_parameter_evidence) "
            "+ (select count(*) from planet_lifecycle_evidence) "
            "+ (select count(*) from compact_object_evidence) "
            "+ (select count(*) from extended_object_evidence)",
        ),
    }
    return {
        "schema_version": "spacegate.gaia_ap_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": {
            "source_tables": rows(
                con,
                "select source_table,count(*) row_count from source_records "
                "group by source_table order by source_table",
            ),
            "parameter_set_kinds": rows(
                con,
                "select json_extract_string(quality_json,'$.parameter_set_kind') "
                "parameter_set_kind,count(*) parameter_set_count "
                "from stellar_parameter_sets group by 1 order by 1",
            ),
            "classifications": rows(
                con,
                "select classification_scheme,classification_raw,count(*) evidence_count "
                "from stellar_classification_evidence group by all order by all",
            ),
            "domain_counts": rows(
                con,
                "select * from (values "
                "('astrometry_distance_evidence',(select count(*) from astrometry_distance_evidence)),"
                "('photometry_extinction_evidence',(select count(*) from photometry_extinction_evidence)),"
                "('stellar_classification_evidence',(select count(*) from stellar_classification_evidence)),"
                "('stellar_parameter_evidence',(select count(*) from stellar_parameter_evidence)),"
                "('variability_activity_rotation_evidence',(select count(*) from variability_activity_rotation_evidence))) "
                "as counts(domain,evidence_count) order by domain",
            ),
            "bindings": rows(
                con,
                "select binding_scope,component_scope,binding_status,count(*) outcome_count "
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
        default=DEFAULT_STATE
        / "reports/evidence_lake_v2/e4_gaia_ap_scientific_evidence_audit.json",
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
        f"Gaia AP scientific evidence audit {report['status']}: "
        f"{sum(report['checks'].values())} discrepancies"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
