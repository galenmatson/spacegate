#!/usr/bin/env python3
"""Audit Gaia DR3 supplementary astrophysical-parameter evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ID = "gaia.dr3.astrophysical_parameters_supp"
RELEASE_ID = "gaia_dr3_1250ly_parameter_sets_v2"
ADAPTER_VERSION = "gaia_dr3_astrophysical_parameters_supp_scientific_evidence_v1"
RAW_SNAPSHOT_ID = "c80bde75b53fb38389c242a2"
TYPED_SNAPSHOT_ID = "969a799b389b53c9b228b3dc"
GAIA_SOLUTION_ID = "1636148068921376768"
FIELD_OCCURRENCES = 354
EXPECTED_TABLE_ROWS = {
    "gaia_dr3_ap_supp_photometric_models_uncertain_distance_supplement_v1": 152_407,
    "gaia_dr3_ap_supp_photometric_models_v2": 6_802_649,
    "gaia_dr3_ap_supp_spectroscopic_models_uncertain_distance_supplement_v1": 16_849,
    "gaia_dr3_ap_supp_spectroscopic_models_v2": 1_047_467,
}
EXPECTED_DOMAIN_COUNTS = {
    "astrometry_distance_evidence": 10_942_232,
    "photometry_extinction_evidence": 66_558_671,
    "stellar_classification_evidence": 905_314,
    "stellar_parameter_evidence": 52_352_445,
    "stellar_parameter_sets": 12_904_333,
}
EXPECTED_STELLAR_SCOPES = {
    "GSP-Phot_A",
    "GSP-Phot_MARCS",
    "GSP-Phot_OB",
    "GSP-Phot_PHOENIX",
    "GSP-Spec_ANN",
    "FLAME_spectroscopic",
}
TOTAL_ROWS = sum(EXPECTED_TABLE_ROWS.values())


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def values_clause(values: dict[str, int]) -> str:
    return ",".join(
        f"('{name}',{count})" for name, count in sorted(values.items())
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
            + values_clause(EXPECTED_TABLE_ROWS)
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
            "binding_status<>'unresolved' or binding_scope<>'star' or "
            "component_scope is not null",
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
        "unexpected_domain_counts": scalar(
            con,
            "with expected(domain,evidence_count) as (values "
            + values_clause(EXPECTED_DOMAIN_COUNTS)
            + "), actual as (select * from (values "
            "('astrometry_distance_evidence',(select count(*) from astrometry_distance_evidence)),"
            "('photometry_extinction_evidence',(select count(*) from photometry_extinction_evidence)),"
            "('stellar_classification_evidence',(select count(*) from stellar_classification_evidence)),"
            "('stellar_parameter_evidence',(select count(*) from stellar_parameter_evidence)),"
            "('stellar_parameter_sets',(select count(*) from stellar_parameter_sets))) "
            "as counts(domain,evidence_count)) select count(*) from ("
            "select * from expected except select * from actual union all "
            "select * from actual except select * from expected)",
        ),
        "missing_or_unexpected_stellar_scopes": scalar(
            con,
            "with expected(evidence_scope) as (values "
            + ",".join(f"('{scope}')" for scope in sorted(EXPECTED_STELLAR_SCOPES))
            + "), actual as (select distinct json_extract_string(quality_json,'$.evidence_scope') "
            "evidence_scope from stellar_parameter_sets) select count(*) from ("
            "select * from expected except select * from actual union all "
            "select * from actual except select * from expected)",
        ),
        "wrong_best_library_lineage": scalar(
            con,
            "select count(*) from source_records where source_table like "
            "'gaia_dr3_ap_supp_photometric_models%' and "
            "json_extract_string(source_context_json,'$.libname_best_gspphot') "
            "not in ('MARCS','PHOENIX','OB','A')",
        ),
        "invalid_interval_endpoints": scalar(
            con,
            "select count(*) from stellar_parameter_evidence where "
            "json_extract_string(quality_json,'$.uncertainty_field_semantics')="
            "'interval_endpoints' and uncertainty_lower>uncertainty_upper",
        )
        + scalar(
            con,
            "select count(*) from astrometry_distance_evidence where "
            "json_extract_string(quality_json,'$.uncertainty_field_semantics')="
            "'interval_endpoints' and uncertainty_lower>uncertainty_upper",
        )
        + scalar(
            con,
            "select count(*) from photometry_extinction_evidence where "
            "json_extract_string(quality_json,'$.uncertainty_field_semantics')="
            "'interval_endpoints' and uncertainty_lower>uncertainty_upper",
        ),
        "unexpected_source_nonbracketing_intervals": abs(
            scalar(
                con,
                "select count(*) from stellar_parameter_evidence e "
                "join source_records r using(source_record_id) where "
                "r.source_table='gaia_dr3_ap_supp_spectroscopic_models_v2' and "
                "e.quantity_key='stellar_luminosity' and "
                "e.normalized_value is not null and "
                "((e.uncertainty_lower is not null and "
                "e.uncertainty_lower>e.normalized_value) or "
                "(e.uncertainty_upper is not null and "
                "e.uncertainty_upper<e.normalized_value))",
            )
            - 2
        )
        + scalar(
            con,
            "select count(*) from stellar_parameter_evidence e "
            "join source_records r using(source_record_id) where "
            "e.normalized_value is not null and "
            "((e.uncertainty_lower is not null and "
            "e.uncertainty_lower>e.normalized_value) or "
            "(e.uncertainty_upper is not null and "
            "e.uncertainty_upper<e.normalized_value)) and not ("
            "r.source_table='gaia_dr3_ap_supp_spectroscopic_models_v2' and "
            "e.quantity_key='stellar_luminosity')",
        ),
        "missing_parameter_set_lineage": scalar(
            con,
            "select count(*) from stellar_parameter_sets where "
            "method is null or trim(method)='' or model is null or trim(model)='' or "
            "json_extract_string(quality_json,'$.parameter_set_kind') is null or "
            "json_extract_string(quality_json,'$.evidence_scope') is null",
        ),
        "orphan_parameter_evidence": scalar(
            con,
            "select count(*) from stellar_parameter_evidence e "
            "left join stellar_parameter_sets s using(parameter_set_id) "
            "where s.parameter_set_id is null",
        ),
        "missing_citations": scalar(
            con, "select case when count(*)=0 then 1 else 0 end from citations"
        )
        + scalar(
            con,
            "select count(*) from citations where source_id<>" + repr(SOURCE_ID),
        ),
        "premature_unrelated_domain_evidence": scalar(
            con,
            "select (select count(*) from spectra_product_index) "
            "+ (select count(*) from variability_activity_rotation_evidence) "
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
        "schema_version": "spacegate.gaia_ap_supp_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": {
            "source_tables": rows(
                con,
                "select source_table,count(*) row_count from source_records "
                "group by source_table order by source_table",
            ),
            "parameter_sets": rows(
                con,
                "select json_extract_string(quality_json,'$.evidence_scope') "
                "evidence_scope,model,count(*) parameter_set_count "
                "from stellar_parameter_sets group by all order by all",
            ),
            "best_photometric_libraries": rows(
                con,
                "select json_extract_string(source_context_json,'$.libname_best_gspphot') "
                "library,count(*) source_count from source_records where source_table "
                "like 'gaia_dr3_ap_supp_photometric_models%' group by 1 order by 1",
            ),
            "domain_counts": rows(
                con,
                "select * from (values "
                "('astrometry_distance_evidence',(select count(*) from astrometry_distance_evidence)),"
                "('photometry_extinction_evidence',(select count(*) from photometry_extinction_evidence)),"
                "('stellar_classification_evidence',(select count(*) from stellar_classification_evidence)),"
                "('stellar_parameter_evidence',(select count(*) from stellar_parameter_evidence)),"
                "('stellar_parameter_sets',(select count(*) from stellar_parameter_sets))) "
                "as counts(domain,evidence_count) order by domain",
            ),
            "bindings": rows(
                con,
                "select binding_scope,component_scope,binding_status,count(*) outcome_count "
                "from object_binding_outcomes group by all order by all",
            ),
            "source_nonbracketing_intervals": rows(
                con,
                "select r.source_table,e.quantity_key,count(*) anomaly_count "
                "from stellar_parameter_evidence e join source_records r "
                "using(source_record_id) where e.normalized_value is not null and "
                "((e.uncertainty_lower is not null and "
                "e.uncertainty_lower>e.normalized_value) or "
                "(e.uncertainty_upper is not null and "
                "e.uncertainty_upper<e.normalized_value)) group by all order by all",
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
        / "reports/evidence_lake_v2/e4_gaia_ap_supp_scientific_evidence_audit.json",
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
        f"Gaia AP supplementary scientific evidence audit {report['status']}: "
        f"{sum(report['checks'].values())} discrepancies"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
