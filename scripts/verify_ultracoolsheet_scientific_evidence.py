#!/usr/bin/env python3
"""Audit the UltracoolSheet scientific evidence checkpoint."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


EXPECTED_IDENTIFIERS = {
    "gaia_dr2_source_id": 2_040,
    "gaia_dr3_source_id": 2_077,
    "gucds_shortname": 3_299,
    "panstarrs1_designation": 2_976,
    "simbad_preferred_designation": 3_827,
    "simpledb_object_name": 3_079,
    "source_mko_designation": 1_284,
    "source_search_designation": 3_828,
    "twomass_designation": 2_832,
    "ultracoolsheet_name": 3_890,
    "wise_designation": 3_709,
}
EXPECTED_CLASSIFICATIONS = {
    "infrared_gravity_class": 640,
    "infrared_spectral_type": 2_832,
    "optical_gravity_class": 93,
    "optical_spectral_type": 1_786,
    "ultracoolsheet_age_category": 3_890,
    "ultracoolsheet_literature_flag": 1_032,
    "ultracoolsheet_youth_evidence": 614,
}
EXPECTED_STELLAR_PARAMETERS = {
    "absolute_spectral_numeric_code": 3_850,
    "infrared_spectral_numeric_code": 2_828,
    "maintainer_age_distribution": 3_890,
    "maintainer_selected_age": 3_890,
    "optical_spectral_numeric_code": 1_783,
    "photometric_distance_spectral_numeric_code": 3_768,
    "selected_spectral_numeric_code": 3_850,
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
    identifier_counts = {
        str(namespace): int(count)
        for namespace, count in con.execute(
            "select namespace,count(*) from identifier_claim_evidence group by namespace"
        ).fetchall()
    }
    classification_counts = {
        str(scheme): int(count)
        for scheme, count in con.execute(
            "select classification_scheme,count(*) "
            "from stellar_classification_evidence group by classification_scheme"
        ).fetchall()
    }
    stellar_parameter_counts = {
        str(quantity): int(count)
        for quantity, count in con.execute(
            "select quantity_key,count(*) from stellar_parameter_evidence "
            "group by quantity_key"
        ).fetchall()
    }
    nonfinite = scalar(
        con,
        "select "
        "(select count(*) from astrometry_distance_evidence where "
        "not isfinite(normalized_value) or not isfinite(uncertainty_lower) "
        "or not isfinite(uncertainty_upper)) + "
        "(select count(*) from photometry_extinction_evidence where "
        "not isfinite(normalized_value) or not isfinite(uncertainty_lower) "
        "or not isfinite(uncertainty_upper)) + "
        "(select count(*) from stellar_parameter_evidence where "
        "not isfinite(normalized_value) or not isfinite(uncertainty_lower) "
        "or not isfinite(uncertainty_upper))",
    )
    checks = {
        "unexpected_source_record_count": abs(
            scalar(con, "select count(*) from source_records") - 3_890
        ),
        "collapsed_or_duplicate_source_rows": scalar(
            con, "select count(*) from source_records where source_duplicate_count<>1"
        ),
        "unexpected_identifier_counts": mismatch_count(
            identifier_counts, EXPECTED_IDENTIFIERS
        ),
        "identifier_placeholder_or_unsplit_list_promoted": scalar(
            con,
            "select count(*) from identifier_claim_evidence "
            "where lower(identifier_raw) in ('null','nan') or identifier_raw like '%|%'",
        ),
        "gaia_release_namespace_conflation": scalar(
            con,
            "select count(*) from identifier_claim_evidence "
            "where namespace not in ('gaia_dr2_source_id','gaia_dr3_source_id') "
            "and namespace like 'gaia_%source_id'",
        ),
        "gaia_astrometric_proxy_scope_mismatch": scalar(
            con,
            "select count(*) from identifier_claim_evidence i "
            "join source_records r using(source_record_id) "
            "where i.namespace in ('gaia_dr2_source_id','gaia_dr3_source_id') "
            "and case json_extract_string(r.source_context_json,'$.astrom_Gaia') "
            "when 'O' then i.claim_scope<>'star_or_substellar_object' "
            "when 'P' then i.claim_scope<>'associated_primary_astrometric_proxy' "
            "else true end",
        ),
        "unexpected_classification_counts": mismatch_count(
            classification_counts, EXPECTED_CLASSIFICATIONS
        ),
        "unexpected_stellar_parameter_counts": mismatch_count(
            stellar_parameter_counts, EXPECTED_STELLAR_PARAMETERS
        ),
        "lexical_age_distribution_normalized": scalar(
            con,
            "select count(*) from stellar_parameter_evidence "
            "where quantity_key='maintainer_age_distribution' "
            "and normalized_value is not null",
        ),
        "unexpected_astrometry_count": abs(
            scalar(con, "select count(*) from astrometry_distance_evidence")
            - 149_636
        ),
        "unexpected_photometry_count": abs(
            scalar(con, "select count(*) from photometry_extinction_evidence")
            - 50_134
        ),
        "nonfinite_normalized_measurement": nonfinite,
        "panstarrs_missing_uncertainty_sentinel_leak": scalar(
            con,
            "select count(*) from photometry_extinction_evidence "
            "where bandpass like 'Pan-STARRS1 %' "
            "and (uncertainty_lower=999 or uncertainty_upper=999)",
        ),
        "unexpected_banyan_membership": abs(
            scalar(
                con,
                "select count(*) from cluster_membership_evidence "
                "where method='banyan_sigma_best_young_hypothesis' "
                "and membership_probability between 0 and 1 "
                "and cluster_identity_raw<>'null'",
            )
            - 3_875
        ),
        "unexpected_other_membership": scalar(
            con,
            "select count(*) from cluster_membership_evidence "
            "where method<>'banyan_sigma_best_young_hypothesis'",
        ),
        "unexpected_product_lineage_count": abs(
            scalar(
                con,
                "select count(*) from observation_product_lineage "
                "where product_kind='archive_product' and product_key='url_simpleDB'",
            )
            - 3_079
        ),
        "product_placeholder_promoted": scalar(
            con,
            "select count(*) from observation_product_lineage "
            "where lower(product_locator) in ('null','nan')",
        ),
        "unexpected_citation_count": abs(
            scalar(con, "select count(*) from citations") - 1_001
        ),
        "unexpected_evidence_citation_count": abs(
            scalar(con, "select count(*) from evidence_citations") - 152_122
        ),
        "unaccounted_or_pending_fields": scalar(
            con,
            "select count(*) from source_field_dispositions "
            "where mapping_status<>'materialized'",
        ),
        "unexpected_field_count": abs(
            scalar(con, "select count(*) from source_field_dispositions") - 242
        ),
        "premature_relation_or_orbit_promotion": scalar(
            con,
            "select (select count(*) from relation_claim_evidence) "
            "+ (select count(*) from orbital_solution_evidence)",
        ),
        "premature_planet_promotion": scalar(
            con,
            "select (select count(*) from planet_lifecycle_evidence) "
            "+ (select count(*) from planet_parameter_evidence)",
        ),
        "source_records_without_binding_outcome": scalar(
            con,
            "select count(*) from source_records r "
            "left join object_binding_outcomes b using(source_record_id) "
            "where b.source_record_id is null",
        ),
        "premature_non_unresolved_bindings": scalar(
            con,
            "select count(*) from object_binding_outcomes "
            "where binding_status<>'unresolved'",
        ),
    }
    summaries = {
        "identifiers_by_namespace": rows(
            con,
            "select namespace,count(*) claim_count,count(distinct identifier_normalized) "
            "distinct_value_count from identifier_claim_evidence "
            "group by namespace order by namespace",
        ),
        "gaia_identifiers_by_astrometry_owner_and_scope": rows(
            con,
            "select json_extract_string(r.source_context_json,'$.astrom_Gaia') "
            "astrometry_owner,i.namespace,i.claim_scope,count(*) claim_count "
            "from identifier_claim_evidence i join source_records r using(source_record_id) "
            "where i.namespace in ('gaia_dr2_source_id','gaia_dr3_source_id') "
            "group by all order by 1,2,3",
        ),
        "classifications_by_scheme": rows(
            con,
            "select classification_scheme,count(*) evidence_count "
            "from stellar_classification_evidence group by classification_scheme "
            "order by classification_scheme",
        ),
        "astrometry_by_quantity": rows(
            con,
            "select quantity_key,count(*) evidence_count "
            "from astrometry_distance_evidence group by quantity_key order by quantity_key",
        ),
        "photometry_by_bandpass": rows(
            con,
            "select bandpass,count(*) evidence_count "
            "from photometry_extinction_evidence group by bandpass order by bandpass",
        ),
        "bindings": rows(
            con,
            "select binding_status,binding_scope,count(*) outcome_count "
            "from object_binding_outcomes group by all order by binding_scope",
        ),
    }
    return {
        "schema_version": "spacegate.ultracoolsheet_scientific_evidence_audit.v1",
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
            / "e4_ultracoolsheet_scope_audit.json"
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
        f"UltracoolSheet scientific evidence audit {report['status']}: "
        f"{report['build_id']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
