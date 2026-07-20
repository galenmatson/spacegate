#!/usr/bin/env python3
"""Audit the bounded LAMOST DR11 scientific-evidence checkpoint."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


SOURCE_ROWS = 1_659_281
TABLE_ROWS = {
    "lamost_dr11_v2_lrs_mstellar": 496_415,
    "lamost_dr11_v2_lrs_stellar": 661_941,
    "lamost_dr11_v2_mrs_stellar": 500_925,
}
PARAMETER_SETS = 2_560_712
PARAMETERS = 15_928_014
CLASSIFICATIONS = 2_316_712
ACTIVITY = 737_985
ASTROMETRY_BUNDLES = 1_659_281
ASTROMETRY_MEASUREMENTS = 12_384_183
PRODUCTS = 1_659_281
IDENTIFIERS = 9_953_069
EVIDENCE_LINKS = 31_366_894
EXTERNAL_TARGET_HASHES = {
    "524606bf49b9be6feac2bdff067b1fd333365a5c0081e751cef863f59799466a",
    "ebf7c1620786e79bb155d08e7e991f0c293ac791d239ab968281ccd77d3e3f65",
}


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def audit(con: duckdb.DuckDBPyConnection, manifest: dict[str, Any]) -> dict[str, Any]:
    report = manifest["report"]
    memberships = report.get("external_memberships", {}).get(
        "spectroscopy.lamost_dr11", {}
    )
    source_report = report.get("sources", [{}])[0]
    table_reports = {
        row.get("source_table"): row for row in source_report.get("tables", [])
    }
    membership_failures = 0
    cache_failures = 0
    for table_name, expected_rows in TABLE_ROWS.items():
        groups = memberships.get(table_name, [])
        targets = groups[0].get("targets", []) if len(groups) == 1 else []
        membership_failures += int(
            len(groups) != 1
            or groups[0].get("match") != "any"
            or groups[0].get("normalization")
            != "unsigned_integer_decimal_v1"
            or len(targets) != 2
            or {target.get("target_table_sha256") for target in targets}
            != EXTERNAL_TARGET_HASHES
            or {target.get("source_id") for target in targets}
            != {"gaia.dr3.gaia_source"}
        )
        cache = table_reports.get(table_name, {}).get("selected_row_cache", {})
        cache_failures += int(
            cache.get("enabled") is not True
            or cache.get("row_count") != expected_rows
            or cache.get("source_row_hash_mismatches") != 0
            or cache.get("storage") != "duckdb_temporary_table"
        )
    checks = {
        "wrong_external_membership_lineage": membership_failures,
        "bad_selected_row_cache": cache_failures,
        "unexpected_source_metadata": abs(
            scalar(con, "select count(*) from evidence_sources") - 1
        )
        + scalar(
            con,
            "select count(*) from evidence_sources where "
            "source_id<>'spectroscopy.lamost_dr11' "
            "or release_id<>'dr11_v2_0_2025_09' "
            "or adapter_version<>'lamost_dr11_scientific_evidence_v1'",
        ),
        "unexpected_source_record_count": abs(
            scalar(con, "select count(*) from source_records") - SOURCE_ROWS
        ),
        "unexpected_table_record_count": sum(
            abs(
                scalar(
                    con,
                    "select count(*) from source_records "
                    f"where source_table='{table_name}'",
                )
                - expected
            )
            for table_name, expected in TABLE_ROWS.items()
        ),
        "unexpected_duplicate_rows": scalar(
            con, "select count(*) from source_records where source_duplicate_count<>1"
        ),
        "unexpected_identifier_count": abs(
            scalar(con, "select count(*) from identifier_claim_evidence") - IDENTIFIERS
        ),
        "bad_release_scoped_identities": scalar(
            con,
            "select count(*) from identifier_claim_evidence where "
            "identifier_normalized is null or trim(identifier_normalized)='' "
            "or (namespace in ('gaia_dr3_source_id','panstarrs1_object_id') "
            "and identifier_normalized='0')",
        )
        + abs(
            scalar(
                con,
                "select count(*) from identifier_claim_evidence "
                "where namespace='lamost_obsid'",
            )
            - SOURCE_ROWS
        )
        + abs(
            scalar(
                con,
                "select count(*) from identifier_claim_evidence "
                "where namespace='gaia_dr3_source_id' "
                "and (quality_json->>'$.source_field')='gaia_source_id'",
            )
            - SOURCE_ROWS
        ),
        "wrong_gaia_release_namespace": scalar(
            con,
            "select count(*) from identifier_claim_evidence "
            "where namespace in ('gaia_dr2_source_id','gaia_edr3_source_id')",
        ),
        "bad_gp_id_scope": scalar(
            con,
            "select count(*) from identifier_claim_evidence where "
            "(quality_json->>'$.source_field')='gp_id' and namespace not in "
            "('gaia_dr3_source_id','panstarrs1_object_id',"
            "'lamost_coordinate_source_id')",
        ),
        "identifier_normalization_rejections": scalar(
            con, "select count(*) from identifier_normalization_rejections"
        ),
        "unexpected_parameter_set_count": abs(
            scalar(con, "select count(*) from stellar_parameter_sets") - PARAMETER_SETS
        ),
        "unexpected_parameter_count": abs(
            scalar(con, "select count(*) from stellar_parameter_evidence") - PARAMETERS
        ),
        "incoherent_parameter_set_link": scalar(
            con,
            "select count(*) from stellar_parameter_evidence e "
            "left join stellar_parameter_sets s using(parameter_set_id) "
            "where s.parameter_set_id is null or e.source_record_id<>s.source_record_id",
        ),
        "unexpected_classification_count": abs(
            scalar(con, "select count(*) from stellar_classification_evidence")
            - CLASSIFICATIONS
        ),
        "classification_scheme_conflation": abs(
            scalar(
                con,
                "select count(*) from stellar_classification_evidence "
                "where classification_scheme='lamost_object_class'",
            )
            - 1_158_356
        )
        + abs(
            scalar(
                con,
                "select count(*) from stellar_classification_evidence "
                "where classification_scheme='spectral_type'",
            )
            - 1_158_356
        ),
        "unexpected_activity_count": abs(
            scalar(con, "select count(*) from variability_activity_rotation_evidence")
            - ACTIVITY
        ),
        "unexpected_astrometry_bundle_count": abs(
            scalar(con, "select count(*) from astrometry_distance_evidence_bundles")
            - ASTROMETRY_BUNDLES
        ),
        "unexpected_astrometry_measurement_count": abs(
            scalar(
                con,
                "select coalesce(sum(len(measurements)),0) "
                "from astrometry_distance_evidence_bundles",
            )
            - ASTROMETRY_MEASUREMENTS
        ),
        "empty_astrometry_bundle": scalar(
            con,
            "select count(*) from astrometry_distance_evidence_bundles "
            "where len(measurements)=0",
        ),
        "unexpected_product_count": abs(
            scalar(con, "select count(*) from observation_product_lineage") - PRODUCTS
        ),
        "bad_product_locator": scalar(
            con,
            "select count(*) from observation_product_lineage where "
            "nullif(trim(product_locator),'') is null "
            "or retrieval_policy<>'on_demand_official_archive' "
            "or not starts_with(product_key,'lamost-dr11-')",
        )
        + abs(
            scalar(
                con,
                "select count(distinct product_key) from observation_product_lineage",
            )
            - PRODUCTS
        ),
        "nonfinite_normalized_value": scalar(
            con,
            "select (select count(*) from stellar_parameter_evidence "
            "where not isfinite(normalized_value)) + "
            "(select count(*) from variability_activity_rotation_evidence "
            "where not isfinite(normalized_value)) + "
            "(select count(*) from astrometry_distance_evidence_bundles b, "
            "unnest(b.measurements) nested(measurement) "
            "where not isfinite(measurement.normalized_value))",
        ),
        "unfiltered_missing_sentinel": scalar(
            con,
            "select (select count(*) from stellar_parameter_evidence "
            "where normalized_value=-9999) + "
            "(select count(*) from variability_activity_rotation_evidence "
            "where normalized_value=-9999) + "
            "(select count(*) from astrometry_distance_evidence_bundles b, "
            "unnest(b.measurements) nested(measurement) "
            "where measurement.normalized_value=-9999)",
        ),
        "missing_source_reference": scalar(
            con,
            "select (select count(*) from stellar_parameter_evidence "
            "where reference_raw is null) + "
            "(select count(*) from stellar_classification_evidence "
            "where reference_raw is null) + "
            "(select count(*) from variability_activity_rotation_evidence "
            "where reference_raw is null) + "
            "(select count(*) from astrometry_distance_evidence_bundles b, "
            "unnest(b.measurements) nested(measurement) "
            "where measurement.reference_raw is null)",
        ),
        "unexpected_field_accounting": abs(
            scalar(con, "select count(*) from source_field_dispositions") - 185
        )
        + scalar(
            con,
            "select count(*) from source_field_dispositions "
            "where mapping_status not in ('materialized','excluded')",
        )
        + abs(
            scalar(
                con,
                "select count(*) from source_field_dispositions "
                "where mapping_status='excluded'",
            )
            - 15
        ),
        "copied_photometry_materialized": scalar(
            con,
            "select count(*) from source_field_dispositions where source_field in "
            "('mag_ps_g','mag_ps_r','mag_ps_i','mag_ps_z','mag_ps_y',"
            "'gaia_g_mean_mag','gaia_bp_mean_mag','gaia_rp_mean_mag') "
            "and mapping_status<>'excluded'",
        ),
        "unexpected_citations": abs(
            scalar(con, "select count(*) from citations") - 3
        )
        + abs(
            scalar(con, "select count(*) from evidence_citations") - EVIDENCE_LINKS
        ),
        "premature_binding": scalar(
            con,
            "select count(*) from object_binding_outcomes "
            "where binding_status<>'unresolved'",
        ),
        "premature_other_domains": scalar(
            con,
            "select (select count(*) from astrometry_distance_evidence) + "
            "(select count(*) from photometry_extinction_evidence) + "
            "(select count(*) from relation_claim_evidence) + "
            "(select count(*) from orbital_solution_evidence) + "
            "(select count(*) from planet_parameter_evidence) + "
            "(select count(*) from compact_object_evidence) + "
            "(select count(*) from extended_object_evidence)",
        ),
    }
    return {
        "schema_version": "spacegate.lamost_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": {
            "stellar_quantities": rows(
                con,
                "select quantity_key,count(*) evidence_count "
                "from stellar_parameter_evidence group by 1 order by 1",
            ),
            "classifications": rows(
                con,
                "select classification_scheme,count(*) evidence_count "
                "from stellar_classification_evidence group by 1 order by 1",
            ),
            "activity": rows(
                con,
                "select evidence_kind,quantity_key,count(*) evidence_count "
                "from variability_activity_rotation_evidence group by 1,2 order by 1,2",
            ),
            "astrometry": rows(
                con,
                "select measurement.quantity_key,count(*) evidence_count "
                "from astrometry_distance_evidence_bundles b, "
                "unnest(b.measurements) nested(measurement) group by 1 order by 1",
            ),
            "products": rows(
                con,
                "select product_kind,retrieval_policy,processing_level,"
                "count(*) product_count from observation_product_lineage "
                "group by 1,2,3 order by 1",
            ),
            "field_dispositions": rows(
                con,
                "select source_table,disposition,mapping_status,count(*) field_count "
                "from source_field_dispositions group by 1,2,3 order by 1,2,3",
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--memory-limit", default="12GB")
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
        / "e4_lamost_scientific_evidence_audit.json",
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    audit_temporary = None
    if args.temp_directory is not None:
        args.temp_directory.mkdir(parents=True, exist_ok=True)
        audit_temporary = Path(
            tempfile.mkdtemp(
                prefix=f"lamost-audit-{manifest['build_id']}.",
                dir=args.temp_directory,
            )
        )
    try:
        con = duckdb.connect(str(database), read_only=True)
        if audit_temporary is not None:
            con.execute(f"set temp_directory='{audit_temporary}'")
        con.execute(f"set memory_limit='{args.memory_limit}'")
        con.execute(f"set threads={max(1, args.threads)}")
        report = audit(con, manifest)
    finally:
        if "con" in locals():
            con.close()
        if audit_temporary is not None:
            shutil.rmtree(audit_temporary, ignore_errors=True)
    report.update(
        {
            "build_id": str(manifest["build_id"]),
            "database": str(database),
            "threads": max(1, args.threads),
            "memory_limit": str(args.memory_limit),
            "temporary_storage_removed": (
                audit_temporary is None or not audit_temporary.exists()
            ),
        }
    )
    write_json(args.report, report)
    print(f"LAMOST scientific evidence audit {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
