#!/usr/bin/env python3
"""Audit the bounded GALAH DR4 scientific-evidence checkpoint."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, sql_string, write_json


ALLSTAR_ROWS = 117_885
PARAMETER_SETS = 233_098
PARAMETERS = 4_052_282
PHOTOMETRY = 973_436
ASTROMETRY = 857_173
ACTIVITY = 623_253
IDENTIFIERS = 353_655
EVIDENCE_LINKS = 6_506_144
REFERENCE = "GALAH DR4 allStar 240705"
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
    memberships = (
        report.get("external_memberships", {})
        .get("spectroscopy.galah_dr4", {})
        .get("galah_dr4_allstar_240705", [])
    )
    targets = memberships[0].get("targets", []) if len(memberships) == 1 else []
    source_report = report.get("sources", [{}])[0]
    table_report = source_report.get("tables", [{}])[0]
    selected_cache = table_report.get("selected_row_cache", {})
    reference_predicate = f"reference_raw<>{sql_string(REFERENCE)}"
    checks = {
        "wrong_external_membership_lineage": int(
            len(memberships) != 1
            or memberships[0].get("match") != "any"
            or memberships[0].get("normalization")
            != "unsigned_integer_decimal_v1"
            or len(targets) != 2
            or {target.get("target_table_sha256") for target in targets}
            != EXTERNAL_TARGET_HASHES
            or {target.get("source_id") for target in targets}
            != {"gaia.dr3.gaia_source"}
        ),
        "bad_selected_row_cache": int(
            selected_cache.get("enabled") is not True
            or selected_cache.get("row_count") != ALLSTAR_ROWS
            or selected_cache.get("source_row_hash_mismatches") != 0
            or selected_cache.get("storage") != "duckdb_temporary_table"
        ),
        "unexpected_source_metadata": abs(
            scalar(con, "select count(*) from evidence_sources") - 1
        )
        + scalar(
            con,
            "select count(*) from evidence_sources where "
            "source_id<>'spectroscopy.galah_dr4' "
            "or release_id<>'galah_dr4_240705' "
            "or adapter_version<>'galah_dr4_scientific_evidence_v1'",
        ),
        "unexpected_source_record_count": abs(
            scalar(con, "select count(*) from source_records") - ALLSTAR_ROWS
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
            "or (namespace='gaia_dr3_source_id' and identifier_normalized='0')",
        )
        + sum(
            abs(
                scalar(
                    con,
                    "select count(*) from identifier_claim_evidence "
                    f"where namespace={sql_string(namespace)}",
                )
                - ALLSTAR_ROWS
            )
            for namespace in ("galah_sobject_id", "2mass_id", "gaia_dr3_source_id")
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
        "distance_mislabeled_as_radius": scalar(
            con,
            "select count(*) from stellar_parameter_evidence "
            "where quantity_key like 'stellar_radius%'",
        )
        + abs(
            scalar(
                con,
                "select count(*) from astrometry_distance_evidence where "
                "quantity_key in ('distance_model_lower_bound',"
                "'distance_model_median','distance_model_upper_bound')",
            )
            - (3 * 116_549)
        ),
        "missing_distance_bound_semantics": scalar(
            con,
            "select count(*) from astrometry_distance_evidence where "
            "(quantity_key='distance_model_lower_bound' and bound_semantics<>'lower_bound') "
            "or (quantity_key='distance_model_upper_bound' and bound_semantics<>'upper_bound')",
        ),
        "unexpected_photometry_count": abs(
            scalar(con, "select count(*) from photometry_extinction_evidence")
            - PHOTOMETRY
        ),
        "source_reddening_clamped_or_lost": abs(
            scalar(
                con,
                "select count(*) from photometry_extinction_evidence "
                "where quantity_key='reddening'",
            )
            - 117_660
        )
        + int(
            con.execute(
                "select coalesce(max(normalized_value),0)<=300 "
                "from photometry_extinction_evidence where quantity_key='reddening'"
            ).fetchone()[0]
        ),
        "unexpected_astrometry_count": abs(
            scalar(con, "select count(*) from astrometry_distance_evidence") - ASTROMETRY
        ),
        "unexpected_activity_count": abs(
            scalar(con, "select count(*) from variability_activity_rotation_evidence")
            - ACTIVITY
        ),
        "unfiltered_nonfinite_domain_value": scalar(
            con,
            "select (select count(*) from stellar_parameter_evidence "
            "where not isfinite(normalized_value)) + "
            "(select count(*) from photometry_extinction_evidence "
            "where not isfinite(normalized_value)) + "
            "(select count(*) from astrometry_distance_evidence "
            "where not isfinite(normalized_value)) + "
            "(select count(*) from variability_activity_rotation_evidence "
            "where not isfinite(normalized_value))",
        ),
        "wrong_domain_reference": scalar(
            con,
            "select (select count(*) from stellar_parameter_evidence where "
            + reference_predicate
            + ") + (select count(*) from photometry_extinction_evidence where "
            + reference_predicate
            + ") + (select count(*) from astrometry_distance_evidence where "
            + reference_predicate
            + ") + (select count(*) from variability_activity_rotation_evidence where "
            + reference_predicate
            + ")",
        ),
        "unexpected_field_accounting": abs(
            scalar(con, "select count(*) from source_field_dispositions") - 184
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
        "copied_catalog_values_materialized": scalar(
            con,
            "select count(*) from source_field_dispositions where source_field in "
            "('rv_gaia_dr3','e_rv_gaia_dr3','phot_g_mean_mag','bp_rp','j_m',"
            "'j_msigcom','h_m','h_msigcom','ks_m','ks_msigcom','W2mag',"
            "'e_W2mag','ruwe','parallax','parallax_error') "
            "and mapping_status<>'excluded'",
        ),
        "unexpected_citations": abs(
            scalar(con, "select count(*) from citations") - 1
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
            "select (select count(*) from stellar_classification_evidence) + "
            "(select count(*) from relation_claim_evidence) + "
            "(select count(*) from orbital_solution_evidence) + "
            "(select count(*) from planet_parameter_evidence) + "
            "(select count(*) from compact_object_evidence) + "
            "(select count(*) from extended_object_evidence) + "
            "(select count(*) from observation_product_lineage)",
        ),
    }
    return {
        "schema_version": "spacegate.galah_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": {
            "stellar_quantities": rows(
                con,
                "select quantity_key,count(*) evidence_count "
                "from stellar_parameter_evidence group by 1 order by 1",
            ),
            "photometry_and_interstellar": rows(
                con,
                "select quantity_key,bandpass,count(*) evidence_count "
                "from photometry_extinction_evidence group by 1,2 order by 1,2",
            ),
            "astrometry": rows(
                con,
                "select quantity_key,count(*) evidence_count "
                "from astrometry_distance_evidence group by 1 order by 1",
            ),
            "activity": rows(
                con,
                "select evidence_kind,quantity_key,count(*) evidence_count "
                "from variability_activity_rotation_evidence group by 1,2 order by 1,2",
            ),
            "field_dispositions": rows(
                con,
                "select disposition,mapping_status,count(*) field_count "
                "from source_field_dispositions group by 1,2 order by 1,2",
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
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
        / "e4_galah_scientific_evidence_audit.json",
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    audit_temporary = None
    if args.temp_directory is not None:
        args.temp_directory.mkdir(parents=True, exist_ok=True)
        audit_temporary = Path(
            tempfile.mkdtemp(
                prefix=f"galah-audit-{manifest['build_id']}.",
                dir=args.temp_directory,
            )
        )
    try:
        con = duckdb.connect(str(database), read_only=True)
        if audit_temporary is not None:
            con.execute(f"set temp_directory={sql_string(str(audit_temporary))}")
        con.execute(f"set memory_limit={sql_string(str(args.memory_limit))}")
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
    print(f"GALAH scientific evidence audit {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
