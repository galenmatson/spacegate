#!/usr/bin/env python3
"""Audit the bounded APOGEE DR17 scientific-evidence checkpoint."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, sql_string, write_json


ALLSTAR_ROWS = 178_099
SOURCE_RECORDS = 180_314
PARAMETER_SETS = 163_971
PARAMETERS = 3_280_268
PHOTOMETRY = 1_357_072
ASTROMETRY = 529_676
PRODUCTS = 173_478
IDENTIFIERS = 890_495
EVIDENCE_LINKS = 5_167_016
REFERENCE = "SDSS DR17 APOGEE allStar"
EXTERNAL_TARGET_HASH = (
    "761329ad83f6fc06c9b8f824855088054518b5eb70e4dca26364a99fad42737f"
)


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    result = con.execute(query)
    columns = [value[0] for value in result.description]
    return [dict(zip(columns, values, strict=True)) for values in result.fetchall()]


def audit(con: duckdb.DuckDBPyConnection, manifest: dict[str, Any]) -> dict[str, Any]:
    report = manifest["report"]
    memberships = report.get("external_memberships", {}).get(
        "spectroscopy.apogee_dr17", {}
    ).get("apogee_dr17_allstar", [])
    target = memberships[0]["targets"][0] if len(memberships) == 1 else {}
    checks = {
        "wrong_external_membership_lineage": int(
            len(memberships) != 1
            or memberships[0].get("normalization")
            != "unsigned_integer_decimal_v1"
            or target.get("source_id") != "distance.gaia_edr3_bailer_jones"
            or target.get("target_table_sha256") != EXTERNAL_TARGET_HASH
        ),
        "unexpected_source_metadata": abs(
            scalar(con, "select count(*) from evidence_sources") - 1
        ) + scalar(
            con,
            "select count(*) from evidence_sources where "
            "source_id<>'spectroscopy.apogee_dr17' or release_id<>'sdss_dr17' "
            "or adapter_version<>'apogee_dr17_scientific_evidence_v1'",
        ),
        "unexpected_source_record_count": abs(
            scalar(con, "select count(*) from source_records") - SOURCE_RECORDS
        ),
        "unexpected_allstar_count": abs(
            scalar(
                con,
                "select count(*) from source_records "
                "where source_table='apogee_dr17_allstar'",
            )
            - ALLSTAR_ROWS
        ),
        "unexpected_duplicate_rows": scalar(
            con,
            "select count(*) from source_records where source_duplicate_count<>1 "
            "and source_table<>'apogee_dr17_field_versions'",
        ) + abs(
            scalar(
                con,
                "select coalesce(sum(source_duplicate_count-1),0) from source_records",
            )
            - 1
        ),
        "unexpected_identifier_count": abs(
            scalar(con, "select count(*) from identifier_claim_evidence") - IDENTIFIERS
        ),
        "bad_gaia_edr3_identity": scalar(
            con,
            "select count(*) from identifier_claim_evidence "
            "where namespace='gaia_edr3_source_id' "
            "and (identifier_normalized='0' or identifier_normalized is null)",
        ) + abs(
            scalar(
                con,
                "select count(*) from identifier_claim_evidence "
                "where namespace='gaia_edr3_source_id'",
            )
            - ALLSTAR_ROWS
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
        "unfiltered_parameter_sentinel": scalar(
            con,
            "select count(*) from stellar_parameter_evidence "
            "where normalized_value=-9999 or not isfinite(normalized_value)",
        ),
        "wrong_parameter_reference": scalar(
            con,
            "select count(*) from stellar_parameter_evidence "
            f"where reference_raw<>{sql_string(REFERENCE)}",
        ),
        "unexpected_photometry_count": abs(
            scalar(con, "select count(*) from photometry_extinction_evidence")
            - PHOTOMETRY
        ),
        "unexpected_astrometry_count": abs(
            scalar(con, "select count(*) from astrometry_distance_evidence")
            - ASTROMETRY
        ),
        "unexpected_product_count": abs(
            scalar(con, "select count(*) from observation_product_lineage") - PRODUCTS
        ),
        "unfiltered_domain_sentinel": scalar(
            con,
            "select (select count(*) from photometry_extinction_evidence "
            "where normalized_value=-9999 or not isfinite(normalized_value)) + "
            "(select count(*) from astrometry_distance_evidence "
            "where normalized_value=-9999 or not isfinite(normalized_value))",
        ),
        "wrong_domain_reference": scalar(
            con,
            "select (select count(*) from photometry_extinction_evidence "
            f"where reference_raw<>{sql_string(REFERENCE)}) + "
            "(select count(*) from astrometry_distance_evidence "
            f"where reference_raw<>{sql_string(REFERENCE)})",
        ),
        "unexpected_field_accounting": abs(
            scalar(con, "select count(*) from source_field_dispositions") - 243
        ) + scalar(
            con,
            "select count(*) from source_field_dispositions "
            "where mapping_status not in ('materialized','excluded')",
        ) + abs(
            scalar(
                con,
                "select count(*) from source_field_dispositions "
                "where mapping_status='excluded'",
            )
            - 32
        ),
        "copied_gaia_values_materialized": scalar(
            con,
            "select count(*) from source_field_dispositions where "
            "source_field like 'GAIAEDR3_%' and source_field<>'GAIAEDR3_SOURCE_ID' "
            "and mapping_status<>'excluded'",
        ),
        "unexpected_citations": abs(scalar(con, "select count(*) from citations") - 1)
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
            "(select count(*) from extended_object_evidence)",
        ),
    }
    return {
        "schema_version": "spacegate.apogee_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": {
            "stellar_quantities": rows(
                con,
                "select quantity_key,count(*) evidence_count "
                "from stellar_parameter_evidence group by 1 order by 1",
            ),
            "photometry": rows(
                con,
                "select quantity_key,bandpass,count(*) evidence_count "
                "from photometry_extinction_evidence group by 1,2 order by 1,2",
            ),
            "astrometry": rows(
                con,
                "select quantity_key,count(*) evidence_count "
                "from astrometry_distance_evidence group by 1 order by 1",
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
        / "e4_apogee_scientific_evidence_audit.json",
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    audit_temporary = None
    if args.temp_directory is not None:
        args.temp_directory.mkdir(parents=True, exist_ok=True)
        audit_temporary = Path(
            tempfile.mkdtemp(
                prefix=f"apogee-audit-{manifest['build_id']}.",
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
    print(f"APOGEE scientific evidence audit {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
