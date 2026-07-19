#!/usr/bin/env python3
"""Audit the bounded Bailer-Jones EDR3 distance-evidence checkpoint."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, sql_string, write_json


SOURCE_ROWS = 17_310_560
PHOTOGEOMETRIC_ROWS = 15_914_748
NESTED_MEASUREMENTS = SOURCE_ROWS + PHOTOGEOMETRIC_ROWS
EXPECTED_QUANTITIES = {
    "geometric_distance_posterior_median": SOURCE_ROWS,
    "photogeometric_distance_posterior_median": PHOTOGEOMETRIC_ROWS,
}
BOUND_SEMANTICS = "posterior_16th_84th_percentile_interval_endpoints"
REFERENCE = "2021AJ....161..147B"


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
    nested = (
        "from astrometry_distance_evidence_bundles b, "
        "unnest(b.measurements) as nested(measurement)"
    )
    quantity_counts = {
        str(quantity): int(count)
        for quantity, count in con.execute(
            f"select measurement.quantity_key,count(*) {nested} group by 1"
        ).fetchall()
    }
    checks = {
        "unexpected_source_metadata": abs(
            scalar(con, "select count(*) from evidence_sources") - 1
        )
        + scalar(
            con,
            "select count(*) from evidence_sources where "
            "source_id<>'distance.gaia_edr3_bailer_jones' or "
            "release_id<>'vizier_i_352_2021' or "
            "adapter_version<>'bailer_jones_edr3_distance_evidence_v1'",
        ),
        "unexpected_source_record_count": abs(
            scalar(con, "select count(*) from source_records") - SOURCE_ROWS
        ),
        "unexpected_source_record_scope": scalar(
            con, "select count(*) from source_records where object_scope<>'star'"
        ),
        "collapsed_or_duplicate_source_rows": scalar(
            con, "select count(*) from source_records where source_duplicate_count<>1"
        ),
        "nonempty_redundant_source_context": scalar(
            con, "select count(*) from source_records where source_context_json<>'{}'"
        ),
        "unexpected_identifier_count": abs(
            scalar(con, "select count(*) from identifier_claim_evidence") - SOURCE_ROWS
        ),
        "duplicate_or_zero_gaia_identifiers": scalar(
            con,
            "select count(*)-count(distinct identifier_normalized) "
            "from identifier_claim_evidence where namespace='gaia_edr3_source_id'",
        )
        + scalar(
            con,
            "select count(*) from identifier_claim_evidence "
            "where namespace<>'gaia_edr3_source_id' or identifier_normalized='0'",
        ),
        "identifier_normalization_rejections": scalar(
            con, "select count(*) from identifier_normalization_rejections"
        ),
        "unexpected_binding_count": abs(
            scalar(con, "select count(*) from object_binding_outcomes") - SOURCE_ROWS
        ),
        "premature_or_wrong_scope_bindings": scalar(
            con,
            "select count(*) from object_binding_outcomes "
            "where binding_status<>'unresolved' or binding_scope<>'star'",
        ),
        "unexpected_bundle_count": abs(
            scalar(con, "select count(*) from astrometry_distance_evidence_bundles")
            - SOURCE_ROWS
        ),
        "empty_or_oversized_bundle": scalar(
            con,
            "select count(*) from astrometry_distance_evidence_bundles "
            "where len(measurements) not between 1 and 2",
        ),
        "unexpected_nested_measurement_count": abs(
            scalar(con, f"select count(*) {nested}") - NESTED_MEASUREMENTS
        ),
        "unexpected_quantity_counts": mismatch_count(
            quantity_counts, EXPECTED_QUANTITIES
        ),
        "invalid_posterior_interval": scalar(
            con,
            "select count(*) "
            + nested
            + " where measurement.uncertainty_lower is null "
            "or measurement.uncertainty_upper is null "
            "or not isfinite(measurement.normalized_value) "
            "or not isfinite(measurement.uncertainty_lower) "
            "or not isfinite(measurement.uncertainty_upper) "
            "or measurement.uncertainty_lower>measurement.normalized_value "
            "or measurement.normalized_value>measurement.uncertainty_upper",
        ),
        "wrong_bound_semantics": scalar(
            con,
            "select count(*) "
            + nested
            + f" where measurement.bound_semantics<>'{BOUND_SEMANTICS}'",
        ),
        "missing_flag_context": scalar(
            con,
            "select count(*) "
            + nested
            + " where json_extract_string(measurement.quality_json, '$.Flag') is null",
        ),
        "wrong_reference": scalar(
            con,
            "select count(*) "
            + nested
            + f" where measurement.reference_raw<>'{REFERENCE}'",
        ),
        "copied_gaia_coordinates_materialized": scalar(
            con,
            "select count(*) "
            + nested
            + " where measurement.quantity_key in ('right_ascension','declination')",
        ),
        "unexpected_flat_astrometry": scalar(
            con, "select count(*) from astrometry_distance_evidence"
        ),
        "unexpected_field_count": abs(
            scalar(con, "select count(*) from source_field_dispositions") - 10
        ),
        "unaccounted_or_pending_fields": scalar(
            con,
            "select count(*) from source_field_dispositions "
            "where mapping_status not in ('materialized','excluded')",
        ),
        "wrong_excluded_coordinate_count": abs(
            scalar(
                con,
                "select count(*) from source_field_dispositions "
                "where source_field in ('RA_ICRS','DE_ICRS') "
                "and disposition='exclude' and mapping_status='excluded'",
            )
            - 2
        ),
        "lost_case_collision_lineage": abs(
            scalar(
                con,
                "select count(*) from source_field_dispositions where "
                "(source_field='B_rgeo__source_case_2' and source_native_field='B_rgeo') "
                "or (source_field='B_rpgeo__source_case_2' "
                "and source_native_field='B_rpgeo')",
            )
            - 2
        ),
        "unexpected_source_native_alias_count": abs(
            scalar(
                con,
                "select count(*) from source_field_dispositions "
                "where source_field<>source_native_field",
            )
            - 2
        ),
        "unexpected_citation_count": abs(
            scalar(con, "select count(*) from citations") - 1
        )
        + scalar(
            con,
            "select count(*) from citations where "
            "source_id<>'distance.gaia_edr3_bailer_jones' or "
            f"citation_text_raw<>'{REFERENCE}'",
        ),
        "unexpected_evidence_citation_count": abs(
            scalar(con, "select count(*) from evidence_citations")
            - NESTED_MEASUREMENTS
        ),
        "premature_other_domain_evidence": scalar(
            con,
            "select (select count(*) from stellar_parameter_evidence) "
            "+ (select count(*) from stellar_classification_evidence) "
            "+ (select count(*) from photometry_extinction_evidence) "
            "+ (select count(*) from relation_claim_evidence) "
            "+ (select count(*) from orbital_solution_evidence) "
            "+ (select count(*) from planet_parameter_evidence) "
            "+ (select count(*) from compact_object_evidence) "
            "+ (select count(*) from extended_object_evidence)",
        ),
    }
    return {
        "schema_version": "spacegate.bailer_jones_scientific_evidence_audit.v1",
        "status": "pass" if not any(checks.values()) else "fail",
        "checks": checks,
        "summaries": {
            "quantities": rows(
                con,
                "select measurement.quantity_key,count(*) evidence_count,"
                "min(measurement.normalized_value) minimum_pc,"
                "max(measurement.normalized_value) maximum_pc "
                + nested
                + " group by 1 order by 1",
            ),
            "field_dispositions": rows(
                con,
                "select source_field,source_native_field,disposition,"
                "destination_table,mapping_status,reason "
                "from source_field_dispositions order by source_field",
            ),
            "bindings": rows(
                con,
                "select binding_status,binding_scope,count(*) outcome_count "
                "from object_binding_outcomes group by all order by all",
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
        default=(
            DEFAULT_STATE
            / "reports"
            / "evidence_lake_v2"
            / "e4_bailer_jones_scientific_evidence_audit.json"
        ),
    )
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    database = args.manifest.parent / str(manifest["database"])
    audit_temporary = None
    if args.temp_directory is not None:
        args.temp_directory.mkdir(parents=True, exist_ok=True)
        audit_temporary = Path(
            tempfile.mkdtemp(prefix=f"bailer-jones-audit-{manifest['build_id']}.", dir=args.temp_directory)
        )
    try:
        con = duckdb.connect(str(database), read_only=True)
        if audit_temporary is not None:
            con.execute(f"set temp_directory={sql_string(str(audit_temporary))}")
        con.execute(f"set memory_limit={sql_string(str(args.memory_limit))}")
        con.execute(f"set threads={max(1, args.threads)}")
        report = audit(con)
    finally:
        if "con" in locals():
            con.close()
        if audit_temporary is not None:
            shutil.rmtree(audit_temporary, ignore_errors=True)
    report["build_id"] = str(manifest["build_id"])
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
    print(
        f"Bailer-Jones scientific evidence audit {report['status']}: "
        f"{report['build_id']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
