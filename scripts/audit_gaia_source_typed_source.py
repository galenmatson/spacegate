#!/usr/bin/env python3
"""Audit the Gaia DR3 backbone and uncertainty-envelope typed tables."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compile_scientific_evidence import (  # noqa: E402
    DEFAULT_REGISTRY,
    DEFAULT_STATE,
    load_json,
    source_input,
    write_json,
)


SOURCE_ID = "gaia.dr3.gaia_source"
HARD_TABLE = "gaia_dr3_source_envelope_v2"
SUPPLEMENT_TABLE = "gaia_dr3_source_uncertain_distance_supplement_v1"
PARALLAX_FLOOR_MAS = 2.609272
EXPECTED_FIELD_COUNT = 152
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/"
    "e4_gaia_source_typed_source_audit.json"
)
REQUIRED_FIELDS = {
    "solution_id",
    "designation",
    "source_id",
    "ref_epoch",
    "ra",
    "ra_error",
    "dec",
    "dec_error",
    "parallax",
    "parallax_error",
    "pmra",
    "pmra_error",
    "pmdec",
    "pmdec_error",
    "ra_dec_corr",
    "ruwe",
    "phot_g_mean_flux",
    "phot_g_mean_flux_error",
    "phot_g_mean_mag",
    "phot_bp_mean_flux",
    "phot_bp_mean_mag",
    "phot_rp_mean_flux",
    "phot_rp_mean_mag",
    "radial_velocity",
    "radial_velocity_error",
    "vbroad",
    "vbroad_error",
    "phot_variable_flag",
    "l",
    "b",
    "has_xp_continuous",
    "has_xp_sampled",
    "has_rvs",
    "has_epoch_photometry",
    "has_epoch_rv",
    "classprob_dsc_combmod_quasar",
    "classprob_dsc_combmod_galaxy",
    "classprob_dsc_combmod_star",
    "teff_gspphot",
    "distance_gspphot",
    "libname_gspphot",
}


def query_row(
    con: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[str],
) -> dict[str, Any]:
    result = con.execute(query, parameters)
    columns = [str(row[0]) for row in result.description]
    return dict(zip(columns, result.fetchone(), strict=True))


def table_summary(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    *,
    supplement: bool,
    error_fields: list[str],
    correlation_fields: list[str],
    probability_fields: list[str],
) -> dict[str, Any]:
    invalid_errors = " + ".join(
        f"count(*) filter (where \"{field}\" < 0)" for field in error_fields
    ) or "0"
    invalid_correlations = " + ".join(
        f"count(*) filter (where \"{field}\" not between -1 and 1)"
        for field in correlation_fields
    ) or "0"
    invalid_probabilities = " + ".join(
        f"count(*) filter (where \"{field}\" not between 0 and 1)"
        for field in probability_fields
    ) or "0"
    wrong_branch = (
        f"parallax >= {PARALLAX_FLOOR_MAS}"
        if supplement
        else f"parallax < {PARALLAX_FLOOR_MAS} or parallax is null"
    )
    return query_row(
        con,
        f"""
        select
          count(*)::bigint as row_count,
          count(*) filter (where source_id is null or source_id <= 0)::bigint
            as invalid_source_id,
          (count(*) - count(distinct source_id))::bigint
            as duplicate_source_id_excess,
          count(*) filter (
            where designation is null
               or designation <> 'Gaia DR3 ' || cast(source_id as varchar)
          )::bigint as designation_mismatch,
          count(*) filter (
            where ra is null or dec is null
               or ra not between 0 and 360 or dec not between -90 and 90
          )::bigint as invalid_coordinates,
          count(*) filter (where ref_epoch is null or ref_epoch <> 2016.0)::bigint
            as unexpected_reference_epoch,
          count(*) filter (where {wrong_branch})::bigint as wrong_envelope_branch,
          ({invalid_errors})::bigint as negative_error_occurrences,
          ({invalid_correlations})::bigint as invalid_correlation_occurrences,
          ({invalid_probabilities})::bigint as invalid_probability_occurrences,
          count(*) filter (where radial_velocity is not null)::bigint
            as radial_velocity_rows,
          count(*) filter (where teff_gspphot is not null)::bigint
            as projected_gspphot_rows,
          count(*) filter (where has_xp_continuous)::bigint
            as xp_continuous_rows,
          count(*) filter (where has_rvs)::bigint as rvs_product_rows,
          count(*) filter (where has_epoch_photometry)::bigint
            as epoch_photometry_rows
        from read_parquet(?)
        """,
        [str(path)],
    )


def audit(typed_root: Path, typed_manifest: dict[str, Any]) -> dict[str, Any]:
    tables = {str(row["source_name"]): row for row in typed_manifest["tables"]}
    missing_tables = sorted({HARD_TABLE, SUPPLEMENT_TABLE} - set(tables))
    pending_tables = sorted(
        name for name, row in tables.items() if str(row.get("status")) != "typed"
    )
    schemas = {
        name: [(str(field["name"]), str(field["type"])) for field in row.get("columns", [])]
        for name, row in tables.items()
        if name in {HARD_TABLE, SUPPLEMENT_TABLE}
    }
    missing_fields = {
        name: sorted(REQUIRED_FIELDS - {field for field, _ in schema})
        for name, schema in schemas.items()
    }
    checks: dict[str, Any] = {
        "missing_required_tables": missing_tables,
        "pending_typed_tables": pending_tables,
        "wrong_field_counts": {
            name: len(schema)
            for name, schema in schemas.items()
            if len(schema) != EXPECTED_FIELD_COUNT
        },
        "missing_required_fields": {
            name: fields for name, fields in missing_fields.items() if fields
        },
        "branch_schema_mismatch": bool(
            not missing_tables and schemas[HARD_TABLE] != schemas[SUPPLEMENT_TABLE]
        ),
    }
    summaries: dict[str, Any] = {
        "typed_tables": sorted(tables),
        "parallax_floor_mas": PARALLAX_FLOOR_MAS,
        "branch_field_counts": {name: len(schema) for name, schema in schemas.items()},
    }
    if not any(checks.values()):
        hard_path = typed_root / str(tables[HARD_TABLE]["parquet_path"])
        supplement_path = typed_root / str(tables[SUPPLEMENT_TABLE]["parquet_path"])
        fields = [field for field, _ in schemas[HARD_TABLE]]
        error_fields = sorted(field for field in fields if field.endswith("_error"))
        correlation_fields = sorted(field for field in fields if field.endswith("_corr"))
        probability_fields = sorted(
            field for field in fields if field.startswith("classprob_")
        )
        con = duckdb.connect()
        try:
            hard = table_summary(
                con,
                hard_path,
                supplement=False,
                error_fields=error_fields,
                correlation_fields=correlation_fields,
                probability_fields=probability_fields,
            )
            supplement = table_summary(
                con,
                supplement_path,
                supplement=True,
                error_fields=error_fields,
                correlation_fields=correlation_fields,
                probability_fields=probability_fields,
            )
            overlap = int(
                con.execute(
                    "select count(*) from read_parquet(?) h semi join "
                    "read_parquet(?) s using(source_id)",
                    [str(hard_path), str(supplement_path)],
                ).fetchone()[0]
            )
            solution_ids = [
                str(row[0])
                for row in con.execute(
                    "select distinct solution_id from ("
                    "select solution_id from read_parquet(?) union all "
                    "select solution_id from read_parquet(?)) order by 1",
                    [str(hard_path), str(supplement_path)],
                ).fetchall()
            ]
        finally:
            con.close()
        checks.update(
            {
                "hard_manifest_row_count_delta": int(hard["row_count"])
                - int(tables[HARD_TABLE]["row_count"]),
                "supplement_manifest_row_count_delta": int(supplement["row_count"])
                - int(tables[SUPPLEMENT_TABLE]["row_count"]),
                "cross_branch_source_id_overlap": overlap,
                "unexpected_solution_id_count": max(0, len(solution_ids) - 1),
            }
        )
        for prefix, summary in (("hard", hard), ("supplement", supplement)):
            for key in (
                "invalid_source_id",
                "duplicate_source_id_excess",
                "designation_mismatch",
                "invalid_coordinates",
                "unexpected_reference_epoch",
                "wrong_envelope_branch",
                "negative_error_occurrences",
                "invalid_correlation_occurrences",
                "invalid_probability_occurrences",
            ):
                checks[f"{prefix}_{key}"] = int(summary[key])
        summaries.update(
            {
                "hard_branch": hard,
                "uncertainty_supplement": supplement,
                "solution_ids": solution_ids,
                "audited_error_fields": error_fields,
                "audited_correlation_fields": correlation_fields,
                "audited_probability_fields": probability_fields,
            }
        )
    failed = any(bool(value) for value in checks.values())
    return {
        "schema_version": "spacegate.gaia_dr3_source_typed_source_audit.v1",
        "status": "fail" if failed else "pass",
        "source_id": SOURCE_ID,
        "release_id": typed_manifest["release_id"],
        "raw_snapshot_id": typed_manifest["snapshot_id"],
        "typed_snapshot_id": typed_manifest["typed_snapshot_id"],
        "typed_content_sha256": typed_manifest["content_sha256"],
        "checks": checks,
        "summaries": summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    registry = load_json(args.registry)
    source = next(
        row for row in registry["sources"] if str(row["source_id"]) == SOURCE_ID
    )
    resolved = source_input(args.state_dir, source)
    report = audit(resolved["typed_path"], resolved["typed_manifest"])
    write_json(args.report, report)
    hard = report["summaries"].get("hard_branch", {})
    supplement = report["summaries"].get("uncertainty_supplement", {})
    print(
        f"Gaia source typed audit {report['status']}: "
        f"hard={hard.get('row_count', 0):,} "
        f"supplement={supplement.get('row_count', 0):,}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
