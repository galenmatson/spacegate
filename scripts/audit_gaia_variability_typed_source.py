#!/usr/bin/env python3
"""Audit Gaia DR3 variability summaries and masked rotation vectors."""

from __future__ import annotations

import argparse
import math
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


SOURCE_ID = "gaia.dr3.variability"
SUMMARY_TABLES = (
    "gaia_dr3_variability_summary_v2",
    "gaia_dr3_variability_summary_uncertain_distance_supplement_v1",
)
ROTATION_TABLES = (
    "gaia_dr3_rotation_modulation_v2",
    "gaia_dr3_rotation_modulation_uncertain_distance_supplement_v1",
)
EXPECTED_FIELDS = {**{name: 68 for name in SUMMARY_TABLES}, **{name: 66 for name in ROTATION_TABLES}}
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/"
    "e4_gaia_variability_source_audit.json"
)


def field_roles(
    columns: list[dict[str, Any]], *, vector_fields: list[str] | None = None
) -> dict[str, list[str]]:
    names = [str(field["name"]) for field in columns]
    vectors = set(vector_fields or [])
    identity = {"solution_id", "source_id"} & set(names)
    membership = {name for name in names if name.startswith("in_vari_")}
    cardinality = {"num_segments", "num_outliers"} & set(names)
    assigned = identity | membership | cardinality | vectors
    return {
        "identity": sorted(identity),
        "membership_flags": sorted(membership),
        "cardinalities": sorted(cardinality),
        "masked_vectors": sorted(vectors),
        "scalar_solution_fields": sorted(set(names) - assigned),
    }


def parse_vector(raw: Any) -> tuple[list[float | None], bool]:
    if raw is None:
        return [], False
    text = str(raw).strip()
    if len(text) < 2 or not text.startswith("[") or not text.endswith("]"):
        return [], True
    values: list[float | None] = []
    for token in text[1:-1].split():
        if token == "--":
            values.append(None)
            continue
        try:
            value = float(token)
        except ValueError:
            return [], True
        if not math.isfinite(value):
            return [], True
        values.append(value)
    return values, False


def query_dicts(
    con: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[str],
) -> list[dict[str, Any]]:
    result = con.execute(query, parameters)
    columns = [str(row[0]) for row in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def audit(typed_root: Path, typed_manifest: dict[str, Any]) -> dict[str, Any]:
    tables = {str(row["source_name"]): row for row in typed_manifest["tables"]}
    required_tables = set(SUMMARY_TABLES + ROTATION_TABLES)
    missing_tables = sorted(required_tables - set(tables))
    pending_tables = sorted(
        name for name, row in tables.items() if str(row.get("status")) != "typed"
    )
    field_counts = {
        name: len(row.get("columns") or [])
        for name, row in tables.items()
        if name in required_tables
    }
    checks: dict[str, Any] = {
        "missing_required_tables": missing_tables,
        "pending_typed_tables": pending_tables,
        "wrong_field_counts": {
            name: count
            for name, count in field_counts.items()
            if count != EXPECTED_FIELDS[name]
        },
    }
    summaries: dict[str, Any] = {"tables": []}
    if not any(checks.values()):
        summary_schemas = [
            [(field["name"], field["type"]) for field in tables[name]["columns"]]
            for name in SUMMARY_TABLES
        ]
        rotation_schemas = [
            [(field["name"], field["type"]) for field in tables[name]["columns"]]
            for name in ROTATION_TABLES
        ]
        checks["summary_branch_schema_mismatch"] = summary_schemas[0] != summary_schemas[1]
        checks["rotation_branch_schema_mismatch"] = rotation_schemas[0] != rotation_schemas[1]
        con = duckdb.connect()
        con.execute("set threads=4")
        con.execute("set memory_limit='8GB'")
        total_rows = 0
        null_source_ids = 0
        duplicate_source_ids = 0
        unexpected_solution_ids = 0
        vector_length_mismatches = 0
        invalid_vector_tokens = 0
        null_vector_values = 0
        masked_vector_values = 0
        nonpositive_best_periods = 0
        negative_best_period_errors = 0
        invalid_segment_period_errors = 0
        invalid_segment_fap = 0
        expected_solution_id: str | None = None
        try:
            for table_name in SUMMARY_TABLES:
                path = typed_root / str(tables[table_name]["parquet_path"])
                row = query_dicts(
                    con,
                    "select count(*)::bigint row_count, "
                    "count(*) filter(where source_id is null)::bigint null_source_ids, "
                    "count(*)-count(distinct source_id)::bigint duplicate_source_ids "
                    "from read_parquet(?)",
                    [str(path)],
                )[0]
                solutions = [
                    str(value[0])
                    for value in con.execute(
                        "select distinct solution_id from read_parquet(?) order by 1",
                        [str(path)],
                    ).fetchall()
                ]
                if expected_solution_id is None and len(solutions) == 1:
                    expected_solution_id = solutions[0]
                unexpected_solution_ids += len(
                    [value for value in solutions if value != expected_solution_id]
                )
                membership_fields = [
                    str(field["name"])
                    for field in tables[table_name]["columns"]
                    if str(field["name"]).startswith("in_vari_")
                ]
                counts = query_dicts(
                    con,
                    "select "
                    + ",".join(
                        f"count(*) filter(where \"{field}\")::bigint as \"{field}\""
                        for field in membership_fields
                    )
                    + " from read_parquet(?)",
                    [str(path)],
                )[0]
                actual_rows = int(row["row_count"])
                checks[f"{table_name}_manifest_row_count_delta"] = actual_rows - int(
                    tables[table_name]["row_count"]
                )
                total_rows += actual_rows
                null_source_ids += int(row["null_source_ids"])
                duplicate_source_ids += int(row["duplicate_source_ids"])
                summaries["tables"].append(
                    {
                        "source_table": table_name,
                        "row_count": actual_rows,
                        "field_count": field_counts[table_name],
                        "solution_ids": solutions,
                        "membership_flag_true_counts": counts,
                        "field_roles": field_roles(tables[table_name]["columns"]),
                    }
                )
            for table_name in ROTATION_TABLES:
                path = typed_root / str(tables[table_name]["parquet_path"])
                vector_fields = [
                    str(field["name"])
                    for field in tables[table_name]["columns"]
                    if str(field["type"]) == "VARCHAR"
                ]
                selected = [
                    "source_id",
                    "solution_id",
                    "num_segments",
                    "num_outliers",
                    "best_rotation_period",
                    "best_rotation_period_error",
                    *vector_fields,
                ]
                result = con.execute(
                    "select " + ",".join(f'\"{field}\"' for field in selected)
                    + " from read_parquet(?) order by source_id",
                    [str(path)],
                )
                columns = [str(row[0]) for row in result.description]
                row_count = 0
                source_ids: set[int] = set()
                solution_ids: set[str] = set()
                minimum_period: float | None = None
                maximum_period: float | None = None
                maximum_segments = 0
                while batch := result.fetchmany(2048):
                    for raw_row in batch:
                        row = dict(zip(columns, raw_row, strict=True))
                        row_count += 1
                        if row["source_id"] is None:
                            null_source_ids += 1
                        else:
                            source_ids.add(int(row["source_id"]))
                        solution_ids.add(str(row["solution_id"]))
                        segments = int(row["num_segments"])
                        outliers = int(row["num_outliers"])
                        maximum_segments = max(maximum_segments, segments)
                        period = float(row["best_rotation_period"])
                        error = float(row["best_rotation_period_error"])
                        nonpositive_best_periods += period <= 0
                        negative_best_period_errors += error < 0
                        minimum_period = period if minimum_period is None else min(minimum_period, period)
                        maximum_period = period if maximum_period is None else max(maximum_period, period)
                        for field in vector_fields:
                            if row[field] is None:
                                null_vector_values += 1
                                continue
                            values, invalid = parse_vector(row[field])
                            invalid_vector_tokens += invalid
                            if invalid:
                                continue
                            expected_length = outliers if field == "outliers_time" else segments
                            vector_length_mismatches += len(values) != expected_length
                            masked_vector_values += sum(value is None for value in values)
                            if field == "segments_rotation_period_error":
                                invalid_segment_period_errors += sum(
                                    value is not None and value < 0 for value in values
                                )
                            if field == "segments_rotation_period_fap":
                                invalid_segment_fap += sum(
                                    value is not None and not 0 <= value <= 1 for value in values
                                )
                duplicate_source_ids += row_count - len(source_ids)
                unexpected_solution_ids += len(
                    [value for value in solution_ids if value != expected_solution_id]
                )
                checks[f"{table_name}_manifest_row_count_delta"] = row_count - int(
                    tables[table_name]["row_count"]
                )
                total_rows += row_count
                summaries["tables"].append(
                    {
                        "source_table": table_name,
                        "row_count": row_count,
                        "field_count": field_counts[table_name],
                        "solution_ids": sorted(solution_ids),
                        "rotation_ranges": {
                            "minimum_best_period_days": minimum_period,
                            "maximum_best_period_days": maximum_period,
                            "maximum_segments": maximum_segments,
                        },
                        "vector_contract": {
                            "vector_fields": len(vector_fields),
                            "vector_field_names": sorted(vector_fields),
                            "segment_count_field": "num_segments",
                            "outlier_count_field": "num_outliers",
                            "mask_marker": "--",
                        },
                        "field_roles": field_roles(
                            tables[table_name]["columns"],
                            vector_fields=vector_fields,
                        ),
                    }
                )
        finally:
            con.close()
        checks.update(
            {
                "null_source_ids": null_source_ids,
                "duplicate_source_ids": duplicate_source_ids,
                "unexpected_solution_ids": unexpected_solution_ids,
                "vector_length_mismatches": vector_length_mismatches,
                "invalid_vector_tokens": invalid_vector_tokens,
                "nonpositive_best_periods": nonpositive_best_periods,
                "negative_best_period_errors": negative_best_period_errors,
                "invalid_segment_period_errors": invalid_segment_period_errors,
                "invalid_segment_fap": invalid_segment_fap,
            }
        )
        summaries.update(
            {
                "row_count": total_rows,
                "field_occurrences": sum(field_counts.values()),
                "null_vector_values": null_vector_values,
                "masked_vector_values": masked_vector_values,
                "representation_decision": {
                    "summary": "coherent per-source variability-summary parameter set",
                    "rotation": "coherent per-source rotation solution with typed masked vectors",
                    "source_array_mask": "-- maps to null while E1 retains exact strings",
                },
            }
        )
    failed = any(bool(value) for value in checks.values())
    return {
        "schema_version": "spacegate.gaia_dr3_variability_source_audit.v1",
        "status": "fail" if failed else "pass",
        "source_id": SOURCE_ID,
        "release_id": typed_manifest["release_id"],
        "raw_snapshot_id": typed_manifest["snapshot_id"],
        "typed_snapshot_id": typed_manifest["typed_snapshot_id"],
        "typed_content_sha256": typed_manifest["content_sha256"],
        "checks": checks,
        **summaries,
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
    print(
        f"Gaia variability source audit {report['status']}: "
        f"rows={report.get('row_count', 0):,} "
        f"masked_values={report.get('masked_vector_values', 0):,}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
