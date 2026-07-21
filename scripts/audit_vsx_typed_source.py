#!/usr/bin/env python3
"""Audit the pinned source-native VSX tables before E4 materialization."""

from __future__ import annotations

import argparse
import json
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


SOURCE_ID = "classification.vsx"
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/e4_vsx_typed_source_audit.json"
)
REQUIRED_OBJECT_FIELDS = {
    "source_line_number",
    "OID",
    "Name",
    "V",
    "RAdeg",
    "DEdeg",
    "Type",
    "l_max",
    "max",
    "u_max",
    "n_max",
    "f_min",
    "l_min",
    "min",
    "u_min",
    "n_min",
    "Epoch",
    "u_Epoch",
    "l_Period",
    "Period",
    "u_Period",
    "Sp",
    "raw_row",
}


def query_row(
    con: duckdb.DuckDBPyConnection, query: str, path: Path
) -> dict[str, Any]:
    result = con.execute(query, [str(path)])
    columns = [str(row[0]) for row in result.description]
    return dict(zip(columns, result.fetchone(), strict=True))


def query_rows(
    con: duckdb.DuckDBPyConnection, query: str, path: Path
) -> list[dict[str, Any]]:
    result = con.execute(query, [str(path)])
    columns = [str(row[0]) for row in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def audit(typed_root: Path, typed_manifest: dict[str, Any]) -> dict[str, Any]:
    tables = {str(row["source_name"]): row for row in typed_manifest["tables"]}
    missing_tables = sorted({"vsx_dat", "vsx_readme"} - set(tables))
    pending_tables = sorted(
        name for name, row in tables.items() if str(row.get("status")) != "typed"
    )
    object_fields = {
        str(row["name"]) for row in tables.get("vsx_dat", {}).get("columns", [])
    }
    missing_object_fields = sorted(REQUIRED_OBJECT_FIELDS - object_fields)
    hard_checks: dict[str, Any] = {
        "missing_required_tables": missing_tables,
        "pending_typed_tables": pending_tables,
        "missing_required_object_fields": missing_object_fields,
    }
    summaries: dict[str, Any] = {
        "typed_tables": sorted(tables),
        "field_occurrences": sum(len(row.get("columns") or []) for row in tables.values()),
    }
    if not missing_tables and not missing_object_fields:
        path = typed_root / str(tables["vsx_dat"]["parquet_path"])
        con = duckdb.connect()
        try:
            identity = query_row(
                con,
                """
                select
                  count(*)::bigint as row_count,
                  count(*) filter (where nullif(trim(OID), '') is null)::bigint
                    as missing_oid,
                  (count(*) - count(distinct trim(OID)))::bigint
                    as duplicate_oid_excess,
                  count(*) filter (where nullif(trim(Name), '') is null)::bigint
                    as missing_name,
                  (count(*) - count(distinct upper(trim(Name))))::bigint
                    as duplicate_public_name_excess,
                  count(*) filter (
                    where try_cast(trim(RAdeg) as double) not between 0 and 360
                       or try_cast(trim(DEdeg) as double) not between -90 and 90
                  )::bigint as invalid_coordinates,
                  count(*) filter (where trim(V) not in ('0', '1', '2', '3'))::bigint
                    as invalid_status
                from read_parquet(?)
                """,
                path,
            )
            coverage = query_row(
                con,
                """
                select
                  count(*) filter (where nullif(trim(Type), '') is not null)::bigint
                    as variability_type_rows,
                  count(distinct trim(Type))::bigint as variability_type_lexemes,
                  count(*) filter (where try_cast(trim(Period) as double) is not null)::bigint
                    as period_rows,
                  count(*) filter (where try_cast(trim(Period) as double) <= 0)::bigint
                    as nonpositive_period_rows,
                  count(*) filter (where nullif(trim(Sp), '') is not null)::bigint
                    as spectral_classification_rows,
                  count(*) filter (where try_cast(trim(max) as double) is not null)::bigint
                    as maximum_or_amplitude_rows,
                  count(*) filter (where try_cast(trim(min) as double) is not null)::bigint
                    as minimum_or_amplitude_rows,
                  count(*) filter (where try_cast(trim(Epoch) as double) is not null)::bigint
                    as epoch_rows
                from read_parquet(?)
                """,
                path,
            )
            statuses = query_rows(
                con,
                """
                select trim(V) as status_code, count(*)::bigint as row_count
                from read_parquet(?) group by 1 order by 1
                """,
                path,
            )
            name_collisions = query_rows(
                con,
                """
                select
                  upper(trim(Name)) as normalized_name,
                  count(*)::bigint as row_count,
                  list(trim(OID) order by try_cast(trim(OID) as bigint)) as vsx_oids
                from read_parquet(?)
                group by 1 having count(*) > 1 order by 1
                """,
                path,
            )
            flags = query_row(
                con,
                """
                select
                  count(*) filter (where trim(l_max) not in ('', '<', '>'))::bigint
                    as invalid_maximum_limit_flags,
                  count(*) filter (where trim(l_min) not in ('', '<', '>'))::bigint
                    as invalid_minimum_limit_flags,
                  count(*) filter (where trim(l_Period) not in ('', '<', '>', '('))::bigint
                    as invalid_period_limit_flags,
                  count(*) filter (where trim(u_max) not in ('', ':'))::bigint
                    as invalid_maximum_uncertainty_flags,
                  count(*) filter (where trim(u_min) not in ('', ':'))::bigint
                    as invalid_minimum_uncertainty_flags,
                  count(*) filter (where trim(u_Epoch) not in ('', ':'))::bigint
                    as invalid_epoch_uncertainty_flags,
                  count(*) filter (
                    where trim(u_Period) <> '' and trim(u_Period) <> ':'
                      and trim(u_Period) <> ')'
                      and not regexp_full_match(trim(u_Period), '[*/][0-9]+')
                  )::bigint as invalid_period_uncertainty_flags,
                  count(*) filter (where trim(f_min) not in ('', 'Y'))::bigint
                    as invalid_amplitude_flags
                from read_parquet(?)
                """,
                path,
            )
        finally:
            con.close()
        expected_rows = int(tables["vsx_dat"]["row_count"])
        hard_checks.update(
            {
                "manifest_row_count_delta": int(identity["row_count"]) - expected_rows,
                "missing_oid": int(identity["missing_oid"]),
                "duplicate_oid_excess": int(identity["duplicate_oid_excess"]),
                "missing_name": int(identity["missing_name"]),
                "invalid_coordinates": int(identity["invalid_coordinates"]),
                "invalid_status": int(identity["invalid_status"]),
                "nonpositive_period_rows": int(coverage["nonpositive_period_rows"]),
                **{key: int(value) for key, value in flags.items()},
            }
        )
        summaries.update(
            {
                "identity": identity,
                "scientific_coverage": coverage,
                "status_counts": statuses,
                "public_name_collisions": name_collisions,
            }
        )
    bibliography_tables = sorted(
        name for name in tables if name in {"vsx_refs", "refs_dat", "vsx_references"}
    )
    incomplete_checks = {
        "missing_source_bibliography_table": not bibliography_tables,
    }
    hard_failure = any(
        bool(value) if isinstance(value, list) else int(value) != 0
        for value in hard_checks.values()
    )
    status = "fail" if hard_failure else "incomplete" if any(incomplete_checks.values()) else "pass"
    return {
        "schema_version": "spacegate.vsx_typed_source_audit.v1",
        "status": status,
        "source_id": SOURCE_ID,
        "release_id": typed_manifest["release_id"],
        "raw_snapshot_id": typed_manifest["snapshot_id"],
        "typed_snapshot_id": typed_manifest["typed_snapshot_id"],
        "typed_content_sha256": typed_manifest["content_sha256"],
        "checks": hard_checks,
        "incomplete_checks": incomplete_checks,
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
    print(
        f"VSX typed source {report['status']}: "
        f"rows={report['summaries'].get('identity', {}).get('row_count', 0):,} "
        f"name_collisions={len(report['summaries'].get('public_name_collisions', []))}"
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
