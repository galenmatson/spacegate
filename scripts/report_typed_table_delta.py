#!/usr/bin/env python3
"""Compare two source-native typed Parquet tables by stable source keys."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def table_schema(
    con: duckdb.DuckDBPyConnection, path: Path
) -> list[dict[str, str]]:
    return [
        {"name": str(row[0]), "type": str(row[1])}
        for row in con.execute(
            "describe select * from read_parquet(?)", [str(path)]
        ).fetchall()
    ]


def count_scalar(
    con: duckdb.DuckDBPyConnection, query: str, paths: list[Path]
) -> int:
    return int(con.execute(query, [str(path) for path in paths]).fetchone()[0])


def key_sample(
    con: duckdb.DuckDBPyConnection,
    query: str,
    paths: list[Path],
    keys: list[str],
) -> list[dict[str, str | None]]:
    return [
        {
            key: None if value is None else str(value)
            for key, value in zip(keys, row, strict=True)
        }
        for row in con.execute(query, [str(path) for path in paths]).fetchall()
    ]


def compare(
    *,
    source_id: str,
    old_path: Path,
    new_path: Path,
    keys: list[str],
    lineage_fields: list[str],
    sample_limit: int = 100,
) -> dict[str, Any]:
    if not keys:
        raise ValueError("at least one stable source key is required")
    con = duckdb.connect()
    con.execute("set threads=4")
    con.execute("set memory_limit='8GB'")
    try:
        old_schema = table_schema(con, old_path)
        new_schema = table_schema(con, new_path)
        old_names = [field["name"] for field in old_schema]
        new_names = [field["name"] for field in new_schema]
        missing_keys = sorted(set(keys) - (set(old_names) & set(new_names)))
        if missing_keys:
            raise ValueError(f"stable keys absent from a table: {missing_keys}")
        unknown_lineage = sorted(
            set(lineage_fields) - (set(old_names) | set(new_names))
        )
        if unknown_lineage:
            raise ValueError(f"lineage fields absent from both tables: {unknown_lineage}")

        key_sql = ",".join(quote_identifier(key) for key in keys)
        distinct_key_sql = (
            quote_identifier(keys[0])
            if len(keys) == 1
            else "(" + key_sql + ")"
        )
        blank_key_sql = " or ".join(
            f"{quote_identifier(key)} is null or "
            f"nullif(trim(cast({quote_identifier(key)} as varchar)),'') is null"
            for key in keys
        )
        old_count = count_scalar(
            con, "select count(*) from read_parquet(?)", [old_path]
        )
        new_count = count_scalar(
            con, "select count(*) from read_parquet(?)", [new_path]
        )
        checks = {
            "old_duplicate_key_excess": count_scalar(
                con,
                f"select count(*)-count(distinct {distinct_key_sql}) "
                "from read_parquet(?)",
                [old_path],
            ),
            "new_duplicate_key_excess": count_scalar(
                con,
                f"select count(*)-count(distinct {distinct_key_sql}) "
                "from read_parquet(?)",
                [new_path],
            ),
            "old_blank_key_rows": count_scalar(
                con,
                f"select count(*) from read_parquet(?) where {blank_key_sql}",
                [old_path],
            ),
            "new_blank_key_rows": count_scalar(
                con,
                f"select count(*) from read_parquet(?) where {blank_key_sql}",
                [new_path],
            ),
        }

        common_fields = [
            field for field in old_names if field in set(new_names) and field not in keys
        ]
        scientific_fields = [
            field for field in common_fields if field not in set(lineage_fields)
        ]
        shared_lineage_fields = [
            field for field in common_fields if field in set(lineage_fields)
        ]
        comparisons = {
            field: (
                f'o.{quote_identifier(field)} is distinct from '
                f'n.{quote_identifier(field)}'
            )
            for field in common_fields
        }
        aggregate_fields = [
            f"count(*) filter(where {comparisons[field]})::bigint "
            f"as {quote_identifier(field)}"
            for field in common_fields
        ]
        changed_counts: dict[str, int] = {}
        changed_any_row_count = 0
        changed_scientific_row_count = 0
        changed_lineage_row_count = 0
        if aggregate_fields:
            result = con.execute(
                "select "
                + ",".join(aggregate_fields)
                + " from read_parquet(?) o join read_parquet(?) n using("
                + key_sql
                + ")",
                [str(old_path), str(new_path)],
            )
            values = result.fetchone()
            changed_counts = {
                field: int(value)
                for field, value in zip(common_fields, values, strict=True)
            }
            def changed_rows(fields: list[str]) -> int:
                if not fields:
                    return 0
                return count_scalar(
                    con,
                    "select count(*) from read_parquet(?) o join read_parquet(?) n using("
                    + key_sql
                    + ") where "
                    + " or ".join(comparisons[field] for field in fields),
                    [old_path, new_path],
                )

            changed_any_row_count = changed_rows(common_fields)
            changed_scientific_row_count = changed_rows(scientific_fields)
            changed_lineage_row_count = changed_rows(shared_lineage_fields)

        added_count = count_scalar(
            con,
            "select count(*) from read_parquet(?) n anti join read_parquet(?) o using("
            + key_sql
            + ")",
            [new_path, old_path],
        )
        removed_count = count_scalar(
            con,
            "select count(*) from read_parquet(?) o anti join read_parquet(?) n using("
            + key_sql
            + ")",
            [old_path, new_path],
        )
        common_count = count_scalar(
            con,
            "select count(*) from read_parquet(?) o join read_parquet(?) n using("
            + key_sql
            + ")",
            [old_path, new_path],
        )
        order_sql = ",".join(f"cast({quote_identifier(key)} as varchar)" for key in keys)
        sample_select = ",".join(quote_identifier(key) for key in keys)
        added_sample = key_sample(
            con,
            "select "
            + sample_select
            + " from read_parquet(?) n anti join read_parquet(?) o using("
            + key_sql
            + f") order by {order_sql} limit {int(sample_limit)}",
            [new_path, old_path],
            keys,
        )
        removed_sample = key_sample(
            con,
            "select "
            + sample_select
            + " from read_parquet(?) o anti join read_parquet(?) n using("
            + key_sql
            + f") order by {order_sql} limit {int(sample_limit)}",
            [old_path, new_path],
            keys,
        )
        scientific_changed_sample: list[dict[str, str | None]] = []
        if scientific_fields:
            scientific_changed_sample = key_sample(
                con,
                "select "
                + sample_select
                + " from read_parquet(?) o join read_parquet(?) n using("
                + key_sql
                + ") where "
                + " or ".join(comparisons[field] for field in scientific_fields)
                + f" order by {order_sql} limit {int(sample_limit)}",
                [old_path, new_path],
                keys,
            )
    finally:
        con.close()

    status = "pass" if not any(checks.values()) else "fail"
    return {
        "schema_version": "spacegate.typed_table_delta.v1",
        "status": status,
        "source_id": source_id,
        "stable_key_fields": keys,
        "old": {
            "path": str(old_path),
            "bytes": old_path.stat().st_size,
            "sha256": sha256_file(old_path),
            "row_count": old_count,
            "schema": old_schema,
        },
        "new": {
            "path": str(new_path),
            "bytes": new_path.stat().st_size,
            "sha256": sha256_file(new_path),
            "row_count": new_count,
            "schema": new_schema,
        },
        "checks": checks,
        "schema_delta": {
            "added_fields": sorted(set(new_names) - set(old_names)),
            "removed_fields": sorted(set(old_names) - set(new_names)),
        },
        "identity_delta": {
            "added_count": added_count,
            "removed_count": removed_count,
            "common_count": common_count,
            "added_sample": added_sample,
            "removed_sample": removed_sample,
            "sample_limit": sample_limit,
        },
        "value_delta": {
            "changed_any_common_row_count": changed_any_row_count,
            "changed_scientific_common_row_count": changed_scientific_row_count,
            "changed_lineage_common_row_count": changed_lineage_row_count,
            "scientific_field_change_counts": {
                field: changed_counts[field]
                for field in scientific_fields
                if changed_counts.get(field)
            },
            "lineage_field_change_counts": {
                field: changed_counts[field]
                for field in shared_lineage_fields
                if changed_counts.get(field)
            },
            "scientifically_changed_key_sample": scientific_changed_sample,
            "sample_limit": sample_limit,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--old", type=Path, required=True)
    parser.add_argument("--new", type=Path, required=True)
    parser.add_argument("--key", action="append", required=True)
    parser.add_argument("--lineage-field", action="append", default=[])
    parser.add_argument("--sample-limit", type=int, default=100)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.sample_limit < 0 or args.sample_limit > 1000:
        raise SystemExit("--sample-limit must be between 0 and 1000")
    report = compare(
        source_id=args.source_id,
        old_path=args.old,
        new_path=args.new,
        keys=args.key,
        lineage_fields=args.lineage_field,
        sample_limit=args.sample_limit,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.report.with_suffix(args.report.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temporary.replace(args.report)
    print(
        f"Typed table delta {report['status']}: source={args.source_id} "
        f"old={report['old']['row_count']:,} new={report['new']['row_count']:,} "
        f"added={report['identity_delta']['added_count']:,} "
        f"removed={report['identity_delta']['removed_count']:,} "
        f"scientifically_changed="
        f"{report['value_delta']['changed_scientific_common_row_count']:,}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
