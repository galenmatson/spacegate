#!/usr/bin/env python3
"""Compare two parsed JPL Horizons snapshots without conflating lineage fields."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


LINEAGE_FIELDS = {
    "retrieved_at",
    "source_row_hash",
    "horizons_query_url",
    "horizons_response_path",
    "horizons_response_sha256",
    "operator_seed_version",
    "operator_seed_sha256",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_rows(path: Path) -> tuple[list[str], dict[str, dict[str, str]], int]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields = list(reader.fieldnames or [])
        rows: dict[str, dict[str, str]] = {}
        duplicate_keys = 0
        for row in reader:
            key = str(row.get("source_pk") or "").strip()
            if key in rows:
                duplicate_keys += 1
            rows[key] = {str(field): str(value or "") for field, value in row.items()}
    return fields, rows, duplicate_keys


def numeric_delta(old_raw: str, new_raw: str) -> dict[str, float] | None:
    try:
        old = float(old_raw)
        new = float(new_raw)
    except ValueError:
        return None
    absolute = new - old
    relative = absolute / old if old else None
    result: dict[str, float] = {"absolute": absolute}
    if relative is not None:
        result["relative"] = relative
    return result


def compare(source_id: str, old_path: Path, new_path: Path) -> dict[str, Any]:
    old_fields, old_rows, old_duplicates = read_rows(old_path)
    new_fields, new_rows, new_duplicates = read_rows(new_path)
    old_keys = set(old_rows)
    new_keys = set(new_rows)
    common_fields = sorted(set(old_fields) & set(new_fields))
    scientific_fields = [
        field
        for field in common_fields
        if field not in LINEAGE_FIELDS and field != "source_pk"
    ]
    lineage_fields = [field for field in common_fields if field in LINEAGE_FIELDS]
    scientific_changes: list[dict[str, Any]] = []
    lineage_change_counts: dict[str, int] = {}
    for key in sorted(old_keys & new_keys, key=lambda value: (len(value), value)):
        row_changes = []
        for field in scientific_fields:
            old_value = old_rows[key][field]
            new_value = new_rows[key][field]
            if old_value == new_value:
                continue
            row_changes.append(
                {
                    "field": field,
                    "old": old_value,
                    "new": new_value,
                    "numeric_delta": numeric_delta(old_value, new_value),
                }
            )
        if row_changes:
            scientific_changes.append(
                {
                    "source_pk": key,
                    "object_name": new_rows[key].get("object_name")
                    or old_rows[key].get("object_name"),
                    "changes": row_changes,
                }
            )
        for field in lineage_fields:
            if old_rows[key][field] != new_rows[key][field]:
                lineage_change_counts[field] = lineage_change_counts.get(field, 0) + 1
    field_change_counts: dict[str, int] = {}
    for row in scientific_changes:
        for change in row["changes"]:
            field = str(change["field"])
            field_change_counts[field] = field_change_counts.get(field, 0) + 1
    checks = {
        "old_duplicate_source_pk_excess": old_duplicates,
        "new_duplicate_source_pk_excess": new_duplicates,
        "blank_old_source_pk": int("" in old_keys),
        "blank_new_source_pk": int("" in new_keys),
    }
    status = "pass" if not any(checks.values()) else "fail"
    return {
        "schema_version": "spacegate.horizons_snapshot_delta.v1",
        "status": status,
        "source_id": source_id,
        "old": {
            "path": str(old_path),
            "bytes": old_path.stat().st_size,
            "sha256": sha256_file(old_path),
            "row_count": len(old_rows),
        },
        "new": {
            "path": str(new_path),
            "bytes": new_path.stat().st_size,
            "sha256": sha256_file(new_path),
            "row_count": len(new_rows),
        },
        "checks": checks,
        "schema_delta": {
            "added_fields": sorted(set(new_fields) - set(old_fields)),
            "removed_fields": sorted(set(old_fields) - set(new_fields)),
        },
        "identity_delta": {
            "added_source_pk": sorted(new_keys - old_keys),
            "removed_source_pk": sorted(old_keys - new_keys),
            "common_source_pk_count": len(old_keys & new_keys),
        },
        "scientific_delta": {
            "changed_object_count": len(scientific_changes),
            "field_change_counts": dict(sorted(field_change_counts.items())),
            "objects": scientific_changes,
        },
        "lineage_change_counts": dict(sorted(lineage_change_counts.items())),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--old", type=Path, required=True)
    parser.add_argument("--new", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = compare(args.source_id, args.old, args.new)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.report.with_suffix(args.report.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temporary.replace(args.report)
    print(
        f"Horizons snapshot delta {report['status']}: "
        f"source={args.source_id} old={report['old']['row_count']} "
        f"new={report['new']['row_count']} "
        f"changed={report['scientific_delta']['changed_object_count']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
