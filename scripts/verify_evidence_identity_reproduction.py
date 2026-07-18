#!/usr/bin/env python3
"""Compare two independently compiled Evidence Lake identity graph artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    )
    temp = Path(handle.name)
    try:
        with handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp, path)
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def compare_graphs(expected_path: Path, actual_path: Path) -> dict[str, Any]:
    expected = load_json(expected_path)
    actual = load_json(actual_path)
    errors: list[str] = []
    if expected.get("status") != "pass" or actual.get("status") != "pass":
        errors.append("both graph reports must have pass status")
    for key in ("schema_version", "graph_id", "policy_version", "inputs"):
        if expected.get(key) != actual.get(key):
            errors.append(f"report mismatch: {key}")
    expected_tables = expected.get("tables", {})
    actual_tables = actual.get("tables", {})
    if set(expected_tables) != set(actual_tables):
        errors.append("table set mismatch")
    table_results = []
    for table in sorted(set(expected_tables) | set(actual_tables)):
        expected_table = expected_tables.get(table, {})
        actual_table = actual_tables.get(table, {})
        mismatches = [
            key
            for key in ("row_count", "bytes", "sha256")
            if expected_table.get(key) != actual_table.get(key)
        ]
        actual_file = actual_path.parent / str(actual_table.get("path", ""))
        if not actual_file.is_file():
            mismatches.append("artifact_missing")
        elif file_sha256(actual_file) != actual_table.get("sha256"):
            mismatches.append("artifact_sha256")
        if mismatches:
            errors.append(f"table {table}: {','.join(mismatches)}")
        table_results.append(
            {
                "table": table,
                "status": "pass" if not mismatches else "fail",
                "mismatches": mismatches,
                "row_count": actual_table.get("row_count"),
                "bytes": actual_table.get("bytes"),
                "sha256": actual_table.get("sha256"),
            }
        )
    return {
        "schema_version": "spacegate.evidence_identity_reproduction.v1",
        "generated_at": utc_now(),
        "status": "pass" if not errors else "fail",
        "graph_id": expected.get("graph_id"),
        "expected_report": str(expected_path),
        "actual_report": str(actual_path),
        "errors": errors,
        "tables": table_results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--actual", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = compare_graphs(args.expected.resolve(), args.actual.resolve())
    json_write(args.report.resolve(), result)
    print(
        f"Evidence identity reproduction {result['status']}: "
        f"graph={result['graph_id']} tables={len(result['tables'])}"
    )
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
