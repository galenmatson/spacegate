#!/usr/bin/env python3
"""Compare two clean Smart Tag builds and emit a deterministic-rebuild report."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def compare(first_root: Path, second_root: Path) -> dict[str, Any]:
    first = load(first_root.resolve(strict=True) / "manifest.json")
    second = load(second_root.resolve(strict=True) / "manifest.json")
    compared = {
        "build_id": first.get("build_id") == second.get("build_id"),
        "registry_hash": first.get("registry_hash") == second.get("registry_hash"),
        "compiler_version": (
            first.get("compiler_version") == second.get("compiler_version")
        ),
        "input_lineage": (
            first.get("input_lineage") == second.get("input_lineage")
        ),
        "counts": first.get("counts") == second.get("counts"),
        "logical_hashes": (
            first.get("logical_hashes") == second.get("logical_hashes")
        ),
        "database_sha256": (
            first.get("artifacts", {}).get("database", {}).get("sha256")
            == second.get("artifacts", {}).get("database", {}).get("sha256")
        ),
        "assignments_sha256": (
            first.get("artifacts", {}).get("assignments", {}).get("sha256")
            == second.get("artifacts", {}).get("assignments", {}).get("sha256")
        ),
        "source_contributions_sha256": (
            first.get("artifacts", {})
            .get("source_contributions", {})
            .get("sha256")
            == second.get("artifacts", {})
            .get("source_contributions", {})
            .get("sha256")
        ),
    }
    return {
        "schema_version": "spacegate.smart_tag_determinism_report.v1",
        "status": "pass" if all(compared.values()) else "fail",
        "build_id": first.get("build_id"),
        "registry_hash": first.get("registry_hash"),
        "compiler_version": first.get("compiler_version"),
        "comparisons": compared,
        "first": str(first_root.resolve()),
        "second": str(second_root.resolve()),
        "counts": first.get("counts"),
        "logical_hashes": first.get("logical_hashes"),
        "physical_artifacts": {
            key: {
                "bytes": first.get("artifacts", {}).get(key, {}).get("bytes"),
                "sha256": first.get("artifacts", {}).get(key, {}).get("sha256"),
            }
            for key in ("database", "assignments", "source_contributions")
        },
        "timings_seconds": {
            "first": first.get("timings"),
            "second": second.get("timings"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first", required=True, type=Path)
    parser.add_argument("--second", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = compare(args.first, args.second)
        atomic_json(args.output.resolve(), report)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Smart Tag comparison failed: {exc}") from exc
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
