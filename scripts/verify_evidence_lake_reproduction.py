#!/usr/bin/env python3
"""Rebuild Evidence Lake typed snapshots from raw into a clean temporary root."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from evidence_lake_registry import DEFAULT_REGISTRY, load_json, validate_registry
from evidence_lake_store import (
    build_typed_snapshot,
    latest_snapshot_dir,
    selected_sources,
    utc_now,
)


def table_fingerprint(table: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_name": table.get("source_name"),
        "status": table.get("status"),
        "row_count": table.get("row_count"),
        "columns": table.get("columns"),
        "bytes": table.get("bytes"),
        "sha256": table.get("sha256"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument(
        "--scratch-parent",
        type=Path,
        help="Parent for the clean temporary typed root; defaults to state/tmp.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_json(args.registry)
    errors = validate_registry(registry)
    if errors:
        raise SystemExit("\n".join(errors))
    requested = set(args.source)
    sources = list(selected_sources(registry, requested))
    known = {str(source["source_id"]) for source in sources}
    if requested - known:
        raise SystemExit(f"unknown or inactive sources: {sorted(requested - known)}")

    state_dir = args.state_dir.resolve()
    raw_root = state_dir / "raw" / "evidence_lake_v2"
    typed_root = state_dir / "typed" / "evidence_lake_v2"
    scratch_parent = (args.scratch_parent or state_dir / "tmp").resolve()
    scratch_parent.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="evidence_lake_reproduction_", dir=scratch_parent))
    reproduced_root = scratch / "typed"
    reports: list[dict[str, Any]] = []
    try:
        for source in sources:
            raw_dir = latest_snapshot_dir(raw_root, source)
            expected = build_typed_snapshot(source, raw_dir, typed_root)
            reproduced = build_typed_snapshot(source, raw_dir, reproduced_root)
            expected_tables = [table_fingerprint(table) for table in expected["tables"]]
            reproduced_tables = [table_fingerprint(table) for table in reproduced["tables"]]
            matches = {
                "typed_snapshot_id": expected["typed_snapshot_id"]
                == reproduced["typed_snapshot_id"],
                "content_sha256": expected["content_sha256"]
                == reproduced["content_sha256"],
                "tables": expected_tables == reproduced_tables,
            }
            reports.append(
                {
                    "source_id": source["source_id"],
                    "release_id": source["release_id"],
                    "raw_snapshot_id": raw_dir.name,
                    "parser_contract_version": expected["parser_contract_version"],
                    "expected_typed_snapshot_id": expected["typed_snapshot_id"],
                    "reproduced_typed_snapshot_id": reproduced["typed_snapshot_id"],
                    "expected_content_sha256": expected["content_sha256"],
                    "reproduced_content_sha256": reproduced["content_sha256"],
                    "table_count": len(expected_tables),
                    "matches": matches,
                    "status": "pass" if all(matches.values()) else "fail",
                }
            )
            print(f"reproduce {source['source_id']} {reports[-1]['status']}")
    finally:
        shutil.rmtree(scratch)

    payload = {
        "schema_version": "spacegate.evidence_lake_reproduction.v1",
        "generated_at": utc_now(),
        "clean_temporary_root_removed": not scratch.exists(),
        "source_count": len(reports),
        "status": "pass"
        if reports and all(report["status"] == "pass" for report in reports)
        else "fail",
        "sources": reports,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0 if payload["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
