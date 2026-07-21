#!/usr/bin/env python3
"""Verify that every blocking E5 source has one owned implementation batch."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DISPOSITIONS = ROOT / "config/evidence_lake/e5_source_dispositions.json"
DEFAULT_BATCHES = ROOT / "config/evidence_lake/e5_policy_batches.json"
DEFAULT_SELECTION = ROOT / "config/evidence_lake/e5_selection_policies.json"
ALLOWED_STATUSES = {"planned", "in_progress", "complete"}


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def audit(
    dispositions: dict[str, Any],
    batches: dict[str, Any],
    selection: dict[str, Any],
) -> dict[str, Any]:
    explicit = dispositions.get("explicit_dispositions") or {}
    selected = {
        str(row.get("source_id"))
        for row in selection.get("selection_sources") or []
        if row.get("source_id")
    }
    nonblocking = {
        str(source_id)
        for source_id, row in explicit.items()
        if row.get("blocks_e5") is False
    }
    resolved = selected | nonblocking
    blocking = {
        str(source_id)
        for source_id, row in explicit.items()
        if row.get("blocks_e5") is True
    }
    rows = batches.get("batches") or []
    batch_ids = [str(row.get("batch_id") or "") for row in rows]
    source_rows = [
        (str(source_id), str(row.get("batch_id") or ""))
        for row in rows
        for source_id in row.get("sources") or []
    ]
    completed_rows = [
        (str(source_id), str(row.get("batch_id") or ""))
        for row in rows
        for source_id in row.get("completed_sources") or []
    ]
    source_counts = Counter(source_id for source_id, _ in source_rows)
    completed_counts = Counter(source_id for source_id, _ in completed_rows)
    all_counts = source_counts + completed_counts
    assigned = set(source_counts)
    completed = set(completed_counts)
    unknown_dependencies = sorted(
        {
            str(dependency)
            for row in rows
            for dependency in row.get("depends_on") or []
            if dependency not in set(batch_ids)
        }
    )
    checks = {
        "schema_mismatch": int(
            batches.get("schema_version") != "spacegate.e5_policy_batches.v1"
        ),
        "disposition_version_mismatch": int(
            batches.get("disposition_version")
            != dispositions.get("disposition_version")
        ),
        "duplicate_batch_ids": sorted(
            batch_id
            for batch_id, count in Counter(batch_ids).items()
            if not batch_id or count > 1
        ),
        "duplicate_source_assignments": sorted(
            source_id for source_id, count in all_counts.items() if count > 1
        ),
        "missing_blocking_sources": sorted(blocking - assigned),
        "nonblocking_or_unknown_sources": sorted(assigned - blocking),
        "unresolved_completed_sources": sorted(completed - resolved),
        "unknown_dependencies": unknown_dependencies,
        "invalid_batch_metadata": sorted(
            str(row.get("batch_id") or "<missing>")
            for row in rows
            if row.get("status") not in ALLOWED_STATUSES
            or not row.get("output_contract")
            or not isinstance(row.get("sources"), list)
            or not isinstance(row.get("completed_sources", []), list)
            or (
                row.get("status") in {"planned", "in_progress"}
                and not row.get("sources")
            )
            or (row.get("status") == "complete" and row.get("sources"))
            or (
                row.get("status") == "complete"
                and not row.get("completed_sources")
            )
        ),
    }
    failing = {
        key: value
        for key, value in checks.items()
        if value not in (0, [], {})
    }
    return {
        "schema_version": "spacegate.e5_policy_batch_audit.v1",
        "status": "fail" if failing else "pass",
        "batch_version": batches.get("batch_version"),
        "disposition_version": dispositions.get("disposition_version"),
        "blocking_source_count": len(blocking),
        "completed_source_count": len(completed),
        "batch_count": len(rows),
        "checks": checks,
        "failing_checks": failing,
        "batches": [
            {
                "batch_id": row["batch_id"],
                "status": row["status"],
                "output_contract": row["output_contract"],
                "source_count": len(row["sources"]),
                "sources": row["sources"],
                "completed_source_count": len(row.get("completed_sources") or []),
                "completed_sources": row.get("completed_sources") or [],
            }
            for row in rows
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dispositions", type=Path, default=DEFAULT_DISPOSITIONS)
    parser.add_argument("--batches", type=Path, default=DEFAULT_BATCHES)
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit(
        load(args.dispositions),
        load(args.batches),
        load(args.selection),
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"E5 policy batches {report['status']}: "
        f"blocking={report['blocking_source_count']} "
        f"completed={report['completed_source_count']} "
        f"batches={report['batch_count']}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
