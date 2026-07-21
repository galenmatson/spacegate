#!/usr/bin/env python3
"""Rebuild E5 selected facts in scratch and compare deterministic projections."""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path
from typing import Any

from compile_selected_facts import (
    DEFAULT_POLICY,
    atomic_json,
    compile_selected_facts,
    load_json,
)


def comparison_projection(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": report["status"],
        "build_id": report["build_id"],
        "build_sha256": report["build_sha256"],
        "policy_version": report["policy_version"],
        "evidence_release_set_id": report["evidence_release_set_id"],
        "identity_graph_id": report["identity_graph_id"],
        "canonical_reference_build_id": report["canonical_reference_build_id"],
        "table_counts": report["table_counts"],
        "integrity_checks": report["integrity_checks"],
        "logical_content_sha256": report["logical_content_sha256"],
        "parquet_files": {
            name: metadata
            for name, metadata in report["files"].items()
            if name.endswith(".parquet")
        },
    }


def compare_reports(reference: dict[str, Any], reproduced: dict[str, Any]) -> list[str]:
    expected = comparison_projection(reference)
    actual = comparison_projection(reproduced)
    return [key for key in expected if expected[key] != actual[key]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument(
        "--reference-report",
        type=Path,
        default=Path(
            "/data/spacegate/state/reports/evidence_lake_v2/"
            "e5_selected_facts_v2_partitioned.json"
        ),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(
            "/data/spacegate/state/reports/evidence_lake_v2/"
            "e5_selected_facts_v2_reproduction.json"
        ),
    )
    parser.add_argument(
        "--scratch-parent",
        type=Path,
        default=Path("/mnt/space/spacegate/e5-selected-fact-reproduction"),
    )
    parser.add_argument("--memory-limit", default="48GB")
    parser.add_argument("--threads", type=int, default=12)
    args = parser.parse_args()

    reference = load_json(args.reference_report)
    args.scratch_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="selected-fact-reproduction.",
        dir=args.scratch_parent,
    ) as scratch_name:
        scratch = Path(scratch_name)
        reproduced = compile_selected_facts(
            state_dir=args.state_dir,
            policy_path=args.policy,
            artifact_root=scratch / "artifacts",
            report_path=scratch / "compile-report.json",
            memory_limit=args.memory_limit,
            threads=args.threads,
            temp_directory=scratch / "spill",
        )
    differences = compare_reports(reference, reproduced)
    report = {
        "schema_version": "spacegate.selected_fact_reproduction.v1",
        "status": "pass" if not differences else "fail",
        "reference_report": str(args.reference_report),
        "build_id": reference["build_id"],
        "reference_logical_content_sha256": reference["logical_content_sha256"],
        "reproduced_logical_content_sha256": reproduced["logical_content_sha256"],
        "differing_sections": differences,
        "scratch_removed": not Path(scratch_name).exists(),
    }
    atomic_json(args.report, report)
    if differences:
        print(f"selected-fact reproduction fail: {differences}")
        return 1
    print(f"selected-fact reproduction pass: {reference['build_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
