#!/usr/bin/env python3
"""Rebuild scientific evidence in scratch and compare deterministic logical hashes."""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path
from typing import Any

from compile_scientific_evidence import (
    DEFAULT_CONTRACT,
    DEFAULT_REGISTRY,
    DEFAULT_STATE,
    compile_evidence,
    load_json,
    write_json,
)


def comparison_projection(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "build_id": report["build_id"],
        "contract_version": report["contract_version"],
        "compiler_version": report["compiler_version"],
        "input_fingerprint": report["input_fingerprint"],
        "status": report["status"],
        "sources": report["sources"],
        "mapping_status_counts": report["mapping_status_counts"],
        "identifier_claim_counts_by_namespace": report[
            "identifier_claim_counts_by_namespace"
        ],
        "lifecycle_claim_counts": report["lifecycle_claim_counts"],
        "logical_content_sha256": report["logical_content_sha256"],
        "tables": report["tables"],
        "created_at": report["created_at"],
    }


def compare_reports(
    reference: dict[str, Any],
    reproduced: dict[str, Any],
) -> list[str]:
    expected = comparison_projection(reference)
    actual = comparison_projection(reproduced)
    return [
        key
        for key in expected
        if expected[key] != actual[key]
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument(
        "--reference-report",
        type=Path,
        default=DEFAULT_STATE
        / "reports"
        / "evidence_lake_v2"
        / "e4_scientific_evidence_foundation.json",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_STATE
        / "reports"
        / "evidence_lake_v2"
        / "e4_scientific_evidence_reproduction.json",
    )
    parser.add_argument(
        "--scratch-parent",
        type=Path,
        default=DEFAULT_STATE / "tmp",
    )
    args = parser.parse_args()
    reference = load_json(args.reference_report)
    args.scratch_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="scientific-evidence-reproduction.",
        dir=args.scratch_parent,
    ) as scratch_name:
        scratch = Path(scratch_name)
        reproduced = compile_evidence(
            state_dir=args.state_dir,
            contract_path=args.contract,
            registry_path=args.registry,
            selected_source_ids=set(args.source),
            report_path=scratch / "report.json",
            artifact_root=scratch / "artifacts",
        )
    differences = compare_reports(reference, reproduced)
    report = {
        "schema_version": "spacegate.scientific_evidence_reproduction.v1",
        "status": "pass" if not differences else "fail",
        "reference_report": str(args.reference_report),
        "build_id": reference["build_id"],
        "reference_logical_content_sha256": reference["logical_content_sha256"],
        "reproduced_logical_content_sha256": reproduced["logical_content_sha256"],
        "differing_sections": differences,
        "scratch_removed": not Path(scratch_name).exists(),
    }
    write_json(args.report, report)
    if differences:
        print(f"scientific evidence reproduction fail: {differences}")
        return 1
    print(f"scientific evidence reproduction pass: {reference['build_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
