#!/usr/bin/env python3
"""Recompile selected system placements in isolation and compare products."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import compile_selected_system_placements as compiler
import verify_selected_system_placements as verifier


DEFAULT_SCRATCH_PARENT = Path("/mnt/space/spacegate")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compare(
    reference: dict[str, Any], reproduced: dict[str, Any], audit: dict[str, Any]
) -> dict[str, Any]:
    checks = {
        "build_id_match": reference.get("build_id") == reproduced.get("build_id"),
        "policy_sha256_match": reference.get("policy_sha256") == reproduced.get("policy_sha256"),
        "compiler_sha256_match": reference.get("compiler_sha256") == reproduced.get("compiler_sha256"),
        "input_sha256_match": reference.get("input_sha256") == reproduced.get("input_sha256"),
        "input_attestation_match": reference.get("input_attestation") == reproduced.get("input_attestation"),
        "product_contract_match": reference.get("products") == reproduced.get("products"),
        "winner_counts_match": reference.get("winner_counts") == reproduced.get("winner_counts"),
        "verification_match": reference.get("verification") == reproduced.get("verification"),
        "independent_audit_pass": audit.get("status") == "pass",
    }
    return {
        "schema_version": "spacegate.selected_system_placements_reproduction.v1",
        "generated_at": utc_now(),
        "build_id": reference.get("build_id"),
        "status": "pass" if all(checks.values()) else "fail",
        "checks": checks,
        "failing_checks": sorted(name for name, passed in checks.items() if not passed),
        "reference_products": reference.get("products"),
        "reproduced_products": reproduced.get("products"),
        "reproduced_timings": reproduced.get("timings"),
        "independent_audit": audit,
        "scratch_removed": True,
    }


def reproduce(
    policy_path: Path,
    state_dir: Path,
    reference_manifest_path: Path,
    scratch_parent: Path,
) -> dict[str, Any]:
    reference = compiler.load_object(reference_manifest_path)
    scratch_parent.mkdir(parents=True, exist_ok=True)
    scratch_root = Path(tempfile.mkdtemp(
        prefix="e5-system-placement-reproduction-", dir=scratch_parent
    ))
    try:
        output_root = scratch_root / "output"
        work_root = scratch_root / "work"
        reproduced = compiler.compile_placements(
            policy_path, state_dir, work_root, output_root
        )
        reproduced_manifest = output_root / reproduced["build_id"] / "manifest.json"
        audit = verifier.verify(policy_path, state_dir, reproduced_manifest)
        report = compare(reference, reproduced, audit)
    finally:
        shutil.rmtree(scratch_root)
    report["scratch_removed"] = not scratch_root.exists()
    if not report["scratch_removed"]:
        report["status"] = "fail"
        report["failing_checks"].append("scratch_removed")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--reference-manifest", type=Path, required=True)
    parser.add_argument("--scratch-parent", type=Path, default=DEFAULT_SCRATCH_PARENT)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = reproduce(
        args.policy.resolve(), args.state_dir.resolve(),
        args.reference_manifest.resolve(), args.scratch_parent.resolve(),
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
