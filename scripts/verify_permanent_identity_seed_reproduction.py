#!/usr/bin/env python3
"""Recompile the E7 permanent identity seed and compare deterministic products."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import compile_permanent_identity_seed as compiler


DEFAULT_SCRATCH_PARENT = Path("/mnt/space/spacegate")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compare(reference: dict[str, Any], reproduced: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "seed_id_match": reference.get("seed_id") == reproduced.get("seed_id"),
        "policy_sha256_match": reference.get("policy_sha256") == reproduced.get("policy_sha256"),
        "identity_graph_sha256_match": reference.get("identity_graph_sha256") == reproduced.get("identity_graph_sha256"),
        "hierarchy_source_sha256_match": reference.get("hierarchy_source_sha256") == reproduced.get("hierarchy_source_sha256"),
        "product_contract_match": reference.get("products") == reproduced.get("products"),
        "verification_match": reference.get("verification") == reproduced.get("verification"),
        "scientific_authority_false": reproduced.get("scientific_authority") is False,
    }
    return {
        "schema_version": "spacegate.permanent_identity_seed_reproduction.v1",
        "generated_at": utc_now(),
        "seed_id": reference.get("seed_id"),
        "status": "pass" if all(checks.values()) else "fail",
        "checks": checks,
        "failing_checks": sorted(key for key, passed in checks.items() if not passed),
        "reference_products": reference.get("products"),
        "reproduced_products": reproduced.get("products"),
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
    scratch_root = Path(tempfile.mkdtemp(prefix="e7-identity-seed-reproduction-", dir=scratch_parent))
    try:
        reproduced = compiler.compile_seed(policy_path, state_dir, scratch_root)
        report = compare(reference, reproduced)
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
        args.policy.resolve(),
        args.state_dir.resolve(),
        args.reference_manifest.resolve(),
        args.scratch_parent.resolve(),
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
