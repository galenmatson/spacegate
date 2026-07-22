#!/usr/bin/env python3
"""Rebuild the E7 clean foundation in isolation and compare every artifact hash."""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import compile_e7_clean_foundation as compiler
import verify_e7_clean_foundation as verifier


DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/e7_clean_foundation_reproduction.json"
)


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def reproduce(
    *, policy_path: Path, state_dir: Path, artifact_root: Path, build_id: str,
    scratch_parent: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    reference_dir = artifact_root / build_id
    reference = load_object(reference_dir / "manifest.json")
    scratch = Path(tempfile.mkdtemp(prefix="e7-clean-foundation-reproduction-", dir=scratch_parent))
    output_root = scratch / "artifacts"
    try:
        rebuilt = compiler.compile_foundation(
            policy_path, state_dir, output_root, link_into_state=False
        )
        rebuilt_dir = output_root / rebuilt["build_id"]
        independent = verifier.verify(rebuilt_dir)
        reference_products = reference.get("products") or {}
        rebuilt_products = rebuilt.get("products") or {}
        byte_exact_products = sorted(
            key for key, spec in reference_products.items()
            if spec.get("determinism") == "byte_exact"
        )
        query_databases = sorted(
            key for key, spec in reference_products.items()
            if spec.get("determinism") == "logical_tables"
        )
        differing_byte_exact_products = sorted(
            key for key in byte_exact_products
            if reference_products.get(key) != rebuilt_products.get(key)
        )
        differing_database_containers = sorted(
            key for key in query_databases
            if reference_products.get(key) != rebuilt_products.get(key)
        )
        checks = {
            "build_id_match": rebuilt.get("build_id") == reference.get("build_id") == build_id,
            "canonical_parquet_hashes_match": not differing_byte_exact_products,
            "database_logical_verification_pass": independent.get("status") == "pass",
            "counts_match": rebuilt.get("counts") == reference.get("counts"),
            "accounting_match": rebuilt.get("accounting") == reference.get("accounting"),
            "invariants_match": rebuilt.get("verification") == reference.get("verification"),
            "independent_verification_pass": independent.get("status") == "pass",
        }
        rebuild_timing = rebuilt.get("timing") or {}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    scratch_removed = not scratch.exists()
    checks["scratch_removed"] = scratch_removed
    failures = [key for key, passed in checks.items() if not passed]
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "schema_version": "spacegate.e7_clean_foundation_reproduction.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": build_id,
        "status": "pass" if not failures else "fail",
        "checks": checks,
        "failing_checks": failures,
        "differing_byte_exact_products": differing_byte_exact_products,
        "differing_database_containers": differing_database_containers,
        "scratch_removed": scratch_removed,
        "rebuild_timing": rebuild_timing,
        "total_timing": {
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "peak_rss_kib": int(usage.ru_maxrss),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--artifact-root", type=Path, default=compiler.DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--scratch-parent", type=Path, default=Path("/mnt/space/spacegate"))
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = reproduce(
        policy_path=args.policy.resolve(), state_dir=args.state_dir.resolve(),
        artifact_root=args.artifact_root.resolve(), build_id=args.build_id,
        scratch_parent=args.scratch_parent.resolve(),
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
