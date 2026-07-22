#!/usr/bin/env python3
"""Reproduce clean E7 extended objects and compare canonical hashes."""

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

import compile_e7_clean_extended_objects as compiler
import verify_e7_clean_extended_objects as verifier


DEFAULT_REPORT = Path("/data/spacegate/state/reports/evidence_lake_v2/e7_clean_extended_objects_reproduction.json")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def reproduce(
    policy_path: Path, state: Path, artifact_root: Path, build_id: str,
    scratch_parent: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    reference = load_object(artifact_root / build_id / "manifest.json")
    scratch = Path(tempfile.mkdtemp(prefix="e7-clean-extended-reproduction-", dir=scratch_parent))
    try:
        rebuilt = compiler.compile_extended(
            policy_path, state, scratch / "artifacts", link_into_state=False,
        )
        independent = verifier.verify(scratch / "artifacts" / rebuilt["build_id"])
        byte_exact = sorted(
            key for key, spec in reference["products"].items()
            if spec.get("determinism") == "byte_exact"
        )
        differences = sorted(
            key for key in byte_exact
            if reference["products"].get(key) != rebuilt["products"].get(key)
        )
        checks = {
            "build_id_match": rebuilt.get("build_id") == reference.get("build_id") == build_id,
            "canonical_parquet_hashes_match": not differences,
            "counts_match": rebuilt.get("counts") == reference.get("counts"),
            "invariants_match": rebuilt.get("verification") == reference.get("verification"),
            "database_logical_verification_pass": independent.get("status") == "pass",
        }
        report = {
            "schema_version": "spacegate.e7_clean_extended_objects_reproduction.v1",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "build_id": build_id,
            "status": "pass" if all(checks.values()) else "fail",
            "checks": checks,
            "failing_checks": sorted(name for name, passed in checks.items() if not passed),
            "differing_byte_exact_products": differences,
            "rebuild_timing": rebuilt.get("timing"),
            "independent_verification": independent,
            "scratch_removed": True,
        }
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    report["scratch_removed"] = not scratch.exists()
    if not report["scratch_removed"]:
        report["status"] = "fail"
        report["failing_checks"].append("scratch_removed")
    usage = resource.getrusage(resource.RUSAGE_SELF)
    report["total_timing"] = {
        "wall_seconds": round(time.monotonic() - started, 6),
        "cpu_seconds": round(time.process_time() - cpu_started, 6),
        "peak_rss_kib": int(usage.ru_maxrss),
    }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--artifact-root", type=Path, default=compiler.DEFAULT_OUTPUT)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--scratch-parent", type=Path, default=Path("/mnt/space/spacegate"))
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = reproduce(
        args.policy.resolve(), args.state_dir.resolve(), args.artifact_root.resolve(),
        args.build_id, args.scratch_parent.resolve(),
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
