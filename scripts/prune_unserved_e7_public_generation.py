#!/usr/bin/env python3
"""Fail-closed retirement of one explicitly unserved E7 public generation."""

from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path
from typing import Any

from prune_evidence_lake_artifacts import (
    allocated_bytes,
    file_hash,
    open_processes,
    stable_hash,
    tree_identity,
    utc_now,
    write_json,
)


DEFAULT_STATE = Path("/data/spacegate/state")
BUILD_NAME = re.compile(r"^e7_([0-9a-f]{24})_public$")
CONTRACT = "spacegate.unserved_e7_public_retention.v1"


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read retention evidence {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"retention evidence is not a JSON object: {path}")
    return value


def inspect(state_dir: Path, build_id: str) -> dict[str, Any]:
    match = BUILD_NAME.fullmatch(build_id)
    if not match:
        raise ValueError(f"invalid E7 public generation: {build_id}")
    out_root = (state_dir / "out").resolve(strict=True)
    candidate = out_root / build_id
    if candidate.is_symlink() or not candidate.is_dir():
        raise ValueError(f"candidate is not a real public directory: {candidate}")
    candidate = candidate.resolve(strict=True)
    if candidate.parent != out_root:
        raise ValueError(f"candidate escapes public output root: {candidate}")

    served = state_dir / "served/current"
    if served.is_symlink() and served.resolve(strict=True) == candidate:
        raise ValueError(f"candidate is currently served: {candidate}")
    for pointer_root in ("rollback", "published"):
        root = state_dir / pointer_root
        if not root.is_dir():
            continue
        for pointer in root.rglob("*"):
            if pointer.is_symlink() and pointer.resolve(strict=True) == candidate:
                raise ValueError(f"candidate is protected by {pointer}")

    required_products = (
        "core.duckdb", "arm.duckdb", "disc.duckdb",
        "canonical_hierarchy.duckdb", "parquet",
    )
    missing = [name for name in required_products if not (candidate / name).exists()]
    if missing:
        raise ValueError(f"candidate is incomplete: missing={missing}")

    report_root = state_dir / "reports" / build_id
    slice_report_path = report_root / "slice_policy_report.json"
    verification_path = report_root / "derived_build_verification_report.json"
    performance_path = report_root / "slice_build_performance_report.json"
    slice_report = load_json(slice_report_path)
    verification = load_json(verification_path)
    performance = load_json(performance_path)
    bundle_id = match.group(1)
    if (
        slice_report.get("slice_build_id") != build_id
        or slice_report.get("source_build_id") != bundle_id
    ):
        raise ValueError("slice report identity mismatch")
    if (
        verification.get("status") != "pass"
        or verification.get("build_id") != build_id
        or verification.get("source_build_id") != bundle_id
        or verification.get("failures")
    ):
        raise ValueError("derived verification is not a matching pass")
    if (
        performance.get("status") != "pass"
        or performance.get("slice_build_id") != build_id
        or performance.get("source_build_id") != bundle_id
    ):
        raise ValueError("performance report identity mismatch")

    identity = tree_identity(candidate)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError("candidate contains symlinks")
    if any(row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity):
        raise ValueError("candidate contains shared files")
    active = open_processes(candidate)
    if active:
        raise ValueError(f"candidate is open by live processes: {active}")
    return {
        "build_id": build_id,
        "bundle_build_id": bundle_id,
        "path": str(candidate),
        "allocated_bytes": allocated_bytes(candidate),
        "tree_entry_count": len(identity),
        "tree_identity_sha256": stable_hash(identity),
        "retained_reports": [
            {"path": str(path.resolve()), "sha256": file_hash(path)}
            for path in (slice_report_path, verification_path, performance_path)
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply-candidate-hash")
    args = parser.parse_args()

    state_dir = args.state_dir.resolve(strict=True)
    candidate = inspect(state_dir, args.build_id)
    candidate_hash = stable_hash(
        {
            "build_id": candidate["build_id"],
            "path": candidate["path"],
            "allocated_bytes": candidate["allocated_bytes"],
            "tree_identity_sha256": candidate["tree_identity_sha256"],
            "retained_reports": candidate["retained_reports"],
        }
    )
    apply = args.apply_candidate_hash is not None
    if apply and args.apply_candidate_hash != candidate_hash:
        raise ValueError(
            f"candidate hash mismatch: expected {args.apply_candidate_hash}, observed {candidate_hash}"
        )
    reclaimed = 0
    if apply:
        shutil.rmtree(candidate["path"])
        reclaimed = int(candidate["allocated_bytes"])
    report = {
        "schema_version": CONTRACT,
        "generated_at": utc_now(),
        "status": "pass",
        "mode": "apply" if apply else "dry_run",
        "reason": args.reason,
        "candidate": candidate,
        "candidate_set_sha256": candidate_hash,
        "reclaimable_allocated_bytes": int(candidate["allocated_bytes"]),
        "reclaimed_allocated_bytes": reclaimed,
    }
    write_json(args.report, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
