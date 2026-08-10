#!/usr/bin/env python3
"""Fail-closed retirement for explicitly named rejected build workspaces."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any

from prune_evidence_lake_artifacts import (
    allocated_bytes,
    open_processes,
    stable_hash,
    tree_identity,
    utc_now,
    write_json,
)


CONTRACT = "spacegate.rejected_build_retention.v1"
DEFAULT_STATE = Path("/data/spacegate/state")


def state_references(state_dir: Path, candidate: Path) -> list[str]:
    references: list[str] = []
    for link in state_dir.rglob("*"):
        if not link.is_symlink():
            continue
        try:
            target = link.resolve(strict=True)
        except FileNotFoundError:
            continue
        if target == candidate or candidate in target.parents:
            references.append(str(link))
    return sorted(references)


def inspect(state_dir: Path, name: str, minimum_age_minutes: float) -> dict[str, Any]:
    state_dir = state_dir.resolve(strict=True)
    root = (state_dir / "rejected").resolve(strict=True)
    candidate = (root / name).resolve(strict=True)
    if candidate.parent != root:
        raise ValueError(f"candidate must be a direct rejected child: {name}")
    if candidate.is_symlink() or not candidate.is_dir():
        raise ValueError(f"candidate must be a real rejected directory: {name}")
    identity = tree_identity(candidate)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError(f"rejected candidate contains symlinks: {name}")
    if any(
        row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity
    ):
        raise ValueError(f"rejected candidate contains shared files: {name}")
    active = open_processes(candidate)
    if active:
        raise ValueError(f"rejected candidate is open by live processes: {name}:{active}")
    references = state_references(state_dir, candidate)
    if references:
        raise ValueError(f"rejected candidate is state-linked: {name}:{references}")
    newest_mtime_ns = max(int(row["mtime_ns"]) for row in identity)
    age_seconds = max(
        0.0, datetime.now(timezone.utc).timestamp() - newest_mtime_ns / 1e9
    )
    if age_seconds < minimum_age_minutes * 60:
        raise ValueError(f"rejected candidate is newer than minimum age: {name}")
    return {
        "name": name,
        "path": str(candidate),
        "artifact_state": "explicitly_rejected_build_workspace",
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_entry_count": len(identity),
        "tree_identity_sha256": stable_hash(identity),
    }


def plan(
    state_dir: Path,
    names: list[str],
    *,
    minimum_age_minutes: float,
    reason: str,
) -> dict[str, Any]:
    if not names or len(names) != len(set(names)):
        raise ValueError("provide one or more unique rejected candidates")
    if not reason.strip():
        raise ValueError("an explicit rejection reason is required")
    rows = [inspect(state_dir, name, minimum_age_minutes) for name in sorted(names)]
    candidate_set_sha256 = stable_hash(
        [
            {
                "name": row["name"],
                "allocated_bytes": row["allocated_bytes"],
                "tree_identity_sha256": row["tree_identity_sha256"],
            }
            for row in rows
        ]
    )
    return {
        "schema_version": CONTRACT,
        "status": "pass",
        "action": "dry_run",
        "generated_at": utc_now(),
        "state_dir": str(state_dir.resolve(strict=True)),
        "reason": reason.strip(),
        "minimum_age_minutes": minimum_age_minutes,
        "candidate_count": len(rows),
        "candidate_set_sha256": candidate_set_sha256,
        "reclaimable_bytes": sum(int(row["allocated_bytes"]) for row in rows),
        "candidates": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--candidate", action="append", default=[])
    parser.add_argument("--minimum-age-minutes", type=float, default=60.0)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-candidate-set-sha256")
    args = parser.parse_args()

    report = plan(
        args.state_dir,
        args.candidate,
        minimum_age_minutes=args.minimum_age_minutes,
        reason=args.reason,
    )
    if args.apply:
        if args.expected_candidate_set_sha256 != report["candidate_set_sha256"]:
            raise ValueError("apply requires the exact dry-run candidate-set hash")
        for row in report["candidates"]:
            shutil.rmtree(row["path"])
        report = {**report, "action": "applied", "applied_at": utc_now()}
    write_json(args.report, report)
    print(
        f"{report['action']}: candidates={report['candidate_count']} "
        f"bytes={report['reclaimable_bytes']} "
        f"hash={report['candidate_set_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
