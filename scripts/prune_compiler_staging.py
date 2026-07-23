#!/usr/bin/env python3
"""Fail-closed retention for explicitly named interrupted compiler staging trees."""

from __future__ import annotations

import argparse
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from prune_evidence_lake_artifacts import (
    allocated_bytes,
    open_processes,
    stable_hash,
    tree_identity,
    utc_now,
    write_json,
)


CONTRACT = "spacegate.compiler_staging_retention.v1"
STAGING_NAME = re.compile(r"^\.[0-9a-f]{24}\.[A-Za-z0-9_-]+$")
ALLOWED_ROOTS = (
    Path("/data/spacegate/state"),
    Path("/mnt/space/spacegate"),
)


def bounded_root(value: Path) -> Path:
    root = value.resolve(strict=True)
    if not any(root == allowed or allowed in root.parents for allowed in ALLOWED_ROOTS):
        raise ValueError(f"compiler artifact root is outside Spacegate storage: {root}")
    return root


def inspect(root: Path, name: str, minimum_age_minutes: float) -> dict[str, Any]:
    root = bounded_root(root)
    if not STAGING_NAME.fullmatch(name):
        raise ValueError(f"candidate is not a compiler staging name: {name}")
    raw = root / name
    if raw.is_symlink() or not raw.is_dir():
        raise ValueError(f"candidate must be a real direct-child directory: {name}")
    candidate = raw.resolve(strict=True)
    if candidate.parent != root:
        raise ValueError(f"candidate escapes compiler artifact root: {name}")
    if (candidate / "manifest.json").exists():
        raise ValueError(f"manifest-bearing artifact is protected: {name}")
    databases = sorted(candidate.rglob("*.duckdb"))
    if not databases:
        raise ValueError(f"candidate has no compiler database payload: {name}")
    identity = tree_identity(candidate)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError(f"candidate contains symlinks: {name}")
    if any(
        row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity
    ):
        raise ValueError(f"candidate contains shared files: {name}")
    active = open_processes(candidate)
    if active:
        raise ValueError(f"candidate is open by live processes: {name}:{active}")
    newest_mtime_ns = max(int(row["mtime_ns"]) for row in identity)
    age_seconds = max(
        0.0, datetime.now(timezone.utc).timestamp() - newest_mtime_ns / 1e9
    )
    if age_seconds < minimum_age_minutes * 60:
        raise ValueError(
            f"candidate is newer than minimum age: {age_seconds:.1f}s"
        )
    return {
        "name": name,
        "path": str(candidate),
        "artifact_state": "interrupted_manifestless_compiler_staging",
        "database_files": [str(path.relative_to(candidate)) for path in databases],
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_entry_count": len(identity),
        "tree_identity_sha256": stable_hash(identity),
    }


def plan(
    root: Path, names: list[str], *, reason: str, minimum_age_minutes: float
) -> dict[str, Any]:
    if not names or len(names) != len(set(names)):
        raise ValueError("provide one or more unique explicit staging candidates")
    if not reason.strip():
        raise ValueError("an explicit retention reason is required")
    rows = [
        inspect(root, name, minimum_age_minutes)
        for name in sorted(names)
    ]
    candidate_set_sha256 = stable_hash([
        {
            "name": row["name"],
            "allocated_bytes": row["allocated_bytes"],
            "tree_identity_sha256": row["tree_identity_sha256"],
        }
        for row in rows
    ])
    return {
        "schema_version": CONTRACT,
        "status": "pass",
        "action": "dry_run",
        "root": str(bounded_root(root)),
        "reason": reason.strip(),
        "minimum_age_minutes": minimum_age_minutes,
        "candidate_count": len(rows),
        "candidate_set_sha256": candidate_set_sha256,
        "reclaimable_bytes": sum(int(row["allocated_bytes"]) for row in rows),
        "candidates": rows,
        "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--candidate", action="append", default=[])
    parser.add_argument("--minimum-age-minutes", type=float, default=60.0)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-candidate-set-sha256")
    args = parser.parse_args()
    report = plan(
        args.root,
        args.candidate,
        reason=args.reason,
        minimum_age_minutes=args.minimum_age_minutes,
    )
    report_path = args.report.resolve()
    for row in report["candidates"]:
        try:
            report_path.relative_to(Path(row["path"]))
        except ValueError:
            continue
        raise ValueError("retention report cannot be stored inside a candidate")
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
