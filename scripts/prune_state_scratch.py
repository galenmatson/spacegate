#!/usr/bin/env python3
"""Fail-closed retention for explicitly named Spacegate scratch diagnostics."""

from __future__ import annotations

import argparse
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


DEFAULT_STATE = Path("/data/spacegate/state")
CONTRACT = "spacegate.state_scratch_retention.v2"


def scratch_root_for_scope(state_dir: Path, scope: str) -> Path:
    state = state_dir.resolve(strict=True)
    if scope == "state":
        return state / "tmp"
    if scope == "host":
        return state.parent / "tmp"
    raise ValueError(f"unsupported scratch scope: {scope}")


def inspect_candidate(
    scratch_root: Path,
    name: str,
    *,
    minimum_age_minutes: float,
) -> dict[str, Any]:
    if not name or name in {".", ".."} or "/" in name:
        raise ValueError(f"scratch candidate must be one direct-child name: {name!r}")
    root = scratch_root.resolve(strict=True)
    candidate = (root / name).resolve(strict=True)
    if candidate.parent != root:
        raise ValueError(f"scratch candidate escapes state/tmp: {name}")
    if candidate.is_symlink() or not (candidate.is_file() or candidate.is_dir()):
        raise ValueError(f"scratch candidate must be a real file or directory: {name}")
    identity = tree_identity(candidate)
    if any(row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity):
        raise ValueError(f"scratch candidate contains shared files: {name}")
    active = open_processes(candidate)
    if active:
        raise ValueError(f"scratch candidate is open by live processes: {name}:{active}")
    newest_mtime_ns = max(int(row["mtime_ns"]) for row in identity)
    age_seconds = max(
        0.0,
        datetime.now(timezone.utc).timestamp() - newest_mtime_ns / 1e9,
    )
    if age_seconds < minimum_age_minutes * 60:
        raise ValueError(
            f"scratch candidate is newer than minimum age: {name}:"
            f"{age_seconds:.1f}s<{minimum_age_minutes * 60:.1f}s"
        )
    return {
        "name": name,
        "path": str(candidate),
        "kind": "directory" if candidate.is_dir() else "file",
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_entry_count": len(identity),
        "symlink_count": sum(row["kind"] == "symlink" for row in identity),
        "tree_identity_sha256": stable_hash(identity),
    }


def retention_report(
    state_dir: Path,
    candidates: list[str],
    *,
    scratch_scope: str,
    reason: str,
    minimum_age_minutes: float,
) -> dict[str, Any]:
    if not candidates or len(candidates) != len(set(candidates)):
        raise ValueError("provide one or more unique explicit scratch candidates")
    if not reason.strip():
        raise ValueError("an explicit retention reason is required")
    scratch_root = scratch_root_for_scope(state_dir, scratch_scope).resolve(strict=True)
    rows = [
        inspect_candidate(
            scratch_root,
            name,
            minimum_age_minutes=minimum_age_minutes,
        )
        for name in sorted(candidates)
    ]
    candidate_hash = stable_hash(
        [
            {
                "name": row["name"],
                "kind": row["kind"],
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
        "scratch_scope": scratch_scope,
        "scratch_root": str(scratch_root),
        "reason": reason.strip(),
        "minimum_age_minutes": minimum_age_minutes,
        "candidate_set_sha256": candidate_hash,
        "candidate_count": len(rows),
        "reclaimable_bytes": sum(int(row["allocated_bytes"]) for row in rows),
        "candidates": rows,
        "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument(
        "--scratch-scope",
        choices=("state", "host"),
        default="state",
        help="state selects STATE/tmp; host selects the sibling tmp directory",
    )
    parser.add_argument("--candidate", action="append", default=[])
    parser.add_argument("--minimum-age-minutes", type=float, default=60.0)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-candidate-set-sha256")
    args = parser.parse_args()
    if args.minimum_age_minutes < 0:
        raise ValueError("minimum age must be nonnegative")
    report = retention_report(
        args.state_dir,
        args.candidate,
        scratch_scope=args.scratch_scope,
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
            raise ValueError("apply requires the exact current dry-run candidate-set hash")
        for row in report["candidates"]:
            path = Path(row["path"])
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
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
