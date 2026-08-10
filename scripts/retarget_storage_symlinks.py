#!/usr/bin/env python3
"""Atomically retarget a bounded set of state symlinks between storage roots."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from prune_evidence_lake_artifacts import stable_hash, utc_now, write_json


CONTRACT = "spacegate.storage_symlink_retarget.v1"
DEFAULT_STATE = Path("/data/spacegate/state")


def contained(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def inspect(state_dir: Path, source_root: Path, destination_root: Path) -> dict[str, Any]:
    state_dir = state_dir.resolve(strict=True)
    source_root = source_root.resolve(strict=True)
    destination_root = destination_root.resolve(strict=True)
    if source_root == destination_root:
        raise ValueError("source and destination roots must differ")

    rows: list[dict[str, str]] = []
    for link in sorted(path for path in state_dir.rglob("*") if path.is_symlink()):
        raw_target = os.readlink(link)
        target = Path(raw_target)
        if not target.is_absolute():
            target = link.parent / target
        lexical_target = Path(os.path.abspath(target))
        try:
            resolved_target = target.resolve(strict=True)
        except FileNotFoundError:
            if contained(lexical_target, source_root):
                raise ValueError(f"source target is broken: {link} -> {lexical_target}")
            continue
        if not contained(resolved_target, source_root):
            continue
        relative = resolved_target.relative_to(source_root)
        destination = destination_root / relative
        if not destination.exists():
            raise ValueError(f"destination target is missing: {link} -> {destination}")
        if resolved_target.is_dir() != destination.is_dir():
            raise ValueError(f"destination target kind differs: {link} -> {destination}")
        rows.append(
            {
                "link": str(link),
                "old_target": str(resolved_target),
                "new_target": str(destination),
            }
        )

    candidate_set_sha256 = stable_hash(rows)
    return {
        "schema_version": CONTRACT,
        "status": "pass",
        "action": "dry_run",
        "generated_at": utc_now(),
        "state_dir": str(state_dir),
        "source_root": str(source_root),
        "destination_root": str(destination_root),
        "candidate_count": len(rows),
        "candidate_set_sha256": candidate_set_sha256,
        "candidates": rows,
    }


def apply(report: dict[str, Any], expected_hash: str) -> dict[str, Any]:
    if not report["candidates"]:
        raise ValueError("refusing an empty symlink migration")
    if report["candidate_set_sha256"] != expected_hash:
        raise ValueError("apply requires the exact dry-run candidate-set hash")
    for row in report["candidates"]:
        link = Path(row["link"])
        current = Path(row["old_target"])
        if link.resolve(strict=True) != current:
            raise ValueError(f"symlink changed after dry run: {link}")
        temporary = link.with_name(f".{link.name}.storage-migration.tmp")
        if temporary.exists() or temporary.is_symlink():
            raise ValueError(f"temporary retarget path already exists: {temporary}")
        temporary.symlink_to(row["new_target"])
        os.replace(temporary, link)
    return {
        **report,
        "action": "applied",
        "applied_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--destination-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-candidate-set-sha256")
    args = parser.parse_args()

    report = inspect(args.state_dir, args.source_root, args.destination_root)
    if args.apply:
        report = apply(report, args.expected_candidate_set_sha256 or "")
    write_json(args.report, report)
    print(
        f"{report['action']}: candidates={report['candidate_count']} "
        f"hash={report['candidate_set_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
