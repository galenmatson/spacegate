#!/usr/bin/env python3
"""Fail-closed retention for published database archives and their reports."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def current_archive(dl_root: Path) -> Path:
    link = dl_root / "current"
    if not link.is_symlink():
        raise ValueError(f"published current pointer is not a symlink: {link}")
    resolved = link.resolve(strict=True)
    db_root = (dl_root / "db").resolve(strict=True)
    if resolved.parent != db_root or resolved.suffix != ".7z":
        raise ValueError(f"published current pointer escapes db archive root: {resolved}")
    return resolved


def open_paths() -> set[Path]:
    paths: set[Path] = set()
    proc = Path("/proc")
    for process in proc.iterdir():
        if not process.name.isdigit():
            continue
        fd_root = process / "fd"
        try:
            descriptors = list(fd_root.iterdir())
        except (FileNotFoundError, PermissionError):
            continue
        for descriptor in descriptors:
            try:
                target = descriptor.resolve(strict=True)
            except (FileNotFoundError, PermissionError, RuntimeError):
                continue
            paths.add(target)
    return paths


def path_inventory(path: Path) -> list[dict[str, Any]]:
    members = [path] if path.is_file() else sorted(path.rglob("*"))
    inventory = []
    for member in members:
        if member.is_symlink():
            raise ValueError(f"candidate contains symlink: {member}")
        if not member.is_file():
            continue
        stat = member.stat()
        if stat.st_nlink != 1:
            raise ValueError(f"candidate contains shared file: {member}")
        inventory.append(
            {
                "relative_path": str(member.relative_to(path.parent)),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "inode": stat.st_ino,
            }
        )
    return inventory


def build_plan(dl_root: Path, keep_archives: int) -> dict[str, Any]:
    if keep_archives < 1:
        raise ValueError("keep_archives must retain at least the current archive")
    db_root = dl_root / "db"
    reports_root = dl_root / "reports"
    current = current_archive(dl_root)
    archives = sorted(
        (path for path in db_root.glob("*.7z") if path.is_file()),
        key=lambda path: (path.stat().st_mtime_ns, path.name),
        reverse=True,
    )
    retained = set(archives[:keep_archives]) | {current}
    candidates: list[Path] = [path for path in archives if path not in retained]
    retained_build_ids = {path.stem for path in retained}
    if reports_root.exists():
        candidates.extend(
            path
            for path in sorted(reports_root.iterdir())
            if path.is_dir() and path.name not in retained_build_ids
        )
    opened = open_paths()
    rows = []
    for path in sorted(candidates):
        resolved = path.resolve(strict=True)
        if resolved in opened or any(
            opened_path.is_relative_to(resolved) for opened_path in opened
        ):
            raise ValueError(f"candidate is open by a live process: {path}")
        inventory = path_inventory(path)
        rows.append(
            {
                "path": str(path),
                "kind": "archive" if path.parent == db_root else "report_directory",
                "allocated_bytes": sum(row["size"] for row in inventory),
                "inventory": inventory,
            }
        )
    identity = [
        {
            "path": row["path"],
            "kind": row["kind"],
            "inventory": row["inventory"],
        }
        for row in rows
    ]
    candidate_hash = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema_version": "spacegate.published_download_retention.v1",
        "dl_root": str(dl_root),
        "current_archive": str(current),
        "keep_archives": keep_archives,
        "retained_archives": [str(path) for path in sorted(retained)],
        "candidates": rows,
        "candidate_count": len(rows),
        "reclaimable_bytes": sum(row["allocated_bytes"] for row in rows),
        "candidate_set_sha256": candidate_hash,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dl-root", type=Path, required=True)
    parser.add_argument("--keep-archives", type=int, default=3)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-candidate-set-sha256")
    args = parser.parse_args()
    plan = build_plan(args.dl_root, args.keep_archives)
    plan["reason"] = args.reason
    plan["mode"] = "apply" if args.apply else "dry_run"
    if args.apply:
        if not args.expected_candidate_set_sha256:
            raise ValueError("apply requires --expected-candidate-set-sha256")
        if args.expected_candidate_set_sha256 != plan["candidate_set_sha256"]:
            raise ValueError("candidate set changed since reviewed dry run")
        for row in plan["candidates"]:
            path = Path(row["path"])
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        plan["applied"] = True
    else:
        plan["applied"] = False
    write_json(args.report, plan)
    action = "applied" if args.apply else "dry_run"
    print(
        f"{action}: candidates={plan['candidate_count']} "
        f"bytes={plan['reclaimable_bytes']} hash={plan['candidate_set_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
