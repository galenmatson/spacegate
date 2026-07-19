#!/usr/bin/env python3
"""Fail-closed retention for interrupted Evidence Lake compiler artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATE = Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state"))
TEMPORARY_NAME = re.compile(r"^\.[0-9a-f]{24}\.[A-Za-z0-9_-]+$")
CONTRACT = "spacegate.evidence_artifact_retention.v1"


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def allocated_bytes(path: Path) -> int:
    return sum(
        child.lstat().st_blocks * 512
        for child in [path, *sorted(path.rglob("*"))]
        if not child.is_symlink()
    )


def tree_identity(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for child in [path, *sorted(path.rglob("*"))]:
        stat = child.lstat()
        rows.append(
            {
                "path": "." if child == path else child.relative_to(path).as_posix(),
                "kind": "symlink" if child.is_symlink() else "dir" if child.is_dir() else "file",
                "bytes": stat.st_size,
                "allocated_bytes": stat.st_blocks * 512,
                "mtime_ns": stat.st_mtime_ns,
                "inode": stat.st_ino,
                "link_count": stat.st_nlink,
            }
        )
    return rows


def open_processes(candidate: Path, proc_root: Path = Path("/proc")) -> list[int]:
    pids: set[int] = set()
    if not proc_root.exists():
        raise ValueError("process filesystem is unavailable; cannot prove artifact is idle")
    for process in proc_root.iterdir():
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
                target.relative_to(candidate)
            except (FileNotFoundError, PermissionError, RuntimeError, ValueError):
                continue
            pids.add(int(process.name))
            break
    return sorted(pids)


def inspect_candidate(
    root: Path,
    value: str,
    *,
    minimum_age_minutes: float,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    candidate = (root / value).resolve(strict=True)
    if candidate.parent != root:
        raise ValueError(f"candidate must be a direct child of artifact root: {value}")
    if not TEMPORARY_NAME.fullmatch(candidate.name):
        raise ValueError(f"candidate is not an interrupted temporary artifact: {value}")
    if candidate.is_symlink() or not candidate.is_dir():
        raise ValueError(f"candidate must be a real directory: {value}")
    if (candidate / "manifest.json").exists():
        raise ValueError(f"manifest-bearing artifact is protected: {value}")
    if not (candidate / "scientific_evidence.duckdb").is_file():
        raise ValueError(f"candidate lacks the expected compiler database: {value}")
    identity = tree_identity(candidate)
    symlinks = [row["path"] for row in identity if row["kind"] == "symlink"]
    hardlinks = [
        row["path"]
        for row in identity
        if row["kind"] == "file" and int(row["link_count"]) > 1
    ]
    if symlinks:
        raise ValueError(f"candidate contains symlinks: {value}: {symlinks[:5]}")
    if hardlinks:
        raise ValueError(f"candidate contains shared files: {value}: {hardlinks[:5]}")
    active_pids = open_processes(candidate, proc_root)
    if active_pids:
        raise ValueError(f"candidate is open by live processes: {value}: {active_pids}")
    newest_mtime_ns = max(int(row["mtime_ns"]) for row in identity)
    age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - newest_mtime_ns / 1e9)
    if age_seconds < minimum_age_minutes * 60:
        raise ValueError(
            f"candidate is newer than minimum age: {value}: "
            f"{age_seconds:.1f}s < {minimum_age_minutes * 60:.1f}s"
        )
    return {
        "name": candidate.name,
        "path": str(candidate),
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_identity_sha256": stable_hash(identity),
        "tree_entry_count": len(identity),
    }


def retention_report(
    root: Path,
    candidates: list[str],
    *,
    reason: str,
    minimum_age_minutes: float,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    if not candidates:
        raise ValueError("at least one explicit candidate is required")
    if not reason.strip():
        raise ValueError("an explicit retention reason is required")
    if len(candidates) != len(set(candidates)):
        raise ValueError("candidate list contains duplicates")
    rows = [
        inspect_candidate(
            root,
            value,
            minimum_age_minutes=minimum_age_minutes,
            proc_root=proc_root,
        )
        for value in sorted(candidates)
    ]
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
        "artifact_root": str(root.resolve(strict=True)),
        "minimum_age_minutes": minimum_age_minutes,
        "reason": reason.strip(),
        "candidate_set_sha256": candidate_set_sha256,
        "candidate_count": len(rows),
        "reclaimable_bytes": sum(int(row["allocated_bytes"]) for row in rows),
        "candidates": rows,
        "checked_at": utc_now(),
    }


def apply_retention(report: dict[str, Any], expected_candidate_set_sha256: str) -> dict[str, Any]:
    if (
        not expected_candidate_set_sha256
        or expected_candidate_set_sha256 != report["candidate_set_sha256"]
    ):
        raise ValueError("apply requires the exact current dry-run candidate-set hash")
    for row in report["candidates"]:
        shutil.rmtree(row["path"])
    return {
        **report,
        "action": "applied",
        "applied_at": utc_now(),
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
    if args.minimum_age_minutes < 0:
        raise ValueError("minimum age must be nonnegative")
    root = (
        args.state_dir
        / "derived"
        / "evidence_lake_v2"
        / "scientific_evidence"
    )
    report = retention_report(
        root,
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
        report = apply_retention(
            report,
            str(args.expected_candidate_set_sha256 or ""),
        )
    write_json(args.report, report)
    print(
        f"{report['action']}: candidates={report['candidate_count']} "
        f"bytes={report['reclaimable_bytes']} "
        f"hash={report['candidate_set_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
