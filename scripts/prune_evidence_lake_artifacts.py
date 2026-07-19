#!/usr/bin/env python3
"""Fail-closed retention for interrupted or independently failed E4 artifacts."""

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
BUILD_NAME = re.compile(r"^[0-9a-f]{24}$")
CONTRACT = "spacegate.evidence_artifact_retention.v1"
SUPPORTED_FAILED_ARTIFACT_AUDITS = {
    "spacegate.scientific_evidence_artifact_audit.v1",
    "spacegate.gcvs_scientific_evidence_audit.v1",
    "spacegate.hunt_reffert_scientific_evidence_audit.v1",
    "spacegate.extended_catalog_scientific_evidence_audit.v1",
}


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


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        "artifact_state": "interrupted_manifestless",
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_identity_sha256": stable_hash(identity),
        "tree_entry_count": len(identity),
    }


def current_pointer_references(root: Path, candidate: Path) -> list[str]:
    references: list[str] = []
    for pointer in sorted(root.glob("current*")):
        try:
            if pointer.is_symlink() and pointer.resolve(strict=True) == candidate:
                references.append(str(pointer))
            elif pointer.is_file() and pointer.read_text(encoding="utf-8").strip() == candidate.name:
                references.append(str(pointer))
        except (FileNotFoundError, UnicodeDecodeError):
            continue
    return references


def inspect_failed_artifact(
    root: Path,
    value: str,
    audit_report: Path,
    *,
    minimum_age_minutes: float,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    candidate = (root / value).resolve(strict=True)
    if candidate.parent != root or not BUILD_NAME.fullmatch(candidate.name):
        raise ValueError(f"failed artifact must be a direct build-id child: {value}")
    if candidate.is_symlink() or not candidate.is_dir():
        raise ValueError(f"failed artifact must be a real directory: {value}")
    manifest_path = candidate / "manifest.json"
    database_path = candidate / "scientific_evidence.duckdb"
    if not manifest_path.is_file() or not database_path.is_file():
        raise ValueError(f"failed artifact lacks immutable manifest/database: {value}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("build_id") != candidate.name:
        raise ValueError(f"failed artifact manifest build identity mismatch: {value}")
    configured_database = (candidate / str(manifest.get("database") or "")).resolve()
    if configured_database != database_path or configured_database.parent != candidate:
        raise ValueError(f"failed artifact database path is unsafe: {value}")
    if file_hash(database_path) != manifest.get("database_sha256"):
        raise ValueError(f"failed artifact database checksum mismatch: {value}")

    audit_path = audit_report.resolve(strict=True)
    try:
        audit_path.relative_to(candidate)
    except ValueError:
        pass
    else:
        raise ValueError("failed artifact audit must be stored outside the candidate")
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    if audit.get("schema_version") not in SUPPORTED_FAILED_ARTIFACT_AUDITS:
        raise ValueError(f"unsupported failed-artifact audit contract: {audit_path}")
    if audit.get("status") != "fail" or audit.get("build_id") != candidate.name:
        raise ValueError(f"audit does not fail the requested artifact: {value}")
    if Path(str(audit.get("database") or "")).resolve() != database_path:
        raise ValueError(f"audit database does not match failed artifact: {value}")
    failed_checks = {
        str(name): int(count)
        for name, count in (audit.get("checks") or {}).items()
        if int(count) != 0
    }
    if not failed_checks:
        raise ValueError(f"failed-artifact audit has no failing checks: {value}")

    identity = tree_identity(candidate)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError(f"failed artifact contains symlinks: {value}")
    if any(
        row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity
    ):
        raise ValueError(f"failed artifact contains shared files: {value}")
    active_pids = open_processes(candidate, proc_root)
    if active_pids:
        raise ValueError(f"failed artifact is open by live processes: {value}: {active_pids}")
    pointer_references = current_pointer_references(root, candidate)
    if pointer_references:
        raise ValueError(
            f"failed artifact is referenced by current pointer: {value}: {pointer_references}"
        )
    newest_mtime_ns = max(int(row["mtime_ns"]) for row in identity)
    age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - newest_mtime_ns / 1e9)
    if age_seconds < minimum_age_minutes * 60:
        raise ValueError(
            f"failed artifact is newer than minimum age: {value}: "
            f"{age_seconds:.1f}s < {minimum_age_minutes * 60:.1f}s"
        )
    return {
        "name": candidate.name,
        "path": str(candidate),
        "artifact_state": "immutable_independently_audit_failed",
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_identity_sha256": stable_hash(identity),
        "tree_entry_count": len(identity),
        "manifest_sha256": file_hash(manifest_path),
        "database_sha256": manifest["database_sha256"],
        "audit_report": str(audit_path),
        "audit_schema_version": audit["schema_version"],
        "audit_sha256": file_hash(audit_path),
        "failed_checks": failed_checks,
    }


def retention_report(
    root: Path,
    candidates: list[str],
    *,
    failed_audits: dict[str, Path] | None = None,
    reason: str,
    minimum_age_minutes: float,
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    failed_audits = failed_audits or {}
    if not candidates and not failed_audits:
        raise ValueError("at least one explicit candidate is required")
    if not reason.strip():
        raise ValueError("an explicit retention reason is required")
    all_names = [*candidates, *failed_audits]
    if len(all_names) != len(set(all_names)):
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
    rows.extend(
        inspect_failed_artifact(
            root,
            value,
            failed_audits[value],
            minimum_age_minutes=minimum_age_minutes,
            proc_root=proc_root,
        )
        for value in sorted(failed_audits)
    )
    rows.sort(key=lambda row: str(row["name"]))
    candidate_set_sha256 = stable_hash(
        [
            {
                "name": row["name"],
                "artifact_state": row["artifact_state"],
                "allocated_bytes": row["allocated_bytes"],
                "tree_identity_sha256": row["tree_identity_sha256"],
                "audit_sha256": row.get("audit_sha256"),
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
    parser.add_argument(
        "--failed-audit",
        action="append",
        default=[],
        metavar="BUILD_ID=REPORT",
        help="retire an immutable build only when this independent audit fails it",
    )
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
    failed_audits: dict[str, Path] = {}
    for specification in args.failed_audit:
        if specification.count("=") != 1:
            raise ValueError("failed audit must use BUILD_ID=REPORT")
        build_id, report = specification.split("=", 1)
        if not build_id or not report or build_id in failed_audits:
            raise ValueError("failed audit must identify one unique build and report")
        failed_audits[build_id] = Path(report)
    report = retention_report(
        root,
        args.candidate,
        failed_audits=failed_audits,
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
