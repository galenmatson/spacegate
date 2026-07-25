#!/usr/bin/env python3
"""Atomically promote a verified staged public-read artifact with rollback files."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def run(args: argparse.Namespace) -> dict[str, Any]:
    active_dir = Path(args.active_dir).resolve()
    staging_dir = Path(args.staging_dir).resolve()
    active_database = active_dir / "public_read.sqlite"
    active_manifest = active_dir / "manifest.json"
    staged_database = staging_dir / "public_read.sqlite"
    staged_manifest = staging_dir / "manifest.json"
    for path in (active_database, active_manifest, staged_database, staged_manifest):
        if not path.is_file():
            raise SystemExit(f"Missing public-read promotion artifact: {path}")
    active = json.loads(active_manifest.read_text(encoding="utf-8"))
    staged = json.loads(staged_manifest.read_text(encoding="utf-8"))
    if active.get("build_id") != staged.get("build_id"):
        raise SystemExit("Active and staged build identities differ")
    if staged.get("status") != "pass":
        raise SystemExit("Staged manifest has not passed materialization")
    expected_hash = (staged.get("artifact") or {}).get("sha256")
    actual_hash = sha256_file(staged_database)
    if expected_hash != actual_hash:
        raise SystemExit("Staged database hash disagrees with its manifest")
    connection = sqlite3.connect(
        f"file:{staged_database}?mode=ro&immutable=1",
        uri=True,
    )
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    connection.close()
    if integrity != "ok":
        raise SystemExit(f"Staged SQLite integrity failed: {integrity}")

    suffix = args.backup_suffix or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_database = active_dir / f"public_read.rollback.{suffix}.sqlite"
    backup_manifest = active_dir / f"manifest.rollback.{suffix}.json"
    if backup_database.exists() or backup_manifest.exists():
        raise SystemExit("Requested rollback artifact names already exist")

    staged_database_moved = False
    staged_manifest_moved = False
    os.replace(active_database, backup_database)
    try:
        os.replace(active_manifest, backup_manifest)
        os.replace(staged_database, active_database)
        staged_database_moved = True
        os.replace(staged_manifest, active_manifest)
        staged_manifest_moved = True
    except Exception:
        if staged_manifest_moved and active_manifest.exists():
            os.replace(active_manifest, staged_manifest)
        if staged_database_moved and active_database.exists():
            os.replace(active_database, staged_database)
        if backup_database.exists():
            os.replace(backup_database, active_database)
        if backup_manifest.exists():
            os.replace(backup_manifest, active_manifest)
        raise

    report = {
        "schema_version": "spacegate.public_read_promotion.v1",
        "status": "pass",
        "build_id": staged["build_id"],
        "promoted_sha256": actual_hash,
        "promoted_bytes": active_database.stat().st_size,
        "sqlite_integrity": integrity,
        "rollback_database": str(backup_database),
        "rollback_manifest": str(backup_manifest),
        "promoted_at_utc": utc_now(),
    }
    report_path = (
        Path(args.report).resolve()
        if args.report
        else active_dir / "promotion_report.json"
    )
    atomic_json(report_path, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--active-dir", required=True)
    parser.add_argument("--staging-dir", required=True)
    parser.add_argument("--backup-suffix")
    parser.add_argument("--report")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
