#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from compile_smart_tags import (
    ASSIGNMENT_SCHEMA,
    MANIFEST_SCHEMA,
    SCHEMA_VERSION,
    SOURCE_SUMMARY_SCHEMA,
    sha256_file,
)


def verify_artifact(root: Path, expected_build_id: str | None = None) -> dict[str, Any]:
    root = root.resolve(strict=True)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise ValueError("unsupported smart-tag manifest schema")
    if manifest.get("status") != "pass":
        raise ValueError("smart-tag manifest did not pass")
    if expected_build_id and manifest.get("build_id") != expected_build_id:
        raise ValueError("smart-tag build identity mismatch")
    for name, spec in manifest["artifacts"].items():
        path = root / spec["path"]
        if not path.is_file():
            raise ValueError(f"missing smart-tag artifact: {name}")
        if path.stat().st_size != spec["bytes"]:
            raise ValueError(f"smart-tag artifact byte mismatch: {name}")
        if sha256_file(path) != spec["sha256"]:
            raise ValueError(f"smart-tag artifact checksum mismatch: {name}")
    database = root / manifest["artifacts"]["database"]["path"]
    con = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
    try:
        metadata = dict(con.execute("SELECT key,value FROM metadata"))
        expected_metadata = {
            "schema_version": SCHEMA_VERSION,
            "assignment_schema_version": ASSIGNMENT_SCHEMA,
            "source_summary_schema_version": SOURCE_SUMMARY_SCHEMA,
            "build_id": manifest["build_id"],
            "registry_hash": manifest["registry_hash"],
        }
        for key, expected in expected_metadata.items():
            if metadata.get(key) != expected:
                raise ValueError(f"smart-tag metadata mismatch: {key}")
        if con.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("smart-tag SQLite quick_check failed")
        missing_definitions = con.execute(
            """
            SELECT count(*) FROM tag_assignments a
            LEFT JOIN tag_definitions d USING(tag_key)
            WHERE d.tag_key IS NULL
            """
        ).fetchone()[0]
        if missing_definitions:
            raise ValueError("smart-tag assignments reference missing definitions")
        missing_rollups = con.execute(
            """
            SELECT count(*) FROM tag_assignments a
            JOIN tag_definitions d USING(tag_key)
            LEFT JOIN system_tag_membership m
              ON m.system_id=a.system_id AND m.tag_key=a.tag_key
            WHERE d.rollup IN ('direct','member_to_system')
              AND m.tag_key IS NULL
            """
        ).fetchone()[0]
        if missing_rollups:
            raise ValueError("smart-tag assignments are missing system rollups")
        counts = {
            table: con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "tag_definitions",
                "tag_assignments",
                "system_tag_membership",
                "source_definitions",
                "system_sources",
                "quarantine",
            )
        }
        if counts != manifest["counts"]:
            raise ValueError("smart-tag manifest counts do not match SQLite")
    finally:
        con.close()
    return {
        "status": "pass",
        "build_id": manifest["build_id"],
        "registry_hash": manifest["registry_hash"],
        "sample_limit": manifest.get("sample_limit"),
        "counts": counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify a Spacegate smart-tag artifact.")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--build-id")
    args = parser.parse_args()
    report = verify_artifact(args.artifact, args.build_id)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
