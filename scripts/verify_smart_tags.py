#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from compile_smart_tags import (
    ASSIGNMENT_SCHEMA,
    COMPILER_VERSION,
    MANIFEST_SCHEMA,
    SCHEMA_VERSION,
    SOURCE_SUMMARY_SCHEMA,
    SOURCE_CONTRIBUTION_SCHEMA,
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
    if manifest.get("compiler_version") != COMPILER_VERSION:
        raise ValueError("smart-tag compiler version mismatch")
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
            "source_contribution_schema_version": SOURCE_CONTRIBUTION_SCHEMA,
            "build_id": manifest["build_id"],
            "registry_hash": manifest["registry_hash"],
            "compiler_version": COMPILER_VERSION,
        }
        for key, expected in expected_metadata.items():
            if metadata.get(key) != expected:
                raise ValueError(f"smart-tag metadata mismatch: {key}")
        if con.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("smart-tag SQLite quick_check failed")
        missing_definitions = con.execute(
            """
            SELECT count(*) FROM system_tag_membership m
            LEFT JOIN tag_definitions d USING(tag_id)
            WHERE d.tag_id IS NULL
            """
        ).fetchone()[0]
        if missing_definitions:
            raise ValueError("smart-tag memberships reference missing definitions")
        zero_status = con.execute(
            "SELECT count(*) FROM system_tag_membership "
            "WHERE evidence_status_mask=0"
        ).fetchone()[0]
        if zero_status:
            raise ValueError(
                "smart-tag membership has an unrepresented evidence status"
            )
        hot_counts = {
            table: con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "tag_definitions",
                "system_tag_membership",
                "source_definitions",
                "system_sources",
                "quarantine",
            )
        }
        for key, value in hot_counts.items():
            if value != manifest["counts"][key]:
                raise ValueError(
                    f"smart-tag manifest count does not match SQLite: {key}"
                )
    finally:
        con.close()
    assignments = root / manifest["artifacts"]["assignments"]["path"]
    assignment_count = pq.ParquetFile(assignments).metadata.num_rows
    if assignment_count != manifest["counts"]["tag_assignments"]:
        raise ValueError("smart-tag assignment count does not match Parquet")
    contributions = root / manifest["artifacts"]["source_contributions"]["path"]
    contribution_count = pq.ParquetFile(contributions).metadata.num_rows
    if contribution_count != manifest["counts"]["source_contributions"]:
        raise ValueError(
            "smart-tag source-contribution count does not match Parquet"
        )
    counts = {
        **hot_counts,
        "tag_assignments": assignment_count,
        "source_contributions": contribution_count,
    }
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
