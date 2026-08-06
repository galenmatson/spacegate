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
        invalid_hero = con.execute(
            """
            SELECT count(*)
            FROM system_hero_tags h
            LEFT JOIN system_tag_membership m
              ON m.system_id=h.system_id AND m.tag_id=h.tag_id
            WHERE m.tag_id IS NULL OR h.hero_rank NOT BETWEEN 1 AND 4
               OR h.hero_family_code NOT BETWEEN 1 AND 3
               OR h.claim_mode_code NOT BETWEEN 1 AND 8
               OR trim(h.origin_target_key)=''
            """
        ).fetchone()[0]
        if invalid_hero:
            raise ValueError("smart-tag hero projection contains invalid rows")
        oversized_heroes = con.execute(
            """
            SELECT count(*) FROM (
              SELECT system_id,count(*) AS n
              FROM system_hero_tags GROUP BY system_id HAVING n>4
            )
            """
        ).fetchone()[0]
        if oversized_heroes:
            raise ValueError("smart-tag hero projection exceeds composition budget")
        invalid_subjects = con.execute(
            """
            SELECT count(*)
            FROM subject_tag_assignments a
            LEFT JOIN tag_definitions d USING(tag_id)
            WHERE d.tag_id IS NULL
               OR a.scope_code NOT IN (1,2)
               OR a.target_object_id<0
               OR (a.target_object_id=0 AND trim(a.target_key)='')
               OR (a.scope_code=2 AND a.target_object_id=0)
               OR a.evidence_status_code NOT BETWEEN 1 AND 10
               OR a.basis_code NOT BETWEEN 1 AND 3
            """
        ).fetchone()[0]
        if invalid_subjects:
            raise ValueError("smart-tag subject projection contains invalid rows")
        subject_count = con.execute(
            "SELECT count(*) FROM subject_tag_assignments"
        ).fetchone()[0]
        coverage = json.loads(
            (root / manifest["artifacts"]["coverage"]["path"]).read_text(
                encoding="utf-8"
            )
        )
        expected_subject_count = int(
            (coverage.get("hot_projection_counts") or {}).get(
                "subject_tag_assignments", -1
            )
        )
        if subject_count != expected_subject_count:
            raise ValueError(
                "smart-tag subject projection count does not match coverage"
            )
        hot_counts = {
            table: con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "tag_definitions",
                "system_tag_membership",
                "system_hero_tags",
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
        "subject_tag_assignments": subject_count,
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
