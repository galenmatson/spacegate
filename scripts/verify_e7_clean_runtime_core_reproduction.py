#!/usr/bin/env python3
"""Rebuild clean runtime CORE in isolation and compare canonical products."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb

import compile_e7_clean_runtime_core as compiler
import verify_e7_clean_runtime_core as verifier


DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/e7_clean_runtime_core_reproduction.json"
)
DEFAULT_SCRATCH_ROOT = Path("/mnt/space/spacegate")


def logical_signatures(database: Path) -> dict[str, dict[str, int]]:
    con = duckdb.connect(str(database), read_only=True)
    try:
        tables = [
            str(row[0])
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY 1"
            ).fetchall()
        ]
        signatures: dict[str, dict[str, int]] = {}
        for table in tables:
            columns = [str(row[0]) for row in con.execute(f"DESCRIBE {table}").fetchall()]
            quoted = ",".join('"' + column.replace('"', '""') + '"' for column in columns)
            count, digest = con.execute(
                f"SELECT count(*)::BIGINT,coalesce(bit_xor(hash({quoted})),0)::UBIGINT FROM {table}"
            ).fetchone()
            signatures[table] = {"rows": int(count), "hash_xor": int(digest)}
        return signatures
    finally:
        con.close()


def reproduce(
    build_id: str,
    artifact_root: Path,
    policy: Path,
    state: Path,
    scratch_root: Path,
) -> dict[str, Any]:
    accepted_dir = artifact_root / build_id
    accepted = compiler.load_object(accepted_dir / "manifest.json")
    scratch = Path(tempfile.mkdtemp(prefix="e7-runtime-core-reproduction-", dir=scratch_root))
    output_root = scratch / "out"
    try:
        rebuilt = compiler.compile_runtime_core(
            policy, state, output_root, link_into_state=False
        )
        rebuilt_dir = output_root / rebuilt["build_id"]
        differing_files: list[dict[str, Any]] = []
        for relative, expected in sorted((accepted.get("products") or {}).items()):
            if expected.get("determinism") != "byte_exact":
                continue
            actual = (rebuilt.get("products") or {}).get(relative) or {}
            if actual.get("sha256") != expected.get("sha256") or actual.get("bytes") != expected.get("bytes"):
                differing_files.append({"path": relative, "expected": expected, "actual": actual})
        accepted_hierarchy = logical_signatures(accepted_dir / "canonical_hierarchy.duckdb")
        rebuilt_hierarchy = logical_signatures(rebuilt_dir / "canonical_hierarchy.duckdb")
        independent = verifier.verify(rebuilt_dir)
        status = (
            "pass"
            if rebuilt["build_id"] == build_id
            and not differing_files
            and accepted_hierarchy == rebuilt_hierarchy
            and independent["status"] == "pass"
            else "fail"
        )
        return {
            "schema_version": "spacegate.e7_clean_runtime_core_reproduction.v1",
            "build_id": build_id,
            "status": status,
            "rebuilt_build_id": rebuilt["build_id"],
            "differing_files": differing_files,
            "hierarchy_logical_match": accepted_hierarchy == rebuilt_hierarchy,
            "independent_verification_status": independent["status"],
            "performance": rebuilt["performance"],
            "scratch_removed": True,
        }
    finally:
        shutil.rmtree(scratch)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--artifact-root", type=Path, default=compiler.DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--scratch-root", type=Path, default=DEFAULT_SCRATCH_ROOT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = reproduce(
        args.build_id,
        args.artifact_root.resolve(),
        args.policy.resolve(),
        args.state_dir.resolve(),
        args.scratch_root.resolve(),
    )
    compiler.write_object_atomic(args.report.resolve(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
