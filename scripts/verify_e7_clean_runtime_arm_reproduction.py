#!/usr/bin/env python3
"""Rebuild clean runtime ARM in isolation and compare logical products."""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import compile_e7_clean_runtime_arm as compiler
import verify_e7_clean_runtime_arm as verifier


DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/e7_clean_runtime_arm_reproduction.json"
)


def logical_signatures(database: Path) -> dict[str, dict[str, Any]]:
    con = duckdb.connect(str(database), read_only=True)
    try:
        tables = [
            str(row[0])
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY 1"
            ).fetchall()
        ]
        signatures: dict[str, dict[str, Any]] = {}
        for table in tables:
            described = [tuple(str(value) for value in row) for row in con.execute(f"DESCRIBE {table}").fetchall()]
            columns = [row[0] for row in described]
            quoted = ",".join('"' + column.replace('"', '""') + '"' for column in columns)
            count, digest = con.execute(
                f"SELECT count(*)::BIGINT,coalesce(bit_xor(hash({quoted})),0)::UBIGINT FROM {table}"
            ).fetchone()
            signatures[table] = {
                "rows": int(count),
                "hash_xor": int(digest),
                "schema_sha256": hashlib.sha256(
                    json.dumps(described, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
            }
        return signatures
    finally:
        con.close()


def reproduce(
    *, build_id: str, artifact_root: Path, policy: Path, state: Path,
    scratch_parent: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    accepted_dir = artifact_root / build_id
    accepted = compiler.load_object(accepted_dir / "manifest.json")
    scratch = Path(tempfile.mkdtemp(prefix="e7-runtime-arm-reproduction-", dir=scratch_parent))
    try:
        rebuilt = compiler.compile_runtime_arm(
            policy, state, scratch / "artifacts", link_into_state=False
        )
        rebuilt_dir = scratch / "artifacts" / rebuilt["build_id"]
        accepted_signatures = logical_signatures(accepted_dir / "arm.duckdb")
        rebuilt_signatures = logical_signatures(rebuilt_dir / "arm.duckdb")
        independent = verifier.verify(rebuilt_dir)
        checks = {
            "build_id_match": rebuilt.get("build_id") == accepted.get("build_id") == build_id,
            "logical_table_signatures_match": rebuilt_signatures == accepted_signatures,
            "verification_sections_match": rebuilt.get("verification") == accepted.get("verification"),
            "independent_verification_pass": independent.get("status") == "pass",
        }
        differing_tables = sorted(
            table
            for table in set(accepted_signatures) | set(rebuilt_signatures)
            if accepted_signatures.get(table) != rebuilt_signatures.get(table)
        )
        rebuild_performance = rebuilt.get("performance") or {}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    checks["scratch_removed"] = not scratch.exists()
    failures = [key for key, value in checks.items() if not value]
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "schema_version": "spacegate.e7_clean_runtime_arm_reproduction.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": build_id,
        "status": "pass" if not failures else "fail",
        "checks": checks,
        "failing_checks": failures,
        "differing_tables": differing_tables,
        "rebuild_performance": rebuild_performance,
        "total_timing": {
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "peak_rss_kib": int(usage.ru_maxrss),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--artifact-root", type=Path, default=compiler.DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--scratch-parent", type=Path, default=Path("/mnt/space/spacegate"))
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = reproduce(
        build_id=args.build_id,
        artifact_root=args.artifact_root.resolve(),
        policy=args.policy.resolve(),
        state=args.state_dir.resolve(),
        scratch_parent=args.scratch_parent.resolve(),
    )
    compiler.write_object_atomic(args.report.resolve(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
