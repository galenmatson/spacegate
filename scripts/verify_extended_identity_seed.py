#!/usr/bin/env python3
"""Independently audit and reproduce the permanent extended-object identity seed."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import compile_extended_identity_seed as compiler


DEFAULT_SCRATCH = Path("/mnt/space/spacegate")


def audit(policy_path: Path, manifest_path: Path) -> dict[str, Any]:
    started = time.monotonic()
    policy = compiler.load_object(policy_path)
    compiler.validate_policy(policy)
    manifest = compiler.load_object(manifest_path)
    checks: dict[str, bool] = {
        "manifest_status_pass": manifest.get("status") == "pass",
        "scientific_authority_false": manifest.get("scientific_authority") is False,
        "policy_sha256_match": manifest.get("policy_sha256") == compiler.file_hash(policy_path),
        "compiler_sha256_match": manifest.get("compiler_sha256")
        == compiler.file_hash(Path(compiler.__file__).resolve()),
    }
    metrics: dict[str, Any] = {}
    con = duckdb.connect()
    try:
        for name, spec in policy["tables"].items():
            product = (manifest.get("products") or {}).get(name) or {}
            path = manifest_path.parent / str(product.get("path") or "")
            prefix = f"{name}_"
            checks[prefix + "exists"] = path.is_file()
            checks[prefix + "bytes_match"] = path.is_file() and path.stat().st_size == product.get("bytes")
            checks[prefix + "sha256_match"] = path.is_file() and compiler.file_hash(path) == product.get("sha256")
            if not path.is_file():
                continue
            columns = [str(row[0]) for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet({compiler.sql_literal(path)})"
            ).fetchall()]
            rows = int(con.execute(
                f"SELECT count(*) FROM read_parquet({compiler.sql_literal(path)})"
            ).fetchone()[0])
            checks[prefix + "columns_match"] = columns == spec["columns"]
            checks[prefix + "row_count_match"] = rows == product.get("row_count")
            metrics[name] = {"rows": rows, "columns": len(columns)}
        products = manifest.get("products") or {}
        nodes = manifest_path.parent / products["extended_identity_nodes"]["path"]
        aliases = manifest_path.parent / products["extended_object_aliases"]["path"]
        identifiers = manifest_path.parent / products["extended_object_identifiers"]["path"]
        if all(path.is_file() for path in (nodes, aliases, identifiers)):
            con.execute(f"CREATE VIEW nodes AS SELECT * FROM read_parquet({compiler.sql_literal(nodes)})")
            con.execute(f"CREATE VIEW aliases AS SELECT * FROM read_parquet({compiler.sql_literal(aliases)})")
            con.execute(f"CREATE VIEW identifiers AS SELECT * FROM read_parquet({compiler.sql_literal(identifiers)})")
            invariants = {
                "duplicate_object_ids": "SELECT count(*) FROM (SELECT extended_object_id FROM nodes GROUP BY 1 HAVING count(*)>1)",
                "duplicate_stable_keys": "SELECT count(*) FROM (SELECT stable_object_key FROM nodes GROUP BY 1 HAVING count(*)>1)",
                "empty_stable_keys": "SELECT count(*) FROM nodes WHERE nullif(trim(stable_object_key),'') IS NULL",
                "orphan_aliases": "SELECT count(*) FROM aliases a LEFT JOIN nodes n USING(extended_object_id) WHERE n.extended_object_id IS NULL",
                "orphan_identifiers": "SELECT count(*) FROM identifiers i LEFT JOIN nodes n USING(extended_object_id) WHERE n.extended_object_id IS NULL",
            }
            observed = {name: int(con.execute(query).fetchone()[0]) for name, query in invariants.items()}
            metrics["invariants"] = observed
            checks["invariants_zero"] = not any(observed.values())
    finally:
        con.close()
    failures = sorted(name for name, passed in checks.items() if not passed)
    return {
        "schema_version": "spacegate.extended_identity_seed_audit.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seed_id": manifest.get("seed_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks,
        "failing_checks": failures,
        "metrics": metrics,
        "wall_seconds": round(time.monotonic() - started, 6),
    }


def reproduce(
    policy_path: Path, state_dir: Path, manifest_path: Path, scratch_parent: Path
) -> dict[str, Any]:
    reference = compiler.load_object(manifest_path)
    scratch = Path(tempfile.mkdtemp(prefix="e7-extended-identity-", dir=scratch_parent))
    try:
        rebuilt = compiler.compile_seed(policy_path, state_dir, scratch)
        rebuilt_manifest = scratch / rebuilt["seed_id"] / "manifest.json"
        independent = audit(policy_path, rebuilt_manifest)
        checks = {
            "seed_id_match": rebuilt.get("seed_id") == reference.get("seed_id"),
            "policy_match": rebuilt.get("policy_sha256") == reference.get("policy_sha256"),
            "compiler_match": rebuilt.get("compiler_sha256") == reference.get("compiler_sha256"),
            "source_match": rebuilt.get("migration_source_core_sha256")
            == reference.get("migration_source_core_sha256"),
            "products_match": rebuilt.get("products") == reference.get("products"),
            "verification_match": rebuilt.get("verification") == reference.get("verification"),
            "independent_audit_pass": independent.get("status") == "pass",
        }
        report = {
            "schema_version": "spacegate.extended_identity_seed_reproduction.v1",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "seed_id": reference.get("seed_id"),
            "status": "pass" if all(checks.values()) else "fail",
            "checks": checks,
            "failing_checks": sorted(name for name, passed in checks.items() if not passed),
            "products": rebuilt.get("products"),
            "reproduced_timing": rebuilt.get("timing"),
            "independent_audit": independent,
            "scratch_removed": True,
        }
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    report["scratch_removed"] = not scratch.exists()
    if not report["scratch_removed"]:
        report["status"] = "fail"
        report["failing_checks"].append("scratch_removed")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--reproduce", action="store_true")
    parser.add_argument("--scratch-parent", type=Path, default=DEFAULT_SCRATCH)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = reproduce(
        args.policy.resolve(), args.state_dir.resolve(), args.manifest.resolve(),
        args.scratch_parent.resolve(),
    ) if args.reproduce else audit(args.policy.resolve(), args.manifest.resolve())
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
