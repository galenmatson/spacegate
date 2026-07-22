#!/usr/bin/env python3
"""Independently audit and optionally reproduce the identity vocabulary seed."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import compile_permanent_identity_vocabulary as compiler


DEFAULT_SCRATCH = Path("/mnt/space/spacegate")


def audit(
    policy_path: Path, state_dir: Path, manifest_path: Path
) -> dict[str, Any]:
    policy = compiler.load_object(policy_path)
    compiler.validate_policy(policy)
    manifest = compiler.load_object(manifest_path)
    product = (manifest.get("products") or {}).get("aliases") or {}
    path = manifest_path.parent / str(product.get("path") or "")
    checks = {
        "manifest_status_pass": manifest.get("status") == "pass",
        "scientific_authority_false": manifest.get("scientific_authority") is False,
        "policy_sha256_match": manifest.get("policy_sha256") == compiler.file_sha256(policy_path),
        "compiler_sha256_match": manifest.get("compiler_sha256")
        == compiler.file_sha256(Path(compiler.__file__).resolve()),
        "product_exists": path.is_file(),
        "product_bytes_match": path.is_file() and path.stat().st_size == product.get("bytes"),
        "product_sha256_match": path.is_file()
        and compiler.file_sha256(path) == product.get("sha256"),
    }
    metrics: dict[str, Any] = {}
    if path.is_file():
        identity_db = (
            state_dir / "derived/evidence_lake_v2/identity"
            / policy["identity_graph_id"] / "identity_graph.duckdb"
        )
        con = duckdb.connect()
        try:
            con.execute(f"ATTACH {compiler.sql_literal(str(identity_db))} AS identity (READ_ONLY)")
            con.execute(
                f"CREATE VIEW vocabulary AS SELECT * FROM read_parquet({compiler.sql_literal(str(path))})"
            )
            columns = [str(row[0]) for row in con.execute("DESCRIBE vocabulary").fetchall()]
            metrics = {
                "row_count": int(con.execute("SELECT count(*) FROM vocabulary").fetchone()[0]),
                "duplicate_alias_seed_ids": int(con.execute(
                    "SELECT count(*) FROM (SELECT alias_seed_id FROM vocabulary GROUP BY 1 HAVING count(*)>1)"
                ).fetchone()[0]),
                "semantic_alias_duplicates": int(con.execute(
                    "SELECT count(*) FROM (SELECT stable_object_key,target_type,alias_norm FROM vocabulary GROUP BY 1,2,3 HAVING count(*)>1)"
                ).fetchone()[0]),
                "missing_identity": int(con.execute(
                    "SELECT count(*) FROM vocabulary v LEFT JOIN identity.canonical_object_nodes n ON n.object_type=v.target_type AND n.stable_object_key=v.stable_object_key WHERE n.stable_object_key IS NULL"
                ).fetchone()[0]),
                "empty_normalized_aliases": int(con.execute(
                    "SELECT count(*) FROM vocabulary WHERE nullif(trim(alias_norm),'') IS NULL"
                ).fetchone()[0]),
                "prohibited_column_count": len(set(columns) & set(policy["prohibited_scientific_columns"])),
            }
        finally:
            con.close()
        checks.update({
            "output_columns_match": columns == policy["output_columns"],
            "row_count_match": metrics["row_count"] == product.get("row_count"),
            "duplicate_alias_seed_ids_zero": metrics["duplicate_alias_seed_ids"] == 0,
            "semantic_alias_duplicates_zero": metrics["semantic_alias_duplicates"] == 0,
            "missing_identity_zero": metrics["missing_identity"] == 0,
            "empty_normalized_aliases_zero": metrics["empty_normalized_aliases"] == 0,
            "prohibited_column_count_zero": metrics["prohibited_column_count"] == 0,
        })
    failing = sorted(name for name, passed in checks.items() if not passed)
    return {
        "schema_version": "spacegate.permanent_identity_vocabulary_audit.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seed_id": manifest.get("seed_id"),
        "status": "pass" if not failing else "fail",
        "checks": checks,
        "failing_checks": failing,
        "metrics": metrics,
    }


def reproduce(
    policy_path: Path, state_dir: Path, manifest_path: Path, scratch_parent: Path
) -> dict[str, Any]:
    reference = compiler.load_object(manifest_path)
    scratch_parent.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="e7-identity-vocabulary-", dir=scratch_parent))
    try:
        reproduced = compiler.compile_vocabulary(policy_path, state_dir, scratch)
        reproduced_manifest = scratch / reproduced["seed_id"] / "manifest.json"
        independent = audit(policy_path, state_dir, reproduced_manifest)
        checks = {
            "seed_id_match": reference.get("seed_id") == reproduced.get("seed_id"),
            "policy_sha256_match": reference.get("policy_sha256") == reproduced.get("policy_sha256"),
            "compiler_sha256_match": reference.get("compiler_sha256") == reproduced.get("compiler_sha256"),
            "input_identity_match": reference.get("identity_graph_sha256") == reproduced.get("identity_graph_sha256"),
            "input_migration_core_match": reference.get("migration_source_core_sha256") == reproduced.get("migration_source_core_sha256"),
            "products_match": reference.get("products") == reproduced.get("products"),
            "verification_match": reference.get("verification") == reproduced.get("verification"),
            "independent_audit_pass": independent.get("status") == "pass",
        }
        result = {
            "schema_version": "spacegate.permanent_identity_vocabulary_reproduction.v1",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "seed_id": reference.get("seed_id"),
            "status": "pass" if all(checks.values()) else "fail",
            "checks": checks,
            "failing_checks": sorted(name for name, passed in checks.items() if not passed),
            "products": reproduced.get("products"),
            "reproduced_timing": reproduced.get("timing"),
            "independent_audit": independent,
            "scratch_removed": True,
        }
    finally:
        shutil.rmtree(scratch)
    result["scratch_removed"] = not scratch.exists()
    if not result["scratch_removed"]:
        result["status"] = "fail"
        result["failing_checks"].append("scratch_removed")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--reproduce", action="store_true")
    parser.add_argument("--scratch-parent", type=Path, default=DEFAULT_SCRATCH)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = (
        reproduce(
            args.policy.resolve(), args.state_dir.resolve(), args.manifest.resolve(),
            args.scratch_parent.resolve(),
        )
        if args.reproduce
        else audit(args.policy.resolve(), args.state_dir.resolve(), args.manifest.resolve())
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
