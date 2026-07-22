#!/usr/bin/env python3
"""Rebuild an E6 shadow product in isolation and compare deterministic outputs."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import audit_e6_shadow_build as auditor
import compile_e6_shadow_build as compiler
import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_POLICY = ROOT / "config/evidence_lake/e6_shadow_build.json"
DEFAULT_WORK_ROOT = Path("/mnt/space/spacegate/e6-reproduction")


def table_logical_hash(database: Path, table: str, scratch: Path) -> dict[str, Any]:
    scratch.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(
        str(database),
        read_only=True,
        config={"temp_directory": str(scratch), "threads": "12", "memory_limit": "48GB"},
    )
    try:
        rows = con.execute(
            f"""
            WITH row_hashes AS (
              SELECT sha256(to_json(source_row)) row_hash
              FROM {compiler.sql_identifier(table)} source_row
            ), bucket_hashes AS (
              SELECT substr(row_hash,1,2) bucket,count(*) row_count,
                     sha256(string_agg(row_hash,'' ORDER BY row_hash)) bucket_hash
              FROM row_hashes GROUP BY bucket
            )
            SELECT bucket,row_count,bucket_hash FROM bucket_hashes ORDER BY bucket
            """
        ).fetchall()
    finally:
        con.close()
    parts = [f"{bucket}:{count}:{digest}" for bucket, count, digest in rows]
    return {
        "row_count": sum(int(row[1]) for row in rows),
        "logical_sha256": hashlib.sha256("|".join(parts).encode("ascii")).hexdigest(),
        "algorithm": "sha256_bucketed_json_row_multiset_v1",
    }


def logical_product_hashes(build_dir: Path, scratch: Path) -> dict[str, dict[str, Any]]:
    tables = {
        "core.duckdb": ["aliases", "build_metadata", "planets", "stars", "systems"],
        "arm.duckdb": [
            "build_metadata",
            "e6_evidence_artifact_registry",
            "e6_selected_planet_parameters",
            "e6_selected_stellar_astrometry",
            "e6_selected_stellar_classification",
            "e6_selected_stellar_photometry",
            "e6_selected_stellar_physics",
            "e6_selected_stellar_variability",
            "e6_selected_stellar_parameter_subject_supplement",
            "e6_selected_stellar_display_classifications",
            "stellar_leaf_display_classifications",
        ],
        "canonical_hierarchy.duckdb": ["build_metadata"],
        "disc.duckdb": ["build_metadata"],
    }
    return {
        f"{database}:{table}": table_logical_hash(
            build_dir / database, table, scratch / database / table
        )
        for database, database_tables in tables.items()
        for table in database_tables
    }


def verify_reproduction(
    *,
    state: Path,
    build_id: str,
    policy_path: Path,
    work_root: Path,
    report_path: Path | None = None,
    memory_limit: str = "48GB",
    threads: int = 12,
) -> dict[str, Any]:
    state = state.resolve()
    reference_dir = state / "out" / build_id
    reference_manifest = compiler.load_json(reference_dir / "manifest.json")
    policy = compiler.load_json(policy_path.resolve())
    compiler.validate_policy(policy)
    work_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=work_root))
    reproduced_state = temporary / "state"
    reproduced_out = reproduced_state / "out"
    reproduced_derived = reproduced_state / "derived"
    reproduced_out.mkdir(parents=True)
    reproduced_derived.mkdir(parents=True)
    (reproduced_derived / "evidence_lake_v2").symlink_to(
        state / "derived/evidence_lake_v2", target_is_directory=True
    )
    base_id = str(policy["stability_reference_build_id"])
    (reproduced_out / base_id).symlink_to(
        state / "out" / base_id, target_is_directory=True
    )
    spill = temporary / "spill"
    started = time.monotonic()
    try:
        compiler.compile_shadow_build(
            state=reproduced_state,
            policy_path=policy_path,
            memory_limit=memory_limit,
            threads=threads,
            temp_directory=spill,
        )
        reproduced_dir = reproduced_out / build_id
        reproduced_manifest = compiler.load_json(reproduced_dir / "manifest.json")
        reference_products = reference_manifest["report"]["product_files"]
        reproduced_products = reproduced_manifest["report"]["product_files"]
        reference_logical = logical_product_hashes(reference_dir, spill / "logical-reference")
        reproduced_logical = logical_product_hashes(
            reproduced_dir, spill / "logical-reproduced"
        )
        checks = {
            "build_id_match": int(reproduced_manifest.get("build_id") != build_id),
            "build_sha256_match": int(
                reproduced_manifest.get("build_sha256")
                != reference_manifest.get("build_sha256")
            ),
            "product_file_set_match": int(
                set(reproduced_products) != set(reference_products)
            ),
            "product_logical_hashes_match": int(reproduced_logical != reference_logical),
            "inventory_match": int(
                reproduced_manifest["report"].get("inventory_after")
                != reference_manifest["report"].get("inventory_after")
            ),
            "core_update_report_match": int(
                reproduced_manifest["report"].get("core_updates")
                != reference_manifest["report"].get("core_updates")
            ),
            "projection_counts_match": int(
                reproduced_manifest["report"].get("projection_table_counts")
                != reference_manifest["report"].get("projection_table_counts")
            ),
            "selected_consumer_report_match": int(
                {
                    key: reproduced_manifest["report"]
                    .get("selected_consumer_report", {})
                    .get(key)
                    for key in (
                        "status",
                        "projection_version",
                        "stellar_parameter_rows",
                        "stellar_classification_rows",
                        "classification_by_basis",
                        "classification_conflicts",
                        "classification_alternative_disagreements",
                        "checks",
                    )
                }
                != {
                    key: reference_manifest["report"]
                    .get("selected_consumer_report", {})
                    .get(key)
                    for key in (
                        "status",
                        "projection_version",
                        "stellar_parameter_rows",
                        "stellar_classification_rows",
                        "classification_by_basis",
                        "classification_conflicts",
                        "classification_alternative_disagreements",
                        "checks",
                    )
                }
            ),
            "stellar_leaf_report_match": int(
                {
                    key: reproduced_manifest["report"]
                    .get("stellar_leaf_report", {})
                    .get(key)
                    for key in (
                        "status",
                        "schema_version",
                        "rows",
                        "by_status",
                        "classification_conflicts",
                        "duplicate_leaf_keys",
                        "invalid_rows",
                    )
                }
                != {
                    key: reference_manifest["report"]
                    .get("stellar_leaf_report", {})
                    .get(key)
                    for key in (
                        "status",
                        "schema_version",
                        "rows",
                        "by_status",
                        "classification_conflicts",
                        "duplicate_leaf_keys",
                        "invalid_rows",
                    )
                }
            ),
        }
        reproduced_audit = auditor.audit_build(
            state=reproduced_state,
            build_id=build_id,
            policy_path=policy_path,
        )
        failing = {key: value for key, value in checks.items() if value != 0}
        report = {
            "schema_version": "spacegate.e6_shadow_reproduction.v1",
            "status": "pass" if not failing else "fail",
            "build_id": build_id,
            "reference_manifest_sha256": compiler.file_sha256(
                reference_dir / "manifest.json"
            ),
            "reproduced_manifest_sha256": compiler.file_sha256(
                reproduced_dir / "manifest.json"
            ),
            "reference_products": reference_products,
            "reproduced_products": reproduced_products,
            "logical_hash_scope": {
                "generated_or_mutated_tables": sorted(reference_logical),
                "copied_projection_tables": (
                    "covered by pinned source database hashes and exact copied-table counts"
                ),
                "unchanged_base_tables": "covered by compiler copy contract and independent audit",
            },
            "reference_logical_hashes": reference_logical,
            "reproduced_logical_hashes": reproduced_logical,
            "physical_byte_hashes_match": reproduced_products == reference_products,
            "checks": checks,
            "failing_checks": failing,
            "reproduced_audit_status": reproduced_audit["status"],
            "wall_seconds": round(time.monotonic() - started, 6),
        }
        if report_path:
            compiler.atomic_json(report_path, report)
        if failing:
            raise ValueError(f"E6 shadow reproduction failed: {failing}")
        return report
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--work-root", type=Path, default=DEFAULT_WORK_ROOT)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--memory-limit", default="48GB")
    parser.add_argument("--threads", type=int, default=12)
    args = parser.parse_args()
    report = verify_reproduction(
        state=args.state_dir,
        build_id=args.build_id,
        policy_path=args.policy,
        work_root=args.work_root,
        report_path=args.report,
        memory_limit=args.memory_limit,
        threads=args.threads,
    )
    print(f"E6 shadow reproduction {report['build_id']} {report['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
