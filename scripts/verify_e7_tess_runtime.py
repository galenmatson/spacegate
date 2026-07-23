#!/usr/bin/env python3
"""Independently verify and reproduce an E7 TESS runtime artifact."""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import compile_e7_tess_runtime as compiler


DEFAULT_SCRATCH = Path("/mnt/space/spacegate")
REQUIRED_COLUMNS = {
    "tess_target_identity": {
        "tess_identity_id", "tic_id", "resolution_status", "star_id", "system_id",
        "source_row_hash", "retrieval_checksum", "transform_version",
    },
    "tess_missing_object_audit": {
        "audit_id", "tic_id", "resolution_status", "gap_class", "source_row_hash",
    },
    "toi_current_evidence": {
        "toi_evidence_id", "source_key", "tic_id", "toi", "disposition",
        "star_id", "system_id", "planet_id", "orbital_period_days",
        "transit_epoch_bjd", "planet_radius_earth", "source_row_hash",
        "retrieval_checksum", "transform_version",
    },
    "toi_disposition_history": {
        "history_id", "source_key", "tic_id", "disposition", "effective_at",
        "source_row_hash", "first_observed_at", "last_observed_at",
        "retrieval_checksum", "transform_version",
    },
}


def audit(policy_path: Path, state: Path, manifest_path: Path) -> dict[str, Any]:
    started = time.monotonic()
    policy = compiler.load_object(policy_path)
    compiler.validate_policy(policy)
    manifest = compiler.load_object(manifest_path)
    checks: dict[str, bool] = {
        "manifest_status_pass": manifest.get("status") == "pass",
        "manifest_schema": manifest.get("schema_version") == "spacegate.e7_tess_runtime_manifest.v1",
        "policy_sha256_match": manifest.get("policy_sha256") == compiler.file_sha256(policy_path),
        "compiler_sha256_match": manifest.get("compiler_sha256") == compiler.file_sha256(Path(compiler.__file__).resolve()),
        "stability_databases_not_opened": manifest.get("stability_databases_opened") == [],
        "canonical_inventory_not_mutated": manifest.get("canonical_inventory_mutations") == 0,
    }
    inputs = compiler.resolve_inputs(policy, state)
    con = duckdb.connect()
    metrics: dict[str, Any] = {}
    try:
        con.execute(f"ATTACH {compiler.sql_literal(inputs['core'])} AS core (READ_ONLY)")
        for table in compiler.PRODUCTS:
            path = manifest_path.parent / f"{table}.parquet"
            product = (manifest.get("products") or {}).get(path.name) or {}
            checks[f"{table}_exists"] = path.is_file()
            checks[f"{table}_bytes_match"] = path.is_file() and path.stat().st_size == product.get("bytes")
            checks[f"{table}_sha256_match"] = path.is_file() and compiler.file_sha256(path) == product.get("sha256")
            checks[f"{table}_byte_exact"] = product.get("determinism") == "byte_exact"
            if path.is_file():
                con.execute(
                    f"CREATE VIEW {table} AS SELECT * FROM read_parquet("
                    f"{compiler.sql_literal(path)})"
                )
                columns = {str(row[0]) for row in con.execute(f"DESCRIBE {table}").fetchall()}
                checks[f"{table}_required_columns"] = not (REQUIRED_COLUMNS[table] - columns)
        scalar = lambda sql: int(con.execute(sql).fetchone()[0] or 0)
        counts = {
            "targeted_tics": scalar("SELECT count(*) FROM tess_target_identity"),
            "targets_accepted": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='accepted'"),
            "targets_ambiguous": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='ambiguous'"),
            "targets_excluded": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='excluded'"),
            "targets_missing": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='missing'"),
            "targets_source_missing": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='source_missing'"),
            "missing_object_audit": scalar("SELECT count(*) FROM tess_missing_object_audit"),
            "tois": scalar("SELECT count(*) FROM toi_current_evidence"),
            "toi_confirmed_known": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('CP','KP')"),
            "toi_candidates": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('PC','APC')"),
            "toi_negative": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('FP','FA')"),
            "toi_unclassified": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IS NULL"),
            "toi_confirmed_known_planet_links": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('CP','KP') AND planet_id IS NOT NULL"),
            "toi_history_events": scalar("SELECT count(*) FROM toi_disposition_history"),
            "canonical_inventory_mutations": int(manifest.get("canonical_inventory_mutations", -1)),
        }
        expected = {key: int(value) for key, value in policy["acceptance"].items()}
        scalars = {
            "acceptance_count_delta": sum(abs(counts.get(key, -1)-value) for key, value in expected.items()),
            "manifest_count_delta": sum(abs(counts.get(key, -1)-int(value)) for key, value in (manifest.get("verification", {}).get("counts") or {}).items()),
            "duplicate_tic_ids": scalar("SELECT count(*) FROM (SELECT tic_id FROM tess_target_identity GROUP BY 1 HAVING count(*)<>1)"),
            "duplicate_toi_ids": scalar("SELECT count(*) FROM (SELECT source_key FROM toi_current_evidence GROUP BY 1 HAVING count(*)<>1)"),
            "duplicate_history_events": scalar("SELECT count(*) FROM (SELECT source_key,disposition,effective_at FROM toi_disposition_history GROUP BY ALL HAVING count(*)<>1)"),
            "accepted_target_without_clean_ids": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='accepted' AND (star_id IS NULL OR system_id IS NULL)"),
            "unaccepted_target_with_clean_ids": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status<>'accepted' AND (star_id IS NOT NULL OR system_id IS NOT NULL)"),
            "candidate_or_negative_planet_links": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('PC','APC','FP','FA') AND planet_id IS NOT NULL"),
            "confirmed_link_without_clean_planet": scalar("SELECT count(*) FROM toi_current_evidence t LEFT JOIN core.planets p ON p.planet_id=t.planet_id WHERE t.planet_id IS NOT NULL AND p.planet_id IS NULL"),
            "accepted_host_without_clean_objects": scalar("SELECT count(*) FROM toi_current_evidence t LEFT JOIN core.stars s ON s.star_id=t.star_id LEFT JOIN core.systems y ON y.system_id=t.system_id WHERE t.host_resolution_status='accepted' AND (s.star_id IS NULL OR y.system_id IS NULL)"),
            "history_without_current_toi": scalar("SELECT count(*) FROM toi_disposition_history h LEFT JOIN toi_current_evidence t USING(source_key) WHERE t.source_key IS NULL"),
            "current_toi_without_history": scalar("SELECT count(*) FROM toi_current_evidence t LEFT JOIN toi_disposition_history h USING(source_key) WHERE h.source_key IS NULL"),
            "history_current_disposition_mismatch": scalar("SELECT count(*) FROM toi_current_evidence t JOIN toi_disposition_history h USING(source_key) WHERE t.disposition IS DISTINCT FROM h.disposition"),
            "missing_target_audit_delta": abs(scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status<>'accepted'")-counts["missing_object_audit"]),
            "missing_source_provenance": scalar("SELECT count(*) FROM toi_current_evidence WHERE source_row_hash IS NULL OR retrieval_checksum IS NULL OR source_version IS NULL"),
        }
        checks.update({key: value == 0 for key, value in scalars.items()})
        metrics = {"counts": counts, "scalar_checks": scalars}
    finally:
        con.close()
    failures = [key for key, value in checks.items() if not value]
    return {
        "schema_version": "spacegate.e7_tess_runtime_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks,"failing_checks": failures,"metrics": metrics,
        "wall_seconds": round(time.monotonic()-started, 6),
    }


def reproduce(
    policy_path: Path, state: Path, manifest_path: Path, scratch_parent: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    accepted = compiler.load_object(manifest_path)
    scratch = Path(tempfile.mkdtemp(prefix="e7-tess-runtime-reproduction-", dir=scratch_parent))
    try:
        rebuilt = compiler.compile_runtime(
            policy_path,state,scratch / "artifacts",link_into_state=False,
        )
        rebuilt_dir = scratch / "artifacts" / rebuilt["build_id"]
        rebuilt_manifest = rebuilt_dir / "manifest.json"
        independent = audit(policy_path,state,rebuilt_manifest)
        checks = {
            "build_id_match": rebuilt.get("build_id") == accepted.get("build_id"),
            "verification_match": rebuilt.get("verification") == accepted.get("verification"),
            "products_match": rebuilt.get("products") == accepted.get("products"),
            "independent_verification_pass": independent.get("status") == "pass",
        }
        rebuild_performance = rebuilt.get("performance") or {}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    checks["scratch_removed"] = not scratch.exists()
    failures = [key for key, value in checks.items() if not value]
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "schema_version": "spacegate.e7_tess_runtime_reproduction.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": accepted.get("build_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks,"failing_checks": failures,
        "rebuild_performance": rebuild_performance,
        "total_timing": {
            "wall_seconds": round(time.monotonic()-started, 6),
            "cpu_seconds": round(time.process_time(), 6),
            "peak_rss_kib": int(usage.ru_maxrss),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=compiler.DEFAULT_STATE)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--reproduce", action="store_true")
    parser.add_argument("--scratch-parent", type=Path, default=DEFAULT_SCRATCH)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.reproduce:
        report = reproduce(
            args.policy.resolve(),args.state_dir.resolve(),args.manifest.resolve(),
            args.scratch_parent.resolve(),
        )
    else:
        report = audit(
            args.policy.resolve(),args.state_dir.resolve(),args.manifest.resolve(),
        )
    if args.report:
        compiler.write_object_atomic(args.report.resolve(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
