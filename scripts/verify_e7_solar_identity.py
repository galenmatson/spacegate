#!/usr/bin/env python3
"""Independently verify and reproduce an E7 permanent Solar identity artifact."""

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

import compile_e7_solar_identity as compiler


DEFAULT_SCRATCH = Path("/mnt/space/spacegate")
TABLES = {
    "solar_component_identities.parquet": "identities",
    "solar_component_aliases.parquet": "aliases",
    "solar_component_identifiers.parquet": "identifiers",
    "solar_relation_identity_outcomes.parquet": "relation_outcomes",
}
PROHIBITED_IDENTITY_COLUMNS = {
    "ra_deg", "dec_deg", "dist_pc", "dist_ly", "mass", "radius",
    "orbital_period_days", "semi_major_axis_au", "eccentricity",
}


def audit(policy_path: Path, state: Path, manifest_path: Path) -> dict[str, Any]:
    started = time.monotonic()
    policy = compiler.load_object(policy_path)
    compiler.validate_policy(policy)
    manifest = compiler.load_object(manifest_path)
    checks: dict[str, bool] = {
        "manifest_status_pass": manifest.get("status") == "pass",
        "manifest_schema": manifest.get("schema_version")
        == "spacegate.e7_solar_identity_manifest.v1",
        "scientific_authority_false": manifest.get("scientific_authority") is False,
        "stability_databases_not_opened": manifest.get("stability_databases_opened") == [],
        "policy_sha256_match": manifest.get("policy_sha256") == compiler.file_sha256(policy_path),
        "compiler_sha256_match": manifest.get("compiler_sha256")
        == compiler.file_sha256(Path(compiler.__file__).resolve()),
    }
    evidence_root, evidence_manifest = compiler.resolve_input(
        state, policy["inputs"]["scientific_evidence"]
    )
    core_root, core_manifest = compiler.resolve_input(
        state, policy["inputs"]["clean_runtime_core"]
    )
    evidence_db = compiler.resolve_database(
        evidence_root, evidence_manifest, "scientific_evidence.duckdb"
    )
    core_db = compiler.resolve_database(core_root, core_manifest, "core.duckdb")
    product_paths: dict[str, Path] = {}
    metrics: dict[str, Any] = {}
    for filename, count_key in TABLES.items():
        product = (manifest.get("products") or {}).get(filename) or {}
        path = manifest_path.parent / filename
        product_paths[count_key] = path
        prefix = filename.removesuffix(".parquet")
        checks[f"{prefix}_exists"] = path.is_file()
        checks[f"{prefix}_bytes_match"] = path.is_file() and path.stat().st_size == product.get("bytes")
        checks[f"{prefix}_sha256_match"] = path.is_file() and compiler.file_sha256(path) == product.get("sha256")
        checks[f"{prefix}_byte_exact"] = product.get("determinism") == "byte_exact"

    con = duckdb.connect()
    try:
        for name, path in product_paths.items():
            con.execute(
                f"CREATE VIEW {name} AS SELECT * FROM read_parquet({compiler.sql_literal(path)})"
            )
        con.execute(f"ATTACH {compiler.sql_literal(evidence_db)} AS evidence (READ_ONLY)")
        con.execute(f"ATTACH {compiler.sql_literal(core_db)} AS core (READ_ONLY)")
        counts = {
            "identities": int(con.execute("SELECT count(*) FROM identities").fetchone()[0]),
            "natural_identities": int(con.execute("SELECT count(*) FROM identities WHERE identity_kind='natural'").fetchone()[0]),
            "artificial_identities": int(con.execute("SELECT count(*) FROM identities WHERE identity_kind='artificial'").fetchone()[0]),
            "core_bound_identities": int(con.execute("SELECT count(*) FROM identities WHERE core_object_id IS NOT NULL").fetchone()[0]),
            "arm_only_identities": int(con.execute("SELECT count(*) FROM identities WHERE core_object_id IS NULL").fetchone()[0]),
            "aliases": int(con.execute("SELECT count(*) FROM aliases").fetchone()[0]),
            "identifiers": int(con.execute("SELECT count(*) FROM identifiers").fetchone()[0]),
            "relation_outcomes": int(con.execute("SELECT count(*) FROM relation_outcomes").fetchone()[0]),
            "relations_accepted": int(con.execute("SELECT count(*) FROM relation_outcomes WHERE relation_status='accepted'").fetchone()[0]),
            "relations_reference_origin": int(con.execute("SELECT count(*) FROM relation_outcomes WHERE relation_status='reference_origin'").fetchone()[0]),
            "canonical_containment_promotions": int(con.execute("SELECT count(*) FROM relation_outcomes WHERE canonical_containment").fetchone()[0]),
        }
        expected = {key: int(value) for key, value in policy["acceptance"].items()}
        scalar_checks = {
            "acceptance_count_delta": sum(abs(counts.get(key, -1) - value) for key, value in expected.items()),
            "manifest_count_delta": sum(abs(counts.get(key, -1) - int(value)) for key, value in (manifest.get("verification", {}).get("counts") or {}).items()),
            "duplicate_stable_keys": int(con.execute("SELECT count(*) FROM (SELECT stable_component_key FROM identities GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "duplicate_jpl_targets": int(con.execute("SELECT count(*) FROM (SELECT jpl_horizons_target FROM identities GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "identity_without_three_identifiers": int(con.execute("SELECT count(*) FROM (SELECT solar_identity_id FROM identifiers GROUP BY 1 HAVING count(*)<>3)").fetchone()[0]),
            "orphan_aliases": int(con.execute("SELECT count(*) FROM aliases a LEFT JOIN identities i USING(solar_identity_id) WHERE i.solar_identity_id IS NULL").fetchone()[0]),
            "orphan_identifiers": int(con.execute("SELECT count(*) FROM identifiers x LEFT JOIN identities i USING(solar_identity_id) WHERE i.solar_identity_id IS NULL").fetchone()[0]),
            "unresolved_relations": int(con.execute("SELECT count(*) FROM relation_outcomes WHERE relation_status NOT IN ('accepted','reference_origin')").fetchone()[0]),
            "relations_without_source_evidence": int(con.execute("SELECT count(*) FROM relation_outcomes r LEFT JOIN evidence.relation_claim_evidence e ON e.evidence_id=r.relation_evidence_id WHERE e.evidence_id IS NULL").fetchone()[0]),
            "identities_without_source_records": int(con.execute("SELECT count(*) FROM identities i LEFT JOIN evidence.source_records r USING(source_record_id) WHERE r.source_record_id IS NULL").fetchone()[0]),
            "core_stars_not_found": int(con.execute("SELECT count(*) FROM identities i LEFT JOIN core.stars s ON i.core_stable_object_key=s.stable_object_key WHERE i.core_object_type='star' AND s.star_id IS NULL").fetchone()[0]),
            "core_planets_not_found": int(con.execute("SELECT count(*) FROM identities i LEFT JOIN core.planets p ON i.core_stable_object_key=p.stable_object_key WHERE i.core_object_type='planet' AND p.planet_id IS NULL").fetchone()[0]),
            "core_name_mismatches": int(con.execute("SELECT count(*) FROM identities i LEFT JOIN core.stars s ON i.core_stable_object_key=s.stable_object_key LEFT JOIN core.planets p ON i.core_stable_object_key=p.stable_object_key WHERE i.core_object_id IS NOT NULL AND lower(trim(i.display_name))<>lower(trim(coalesce(s.star_name,p.planet_name)))").fetchone()[0]),
            "name_only_bindings": int(con.execute("SELECT count(*) FROM identities WHERE identity_method LIKE '%name_only%'").fetchone()[0]),
        }
        identity_columns = {
            str(row[0]) for row in con.execute("DESCRIBE identities").fetchall()
        }
        checks["no_prohibited_scientific_columns"] = not (
            identity_columns & PROHIBITED_IDENTITY_COLUMNS
        )
        checks.update({key: value == 0 for key, value in scalar_checks.items()})
        metrics = {"counts": counts, "scalar_checks": scalar_checks}
    finally:
        con.close()
    failures = sorted(key for key, passed in checks.items() if not passed)
    return {
        "schema_version": "spacegate.e7_solar_identity_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks,
        "failing_checks": failures,
        "metrics": metrics,
        "wall_seconds": round(time.monotonic() - started, 6),
    }


def reproduce(
    policy_path: Path, state: Path, manifest_path: Path, scratch_parent: Path
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    reference = compiler.load_object(manifest_path)
    scratch = Path(tempfile.mkdtemp(prefix="e7-solar-identity-reproduction-", dir=scratch_parent))
    try:
        rebuilt = compiler.compile_identity(
            policy_path, state, scratch / "artifacts", link_into_state=False
        )
        rebuilt_manifest = scratch / "artifacts" / rebuilt["build_id"] / "manifest.json"
        independent = audit(policy_path, state, rebuilt_manifest)
        checks = {
            "build_id_match": rebuilt.get("build_id") == reference.get("build_id"),
            "input_identity_match": rebuilt.get("inputs") == reference.get("inputs"),
            "products_match": rebuilt.get("products") == reference.get("products"),
            "verification_match": rebuilt.get("verification") == reference.get("verification"),
            "independent_verification_pass": independent.get("status") == "pass",
        }
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    checks["scratch_removed"] = not scratch.exists()
    failures = sorted(key for key, passed in checks.items() if not passed)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "schema_version": "spacegate.e7_solar_identity_reproduction.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": reference.get("build_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks,
        "failing_checks": failures,
        "rebuild_performance": rebuilt.get("performance"),
        "total_timing": {
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
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
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
