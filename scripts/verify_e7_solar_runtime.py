#!/usr/bin/env python3
"""Independently verify and reproduce clean E7 Solar runtime selection."""

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

import compile_e7_solar_runtime as compiler


DEFAULT_SCRATCH = Path("/mnt/space/spacegate")
PRODUCTS = {
    "selected_solar_target_bindings.parquet": "targets",
    "selected_solar_relation_bindings.parquet": "relations",
    "selected_solar_orbital_solutions.parquet": "orbits",
    "selected_solar_physical_parameters.parquet": "physical",
}


def audit(policy_path: Path, state: Path, manifest_path: Path) -> dict[str, Any]:
    started = time.monotonic()
    policy = compiler.load_object(policy_path)
    compiler.validate_policy(policy)
    manifest = compiler.load_object(manifest_path)
    checks: dict[str, bool] = {
        "manifest_status_pass": manifest.get("status") == "pass",
        "manifest_schema": manifest.get("schema_version")
        == "spacegate.e7_solar_runtime_manifest.v1",
        "stability_databases_not_opened": manifest.get("stability_databases_opened") == [],
        "policy_sha256_match": manifest.get("policy_sha256") == compiler.file_sha256(policy_path),
        "compiler_sha256_match": manifest.get("compiler_sha256")
        == compiler.file_sha256(Path(compiler.__file__).resolve()),
    }
    evidence_root, evidence_manifest = compiler.resolve_input(
        state, policy["inputs"]["scientific_evidence"]
    )
    evidence_db = compiler.resolve_product(
        evidence_root, evidence_manifest, "scientific_evidence.duckdb"
    )
    paths: dict[str, Path] = {}
    for filename, table in PRODUCTS.items():
        product = (manifest.get("products") or {}).get(filename) or {}
        path = manifest_path.parent / filename
        paths[table] = path
        prefix = filename.removesuffix(".parquet")
        checks[f"{prefix}_exists"] = path.is_file()
        checks[f"{prefix}_bytes_match"] = path.is_file() and path.stat().st_size == product.get("bytes")
        checks[f"{prefix}_sha256_match"] = path.is_file() and compiler.file_sha256(path) == product.get("sha256")
        checks[f"{prefix}_byte_exact"] = product.get("determinism") == "byte_exact"

    con = duckdb.connect()
    metrics: dict[str, Any] = {}
    try:
        for table, path in paths.items():
            con.execute(
                f"CREATE VIEW {table} AS SELECT * FROM read_parquet({compiler.sql_literal(path)})"
            )
        con.execute(f"ATTACH {compiler.sql_literal(evidence_db)} AS evidence (READ_ONLY)")
        counts = {
            "target_bindings": int(con.execute("SELECT count(*) FROM targets").fetchone()[0]),
            "natural_targets": int(con.execute("SELECT count(*) FROM targets WHERE identity_kind='natural'").fetchone()[0]),
            "artificial_targets": int(con.execute("SELECT count(*) FROM targets WHERE identity_kind='artificial'").fetchone()[0]),
            "relation_bindings": int(con.execute("SELECT count(*) FROM relations").fetchone()[0]),
            "relations_accepted": int(con.execute("SELECT count(*) FROM relations WHERE binding_status='accepted'").fetchone()[0]),
            "relations_reference_origin": int(con.execute("SELECT count(*) FROM relations WHERE binding_status='reference_origin'").fetchone()[0]),
            "orbital_solutions": int(con.execute("SELECT count(*) FROM orbits").fetchone()[0]),
            "runtime_eligible_orbital_solutions": int(con.execute("SELECT count(*) FROM orbits WHERE runtime_eligible").fetchone()[0]),
            "periodic_renderable_solutions": int(con.execute("SELECT count(*) FROM orbits WHERE periodic_renderable").fetchone()[0]),
            "hyperbolic_trajectory_solutions": int(con.execute("SELECT count(*) FROM orbits WHERE render_mode='hyperbolic_trajectory'").fetchone()[0]),
            "reference_context_solutions": int(con.execute("SELECT count(*) FROM orbits WHERE selection_status='reference_origin_context'").fetchone()[0]),
            "contract_valid_orbital_solutions": int(con.execute("SELECT count(*) FROM orbits WHERE solution_contract_valid").fetchone()[0]),
            "complete_periodic_solutions": int(con.execute("SELECT count(*) FROM orbits WHERE solution_contract_valid AND orbital_period_days IS NOT NULL AND semi_major_axis_au IS NOT NULL AND eccentricity IS NOT NULL AND periapsis_distance_au IS NOT NULL AND inclination_deg IS NOT NULL AND longitude_ascending_node_deg IS NOT NULL AND argument_periapsis_deg IS NOT NULL AND time_periapsis_tdb_jd IS NOT NULL AND mean_motion_deg_day IS NOT NULL AND mean_anomaly_deg IS NOT NULL AND true_anomaly_deg IS NOT NULL AND apoapsis_distance_au IS NOT NULL").fetchone()[0]),
            "complete_hyperbolic_solutions": int(con.execute("SELECT count(*) FROM orbits WHERE solution_contract_valid AND render_mode='hyperbolic_trajectory' AND orbital_period_days IS NULL AND semi_major_axis_au<0 AND eccentricity>=1 AND periapsis_distance_au IS NOT NULL AND inclination_deg IS NOT NULL AND longitude_ascending_node_deg IS NOT NULL AND argument_periapsis_deg IS NOT NULL AND time_periapsis_tdb_jd IS NOT NULL AND mean_motion_deg_day IS NOT NULL AND mean_anomaly_deg IS NOT NULL AND true_anomaly_deg IS NOT NULL AND apoapsis_distance_au IS NOT NULL").fetchone()[0]),
            "physical_parameter_sets": int(con.execute("SELECT count(*) FROM physical").fetchone()[0]),
            "canonical_containment_promotions": int(con.execute("SELECT (SELECT count(*) FROM relations WHERE canonical_containment)+(SELECT count(*) FROM orbits WHERE canonical_containment)").fetchone()[0]),
        }
        expected = {key: int(value) for key, value in policy["acceptance"].items()}
        scalars = {
            "acceptance_count_delta": sum(abs(counts.get(key, -1) - value) for key, value in expected.items()),
            "manifest_count_delta": sum(abs(counts.get(key, -1) - int(value)) for key, value in (manifest.get("verification", {}).get("counts") or {}).items()),
            "duplicate_targets": int(con.execute("SELECT count(*) FROM (SELECT solar_identity_id FROM targets GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "duplicate_relations": int(con.execute("SELECT count(*) FROM (SELECT relation_evidence_id FROM relations GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "duplicate_orbits": int(con.execute("SELECT count(*) FROM (SELECT evidence_id FROM orbits GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "orphan_orbit_relations": int(con.execute("SELECT count(*) FROM orbits o LEFT JOIN relations r ON r.binding_id=o.relation_binding_id WHERE r.binding_id IS NULL").fetchone()[0]),
            "orphan_orbit_targets": int(con.execute("SELECT count(*) FROM orbits o LEFT JOIN targets t USING(solar_identity_id) WHERE t.solar_identity_id IS NULL").fetchone()[0]),
            "orbit_evidence_missing": int(con.execute("SELECT count(*) FROM orbits o LEFT JOIN evidence.orbital_solution_evidence e USING(evidence_id) WHERE e.evidence_id IS NULL").fetchone()[0]),
            "physical_evidence_missing": int(con.execute("SELECT count(*) FROM physical p LEFT JOIN evidence.solar_system_object_parameter_sets e USING(evidence_id) WHERE e.evidence_id IS NULL").fetchone()[0]),
            "invalid_runtime_solutions": int(con.execute("SELECT count(*) FROM orbits WHERE runtime_eligible AND NOT solution_contract_valid").fetchone()[0]),
            "hyperbolic_marked_periodic": int(con.execute("SELECT count(*) FROM orbits WHERE render_mode='hyperbolic_trajectory' AND periodic_renderable").fetchone()[0]),
            "reference_context_marked_renderable": int(con.execute("SELECT count(*) FROM orbits WHERE selection_status='reference_origin_context' AND (runtime_eligible OR periodic_renderable)").fetchone()[0]),
            "artificial_physical_leakage": int(con.execute("SELECT count(*) FROM physical WHERE identity_kind='artificial'").fetchone()[0]),
        }
        checks.update({key: value == 0 for key, value in scalars.items()})
        metrics = {"counts": counts, "scalar_checks": scalars}
    finally:
        con.close()
    failures = sorted(key for key, passed in checks.items() if not passed)
    return {
        "schema_version": "spacegate.e7_solar_runtime_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"), "status": "pass" if not failures else "fail",
        "checks": checks, "failing_checks": failures, "metrics": metrics,
        "wall_seconds": round(time.monotonic()-started,6),
    }


def reproduce(policy_path: Path, state: Path, manifest_path: Path, scratch_parent: Path) -> dict[str, Any]:
    started=time.monotonic(); cpu_started=time.process_time()
    reference=compiler.load_object(manifest_path)
    scratch=Path(tempfile.mkdtemp(prefix="e7-solar-runtime-reproduction-",dir=scratch_parent))
    try:
        rebuilt=compiler.compile_runtime(policy_path,state,scratch/"artifacts",link_into_state=False)
        rebuilt_manifest=scratch/"artifacts"/rebuilt["build_id"]/"manifest.json"
        independent=audit(policy_path,state,rebuilt_manifest)
        checks={
            "build_id_match":rebuilt.get("build_id")==reference.get("build_id"),
            "inputs_match":rebuilt.get("inputs")==reference.get("inputs"),
            "products_match":rebuilt.get("products")==reference.get("products"),
            "verification_match":rebuilt.get("verification")==reference.get("verification"),
            "independent_verification_pass":independent.get("status")=="pass",
        }
    finally:
        shutil.rmtree(scratch,ignore_errors=True)
    checks["scratch_removed"]=not scratch.exists()
    failures=sorted(key for key,value in checks.items() if not value)
    usage=resource.getrusage(resource.RUSAGE_SELF)
    return {
        "schema_version":"spacegate.e7_solar_runtime_reproduction.v1",
        "generated_at":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id":reference.get("build_id"),"status":"pass" if not failures else "fail",
        "checks":checks,"failing_checks":failures,"rebuild_performance":rebuilt.get("performance"),
        "total_timing":{"wall_seconds":round(time.monotonic()-started,6),"cpu_seconds":round(time.process_time()-cpu_started,6),"peak_rss_kib":int(usage.ru_maxrss)},
    }


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy",type=Path,default=compiler.DEFAULT_POLICY)
    parser.add_argument("--state-dir",type=Path,default=compiler.DEFAULT_STATE)
    parser.add_argument("--manifest",type=Path,required=True)
    parser.add_argument("--reproduce",action="store_true")
    parser.add_argument("--scratch-parent",type=Path,default=DEFAULT_SCRATCH)
    parser.add_argument("--report",type=Path)
    args=parser.parse_args()
    report=reproduce(args.policy.resolve(),args.state_dir.resolve(),args.manifest.resolve(),args.scratch_parent.resolve()) if args.reproduce else audit(args.policy.resolve(),args.state_dir.resolve(),args.manifest.resolve())
    rendered=json.dumps(report,indent=2,sort_keys=True)+"\n"
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True); args.report.write_text(rendered,encoding="utf-8")
    print(rendered,end="")
    return 0 if report["status"]=="pass" else 1


if __name__=="__main__":
    raise SystemExit(main())
