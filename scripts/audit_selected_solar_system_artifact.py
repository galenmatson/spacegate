#!/usr/bin/env python3
"""Independently audit a selected natural Solar System evidence artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_solar_system_policies.json"
DEFAULT_STATE = Path("/data/spacegate/state")


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(canonical_json(value) + b"\n")
    os.replace(temporary, path)


def sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def audit(*, artifact: Path, policy_path: Path) -> dict[str, Any]:
    manifest_path = artifact / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"missing manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    expected_files = {
        "parquet/solar_target_bindings.parquet",
        "parquet/solar_relation_bindings.parquet",
        "parquet/solar_orbital_solution_projection.parquet",
        "parquet/solar_physical_parameter_projection.parquet",
    }
    checks: dict[str, int] = {
        "bad_manifest_schema": int(manifest.get("schema_version") != "spacegate.e5_selected_solar_system.v1"),
        "build_id_path_mismatch": int(manifest.get("build_id") != artifact.name),
        "policy_version_mismatch": int(manifest.get("policy_version") != policy.get("policy_version")),
        "policy_sha256_mismatch": int(
            manifest.get("policy_sha256") != hashlib.sha256(canonical_json(policy)).hexdigest()
        ),
        "canonical_reference_mismatch": int(
            manifest.get("canonical_reference_build_id") != policy.get("canonical_reference_build_id")
        ),
        "evidence_build_mismatch": int(
            manifest.get("evidence_build_id") != policy.get("source", {}).get("evidence_build_id")
        ),
    }
    deterministic = manifest.get("deterministic_files") or {}
    checks["missing_deterministic_files"] = len(expected_files - set(deterministic))
    hash_mismatches = 0
    for relative in expected_files:
        path = artifact / relative
        entry = deterministic.get(relative) or {}
        if not path.is_file() or entry.get("sha256") != sha256_file(path):
            hash_mismatches += 1
    checks["deterministic_file_hash_mismatches"] = hash_mismatches

    if checks["missing_deterministic_files"] == 0 and hash_mismatches == 0:
        con = duckdb.connect()
        table_paths = {name.removesuffix(".parquet"): artifact / "parquet" / name for name in (
            "solar_target_bindings.parquet",
            "solar_relation_bindings.parquet",
            "solar_orbital_solution_projection.parquet",
            "solar_physical_parameter_projection.parquet",
        )}
        for table, path in table_paths.items():
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet({sql_literal(path)})")
        queries = {
            "duplicate_target_binding_ids": "SELECT count(*)-count(DISTINCT binding_id) FROM solar_target_bindings",
            "duplicate_source_target_bindings": "SELECT count(*) FROM (SELECT source_record_id FROM solar_target_bindings GROUP BY 1 HAVING count(*)<>1)",
            "accepted_targets_without_one_candidate": "SELECT count(*) FROM solar_target_bindings WHERE binding_status='accepted' AND canonical_candidate_count<>1",
            "accepted_targets_without_components": "SELECT count(*) FROM solar_target_bindings WHERE binding_status='accepted' AND canonical_component_key IS NULL",
            "unaccepted_targets_with_components": "SELECT count(*) FROM solar_target_bindings WHERE binding_status<>'accepted' AND canonical_component_key IS NOT NULL",
            "duplicate_relation_binding_ids": "SELECT count(*)-count(DISTINCT binding_id) FROM solar_relation_bindings",
            "accepted_relations_without_two_components": "SELECT count(*) FROM solar_relation_bindings WHERE binding_status='accepted' AND (target_component_key IS NULL OR center_component_key IS NULL)",
            "reference_relations_without_declared_origins": "SELECT count(*) FROM solar_relation_bindings WHERE binding_status='reference_origin' AND (target_component_key IS NULL OR external_reference_origin IS NULL OR center_component_key IS NOT NULL)",
            "eligible_orbits_without_two_components": "SELECT count(*) FROM solar_orbital_solution_projection WHERE projection_status='eligible_for_epoch_frame_orbit_selection' AND (target_component_key IS NULL OR center_component_key IS NULL)",
            "reference_orbits_without_origins": "SELECT count(*) FROM solar_orbital_solution_projection WHERE projection_status='reference_origin_context' AND external_reference_origin IS NULL",
            "eligible_orbits_with_invalid_contract": "SELECT count(*) FROM solar_orbital_solution_projection WHERE projection_status='eligible_for_epoch_frame_orbit_selection' AND NOT solution_contract_valid",
            "eligible_orbits_without_complete_elements": "SELECT count(*) FROM solar_orbital_solution_projection WHERE projection_status='eligible_for_epoch_frame_orbit_selection' AND (orbital_period_days IS NULL OR semi_major_axis_au IS NULL OR eccentricity IS NULL OR periapsis_distance_au IS NULL OR inclination_deg IS NULL OR longitude_ascending_node_deg IS NULL OR argument_periapsis_deg IS NULL OR time_periapsis_tdb_jd IS NULL OR mean_motion_deg_day IS NULL OR mean_anomaly_deg IS NULL OR true_anomaly_deg IS NULL OR apoapsis_distance_au IS NULL)",
            "eligible_physical_sets_without_targets": "SELECT count(*) FROM solar_physical_parameter_projection WHERE projection_status='eligible_for_physical_quantity_selection' AND target_component_key IS NULL",
            "canonical_relation_promotions": "SELECT (SELECT count(*) FROM solar_relation_bindings WHERE canonical_relation_promotion)+(SELECT count(*) FROM solar_orbital_solution_projection WHERE canonical_relation_promotion)",
        }
        checks.update({name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in queries.items()})
        scalar = lambda sql: int(con.execute(sql).fetchone()[0] or 0)
        observed = {
            "target_bindings": scalar("SELECT count(*) FROM solar_target_bindings"),
            "targets_accepted": scalar("SELECT count(*) FROM solar_target_bindings WHERE binding_status='accepted'"),
            "targets_missing": scalar("SELECT count(*) FROM solar_target_bindings WHERE binding_status='missing'"),
            "targets_ambiguous": scalar("SELECT count(*) FROM solar_target_bindings WHERE binding_status='ambiguous'"),
            "relation_bindings": scalar("SELECT count(*) FROM solar_relation_bindings"),
            "relations_accepted": scalar("SELECT count(*) FROM solar_relation_bindings WHERE binding_status='accepted'"),
            "relations_reference_origin": scalar("SELECT count(*) FROM solar_relation_bindings WHERE binding_status='reference_origin'"),
            "relations_missing": scalar("SELECT count(*) FROM solar_relation_bindings WHERE binding_status='missing'"),
            "relations_ambiguous": scalar("SELECT count(*) FROM solar_relation_bindings WHERE binding_status='ambiguous'"),
            "orbital_solutions": scalar("SELECT count(*) FROM solar_orbital_solution_projection"),
            "orbital_solutions_complete_elements": scalar("SELECT count(*) FROM solar_orbital_solution_projection WHERE orbital_period_days IS NOT NULL AND semi_major_axis_au IS NOT NULL AND eccentricity IS NOT NULL AND periapsis_distance_au IS NOT NULL AND inclination_deg IS NOT NULL AND longitude_ascending_node_deg IS NOT NULL AND argument_periapsis_deg IS NOT NULL AND time_periapsis_tdb_jd IS NOT NULL AND mean_motion_deg_day IS NOT NULL AND mean_anomaly_deg IS NOT NULL AND true_anomaly_deg IS NOT NULL AND apoapsis_distance_au IS NOT NULL"),
            "orbital_solutions_eligible": scalar("SELECT count(*) FROM solar_orbital_solution_projection WHERE projection_status='eligible_for_epoch_frame_orbit_selection'"),
            "orbital_solutions_reference_context": scalar("SELECT count(*) FROM solar_orbital_solution_projection WHERE projection_status='reference_origin_context'"),
            "physical_parameter_sets": scalar("SELECT count(*) FROM solar_physical_parameter_projection"),
            "physical_parameter_sets_eligible": scalar("SELECT count(*) FROM solar_physical_parameter_projection WHERE projection_status='eligible_for_physical_quantity_selection'"),
            "radius_values": scalar("SELECT count(*) FROM solar_physical_parameter_projection WHERE radius_km IS NOT NULL"),
            "mass_values": scalar("SELECT count(*) FROM solar_physical_parameter_projection WHERE mass_kg IS NOT NULL"),
            "canonical_relation_promotions": checks["canonical_relation_promotions"],
        }
        con.close()
    else:
        observed = {}
    expected = {key: int(value) for key, value in policy.get("source", {}).get("acceptance", {}).items()}
    checks["acceptance_mismatches"] = sum(
        int(observed.get(key) != value) for key, value in expected.items()
    )
    checks["manifest_observed_mismatch"] = int(manifest.get("observed") != observed)
    failing = {name: count for name, count in checks.items() if count}
    return {
        "schema_version": "spacegate.e5_selected_solar_system_artifact_audit.v1",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "artifact_path": str(artifact),
        "build_id": manifest.get("build_id"),
        "observed": observed,
        "expected": expected,
        "checks": checks,
        "failing_checks": failing,
        "status": "fail" if failing else "pass",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = audit(artifact=args.artifact, policy_path=args.policy)
    report_path = args.report or DEFAULT_STATE / "reports/evidence_lake_v2/e5_selected_solar_system_artifact_audit.json"
    write_json(report_path, report)
    print(f"Selected Solar System artifact {report['status']}: build={report['build_id']}")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
