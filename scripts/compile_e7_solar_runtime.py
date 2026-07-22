#!/usr/bin/env python3
"""Select clean Solar runtime evidence through permanent Solar identity."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_solar_runtime.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT = Path("/mnt/space/spacegate/e7-solar-runtime")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_object_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_solar_runtime_policy.v1":
        raise ValueError("unsupported Solar runtime policy")
    expected = {
        "open_stability_databases": False,
        "source_relations_create_canonical_containment": False,
        "reference_origin_solutions_renderable": False,
        "complete_epoch_frame_contract_required": True,
        "artificial_and_natural_identity_scopes_remain_distinct": True,
    }
    if policy.get("rules") != expected:
        raise ValueError("unsafe Solar runtime rules")
    if set(policy.get("inputs") or {}) != {"scientific_evidence", "solar_identity"}:
        raise ValueError("Solar runtime inputs are incomplete")
    contract = policy.get("solution_contract") or {}
    required = {
        "method", "model", "frame", "normalization_version",
        "physical_parameter_set_kind", "physical_normalization_version",
    }
    if set(contract) != required or any(not contract[key] for key in required):
        raise ValueError("Solar solution contract is incomplete")
    for name, spec in policy["inputs"].items():
        relative = Path(str(spec.get("relative_path") or ""))
        if (
            not spec.get("build_id")
            or len(str(spec.get("manifest_sha256") or "")) != 64
            or relative.is_absolute()
            or ".." in relative.parts
        ):
            raise ValueError(f"invalid bounded Solar runtime input: {name}")


def resolve_input(state: Path, spec: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    root = (state / spec["relative_path"]).resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file() or file_sha256(manifest_path) != spec["manifest_sha256"]:
        raise ValueError(f"Solar runtime input manifest mismatch: {root}")
    manifest = load_object(manifest_path)
    status = manifest.get("status") or (manifest.get("report") or {}).get("status")
    if manifest.get("build_id") != spec["build_id"] or status != "pass":
        raise ValueError(f"unaccepted Solar runtime input: {root}")
    return root, manifest


def resolve_product(root: Path, manifest: dict[str, Any], relative: str) -> Path:
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    if relative == manifest.get("database"):
        expected = manifest.get("database_sha256")
    else:
        expected = ((manifest.get("products") or {}).get(relative) or {}).get("sha256")
    if len(str(expected or "")) != 64 or file_sha256(path) != expected:
        raise ValueError(f"Solar runtime input product mismatch: {path}")
    return path


def materialize(con: duckdb.DuckDBPyConnection, policy: dict[str, Any], build_id: str) -> None:
    contract = policy["solution_contract"]
    policy_version = sql_literal(policy["policy_version"])
    build = sql_literal(build_id)
    con.execute(
        f"""
        CREATE TABLE selected_solar_target_bindings AS
        SELECT sha256(concat_ws('|','selected-solar-target',i.solar_identity_id,{policy_version})) binding_id,
          i.solar_identity_id,i.stable_component_key,i.system_stable_object_key,
          i.identity_kind,i.display_name,i.object_class,i.object_kind,
          i.jpl_horizons_target,i.core_object_type,i.core_object_id,
          i.source_record_id,i.source_id,i.release_id,i.source_table,
          'accepted'::VARCHAR binding_status,'permanent_solar_identity'::VARCHAR binding_method,
          {policy_version} policy_version,{build} build_id
        FROM identity.identities i ORDER BY i.stable_component_key;

        CREATE TABLE selected_solar_relation_bindings AS
        SELECT sha256(concat_ws('|','selected-solar-relation',r.relation_identity_id,{policy_version})) binding_id,
          r.relation_identity_id,r.relation_evidence_id,r.source_record_id,
          r.relation_kind,r.relation_scope,r.target_command,r.center_command,
          r.target_component_key,r.center_component_key,r.external_reference_origin,
          r.relation_status AS binding_status,
          CASE WHEN r.relation_status='accepted' THEN 'permanent_target_and_center_identity'
               ELSE 'declared_external_reference_origin' END binding_method,
          false AS canonical_containment,r.method,r.reference_raw,r.epoch_raw,
          r.quality_json,r.source_id,r.release_id,{policy_version} policy_version,
          {build} build_id
        FROM identity.relations r ORDER BY r.relation_evidence_id;

        CREATE TABLE selected_solar_orbital_solutions AS
        SELECT sha256(concat_ws('|','selected-solar-orbit',o.evidence_id,{policy_version})) orbital_solution_id,
          o.evidence_id,o.source_record_id,o.relation_claim_id,r.binding_id relation_binding_id,
          t.solar_identity_id,t.identity_kind,t.stable_component_key target_component_key,
          r.center_component_key,r.external_reference_origin,r.relation_kind,
          try_cast(o.epoch_raw AS DOUBLE) epoch_tdb_jd,o.frame_raw,o.method,o.model,
          o.reference_raw,o.quality_json,o.normalization_version,o.solution_key,
          try_cast(json_extract_string(o.parameter_set_raw,'$.orbital_period_days') AS DOUBLE) orbital_period_days,
          try_cast(json_extract_string(o.parameter_set_raw,'$.semi_major_axis_au') AS DOUBLE) semi_major_axis_au,
          try_cast(json_extract_string(o.parameter_set_raw,'$.eccentricity') AS DOUBLE) eccentricity,
          try_cast(json_extract_string(o.parameter_set_raw,'$.periapsis_distance_au') AS DOUBLE) periapsis_distance_au,
          try_cast(json_extract_string(o.parameter_set_raw,'$.inclination_deg') AS DOUBLE) inclination_deg,
          try_cast(json_extract_string(o.parameter_set_raw,'$.longitude_ascending_node_deg') AS DOUBLE) longitude_ascending_node_deg,
          try_cast(json_extract_string(o.parameter_set_raw,'$.argument_periapsis_deg') AS DOUBLE) argument_periapsis_deg,
          try_cast(json_extract_string(o.parameter_set_raw,'$.time_periapsis_tdb_jd') AS DOUBLE) time_periapsis_tdb_jd,
          try_cast(json_extract_string(o.parameter_set_raw,'$.mean_motion_deg_day') AS DOUBLE) mean_motion_deg_day,
          try_cast(json_extract_string(o.parameter_set_raw,'$.mean_anomaly_deg') AS DOUBLE) mean_anomaly_deg,
          try_cast(json_extract_string(o.parameter_set_raw,'$.true_anomaly_deg') AS DOUBLE) true_anomaly_deg,
          try_cast(json_extract_string(o.parameter_set_raw,'$.apoapsis_distance_au') AS DOUBLE) apoapsis_distance_au,
          o.epoch_raw IS NOT NULL
            AND o.frame_raw={sql_literal(contract['frame'])}
            AND o.method={sql_literal(contract['method'])}
            AND o.model={sql_literal(contract['model'])}
            AND o.normalization_version={sql_literal(contract['normalization_version'])}
            AS solution_contract_valid,
          r.binding_status='accepted' AS runtime_eligible,
          r.binding_status='accepted'
            AND try_cast(json_extract_string(o.parameter_set_raw,'$.orbital_period_days') AS DOUBLE) IS NOT NULL
            AS periodic_renderable,
          CASE
            WHEN r.binding_status='reference_origin' THEN 'reference_context'
            WHEN try_cast(json_extract_string(o.parameter_set_raw,'$.eccentricity') AS DOUBLE)>=1
              OR try_cast(json_extract_string(o.parameter_set_raw,'$.semi_major_axis_au') AS DOUBLE)<0
              THEN 'hyperbolic_trajectory'
            ELSE 'periodic_orbit' END AS render_mode,
          CASE WHEN r.binding_status='accepted' THEN 'selected_renderable'
               WHEN r.binding_status='reference_origin' THEN 'reference_origin_context'
               ELSE 'unresolved_relation' END selection_status,
          false AS canonical_containment,o.parameter_set_raw,
          {policy_version} policy_version,{build} build_id
        FROM evidence.orbital_solution_evidence o
        JOIN selected_solar_relation_bindings r
          ON r.relation_evidence_id=o.relation_claim_id
        JOIN selected_solar_target_bindings t
          ON t.source_record_id=o.source_record_id
        ORDER BY o.evidence_id;

        CREATE TABLE selected_solar_physical_parameters AS
        SELECT sha256(concat_ws('|','selected-solar-physical',p.evidence_id,{policy_version})) selected_parameter_set_id,
          p.evidence_id,p.parameter_schema_id,p.source_record_id,t.solar_identity_id,
          t.stable_component_key target_component_key,t.identity_kind,t.object_class,
          p.component_scope,p.parameter_set_kind,p.values_json,
          try_cast(json_extract_string(p.values_json,'$[0]') AS DOUBLE) radius_km,
          try_cast(json_extract_string(p.values_json,'$[1]') AS DOUBLE) mass_kg,
          p.epoch_raw,p.method,p.model,p.reference_raw,p.quality_json,
          p.normalization_version,'selected'::VARCHAR selection_status,
          {policy_version} policy_version,{build} build_id
        FROM evidence.solar_system_object_parameter_sets p
        JOIN selected_solar_target_bindings t USING(source_record_id)
        WHERE p.parameter_set_kind={sql_literal(contract['physical_parameter_set_kind'])}
          AND p.normalization_version={sql_literal(contract['physical_normalization_version'])}
        ORDER BY p.evidence_id;
        """
    )


def verify(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> dict[str, Any]:
    scalar = lambda sql: int(con.execute(sql).fetchone()[0] or 0)
    counts = {
        "target_bindings": scalar("SELECT count(*) FROM selected_solar_target_bindings"),
        "natural_targets": scalar("SELECT count(*) FROM selected_solar_target_bindings WHERE identity_kind='natural'"),
        "artificial_targets": scalar("SELECT count(*) FROM selected_solar_target_bindings WHERE identity_kind='artificial'"),
        "relation_bindings": scalar("SELECT count(*) FROM selected_solar_relation_bindings"),
        "relations_accepted": scalar("SELECT count(*) FROM selected_solar_relation_bindings WHERE binding_status='accepted'"),
        "relations_reference_origin": scalar("SELECT count(*) FROM selected_solar_relation_bindings WHERE binding_status='reference_origin'"),
        "orbital_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions"),
        "runtime_eligible_orbital_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE runtime_eligible"),
        "periodic_renderable_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE periodic_renderable"),
        "hyperbolic_trajectory_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE render_mode='hyperbolic_trajectory'"),
        "reference_context_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE selection_status='reference_origin_context'"),
        "contract_valid_orbital_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE solution_contract_valid"),
        "complete_periodic_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE solution_contract_valid AND orbital_period_days IS NOT NULL AND semi_major_axis_au IS NOT NULL AND eccentricity IS NOT NULL AND periapsis_distance_au IS NOT NULL AND inclination_deg IS NOT NULL AND longitude_ascending_node_deg IS NOT NULL AND argument_periapsis_deg IS NOT NULL AND time_periapsis_tdb_jd IS NOT NULL AND mean_motion_deg_day IS NOT NULL AND mean_anomaly_deg IS NOT NULL AND true_anomaly_deg IS NOT NULL AND apoapsis_distance_au IS NOT NULL"),
        "complete_hyperbolic_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE solution_contract_valid AND render_mode='hyperbolic_trajectory' AND orbital_period_days IS NULL AND semi_major_axis_au<0 AND eccentricity>=1 AND periapsis_distance_au IS NOT NULL AND inclination_deg IS NOT NULL AND longitude_ascending_node_deg IS NOT NULL AND argument_periapsis_deg IS NOT NULL AND time_periapsis_tdb_jd IS NOT NULL AND mean_motion_deg_day IS NOT NULL AND mean_anomaly_deg IS NOT NULL AND true_anomaly_deg IS NOT NULL AND apoapsis_distance_au IS NOT NULL"),
        "physical_parameter_sets": scalar("SELECT count(*) FROM selected_solar_physical_parameters"),
        "canonical_containment_promotions": scalar("SELECT (SELECT count(*) FROM selected_solar_relation_bindings WHERE canonical_containment)+(SELECT count(*) FROM selected_solar_orbital_solutions WHERE canonical_containment)"),
    }
    expected = {key: int(value) for key, value in policy["acceptance"].items()}
    checks = {
        "acceptance_count_delta": sum(abs(counts.get(key, -1) - value) for key, value in expected.items()),
        "duplicate_targets": scalar("SELECT count(*) FROM (SELECT solar_identity_id FROM selected_solar_target_bindings GROUP BY 1 HAVING count(*)<>1)"),
        "duplicate_relations": scalar("SELECT count(*) FROM (SELECT relation_evidence_id FROM selected_solar_relation_bindings GROUP BY 1 HAVING count(*)<>1)"),
        "duplicate_solutions": scalar("SELECT count(*) FROM (SELECT evidence_id FROM selected_solar_orbital_solutions GROUP BY 1 HAVING count(*)<>1)"),
        "unbound_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE target_component_key IS NULL OR relation_binding_id IS NULL"),
        "invalid_runtime_solutions": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE runtime_eligible AND NOT solution_contract_valid"),
        "renderable_reference_origins": scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE external_reference_origin IS NOT NULL AND periodic_renderable"),
        "artificial_physical_leakage": scalar("SELECT count(*) FROM selected_solar_physical_parameters WHERE identity_kind='artificial'"),
    }
    failing = {key: value for key, value in checks.items() if value}
    return {"status": "pass" if not failing else "fail", "counts": counts, "expected_counts": expected, "checks": checks, "failing_checks": failing}


def compile_runtime(
    policy_path: Path, state: Path, output_root: Path, *, link_into_state: bool
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    policy = load_object(policy_path)
    validate_policy(policy)
    evidence_root, evidence_manifest = resolve_input(state, policy["inputs"]["scientific_evidence"])
    identity_root, identity_manifest = resolve_input(state, policy["inputs"]["solar_identity"])
    evidence_db = resolve_product(evidence_root, evidence_manifest, "scientific_evidence.duckdb")
    identity_products = {}
    for name in (
        "solar_component_identities.parquet", "solar_relation_identity_outcomes.parquet"
    ):
        identity_products[name] = resolve_product(identity_root, identity_manifest, name)
    policy_sha = file_sha256(policy_path)
    compiler_sha = file_sha256(Path(__file__).resolve())
    input_identity = {
        name: {"build_id": spec["build_id"], "manifest_sha256": spec["manifest_sha256"]}
        for name, spec in policy["inputs"].items()
    }
    build_id = stable_hash({"policy_sha256": policy_sha, "compiler_sha256": compiler_sha, "inputs": input_identity})[:24]
    final = output_root / build_id
    if (final / "manifest.json").is_file():
        manifest = load_object(final / "manifest.json")
        if manifest.get("build_id") != build_id:
            raise ValueError("Solar runtime build collision")
        return manifest
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        con = duckdb.connect()
        products: dict[str, dict[str, Any]] = {}
        try:
            con.execute(f"ATTACH {sql_literal(evidence_db)} AS evidence (READ_ONLY)")
            con.execute("CREATE SCHEMA identity")
            con.execute(f"CREATE VIEW identity.identities AS SELECT * FROM read_parquet({sql_literal(identity_products['solar_component_identities.parquet'])})")
            con.execute(f"CREATE VIEW identity.relations AS SELECT * FROM read_parquet({sql_literal(identity_products['solar_relation_identity_outcomes.parquet'])})")
            materialize(con, policy, build_id)
            verification = verify(con, policy)
            if verification["status"] != "pass":
                raise ValueError(f"Solar runtime verification failed: {verification['failing_checks']}")
            for table in (
                "selected_solar_target_bindings", "selected_solar_relation_bindings",
                "selected_solar_orbital_solutions", "selected_solar_physical_parameters",
            ):
                path = staging / f"{table}.parquet"
                con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO {sql_literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)")
                products[path.name] = {"rows": int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]), "bytes": path.stat().st_size, "sha256": file_sha256(path), "determinism": "byte_exact"}
        finally:
            con.close()
        manifest = {
            "schema_version": "spacegate.e7_solar_runtime_manifest.v1", "build_id": build_id,
            "status": "pass", "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "policy_version": policy["policy_version"], "compiler_version": policy["compiler_version"],
            "policy_sha256": policy_sha, "compiler_sha256": compiler_sha, "inputs": input_identity,
            "stability_databases_opened": [], "verification": verification, "products": products,
            "performance": {"wall_seconds": round(time.monotonic()-started,6), "cpu_seconds": round(time.process_time()-cpu_started,6), "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)},
        }
        write_object_atomic(staging / "manifest.json", manifest)
        os.replace(staging, final)
        if link_into_state:
            links = state / "derived/evidence_lake_v2/solar_runtime"
            links.mkdir(parents=True, exist_ok=True)
            link = links / build_id
            if not link.exists() and not link.is_symlink():
                link.symlink_to(final)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_runtime(args.policy.resolve(), args.state_dir.resolve(), args.output_root.resolve(), link_into_state=not args.no_state_link)
    if args.report:
        write_object_atomic(args.report.resolve(), manifest)
    print(json.dumps({"build_id": manifest["build_id"], "status": manifest["status"], "counts": manifest["verification"]["counts"], "wall_seconds": manifest["performance"]["wall_seconds"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
