#!/usr/bin/env python3
"""Compile epoch/frame-bound natural Solar System evidence from JPL Horizons."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_solar_system_policies.json"
DEFAULT_STATE = Path("/data/spacegate/state")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e5_solar_system_policies.v1":
        raise ValueError("unsupported Solar System policy schema")
    source = policy.get("source") or {}
    required = (
        "source_id", "release_id", "evidence_build_id", "object_source_table",
        "target_namespace", "target_claim_scope", "relation_kind",
        "component_source_catalog", "canonical_identifier_json_key",
        "required_solution_contract", "physical_parameter_set_kind",
        "physical_normalization_version", "acceptance",
    )
    missing = [key for key in required if key not in source]
    if missing:
        raise ValueError(f"Solar System policy missing fields: {missing}")
    if source["source_id"] != "solar_system.jpl_horizons_authority":
        raise ValueError("Solar System selected projection is natural Horizons authority only")
    if not source.get("external_reference_origins"):
        raise ValueError("Solar System policy must classify external reference origins")


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          build_id VARCHAR, policy_version VARCHAR, policy_sha256 VARCHAR,
          canonical_reference_build_id VARCHAR, generated_at VARCHAR, status VARCHAR
        );
        CREATE TABLE solar_target_bindings (
          binding_id VARCHAR, source_record_id VARCHAR, source_id VARCHAR,
          release_id VARCHAR, evidence_build_id VARCHAR, source_table VARCHAR,
          source_record_key VARCHAR, target_command VARCHAR,
          canonical_candidate_count BIGINT, canonical_component_entity_id BIGINT,
          canonical_component_key VARCHAR, canonical_component_type VARCHAR,
          canonical_core_object_type VARCHAR, canonical_core_object_id BIGINT,
          binding_status VARCHAR, binding_method VARCHAR, binding_reason VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE solar_relation_bindings (
          binding_id VARCHAR, relation_evidence_id VARCHAR, source_record_id VARCHAR,
          relation_kind VARCHAR, target_command VARCHAR, center_command VARCHAR,
          target_binding_status VARCHAR, target_component_key VARCHAR,
          center_candidate_count BIGINT, center_binding_status VARCHAR,
          center_component_key VARCHAR, external_reference_origin VARCHAR,
          binding_status VARCHAR, binding_reason VARCHAR,
          canonical_relation_promotion BOOLEAN, policy_version VARCHAR
        );
        CREATE TABLE solar_orbital_solution_projection (
          evidence_id VARCHAR, source_record_id VARCHAR, relation_claim_id VARCHAR,
          solution_key JSON, parameter_set_raw JSON, epoch_tdb_jd DOUBLE,
          frame_raw VARCHAR, method VARCHAR, model VARCHAR, reference_raw VARCHAR,
          quality_json JSON, normalization_version VARCHAR, relation_binding_id VARCHAR,
          target_component_key VARCHAR, center_component_key VARCHAR,
          external_reference_origin VARCHAR, orbital_period_days DOUBLE,
          semi_major_axis_au DOUBLE, eccentricity DOUBLE,
          periapsis_distance_au DOUBLE, inclination_deg DOUBLE,
          longitude_ascending_node_deg DOUBLE, argument_periapsis_deg DOUBLE,
          time_periapsis_tdb_jd DOUBLE, mean_motion_deg_day DOUBLE,
          mean_anomaly_deg DOUBLE, true_anomaly_deg DOUBLE,
          apoapsis_distance_au DOUBLE,
          solution_contract_valid BOOLEAN, projection_status VARCHAR,
          projection_reason VARCHAR, canonical_relation_promotion BOOLEAN,
          policy_version VARCHAR
        );
        CREATE TABLE solar_physical_parameter_projection (
          evidence_id VARCHAR, parameter_schema_id VARCHAR, source_record_id VARCHAR,
          target_binding_id VARCHAR, target_component_key VARCHAR,
          component_scope VARCHAR, parameter_set_kind VARCHAR, values_json JSON,
          radius_km DOUBLE, mass_kg DOUBLE, epoch_raw VARCHAR, method VARCHAR,
          model VARCHAR, reference_raw VARCHAR, quality_json JSON,
          normalization_version VARCHAR, projection_status VARCHAR,
          projection_reason VARCHAR, policy_version VARCHAR
        );
        """
    )


def materialize(
    con: duckdb.DuckDBPyConnection,
    *,
    policy: dict[str, Any],
) -> dict[str, int]:
    source = policy["source"]
    contract = source["required_solution_contract"]
    policy_sql = sql_literal(policy["policy_version"])
    source_id = sql_literal(source["source_id"])
    release_id = sql_literal(source["release_id"])
    evidence_build = sql_literal(source["evidence_build_id"])
    object_table = sql_literal(source["object_source_table"])
    target_namespace = sql_literal(source["target_namespace"])
    target_scope = sql_literal(source["target_claim_scope"])
    component_catalog = sql_literal(source["component_source_catalog"])
    identifier_key = sql_literal(source["canonical_identifier_json_key"])
    relation_kind = sql_literal(source["relation_kind"])

    con.execute(
        f"""
        CREATE TEMP TABLE source_targets AS
        SELECT r.source_record_id,r.source_table,
               json_extract_string(r.logical_key_json,'$.source_pk') source_record_key,
               i.identifier_normalized target_command
        FROM evidence.source_records r
        JOIN evidence.identifier_claim_evidence i USING(source_record_id)
        WHERE r.source_id={source_id} AND r.release_id={release_id}
          AND r.source_table={object_table} AND i.namespace={target_namespace}
          AND i.claim_scope={target_scope};

        CREATE TEMP TABLE target_candidates AS
        SELECT t.source_record_id,c.component_entity_id,c.stable_component_key,
               c.component_type,c.core_object_type,c.core_object_id,
               'exact_sol_authority_source_key' binding_method
        FROM source_targets t
        JOIN arm.component_entities c
          ON c.source_catalog={component_catalog}
         AND c.source_pk=t.source_record_key
        UNION ALL
        SELECT t.source_record_id,c.component_entity_id,c.stable_component_key,
               c.component_type,c.core_object_type,c.core_object_id,
               'exact_canonical_jpl_command' binding_method
        FROM source_targets t
        JOIN core.stars s
          ON json_extract_string(s.catalog_ids_json,'$.' || {identifier_key})=t.target_command
        JOIN arm.component_entities c
          ON c.core_object_type='star' AND c.core_object_id=s.star_id;

        INSERT INTO solar_target_bindings
        WITH deduplicated AS (
          SELECT DISTINCT source_record_id,component_entity_id,stable_component_key,
                 component_type,core_object_type,core_object_id,binding_method
          FROM target_candidates
        ), candidates AS (
          SELECT source_record_id,count(DISTINCT component_entity_id) candidate_count,
                 min(component_entity_id) component_entity_id,
                 min(stable_component_key) component_key,
                 min(component_type) component_type,
                 min(core_object_type) core_object_type,
                 min(core_object_id) core_object_id,
                 string_agg(DISTINCT binding_method,',' ORDER BY binding_method) binding_methods
          FROM deduplicated GROUP BY source_record_id
        )
        SELECT sha256(concat_ws('|',{source_id},t.source_record_id,'solar-target',{policy_sql})),
               t.source_record_id,{source_id},{release_id},{evidence_build},t.source_table,
               t.source_record_key,t.target_command,coalesce(c.candidate_count,0),
               CASE WHEN c.candidate_count=1 THEN c.component_entity_id END,
               CASE WHEN c.candidate_count=1 THEN c.component_key END,
               CASE WHEN c.candidate_count=1 THEN c.component_type END,
               CASE WHEN c.candidate_count=1 THEN c.core_object_type END,
               CASE WHEN c.candidate_count=1 THEN c.core_object_id END,
               CASE WHEN c.candidate_count=1 THEN 'accepted'
                    WHEN c.candidate_count>1 THEN 'ambiguous' ELSE 'missing' END,
               c.binding_methods,
               CASE WHEN c.candidate_count=1
                      THEN 'one canonical component from exact authoritative identifier evidence'
                    WHEN c.candidate_count>1
                      THEN 'authoritative identifier strategies resolve to multiple canonical components'
                    ELSE 'no canonical component carries the exact reviewed source key or JPL command' END,
               {policy_sql}
        FROM source_targets t LEFT JOIN candidates c USING(source_record_id);
        """
    )

    con.execute(
        "CREATE TEMP TABLE external_origins(command VARCHAR,origin_kind VARCHAR)"
    )
    con.executemany(
        "INSERT INTO external_origins VALUES (?,?)",
        sorted(source["external_reference_origins"].items()),
    )
    con.execute(
        f"""
        INSERT INTO solar_relation_bindings
        WITH relation_source AS (
          SELECT r.evidence_id relation_evidence_id,r.source_record_id,r.relation_kind,
                 r.left_identity_raw target_command,r.right_identity_raw center_command
          FROM evidence.relation_claim_evidence r
          JOIN evidence.source_records s USING(source_record_id)
          WHERE s.source_id={source_id} AND s.release_id={release_id}
            AND r.relation_kind={relation_kind}
        ), center_candidates AS (
          SELECT rs.relation_evidence_id,count(*) FILTER (WHERE b.binding_status='accepted') center_count,
                 min(b.canonical_component_key) FILTER (WHERE b.binding_status='accepted') center_key
          FROM relation_source rs
          LEFT JOIN solar_target_bindings b ON b.target_command=rs.center_command
          GROUP BY rs.relation_evidence_id
        )
        SELECT sha256(concat_ws('|',{source_id},rs.relation_evidence_id,'solar-relation',{policy_sql})),
               rs.relation_evidence_id,rs.source_record_id,rs.relation_kind,
               rs.target_command,rs.center_command,t.binding_status,t.canonical_component_key,
               coalesce(c.center_count,0),
               CASE WHEN c.center_count=1 THEN 'accepted'
                    WHEN o.command IS NOT NULL THEN 'reference_origin'
                    WHEN c.center_count>1 THEN 'ambiguous' ELSE 'missing' END,
               CASE WHEN c.center_count=1 THEN c.center_key END,o.origin_kind,
               CASE WHEN t.binding_status='accepted' AND c.center_count=1 THEN 'accepted'
                    WHEN t.binding_status='accepted' AND o.command IS NOT NULL THEN 'reference_origin'
                    WHEN t.binding_status='ambiguous' OR c.center_count>1 THEN 'ambiguous'
                    ELSE 'missing' END,
               CASE WHEN t.binding_status='accepted' AND c.center_count=1
                      THEN 'target and physical center independently resolve through exact JPL commands'
                    WHEN t.binding_status='accepted' AND o.command IS NOT NULL
                      THEN 'target resolves; center is a declared non-object reference origin'
                    WHEN t.binding_status<>'accepted'
                      THEN 'orbit target does not resolve to one canonical component'
                    WHEN c.center_count>1
                      THEN 'orbit center command resolves to multiple canonical components'
                    ELSE 'orbit center command has no canonical component or declared reference origin' END,
               false,{policy_sql}
        FROM relation_source rs
        JOIN solar_target_bindings t ON t.source_record_id=rs.source_record_id
        LEFT JOIN center_candidates c USING(relation_evidence_id)
        LEFT JOIN external_origins o ON o.command=rs.center_command;
        """
    )

    con.execute(
        f"""
        INSERT INTO solar_orbital_solution_projection
        SELECT o.evidence_id,o.source_record_id,o.relation_claim_id,o.solution_key,
               o.parameter_set_raw,try_cast(o.epoch_raw AS DOUBLE),o.frame_raw,o.method,
               o.model,o.reference_raw,o.quality_json,o.normalization_version,b.binding_id,
               b.target_component_key,b.center_component_key,b.external_reference_origin,
               try_cast(json_extract_string(o.parameter_set_raw,'$.orbital_period_days') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.semi_major_axis_au') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.eccentricity') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.periapsis_distance_au') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.inclination_deg') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.longitude_ascending_node_deg') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.argument_periapsis_deg') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.time_periapsis_tdb_jd') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.mean_motion_deg_day') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.mean_anomaly_deg') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.true_anomaly_deg') AS DOUBLE),
               try_cast(json_extract_string(o.parameter_set_raw,'$.apoapsis_distance_au') AS DOUBLE),
               o.epoch_raw IS NOT NULL
                 AND o.frame_raw={sql_literal(contract['frame_raw'])}
                 AND o.method={sql_literal(contract['method'])}
                 AND o.model={sql_literal(contract['model'])}
                 AND o.normalization_version={sql_literal(contract['normalization_version'])},
               CASE WHEN b.binding_status='accepted'
                          AND o.epoch_raw IS NOT NULL
                          AND o.frame_raw={sql_literal(contract['frame_raw'])}
                          AND o.method={sql_literal(contract['method'])}
                          AND o.model={sql_literal(contract['model'])}
                          AND o.normalization_version={sql_literal(contract['normalization_version'])}
                      THEN 'eligible_for_epoch_frame_orbit_selection'
                    WHEN b.binding_status='reference_origin'
                          AND o.epoch_raw IS NOT NULL
                          AND o.frame_raw={sql_literal(contract['frame_raw'])}
                          AND o.method={sql_literal(contract['method'])}
                          AND o.model={sql_literal(contract['model'])}
                          AND o.normalization_version={sql_literal(contract['normalization_version'])}
                      THEN 'reference_origin_context'
                    WHEN o.epoch_raw IS NULL OR o.frame_raw<>{sql_literal(contract['frame_raw'])}
                          OR o.method<>{sql_literal(contract['method'])}
                          OR o.model<>{sql_literal(contract['model'])}
                          OR o.normalization_version<>{sql_literal(contract['normalization_version'])}
                      THEN 'invalid_solution_contract'
                    ELSE 'unresolved_relation' END,
               CASE WHEN b.binding_status='accepted'
                      THEN 'coherent osculating elements retain exact target, center, epoch, frame, method, model, and response lineage'
                    WHEN b.binding_status='reference_origin'
                      THEN 'coherent solution retained against a declared non-object reference origin'
                    ELSE b.binding_reason END,
               false,{policy_sql}
        FROM evidence.orbital_solution_evidence o
        JOIN evidence.source_records s USING(source_record_id)
        JOIN solar_relation_bindings b ON b.relation_evidence_id=o.relation_claim_id
        WHERE s.source_id={source_id} AND s.release_id={release_id};

        INSERT INTO solar_physical_parameter_projection
        SELECT p.evidence_id,p.parameter_schema_id,p.source_record_id,b.binding_id,
               b.canonical_component_key,p.component_scope,p.parameter_set_kind,p.values_json,
               try_cast(json_extract_string(p.values_json,'$[0]') AS DOUBLE),
               try_cast(json_extract_string(p.values_json,'$[1]') AS DOUBLE),
               p.epoch_raw,p.method,p.model,p.reference_raw,p.quality_json,
               p.normalization_version,
               CASE WHEN b.binding_status='accepted'
                          AND p.parameter_set_kind={sql_literal(source['physical_parameter_set_kind'])}
                          AND p.normalization_version={sql_literal(source['physical_normalization_version'])}
                      THEN 'eligible_for_physical_quantity_selection'
                    WHEN b.binding_status<>'accepted' THEN 'unresolved_target'
                    ELSE 'invalid_parameter_set_contract' END,
               CASE WHEN b.binding_status='accepted'
                      THEN 'coherent source-published radius/mass set on one exact canonical natural object'
                    ELSE b.binding_reason END,
               {policy_sql}
        FROM evidence.solar_system_object_parameter_sets p
        JOIN evidence.source_records s USING(source_record_id)
        JOIN solar_target_bindings b USING(source_record_id)
        WHERE s.source_id={source_id} AND s.release_id={release_id};
        """
    )

    scalar = lambda sql: int(con.execute(sql).fetchone()[0] or 0)
    return {
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
        "canonical_relation_promotions": scalar("SELECT count(*) FROM solar_relation_bindings WHERE canonical_relation_promotion")
            + scalar("SELECT count(*) FROM solar_orbital_solution_projection WHERE canonical_relation_promotion"),
    }


def verify(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
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
    result = {name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in queries.items()}
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"Solar System projection checks failed: {failing}")
    return result


def compile_solar_system(
    *,
    policy_path: Path,
    state: Path,
    output_root: Path,
    report_path: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    policy = read_json(policy_path)
    validate_policy(policy)
    source = policy["source"]
    policy_sha = sha256_bytes(canonical_json(policy))
    compiler_sha = sha256_file(Path(__file__).resolve())
    canonical_build = str(policy["canonical_reference_build_id"])
    core_db = state / "out" / canonical_build / "core.duckdb"
    arm_db = state / "out" / canonical_build / "arm.duckdb"
    evidence_db = state / "derived/evidence_lake_v2/scientific_evidence" / source["evidence_build_id"] / "scientific_evidence.duckdb"
    required = [core_db, arm_db, evidence_db]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing Solar System compiler inputs: {missing}")
    build_id = sha256_bytes(canonical_json({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "canonical_reference_build_id": canonical_build,
        "evidence_build_id": source["evidence_build_id"],
    }))[:24]
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / build_id
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        database = staging / "selected_solar_system.duckdb"
        con = duckdb.connect(str(database))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        spill = staging / "spill"
        spill.mkdir()
        con.execute(f"SET temp_directory={sql_literal(spill)}")
        con.execute(f"ATTACH {sql_literal(core_db)} AS core (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(arm_db)} AS arm (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(evidence_db)} AS evidence (READ_ONLY)")
        create_schema(con)
        observed = materialize(con, policy=policy)
        expected = {key: int(value) for key, value in source["acceptance"].items()}
        if observed != expected:
            raise ValueError(f"Solar System acceptance counts changed: expected={expected}:observed={observed}")
        checks = verify(con)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?)",
            [build_id, policy["policy_version"], policy_sha, canonical_build, utc_now(), "pass"],
        )
        con.execute("CHECKPOINT")
        parquet = staging / "parquet"
        parquet.mkdir()
        for table, order_key in (
            ("solar_target_bindings", "binding_id"),
            ("solar_relation_bindings", "binding_id"),
            ("solar_orbital_solution_projection", "evidence_id"),
            ("solar_physical_parameter_projection", "evidence_id"),
        ):
            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order_key}) TO "
                f"{sql_literal(parquet / (table + '.parquet'))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        con.close()
        shutil.rmtree(spill)
        files: dict[str, dict[str, Any]] = {}
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                files[str(path.relative_to(staging))] = {
                    "bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
        deterministic_files = {name: value for name, value in files.items() if name.startswith("parquet/")}
        manifest = {
            "schema_version": "spacegate.e5_selected_solar_system.v1",
            "build_id": build_id,
            "policy_version": policy["policy_version"],
            "policy_sha256": policy_sha,
            "compiler_version": policy["compiler_version"],
            "compiler_sha256": compiler_sha,
            "canonical_reference_build_id": canonical_build,
            "evidence_build_id": source["evidence_build_id"],
            "source_id": source["source_id"],
            "observed": observed,
            "verification": checks,
            "deterministic_files": deterministic_files,
            "generated_at": utc_now(),
            "status": "pass",
        }
        write_json(staging / "manifest.json", manifest)
        if destination.exists():
            existing = read_json(destination / "manifest.json")
            comparable = (
                "policy_sha256", "canonical_reference_build_id", "evidence_build_id",
                "observed", "verification", "deterministic_files",
            )
            if any(existing.get(key) != manifest.get(key) for key in comparable):
                raise ValueError(f"deterministic Solar System build differs: {build_id}")
            shutil.rmtree(staging)
        else:
            os.replace(staging, destination)
        report = {**manifest, "artifact_path": str(destination), "wall_seconds": round(time.monotonic() - started, 3)}
        write_json(report_path, report)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state", type=Path, default=Path(os.environ.get("SPACEGATE_STATE_DIR", DEFAULT_STATE)))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    output_root = args.output_root or args.state / "derived/evidence_lake_v2/selected_solar_system"
    report_path = args.report or args.state / "reports/evidence_lake_v2/e5_selected_solar_system_report.json"
    report = compile_solar_system(policy_path=args.policy, state=args.state, output_root=output_root, report_path=report_path)
    print(f"Selected Solar System evidence pass: build={report['build_id']} wall={report['wall_seconds']}s")


if __name__ == "__main__":
    main()
