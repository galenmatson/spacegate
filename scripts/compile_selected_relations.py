#!/usr/bin/env python3
"""Compile release-scoped relation evidence with independent endpoint bindings."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_relation_policies.json"
DEFAULT_STATE = Path("/data/spacegate/state")
UNSIGNED_DECIMAL = re.compile(r"^[0-9]+$")


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
    if policy.get("schema_version") != "spacegate.e5_relation_policies.v1":
        raise ValueError("unsupported relation policy schema")
    if not policy.get("sources"):
        raise ValueError("relation policy has no sources")
    for source in policy["sources"]:
        binding = source["endpoint_binding"]
        projection = source["projection"]
        if binding.get("normalization") != "unsigned_decimal":
            raise ValueError(f"unsupported endpoint normalization: {source['source_id']}")
        if projection.get("high_confidence_operator") != "lt":
            raise ValueError(f"unsupported confidence operator: {source['source_id']}")
        if projection.get("canonical_containment_promotion") is not False:
            raise ValueError(f"relation policy may not promote containment: {source['source_id']}")


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          build_id VARCHAR, policy_version VARCHAR, policy_sha256 VARCHAR,
          identity_graph_id VARCHAR, identity_report_sha256 VARCHAR,
          generated_at VARCHAR, status VARCHAR
        );
        CREATE TABLE relation_endpoint_bindings (
          endpoint_binding_id VARCHAR, relation_evidence_id VARCHAR,
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, endpoint_role VARCHAR,
          source_namespace VARCHAR, identifier_raw VARCHAR,
          identifier_normalized VARCHAR, canonical_namespace VARCHAR,
          canonical_object_node_key VARCHAR, stable_object_key VARCHAR,
          system_stable_object_key VARCHAR, object_type VARCHAR,
          binding_status VARCHAR, binding_method VARCHAR, binding_reason VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE relation_evidence_projection (
          projected_relation_id VARCHAR, relation_evidence_id VARCHAR,
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, left_endpoint_binding_id VARCHAR,
          right_endpoint_binding_id VARCHAR, left_stable_object_key VARCHAR,
          right_stable_object_key VARCHAR, left_system_stable_object_key VARCHAR,
          right_system_stable_object_key VARCHAR, relation_kind VARCHAR,
          relation_scope VARCHAR, probability DOUBLE, probability_semantics VARCHAR,
          confidence_statistic_key VARCHAR, confidence_statistic_value_raw VARCHAR,
          confidence_statistic_value DOUBLE, confidence_statistic_unit VARCHAR,
          confidence_statistic_semantics VARCHAR, evidence_polarity VARCHAR,
          method VARCHAR, reference_raw VARCHAR, epoch_raw VARCHAR,
          quality_json JSON, projection_status VARCHAR, projection_reason VARCHAR,
          high_confidence_threshold DOUBLE, policy_version VARCHAR
        );
        """
    )


def compile_source(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_db: Path,
    policy_version: str,
) -> dict[str, Any]:
    alias = "source_evidence"
    con.execute(f"ATTACH {sql_literal(source_db)} AS {alias} (READ_ONLY)")
    binding = source["endpoint_binding"]
    projection = source["projection"]
    source_id = sql_literal(source["source_id"])
    release_id = sql_literal(source["release_id"])
    evidence_build_id = sql_literal(source["evidence_build_id"])
    policy_sql = sql_literal(policy_version)
    source_namespace = sql_literal(binding["source_namespace"])
    canonical_namespace = sql_literal(binding["canonical_namespace"])
    object_type = sql_literal(source["object_type"])
    relation_table = str(source["relation_table"])
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation_table):
        raise ValueError(f"unsafe relation table: {relation_table}")

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE endpoint_subjects AS
        SELECT evidence_id relation_evidence_id, source_record_id,
               'left'::VARCHAR endpoint_role,
               left_identity_namespace source_namespace,
               left_identity_raw identifier_raw
        FROM {alias}.{relation_table}
        UNION ALL
        SELECT evidence_id, source_record_id, 'right',
               right_identity_namespace, right_identity_raw
        FROM {alias}.{relation_table};

        CREATE OR REPLACE TEMP TABLE endpoint_candidates AS
        SELECT e.relation_evidence_id, e.endpoint_role,
               b.object_node_key, b.stable_object_key,
               b.system_stable_object_key, o.object_type
        FROM endpoint_subjects e
        JOIN identity.canonical_identifier_bindings b
          ON b.namespace={canonical_namespace}
         AND b.id_value_norm=e.identifier_raw
        JOIN identity.canonical_object_nodes o
          ON o.object_node_key=b.object_node_key
        WHERE e.source_namespace={source_namespace}
          AND regexp_full_match(e.identifier_raw, '[0-9]+');

        INSERT INTO relation_endpoint_bindings
        WITH resolved AS (
          SELECT relation_evidence_id, endpoint_role,
                 min(object_node_key) FILTER (WHERE object_type={object_type}) object_node_key,
                 min(stable_object_key) FILTER (WHERE object_type={object_type}) stable_object_key,
                 min(system_stable_object_key) FILTER (WHERE object_type={object_type}) system_stable_object_key,
                 min(object_type) FILTER (WHERE object_type={object_type}) object_type,
                 count(DISTINCT stable_object_key) target_count,
                 count(DISTINCT stable_object_key) FILTER (WHERE object_type={object_type}) compatible_target_count
          FROM endpoint_candidates GROUP BY 1,2
        )
        SELECT sha256(concat_ws('|', {source_id}, e.relation_evidence_id,
                                    e.endpoint_role, {policy_sql})),
               e.relation_evidence_id, e.source_record_id, {source_id},
               {release_id}, {evidence_build_id}, e.endpoint_role,
               e.source_namespace, e.identifier_raw,
               CASE WHEN regexp_full_match(e.identifier_raw, '[0-9]+')
                    THEN e.identifier_raw END,
               {canonical_namespace},
               CASE WHEN r.target_count=1 AND r.compatible_target_count=1
                    THEN r.object_node_key END,
               CASE WHEN r.target_count=1 AND r.compatible_target_count=1
                    THEN r.stable_object_key END,
               CASE WHEN r.target_count=1 AND r.compatible_target_count=1
                    THEN r.system_stable_object_key END,
               CASE WHEN r.target_count=1 AND r.compatible_target_count=1
                    THEN r.object_type END,
               CASE
                 WHEN e.source_namespace<>{source_namespace} THEN 'excluded'
                 WHEN NOT regexp_full_match(e.identifier_raw, '[0-9]+') THEN 'excluded'
                 WHEN r.target_count=1 AND r.compatible_target_count=1 THEN 'accepted'
                 WHEN r.target_count>1 THEN 'ambiguous'
                 WHEN r.target_count=1 AND r.compatible_target_count=0 THEN 'excluded'
                 ELSE 'missing'
               END,
               {sql_literal(binding['method'])},
               CASE
                 WHEN e.source_namespace<>{source_namespace} THEN 'unexpected source endpoint namespace'
                 WHEN NOT regexp_full_match(e.identifier_raw, '[0-9]+') THEN 'invalid unsigned-decimal endpoint identifier'
                 WHEN r.target_count=1 AND r.compatible_target_count=1 THEN {sql_literal(binding['reason'])}
                 WHEN r.target_count>1 THEN 'multiple current canonical endpoint targets'
                 WHEN r.target_count=1 AND r.compatible_target_count=0 THEN 'unique endpoint target has incompatible object type'
                 ELSE 'endpoint identifier absent from current canonical graph'
               END,
               {policy_sql}
        FROM endpoint_subjects e
        LEFT JOIN resolved r USING (relation_evidence_id, endpoint_role);
        """
    )

    threshold = float(projection["high_confidence_threshold"])
    con.execute(
        f"""
        INSERT INTO relation_evidence_projection
        SELECT sha256(concat_ws('|', {source_id}, r.evidence_id, {policy_sql})),
               r.evidence_id, r.source_record_id, {source_id}, {release_id},
               {evidence_build_id}, l.endpoint_binding_id, rr.endpoint_binding_id,
               l.stable_object_key, rr.stable_object_key,
               l.system_stable_object_key, rr.system_stable_object_key,
               r.relation_kind, r.relation_scope, r.probability,
               r.probability_semantics, r.confidence_statistic_key,
               r.confidence_statistic_value_raw, r.confidence_statistic_value,
               r.confidence_statistic_unit, r.confidence_statistic_semantics,
               r.evidence_polarity, r.method, r.reference_raw, r.epoch_raw,
               r.quality_json,
               CASE
                 WHEN l.binding_status<>'accepted' OR rr.binding_status<>'accepted'
                   THEN 'unresolved_endpoint_evidence'
                 WHEN r.evidence_polarity={sql_literal(projection['negative_polarity'])}
                   THEN 'negative_control_evidence'
                 WHEN r.evidence_polarity={sql_literal(projection['candidate_polarity'])}
                  AND r.confidence_statistic_key={sql_literal(projection['confidence_statistic_key'])}
                  AND r.confidence_statistic_value < {threshold}
                   THEN 'high_confidence_relation_evidence'
                 WHEN r.evidence_polarity={sql_literal(projection['candidate_polarity'])}
                   THEN 'candidate_relation_evidence'
                 ELSE 'unsupported_polarity_evidence'
               END,
               CASE
                 WHEN l.binding_status<>'accepted' OR rr.binding_status<>'accepted'
                   THEN 'one or both relation endpoints do not resolve uniquely'
                 WHEN r.evidence_polarity={sql_literal(projection['negative_polarity'])}
                   THEN 'source shifted-sky control retained as negative evidence'
                 WHEN r.evidence_polarity={sql_literal(projection['candidate_polarity'])}
                  AND r.confidence_statistic_value < {threshold}
                   THEN {sql_literal(projection['high_confidence_semantics'])}
                 WHEN r.evidence_polarity={sql_literal(projection['candidate_polarity'])}
                   THEN 'source candidate retained without high-confidence promotion'
                 ELSE 'evidence polarity is outside the configured projection policy'
               END,
               {threshold}, {policy_sql}
        FROM {alias}.{relation_table} r
        JOIN relation_endpoint_bindings l
          ON l.relation_evidence_id=r.evidence_id AND l.endpoint_role='left'
        JOIN relation_endpoint_bindings rr
          ON rr.relation_evidence_id=r.evidence_id AND rr.endpoint_role='right'
        WHERE l.source_id={source_id} AND rr.source_id={source_id};
        """
    )

    counts = dict(
        con.execute(
            "SELECT binding_status, COUNT(*) FROM relation_endpoint_bindings "
            "WHERE source_id=? GROUP BY 1 ORDER BY 1",
            [source["source_id"]],
        ).fetchall()
    )
    projections = dict(
        con.execute(
            "SELECT projection_status, COUNT(*) FROM relation_evidence_projection "
            "WHERE source_id=? GROUP BY 1 ORDER BY 1",
            [source["source_id"]],
        ).fetchall()
    )
    total_relations = int(sum(projections.values()))
    total_endpoints = int(sum(counts.values()))
    both_accepted = int(
        con.execute(
            "SELECT COUNT(*) FROM relation_evidence_projection WHERE source_id=? "
            "AND left_stable_object_key IS NOT NULL AND right_stable_object_key IS NOT NULL",
            [source["source_id"]],
        ).fetchone()[0]
    )
    expected = source["acceptance"]
    observed = {
        "relation_claims": total_relations,
        "endpoint_bindings": total_endpoints,
        "both_endpoints_accepted": both_accepted,
        "high_confidence_relations": int(projections.get("high_confidence_relation_evidence", 0)),
        "negative_controls_with_bound_endpoints": int(projections.get("negative_control_evidence", 0)),
    }
    expected_map = {
        key.removeprefix("expected_"): int(value) for key, value in expected.items()
    }
    if observed != expected_map:
        raise ValueError(f"relation acceptance counts changed: expected={expected_map}:observed={observed}")
    con.execute(f"DETACH {alias}")
    return {"source_id": source["source_id"], "binding_outcomes": counts, "projection_outcomes": projections, **observed}


def verify(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    checks = {
        "duplicate_endpoint_binding_ids": "SELECT COUNT(*)-COUNT(DISTINCT endpoint_binding_id) FROM relation_endpoint_bindings",
        "duplicate_projected_relation_ids": "SELECT COUNT(*)-COUNT(DISTINCT projected_relation_id) FROM relation_evidence_projection",
        "relations_without_two_endpoints": "SELECT COUNT(*) FROM (SELECT relation_evidence_id,source_id,COUNT(*) n FROM relation_endpoint_bindings GROUP BY 1,2 HAVING n<>2)",
        "accepted_endpoints_without_targets": "SELECT COUNT(*) FROM relation_endpoint_bindings WHERE binding_status='accepted' AND (stable_object_key IS NULL OR canonical_object_node_key IS NULL)",
        "unaccepted_endpoints_with_targets": "SELECT COUNT(*) FROM relation_endpoint_bindings WHERE binding_status<>'accepted' AND (stable_object_key IS NOT NULL OR canonical_object_node_key IS NOT NULL)",
        "probability_fabrication": "SELECT COUNT(*) FROM relation_evidence_projection WHERE probability IS NOT NULL",
        "negative_control_promotion": "SELECT COUNT(*) FROM relation_evidence_projection WHERE evidence_polarity='negative_control' AND projection_status<>'negative_control_evidence' AND projection_status<>'unresolved_endpoint_evidence'",
        "canonical_containment_rows": "SELECT 0",
    }
    result = {name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()}
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"relation projection checks failed: {failing}")
    return result


def compile_relations(*, policy_path: Path, state: Path, output_root: Path, report_path: Path) -> dict[str, Any]:
    started = time.monotonic()
    policy = read_json(policy_path)
    validate_policy(policy)
    policy_sha = sha256_bytes(canonical_json(policy))
    identity_id = str(policy["identity_graph_id"])
    identity_dir = state / "derived/evidence_lake_v2/identity" / identity_id
    identity_db = identity_dir / "identity_graph.duckdb"
    identity_report = identity_dir / "identity_graph_report.json"
    if not identity_db.is_file() or not identity_report.is_file():
        raise FileNotFoundError(f"missing identity graph: {identity_id}")
    identity_report_sha = sha256_file(identity_report)
    compiler_sha = sha256_file(Path(__file__).resolve())
    build_id = sha256_bytes(canonical_json({
        "policy_sha256": policy_sha,
        "identity_graph_id": identity_id,
        "identity_report_sha256": identity_report_sha,
        "source_build_ids": [s["evidence_build_id"] for s in policy["sources"]],
        "compiler_version": policy["compiler_version"],
        "compiler_sha256": compiler_sha,
    }))[:24]
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / build_id
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        database = staging / "selected_relations.duckdb"
        con = duckdb.connect(str(database))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        spill = staging / "spill"
        spill.mkdir()
        con.execute(f"SET temp_directory={sql_literal(spill)}")
        con.execute(f"ATTACH {sql_literal(identity_db)} AS identity (READ_ONLY)")
        create_schema(con)
        source_reports = []
        evidence_root = state / "derived/evidence_lake_v2/scientific_evidence"
        for source in policy["sources"]:
            source_db = evidence_root / source["evidence_build_id"] / "scientific_evidence.duckdb"
            if not source_db.is_file():
                raise FileNotFoundError(f"missing E4 source artifact: {source_db}")
            source_reports.append(compile_source(con, source=source, source_db=source_db, policy_version=policy["policy_version"]))
        checks = verify(con)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?,?)",
            [build_id, policy["policy_version"], policy_sha, identity_id, identity_report_sha, utc_now(), "pass"],
        )
        con.execute("CHECKPOINT")
        parquet = staging / "parquet"
        parquet.mkdir()
        for table, order_key in [
            ("relation_endpoint_bindings", "endpoint_binding_id"),
            ("relation_evidence_projection", "projected_relation_id"),
        ]:
            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order_key}) TO {sql_literal(parquet / (table + '.parquet'))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        con.close()
        shutil.rmtree(spill)
        files = {}
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                files[str(path.relative_to(staging))] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
        deterministic_files = {
            name: metadata for name, metadata in files.items()
            if name.startswith("parquet/")
        }
        manifest = {
            "schema_version": "spacegate.e5_selected_relations.v1",
            "build_id": build_id,
            "policy_version": policy["policy_version"],
            "policy_sha256": policy_sha,
            "compiler_version": policy["compiler_version"],
            "compiler_sha256": compiler_sha,
            "identity_graph_id": identity_id,
            "identity_report_sha256": identity_report_sha,
            "source_reports": source_reports,
            "verification": checks,
            "files": files,
            "deterministic_files": deterministic_files,
            "generated_at": utc_now(),
        }
        write_json(staging / "manifest.json", manifest)
        if destination.exists():
            existing = read_json(destination / "manifest.json")
            comparable = ("policy_sha256", "identity_graph_id", "identity_report_sha256", "source_reports", "verification", "deterministic_files")
            if any(existing.get(key) != manifest.get(key) for key in comparable):
                raise ValueError(f"deterministic relation build differs: {build_id}")
            shutil.rmtree(staging)
        else:
            os.replace(staging, destination)
        report = {**manifest, "artifact_path": str(destination), "wall_seconds": round(time.monotonic() - started, 6), "status": "pass"}
        write_json(report_path, report)
        return report
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    output_root = args.output_root or args.state / "derived/evidence_lake_v2/selected_relations"
    report = compile_relations(policy_path=args.policy, state=args.state, output_root=output_root, report_path=args.report)
    print(f"Selected relation evidence pass: build={report['build_id']} wall={report['wall_seconds']:.1f}s")


if __name__ == "__main__":
    main()
