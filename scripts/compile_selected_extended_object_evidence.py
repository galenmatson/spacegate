#!/usr/bin/env python3
"""Compile release-scoped extended-object evidence through canonical reconciliation."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_extended_object_policies.json"
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
    if policy.get("schema_version") != "spacegate.e5_extended_object_policies.v1":
        raise ValueError("unsupported extended-object policy schema")
    if policy.get("canonical_id_join") != "unique_double_encoded_reconciliation_id_to_bigint_v1":
        raise ValueError("unsupported canonical extended-object ID bridge")
    if not policy.get("sources"):
        raise ValueError("extended-object policy has no sources")
    supported = {"green_snr_galactic_coordinate_key_v1", "extended_catalog_logical_key_v1"}
    for source in policy["sources"]:
        if source.get("source_key_strategy") not in supported:
            raise ValueError(f"unsupported source-key strategy: {source.get('source_key_strategy')}")
        if source.get("stellar_fact_projection") is not False:
            raise ValueError(f"extended-object policy may not emit stellar facts: {source.get('source_id')}")


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          build_id VARCHAR, policy_version VARCHAR, policy_sha256 VARCHAR,
          canonical_reference_build_id VARCHAR, generated_at VARCHAR, status VARCHAR
        );
        CREATE TABLE extended_object_bindings (
          binding_id VARCHAR, evidence_id VARCHAR, source_record_id VARCHAR,
          source_id VARCHAR, release_id VARCHAR, evidence_build_id VARCHAR,
          source_table VARCHAR, object_scope VARCHAR, source_record_key VARCHAR,
          reconciliation_id BIGINT, reconciliation_outcome VARCHAR,
          reconciliation_reason VARCHAR, reconciliation_object_id_double DOUBLE,
          canonical_candidate_count BIGINT, canonical_extended_object_id BIGINT,
          canonical_stable_object_key VARCHAR, canonical_name VARCHAR,
          canonical_object_family VARCHAR, canonical_object_type VARCHAR,
          binding_status VARCHAR, binding_method VARCHAR, binding_reason VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE extended_object_evidence_projection (
          evidence_id VARCHAR, source_record_id VARCHAR, extended_kind VARCHAR,
          geometry_raw JSON, distance_raw JSON, parameter_set_raw JSON,
          method VARCHAR, model VARCHAR, reference_raw VARCHAR, quality_json JSON,
          normalization_version VARCHAR, binding_id VARCHAR, source_id VARCHAR,
          release_id VARCHAR, source_table VARCHAR, source_record_key VARCHAR,
          reconciliation_outcome VARCHAR, canonical_extended_object_id BIGINT,
          canonical_stable_object_key VARCHAR, authority_role VARCHAR,
          projection_status VARCHAR, projection_reason VARCHAR,
          stellar_fact_projection BOOLEAN, policy_version VARCHAR
        );
        """
    )


def source_key_expression(strategy: str) -> str:
    if strategy == "green_snr_galactic_coordinate_key_v1":
        return (
            "'green_snr:snr:g+' || "
            "json_extract_string(r.logical_key_json,'$.galactic_longitude') || "
            "json_extract_string(r.logical_key_json,'$.galactic_latitude')"
        )
    if strategy == "extended_catalog_logical_key_v1":
        return """
        CASE r.source_table
          WHEN 'openngc_ngc' THEN 'openngc:' || lower(json_extract_string(r.logical_key_json,'$.Name'))
          WHEN 'openngc_addendum' THEN 'openngc:' || lower(json_extract_string(r.logical_key_json,'$.Name'))
          WHEN 'lbn_vii_9' THEN 'lbn:' || json_extract_string(r.logical_key_json,'$.Seq')
          WHEN 'ldn_vii_7a' THEN CASE
            WHEN json_extract_string(r.logical_key_json,'$.LDN') IS NOT NULL
              THEN 'ldn:' || json_extract_string(r.logical_key_json,'$.LDN')
            ELSE 'ldn:seq-' || json_extract_string(e.parameter_set_raw,'$.Seq') END
          WHEN 'barnard_vii_220a' THEN 'barnard:' || json_extract_string(r.logical_key_json,'$.Barn')
          WHEN 'magakian_2003' THEN 'magakian:' || json_extract_string(r.logical_key_json,'$.Seq')
          WHEN 'vdb_vii_21' THEN 'vdb:' || json_extract_string(r.logical_key_json,'$.VdB')
          WHEN 'sharpless_vii_20' THEN 'sh2:' || json_extract_string(r.logical_key_json,'$.Sh2')
          WHEN 'cederblad_vii_231' THEN 'cederblad:' ||
            json_extract_string(r.logical_key_json,'$.Ced') ||
            lower(coalesce(json_extract_string(r.logical_key_json,'$.m_Ced'),''))
        END
        """
    raise ValueError(f"unsupported source-key strategy: {strategy}")


def compile_source(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    alias: str,
    policy_version: str,
) -> dict[str, Any]:
    source_id = sql_literal(source["source_id"])
    release_id = sql_literal(source["release_id"])
    evidence_build_id = sql_literal(source["evidence_build_id"])
    policy_sql = sql_literal(policy_version)
    authority = sql_literal(source["authority_role"])
    key_expression = source_key_expression(source["source_key_strategy"])
    accepted_outcomes = ",".join(sql_literal(value) for value in source["accepted_reconciliation_outcomes"])
    temp_name = "source_mapping_" + alias

    con.execute(
        f"""
        CREATE TEMP TABLE {temp_name} AS
        SELECT e.evidence_id,e.source_record_id,r.source_table,r.object_scope,
               {key_expression} source_record_key
        FROM {alias}.extended_object_evidence e
        JOIN {alias}.source_records r USING(source_record_id)
        WHERE r.source_id={source_id} AND r.release_id={release_id};

        INSERT INTO extended_object_bindings
        WITH candidates AS (
          SELECT m.evidence_id,m.source_record_id,m.source_table,m.object_scope,
                 m.source_record_key,x.extended_object_reconciliation_id reconciliation_id,
                 x.outcome reconciliation_outcome,x.reason reconciliation_reason,
                 x.extended_object_id reconciliation_object_id_double,
                 count(DISTINCT o.stable_object_key) canonical_candidate_count,
                 min(o.extended_object_id) candidate_object_id,
                 min(o.stable_object_key) candidate_key,min(o.canonical_name) candidate_name,
                 min(o.object_family) candidate_family,min(o.object_type) candidate_type
          FROM {temp_name} m
          LEFT JOIN core.extended_object_source_reconciliation x
            ON x.source_record_key=m.source_record_key
          LEFT JOIN core.extended_objects o
            ON cast(o.extended_object_id AS DOUBLE)=x.extended_object_id
          GROUP BY m.evidence_id,m.source_record_id,m.source_table,m.object_scope,
                   m.source_record_key,x.extended_object_reconciliation_id,
                   x.outcome,x.reason,x.extended_object_id
        )
        SELECT sha256(concat_ws('|',{source_id},evidence_id,'extended-object',{policy_sql})),
               evidence_id,source_record_id,{source_id},{release_id},{evidence_build_id},
               source_table,object_scope,source_record_key,reconciliation_id,
               reconciliation_outcome,reconciliation_reason,reconciliation_object_id_double,
               canonical_candidate_count,
               CASE WHEN reconciliation_outcome IN ({accepted_outcomes})
                          AND canonical_candidate_count=1 THEN candidate_object_id END,
               CASE WHEN reconciliation_outcome IN ({accepted_outcomes})
                          AND canonical_candidate_count=1 THEN candidate_key END,
               CASE WHEN reconciliation_outcome IN ({accepted_outcomes})
                          AND canonical_candidate_count=1 THEN candidate_name END,
               CASE WHEN reconciliation_outcome IN ({accepted_outcomes})
                          AND canonical_candidate_count=1 THEN candidate_family END,
               CASE WHEN reconciliation_outcome IN ({accepted_outcomes})
                          AND canonical_candidate_count=1 THEN candidate_type END,
               CASE WHEN reconciliation_outcome IN ({accepted_outcomes})
                          AND canonical_candidate_count=1 THEN 'accepted'
                    WHEN reconciliation_outcome LIKE 'excluded_%' THEN 'excluded'
                    WHEN reconciliation_outcome LIKE 'quarantine_%' THEN 'quarantined'
                    ELSE 'unresolved' END,
               {sql_literal(source['source_key_strategy'])},
               CASE WHEN reconciliation_outcome IN ({accepted_outcomes})
                          AND canonical_candidate_count=1
                      THEN 'exact source record key has one accepted canonical reconciliation target'
                    WHEN reconciliation_outcome LIKE 'excluded_%'
                      THEN 'canonical extended-object policy explicitly excludes this source row'
                    WHEN reconciliation_outcome LIKE 'quarantine_%'
                      THEN 'canonical extended-object policy quarantines this source row'
                    WHEN reconciliation_outcome='redirect'
                      THEN 'source redirect has no canonical target in the current reconciliation'
                    WHEN reconciliation_outcome IS NULL
                      THEN 'source record key is absent from the current canonical reconciliation'
                    WHEN canonical_candidate_count>1
                      THEN 'reconciliation object ID resolves to multiple canonical extended objects'
                    ELSE 'reconciliation outcome does not yield one canonical extended object' END,
               {policy_sql}
        FROM candidates;

        INSERT INTO extended_object_evidence_projection
        SELECT e.evidence_id,e.source_record_id,e.extended_kind,e.geometry_raw,
               e.distance_raw,e.parameter_set_raw,e.method,e.model,e.reference_raw,
               e.quality_json,e.normalization_version,b.binding_id,b.source_id,
               b.release_id,b.source_table,b.source_record_key,
               b.reconciliation_outcome,b.canonical_extended_object_id,
               b.canonical_stable_object_key,{authority},
               CASE b.binding_status
                 WHEN 'accepted' THEN 'eligible_for_extended_quantity_selection'
                 WHEN 'excluded' THEN 'excluded_source_context'
                 WHEN 'quarantined' THEN 'quarantined_source_context'
                 ELSE 'unresolved_identity_evidence' END,
               CASE WHEN b.binding_status='accepted'
                      THEN 'source-native geometry, distance, and parameter set on one reconciled canonical extended object'
                    ELSE b.binding_reason END,
               false,{policy_sql}
        FROM {alias}.extended_object_evidence e
        JOIN extended_object_bindings b
          ON b.source_id={source_id} AND b.evidence_id=e.evidence_id;
        """
    )

    bindings = dict(con.execute(
        "SELECT binding_status,count(*) FROM extended_object_bindings WHERE source_id=? GROUP BY 1",
        [source["source_id"]],
    ).fetchall())
    observed = {
        "bindings": sum(bindings.values()),
        "bindings_accepted": bindings.get("accepted", 0),
        "bindings_excluded": bindings.get("excluded", 0),
        "bindings_quarantined": bindings.get("quarantined", 0),
        "bindings_unresolved": bindings.get("unresolved", 0),
        "evidence": int(con.execute(
            "SELECT count(*) FROM extended_object_evidence_projection WHERE source_id=?",
            [source["source_id"]],
        ).fetchone()[0]),
        "evidence_eligible": int(con.execute(
            "SELECT count(*) FROM extended_object_evidence_projection WHERE source_id=? AND projection_status='eligible_for_extended_quantity_selection'",
            [source["source_id"]],
        ).fetchone()[0]),
        "canonical_candidate_ambiguities": int(con.execute(
            "SELECT count(*) FROM extended_object_bindings WHERE source_id=? AND canonical_candidate_count>1",
            [source["source_id"]],
        ).fetchone()[0]),
        "stellar_fact_rows": int(con.execute(
            "SELECT count(*) FROM extended_object_evidence_projection WHERE source_id=? AND stellar_fact_projection",
            [source["source_id"]],
        ).fetchone()[0]),
    }
    expected = {key.removeprefix("expected_"): int(value) for key, value in source["acceptance"].items()}
    if observed != expected:
        raise ValueError(f"extended-object acceptance counts changed for {source['source_id']}: expected={expected}:observed={observed}")
    return {"source_id": source["source_id"], "observed": observed, "expected": expected}


def verify(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    checks = {
        "duplicate_binding_ids": "SELECT count(*)-count(DISTINCT binding_id) FROM extended_object_bindings",
        "duplicate_source_evidence_bindings": "SELECT count(*) FROM (SELECT source_id,evidence_id FROM extended_object_bindings GROUP BY 1,2 HAVING count(*)<>1)",
        "accepted_bindings_without_targets": "SELECT count(*) FROM extended_object_bindings WHERE binding_status='accepted' AND (canonical_extended_object_id IS NULL OR canonical_stable_object_key IS NULL)",
        "unaccepted_bindings_with_targets": "SELECT count(*) FROM extended_object_bindings WHERE binding_status<>'accepted' AND (canonical_extended_object_id IS NOT NULL OR canonical_stable_object_key IS NOT NULL)",
        "accepted_bindings_without_one_candidate": "SELECT count(*) FROM extended_object_bindings WHERE binding_status='accepted' AND canonical_candidate_count<>1",
        "source_rows_without_keys": "SELECT count(*) FROM extended_object_bindings WHERE source_record_key IS NULL OR source_record_key=''",
        "eligible_unbound_evidence": "SELECT count(*) FROM extended_object_evidence_projection WHERE projection_status='eligible_for_extended_quantity_selection' AND canonical_stable_object_key IS NULL",
        "stellar_fact_rows": "SELECT count(*) FROM extended_object_evidence_projection WHERE stellar_fact_projection",
        "canonical_double_id_collisions": "SELECT count(*) FROM (SELECT cast(extended_object_id AS DOUBLE) encoded_id FROM core.extended_objects GROUP BY 1 HAVING count(*)>1)",
    }
    result = {name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()}
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"extended-object projection checks failed: {failing}")
    return result


def compile_extended_objects(
    *,
    policy_path: Path,
    state: Path,
    output_root: Path,
    report_path: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    policy = read_json(policy_path)
    validate_policy(policy)
    policy_sha = sha256_bytes(canonical_json(policy))
    compiler_sha = sha256_file(Path(__file__).resolve())
    canonical_build = str(policy["canonical_reference_build_id"])
    core_db = state / "out" / canonical_build / "core.duckdb"
    source_paths = [
        state / "derived/evidence_lake_v2/scientific_evidence" / source["evidence_build_id"] / "scientific_evidence.duckdb"
        for source in policy["sources"]
    ]
    required = [core_db, *source_paths]
    if not all(path.is_file() for path in required):
        raise FileNotFoundError(f"missing extended-object compiler input: {[str(path) for path in required if not path.is_file()]}")
    build_id = sha256_bytes(canonical_json({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "canonical_reference_build_id": canonical_build,
        "source_build_ids": [source["evidence_build_id"] for source in policy["sources"]],
    }))[:24]
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / build_id
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        database = staging / "selected_extended_objects.duckdb"
        con = duckdb.connect(str(database))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        spill = staging / "spill"
        spill.mkdir()
        con.execute(f"SET temp_directory={sql_literal(spill)}")
        con.execute(f"ATTACH {sql_literal(core_db)} AS core (READ_ONLY)")
        for index, source_path in enumerate(source_paths):
            con.execute(f"ATTACH {sql_literal(source_path)} AS src{index} (READ_ONLY)")
        create_schema(con)
        source_reports = [
            compile_source(
                con,
                source=source,
                alias=f"src{index}",
                policy_version=policy["policy_version"],
            )
            for index, source in enumerate(policy["sources"])
        ]
        checks = verify(con)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?)",
            [build_id, policy["policy_version"], policy_sha, canonical_build, utc_now(), "pass"],
        )
        con.execute("CHECKPOINT")
        parquet = staging / "parquet"
        parquet.mkdir()
        for table, order_key in (
            ("extended_object_bindings", "binding_id"),
            ("extended_object_evidence_projection", "evidence_id"),
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
            "schema_version": "spacegate.e5_selected_extended_objects.v1",
            "build_id": build_id,
            "policy_version": policy["policy_version"],
            "policy_sha256": policy_sha,
            "compiler_version": policy["compiler_version"],
            "compiler_sha256": compiler_sha,
            "canonical_reference_build_id": canonical_build,
            "source_reports": source_reports,
            "verification": checks,
            "deterministic_files": deterministic_files,
            "generated_at": utc_now(),
            "status": "pass",
        }
        write_json(staging / "manifest.json", manifest)
        if destination.exists():
            existing = read_json(destination / "manifest.json")
            comparable = (
                "policy_sha256", "canonical_reference_build_id", "source_reports",
                "verification", "deterministic_files",
            )
            if any(existing.get(key) != manifest.get(key) for key in comparable):
                raise ValueError(f"deterministic extended-object build differs: {build_id}")
            shutil.rmtree(staging)
        else:
            os.replace(staging, destination)
        report = {
            **manifest,
            "artifact_path": str(destination),
            "wall_seconds": round(time.monotonic() - started, 3),
        }
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
    output_root = args.output_root or args.state / "derived/evidence_lake_v2/selected_extended_objects"
    report_path = args.report or args.state / "reports/evidence_lake_v2/e5_selected_extended_object_report.json"
    report = compile_extended_objects(policy_path=args.policy, state=args.state, output_root=output_root, report_path=report_path)
    print(f"Selected extended-object evidence pass: build={report['build_id']} wall={report['wall_seconds']}s")


if __name__ == "__main__":
    main()
