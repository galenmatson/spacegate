#!/usr/bin/env python3
"""Compile release-scoped cluster facts and probability-bearing memberships."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_cluster_policies.json"
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
    if policy.get("schema_version") != "spacegate.e5_cluster_policies.v1":
        raise ValueError("unsupported cluster policy schema")
    if not policy.get("sources"):
        raise ValueError("cluster policy has no sources")
    for source in policy["sources"]:
        if source.get("canonical_containment_promotion") is not False:
            raise ValueError(f"cluster policy may not promote containment: {source.get('source_id')}")
        if source.get("designation_normalization") != "lower_trim_space_hyphen_to_underscore_v1":
            raise ValueError("unsupported cluster designation normalization")


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          build_id VARCHAR, policy_version VARCHAR, policy_sha256 VARCHAR,
          identity_graph_id VARCHAR, canonical_reference_build_id VARCHAR,
          generated_at VARCHAR, status VARCHAR
        );
        CREATE TABLE cluster_identity_bindings (
          cluster_binding_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, cluster_identity_raw VARCHAR,
          source_designations_json JSON, matched_identifier_evidence_json JSON,
          canonical_candidates_json JSON, canonical_candidate_count BIGINT,
          source_cluster_collision_count BIGINT, canonical_cluster_id BIGINT,
          canonical_cluster_stable_object_key VARCHAR,
          canonical_cluster_name VARCHAR, binding_status VARCHAR,
          binding_method VARCHAR, binding_reason VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE cluster_membership_endpoint_bindings (
          membership_binding_id VARCHAR, membership_evidence_id VARCHAR,
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, cluster_identity_raw VARCHAR,
          cluster_binding_id VARCHAR, cluster_binding_status VARCHAR,
          canonical_cluster_stable_object_key VARCHAR,
          member_gaia_dr3_source_id VARCHAR,
          member_candidate_count BIGINT, member_system_candidate_count BIGINT,
          member_stable_object_key VARCHAR, member_system_stable_object_key VARCHAR,
          member_binding_status VARCHAR, binding_method VARCHAR,
          binding_reason VARCHAR, policy_version VARCHAR
        );
        """
    )


def compile_source(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    policy_version: str,
) -> dict[str, Any]:
    source_id = sql_literal(source["source_id"])
    release_id = sql_literal(source["release_id"])
    evidence_build_id = sql_literal(source["evidence_build_id"])
    policy_sql = sql_literal(policy_version)
    binding_method = sql_literal(source["cluster_binding_method"])
    namespaces = ",".join(sql_literal(value) for value in source["cluster_name_namespaces"])
    normalized = "lower(regexp_replace(trim({}), '[ -]+','_','g'))"

    con.execute(
        f"""
        CREATE TEMP TABLE source_cluster_ids AS
        SELECT DISTINCT cluster_identity_raw
        FROM source.cluster_evidence;

        CREATE TEMP TABLE source_cluster_designations AS
        SELECT DISTINCT e.cluster_identity_raw,i.evidence_id identifier_evidence_id,
               i.namespace,i.identifier_raw,
               {normalized.format('i.identifier_raw')} identifier_normalized
        FROM source.cluster_evidence e
        JOIN source.identifier_claim_evidence i USING(source_record_id)
        WHERE i.namespace IN ({namespaces});

        CREATE TEMP TABLE canonical_cluster_names AS
        SELECT cluster_id,stable_object_key,cluster_name,
               {normalized.format('cluster_name')} identifier_normalized
        FROM core.open_clusters;

        CREATE TEMP TABLE cluster_candidate_matches AS
        SELECT DISTINCT d.cluster_identity_raw,d.identifier_evidence_id,
               d.identifier_raw,c.cluster_id,c.stable_object_key,c.cluster_name
        FROM source_cluster_designations d
        JOIN canonical_cluster_names c USING(identifier_normalized);

        CREATE TEMP TABLE canonical_cluster_collision_counts AS
        SELECT stable_object_key,count(DISTINCT cluster_identity_raw) source_cluster_count
        FROM cluster_candidate_matches
        GROUP BY stable_object_key;

        INSERT INTO cluster_identity_bindings
        WITH summaries AS (
          SELECT ids.cluster_identity_raw,
                 count(DISTINCT m.stable_object_key) canonical_candidate_count,
                 min(m.stable_object_key) candidate_key,
                 min(m.cluster_id) candidate_id,min(m.cluster_name) candidate_name,
                 coalesce(to_json(list(DISTINCT d.identifier_raw ORDER BY d.identifier_raw)
                   FILTER (WHERE d.identifier_raw IS NOT NULL)),'[]'::JSON) source_designations_json,
                 coalesce(to_json(list(DISTINCT m.identifier_evidence_id ORDER BY m.identifier_evidence_id)
                   FILTER (WHERE m.identifier_evidence_id IS NOT NULL)),'[]'::JSON) matched_identifier_evidence_json,
                 coalesce(to_json(list(DISTINCT m.stable_object_key ORDER BY m.stable_object_key)
                   FILTER (WHERE m.stable_object_key IS NOT NULL)),'[]'::JSON) canonical_candidates_json
          FROM source_cluster_ids ids
          LEFT JOIN source_cluster_designations d USING(cluster_identity_raw)
          LEFT JOIN cluster_candidate_matches m
            ON m.cluster_identity_raw=ids.cluster_identity_raw
           AND m.identifier_evidence_id=d.identifier_evidence_id
          GROUP BY ids.cluster_identity_raw
        ), classified AS (
          SELECT s.*,coalesce(c.source_cluster_count,0) source_cluster_collision_count
          FROM summaries s
          LEFT JOIN canonical_cluster_collision_counts c
            ON c.stable_object_key=s.candidate_key
        )
        SELECT sha256(concat_ws('|',{source_id},cluster_identity_raw,'cluster',{policy_sql})),
               {source_id},{release_id},{evidence_build_id},cluster_identity_raw,
               source_designations_json,matched_identifier_evidence_json,
               canonical_candidates_json,canonical_candidate_count,
               source_cluster_collision_count,
               CASE WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                    THEN candidate_id END,
               CASE WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                    THEN candidate_key END,
               CASE WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                    THEN candidate_name END,
               CASE WHEN canonical_candidate_count=0 THEN 'missing'
                    WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                      THEN 'accepted'
                    ELSE 'ambiguous' END,
               {binding_method},
               CASE WHEN canonical_candidate_count=0
                      THEN 'no exact source designation matches a canonical cluster'
                    WHEN canonical_candidate_count>1
                      THEN 'source cluster designations resolve to multiple canonical clusters'
                    WHEN source_cluster_collision_count>1
                      THEN 'multiple release-scoped source clusters claim the same canonical cluster designation'
                    ELSE 'one exact source designation resolves one-to-one to a canonical cluster' END,
               {policy_sql}
        FROM classified;

        CREATE TABLE cluster_evidence_projection AS
        SELECT e.*,r.source_table,b.cluster_binding_id,b.binding_status,
               b.canonical_cluster_id,b.canonical_cluster_stable_object_key,
               {sql_literal(source['cluster_authority'])} authority_role,
               CASE WHEN b.binding_status='accepted'
                          AND r.source_table='hunt_reffert_2024_clusters'
                      THEN 'eligible_for_quantity_selection'
                    WHEN b.binding_status='accepted'
                      THEN 'identity_context_evidence'
                    ELSE 'unresolved_identity_evidence' END projection_status,
               CASE WHEN b.binding_status='accepted'
                          AND r.source_table='hunt_reffert_2024_clusters'
                      THEN 'coherent Hunt/Reffert cluster posterior on one exact canonical cluster'
                    WHEN b.binding_status='accepted'
                      THEN 'source-published literature crossmatch remains cluster identity context'
                    ELSE b.binding_reason END projection_reason,
               {policy_sql} policy_version
        FROM source.cluster_evidence e
        JOIN source.source_records r USING(source_record_id)
        JOIN cluster_identity_bindings b USING(cluster_identity_raw);

        INSERT INTO cluster_membership_endpoint_bindings
        WITH candidates AS (
          SELECT m.evidence_id membership_evidence_id,m.source_record_id,
                 m.cluster_identity_raw,m.member_identity_raw,
                 count(DISTINCT i.stable_object_key) member_candidate_count,
                 count(DISTINCT i.system_stable_object_key) member_system_candidate_count,
                 min(i.stable_object_key) candidate_member_key,
                 min(i.system_stable_object_key) candidate_system_key
          FROM source.cluster_membership_evidence m
          LEFT JOIN identity.canonical_identifier_bindings i
            ON i.namespace='gaia_dr3' AND i.id_value_norm=m.member_identity_raw
          GROUP BY m.evidence_id,m.source_record_id,m.cluster_identity_raw,
                   m.member_identity_raw
        )
        SELECT sha256(concat_ws('|',{source_id},c.membership_evidence_id,'membership',{policy_sql})),
               c.membership_evidence_id,c.source_record_id,{source_id},{release_id},
               {evidence_build_id},c.cluster_identity_raw,b.cluster_binding_id,
               b.binding_status,b.canonical_cluster_stable_object_key,
               c.member_identity_raw,c.member_candidate_count,
               c.member_system_candidate_count,
               CASE WHEN c.member_candidate_count=1 AND c.member_system_candidate_count=1
                    THEN c.candidate_member_key END,
               CASE WHEN c.member_candidate_count=1 AND c.member_system_candidate_count=1
                    THEN c.candidate_system_key END,
               CASE WHEN c.member_candidate_count=0 OR c.member_system_candidate_count=0
                      THEN 'missing'
                    WHEN c.member_candidate_count=1 AND c.member_system_candidate_count=1
                      THEN 'accepted'
                    ELSE 'ambiguous' END,
               'exact_release_scoped_gaia_dr3_cluster_member',
               CASE WHEN c.member_candidate_count=0 OR c.member_system_candidate_count=0
                      THEN 'exact Gaia DR3 member is outside the current canonical reference'
                    WHEN c.member_candidate_count=1 AND c.member_system_candidate_count=1
                      THEN 'exact Gaia DR3 identifier resolves to one canonical star and system'
                    ELSE 'exact Gaia DR3 member resolves to multiple canonical targets' END,
               {policy_sql}
        FROM candidates c
        JOIN cluster_identity_bindings b USING(cluster_identity_raw);

        CREATE TABLE cluster_membership_projection AS
        SELECT m.*,b.membership_binding_id,b.cluster_binding_id,
               b.cluster_binding_status,b.canonical_cluster_stable_object_key,
               b.member_binding_status,b.member_stable_object_key,
               b.member_system_stable_object_key,
               {sql_literal(source['membership_authority'])} authority_role,
               CASE WHEN b.cluster_binding_status='accepted'
                          AND b.member_binding_status='accepted'
                      THEN 'probability_bearing_membership_evidence'
                    WHEN b.cluster_binding_status='ambiguous'
                          AND b.member_binding_status='accepted'
                      THEN 'ambiguous_cluster_identity_evidence'
                    WHEN b.cluster_binding_status='ambiguous'
                      THEN 'ambiguous_cluster_and_unresolved_member_evidence'
                    WHEN b.cluster_binding_status='accepted'
                      THEN 'unresolved_member_identity_evidence'
                    WHEN b.member_binding_status='accepted'
                      THEN 'unresolved_cluster_identity_evidence'
                    ELSE 'unresolved_endpoint_evidence' END projection_status,
               CASE WHEN b.cluster_binding_status='accepted'
                          AND b.member_binding_status='accepted'
                      THEN 'source-published membership probability with two independently resolved endpoints; not canonical containment'
                    WHEN b.cluster_binding_status<>'accepted'
                      THEN 'cluster endpoint is not uniquely bound to the current canonical reference'
                    ELSE b.binding_reason END projection_reason,
               false canonical_containment_promotion,{policy_sql} policy_version
        FROM source.cluster_membership_evidence m
        JOIN cluster_membership_endpoint_bindings b
          ON b.membership_evidence_id=m.evidence_id;
        """
    )

    cluster_counts = dict(con.execute(
        "SELECT binding_status,count(*) FROM cluster_identity_bindings GROUP BY 1"
    ).fetchall())
    member_counts = dict(con.execute(
        "SELECT member_binding_status,count(*) FROM cluster_membership_endpoint_bindings GROUP BY 1"
    ).fetchall())
    combinations = dict(con.execute(
        "SELECT cluster_binding_status || ':' || member_binding_status,count(*) "
        "FROM cluster_membership_endpoint_bindings GROUP BY 1"
    ).fetchall())
    observed = {
        "cluster_bindings": sum(cluster_counts.values()),
        "clusters_accepted": cluster_counts.get("accepted", 0),
        "clusters_missing": cluster_counts.get("missing", 0),
        "clusters_ambiguous": cluster_counts.get("ambiguous", 0),
        "cluster_evidence": int(con.execute("SELECT count(*) FROM cluster_evidence_projection").fetchone()[0]),
        "cluster_characterizations": int(con.execute("SELECT count(*) FROM cluster_evidence_projection WHERE source_table='hunt_reffert_2024_clusters'").fetchone()[0]),
        "cluster_characterizations_eligible": int(con.execute("SELECT count(*) FROM cluster_evidence_projection WHERE projection_status='eligible_for_quantity_selection'").fetchone()[0]),
        "crossmatch_context": int(con.execute("SELECT count(*) FROM cluster_evidence_projection WHERE source_table='hunt_reffert_2024_crossmatch' AND projection_status='identity_context_evidence'").fetchone()[0]),
        "memberships": int(con.execute("SELECT count(*) FROM cluster_membership_projection").fetchone()[0]),
        "member_endpoints_accepted": member_counts.get("accepted", 0),
        "member_endpoints_missing": member_counts.get("missing", 0),
        "member_endpoints_ambiguous": member_counts.get("ambiguous", 0),
        "memberships_both_accepted": combinations.get("accepted:accepted", 0),
        "memberships_cluster_accepted_member_missing": combinations.get("accepted:missing", 0),
        "memberships_cluster_ambiguous_member_accepted": combinations.get("ambiguous:accepted", 0),
        "memberships_cluster_ambiguous_member_missing": combinations.get("ambiguous:missing", 0),
        "memberships_cluster_missing_member_accepted": combinations.get("missing:accepted", 0),
        "memberships_both_missing": combinations.get("missing:missing", 0),
        "canonical_containment_rows": int(con.execute("SELECT count(*) FROM cluster_membership_projection WHERE canonical_containment_promotion").fetchone()[0]),
    }
    expected = {key.removeprefix("expected_"): int(value) for key, value in source["acceptance"].items()}
    if observed != expected:
        raise ValueError(f"cluster acceptance counts changed: expected={expected}:observed={observed}")
    return {"source_id": source["source_id"], "observed": observed, "expected": expected}


def verify(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    checks = {
        "duplicate_cluster_binding_ids": "SELECT count(*)-count(DISTINCT cluster_binding_id) FROM cluster_identity_bindings",
        "duplicate_membership_binding_ids": "SELECT count(*)-count(DISTINCT membership_binding_id) FROM cluster_membership_endpoint_bindings",
        "accepted_clusters_without_targets": "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status='accepted' AND (canonical_cluster_id IS NULL OR canonical_cluster_stable_object_key IS NULL)",
        "unaccepted_clusters_with_targets": "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status<>'accepted' AND canonical_cluster_stable_object_key IS NOT NULL",
        "accepted_cluster_target_collisions": "SELECT count(*) FROM (SELECT canonical_cluster_stable_object_key FROM cluster_identity_bindings WHERE binding_status='accepted' GROUP BY 1 HAVING count(*)<>1)",
        "eligible_noncharacterizations": "SELECT count(*) FROM cluster_evidence_projection WHERE projection_status='eligible_for_quantity_selection' AND source_table<>'hunt_reffert_2024_clusters'",
        "eligible_unbound_clusters": "SELECT count(*) FROM cluster_evidence_projection WHERE projection_status='eligible_for_quantity_selection' AND canonical_cluster_stable_object_key IS NULL",
        "accepted_members_without_targets": "SELECT count(*) FROM cluster_membership_endpoint_bindings WHERE member_binding_status='accepted' AND (member_stable_object_key IS NULL OR member_system_stable_object_key IS NULL)",
        "unaccepted_members_with_targets": "SELECT count(*) FROM cluster_membership_endpoint_bindings WHERE member_binding_status<>'accepted' AND (member_stable_object_key IS NOT NULL OR member_system_stable_object_key IS NOT NULL)",
        "membership_probabilities_outside_unit_interval": "SELECT count(*) FROM cluster_membership_projection WHERE membership_probability IS NULL OR membership_probability<0 OR membership_probability>1",
        "bound_memberships_without_two_targets": "SELECT count(*) FROM cluster_membership_projection WHERE projection_status='probability_bearing_membership_evidence' AND (canonical_cluster_stable_object_key IS NULL OR member_stable_object_key IS NULL)",
        "canonical_containment_rows": "SELECT count(*) FROM cluster_membership_projection WHERE canonical_containment_promotion",
    }
    result = {name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()}
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"cluster projection checks failed: {failing}")
    return result


def compile_clusters(
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
    identity_id = str(policy["identity_graph_id"])
    canonical_build = str(policy["canonical_reference_build_id"])
    identity_dir = state / "derived/evidence_lake_v2/identity" / identity_id
    identity_db = identity_dir / "identity_graph.duckdb"
    identity_report = identity_dir / "identity_graph_report.json"
    core_db = state / "out" / canonical_build / "core.duckdb"
    source = policy["sources"][0]
    source_db = state / "derived/evidence_lake_v2/scientific_evidence" / source["evidence_build_id"] / "scientific_evidence.duckdb"
    required = (identity_db, identity_report, core_db, source_db)
    if not all(path.is_file() for path in required):
        raise FileNotFoundError(f"missing cluster compiler input: {[str(path) for path in required if not path.is_file()]}")
    identity_report_sha = sha256_file(identity_report)
    build_id = sha256_bytes(canonical_json({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "identity_graph_id": identity_id,
        "identity_report_sha256": identity_report_sha,
        "canonical_reference_build_id": canonical_build,
        "source_build_id": source["evidence_build_id"],
    }))[:24]
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / build_id
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        database = staging / "selected_clusters.duckdb"
        con = duckdb.connect(str(database))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        spill = staging / "spill"
        spill.mkdir()
        con.execute(f"SET temp_directory={sql_literal(spill)}")
        con.execute(f"ATTACH {sql_literal(identity_db)} AS identity (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(core_db)} AS core (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(source_db)} AS source (READ_ONLY)")
        create_schema(con)
        source_report = compile_source(con, source=source, policy_version=policy["policy_version"])
        checks = verify(con)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?,?)",
            [build_id, policy["policy_version"], policy_sha, identity_id, canonical_build, utc_now(), "pass"],
        )
        con.execute("CHECKPOINT")
        parquet = staging / "parquet"
        parquet.mkdir()
        exports = (
            ("cluster_identity_bindings", "cluster_binding_id"),
            ("cluster_evidence_projection", "evidence_id"),
            ("cluster_membership_endpoint_bindings", "membership_binding_id"),
            ("cluster_membership_projection", "evidence_id"),
        )
        for table, order_key in exports:
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
            "schema_version": "spacegate.e5_selected_clusters.v1",
            "build_id": build_id,
            "policy_version": policy["policy_version"],
            "policy_sha256": policy_sha,
            "compiler_version": policy["compiler_version"],
            "compiler_sha256": compiler_sha,
            "identity_graph_id": identity_id,
            "identity_report_sha256": identity_report_sha,
            "canonical_reference_build_id": canonical_build,
            "source_reports": [source_report],
            "verification": checks,
            "deterministic_files": deterministic_files,
            "generated_at": utc_now(),
            "status": "pass",
        }
        write_json(staging / "manifest.json", manifest)
        if destination.exists():
            existing = read_json(destination / "manifest.json")
            comparable = (
                "policy_sha256", "identity_graph_id", "identity_report_sha256",
                "canonical_reference_build_id", "source_reports", "verification",
                "deterministic_files",
            )
            if any(existing.get(key) != manifest.get(key) for key in comparable):
                raise ValueError(f"deterministic cluster build differs: {build_id}")
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
    output_root = args.output_root or args.state / "derived/evidence_lake_v2/selected_clusters"
    report_path = args.report or args.state / "reports/evidence_lake_v2/e5_selected_cluster_report.json"
    report = compile_clusters(policy_path=args.policy, state=args.state, output_root=output_root, report_path=report_path)
    print(f"Selected cluster evidence pass: build={report['build_id']} wall={report['wall_seconds']}s")


if __name__ == "__main__":
    main()
