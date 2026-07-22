#!/usr/bin/env python3
"""Compile clean multi-release cluster identity, context, and membership evidence."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_clusters.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT = Path("/mnt/space/spacegate/e7-clean-clusters")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(canonical_json(value) + b"\n")
    os.replace(temporary, path)


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_list(values: list[str]) -> str:
    return ",".join(sql_literal(value) for value in values)


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_clean_cluster_policy.v1":
        raise ValueError("unsupported clean cluster policy schema")
    if policy.get("canonical_containment_promotion") is not False:
        raise ValueError("clean cluster policy may not promote canonical containment")
    if policy.get("designation_normalization") != "lower_trim_space_hyphen_to_underscore_v1":
        raise ValueError("unsupported cluster designation normalization")
    if not policy.get("identity_seed_id") or not policy.get("identity_graph_id"):
        raise ValueError("clean cluster policy lacks permanent identity inputs")
    sources = policy.get("sources") or []
    if len(sources) < 2:
        raise ValueError("clean cluster policy requires current and supplementary sources")
    allowed_bridges = {
        "canonical_identifier_bindings_v1",
        "official_dr2_to_dr3_outcomes_v1",
    }
    for source in sources:
        required = {
            "source_id",
            "release_id",
            "evidence_build_id",
            "designation_namespaces",
            "characterization_tables",
            "member_namespace",
            "member_bridge",
            "cluster_authority",
            "membership_authority",
            "authority_rank",
            "acceptance",
        }
        missing = sorted(required - set(source))
        if missing:
            raise ValueError(f"clean cluster source lacks {missing}")
        if source["member_bridge"] not in allowed_bridges:
            raise ValueError(f"unsupported member bridge: {source['member_bridge']}")


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          build_id VARCHAR, policy_version VARCHAR, policy_sha256 VARCHAR,
          identity_seed_id VARCHAR, identity_graph_id VARCHAR,
          generated_at VARCHAR, status VARCHAR
        );
        CREATE TABLE cluster_identity_bindings (
          cluster_binding_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, cluster_identity_raw VARCHAR,
          source_designations_json JSON, matched_identifier_evidence_json JSON,
          canonical_candidates_json JSON, canonical_candidate_count BIGINT,
          all_scope_candidate_count BIGINT, source_cluster_collision_count BIGINT,
          canonical_extended_object_id BIGINT,
          canonical_cluster_stable_object_key VARCHAR,
          canonical_cluster_name VARCHAR, canonical_object_type VARCHAR,
          binding_status VARCHAR, binding_method VARCHAR,
          binding_reason VARCHAR, authority_rank INTEGER, policy_version VARCHAR
        );
        CREATE TABLE cluster_evidence_projection (
          evidence_id VARCHAR, source_record_id VARCHAR,
          cluster_identity_raw VARCHAR, parameter_set_raw JSON,
          method VARCHAR, model VARCHAR, reference_raw VARCHAR,
          quality_json JSON, normalization_version VARCHAR,
          source_id VARCHAR, release_id VARCHAR, source_table VARCHAR,
          cluster_binding_id VARCHAR, binding_status VARCHAR,
          canonical_extended_object_id BIGINT,
          canonical_cluster_stable_object_key VARCHAR,
          authority_role VARCHAR, authority_rank INTEGER,
          projection_status VARCHAR, projection_reason VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE cluster_membership_endpoint_bindings (
          membership_binding_id VARCHAR, membership_evidence_id VARCHAR,
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, cluster_identity_raw VARCHAR,
          cluster_binding_id VARCHAR, cluster_binding_status VARCHAR,
          canonical_cluster_stable_object_key VARCHAR,
          member_namespace VARCHAR, member_source_identifier VARCHAR,
          upstream_identity_outcome VARCHAR, member_candidate_count BIGINT,
          member_system_candidate_count BIGINT, member_stable_object_key VARCHAR,
          member_system_stable_object_key VARCHAR, member_binding_status VARCHAR,
          binding_method VARCHAR, binding_reason VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE cluster_membership_projection (
          evidence_id VARCHAR, source_record_id VARCHAR,
          cluster_identity_raw VARCHAR, member_identity_raw VARCHAR,
          membership_probability DOUBLE, method VARCHAR, reference_raw VARCHAR,
          quality_json JSON, source_id VARCHAR, release_id VARCHAR,
          membership_binding_id VARCHAR, cluster_binding_id VARCHAR,
          cluster_binding_status VARCHAR,
          canonical_cluster_stable_object_key VARCHAR,
          member_binding_status VARCHAR, member_stable_object_key VARCHAR,
          member_system_stable_object_key VARCHAR, authority_role VARCHAR,
          projection_status VARCHAR, projection_reason VARCHAR,
          canonical_containment_promotion BOOLEAN, policy_version VARCHAR
        );
        """
    )


def create_identity_views(
    con: duckdb.DuckDBPyConnection,
    *,
    seed_dir: Path,
) -> None:
    con.execute(
        f"CREATE VIEW seed_nodes AS SELECT * FROM read_parquet("
        f"{sql_literal(seed_dir / 'extended_identity_nodes.parquet')})"
    )
    con.execute(
        f"CREATE VIEW seed_aliases AS SELECT * FROM read_parquet("
        f"{sql_literal(seed_dir / 'extended_object_aliases.parquet')})"
    )
    con.execute(
        """
        CREATE TEMP TABLE canonical_cluster_aliases AS
        SELECT DISTINCT n.extended_object_id,n.stable_object_key,n.canonical_name,
               n.object_type,a.alias_raw,a.extended_object_alias_id,
               lower(regexp_replace(trim(a.alias_raw), '[ -]+','_','g')) alias_norm
        FROM seed_nodes n JOIN seed_aliases a USING(extended_object_id)
        """
    )


def member_candidate_sql(source: dict[str, Any], alias: str) -> str:
    if source["member_bridge"] == "canonical_identifier_bindings_v1":
        namespace = sql_literal(source["member_namespace"])
        return f"""
          SELECT m.evidence_id membership_evidence_id,m.source_record_id,
                 m.cluster_identity_raw,m.member_identity_raw,
                 CASE WHEN count(DISTINCT i.stable_object_key)=1
                      AND count(DISTINCT i.system_stable_object_key)=1
                      THEN 'accepted'
                      WHEN count(DISTINCT i.stable_object_key)=0
                        OR count(DISTINCT i.system_stable_object_key)=0 THEN 'missing'
                      ELSE 'ambiguous' END upstream_identity_outcome,
                 count(DISTINCT i.stable_object_key) member_candidate_count,
                 count(DISTINCT i.system_stable_object_key) member_system_candidate_count,
                 min(i.stable_object_key) candidate_member_key,
                 min(i.system_stable_object_key) candidate_system_key
          FROM {alias}.cluster_membership_evidence m
          LEFT JOIN identity.canonical_identifier_bindings i
            ON i.namespace={namespace} AND i.id_value_norm=m.member_identity_raw
          GROUP BY m.evidence_id,m.source_record_id,m.cluster_identity_raw,
                   m.member_identity_raw
        """
    return f"""
      SELECT m.evidence_id membership_evidence_id,m.source_record_id,
             m.cluster_identity_raw,m.member_identity_raw,
             coalesce(i.outcome,'missing') upstream_identity_outcome,
             count(DISTINCT i.canonical_stable_object_key)
               FILTER (WHERE i.outcome='accepted') member_candidate_count,
             count(DISTINCT i.canonical_system_stable_object_key)
               FILTER (WHERE i.outcome='accepted') member_system_candidate_count,
             min(i.canonical_stable_object_key)
               FILTER (WHERE i.outcome='accepted') candidate_member_key,
             min(i.canonical_system_stable_object_key)
               FILTER (WHERE i.outcome='accepted') candidate_system_key
      FROM {alias}.cluster_membership_evidence m
      LEFT JOIN identity.dr2_release_outcomes i
        ON i.dr2_source_id=m.member_identity_raw
      GROUP BY m.evidence_id,m.source_record_id,m.cluster_identity_raw,
               m.member_identity_raw,i.outcome
    """


def compile_source(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    alias: str,
    index: int,
    policy_version: str,
    eligible_types: list[str],
) -> dict[str, Any]:
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    prefix = f"s{index}"
    source_id = sql_literal(source["source_id"])
    release_id = sql_literal(source["release_id"])
    evidence_build_id = sql_literal(source["evidence_build_id"])
    policy_sql = sql_literal(policy_version)
    namespaces = sql_list(source["designation_namespaces"])
    characterizations = sql_list(source["characterization_tables"])
    eligible = sql_list(eligible_types)

    con.execute(
        f"""
        CREATE TEMP TABLE {prefix}_cluster_ids AS
        SELECT DISTINCT cluster_identity_raw FROM {alias}.cluster_evidence;

        CREATE TEMP TABLE {prefix}_designations AS
        SELECT DISTINCT e.cluster_identity_raw,i.evidence_id identifier_evidence_id,
               i.identifier_raw,
               lower(regexp_replace(trim(i.identifier_raw), '[ -]+','_','g')) designation_norm
        FROM {alias}.cluster_evidence e
        JOIN {alias}.identifier_claim_evidence i USING(source_record_id)
        WHERE i.namespace IN ({namespaces});

        CREATE TEMP TABLE {prefix}_matches AS
        SELECT DISTINCT d.cluster_identity_raw,d.identifier_evidence_id,
               d.identifier_raw,a.extended_object_id,a.stable_object_key,
               a.canonical_name,a.object_type,
               a.object_type IN ({eligible}) eligible_scope
        FROM {prefix}_designations d
        JOIN canonical_cluster_aliases a
          ON a.alias_norm=d.designation_norm;

        CREATE TEMP TABLE {prefix}_collisions AS
        SELECT stable_object_key,count(DISTINCT cluster_identity_raw) source_cluster_count
        FROM {prefix}_matches WHERE eligible_scope GROUP BY stable_object_key;

        INSERT INTO cluster_identity_bindings
        WITH summaries AS (
          SELECT ids.cluster_identity_raw,
                 count(DISTINCT m.stable_object_key) FILTER (WHERE m.eligible_scope)
                   canonical_candidate_count,
                 count(DISTINCT m.stable_object_key) all_scope_candidate_count,
                 min(m.extended_object_id) FILTER (WHERE m.eligible_scope) candidate_id,
                 min(m.stable_object_key) FILTER (WHERE m.eligible_scope) candidate_key,
                 min(m.canonical_name) FILTER (WHERE m.eligible_scope) candidate_name,
                 min(m.object_type) FILTER (WHERE m.eligible_scope) candidate_type,
                 coalesce(to_json(list(DISTINCT d.identifier_raw ORDER BY d.identifier_raw)
                   FILTER (WHERE d.identifier_raw IS NOT NULL)),'[]'::JSON)
                   source_designations_json,
                 coalesce(to_json(list(DISTINCT m.identifier_evidence_id
                   ORDER BY m.identifier_evidence_id)
                   FILTER (WHERE m.identifier_evidence_id IS NOT NULL)),'[]'::JSON)
                   matched_identifier_evidence_json,
                 coalesce(to_json(list(DISTINCT m.stable_object_key
                   ORDER BY m.stable_object_key)
                   FILTER (WHERE m.stable_object_key IS NOT NULL)),'[]'::JSON)
                   canonical_candidates_json
          FROM {prefix}_cluster_ids ids
          LEFT JOIN {prefix}_designations d USING(cluster_identity_raw)
          LEFT JOIN {prefix}_matches m
            ON m.cluster_identity_raw=ids.cluster_identity_raw
           AND m.identifier_evidence_id=d.identifier_evidence_id
          GROUP BY ids.cluster_identity_raw
        ), classified AS (
          SELECT s.*,coalesce(c.source_cluster_count,0) source_cluster_collision_count
          FROM summaries s LEFT JOIN {prefix}_collisions c
            ON c.stable_object_key=s.candidate_key
        )
        SELECT sha256(concat_ws('|',{source_id},cluster_identity_raw,'cluster',{policy_sql})),
               {source_id},{release_id},{evidence_build_id},cluster_identity_raw,
               source_designations_json,matched_identifier_evidence_json,
               canonical_candidates_json,canonical_candidate_count,
               all_scope_candidate_count,source_cluster_collision_count,
               CASE WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                    THEN candidate_id END,
               CASE WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                    THEN candidate_key END,
               CASE WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                    THEN candidate_name END,
               CASE WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                    THEN candidate_type END,
               CASE WHEN canonical_candidate_count=1 AND source_cluster_collision_count=1
                      THEN 'accepted'
                    WHEN canonical_candidate_count=0 AND all_scope_candidate_count>0
                      THEN 'scope_conflict'
                    WHEN canonical_candidate_count=0 THEN 'missing'
                    ELSE 'ambiguous' END,
               'exact_release_designation_to_permanent_extended_alias_v1',
               CASE WHEN canonical_candidate_count=0 AND all_scope_candidate_count>0
                      THEN 'designation resolves only to an ineligible canonical object scope'
                    WHEN canonical_candidate_count=0
                      THEN 'no exact designation matches permanent extended identity'
                    WHEN canonical_candidate_count>1
                      THEN 'designations resolve to multiple eligible canonical clusters'
                    WHEN source_cluster_collision_count>1
                      THEN 'multiple release-scoped source clusters claim one canonical cluster'
                    ELSE 'one release cluster resolves one-to-one to permanent cluster identity' END,
               {int(source['authority_rank'])},{policy_sql}
        FROM classified;

        INSERT INTO cluster_evidence_projection
        SELECT e.evidence_id,e.source_record_id,e.cluster_identity_raw,e.parameter_set_raw,
               e.method,e.model,e.reference_raw,e.quality_json,e.normalization_version,
               {source_id},{release_id},r.source_table,b.cluster_binding_id,b.binding_status,
               b.canonical_extended_object_id,b.canonical_cluster_stable_object_key,
               {sql_literal(source['cluster_authority'])},{int(source['authority_rank'])},
               CASE WHEN b.binding_status='accepted' AND r.source_table IN ({characterizations})
                      THEN 'eligible_for_quantity_selection'
                    WHEN b.binding_status='accepted' THEN 'identity_context_evidence'
                    ELSE b.binding_status || '_identity_evidence' END,
               CASE WHEN b.binding_status='accepted' AND r.source_table IN ({characterizations})
                      THEN 'coherent source characterization on one accepted cluster identity'
                    WHEN b.binding_status='accepted'
                      THEN 'source identity context on one accepted cluster identity'
                    ELSE b.binding_reason END,{policy_sql}
        FROM {alias}.cluster_evidence e
        JOIN {alias}.source_records r USING(source_record_id)
        JOIN cluster_identity_bindings b
          ON b.source_id={source_id} AND b.cluster_identity_raw=e.cluster_identity_raw;

        INSERT INTO cluster_membership_endpoint_bindings
        WITH candidates AS ({member_candidate_sql(source, alias)})
        SELECT sha256(concat_ws('|',{source_id},c.membership_evidence_id,
                      'membership',{policy_sql})),
               c.membership_evidence_id,c.source_record_id,{source_id},{release_id},
               {evidence_build_id},c.cluster_identity_raw,b.cluster_binding_id,
               b.binding_status,b.canonical_cluster_stable_object_key,
               {sql_literal(source['member_namespace'])},c.member_identity_raw,
               c.upstream_identity_outcome,c.member_candidate_count,
               c.member_system_candidate_count,
               CASE WHEN c.upstream_identity_outcome='accepted'
                          AND c.member_candidate_count=1
                          AND c.member_system_candidate_count=1
                    THEN c.candidate_member_key END,
               CASE WHEN c.upstream_identity_outcome='accepted'
                          AND c.member_candidate_count=1
                          AND c.member_system_candidate_count=1
                    THEN c.candidate_system_key END,
               CASE WHEN c.upstream_identity_outcome='accepted'
                          AND c.member_candidate_count=1
                          AND c.member_system_candidate_count=1 THEN 'accepted'
                    WHEN c.upstream_identity_outcome IN ('excluded','ambiguous','missing')
                      THEN c.upstream_identity_outcome
                    WHEN c.member_candidate_count=0 OR c.member_system_candidate_count=0
                      THEN 'missing' ELSE 'ambiguous' END,
               {sql_literal(source['member_bridge'])},
               CASE WHEN c.upstream_identity_outcome='accepted'
                          AND c.member_candidate_count=1
                          AND c.member_system_candidate_count=1
                      THEN 'release-scoped identifier resolves to one canonical star and system'
                    WHEN c.upstream_identity_outcome='excluded'
                      THEN 'official release bridge excludes this member endpoint'
                    WHEN c.upstream_identity_outcome='ambiguous'
                      THEN 'official release bridge leaves this member endpoint ambiguous'
                    ELSE 'member endpoint is absent from the accepted canonical reference' END,
               {policy_sql}
        FROM candidates c JOIN cluster_identity_bindings b
          ON b.source_id={source_id} AND b.cluster_identity_raw=c.cluster_identity_raw;

        INSERT INTO cluster_membership_projection
        SELECT m.evidence_id,m.source_record_id,m.cluster_identity_raw,
               m.member_identity_raw,m.membership_probability,m.method,m.reference_raw,
               m.quality_json,{source_id},{release_id},b.membership_binding_id,
               b.cluster_binding_id,b.cluster_binding_status,
               b.canonical_cluster_stable_object_key,b.member_binding_status,
               b.member_stable_object_key,b.member_system_stable_object_key,
               {sql_literal(source['membership_authority'])},
               CASE WHEN b.cluster_binding_status='accepted'
                          AND b.member_binding_status='accepted'
                      THEN 'probability_bearing_membership_evidence'
                    WHEN b.cluster_binding_status<>'accepted'
                      THEN b.cluster_binding_status || '_cluster_identity_evidence'
                    ELSE b.member_binding_status || '_member_identity_evidence' END,
               CASE WHEN b.cluster_binding_status='accepted'
                          AND b.member_binding_status='accepted'
                      THEN 'source probability with independently accepted endpoints; not containment'
                    WHEN b.cluster_binding_status<>'accepted'
                      THEN 'cluster endpoint is not uniquely accepted'
                    ELSE b.binding_reason END,
               false,{policy_sql}
        FROM {alias}.cluster_membership_evidence m
        JOIN cluster_membership_endpoint_bindings b
          ON b.source_id={source_id} AND b.membership_evidence_id=m.evidence_id;
        """
    )

    cluster_counts = dict(
        con.execute(
            "SELECT binding_status,count(*) FROM cluster_identity_bindings "
            "WHERE source_id=? GROUP BY 1",
            [source["source_id"]],
        ).fetchall()
    )
    member_counts = dict(
        con.execute(
            "SELECT member_binding_status,count(*) FROM cluster_membership_endpoint_bindings "
            "WHERE source_id=? GROUP BY 1",
            [source["source_id"]],
        ).fetchall()
    )
    observed = {
        "cluster_bindings": sum(cluster_counts.values()),
        "clusters_accepted": cluster_counts.get("accepted", 0),
        "clusters_missing": cluster_counts.get("missing", 0),
        "clusters_ambiguous": cluster_counts.get("ambiguous", 0),
        "clusters_scope_conflict": cluster_counts.get("scope_conflict", 0),
        "cluster_evidence": int(con.execute(
            "SELECT count(*) FROM cluster_evidence_projection WHERE source_id=?",
            [source["source_id"]],
        ).fetchone()[0]),
        "cluster_characterizations_eligible": int(con.execute(
            "SELECT count(*) FROM cluster_evidence_projection WHERE source_id=? "
            "AND projection_status='eligible_for_quantity_selection'",
            [source["source_id"]],
        ).fetchone()[0]),
        "memberships": sum(member_counts.values()),
        "member_endpoints_accepted": member_counts.get("accepted", 0),
        "member_endpoints_missing": member_counts.get("missing", 0),
        "member_endpoints_ambiguous": member_counts.get("ambiguous", 0),
        "member_endpoints_excluded": member_counts.get("excluded", 0),
    }
    if observed != source["acceptance"]:
        raise ValueError(
            f"clean cluster acceptance changed for {source['source_id']}: "
            f"expected={source['acceptance']}:observed={observed}"
        )
    return {
        "source_id": source["source_id"],
        "observed": observed,
        "wall_seconds": round(time.perf_counter() - started_wall, 3),
        "cpu_seconds": round(time.process_time() - started_cpu, 3),
    }


def verify(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    checks = {
        "duplicate_cluster_binding_ids": "SELECT count(*)-count(DISTINCT cluster_binding_id) FROM cluster_identity_bindings",
        "duplicate_membership_binding_ids": "SELECT count(*)-count(DISTINCT membership_binding_id) FROM cluster_membership_endpoint_bindings",
        "accepted_clusters_without_targets": "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status='accepted' AND canonical_cluster_stable_object_key IS NULL",
        "unaccepted_clusters_with_targets": "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status<>'accepted' AND canonical_cluster_stable_object_key IS NOT NULL",
        "accepted_cluster_collisions_within_release": "SELECT count(*) FROM (SELECT source_id,canonical_cluster_stable_object_key FROM cluster_identity_bindings WHERE binding_status='accepted' GROUP BY 1,2 HAVING count(*)<>1)",
        "eligible_unbound_cluster_context": "SELECT count(*) FROM cluster_evidence_projection WHERE projection_status='eligible_for_quantity_selection' AND canonical_cluster_stable_object_key IS NULL",
        "accepted_members_without_targets": "SELECT count(*) FROM cluster_membership_endpoint_bindings WHERE member_binding_status='accepted' AND (member_stable_object_key IS NULL OR member_system_stable_object_key IS NULL)",
        "unaccepted_members_with_targets": "SELECT count(*) FROM cluster_membership_endpoint_bindings WHERE member_binding_status<>'accepted' AND (member_stable_object_key IS NOT NULL OR member_system_stable_object_key IS NOT NULL)",
        "membership_probability_outside_unit_interval": "SELECT count(*) FROM cluster_membership_projection WHERE membership_probability IS NULL OR membership_probability<0 OR membership_probability>1",
        "canonical_containment_rows": "SELECT count(*) FROM cluster_membership_projection WHERE canonical_containment_promotion",
    }
    result = {name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()}
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"clean cluster verification failed: {failing}")
    return result


def compile_clusters(
    *,
    policy_path: Path,
    state: Path,
    output_root: Path,
    report_path: Path,
) -> dict[str, Any]:
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    policy = read_json(policy_path)
    validate_policy(policy)
    policy_sha = stable_hash(policy)
    compiler_sha = sha256_file(Path(__file__).resolve())
    seed_id = str(policy["identity_seed_id"])
    graph_id = str(policy["identity_graph_id"])
    seed_dir = state / "derived/evidence_lake_v2/extended_identity_seed" / seed_id
    seed_manifest = seed_dir / "manifest.json"
    identity_db = state / "derived/evidence_lake_v2/identity" / graph_id / "identity_graph.duckdb"
    source_paths = [
        state / "derived/evidence_lake_v2/scientific_evidence"
        / source["evidence_build_id"] / "scientific_evidence.duckdb"
        for source in policy["sources"]
    ]
    required = [seed_manifest, identity_db, *source_paths]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing clean cluster input: {missing}")
    inputs = {
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "identity_seed_id": seed_id,
        "identity_seed_manifest_sha256": sha256_file(seed_manifest),
        "identity_graph_id": graph_id,
        "identity_graph_sha256": sha256_file(identity_db),
        "source_databases": [
            {"build_id": source["evidence_build_id"], "sha256": sha256_file(path)}
            for source, path in zip(policy["sources"], source_paths, strict=True)
        ],
    }
    build_id = stable_hash(inputs)[:24]
    destination = output_root / build_id
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    phase_timings: list[dict[str, Any]] = []
    try:
        database = staging / "clean_clusters.duckdb"
        con = duckdb.connect(str(database))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        spill = staging / "spill"
        spill.mkdir()
        con.execute(f"SET temp_directory={sql_literal(spill)}")
        con.execute(f"ATTACH {sql_literal(identity_db)} AS identity (READ_ONLY)")
        for index, path in enumerate(source_paths):
            con.execute(f"ATTACH {sql_literal(path)} AS src{index} (READ_ONLY)")
        create_schema(con)
        create_identity_views(con, seed_dir=seed_dir)
        source_reports = []
        for index, source in enumerate(policy["sources"]):
            report = compile_source(
                con,
                source=source,
                alias=f"src{index}",
                index=index,
                policy_version=policy["policy_version"],
                eligible_types=policy["eligible_object_types"],
            )
            source_reports.append({
                "source_id": report["source_id"],
                "observed": report["observed"],
            })
            phase_timings.append({"phase": f"compile_{source['source_id']}", **{
                "wall_seconds": report["wall_seconds"],
                "cpu_seconds": report["cpu_seconds"],
            }})
        checks = verify(con)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?,?)",
            [build_id, policy["policy_version"], policy_sha, seed_id, graph_id, utc_now(), "pass"],
        )
        con.execute("CHECKPOINT")
        parquet = staging / "parquet"
        parquet.mkdir()
        export_wall = time.perf_counter()
        export_cpu = time.process_time()
        for table, order_key in (
            ("cluster_identity_bindings", "source_id,cluster_binding_id"),
            ("cluster_evidence_projection", "source_id,evidence_id"),
            ("cluster_membership_endpoint_bindings", "source_id,membership_binding_id"),
            ("cluster_membership_projection", "source_id,evidence_id"),
        ):
            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order_key}) TO "
                f"{sql_literal(parquet / (table + '.parquet'))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        phase_timings.append({
            "phase": "export_parquet",
            "wall_seconds": round(time.perf_counter() - export_wall, 3),
            "cpu_seconds": round(time.process_time() - export_cpu, 3),
        })
        con.close()
        shutil.rmtree(spill)
        files = {
            str(path.relative_to(staging)): {
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in sorted(staging.rglob("*")) if path.is_file()
        }
        deterministic_files = {
            name: value for name, value in files.items() if name.startswith("parquet/")
        }
        manifest = {
            "schema_version": "spacegate.e7_clean_clusters.v1",
            "build_id": build_id,
            "policy_version": policy["policy_version"],
            "policy_sha256": policy_sha,
            "compiler_version": policy["compiler_version"],
            "compiler_sha256": compiler_sha,
            "identity_seed_id": seed_id,
            "identity_graph_id": graph_id,
            "stability_databases_opened": [],
            "source_reports": source_reports,
            "verification": checks,
            "phase_timings": phase_timings,
            "deterministic_files": deterministic_files,
            "generated_at": utc_now(),
            "status": "pass",
        }
        write_json(staging / "manifest.json", manifest)
        if destination.exists():
            existing = read_json(destination / "manifest.json")
            for key in (
                "policy_sha256",
                "identity_seed_id",
                "identity_graph_id",
                "source_reports",
                "verification",
                "deterministic_files",
            ):
                if existing.get(key) != manifest.get(key):
                    raise ValueError(f"deterministic clean cluster build differs: {build_id}")
            shutil.rmtree(staging)
        else:
            os.replace(staging, destination)
        result = {
            **manifest,
            "artifact_path": str(destination),
            "wall_seconds": round(time.perf_counter() - started_wall, 3),
            "cpu_seconds": round(time.process_time() - started_cpu, 3),
            "artifact_bytes": sum(row["bytes"] for row in files.values()),
        }
        write_json(report_path, result)
        return result
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report_path = args.report or args.state / "reports/evidence_lake_v2/e7_clean_clusters_compile.json"
    report = compile_clusters(
        policy_path=args.policy,
        state=args.state,
        output_root=args.output_root,
        report_path=report_path,
    )
    print(
        f"Clean cluster evidence pass: build={report['build_id']} "
        f"wall={report['wall_seconds']}s"
    )


if __name__ == "__main__":
    main()
