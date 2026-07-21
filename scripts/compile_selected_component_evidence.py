#!/usr/bin/env python3
"""Compile release-scoped MSC and DEBCat component evidence projections."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_component_scope_policies.json"
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
    if policy.get("schema_version") != "spacegate.e5_component_scope_policies.v1":
        raise ValueError("unsupported component-scope policy schema")
    for source_name in ("msc", "debcat"):
        if source_name not in policy:
            raise ValueError(f"missing component-scope source policy: {source_name}")
        if policy[source_name].get("canonical_containment_promotion") is not False:
            raise ValueError(f"component policy may not promote containment: {source_name}")
    if policy["debcat"].get("period_unit") != "d":
        raise ValueError("DEBCat bridge currently requires source-native MSC day periods")


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          build_id VARCHAR, policy_version VARCHAR, policy_sha256 VARCHAR,
          identity_graph_id VARCHAR, canonical_reference_build_id VARCHAR,
          generated_at VARCHAR, status VARCHAR
        );
        CREATE TABLE msc_system_bindings (
          system_binding_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, wds_id_raw VARCHAR,
          canonical_candidate_count BIGINT, canonical_system_id VARCHAR,
          canonical_system_stable_object_key VARCHAR, canonical_system_display_name VARCHAR,
          identity_graph_system_candidate_count BIGINT, identity_graph_binding_status VARCHAR,
          binding_status VARCHAR, binding_method VARCHAR, binding_reason VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE msc_component_entities (
          component_entity_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, source_component_raw VARCHAR,
          wds_id_raw VARCHAR, component_label_raw VARCHAR, component_label_normalized VARCHAR,
          system_binding_id VARCHAR, canonical_system_stable_object_key VARCHAR,
          source_component_key VARCHAR, binding_status VARCHAR,
          binding_method VARCHAR, binding_reason VARCHAR, scope_semantics VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE msc_relation_evidence_projection (
          projected_relation_id VARCHAR, relation_evidence_id VARCHAR,
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          evidence_build_id VARCHAR, left_component_entity_id VARCHAR,
          right_component_entity_id VARCHAR, left_source_component_key VARCHAR,
          right_source_component_key VARCHAR, canonical_system_stable_object_key VARCHAR,
          source_orbit_evidence_id VARCHAR, period_value_raw VARCHAR, period_value DOUBLE,
          period_unit_raw VARCHAR, relation_kind VARCHAR, relation_scope VARCHAR,
          evidence_polarity VARCHAR, method VARCHAR, reference_raw VARCHAR,
          quality_json JSON, projection_status VARCHAR, projection_reason VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE debcat_system_bindings (
          system_binding_id VARCHAR, source_record_id VARCHAR, identifier_evidence_id VARCHAR,
          source_id VARCHAR, release_id VARCHAR, evidence_build_id VARCHAR,
          system_name_raw VARCHAR, system_name_normalized VARCHAR,
          period_days DOUBLE, best_term_priority INTEGER, canonical_candidate_count BIGINT,
          canonical_system_id VARCHAR, canonical_system_stable_object_key VARCHAR,
          canonical_system_display_name VARCHAR, canonical_wds_id VARCHAR,
          binding_status VARCHAR, binding_method VARCHAR, binding_reason VARCHAR,
          policy_version VARCHAR
        );
        CREATE TABLE debcat_relation_bindings (
          relation_binding_id VARCHAR, source_record_id VARCHAR, system_binding_id VARCHAR,
          source_id VARCHAR, release_id VARCHAR, evidence_build_id VARCHAR,
          debcat_period_days DOUBLE, tolerance_days DOUBLE, relation_candidate_count BIGINT,
          msc_projected_relation_id VARCHAR, msc_relation_evidence_id VARCHAR,
          primary_component_entity_id VARCHAR, secondary_component_entity_id VARCHAR,
          primary_source_component_key VARCHAR, secondary_source_component_key VARCHAR,
          canonical_system_stable_object_key VARCHAR, msc_period_days DOUBLE,
          period_delta_days DOUBLE, binding_status VARCHAR, binding_method VARCHAR,
          binding_reason VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE debcat_parameter_set_bindings (
          parameter_set_binding_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
          source_id VARCHAR, release_id VARCHAR, evidence_build_id VARCHAR,
          component_scope VARCHAR, target_scope VARCHAR, target_key VARCHAR,
          canonical_system_stable_object_key VARCHAR, relation_binding_id VARCHAR,
          binding_status VARCHAR, binding_reason VARCHAR, policy_version VARCHAR
        );
        """
    )


def compile_msc(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    policy_version: str,
) -> dict[str, Any]:
    source_id = sql_literal(source["source_id"])
    release_id = sql_literal(source["release_id"])
    evidence_build_id = sql_literal(source["evidence_build_id"])
    policy_sql = sql_literal(policy_version)
    method = sql_literal(source["system_binding_method"])
    namespace = sql_literal(source["component_namespace"])

    con.execute(
        f"""
        CREATE TEMP TABLE msc_component_subjects AS
        SELECT DISTINCT source_component_raw,
               split_part(trim(source_component_raw), ':', 1) wds_id_raw,
               split_part(trim(source_component_raw), ':', 2) component_label_raw
        FROM (
          SELECT left_identity_raw source_component_raw
          FROM msc.relation_claim_evidence
          WHERE left_identity_namespace={namespace}
          UNION ALL
          SELECT right_identity_raw
          FROM msc.relation_claim_evidence
          WHERE right_identity_namespace={namespace}
        );

        CREATE TEMP TABLE msc_system_subjects AS
        SELECT DISTINCT wds_id_raw FROM msc_component_subjects;

        CREATE TEMP TABLE msc_canonical_system_candidates AS
        SELECT s.wds_id_raw, c.system_id::VARCHAR canonical_system_id,
               c.stable_object_key canonical_system_stable_object_key,
               c.system_name canonical_system_display_name
        FROM msc_system_subjects s
        JOIN core.systems c ON c.wds_id=s.wds_id_raw;

        CREATE TEMP TABLE msc_identity_system_candidates AS
        SELECT s.wds_id_raw, b.system_stable_object_key
        FROM msc_system_subjects s
        JOIN identity.canonical_identifier_bindings b
          ON b.namespace='wds' AND b.id_value_raw=s.wds_id_raw;

        INSERT INTO msc_system_bindings
        WITH canonical AS (
          SELECT wds_id_raw, count(DISTINCT canonical_system_stable_object_key) candidate_count,
                 min(canonical_system_id) canonical_system_id,
                 min(canonical_system_stable_object_key) canonical_system_stable_object_key,
                 min(canonical_system_display_name) canonical_system_display_name
          FROM msc_canonical_system_candidates GROUP BY 1
        ), graph AS (
          SELECT wds_id_raw, count(DISTINCT system_stable_object_key) candidate_count
          FROM msc_identity_system_candidates GROUP BY 1
        )
        SELECT sha256(concat_ws('|',{source_id},{release_id},s.wds_id_raw,{policy_sql})),
               {source_id},{release_id},{evidence_build_id},s.wds_id_raw,
               coalesce(c.candidate_count,0),
               CASE WHEN c.candidate_count=1 THEN c.canonical_system_id END,
               CASE WHEN c.candidate_count=1 THEN c.canonical_system_stable_object_key END,
               CASE WHEN c.candidate_count=1 THEN c.canonical_system_display_name END,
               coalesce(g.candidate_count,0),
               CASE WHEN coalesce(g.candidate_count,0)=0 THEN 'missing'
                    WHEN g.candidate_count=1 THEN 'accepted' ELSE 'ambiguous' END,
               CASE WHEN coalesce(c.candidate_count,0)=0 THEN 'missing'
                    WHEN c.candidate_count=1 THEN 'accepted' ELSE 'ambiguous' END,
               {method},
               CASE WHEN coalesce(c.candidate_count,0)=0
                      THEN 'exact WDS identifier is outside the canonical reference systems'
                    WHEN c.candidate_count=1
                      THEN 'exact punctuation-preserving WDS identifier resolves to one canonical system'
                    ELSE 'exact WDS identifier resolves to multiple canonical systems' END,
               {policy_sql}
        FROM msc_system_subjects s
        LEFT JOIN canonical c USING(wds_id_raw)
        LEFT JOIN graph g USING(wds_id_raw);

        INSERT INTO msc_component_entities
        SELECT sha256(concat_ws('|',{source_id},{release_id},s.source_component_raw,{policy_sql})),
               {source_id},{release_id},{evidence_build_id},s.source_component_raw,
               s.wds_id_raw,s.component_label_raw,lower(s.component_label_raw),
               b.system_binding_id,b.canonical_system_stable_object_key,
               CASE WHEN b.binding_status='accepted'
                          AND regexp_full_match(trim(s.source_component_raw),'[^:]+:[^:]+')
                    THEN concat_ws(':','comp','msc',{release_id},lower(s.wds_id_raw),lower(s.component_label_raw)) END,
               CASE WHEN NOT regexp_full_match(trim(s.source_component_raw),'[^:]+:[^:]+')
                      THEN 'excluded'
                    ELSE b.binding_status END,
               {method},
               CASE WHEN NOT regexp_full_match(trim(s.source_component_raw),'[^:]+:[^:]+')
                      THEN 'invalid release-native MSC component identity'
                    WHEN b.binding_status='accepted'
                      THEN 'release-scoped source component anchored to one canonical WDS system'
                    ELSE b.binding_reason END,
               'source-defined component or subsystem; not a canonical star or containment assertion',
               {policy_sql}
        FROM msc_component_subjects s
        JOIN msc_system_bindings b USING(wds_id_raw);

        INSERT INTO msc_relation_evidence_projection
        WITH source_orbits AS (
          SELECT source_record_id,min(evidence_id) source_orbit_evidence_id,
                 min(json_extract_string(parameter_set_raw,'$.P')) period_value_raw,
                 min(try_cast(json_extract_string(parameter_set_raw,'$.P') AS DOUBLE)) period_value,
                 min(json_extract_string(parameter_set_raw,'$.Punit')) period_unit_raw
          FROM msc.orbital_solution_evidence
          WHERE json_extract_string(parameter_set_raw,'$.P') IS NOT NULL
          GROUP BY 1
        )
        SELECT sha256(concat_ws('|',{source_id},r.evidence_id,{policy_sql})),
               r.evidence_id,r.source_record_id,{source_id},{release_id},{evidence_build_id},
               l.component_entity_id,rr.component_entity_id,l.source_component_key,
               rr.source_component_key,
               CASE WHEN l.binding_status='accepted' AND rr.binding_status='accepted'
                          AND l.canonical_system_stable_object_key=rr.canonical_system_stable_object_key
                    THEN l.canonical_system_stable_object_key END,
               o.source_orbit_evidence_id,o.period_value_raw,o.period_value,o.period_unit_raw,
               r.relation_kind,r.relation_scope,r.evidence_polarity,r.method,r.reference_raw,r.quality_json,
               CASE WHEN trim(r.left_identity_raw)=trim(r.right_identity_raw)
                      THEN 'invalid_self_relation_evidence'
                    WHEN l.binding_status='accepted' AND rr.binding_status='accepted'
                          AND l.canonical_system_stable_object_key=rr.canonical_system_stable_object_key
                      THEN 'accepted_relation_evidence'
                    ELSE 'unresolved_endpoint_evidence' END,
               CASE WHEN trim(r.left_identity_raw)=trim(r.right_identity_raw)
                      THEN 'source relation has identical release-native endpoints'
                    WHEN l.binding_status<>'accepted' OR rr.binding_status<>'accepted'
                      THEN 'one or both release-scoped MSC endpoints are unresolved'
                    WHEN l.canonical_system_stable_object_key<>rr.canonical_system_stable_object_key
                      THEN 'MSC endpoints resolve to different canonical systems'
                    ELSE 'both source-defined endpoints resolve within one canonical WDS system' END,
               {policy_sql}
        FROM msc.relation_claim_evidence r
        JOIN msc_component_entities l ON l.source_component_raw=r.left_identity_raw
        JOIN msc_component_entities rr ON rr.source_component_raw=r.right_identity_raw
        LEFT JOIN source_orbits o USING(source_record_id);
        """
    )
    system_counts = dict(con.execute("SELECT binding_status,count(*) FROM msc_system_bindings GROUP BY 1").fetchall())
    identity_counts = dict(con.execute("SELECT identity_graph_binding_status,count(*) FROM msc_system_bindings GROUP BY 1").fetchall())
    component_counts = dict(con.execute("SELECT binding_status,count(*) FROM msc_component_entities GROUP BY 1").fetchall())
    relation_counts = dict(con.execute("SELECT projection_status,count(*) FROM msc_relation_evidence_projection GROUP BY 1").fetchall())
    observed = {
        "system_bindings": sum(system_counts.values()),
        "systems_accepted": system_counts.get("accepted", 0),
        "systems_missing": system_counts.get("missing", 0),
        "systems_ambiguous": system_counts.get("ambiguous", 0),
        "identity_graph_systems_accepted": identity_counts.get("accepted", 0),
        "identity_graph_systems_missing": identity_counts.get("missing", 0),
        "identity_graph_systems_ambiguous": identity_counts.get("ambiguous", 0),
        "component_entities": sum(component_counts.values()),
        "components_accepted": component_counts.get("accepted", 0),
        "components_missing": component_counts.get("missing", 0),
        "components_ambiguous": component_counts.get("ambiguous", 0),
        "relation_claims": sum(relation_counts.values()),
        "relations_accepted": relation_counts.get("accepted_relation_evidence", 0),
        "relations_unresolved": relation_counts.get("unresolved_endpoint_evidence", 0),
        "relations_invalid_self": relation_counts.get("invalid_self_relation_evidence", 0),
    }
    expected = {key.removeprefix("expected_"): int(value) for key, value in source["acceptance"].items()}
    if observed != expected:
        raise ValueError(f"MSC acceptance counts changed: expected={expected}:observed={observed}")
    return {"source_id": source["source_id"], "observed": observed, "expected": expected}


def compile_debcat(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    policy_version: str,
) -> dict[str, Any]:
    source_id = sql_literal(source["source_id"])
    release_id = sql_literal(source["release_id"])
    evidence_build_id = sql_literal(source["evidence_build_id"])
    policy_sql = sql_literal(policy_version)
    name_method = sql_literal(source["name_binding_method"])
    relation_method = sql_literal(source["relation_binding_method"])
    absolute_tolerance = float(source["period_absolute_tolerance_days"])
    relative_tolerance = float(source["period_relative_tolerance"])
    period_unit = sql_literal(source["period_unit"])

    con.execute(
        f"""
        CREATE TEMP TABLE debcat_subjects AS
        SELECT i.source_record_id,i.evidence_id identifier_evidence_id,
               i.identifier_raw system_name_raw,
               lower(trim(regexp_replace(regexp_replace(i.identifier_raw,
                 '[^0-9A-Za-z]+',' ','g'),'\\s+',' ','g'))) system_name_normalized,
               try_cast(json_extract_string(o.parameter_set_raw,'$.period_days_raw') AS DOUBLE) period_days
        FROM debcat.identifier_claim_evidence i
        JOIN debcat.orbital_solution_evidence o USING(source_record_id)
        WHERE i.namespace='debcat_system_name';

        CREATE TEMP TABLE debcat_name_candidates AS
        SELECT d.*,t.system_id,t.term_priority,
               min(t.term_priority) OVER(PARTITION BY d.source_record_id) best_term_priority
        FROM debcat_subjects d
        LEFT JOIN core.system_search_terms t ON t.term_norm=d.system_name_normalized;

        INSERT INTO debcat_system_bindings
        WITH resolved AS (
          SELECT source_record_id,min(identifier_evidence_id) identifier_evidence_id,
                 min(system_name_raw) system_name_raw,
                 min(system_name_normalized) system_name_normalized,
                 min(period_days) period_days,min(best_term_priority) best_term_priority,
                 count(DISTINCT system_id) FILTER(WHERE term_priority=best_term_priority) candidate_count,
                 min(system_id) FILTER(WHERE term_priority=best_term_priority) canonical_system_id
          FROM debcat_name_candidates GROUP BY 1
        )
        SELECT sha256(concat_ws('|',{source_id},r.source_record_id,{policy_sql})),
               r.source_record_id,r.identifier_evidence_id,{source_id},{release_id},{evidence_build_id},
               r.system_name_raw,r.system_name_normalized,r.period_days,r.best_term_priority,
               r.candidate_count,
               CASE WHEN r.candidate_count=1 THEN r.canonical_system_id::VARCHAR END,
               CASE WHEN r.candidate_count=1 THEN s.stable_object_key END,
               CASE WHEN r.candidate_count=1 THEN s.system_name END,
               CASE WHEN r.candidate_count=1 THEN s.wds_id END,
               CASE WHEN r.candidate_count=0 THEN 'missing'
                    WHEN r.candidate_count=1 THEN 'accepted' ELSE 'ambiguous' END,
               {name_method},
               CASE WHEN r.candidate_count=0 THEN 'no exact canonical search term match'
                    WHEN r.candidate_count=1
                      THEN 'one canonical system at the best exact search-term priority'
                    ELSE 'multiple canonical systems at the best exact search-term priority' END,
               {policy_sql}
        FROM resolved r LEFT JOIN core.systems s ON s.system_id=r.canonical_system_id;

        CREATE TEMP TABLE debcat_relation_candidates AS
        SELECT d.source_record_id,m.projected_relation_id,m.relation_evidence_id,
               m.left_component_entity_id,m.right_component_entity_id,
               m.left_source_component_key,m.right_source_component_key,
               m.canonical_system_stable_object_key,m.period_value msc_period_days,
               abs(d.period_days-m.period_value) period_delta_days
        FROM debcat_system_bindings d
        JOIN msc_relation_evidence_projection m
          ON m.canonical_system_stable_object_key=d.canonical_system_stable_object_key
         AND m.projection_status='accepted_relation_evidence'
         AND m.period_unit_raw={period_unit}
         AND abs(d.period_days-m.period_value)<=greatest({absolute_tolerance},d.period_days*{relative_tolerance})
        WHERE d.binding_status='accepted';

        INSERT INTO debcat_relation_bindings
        WITH candidates AS (
          SELECT d.*,
                 count(c.projected_relation_id) OVER(PARTITION BY d.source_record_id) relation_candidate_count,
                 c.projected_relation_id,c.relation_evidence_id,
                 c.left_component_entity_id,c.right_component_entity_id,
                 c.left_source_component_key,c.right_source_component_key,
                 c.msc_period_days,c.period_delta_days,
                 row_number() OVER(PARTITION BY d.source_record_id
                   ORDER BY c.period_delta_days NULLS LAST,c.projected_relation_id NULLS LAST) candidate_rank
          FROM debcat_system_bindings d
          LEFT JOIN debcat_relation_candidates c USING(source_record_id)
        )
        SELECT sha256(concat_ws('|',{source_id},source_record_id,'relation',{policy_sql})),
               source_record_id,system_binding_id,{source_id},{release_id},{evidence_build_id},
               period_days,greatest({absolute_tolerance},period_days*{relative_tolerance}),
               relation_candidate_count,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1 THEN projected_relation_id END,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1 THEN relation_evidence_id END,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1 THEN left_component_entity_id END,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1 THEN right_component_entity_id END,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1 THEN left_source_component_key END,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1 THEN right_source_component_key END,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1
                    THEN canonical_system_stable_object_key END,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1 THEN msc_period_days END,
               CASE WHEN binding_status='accepted' AND relation_candidate_count=1 THEN period_delta_days END,
               CASE WHEN binding_status='missing' THEN 'missing_system'
                    WHEN binding_status='ambiguous' THEN 'ambiguous_system'
                    WHEN relation_candidate_count=0 THEN 'no_period_match'
                    WHEN relation_candidate_count=1 THEN 'accepted' ELSE 'ambiguous_period_match' END,
               {relation_method},
               CASE WHEN binding_status='missing' THEN 'DEBCat system name is absent from the canonical reference'
                    WHEN binding_status='ambiguous' THEN 'DEBCat system name does not resolve uniquely'
                    WHEN relation_candidate_count=0 THEN 'no accepted MSC relation within the configured period neighborhood'
                    WHEN relation_candidate_count=1 THEN 'unique accepted MSC relation within the configured period neighborhood'
                    ELSE 'multiple accepted MSC relations within the configured period neighborhood' END,
               {policy_sql}
        FROM candidates WHERE candidate_rank=1;

        INSERT INTO debcat_parameter_set_bindings
        SELECT sha256(concat_ws('|',{source_id},p.parameter_set_id,{policy_sql})),
               p.parameter_set_id,p.source_record_id,{source_id},{release_id},{evidence_build_id},
               coalesce(p.component_scope,'system'),
               CASE WHEN p.component_scope IS NULL THEN 'canonical_system'
                    ELSE 'msc_source_component' END,
               CASE WHEN p.component_scope IS NULL AND s.binding_status='accepted'
                      THEN s.canonical_system_stable_object_key
                    WHEN p.component_scope='primary' AND r.binding_status='accepted'
                      THEN r.primary_source_component_key
                    WHEN p.component_scope='secondary' AND r.binding_status='accepted'
                      THEN r.secondary_source_component_key END,
               CASE WHEN s.binding_status='accepted' THEN s.canonical_system_stable_object_key END,
               CASE WHEN p.component_scope IN ('primary','secondary') THEN r.relation_binding_id END,
               CASE WHEN p.component_scope IS NULL THEN s.binding_status
                    WHEN p.component_scope IN ('primary','secondary') THEN r.binding_status
                    ELSE 'excluded' END,
               CASE WHEN p.component_scope IS NULL THEN s.binding_reason
                    WHEN p.component_scope IN ('primary','secondary') THEN r.binding_reason
                    ELSE 'unsupported DEBCat parameter-set component scope' END,
               {policy_sql}
        FROM debcat.stellar_parameter_sets p
        JOIN debcat_system_bindings s USING(source_record_id)
        JOIN debcat_relation_bindings r USING(source_record_id);
        """
    )

    authority = source["component_parameter_authority"]
    authority_case = "CASE p.quantity_key " + " ".join(
        f"WHEN {sql_literal(key)} THEN {sql_literal(value)}" for key, value in authority.items()
    ) + " ELSE 'compiled_detached_eclipsing_binary_measurement' END"
    con.execute(
        f"""
        CREATE TABLE debcat_stellar_parameter_projection AS
        SELECT p.*,b.parameter_set_binding_id,b.target_scope,b.target_key,
               b.canonical_system_stable_object_key,b.relation_binding_id,
               {authority_case} authority_role,
               CASE WHEN b.binding_status='accepted' THEN 'eligible_for_quantity_selection'
                    ELSE 'unresolved_scope_evidence' END projection_status,
               b.binding_reason projection_reason,{policy_sql} policy_version
        FROM debcat.stellar_parameter_evidence p
        JOIN debcat_parameter_set_bindings b USING(parameter_set_id,source_record_id);

        CREATE TABLE debcat_classification_projection AS
        SELECT p.*,
               CASE WHEN p.component_scope='primary' AND r.binding_status='accepted'
                      THEN r.primary_source_component_key
                    WHEN p.component_scope='secondary' AND r.binding_status='accepted'
                      THEN r.secondary_source_component_key END target_key,
               CASE WHEN r.binding_status='accepted' THEN r.canonical_system_stable_object_key END
                 canonical_system_stable_object_key,
               r.relation_binding_id,
               {sql_literal(source['classification_authority'])} authority_role,
               CASE WHEN r.binding_status='accepted' AND p.component_scope IN ('primary','secondary')
                      THEN 'eligible_for_quantity_selection' ELSE 'unresolved_scope_evidence' END projection_status,
               r.binding_reason projection_reason,{policy_sql} policy_version
        FROM debcat.stellar_classification_evidence p
        JOIN debcat_relation_bindings r USING(source_record_id);

        CREATE TABLE debcat_photometry_projection AS
        SELECT p.*,s.canonical_system_stable_object_key target_key,
               'canonical_system' target_scope,
               {sql_literal(source['photometry_authority'])} authority_role,
               CASE WHEN s.binding_status='accepted' THEN 'eligible_for_quantity_selection'
                    ELSE 'unresolved_scope_evidence' END projection_status,
               s.binding_reason projection_reason,{policy_sql} policy_version
        FROM debcat.photometry_extinction_evidence p
        JOIN debcat_system_bindings s USING(source_record_id);

        CREATE TABLE debcat_orbital_solution_projection AS
        SELECT p.*,r.relation_binding_id,r.primary_source_component_key,
               r.secondary_source_component_key,r.canonical_system_stable_object_key,
               {sql_literal(source['orbit_authority'])} authority_role,
               CASE WHEN r.binding_status='accepted' THEN 'eligible_for_quantity_selection'
                    ELSE 'unresolved_scope_evidence' END projection_status,
               r.binding_reason projection_reason,{policy_sql} policy_version
        FROM debcat.orbital_solution_evidence p
        JOIN debcat_relation_bindings r USING(source_record_id);
        """
    )

    system_counts = dict(con.execute("SELECT binding_status,count(*) FROM debcat_system_bindings GROUP BY 1").fetchall())
    relation_counts = dict(con.execute("SELECT binding_status,count(*) FROM debcat_relation_bindings GROUP BY 1").fetchall())
    eligible = lambda table: int(con.execute(
        f"SELECT count(*) FROM {table} WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0])
    observed = {
        "system_bindings": sum(system_counts.values()),
        "systems_accepted": system_counts.get("accepted", 0),
        "systems_missing": system_counts.get("missing", 0),
        "systems_ambiguous": system_counts.get("ambiguous", 0),
        "relation_bindings": sum(relation_counts.values()),
        "relations_accepted": relation_counts.get("accepted", 0),
        "relations_missing_system": relation_counts.get("missing_system", 0),
        "relations_no_period_match": relation_counts.get("no_period_match", 0),
        "relations_ambiguous": relation_counts.get("ambiguous_system", 0) + relation_counts.get("ambiguous_period_match", 0),
        "parameter_sets": int(con.execute("SELECT count(*) FROM debcat_parameter_set_bindings").fetchone()[0]),
        "parameter_sets_eligible": int(con.execute("SELECT count(*) FROM debcat_parameter_set_bindings WHERE binding_status='accepted'").fetchone()[0]),
        "parameter_evidence": int(con.execute("SELECT count(*) FROM debcat_stellar_parameter_projection").fetchone()[0]),
        "parameter_evidence_eligible": eligible("debcat_stellar_parameter_projection"),
        "classification_evidence": int(con.execute("SELECT count(*) FROM debcat_classification_projection").fetchone()[0]),
        "classification_evidence_eligible": eligible("debcat_classification_projection"),
        "photometry_evidence": int(con.execute("SELECT count(*) FROM debcat_photometry_projection").fetchone()[0]),
        "photometry_evidence_eligible": eligible("debcat_photometry_projection"),
        "orbital_solutions": int(con.execute("SELECT count(*) FROM debcat_orbital_solution_projection").fetchone()[0]),
        "orbital_solutions_eligible": eligible("debcat_orbital_solution_projection"),
    }
    expected = {key.removeprefix("expected_"): int(value) for key, value in source["acceptance"].items()}
    if observed != expected:
        raise ValueError(f"DEBCat acceptance counts changed: expected={expected}:observed={observed}")
    return {"source_id": source["source_id"], "observed": observed, "expected": expected}


def verify(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    checks = {
        "duplicate_msc_system_binding_ids": "SELECT count(*)-count(DISTINCT system_binding_id) FROM msc_system_bindings",
        "duplicate_msc_component_entity_ids": "SELECT count(*)-count(DISTINCT component_entity_id) FROM msc_component_entities",
        "duplicate_msc_relation_projection_ids": "SELECT count(*)-count(DISTINCT projected_relation_id) FROM msc_relation_evidence_projection",
        "duplicate_debcat_system_binding_ids": "SELECT count(*)-count(DISTINCT system_binding_id) FROM debcat_system_bindings",
        "duplicate_debcat_relation_binding_ids": "SELECT count(*)-count(DISTINCT relation_binding_id) FROM debcat_relation_bindings",
        "duplicate_parameter_set_binding_ids": "SELECT count(*)-count(DISTINCT parameter_set_binding_id) FROM debcat_parameter_set_bindings",
        "accepted_components_without_targets": "SELECT count(*) FROM msc_component_entities WHERE binding_status='accepted' AND (source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_components_with_targets": "SELECT count(*) FROM msc_component_entities WHERE binding_status<>'accepted' AND source_component_key IS NOT NULL",
        "accepted_msc_relations_without_two_targets": "SELECT count(*) FROM msc_relation_evidence_projection WHERE projection_status='accepted_relation_evidence' AND (left_source_component_key IS NULL OR right_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "accepted_msc_self_relations": "SELECT count(*) FROM msc_relation_evidence_projection WHERE projection_status='accepted_relation_evidence' AND left_source_component_key=right_source_component_key",
        "accepted_debcat_relations_without_targets": "SELECT count(*) FROM debcat_relation_bindings WHERE binding_status='accepted' AND (primary_source_component_key IS NULL OR secondary_source_component_key IS NULL OR canonical_system_stable_object_key IS NULL)",
        "unaccepted_debcat_relations_with_component_targets": "SELECT count(*) FROM debcat_relation_bindings WHERE binding_status<>'accepted' AND (primary_source_component_key IS NOT NULL OR secondary_source_component_key IS NOT NULL)",
        "component_parameter_sets_targeting_systems": "SELECT count(*) FROM debcat_parameter_set_bindings WHERE component_scope IN ('primary','secondary') AND binding_status='accepted' AND target_scope<>'msc_source_component'",
        "system_parameter_sets_targeting_components": "SELECT count(*) FROM debcat_parameter_set_bindings WHERE component_scope='system' AND binding_status='accepted' AND target_scope<>'canonical_system'",
        "eligible_parameters_without_exact_evidence": "SELECT count(*) FROM debcat_stellar_parameter_projection WHERE projection_status='eligible_for_quantity_selection' AND (evidence_id IS NULL OR parameter_set_id IS NULL OR target_key IS NULL)",
        "eligible_classifications_without_targets": "SELECT count(*) FROM debcat_classification_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_photometry_without_systems": "SELECT count(*) FROM debcat_photometry_projection WHERE projection_status='eligible_for_quantity_selection' AND target_key IS NULL",
        "eligible_orbits_without_relations": "SELECT count(*) FROM debcat_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection' AND relation_binding_id IS NULL",
        "canonical_containment_rows": "SELECT 0",
    }
    result = {name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()}
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"component-scope projection checks failed: {failing}")
    return result


def compile_components(*, policy_path: Path, state: Path, output_root: Path, report_path: Path) -> dict[str, Any]:
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
    required = (identity_db, identity_report, core_db)
    if not all(path.is_file() for path in required):
        raise FileNotFoundError(f"missing component compiler input: {[str(p) for p in required if not p.is_file()]}")
    identity_report_sha = sha256_file(identity_report)
    source_build_ids = [policy[name]["evidence_build_id"] for name in ("msc", "debcat")]
    build_id = sha256_bytes(canonical_json({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "identity_graph_id": identity_id,
        "identity_report_sha256": identity_report_sha,
        "canonical_reference_build_id": canonical_build,
        "source_build_ids": source_build_ids,
    }))[:24]
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / build_id
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        database = staging / "selected_components.duckdb"
        con = duckdb.connect(str(database))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        spill = staging / "spill"
        spill.mkdir()
        con.execute(f"SET temp_directory={sql_literal(spill)}")
        con.execute(f"ATTACH {sql_literal(identity_db)} AS identity (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(core_db)} AS core (READ_ONLY)")
        evidence_root = state / "derived/evidence_lake_v2/scientific_evidence"
        for alias, name in (("msc", "msc"), ("debcat", "debcat")):
            source_db = evidence_root / policy[name]["evidence_build_id"] / "scientific_evidence.duckdb"
            if not source_db.is_file():
                raise FileNotFoundError(f"missing E4 source artifact: {source_db}")
            con.execute(f"ATTACH {sql_literal(source_db)} AS {alias} (READ_ONLY)")
        create_schema(con)
        source_reports = [
            compile_msc(con, source=policy["msc"], policy_version=policy["policy_version"]),
            compile_debcat(con, source=policy["debcat"], policy_version=policy["policy_version"]),
        ]
        checks = verify(con)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?,?)",
            [build_id, policy["policy_version"], policy_sha, identity_id, canonical_build, utc_now(), "pass"],
        )
        con.execute("CHECKPOINT")
        parquet = staging / "parquet"
        parquet.mkdir()
        exports = [
            ("msc_system_bindings", "system_binding_id"),
            ("msc_component_entities", "component_entity_id"),
            ("msc_relation_evidence_projection", "projected_relation_id"),
            ("debcat_system_bindings", "system_binding_id"),
            ("debcat_relation_bindings", "relation_binding_id"),
            ("debcat_parameter_set_bindings", "parameter_set_binding_id"),
            ("debcat_stellar_parameter_projection", "evidence_id"),
            ("debcat_classification_projection", "evidence_id"),
            ("debcat_photometry_projection", "evidence_id"),
            ("debcat_orbital_solution_projection", "evidence_id"),
        ]
        for table, order_key in exports:
            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order_key}) TO "
                f"{sql_literal(parquet / (table + '.parquet'))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        con.close()
        shutil.rmtree(spill)
        files = {}
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                files[str(path.relative_to(staging))] = {
                    "bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
        deterministic_files = {name: value for name, value in files.items() if name.startswith("parquet/")}
        manifest = {
            "schema_version": "spacegate.e5_selected_components.v1",
            "build_id": build_id,
            "policy_version": policy["policy_version"],
            "policy_sha256": policy_sha,
            "compiler_version": policy["compiler_version"],
            "compiler_sha256": compiler_sha,
            "identity_graph_id": identity_id,
            "identity_report_sha256": identity_report_sha,
            "canonical_reference_build_id": canonical_build,
            "source_reports": source_reports,
            "verification": checks,
            "files": files,
            "deterministic_files": deterministic_files,
            "generated_at": utc_now(),
        }
        write_json(staging / "manifest.json", manifest)
        if destination.exists():
            existing = read_json(destination / "manifest.json")
            comparable = ("policy_sha256", "identity_graph_id", "identity_report_sha256", "canonical_reference_build_id", "source_reports", "verification", "deterministic_files")
            if any(existing.get(key) != manifest.get(key) for key in comparable):
                raise ValueError(f"deterministic component build differs: {build_id}")
            shutil.rmtree(staging)
            manifest = existing
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
    output_root = args.output_root or args.state / "derived/evidence_lake_v2/selected_components"
    report = compile_components(policy_path=args.policy, state=args.state, output_root=output_root, report_path=args.report)
    print(f"Selected component evidence pass: build={report['build_id']} wall={report['wall_seconds']:.1f}s")


if __name__ == "__main__":
    main()
