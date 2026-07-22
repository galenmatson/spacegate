#!/usr/bin/env python3
"""Compile conflict-preserving supplemental planet and TESS evidence."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_planet_evidence_policies.json"
DEFAULT_STATE = Path("/data/spacegate/state")
TABLE_ORDER = (
    ("planet_source_bindings", "binding_id"),
    ("planet_lifecycle_projection", "evidence_id"),
    ("planet_parameter_set_projection", "parameter_set_id"),
    ("planet_parameter_projection", "evidence_id"),
    ("planet_lifecycle_conflicts", "canonical_planet_key"),
    ("tess_target_bindings", "binding_id"),
    ("tess_candidate_projection", "source_record_id"),
    ("tess_transit_projection", "evidence_id"),
    ("tess_planet_parameter_projection", "evidence_id"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(canonical_json(value) + b"\n")
    os.replace(temporary, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e5_planet_evidence_policies.v1":
        raise ValueError("unsupported planet-evidence policy schema")
    required_sources = {
        "exoplanet_lifecycle.exoplanet_eu",
        "exoplanet_lifecycle.hwc",
        "exoplanet_lifecycle.open_exoplanet_catalogue",
        "tess.identity_and_candidate_evidence",
    }
    if set(policy.get("sources") or {}) != required_sources:
        raise ValueError("planet-evidence policy must account for all four E5 blockers")
    authority = policy.get("authority_policy") or {}
    if authority.get("canonical_planet_inventory") != "nasa_rooted_reference_only":
        raise ValueError("supplemental policy cannot own canonical planet inventory")
    if authority.get("toi_candidate") != "candidate_evidence_never_canonical_inventory":
        raise ValueError("TOI candidate inventory boundary is not fail-closed")
    for source_id, source in policy["sources"].items():
        missing = {"release_id", "evidence_build_id"} - set(source)
        if missing:
            raise ValueError(f"{source_id} lacks {sorted(missing)}")


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          build_id VARCHAR, policy_version VARCHAR, policy_sha256 VARCHAR,
          evidence_release_set_id VARCHAR, identity_graph_id VARCHAR,
          canonical_reference_build_id VARCHAR, canonical_planet_count BIGINT,
          generated_at VARCHAR, status VARCHAR
        );
        CREATE TABLE planet_source_bindings (
          binding_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          source_object_key VARCHAR, representative_source_record_id VARCHAR,
          source_identifier_raw VARCHAR, source_identifier_normalized VARCHAR,
          identifier_claim_count BIGINT, canonical_candidate_count BIGINT,
          canonical_planet_id BIGINT, canonical_planet_key VARCHAR,
          canonical_system_id HUGEINT, binding_status VARCHAR,
          binding_method VARCHAR, binding_reason VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE planet_lifecycle_projection (
          evidence_id VARCHAR, source_record_id VARCHAR, source_id VARCHAR,
          release_id VARCHAR, source_object_key VARCHAR, binding_id VARCHAR,
          binding_status VARCHAR, canonical_planet_id BIGINT,
          canonical_planet_key VARCHAR, source_identifier_raw VARCHAR,
          disposition_raw VARCHAR, disposition_normalized VARCHAR,
          evidence_polarity VARCHAR, effective_at_raw VARCHAR,
          supersedes_evidence_id VARCHAR, reference_raw VARCHAR,
          quality_json JSON, evidence_role VARCHAR,
          canonical_inventory_mutation BOOLEAN, policy_version VARCHAR
        );
        CREATE TABLE planet_parameter_set_projection (
          parameter_set_id VARCHAR, source_record_id VARCHAR, source_id VARCHAR,
          release_id VARCHAR, source_object_key VARCHAR, binding_id VARCHAR,
          binding_status VARCHAR, canonical_planet_id BIGINT,
          canonical_planet_key VARCHAR, parameter_set_kind VARCHAR,
          method VARCHAR, model VARCHAR, reference_raw VARCHAR, epoch_raw VARCHAR,
          frame_raw VARCHAR, quality_json JSON, parameter_role VARCHAR,
          selection_status VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE planet_parameter_projection (
          evidence_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
          source_id VARCHAR, canonical_planet_key VARCHAR, quantity_key VARCHAR,
          value_raw VARCHAR, unit_raw VARCHAR, normalized_value DOUBLE,
          normalized_unit VARCHAR, uncertainty_lower DOUBLE,
          uncertainty_upper DOUBLE, bound_semantics VARCHAR, method VARCHAR,
          model VARCHAR, reference_raw VARCHAR, quality_json JSON,
          normalization_version VARCHAR, parameter_role VARCHAR,
          selection_status VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE planet_lifecycle_conflicts (
          canonical_planet_key VARCHAR, canonical_planet_id BIGINT,
          positive_count BIGINT, candidate_count BIGINT, negative_count BIGINT,
          ambiguous_count BIGINT, source_count BIGINT, has_polarity_conflict BOOLEAN,
          policy_version VARCHAR
        );
        CREATE TABLE tess_target_bindings (
          binding_id VARCHAR, target_source_record_id VARCHAR, tic_id VARCHAR,
          source_families VARCHAR, mast_source_record_id VARCHAR,
          gaia_dr2_source_id VARCHAR, graph_outcome VARCHAR, graph_reason VARCHAR,
          canonical_candidate_count BIGINT, canonical_star_key VARCHAR,
          canonical_system_key VARCHAR, binding_status VARCHAR,
          binding_method VARCHAR, binding_reason VARCHAR,
          tic_disposition VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE tess_candidate_projection (
          source_record_id VARCHAR, lifecycle_evidence_id VARCHAR, toi_id VARCHAR,
          tic_id VARCHAR, host_binding_id VARCHAR, host_binding_status VARCHAR,
          canonical_star_key VARCHAR, canonical_system_key VARCHAR,
          disposition_raw VARCHAR, disposition_normalized VARCHAR,
          evidence_polarity VARCHAR, effective_at_raw VARCHAR,
          source_reference_raw VARCHAR, lifecycle_quality_json JSON,
          orbital_period_days DOUBLE, canonical_planet_candidate_count BIGINT,
          canonical_planet_id BIGINT, canonical_planet_key VARCHAR,
          planet_link_status VARCHAR, planet_link_method VARCHAR,
          planet_period_delta_days DOUBLE, evidence_role VARCHAR,
          canonical_inventory_mutation BOOLEAN, policy_version VARCHAR
        );
        CREATE TABLE tess_transit_projection (
          evidence_id VARCHAR, source_record_id VARCHAR, toi_id VARCHAR,
          tic_id VARCHAR, host_binding_status VARCHAR, canonical_star_key VARCHAR,
          canonical_planet_key VARCHAR, signal_identifier_raw VARCHAR,
          quantity_key VARCHAR, value_raw VARCHAR, unit_raw VARCHAR,
          normalized_value DOUBLE, normalized_unit VARCHAR,
          uncertainty_lower DOUBLE, uncertainty_upper DOUBLE,
          bound_semantics VARCHAR, observation_epoch_raw VARCHAR, method VARCHAR,
          model VARCHAR, reference_raw VARCHAR, quality_json JSON,
          normalization_version VARCHAR, evidence_role VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE tess_planet_parameter_projection (
          evidence_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
          toi_id VARCHAR, tic_id VARCHAR, host_binding_status VARCHAR,
          canonical_planet_key VARCHAR, quantity_key VARCHAR, value_raw VARCHAR,
          unit_raw VARCHAR, normalized_value DOUBLE, normalized_unit VARCHAR,
          uncertainty_lower DOUBLE, uncertainty_upper DOUBLE,
          bound_semantics VARCHAR, method VARCHAR, model VARCHAR,
          reference_raw VARCHAR, quality_json JSON,
          normalization_version VARCHAR, evidence_role VARCHAR, policy_version VARCHAR
        );
        """
    )


def attach_inputs(
    con: duckdb.DuckDBPyConnection, *, state: Path, policy: dict[str, Any]
) -> dict[str, Path]:
    aliases = {
        "eu": "exoplanet_lifecycle.exoplanet_eu",
        "hwc": "exoplanet_lifecycle.hwc",
        "oec": "exoplanet_lifecycle.open_exoplanet_catalogue",
        "tess": "tess.identity_and_candidate_evidence",
    }
    paths: dict[str, Path] = {}
    for alias, source_id in aliases.items():
        build_id = policy["sources"][source_id]["evidence_build_id"]
        path = state / "derived/evidence_lake_v2/scientific_evidence" / build_id / "scientific_evidence.duckdb"
        paths[alias] = path
    paths["identity"] = state / "derived/evidence_lake_v2/identity" / policy["identity_graph_id"] / "identity_graph.duckdb"
    build = policy["canonical_reference_build_id"]
    paths["core"] = state / "out" / build / "core.duckdb"
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing planet-evidence inputs: {missing}")
    for alias, path in paths.items():
        con.execute(f"ATTACH {sql_literal(path)} AS {alias} (READ_ONLY)")
    return paths


def validate_input_releases(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> None:
    for alias, source_id in (
        ("eu", "exoplanet_lifecycle.exoplanet_eu"),
        ("hwc", "exoplanet_lifecycle.hwc"),
        ("oec", "exoplanet_lifecycle.open_exoplanet_catalogue"),
        ("tess", "tess.identity_and_candidate_evidence"),
    ):
        rows = con.execute(
            f"SELECT DISTINCT source_id,release_id FROM {alias}.source_records"
        ).fetchall()
        expected = [(source_id, policy["sources"][source_id]["release_id"])]
        if rows != expected:
            raise ValueError(f"{source_id} release mismatch: expected={expected} observed={rows}")


def materialize_supplemental(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> None:
    version = sql_literal(policy["policy_version"])
    con.execute(
        """
        CREATE TEMP TABLE canonical_planet_names AS
        WITH names AS (
          SELECT planet_id,stable_object_key,system_id,
                 trim(regexp_replace(lower(planet_name_norm),'[^a-z0-9]+',' ','g')) name_norm
          FROM core.planets
        )
        SELECT name_norm,count(DISTINCT planet_id) name_count,min(planet_id) planet_id,
               min(stable_object_key) stable_object_key,min(system_id) system_id
        FROM names GROUP BY name_norm;

        CREATE TEMP TABLE source_planet_objects AS
        SELECT 'exoplanet_lifecycle.exoplanet_eu' source_id,r.release_id,
               r.source_record_id source_object_key,r.source_record_id representative_source_record_id
        FROM eu.source_records r
        WHERE EXISTS (SELECT 1 FROM eu.planet_lifecycle_evidence l WHERE l.source_record_id=r.source_record_id)
        UNION ALL
        SELECT 'exoplanet_lifecycle.hwc',r.release_id,r.source_record_id,r.source_record_id
        FROM hwc.source_records r
        WHERE EXISTS (SELECT 1 FROM hwc.planet_parameter_sets p WHERE p.source_record_id=r.source_record_id)
        UNION ALL
        SELECT 'exoplanet_lifecycle.open_exoplanet_catalogue',r.release_id,
               json_extract_string(r.logical_key_json,'$.source_member') || '|' ||
               json_extract_string(r.logical_key_json,'$.source_node_path'),r.source_record_id
        FROM oec.source_records r
        WHERE r.source_table='oec_objects'
          AND EXISTS (SELECT 1 FROM oec.planet_lifecycle_evidence l WHERE l.source_record_id=r.source_record_id);

        CREATE TEMP TABLE source_planet_claims AS
        SELECT o.source_id,o.source_object_key,i.identifier_raw,
               trim(regexp_replace(lower(i.identifier_normalized),'[^a-z0-9]+',' ','g')) identifier_norm
        FROM source_planet_objects o JOIN eu.identifier_claim_evidence i
          ON o.source_id='exoplanet_lifecycle.exoplanet_eu'
         AND i.source_record_id=o.representative_source_record_id
         AND i.namespace='exoplanet_eu_planet_name'
        UNION ALL
        SELECT o.source_id,o.source_object_key,i.identifier_raw,
               trim(regexp_replace(lower(i.identifier_normalized),'[^a-z0-9]+',' ','g'))
        FROM source_planet_objects o JOIN hwc.identifier_claim_evidence i
          ON o.source_id='exoplanet_lifecycle.hwc'
         AND i.source_record_id=o.representative_source_record_id
         AND i.namespace='hwc_planet_name'
        UNION ALL
        SELECT o.source_id,o.source_object_key,i.identifier_raw,
               trim(regexp_replace(lower(i.identifier_normalized),'[^a-z0-9]+',' ','g'))
        FROM source_planet_objects o
        JOIN oec.source_records r ON r.source_table IN ('oec_names','oec_objects')
          AND json_extract_string(r.logical_key_json,'$.source_member') || '|' ||
              json_extract_string(r.logical_key_json,'$.source_node_path')=o.source_object_key
        JOIN oec.identifier_claim_evidence i ON i.source_record_id=r.source_record_id
          AND i.namespace='oec_object_name'
        WHERE o.source_id='exoplanet_lifecycle.open_exoplanet_catalogue';
        """
    )
    con.execute(
        f"""
        INSERT INTO planet_source_bindings
        WITH claim_summary AS (
          SELECT source_id,source_object_key,count(*) claim_count,
                 arg_min(identifier_raw,identifier_norm) identifier_raw,
                 min(identifier_norm) identifier_norm
          FROM source_planet_claims GROUP BY 1,2
        ), candidates AS (
          SELECT c.source_id,c.source_object_key,count(DISTINCT p.planet_id) candidate_count,
                 min(p.planet_id) planet_id,min(p.stable_object_key) planet_key,
                 min(p.system_id) system_id
          FROM source_planet_claims c JOIN canonical_planet_names p
            ON p.name_norm=c.identifier_norm AND p.name_count=1
          GROUP BY 1,2
        )
        SELECT sha256(o.source_id || '|' || o.source_object_key || '|planet-binding|' || {version}),
               o.source_id,o.release_id,o.source_object_key,o.representative_source_record_id,
               s.identifier_raw,s.identifier_norm,coalesce(s.claim_count,0),
               coalesce(c.candidate_count,0),
               CASE WHEN c.candidate_count=1 THEN c.planet_id END,
               CASE WHEN c.candidate_count=1 THEN c.planet_key END,
               CASE WHEN c.candidate_count=1 THEN c.system_id END,
               CASE WHEN c.candidate_count=1 THEN 'accepted'
                    WHEN c.candidate_count>1 THEN 'ambiguous' ELSE 'missing' END,
               CASE WHEN o.source_id='exoplanet_lifecycle.open_exoplanet_catalogue'
                      THEN 'structural_object_all_names_to_canonical_unique_name'
                    ELSE 'canonical_unique_normalized_name' END,
               CASE WHEN c.candidate_count=1 THEN 'all matching source names agree on one unique canonical NASA-rooted planet'
                    WHEN c.candidate_count>1 THEN 'source names resolve to multiple canonical planets'
                    ELSE 'no source name resolves to a unique planet in the current canonical reference' END,
               {version}
        FROM source_planet_objects o
        LEFT JOIN claim_summary s USING(source_id,source_object_key)
        LEFT JOIN candidates c USING(source_id,source_object_key);

        INSERT INTO planet_lifecycle_projection
        WITH lifecycle AS (
          SELECT l.*,r.release_id,r.source_record_id source_object_key,
                 'exoplanet_lifecycle.exoplanet_eu' source_id
          FROM eu.planet_lifecycle_evidence l JOIN eu.source_records r USING(source_record_id)
          UNION ALL
          SELECT l.*,r.release_id,
                 json_extract_string(r.logical_key_json,'$.source_member') || '|' ||
                 json_extract_string(r.logical_key_json,'$.source_node_path'),
                 'exoplanet_lifecycle.open_exoplanet_catalogue'
          FROM oec.planet_lifecycle_evidence l JOIN oec.source_records r USING(source_record_id)
        )
        SELECT l.evidence_id,l.source_record_id,l.source_id,l.release_id,l.source_object_key,
               b.binding_id,b.binding_status,b.canonical_planet_id,b.canonical_planet_key,
               l.source_identifier_raw,l.disposition_raw,l.disposition_normalized,
               l.evidence_polarity,l.effective_at_raw,l.supersedes_evidence_id,
               l.reference_raw,l.quality_json,
               CASE WHEN l.evidence_polarity='positive' AND b.binding_status='accepted' THEN 'supporting_positive_evidence'
                    WHEN l.evidence_polarity='positive' THEN 'unresolved_positive_evidence'
                    WHEN l.evidence_polarity='negative' THEN 'negative_evidence'
                    WHEN l.evidence_polarity='candidate' THEN 'candidate_evidence'
                    ELSE 'ambiguous_evidence' END,
               false,{version}
        FROM lifecycle l JOIN planet_source_bindings b USING(source_id,source_object_key);
        """
    )
    con.execute(
        f"""
        INSERT INTO planet_parameter_set_projection
        WITH sets AS (
          SELECT p.*,r.release_id,r.source_record_id source_object_key,
                 'exoplanet_lifecycle.exoplanet_eu' source_id,
                 'supplemental_parameter_evidence' parameter_role
          FROM eu.planet_parameter_sets p JOIN eu.source_records r USING(source_record_id)
          UNION ALL
          SELECT p.*,r.release_id,r.source_record_id,
                 'exoplanet_lifecycle.hwc','derived_habitability_comparison_evidence'
          FROM hwc.planet_parameter_sets p JOIN hwc.source_records r USING(source_record_id)
          UNION ALL
          SELECT p.*,r.release_id,
                 json_extract_string(r.logical_key_json,'$.source_member') || '|' ||
                 json_extract_string(r.logical_key_json,'$.source_node_path'),
                 'exoplanet_lifecycle.open_exoplanet_catalogue','supplemental_parameter_evidence'
          FROM oec.planet_parameter_sets p JOIN oec.source_records r USING(source_record_id)
        )
        SELECT s.parameter_set_id,s.source_record_id,s.source_id,s.release_id,s.source_object_key,
               b.binding_id,b.binding_status,b.canonical_planet_id,b.canonical_planet_key,
               s.parameter_set_kind,s.method,s.model,s.reference_raw,s.epoch_raw,s.frame_raw,
               s.quality_json,s.parameter_role,
               CASE WHEN b.binding_status<>'accepted' THEN 'unresolved_object'
                    WHEN s.parameter_role='derived_habitability_comparison_evidence' THEN 'evidence_only'
                    ELSE 'fallback_candidate_pending_e6' END,{version}
        FROM sets s JOIN planet_source_bindings b USING(source_id,source_object_key);

        INSERT INTO planet_parameter_projection
        WITH facts AS (
          SELECT *,'exoplanet_lifecycle.exoplanet_eu' source_id FROM eu.planet_parameter_evidence
          UNION ALL SELECT *,'exoplanet_lifecycle.hwc' FROM hwc.planet_parameter_evidence
          UNION ALL SELECT *,'exoplanet_lifecycle.open_exoplanet_catalogue' FROM oec.planet_parameter_evidence
        )
        SELECT f.evidence_id,f.parameter_set_id,f.source_record_id,f.source_id,
               s.canonical_planet_key,f.quantity_key,f.value_raw,f.unit_raw,
               f.normalized_value,f.normalized_unit,f.uncertainty_lower,
               f.uncertainty_upper,f.bound_semantics,f.method,f.model,
               f.reference_raw,f.quality_json,f.normalization_version,
               s.parameter_role,s.selection_status,{version}
        FROM facts f JOIN planet_parameter_set_projection s USING(parameter_set_id,source_id);

        INSERT INTO planet_lifecycle_conflicts
        SELECT canonical_planet_key,min(canonical_planet_id),
               count(*) FILTER (WHERE evidence_polarity='positive'),
               count(*) FILTER (WHERE evidence_polarity='candidate'),
               count(*) FILTER (WHERE evidence_polarity='negative'),
               count(*) FILTER (WHERE evidence_polarity='ambiguous'),
               count(DISTINCT source_id),
               count(*) FILTER (WHERE evidence_polarity='negative')>0 AND
                 count(*) FILTER (WHERE evidence_polarity IN ('positive','candidate'))>0,
               {version}
        FROM planet_lifecycle_projection WHERE binding_status='accepted'
        GROUP BY canonical_planet_key;
        """
    )


def materialize_tess(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> None:
    version = sql_literal(policy["policy_version"])
    tess_policy = policy["sources"]["tess.identity_and_candidate_evidence"]
    absolute = float(tess_policy["period_absolute_tolerance_days"])
    relative = float(tess_policy["period_relative_tolerance"])
    con.execute(
        """
        CREATE TEMP TABLE tess_target_records AS
        SELECT r.source_record_id target_source_record_id,
               json_extract_string(r.source_context_json,'$.source_families') source_families,
               i.identifier_normalized tic_id
        FROM tess.source_records r JOIN tess.identifier_claim_evidence i USING(source_record_id)
        WHERE r.source_table='tess_target_set' AND i.namespace='tic_id'
          AND json_extract_string(i.quality_json,'$.source_field')='tic_id';

        CREATE TEMP TABLE mast_identity AS
        WITH claims AS (
          SELECT r.source_record_id,
                 max(i.identifier_normalized) FILTER (WHERE i.namespace='tic_id' AND
                   json_extract_string(i.quality_json,'$.source_field')='ID') tic_id,
                 max(i.identifier_normalized) FILTER (WHERE i.namespace='gaia_dr2_source_id') dr2_source_id,
                 json_extract_string(r.source_context_json,'$.disposition') tic_disposition
          FROM tess.source_records r LEFT JOIN tess.identifier_claim_evidence i USING(source_record_id)
          WHERE r.source_table='mast_tic_targeted' GROUP BY r.source_record_id,r.source_context_json
        ) SELECT * FROM claims;

        CREATE TEMP TABLE tess_graph_routes AS
        SELECT m.tic_id,m.source_record_id mast_source_record_id,m.dr2_source_id,
               m.tic_disposition,g.outcome graph_outcome,g.reason graph_reason,
               g.canonical_stable_object_key canonical_star_key,
               g.canonical_system_stable_object_key canonical_system_key
        FROM mast_identity m LEFT JOIN identity.source_record_bindings g
          ON g.source_id='tess.identity_and_candidate_evidence'
         AND g.source_name='mast_tic_targeted' AND g.dr2_source_id=m.dr2_source_id;
        """
    )
    con.execute(
        f"""
        INSERT INTO tess_target_bindings
        WITH summary AS (
          SELECT tic_id,min(mast_source_record_id) mast_source_record_id,
                 min(dr2_source_id) gaia_dr2_source_id,
                 min(tic_disposition) tic_disposition,
                 count(DISTINCT canonical_star_key) FILTER (WHERE graph_outcome='accepted') accepted_count,
                 min(canonical_star_key) FILTER (WHERE graph_outcome='accepted') star_key,
                 min(canonical_system_key) FILTER (WHERE graph_outcome='accepted') system_key,
                 arg_min(graph_outcome,CASE graph_outcome WHEN 'ambiguous' THEN 1 WHEN 'excluded' THEN 2 WHEN 'missing' THEN 3 ELSE 4 END) graph_outcome,
                 arg_min(graph_reason,CASE graph_outcome WHEN 'ambiguous' THEN 1 WHEN 'excluded' THEN 2 WHEN 'missing' THEN 3 ELSE 4 END) graph_reason
          FROM tess_graph_routes GROUP BY tic_id
        )
        SELECT sha256('tess-target|' || t.tic_id || '|' || {version}),
               t.target_source_record_id,t.tic_id,t.source_families,s.mast_source_record_id,
               s.gaia_dr2_source_id,s.graph_outcome,s.graph_reason,coalesce(s.accepted_count,0),
               CASE WHEN s.accepted_count=1 AND nullif(s.tic_disposition,'') IS NULL THEN s.star_key END,
               CASE WHEN s.accepted_count=1 AND nullif(s.tic_disposition,'') IS NULL THEN s.system_key END,
               CASE WHEN nullif(s.tic_disposition,'') IS NOT NULL THEN 'excluded'
                    WHEN s.accepted_count=1 THEN 'accepted'
                    WHEN s.accepted_count>1 THEN 'ambiguous'
                    WHEN s.graph_outcome='ambiguous' THEN 'ambiguous'
                    WHEN s.graph_outcome='excluded' THEN 'excluded' ELSE 'missing' END,
               'official_gaia_dr2_dr3_identity_graph',
               CASE WHEN nullif(s.tic_disposition,'') IS NOT NULL THEN 'TIC catalog disposition ' || lower(s.tic_disposition)
                    WHEN s.accepted_count=1 THEN 'one official bidirectional Gaia release binding reaches one canonical star'
                    WHEN s.accepted_count>1 THEN 'TIC routes reach multiple canonical stars'
                    WHEN s.graph_reason IS NOT NULL THEN s.graph_reason
                    WHEN s.mast_source_record_id IS NULL THEN 'targeted TIC has no source catalog record'
                    WHEN s.gaia_dr2_source_id IS NULL THEN 'TIC source record has no Gaia DR2 identity route'
                    ELSE 'no accepted identity-graph route' END,
               s.tic_disposition,{version}
        FROM tess_target_records t LEFT JOIN summary s USING(tic_id);
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE toi_records AS
        WITH claims AS (
          SELECT r.source_record_id,
                 min(i.identifier_normalized) FILTER (WHERE i.namespace='toi_id') toi_id,
                 min(i.identifier_normalized) FILTER (WHERE i.namespace='tic_id') tic_id
          FROM tess.source_records r JOIN tess.identifier_claim_evidence i USING(source_record_id)
          WHERE r.source_table='nasa_toi' GROUP BY r.source_record_id
        ), periods AS (
          SELECT source_record_id,max(normalized_value) orbital_period_days
          FROM tess.planet_parameter_evidence
          WHERE quantity_key='nasa_exoplanet_archive.pl_orbper' GROUP BY source_record_id
        )
        SELECT c.*,p.orbital_period_days,l.evidence_id lifecycle_evidence_id,
               l.disposition_raw,l.disposition_normalized,l.evidence_polarity,
               l.effective_at_raw,l.reference_raw,l.quality_json
        FROM claims c LEFT JOIN periods p USING(source_record_id)
        LEFT JOIN tess.planet_lifecycle_evidence l USING(source_record_id);

        CREATE TEMP TABLE toi_planet_candidates AS
        SELECT t.source_record_id,p.planet_id,p.stable_object_key,
               abs(p.orbital_period_days-t.orbital_period_days) period_delta_days
        FROM toi_records t JOIN tess_target_bindings h USING(tic_id)
        JOIN core.stars s ON s.stable_object_key=h.canonical_star_key
        JOIN core.planets p ON p.system_id=s.system_id
          AND (p.star_id=s.star_id OR p.star_id IS NULL)
        WHERE t.disposition_normalized='CONFIRMED' AND h.binding_status='accepted'
          AND t.orbital_period_days IS NOT NULL AND p.orbital_period_days IS NOT NULL
          AND abs(p.orbital_period_days-t.orbital_period_days)<=greatest({absolute},t.orbital_period_days*{relative});

        INSERT INTO tess_candidate_projection
        WITH candidates AS (
          SELECT source_record_id,count(DISTINCT planet_id) candidate_count,
                 min(planet_id) planet_id,min(stable_object_key) planet_key,
                 min(period_delta_days) period_delta_days
          FROM toi_planet_candidates GROUP BY source_record_id
        )
        SELECT t.source_record_id,t.lifecycle_evidence_id,t.toi_id,t.tic_id,h.binding_id,
               h.binding_status,h.canonical_star_key,h.canonical_system_key,
               t.disposition_raw,t.disposition_normalized,t.evidence_polarity,
               t.effective_at_raw,t.reference_raw,t.quality_json,t.orbital_period_days,
               coalesce(c.candidate_count,0),
               CASE WHEN t.disposition_normalized='CONFIRMED' AND c.candidate_count=1 THEN c.planet_id END,
               CASE WHEN t.disposition_normalized='CONFIRMED' AND c.candidate_count=1 THEN c.planet_key END,
               CASE WHEN t.disposition_normalized='CONFIRMED' AND c.candidate_count=1 THEN 'accepted'
                    WHEN t.disposition_normalized='CONFIRMED' AND c.candidate_count>1 THEN 'ambiguous'
                    WHEN t.disposition_normalized='CONFIRMED' THEN 'missing'
                    ELSE 'not_applicable' END,
               CASE WHEN t.disposition_normalized='CONFIRMED' AND c.candidate_count=1 THEN 'accepted_host_and_unique_period'
                    WHEN t.disposition_normalized='CONFIRMED' AND h.binding_status<>'accepted' THEN 'confirmed_host_unresolved'
                    WHEN t.disposition_normalized='CONFIRMED' AND c.candidate_count>1 THEN 'confirmed_period_collision'
                    WHEN t.disposition_normalized='CONFIRMED' THEN 'confirmed_planet_unmatched'
                    ELSE 'not_applicable' END,
               CASE WHEN c.candidate_count=1 THEN c.period_delta_days END,
               CASE WHEN t.disposition_normalized='CONFIRMED' AND c.candidate_count=1 THEN 'confirmed_planet_link_evidence'
                    WHEN t.evidence_polarity='negative' THEN 'negative_candidate_evidence'
                    WHEN t.evidence_polarity='candidate' THEN 'candidate_evidence'
                    WHEN t.evidence_polarity='positive' THEN 'unresolved_positive_evidence'
                    ELSE 'unclassified_candidate_evidence' END,
               false,{version}
        FROM toi_records t JOIN tess_target_bindings h USING(tic_id)
        LEFT JOIN candidates c USING(source_record_id);

        INSERT INTO tess_transit_projection
        SELECT e.evidence_id,e.source_record_id,c.toi_id,c.tic_id,c.host_binding_status,
               c.canonical_star_key,c.canonical_planet_key,e.signal_identifier_raw,
               e.quantity_key,e.value_raw,e.unit_raw,e.normalized_value,e.normalized_unit,
               e.uncertainty_lower,e.uncertainty_upper,e.bound_semantics,
               e.observation_epoch_raw,e.method,e.model,e.reference_raw,e.quality_json,
               e.normalization_version,'source_transit_evidence', {version}
        FROM tess.transit_observation_evidence e JOIN tess_candidate_projection c USING(source_record_id);

        INSERT INTO tess_planet_parameter_projection
        SELECT e.evidence_id,e.parameter_set_id,e.source_record_id,c.toi_id,c.tic_id,
               c.host_binding_status,c.canonical_planet_key,e.quantity_key,e.value_raw,
               e.unit_raw,e.normalized_value,e.normalized_unit,e.uncertainty_lower,
               e.uncertainty_upper,e.bound_semantics,e.method,e.model,e.reference_raw,
               e.quality_json,e.normalization_version,
               CASE WHEN c.canonical_planet_key IS NOT NULL THEN 'confirmed_planet_supporting_evidence'
                    ELSE 'candidate_parameter_evidence' END,{version}
        FROM tess.planet_parameter_evidence e JOIN tess_candidate_projection c USING(source_record_id);
        """
    )


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def observed_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    counts: dict[str, int] = {
        "canonical_planets_reference": scalar(con, "SELECT count(*) FROM core.planets"),
        "supplemental_planet_objects": scalar(con, "SELECT count(*) FROM planet_source_bindings"),
        "supplemental_planets_accepted": scalar(con, "SELECT count(*) FROM planet_source_bindings WHERE binding_status='accepted'"),
        "supplemental_planets_missing": scalar(con, "SELECT count(*) FROM planet_source_bindings WHERE binding_status='missing'"),
        "supplemental_planets_ambiguous": scalar(con, "SELECT count(*) FROM planet_source_bindings WHERE binding_status='ambiguous'"),
        "supplemental_lifecycle_rows": scalar(con, "SELECT count(*) FROM planet_lifecycle_projection"),
        "supplemental_parameter_sets": scalar(con, "SELECT count(*) FROM planet_parameter_set_projection"),
        "supplemental_parameter_facts": scalar(con, "SELECT count(*) FROM planet_parameter_projection"),
        "supplemental_conflict_rows": scalar(con, "SELECT count(*) FROM planet_lifecycle_conflicts WHERE has_polarity_conflict"),
        "tess_target_bindings": scalar(con, "SELECT count(*) FROM tess_target_bindings"),
        "tess_targets_accepted": scalar(con, "SELECT count(*) FROM tess_target_bindings WHERE binding_status='accepted'"),
        "tess_targets_missing": scalar(con, "SELECT count(*) FROM tess_target_bindings WHERE binding_status='missing'"),
        "tess_targets_excluded": scalar(con, "SELECT count(*) FROM tess_target_bindings WHERE binding_status='excluded'"),
        "tess_targets_ambiguous": scalar(con, "SELECT count(*) FROM tess_target_bindings WHERE binding_status='ambiguous'"),
        "tess_candidates": scalar(con, "SELECT count(*) FROM tess_candidate_projection"),
        "tess_confirmed": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE disposition_normalized='CONFIRMED'"),
        "tess_confirmed_planet_links": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE canonical_planet_key IS NOT NULL"),
        "tess_candidate_evidence": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE evidence_polarity='candidate'"),
        "tess_negative_evidence": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE evidence_polarity='negative'"),
        "tess_unclassified_evidence": scalar(con, "SELECT count(*) FROM tess_candidate_projection WHERE evidence_polarity IS NULL"),
        "tess_transit_facts": scalar(con, "SELECT count(*) FROM tess_transit_projection"),
        "tess_planet_parameter_facts": scalar(con, "SELECT count(*) FROM tess_planet_parameter_projection"),
        "canonical_inventory_mutations": scalar(con, "SELECT (SELECT count(*) FROM planet_lifecycle_projection WHERE canonical_inventory_mutation)+(SELECT count(*) FROM tess_candidate_projection WHERE canonical_inventory_mutation)"),
    }
    for source, key in (
        ("exoplanet_lifecycle.exoplanet_eu", "eu"),
        ("exoplanet_lifecycle.hwc", "hwc"),
        ("exoplanet_lifecycle.open_exoplanet_catalogue", "oec"),
    ):
        for status in ("accepted", "missing", "ambiguous"):
            counts[f"{key}_planets_{status}"] = scalar(
                con,
                f"SELECT count(*) FROM planet_source_bindings WHERE source_id={sql_literal(source)} AND binding_status={sql_literal(status)}",
            )
    return counts


def verify(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    queries = {
        "duplicate_planet_bindings": "SELECT count(*)-count(DISTINCT binding_id) FROM planet_source_bindings",
        "duplicate_planet_source_objects": "SELECT count(*) FROM (SELECT source_id,source_object_key FROM planet_source_bindings GROUP BY 1,2 HAVING count(*)<>1)",
        "accepted_planets_without_one_candidate": "SELECT count(*) FROM planet_source_bindings WHERE binding_status='accepted' AND (canonical_candidate_count<>1 OR canonical_planet_key IS NULL)",
        "unaccepted_planets_with_canonical_keys": "SELECT count(*) FROM planet_source_bindings WHERE binding_status<>'accepted' AND canonical_planet_key IS NOT NULL",
        "lifecycle_rows_without_bindings": "SELECT count(*) FROM planet_lifecycle_projection WHERE binding_id IS NULL",
        "parameter_sets_without_bindings": "SELECT count(*) FROM planet_parameter_set_projection WHERE binding_id IS NULL",
        "parameter_facts_without_sets": "SELECT count(*) FROM planet_parameter_projection WHERE parameter_set_id IS NULL OR selection_status IS NULL",
        "duplicate_tess_target_bindings": "SELECT count(*)-count(DISTINCT binding_id) FROM tess_target_bindings",
        "duplicate_tess_ids": "SELECT count(*) FROM (SELECT tic_id FROM tess_target_bindings GROUP BY 1 HAVING count(*)<>1)",
        "accepted_tess_without_one_candidate": "SELECT count(*) FROM tess_target_bindings WHERE binding_status='accepted' AND (canonical_candidate_count<>1 OR canonical_star_key IS NULL)",
        "unaccepted_tess_with_canonical_keys": "SELECT count(*) FROM tess_target_bindings WHERE binding_status<>'accepted' AND canonical_star_key IS NOT NULL",
        "candidates_without_host_outcomes": "SELECT count(*) FROM tess_candidate_projection WHERE host_binding_id IS NULL OR host_binding_status IS NULL",
        "confirmed_links_without_positive_disposition": "SELECT count(*) FROM tess_candidate_projection WHERE canonical_planet_key IS NOT NULL AND (disposition_normalized<>'CONFIRMED' OR evidence_polarity<>'positive')",
        "nonconfirmed_planet_links": "SELECT count(*) FROM tess_candidate_projection WHERE disposition_normalized IS DISTINCT FROM 'CONFIRMED' AND canonical_planet_key IS NOT NULL",
        "confirmed_links_without_unique_candidate": "SELECT count(*) FROM tess_candidate_projection WHERE canonical_planet_key IS NOT NULL AND canonical_planet_candidate_count<>1",
        "transit_rows_without_candidates": "SELECT count(*) FROM tess_transit_projection WHERE toi_id IS NULL OR host_binding_status IS NULL",
        "tess_parameter_rows_without_candidates": "SELECT count(*) FROM tess_planet_parameter_projection WHERE toi_id IS NULL OR host_binding_status IS NULL",
        "canonical_inventory_mutations": "SELECT (SELECT count(*) FROM planet_lifecycle_projection WHERE canonical_inventory_mutation)+(SELECT count(*) FROM tess_candidate_projection WHERE canonical_inventory_mutation)",
        "canonical_inventory_tables_created": "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema='main' AND table_name IN ('planets','stars','systems','aliases')",
    }
    result = {name: scalar(con, query) for name, query in queries.items()}
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"planet-evidence verification failed: {failing}")
    return result


def compile_planet_evidence(
    *, policy_path: Path, state: Path, output_root: Path, report_path: Path,
    discover_acceptance: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    policy = read_json(policy_path)
    validate_policy(policy)
    policy_sha = hashlib.sha256(canonical_json(policy)).hexdigest()
    compiler_sha = sha256_file(Path(__file__).resolve())
    build_id = hashlib.sha256(canonical_json({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "release_set": policy["evidence_release_set_id"],
        "identity_graph": policy["identity_graph_id"],
        "canonical_reference": policy["canonical_reference_build_id"],
    })).hexdigest()[:24]
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    destination = output_root / build_id
    con: duckdb.DuckDBPyConnection | None = None
    try:
        database = staging / "selected_planet_evidence.duckdb"
        con = duckdb.connect(str(database))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='12GB'")
        spill = staging / "spill"
        spill.mkdir()
        con.execute(f"SET temp_directory={sql_literal(spill)}")
        input_paths = attach_inputs(con, state=state, policy=policy)
        validate_input_releases(con, policy)
        create_schema(con)
        materialize_supplemental(con, policy)
        materialize_tess(con, policy)
        observed = observed_counts(con)
        checks = verify(con)
        if discover_acceptance:
            report = {
                "schema_version": "spacegate.e5_selected_planet_evidence_discovery.v1",
                "build_id": build_id,
                "observed": observed,
                "verification": checks,
                "status": "discovery_only",
                "wall_seconds": round(time.monotonic() - started, 3),
            }
            write_json(report_path, report)
            con.close()
            con = None
            shutil.rmtree(staging)
            return report
        expected = {str(key): int(value) for key, value in (policy.get("acceptance") or {}).items()}
        if not expected or observed != expected:
            raise ValueError(f"planet-evidence acceptance changed: expected={expected}:observed={observed}")
        canonical_count = observed["canonical_planets_reference"]
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?,?,?,?)",
            [build_id,policy["policy_version"],policy_sha,policy["evidence_release_set_id"],
             policy["identity_graph_id"],policy["canonical_reference_build_id"],
             canonical_count,utc_now(),"pass"],
        )
        con.execute("CHECKPOINT")
        parquet = staging / "parquet"
        parquet.mkdir()
        for table, order_key in TABLE_ORDER:
            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order_key}) TO "
                f"{sql_literal(parquet / (table + '.parquet'))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        con.close()
        con = None
        shutil.rmtree(spill)
        deterministic_files = {
            str(path.relative_to(staging)): {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
            for path in sorted(parquet.glob("*.parquet"))
        }
        input_fingerprints = {
            alias: {"path": str(path), "bytes": path.stat().st_size, "sha256": sha256_file(path)}
            for alias, path in sorted(input_paths.items())
        }
        manifest = {
            "schema_version": "spacegate.e5_selected_planet_evidence.v1",
            "build_id": build_id,
            "policy_version": policy["policy_version"],
            "policy_sha256": policy_sha,
            "compiler_version": policy["compiler_version"],
            "compiler_sha256": compiler_sha,
            "evidence_release_set_id": policy["evidence_release_set_id"],
            "identity_graph_id": policy["identity_graph_id"],
            "canonical_reference_build_id": policy["canonical_reference_build_id"],
            "canonical_planet_count_before": canonical_count,
            "canonical_planet_count_after": canonical_count,
            "observed": observed,
            "verification": checks,
            "input_fingerprints": input_fingerprints,
            "deterministic_files": deterministic_files,
            "generated_at": utc_now(),
            "status": "pass",
        }
        write_json(staging / "manifest.json", manifest)
        if destination.exists():
            existing = read_json(destination / "manifest.json")
            comparable = ("policy_sha256","compiler_sha256","observed","verification","input_fingerprints","deterministic_files")
            if any(existing.get(key) != manifest.get(key) for key in comparable):
                raise ValueError(f"deterministic planet-evidence build differs: {build_id}")
            shutil.rmtree(staging)
        else:
            os.replace(staging, destination)
        report = {**manifest,"artifact_path":str(destination),"wall_seconds":round(time.monotonic()-started,3)}
        write_json(report_path, report)
        return report
    except Exception:
        if con is not None:
            con.close()
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state", type=Path, default=Path(os.environ.get("SPACEGATE_STATE_DIR", DEFAULT_STATE)))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--discover-acceptance", action="store_true")
    args = parser.parse_args()
    output_root = args.output_root or args.state / "derived/evidence_lake_v2/selected_planet_evidence"
    report_path = args.report or args.state / "reports/evidence_lake_v2/e5_selected_planet_evidence_report.json"
    report = compile_planet_evidence(
        policy_path=args.policy,state=args.state,output_root=output_root,
        report_path=report_path,discover_acceptance=args.discover_acceptance,
    )
    print(f"Selected planet evidence {report['status']}: build={report['build_id']} wall={report['wall_seconds']}s")


if __name__ == "__main__":
    main()
