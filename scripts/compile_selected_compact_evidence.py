#!/usr/bin/env python3
"""Compile permanent compact-object identities and safely scoped selected facts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_compact_identity_policies.json"
DEFAULT_STATE = Path("/data/spacegate/state")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def atnf_stable_key(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s+", "", value.strip()).lower()
    if not normalized:
        return None
    encoded = re.sub(r"[^a-z0-9+_-]+", "_", normalized).strip("_")
    return f"compact:atnf:name:{encoded}"


def normalize_mcgill_name(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s*#+\s*$", "", value.strip())
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized or None


def mcgill_stable_key(value: str | None) -> str | None:
    normalized = normalize_mcgill_name(value)
    if normalized is None:
        return None
    identity = hashlib.sha256(normalized.lower().encode("utf-8")).hexdigest()[:24]
    return f"compact:mcgill:magnetar:{identity}"


def atnf_error_scale(value_raw: str | None, uncertainty_raw: str | None) -> float | None:
    if value_raw is None or uncertainty_raw is None:
        return None
    value = value_raw.strip()
    uncertainty = uncertainty_raw.strip()
    if not value or not uncertainty:
        return None
    try:
        error_value = float(uncertainty)
    except ValueError:
        return None
    if uncertainty.startswith("-") or "." in uncertainty:
        return abs(error_value)
    match = re.fullmatch(r"[+-]?(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?", value)
    if not match:
        return None
    decimals = len(match.group(2) or "")
    exponent = int(match.group(3) or 0)
    scale = 10.0 ** (exponent - decimals)
    if scale > 1.0 and len(uncertainty) > 1:
        scale *= 10.0 ** (len(uncertainty) - 1)
    result = abs(error_value) * scale
    return result if math.isfinite(result) else None


def sexagesimal_ra_deg(value: str | None) -> float | None:
    if value is None:
        return None
    parts = re.split(r"[:\s]+", value.strip())
    if len(parts) < 2 or len(parts) > 3:
        return None
    try:
        hours = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2]) if len(parts) == 3 else 0.0
    except ValueError:
        return None
    if not (0 <= hours < 24 and 0 <= minutes < 60 and 0 <= seconds < 60):
        return None
    return 15.0 * (hours + minutes / 60.0 + seconds / 3600.0)


def sexagesimal_dec_deg(value: str | None) -> float | None:
    if value is None:
        return None
    parts = re.split(r"[:\s]+", value.strip())
    if len(parts) < 2 or len(parts) > 3:
        return None
    sign = -1.0 if parts[0].startswith("-") else 1.0
    try:
        degrees = abs(float(parts[0]))
        minutes = float(parts[1])
        seconds = float(parts[2]) if len(parts) == 3 else 0.0
    except ValueError:
        return None
    if not (0 <= degrees <= 90 and 0 <= minutes < 60 and 0 <= seconds < 60):
        return None
    result = sign * (degrees + minutes / 60.0 + seconds / 3600.0)
    return result if -90 <= result <= 90 else None


def validate_policy(policy: dict[str, Any], *, bootstrap_counts: bool) -> None:
    if policy.get("schema_version") != "spacegate.e5_compact_identity_policies.v1":
        raise ValueError("unsupported compact identity policy schema")
    if len(policy.get("sources") or []) != 2:
        raise ValueError("compact identity policy must declare ATNF and McGill")
    if not bootstrap_counts and not policy.get("acceptance"):
        raise ValueError("compact identity acceptance counts are not pinned")
    if float((policy.get("envelope") or {}).get("evidence_radius_pc") or 0) <= 0:
        raise ValueError("compact identity evidence radius must be positive")


def register_functions(con: duckdb.DuckDBPyConnection) -> None:
    varchar = duckdb.sqltypes.VARCHAR
    double = duckdb.sqltypes.DOUBLE
    con.create_function("atnf_stable_key", atnf_stable_key, [varchar], varchar, null_handling="special")
    con.create_function("normalize_mcgill_name", normalize_mcgill_name, [varchar], varchar, null_handling="special")
    con.create_function("mcgill_stable_key", mcgill_stable_key, [varchar], varchar, null_handling="special")
    con.create_function("atnf_error_scale", atnf_error_scale, [varchar, varchar], double, null_handling="special")
    con.create_function("sexagesimal_ra_deg", sexagesimal_ra_deg, [varchar], double, null_handling="special")
    con.create_function("sexagesimal_dec_deg", sexagesimal_dec_deg, [varchar], double, null_handling="special")


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          build_id VARCHAR, policy_version VARCHAR, policy_sha256 VARCHAR,
          canonical_reference_build_id VARCHAR, identity_graph_id VARCHAR,
          generated_at VARCHAR, status VARCHAR
        );
        CREATE TABLE compact_identity_nodes (
          object_node_key VARCHAR, stable_object_key VARCHAR, object_family VARCHAR,
          object_type VARCHAR, display_name VARCHAR, source_id VARCHAR,
          release_id VARCHAR, inventory_namespace VARCHAR,
          inventory_identifier VARCHAR, identity_evidence_id VARCHAR,
          identity_method VARCHAR, identity_version VARCHAR
        );
        CREATE TABLE compact_identity_aliases (
          alias_binding_id VARCHAR, object_node_key VARCHAR, stable_object_key VARCHAR,
          namespace VARCHAR, identifier_raw VARCHAR, identifier_normalized VARCHAR,
          source_record_id VARCHAR, identifier_evidence_id VARCHAR,
          source_id VARCHAR, release_id VARCHAR, binding_method VARCHAR
        );
        CREATE TABLE compact_envelope_outcomes (
          outcome_id VARCHAR, object_node_key VARCHAR, stable_object_key VARCHAR,
          source_id VARCHAR, release_id VARCHAR, outcome VARCHAR, reason VARCHAR,
          distance_method VARCHAR, distance_pc DOUBLE, distance_lower_pc DOUBLE,
          distance_upper_pc DOUBLE, parallax_mas DOUBLE, parallax_error_mas DOUBLE,
          ra_deg DOUBLE, dec_deg DOUBLE, distance_evidence_id VARCHAR,
          ra_evidence_id VARCHAR, dec_evidence_id VARCHAR, policy_version VARCHAR
        );
        CREATE TABLE selected_compact_facts (
          fact_id VARCHAR, object_node_key VARCHAR, stable_object_key VARCHAR,
          quantity_key VARCHAR, value_raw VARCHAR, unit_raw VARCHAR,
          normalized_value DOUBLE, normalized_unit VARCHAR,
          uncertainty_lower DOUBLE, uncertainty_upper DOUBLE,
          value_status VARCHAR, method VARCHAR, model VARCHAR,
          reference_raw VARCHAR, source_evidence_id VARCHAR,
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          policy_version VARCHAR, quality_json JSON
        );
        CREATE TABLE compact_scope_quarantine (
          quarantine_id VARCHAR, source_id VARCHAR, compact_stable_object_key VARCHAR,
          compact_identifier VARCHAR, candidate_stable_object_key VARCHAR,
          candidate_gaia_dr3 VARCHAR, simbad_oid VARCHAR, outcome VARCHAR,
          reason VARCHAR, source_audit_sha256 VARCHAR, policy_version VARCHAR
        );
        """
    )


def materialize(
    con: duckdb.DuckDBPyConnection,
    *,
    policy: dict[str, Any],
    scope_audit: dict[str, Any],
    scope_audit_sha: str,
) -> dict[str, int]:
    policy_version = str(policy["policy_version"])
    envelope_pc = float(policy["envelope"]["evidence_radius_pc"])
    con.execute(
        f"""
        CREATE TEMP TABLE atnf_record_identity AS
        SELECT c.source_record_id,
               coalesce(p.identifier_normalized,c.identifier_normalized) canonical_identifier,
               coalesce(p.evidence_id,c.evidence_id) identity_evidence_id
        FROM atnf.identifier_claim_evidence c
        LEFT JOIN atnf.identifier_claim_evidence p
          ON p.source_record_id=c.source_record_id AND p.namespace='psrj'
        WHERE c.namespace='atnf_pulsar_name';

        INSERT INTO compact_identity_nodes
        SELECT 'object:' || stable_object_key, stable_object_key,
               'neutron_star','pulsar',canonical_identifier,
               'compact.atnf','rolling_snapshot_20260709','atnf_pulsar_name',
               canonical_identifier,min(identity_evidence_id),
               'release_scoped_atnf_inventory_identity',
               'compact_source_identity_v1'
        FROM (
          SELECT DISTINCT atnf_stable_key(canonical_identifier) stable_object_key,
                 canonical_identifier,identity_evidence_id
          FROM atnf_record_identity
        )
        GROUP BY stable_object_key,canonical_identifier
        ORDER BY stable_object_key;

        CREATE TEMP TABLE mcgill_catalog_identity AS
        SELECT DISTINCT normalize_mcgill_name(i.identifier_normalized) canonical_identifier,
               i.evidence_id identity_evidence_id
        FROM mcgill.source_records r
        JOIN mcgill.identifier_claim_evidence i USING(source_record_id)
        WHERE r.source_table='mcgill_magnetar_catalog'
          AND i.namespace='magnetar_name';

        INSERT INTO compact_identity_nodes
        SELECT 'object:' || mcgill_stable_key(canonical_identifier),
               mcgill_stable_key(canonical_identifier),'neutron_star','magnetar',
               canonical_identifier,'compact.mcgill_magnetar',
               'snapshot_20260721_with_bibliography','magnetar_name',
               canonical_identifier,min(identity_evidence_id),
               'release_scoped_mcgill_inventory_identity',
               'compact_source_identity_v1'
        FROM mcgill_catalog_identity GROUP BY canonical_identifier
        ORDER BY canonical_identifier;

        INSERT INTO compact_identity_aliases
        SELECT DISTINCT sha256(concat_ws('|',n.stable_object_key,i.namespace,
                              i.identifier_normalized,i.evidence_id)),
               n.object_node_key,n.stable_object_key,i.namespace,i.identifier_raw,
               i.identifier_normalized,i.source_record_id,i.evidence_id,
               'compact.atnf','rolling_snapshot_20260709',
               'same_source_record_release_identity'
        FROM atnf_record_identity r
        JOIN compact_identity_nodes n
          ON n.stable_object_key=atnf_stable_key(r.canonical_identifier)
        JOIN atnf.identifier_claim_evidence i USING(source_record_id);

        INSERT INTO compact_identity_aliases
        SELECT DISTINCT sha256(concat_ws('|',n.stable_object_key,i.namespace,
                              i.identifier_normalized,i.evidence_id)),
               n.object_node_key,n.stable_object_key,i.namespace,i.identifier_raw,
               i.identifier_normalized,i.source_record_id,i.evidence_id,
               'compact.mcgill_magnetar','snapshot_20260721_with_bibliography',
               'exact_normalized_release_name'
        FROM mcgill.identifier_claim_evidence i
        JOIN compact_identity_nodes n
          ON n.source_id='compact.mcgill_magnetar'
         AND lower(n.inventory_identifier)=lower(normalize_mcgill_name(i.identifier_normalized))
        WHERE i.namespace='magnetar_name';

        CREATE TEMP TABLE atnf_parameters AS
        SELECT atnf_stable_key(json_extract_string(parameter_set_key,'$.pulsar_name')) stable_object_key,
               json_extract_string(parameter_set_raw,'$.parameter_name') parameter_name,
               json_extract_string(parameter_set_raw,'$.value_raw') value_raw,
               json_extract_string(parameter_set_raw,'$.uncertainty_raw') uncertainty_raw,
               evidence_id,source_record_id,reference_raw
        FROM atnf.compact_object_evidence
        WHERE compact_kind='pulsar_parameter';

        CREATE TEMP TABLE atnf_context AS
        WITH ranked AS (
          SELECT *,row_number() OVER (
            PARTITION BY stable_object_key,parameter_name ORDER BY evidence_id
          ) rank_number
          FROM atnf_parameters WHERE parameter_name IN ('PX','RAJ','DECJ')
        )
        SELECT stable_object_key,
               max(try_cast(value_raw AS DOUBLE)) FILTER (WHERE parameter_name='PX') px,
               max(atnf_error_scale(value_raw,uncertainty_raw)) FILTER (WHERE parameter_name='PX') px_error,
               max(sexagesimal_ra_deg(value_raw)) FILTER (WHERE parameter_name='RAJ') ra_deg,
               max(sexagesimal_dec_deg(value_raw)) FILTER (WHERE parameter_name='DECJ') dec_deg,
               max(evidence_id) FILTER (WHERE parameter_name='PX') px_evidence_id,
               max(evidence_id) FILTER (WHERE parameter_name='RAJ') ra_evidence_id,
               max(evidence_id) FILTER (WHERE parameter_name='DECJ') dec_evidence_id
        FROM ranked WHERE rank_number=1 GROUP BY stable_object_key;

        INSERT INTO compact_envelope_outcomes
        SELECT sha256(concat_ws('|',n.stable_object_key,'envelope',{sql_literal(policy_version)})),
               n.object_node_key,n.stable_object_key,n.source_id,n.release_id,
               CASE WHEN c.px IS NULL OR NOT isfinite(c.px)
                      THEN 'missing'
                    WHEN c.px + coalesce(c.px_error,0) >= 1000.0/{envelope_pc}
                      THEN 'accepted'
                    ELSE 'excluded' END,
               CASE WHEN c.px IS NULL OR NOT isfinite(c.px)
                      THEN 'no usable source parallax for envelope membership'
                    WHEN c.px + coalesce(c.px_error,0) >= 1000.0/{envelope_pc}
                      THEN 'source parallax interval overlaps the 1,250-ly evidence envelope'
                    ELSE 'source parallax interval is wholly outside the 1,250-ly evidence envelope' END,
               'inverse_atnf_source_parallax_interval_v1',
               CASE WHEN c.px>0 THEN 1000.0/c.px END,
               CASE WHEN c.px + coalesce(c.px_error,0)>0
                    THEN 1000.0/(c.px + coalesce(c.px_error,0)) END,
               CASE WHEN c.px - coalesce(c.px_error,0)>0
                    THEN 1000.0/(c.px - coalesce(c.px_error,0)) END,
               c.px,c.px_error,c.ra_deg,c.dec_deg,c.px_evidence_id,
               c.ra_evidence_id,c.dec_evidence_id,{sql_literal(policy_version)}
        FROM compact_identity_nodes n LEFT JOIN atnf_context c USING(stable_object_key)
        WHERE n.source_id='compact.atnf';

        CREATE TEMP TABLE mcgill_distance AS
        SELECT mcgill_stable_key(json_extract_string(parameter_set_key,'$.Name')) stable_object_key,
               try_cast(json_extract_string(parameter_set_raw,'$.Dist') AS DOUBLE) distance_kpc,
               try_cast(json_extract_string(parameter_set_raw,'$.Dist_EDn') AS DOUBLE) error_down_kpc,
               try_cast(json_extract_string(parameter_set_raw,'$.Dist_EUp') AS DOUBLE) error_up_kpc,
               json_extract_string(parameter_set_raw,'$.Dist_lim') limit_raw,
               evidence_id
        FROM mcgill.compact_object_evidence WHERE compact_kind='magnetar_distance';

        CREATE TEMP TABLE mcgill_position AS
        SELECT mcgill_stable_key(json_extract_string(parameter_set_key,'$.Name')) stable_object_key,
               sexagesimal_ra_deg(json_extract_string(parameter_set_raw,'$.RA')) ra_deg,
               sexagesimal_dec_deg(json_extract_string(parameter_set_raw,'$.Decl')) dec_deg,
               evidence_id
        FROM mcgill.compact_object_evidence WHERE compact_kind='magnetar_position';

        INSERT INTO compact_envelope_outcomes
        SELECT sha256(concat_ws('|',n.stable_object_key,'envelope',{sql_literal(policy_version)})),
               n.object_node_key,n.stable_object_key,n.source_id,n.release_id,
               CASE WHEN d.distance_kpc IS NULL OR NOT isfinite(d.distance_kpc)
                      THEN 'missing'
                    WHEN greatest(0,d.distance_kpc-coalesce(d.error_down_kpc,0))*1000.0 <= {envelope_pc}
                      THEN 'accepted'
                    ELSE 'excluded' END,
               CASE WHEN d.distance_kpc IS NULL OR NOT isfinite(d.distance_kpc)
                      THEN 'no usable source distance for envelope membership'
                    WHEN greatest(0,d.distance_kpc-coalesce(d.error_down_kpc,0))*1000.0 <= {envelope_pc}
                      THEN 'source distance interval overlaps the 1,250-ly evidence envelope'
                    ELSE 'source distance interval is wholly outside the 1,250-ly evidence envelope' END,
               'mcgill_source_distance_interval_v1',d.distance_kpc*1000.0,
               greatest(0,d.distance_kpc-coalesce(d.error_down_kpc,0))*1000.0,
               (d.distance_kpc+coalesce(d.error_up_kpc,0))*1000.0,
               NULL,NULL,p.ra_deg,p.dec_deg,d.evidence_id,p.evidence_id,p.evidence_id,
               {sql_literal(policy_version)}
        FROM compact_identity_nodes n
        LEFT JOIN mcgill_distance d USING(stable_object_key)
        LEFT JOIN mcgill_position p USING(stable_object_key)
        WHERE n.source_id='compact.mcgill_magnetar';
        """
    )

    quantity_rows = policy["atnf_selected_quantities"]
    con.execute(
        "CREATE TEMP TABLE atnf_quantity_policy (parameter_name VARCHAR, quantity_key VARCHAR, unit_raw VARCHAR, normalized_unit VARCHAR)"
    )
    con.executemany(
        "INSERT INTO atnf_quantity_policy VALUES (?,?,?,?)",
        [
            (row["parameter_name"], row["quantity_key"], row["unit"], row["normalized_unit"])
            for row in quantity_rows
        ],
    )
    con.execute(
        f"""
        INSERT INTO selected_compact_facts
        WITH eligible AS (
          SELECT p.*,q.quantity_key,q.unit_raw,q.normalized_unit,
                 CASE WHEN p.parameter_name='RAJ' THEN sexagesimal_ra_deg(p.value_raw)
                      WHEN p.parameter_name='DECJ' THEN sexagesimal_dec_deg(p.value_raw)
                      ELSE try_cast(p.value_raw AS DOUBLE) END normalized_value,
                 CASE WHEN p.parameter_name IN ('RAJ','DECJ') THEN NULL
                      ELSE atnf_error_scale(p.value_raw,p.uncertainty_raw) END normalized_error,
                 row_number() OVER (
                   PARTITION BY p.stable_object_key,q.quantity_key ORDER BY p.evidence_id
                 ) rank_number
          FROM atnf_parameters p JOIN atnf_quantity_policy q USING(parameter_name)
          JOIN compact_envelope_outcomes o USING(stable_object_key)
          WHERE o.outcome='accepted'
        )
        SELECT sha256(concat_ws('|',n.stable_object_key,e.quantity_key,
                              e.evidence_id,{sql_literal(policy_version)})),
               n.object_node_key,n.stable_object_key,e.quantity_key,e.value_raw,
               e.unit_raw,e.normalized_value,e.normalized_unit,
               e.normalized_error,e.normalized_error,'source_measurement',
               'atnf_source_parameter_occurrence',NULL,e.reference_raw,
               e.evidence_id,e.source_record_id,n.source_id,n.release_id,
               {sql_literal(policy_version)},
               json_object('source_parameter_name',e.parameter_name,
                           'source_uncertainty_raw',e.uncertainty_raw,
                           'uncertainty_semantics','ATNF catalog last-digit uncertainty')
        FROM eligible e JOIN compact_identity_nodes n USING(stable_object_key)
        WHERE e.rank_number=1 AND e.normalized_value IS NOT NULL;
        """
    )

    for row in scope_audit.get("matched_target_diagnostics") or []:
        compact_key = atnf_stable_key(str(row.get("source_name") or ""))
        if not compact_key:
            continue
        con.execute(
            "INSERT INTO compact_scope_quarantine VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                sha256_bytes(canonical_json({"compact": compact_key, "candidate": row.get("stable_object_key"), "policy": policy_version})),
                row.get("source_id"),
                compact_key,
                row.get("source_name"),
                row.get("stable_object_key"),
                row.get("gaia_dr3"),
                row.get("simbad_oid"),
                "quarantined_component_scope_conflict",
                "exact SIMBAD/Gaia route reaches an optical stellar leaf; preserve as a candidate counterpart relation and never merge identities",
                scope_audit_sha,
                policy_version,
            ],
        )

    observed: dict[str, int] = {}
    for key, query in {
        "identity_nodes": "SELECT count(*) FROM compact_identity_nodes",
        "atnf_identity_nodes": "SELECT count(*) FROM compact_identity_nodes WHERE source_id='compact.atnf'",
        "mcgill_identity_nodes": "SELECT count(*) FROM compact_identity_nodes WHERE source_id='compact.mcgill_magnetar'",
        "identity_aliases": "SELECT count(*) FROM compact_identity_aliases",
        "envelope_accepted": "SELECT count(*) FROM compact_envelope_outcomes WHERE outcome='accepted'",
        "envelope_excluded": "SELECT count(*) FROM compact_envelope_outcomes WHERE outcome='excluded'",
        "envelope_missing": "SELECT count(*) FROM compact_envelope_outcomes WHERE outcome='missing'",
        "selected_facts": "SELECT count(*) FROM selected_compact_facts",
        "scope_quarantine": "SELECT count(*) FROM compact_scope_quarantine",
    }.items():
        observed[key] = int(con.execute(query).fetchone()[0])
    return observed


def verify(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    queries = {
        "duplicate_object_node_keys": "SELECT count(*)-count(DISTINCT object_node_key) FROM compact_identity_nodes",
        "duplicate_stable_object_keys": "SELECT count(*)-count(DISTINCT stable_object_key) FROM compact_identity_nodes",
        "duplicate_alias_binding_ids": "SELECT count(*)-count(DISTINCT alias_binding_id) FROM compact_identity_aliases",
        "duplicate_outcome_ids": "SELECT count(*)-count(DISTINCT outcome_id) FROM compact_envelope_outcomes",
        "nodes_without_one_outcome": "SELECT count(*) FROM (SELECT n.object_node_key,count(o.outcome_id) n FROM compact_identity_nodes n LEFT JOIN compact_envelope_outcomes o USING(object_node_key) GROUP BY 1 HAVING count(o.outcome_id)<>1)",
        "aliases_without_nodes": "SELECT count(*) FROM compact_identity_aliases a LEFT JOIN compact_identity_nodes n USING(object_node_key) WHERE n.object_node_key IS NULL",
        "facts_without_accepted_outcome": "SELECT count(*) FROM selected_compact_facts f LEFT JOIN compact_envelope_outcomes o USING(object_node_key) WHERE o.outcome<>'accepted' OR o.outcome IS NULL",
        "facts_without_evidence": "SELECT count(*) FROM selected_compact_facts WHERE source_evidence_id IS NULL OR source_record_id IS NULL",
        "atnf_sign_collisions": "SELECT count(*) FROM (SELECT stable_object_key FROM compact_identity_nodes WHERE source_id='compact.atnf' GROUP BY 1 HAVING count(*)>1)",
        "invalid_outcomes": "SELECT count(*) FROM compact_envelope_outcomes WHERE outcome NOT IN ('accepted','excluded','missing','ambiguous','quarantined')",
        "quarantine_merged_into_compact": "SELECT count(*) FROM compact_scope_quarantine q JOIN compact_identity_nodes n ON n.stable_object_key=q.candidate_stable_object_key",
    }
    result = {key: int(con.execute(query).fetchone()[0] or 0) for key, query in queries.items()}
    failing = {key: value for key, value in result.items() if value}
    if failing:
        raise ValueError(f"compact identity verification failed: {failing}")
    return result


def compile_compact(
    *,
    policy_path: Path,
    state: Path,
    output_root: Path,
    report_path: Path,
    bootstrap_counts: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    policy = read_json(policy_path)
    validate_policy(policy, bootstrap_counts=bootstrap_counts)
    policy_sha = sha256_bytes(canonical_json(policy))
    compiler_sha = sha256_file(Path(__file__).resolve())
    source_paths: dict[str, Path] = {}
    for source in policy["sources"]:
        path = state / "derived/evidence_lake_v2/scientific_evidence" / source["evidence_build_id"] / "scientific_evidence.duckdb"
        if not path.is_file():
            raise FileNotFoundError(path)
        actual = sha256_file(path)
        if actual != source["database_sha256"]:
            raise ValueError(f"compact source hash mismatch: {source['source_id']}")
        source_paths[source["source_id"]] = path
    scope_path = state / policy["scope_audit_report"]
    scope_audit = read_json(scope_path)
    scope_sha = sha256_file(scope_path)
    build_id = sha256_bytes(canonical_json({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "source_build_ids": [source["evidence_build_id"] for source in policy["sources"]],
        "scope_audit_sha256": scope_sha,
    }))[:24]
    output_root.mkdir(parents=True, exist_ok=True)
    destination = output_root / build_id
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        database = staging / "selected_compact.duckdb"
        con = duckdb.connect(str(database))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        register_functions(con)
        con.execute(f"ATTACH {sql_literal(source_paths['compact.atnf'])} AS atnf (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(source_paths['compact.mcgill_magnetar'])} AS mcgill (READ_ONLY)")
        create_schema(con)
        observed = materialize(con, policy=policy, scope_audit=scope_audit, scope_audit_sha=scope_sha)
        expected = {key: int(value) for key, value in (policy.get("acceptance") or {}).items()}
        if not bootstrap_counts and observed != expected:
            raise ValueError(f"compact acceptance counts changed: expected={expected}:observed={observed}")
        checks = verify(con)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?,?)",
            [build_id, policy["policy_version"], policy_sha, policy["canonical_reference_build_id"], policy["identity_graph_id"], utc_now(), "pass"],
        )
        con.execute("CHECKPOINT")
        parquet = staging / "parquet"
        parquet.mkdir()
        exports = (
            ("compact_identity_nodes", "object_node_key"),
            ("compact_identity_aliases", "alias_binding_id"),
            ("compact_envelope_outcomes", "outcome_id"),
            ("selected_compact_facts", "fact_id"),
            ("compact_scope_quarantine", "quarantine_id"),
        )
        for table, order_key in exports:
            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order_key}) TO "
                f"{sql_literal(parquet / (table + '.parquet'))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        con.close()
        deterministic_files = {
            str(path.relative_to(staging)): {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
            for path in sorted(parquet.glob("*.parquet"))
        }
        manifest = {
            "schema_version": "spacegate.e5_selected_compact.v1",
            "build_id": build_id,
            "policy_version": policy["policy_version"],
            "policy_sha256": policy_sha,
            "compiler_version": policy["compiler_version"],
            "compiler_sha256": compiler_sha,
            "canonical_reference_build_id": policy["canonical_reference_build_id"],
            "identity_graph_id": policy["identity_graph_id"],
            "scope_audit_sha256": scope_sha,
            "observed": observed,
            "expected": expected,
            "verification": checks,
            "deterministic_files": deterministic_files,
            "generated_at": utc_now(),
            "status": "pass",
        }
        write_json(staging / "manifest.json", manifest)
        if destination.exists():
            prior = read_json(destination / "manifest.json")
            comparable = ("policy_sha256", "compiler_sha256", "scope_audit_sha256", "observed", "verification", "deterministic_files")
            if any(prior.get(key) != manifest.get(key) for key in comparable):
                raise ValueError(f"deterministic compact build differs: {build_id}")
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
    parser.add_argument("--bootstrap-counts", action="store_true")
    args = parser.parse_args()
    output_root = args.output_root or args.state / "derived/evidence_lake_v2/selected_compact_objects"
    report_path = args.report or args.state / "reports/evidence_lake_v2/e5_selected_compact_report.json"
    report = compile_compact(
        policy_path=args.policy,
        state=args.state.resolve(),
        output_root=output_root.resolve(),
        report_path=report_path.resolve(),
        bootstrap_counts=args.bootstrap_counts,
    )
    print(f"Selected compact evidence pass: build={report['build_id']} observed={report['observed']} wall={report['wall_seconds']}s")


if __name__ == "__main__":
    main()
