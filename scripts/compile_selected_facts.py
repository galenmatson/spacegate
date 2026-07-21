#!/usr/bin/env python3
"""Compile E4 evidence shards into immutable, provenance-bearing selected facts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

import audit_e5_source_dispositions as source_disposition_audit


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_selection_policies.json"
DEFAULT_DISPOSITIONS = ROOT / "config/evidence_lake/e5_source_dispositions.json"
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def stable_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier in selection policy: {value!r}")
    return f'"{value}"'


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def json_sql_literal(value: Any) -> str:
    return f"{sql_literal(json.dumps(value, ensure_ascii=False, sort_keys=True))}::JSON"


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def release_set_paths(state_dir: Path, policy: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    release_set_id = str(policy["evidence_release_set_id"])
    root = state_dir / "derived/evidence_lake_v2/scientific_evidence_sets"
    manifest_path = root / release_set_id / "manifest.json"
    manifest = load_json(manifest_path)
    if manifest.get("release_set_id") != release_set_id:
        raise ValueError("selection policy/release-set identity mismatch")
    if manifest.get("release_set_sha256") != policy.get("evidence_release_set_sha256"):
        raise ValueError("selection policy/release-set content hash mismatch")
    if manifest.get("status") != "pass":
        raise ValueError("selected E4 release set is not pass")
    return manifest_path, manifest


def member_by_source(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for member in manifest.get("members") or []:
        for source_id in member.get("source_ids") or []:
            if source_id in result:
                raise ValueError(f"release set repeats source: {source_id}")
            result[source_id] = member
    return result


def validate_policy(policy: dict[str, Any], release_manifest: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.selected_fact_policy.v1":
        raise ValueError("unsupported selected-fact policy schema")
    members = member_by_source(release_manifest)
    seen_groups: set[tuple[str, str]] = set()
    for source in policy.get("selection_sources") or []:
        source_id = str(source.get("source_id") or "")
        if source_id not in members:
            raise ValueError(f"selection source absent from E4 release set: {source_id}")
        storage = str(source.get("storage") or "eav")
        if storage not in {"eav", "coherent_array", "measurement_bundle"}:
            raise ValueError(f"unsupported selection storage: {source_id}:{storage}")
        sql_identifier(str(source["parameter_set_table"]))
        if storage == "eav":
            sql_identifier(str(source["parameter_evidence_table"]))
        elif storage == "coherent_array":
            sql_identifier(str(source["schema_table"]))
            sql_identifier(str(source.get("values_field") or "values_json"))
        else:
            sql_identifier(str(source["bundle_table"]))
            sql_identifier(str(source.get("bundle_id_field") or "bundle_id"))
            sql_identifier(str(source.get("measurements_field") or "measurements"))
        selection_mode = str(source.get("selection_mode") or "ranked_candidates")
        if selection_mode not in {"ranked_candidates", "authoritative_direct"}:
            raise ValueError(f"unsupported selection mode: {source_id}:{selection_mode}")
        if selection_mode == "authoritative_direct" and storage not in {
            "coherent_array", "measurement_bundle"
        }:
            raise ValueError(
                f"authoritative-direct selection requires coherent arrays or measurement bundles: {source_id}"
            )
        binding = source.get("binding") or {}
        if binding.get("strategy") not in {
            "canonical_identifier", "canonical_unique_name",
            "authoritative_release_equivalence",
        }:
            raise ValueError(f"unsupported selection binding strategy: {binding.get('strategy')}")
        if binding.get("strategy") == "authoritative_release_equivalence":
            equivalence = binding.get("release_equivalence") or {}
            required = {
                "source_release", "canonical_release", "relationship",
                "authority_url", "authority_statement",
            }
            if not required.issubset(equivalence) or equivalence.get("source_list_identical") is not True:
                raise ValueError(
                    f"release-equivalence binding lacks authoritative contract: {source_id}"
                )
            if binding.get("claim_namespace") == binding.get("canonical_namespace"):
                raise ValueError(
                    f"release-equivalence binding must preserve distinct namespaces: {source_id}"
                )
        source_quantities: set[str] = set()
        for group in source.get("quantity_groups") or []:
            group_key = str(group.get("group_key") or "")
            key = (source_id, group_key)
            if not group_key or key in seen_groups:
                raise ValueError(f"missing or duplicate quantity group: {key}")
            seen_groups.add(key)
            authorities = group.get("authorities") or []
            ranks = [int(item["rank"]) for item in authorities]
            if not authorities or any(rank <= 0 for rank in ranks):
                raise ValueError(f"quantity group has no positive authority rules: {key}")
            if selection_mode == "authoritative_direct" and len(authorities) != 1:
                raise ValueError(f"authoritative-direct group requires one authority rule: {key}")
            for rule in authorities:
                validate_quality_rule(rule, source_id=source_id, group_key=group_key)
                if str(source.get("storage") or "eav") == "coherent_array":
                    quality_scopes = {
                        str(item.get("scope") or "")
                        for item in rule.get("quality_conditions") or []
                    }
                    if rule.get("quality_order"):
                        quality_scopes.add(str(rule["quality_order"].get("scope") or ""))
                    if "evidence_quality" in quality_scopes:
                        raise ValueError(
                            "coherent-array quality rules cannot use evidence scope: "
                            f"{source_id}:{group_key}"
                        )
            for source_quantity, selected_spec in (group.get("quantities") or {}).items():
                if source_quantity in source_quantities:
                    raise ValueError(f"source quantity appears in multiple groups: {source_id}:{source_quantity}")
                source_quantities.add(source_quantity)
                if isinstance(selected_spec, dict):
                    selected_quantity = str(selected_spec.get("quantity_key") or "")
                    if storage not in {"coherent_array", "measurement_bundle"} or not selected_quantity:
                        raise ValueError(f"invalid structured quantity mapping: {source_id}:{source_quantity}")
                elif not str(selected_spec):
                    raise ValueError(f"blank selected quantity: {source_id}:{source_quantity}")


def validate_quality_rule(
    rule: dict[str, Any], *, source_id: str, group_key: str
) -> None:
    allowed_scopes = {
        "evidence_quality", "parameter_set_quality", "source_context"
    }
    allowed_operators = {
        "eq", "ne", "gt", "gte", "lt", "lte", "bitmask_none", "not_null"
    }
    for condition in rule.get("quality_conditions") or []:
        scope = str(condition.get("scope") or "")
        operator = str(condition.get("operator") or "")
        path = str(condition.get("path") or "")
        if scope not in allowed_scopes or operator not in allowed_operators:
            raise ValueError(
                f"invalid quality condition: {source_id}:{group_key}:{scope}:{operator}"
            )
        if not path.startswith("$.") or len(path) > 256:
            raise ValueError(f"invalid quality JSON path: {source_id}:{group_key}:{path!r}")
        if operator != "not_null" and "value" not in condition:
            raise ValueError(
                f"quality condition lacks value: {source_id}:{group_key}:{path}"
            )
        if operator == "bitmask_none" and int(condition.get("value") or 0) < 0:
            raise ValueError(
                f"quality bitmask must be nonnegative: {source_id}:{group_key}:{path}"
            )
    quality_order = rule.get("quality_order")
    if quality_order is not None:
        scope = str(quality_order.get("scope") or "")
        path = str(quality_order.get("path") or "")
        direction = str(quality_order.get("direction") or "desc")
        if scope not in allowed_scopes or not path.startswith("$.") or len(path) > 256:
            raise ValueError(
                f"invalid quality order: {source_id}:{group_key}:{scope}:{path!r}"
            )
        if direction not in {"asc", "desc"}:
            raise ValueError(
                f"invalid quality order direction: {source_id}:{group_key}:{direction}"
            )


def quality_json_expression(
    scope: str, *, source_alias: str, set_alias: str, evidence_alias: str
) -> str:
    if scope == "evidence_quality":
        return f"{evidence_alias}.quality_json"
    if scope == "parameter_set_quality":
        return f"{set_alias}.quality_json"
    if scope == "source_context":
        return f"{source_alias}.source_context_json"
    raise ValueError(f"unsupported quality JSON scope: {scope}")


def quality_condition_sql(
    condition: dict[str, Any], *, source_alias: str, set_alias: str,
    evidence_alias: str,
) -> str:
    expression = quality_json_expression(
        str(condition["scope"]), source_alias=source_alias,
        set_alias=set_alias, evidence_alias=evidence_alias,
    )
    extracted = f"json_extract_string({expression}, {sql_literal(condition['path'])})"
    operator = str(condition["operator"])
    if operator == "not_null":
        return f"{extracted} IS NOT NULL"
    value = condition["value"]
    value_type = str(condition.get("value_type") or "number")
    if operator == "bitmask_none":
        return f"(try_cast({extracted} AS UBIGINT) & {int(value)}) = 0"
    if value_type == "string":
        left = extracted
        right = sql_literal(value)
    elif value_type == "boolean":
        left = f"try_cast({extracted} AS BOOLEAN)"
        right = "TRUE" if bool(value) else "FALSE"
    else:
        left = f"try_cast({extracted} AS DOUBLE)"
        right = repr(float(value))
    sql_operator = {
        "eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
    }[operator]
    return f"{left} {sql_operator} {right}"


def authority_condition(
    rule: dict[str, Any], source_alias: str, set_alias: str,
    evidence_alias: str = "pe",
) -> str:
    conditions: list[str] = []
    for field, alias in (("source_table", source_alias), ("method", set_alias), ("model", set_alias), ("parameter_set_kind", set_alias)):
        if rule.get(field) is not None:
            conditions.append(f"{alias}.{sql_identifier(field)} = {sql_literal(rule[field])}")
    if rule.get("context_field") is not None:
        context_field = str(rule["context_field"])
        if not SAFE_IDENTIFIER.fullmatch(context_field):
            raise ValueError(f"unsafe source context field: {context_field!r}")
        conditions.append(
            f"json_extract_string({source_alias}.source_context_json, {sql_literal('$.' + context_field)}) "
            f"= {sql_literal(rule.get('context_value'))}"
        )
    conditions.extend(
        quality_condition_sql(
            condition, source_alias=source_alias, set_alias=set_alias,
            evidence_alias=evidence_alias,
        )
        for condition in rule.get("quality_conditions") or []
    )
    return " AND ".join(conditions) if conditions else "TRUE"


def quality_score_sql(
    rule: dict[str, Any], *, source_alias: str = "sr", set_alias: str = "ps",
    evidence_alias: str = "pe",
) -> str:
    quality_order = rule.get("quality_order")
    if quality_order is None:
        return "NULL::DOUBLE"
    expression = quality_json_expression(
        str(quality_order["scope"]), source_alias=source_alias,
        set_alias=set_alias, evidence_alias=evidence_alias,
    )
    extracted = (
        f"try_cast(json_extract_string({expression}, "
        f"{sql_literal(quality_order['path'])}) AS DOUBLE)"
    )
    return f"-({extracted})" if quality_order.get("direction", "desc") == "asc" else extracted


def authority_case(group: dict[str, Any], *, value: str) -> str:
    clauses: list[str] = []
    for rule in group["authorities"]:
        condition = authority_condition(rule, "sr", "ps")
        clauses.append(f"WHEN {condition} THEN {sql_literal(rule[value])}")
    return "CASE " + " ".join(clauses) + " ELSE NULL END"


def quality_score_case(group: dict[str, Any]) -> str:
    clauses: list[str] = []
    for rule in group["authorities"]:
        condition = authority_condition(rule, "sr", "ps", "pe")
        clauses.append(f"WHEN {condition} THEN {quality_score_sql(rule)}")
    return "CASE " + " ".join(clauses) + " ELSE NULL END"


def quantity_values(source: dict[str, Any]) -> str:
    rows: list[str] = []
    for group in source["quantity_groups"]:
        for source_quantity, selected_spec in group["quantities"].items():
            selected_quantity = (
                selected_spec["quantity_key"] if isinstance(selected_spec, dict) else selected_spec
            )
            rows.append(
                "(" + ",".join(
                    [sql_literal(source_quantity), sql_literal(selected_quantity), sql_literal(group["group_key"])]
                ) + ")"
            )
    return ",".join(rows)


def coherent_field_specs(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
) -> dict[str, dict[str, Any]]:
    schema_table = sql_identifier(str(source["schema_table"]))
    rows = con.execute(
        f"SELECT DISTINCT schema_json::VARCHAR FROM {source_alias}.{schema_table}"
    ).fetchall()
    if not rows:
        raise ValueError(f"coherent source has no schemas: {source['source_id']}")
    requested = {
        source_quantity
        for group in source["quantity_groups"]
        for source_quantity in group["quantities"]
    }
    requested.update(
        str(spec["uncertainty_field"])
        for group in source["quantity_groups"]
        for spec in group["quantities"].values()
        if isinstance(spec, dict) and spec.get("uncertainty_field")
    )
    resolved: dict[str, dict[str, Any]] = {}
    for (schema_raw,) in rows:
        schema = json.loads(schema_raw)
        fields = {str(field["name"]): field for field in schema.get("fields") or []}
        missing = sorted(requested - set(fields))
        if missing:
            raise ValueError(f"coherent schema missing requested fields: {source['source_id']}:{missing}")
        for field_name in requested:
            projection = {
                "position": int(fields[field_name]["position"]),
                "datatype": str(fields[field_name].get("datatype") or ""),
                "unit": str(fields[field_name].get("unit") or ""),
            }
            previous = resolved.get(field_name)
            if previous is not None and previous != projection:
                raise ValueError(
                    f"coherent schema field changed across source tables: "
                    f"{source['source_id']}:{field_name}:{previous}:{projection}"
                )
            resolved[field_name] = projection
    return resolved


def authority_sql(source: dict[str, Any]) -> tuple[str, str, str]:
    rank_clauses: list[str] = []
    reason_clauses: list[str] = []
    quality_clauses: list[str] = []
    for group in source["quantity_groups"]:
        group_literal = sql_literal(group["group_key"])
        rank_clauses.append(
            f"WHEN q.group_key = {group_literal} THEN {authority_case(group, value='rank')}"
        )
        reason_clauses.append(
            f"WHEN q.group_key = {group_literal} THEN {authority_case(group, value='reason')}"
        )
        quality_clauses.append(
            f"WHEN q.group_key = {group_literal} THEN {quality_score_case(group)}"
        )
    return (
        "CASE " + " ".join(rank_clauses) + " ELSE NULL END",
        "CASE " + " ".join(reason_clauses) + " ELSE NULL END",
        "CASE " + " ".join(quality_clauses) + " ELSE NULL END",
    )


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE evidence_build (
          selected_fact_build_id VARCHAR,
          policy_version VARCHAR,
          policy_sha256 VARCHAR,
          evidence_release_set_id VARCHAR,
          evidence_release_set_sha256 VARCHAR,
          identity_graph_id VARCHAR,
          identity_graph_sha256 VARCHAR,
          canonical_reference_build_id VARCHAR,
          canonical_reference_sha256 VARCHAR,
          compiler_version VARCHAR,
          compiler_sha256 VARCHAR,
          created_at TIMESTAMP,
          status VARCHAR
        );
        CREATE TABLE selection_source_accounting (
          source_id VARCHAR,
          release_id VARCHAR,
          evidence_build_id VARCHAR,
          object_type VARCHAR,
          eligible_source_records BIGINT,
          accepted_current_bindings BIGINT,
          excluded_or_unresolved_records BIGINT,
          candidate_parameter_sets BIGINT,
          selected_parameter_sets BIGINT,
          selected_facts BIGINT
        );
        CREATE TABLE evidence_object_bindings (
          binding_id VARCHAR,
          source_id VARCHAR,
          release_id VARCHAR,
          evidence_build_id VARCHAR,
          source_record_id VARCHAR,
          binding_scope VARCHAR,
          object_type VARCHAR,
          canonical_object_node_key VARCHAR,
          stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,
          binding_status VARCHAR,
          binding_method VARCHAR,
          binding_reason VARCHAR
        );
        CREATE TABLE parameter_set_selection_decisions (
          decision_id VARCHAR,
          object_type VARCHAR,
          stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,
          quantity_group VARCHAR,
          selected_parameter_set_id VARCHAR,
          selected_source_record_id VARCHAR,
          selected_source_id VARCHAR,
          selected_release_id VARCHAR,
          selected_evidence_build_id VARCHAR,
          authority_rank INTEGER,
          authority_reason VARCHAR,
          selection_quality_score DOUBLE,
          selected_quantity_count INTEGER,
          selected_uncertainty_count INTEGER,
          candidate_parameter_set_count INTEGER,
          runner_up_parameter_set_id VARCHAR,
          runner_up_authority_rank INTEGER,
          runner_up_quality_score DOUBLE,
          policy_version VARCHAR
        );
        CREATE TABLE selected_facts (
          selected_fact_id VARCHAR,
          object_type VARCHAR,
          stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,
          quantity_group VARCHAR,
          quantity_key VARCHAR,
          value_raw VARCHAR,
          normalized_value DOUBLE,
          normalized_unit VARCHAR,
          value_lower DOUBLE,
          value_upper DOUBLE,
          interval_semantics VARCHAR,
          fact_status VARCHAR,
          evidence_build_id VARCHAR,
          evidence_table VARCHAR,
          evidence_id VARCHAR,
          parameter_set_id VARCHAR,
          source_record_id VARCHAR,
          source_id VARCHAR,
          release_id VARCHAR,
          method VARCHAR,
          model VARCHAR,
          reference_raw VARCHAR,
          selection_decision_id VARCHAR,
          authority_rank INTEGER,
          authority_reason VARCHAR,
          policy_version VARCHAR,
          normalization_version VARCHAR,
          quality_json JSON
        );
        CREATE TABLE selected_fact_derivations (
          derivation_id VARCHAR,
          output_selected_fact_id VARCHAR,
          stable_object_key VARCHAR,
          quantity_key VARCHAR,
          algorithm_key VARCHAR,
          algorithm_version VARCHAR,
          input_selected_fact_ids_json JSON,
          applicability VARCHAR,
          formula VARCHAR,
          assumptions_json JSON,
          uncertainty_method VARCHAR,
          confidence_tier VARCHAR,
          supersedes_json JSON,
          policy_version VARCHAR
        );
        """
    )


def create_binding(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> tuple[int, int]:
    source_id = str(source["source_id"])
    object_type = str(source["object_type"])
    binding = source["binding"]
    storage = str(source.get("storage") or "eav")
    set_table = sql_identifier(str(source["parameter_set_table"]))
    component_filter = ""
    if source.get("component_scope_field"):
        component_filter = f"AND ps.{sql_identifier(str(source['component_scope_field']))} IS NULL"
    if storage == "coherent_array":
        values_field = sql_identifier(str(source.get("values_field") or "values_json"))
        specs = coherent_field_specs(con, source=source, source_alias=source_alias)
        present = " OR ".join(
            f"json_extract(ps.{values_field}, '$[{specs[name]['position']}]') IS NOT NULL"
            for group in source["quantity_groups"]
            for name in group["quantities"]
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE eligible_{source_alias} AS
            SELECT DISTINCT ps.source_record_id
            FROM {source_alias}.{set_table} ps
            WHERE ({present}) {component_filter}
            """
        )
    elif storage == "measurement_bundle":
        bundle_table = sql_identifier(str(source["bundle_table"]))
        measurements_field = sql_identifier(str(source.get("measurements_field") or "measurements"))
        source_quantities = sorted(
            str(quantity)
            for group in source["quantity_groups"]
            for quantity in group["quantities"]
        )
        quantity_list = ",".join(sql_literal(value) for value in source_quantities)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE eligible_{source_alias} AS
            SELECT DISTINCT bundle.source_record_id
            FROM {source_alias}.{bundle_table} bundle
            CROSS JOIN UNNEST(bundle.{measurements_field}) AS nested(measurement)
            WHERE nested.measurement.quantity_key IN ({quantity_list})
              AND (nested.measurement.normalized_value IS NOT NULL
                   OR NULLIF(TRIM(nested.measurement.value_raw), '') IS NOT NULL)
            """
        )
    else:
        evidence_table = sql_identifier(str(source["parameter_evidence_table"]))
        quantity_rows = quantity_values(source)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE eligible_{source_alias} AS
            WITH quantities(source_quantity, selected_quantity, group_key) AS (VALUES {quantity_rows})
            SELECT DISTINCT pe.source_record_id
            FROM {source_alias}.{evidence_table} pe
            JOIN quantities q ON q.source_quantity = pe.quantity_key
            JOIN {source_alias}.{set_table} ps ON ps.parameter_set_id = pe.parameter_set_id
            WHERE (pe.normalized_value IS NOT NULL OR NULLIF(TRIM(pe.value_raw), '') IS NOT NULL)
              {component_filter}
            """
        )
    eligible_count = int(con.execute(f"SELECT COUNT(*) FROM eligible_{source_alias}").fetchone()[0])
    strategy = binding["strategy"]
    claim_namespace = sql_literal(binding["claim_namespace"])
    if strategy in {"canonical_identifier", "authoritative_release_equivalence"}:
        normalization = binding.get("normalization")
        if normalization != "unsigned_decimal":
            raise ValueError(f"unsupported canonical identifier normalization: {normalization}")
        normalized = "regexp_extract(ic.identifier_normalized, '([0-9]+)$', 1)"
        canonical_namespace = sql_literal(binding["canonical_namespace"])
        if strategy == "authoritative_release_equivalence":
            equivalence = binding["release_equivalence"]
            binding_method = (
                "authoritative_release_equivalence:"
                f"{equivalence['source_release']}->{equivalence['canonical_release']}"
            )
        else:
            binding_method = "canonical_identifier_graph"
        candidate_sql = f"""
          SELECT e.source_record_id, b.object_node_key, b.stable_object_key,
                 b.system_stable_object_key, {sql_literal(binding_method)} AS binding_method
          FROM eligible_{source_alias} e
          JOIN {source_alias}.identifier_claim_evidence ic
            ON ic.source_record_id = e.source_record_id AND ic.namespace = {claim_namespace}
          JOIN identity.canonical_identifier_bindings b
            ON b.namespace = {canonical_namespace} AND b.id_value_norm = {normalized}
        """
    else:
        if binding.get("normalization") != "spacegate_public_name_v1":
            raise ValueError("unsupported canonical name normalization")
        canonical_table = sql_identifier(str(binding["canonical_table"]))
        canonical_name_field = sql_identifier(str(binding["canonical_name_field"]))
        candidate_sql = f"""
          WITH canonical_names AS (
            SELECT {canonical_name_field} AS name_norm, stable_object_key,
                   system_id, COUNT(*) OVER (PARTITION BY {canonical_name_field}) AS name_count
            FROM core.{canonical_table}
          )
          SELECT e.source_record_id, o.object_node_key, n.stable_object_key,
                 o.system_stable_object_key, 'canonical_unique_name' AS binding_method
          FROM eligible_{source_alias} e
          JOIN {source_alias}.identifier_claim_evidence ic
            ON ic.source_record_id = e.source_record_id AND ic.namespace = {claim_namespace}
          JOIN canonical_names n
            ON n.name_count = 1 AND n.name_norm = TRIM(regexp_replace(lower(ic.identifier_normalized), '[^a-z0-9]+', ' ', 'g'))
          JOIN identity.canonical_object_nodes o ON o.stable_object_key = n.stable_object_key
        """
    con.execute(f"CREATE OR REPLACE TEMP TABLE binding_candidates_{source_alias} AS {candidate_sql}")
    if strategy == "authoritative_release_equivalence":
        configured_method = (
            "authoritative_release_equivalence:"
            f"{binding['release_equivalence']['source_release']}"
            f"->{binding['release_equivalence']['canonical_release']}"
        )
        accepted_reason = (
            "authoritative identical release source list; unique current canonical target"
        )
    elif strategy == "canonical_identifier":
        configured_method = "canonical_identifier_graph"
        accepted_reason = "unique current canonical target"
    else:
        configured_method = "canonical_unique_name"
        accepted_reason = "unique normalized current canonical name"
    con.execute(
        f"""
        INSERT INTO evidence_object_bindings
        WITH resolved AS (
          SELECT source_record_id,
                 MIN(object_node_key) AS object_node_key,
                 MIN(stable_object_key) AS stable_object_key,
                 MIN(system_stable_object_key) AS system_stable_object_key,
                 MIN(binding_method) AS binding_method,
                 COUNT(DISTINCT stable_object_key) AS target_count
          FROM binding_candidates_{source_alias}
          GROUP BY source_record_id
        )
        SELECT sha256(concat_ws('|', {sql_literal(source_id)}, e.source_record_id,
                                    {sql_literal(object_type)})),
               {sql_literal(source_id)}, {sql_literal(release_id)}, {sql_literal(member['build_id'])},
               e.source_record_id, {sql_literal(source['binding_scope'])},
               {sql_literal(object_type)},
               CASE WHEN r.target_count = 1 THEN r.object_node_key END,
               CASE WHEN r.target_count = 1 THEN r.stable_object_key END,
               CASE WHEN r.target_count = 1 THEN r.system_stable_object_key END,
               CASE
                 WHEN r.target_count = 1 THEN 'accepted'
                 WHEN r.target_count > 1 THEN 'ambiguous'
                 ELSE 'missing'
               END,
               COALESCE(r.binding_method, {sql_literal(configured_method)}),
               CASE
                 WHEN r.target_count = 1 THEN {sql_literal(accepted_reason)}
                 WHEN r.target_count > 1 THEN 'multiple current canonical targets'
                 ELSE 'no current canonical target'
               END
        FROM eligible_{source_alias} e
        LEFT JOIN resolved r USING (source_record_id)
        """
    )
    accepted = int(
        con.execute(
            "SELECT COUNT(*) FROM evidence_object_bindings "
            "WHERE source_id = ? AND binding_status = 'accepted'",
            [source_id],
        ).fetchone()[0]
    )
    return eligible_count, accepted


def insert_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    if str(source.get("storage") or "eav") == "measurement_bundle":
        insert_measurement_bundle_direct(
            con,
            source=source,
            source_alias=source_alias,
            member=member,
            release_id=release_id,
        )
        return
    if str(source.get("selection_mode") or "ranked_candidates") == "authoritative_direct":
        insert_coherent_direct(
            con,
            source=source,
            source_alias=source_alias,
            member=member,
            release_id=release_id,
        )
        return
    if str(source.get("storage") or "eav") == "coherent_array":
        insert_coherent_candidates(
            con,
            source=source,
            source_alias=source_alias,
            member=member,
            release_id=release_id,
        )
        return
    set_table = sql_identifier(str(source["parameter_set_table"]))
    evidence_table = sql_identifier(str(source["parameter_evidence_table"]))
    quantity_rows = quantity_values(source)
    rank_sql, reason_sql, quality_sql = authority_sql(source)
    component_filter = ""
    if source.get("component_scope_field"):
        component_filter = f"AND ps.{sql_identifier(str(source['component_scope_field']))} IS NULL"
    con.execute(
        f"""
        INSERT INTO fact_candidates
        WITH quantities(source_quantity, selected_quantity, group_key) AS (VALUES {quantity_rows}),
        candidates AS (
          SELECT b.object_type, b.stable_object_key, b.system_stable_object_key,
                 q.group_key, q.selected_quantity AS quantity_key,
                 pe.value_raw, pe.normalized_value, pe.normalized_unit,
                 pe.uncertainty_lower, pe.uncertainty_upper, pe.bound_semantics,
                 pe.evidence_id, pe.parameter_set_id, pe.source_record_id,
                 ps.method, ps.model, pe.reference_raw,
                 pe.normalization_version, pe.quality_json,
                 {rank_sql} AS authority_rank,
                 {reason_sql} AS authority_reason,
                 {quality_sql} AS selection_quality_score
          FROM {source_alias}.{evidence_table} pe
          JOIN quantities q ON q.source_quantity = pe.quantity_key
          JOIN {source_alias}.{set_table} ps ON ps.parameter_set_id = pe.parameter_set_id
          JOIN {source_alias}.source_records sr ON sr.source_record_id = pe.source_record_id
          JOIN evidence_object_bindings b
            ON b.source_id = {sql_literal(source['source_id'])}
           AND b.source_record_id = pe.source_record_id
           AND b.binding_status = 'accepted'
          WHERE (pe.normalized_value IS NOT NULL OR NULLIF(TRIM(pe.value_raw), '') IS NOT NULL)
            {component_filter}
        )
        SELECT object_type, stable_object_key, system_stable_object_key,
               group_key, quantity_key, value_raw, normalized_value, normalized_unit,
               uncertainty_lower, uncertainty_upper, bound_semantics,
               {sql_literal(member['build_id'])}, {sql_literal(evidence_table.strip(chr(34)))},
               evidence_id, parameter_set_id, source_record_id,
               {sql_literal(source['source_id'])}, {sql_literal(release_id)},
               method, model, reference_raw, authority_rank, authority_reason,
               selection_quality_score, normalization_version, quality_json
        FROM candidates WHERE authority_rank IS NOT NULL
        """
    )


def insert_measurement_bundle_direct(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    if str(source.get("selection_mode") or "") != "authoritative_direct":
        raise ValueError(f"measurement bundles require authoritative-direct selection: {source['source_id']}")
    table_name = str(source["bundle_table"])
    bundle_table = sql_identifier(table_name)
    bundle_id_field = sql_identifier(str(source.get("bundle_id_field") or "bundle_id"))
    measurements_field = sql_identifier(str(source.get("measurements_field") or "measurements"))
    source_id = sql_literal(source["source_id"])
    release = sql_literal(release_id)
    evidence_build = sql_literal(member["build_id"])
    policy_version = sql_literal(source["_policy_version"])

    duplicate_sets = int(
        con.execute(
            f"""
            SELECT COALESCE(SUM(n-1), 0) FROM (
              SELECT b.stable_object_key, COUNT(*) n
              FROM {source_alias}.{bundle_table} bundle
              JOIN evidence_object_bindings b
                ON b.source_id={source_id}
               AND b.source_record_id=bundle.source_record_id
               AND b.binding_status='accepted'
              GROUP BY b.stable_object_key HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    if duplicate_sets:
        raise ValueError(
            f"measurement-bundle source has duplicate object bundles: {source['source_id']}:{duplicate_sets}"
        )

    quantity_rows: list[str] = []
    decision_selects: list[str] = []
    for group in source["quantity_groups"]:
        rule = group["authorities"][0]
        unsupported_rule_fields = set(rule) - {"rank", "reason"}
        if unsupported_rule_fields:
            raise ValueError(
                f"measurement-bundle direct authority must be unconditional: "
                f"{source['source_id']}:{group['group_key']}:{sorted(unsupported_rule_fields)}"
            )
        group_key = sql_literal(group["group_key"])
        source_quantities = [str(value) for value in group["quantities"]]
        quantity_list = ",".join(sql_literal(value) for value in source_quantities)
        decision_selects.append(
            "SELECT "
            f"sha256(concat_ws('|', b.object_type, b.stable_object_key, {group_key}, "
            f"bundle.{bundle_id_field}, {policy_version})), "
            "b.object_type, b.stable_object_key, b.system_stable_object_key, "
            f"{group_key}, bundle.{bundle_id_field}, bundle.source_record_id, "
            f"{source_id}, {release}, {evidence_build}, {int(rule['rank'])}, "
            f"{sql_literal(rule['reason'])}, "
            "COUNT(DISTINCT nested.measurement.quantity_key)::INTEGER, "
            "COUNT(DISTINCT CASE WHEN nested.measurement.uncertainty_lower IS NOT NULL "
            "OR nested.measurement.uncertainty_upper IS NOT NULL "
            "THEN nested.measurement.quantity_key END)::INTEGER, "
            f"NULL, 1, NULL, NULL, NULL, {policy_version} "
            f"FROM {source_alias}.{bundle_table} bundle "
            "JOIN evidence_object_bindings b "
            f"ON b.source_id={source_id} AND b.source_record_id=bundle.source_record_id "
            "AND b.binding_status='accepted' "
            f"CROSS JOIN UNNEST(bundle.{measurements_field}) AS nested(measurement) "
            f"WHERE nested.measurement.quantity_key IN ({quantity_list}) "
            "AND (nested.measurement.normalized_value IS NOT NULL "
            "OR NULLIF(TRIM(nested.measurement.value_raw), '') IS NOT NULL) "
            "GROUP BY b.object_type, b.stable_object_key, b.system_stable_object_key, "
            f"bundle.{bundle_id_field}, bundle.source_record_id"
        )
        for source_quantity, selected_spec in group["quantities"].items():
            if not isinstance(selected_spec, dict):
                selected_spec = {"quantity_key": str(selected_spec)}
            quantity_rows.append(
                "(" + ",".join(
                    [
                        sql_literal(source_quantity),
                        sql_literal(selected_spec["quantity_key"]),
                        sql_literal(group["group_key"]),
                    ]
                ) + ")"
            )
    con.execute(
        "INSERT INTO parameter_set_selection_decisions " + " UNION ALL ".join(decision_selects)
    )
    con.execute(
        f"""
        INSERT INTO selected_facts
        WITH quantities(source_quantity, selected_quantity, quantity_group) AS (
          VALUES {','.join(quantity_rows)}
        )
        SELECT sha256(concat_ws('|', b.object_type, b.stable_object_key,
                                q.selected_quantity, nested.measurement.evidence_id,
                                {policy_version})),
               b.object_type, b.stable_object_key, b.system_stable_object_key,
               q.quantity_group, q.selected_quantity,
               nested.measurement.value_raw,
               nested.measurement.normalized_value,
               nested.measurement.normalized_unit,
               CASE
                 WHEN lower(coalesce(nested.measurement.bound_semantics, '')) LIKE '%endpoint%'
                   THEN nested.measurement.uncertainty_lower
                 WHEN nested.measurement.normalized_value IS NOT NULL
                  AND nested.measurement.uncertainty_lower IS NOT NULL
                   THEN nested.measurement.normalized_value
                        - abs(nested.measurement.uncertainty_lower)
               END,
               CASE
                 WHEN lower(coalesce(nested.measurement.bound_semantics, '')) LIKE '%endpoint%'
                   THEN nested.measurement.uncertainty_upper
                 WHEN nested.measurement.normalized_value IS NOT NULL
                  AND nested.measurement.uncertainty_upper IS NOT NULL
                   THEN nested.measurement.normalized_value
                        + abs(nested.measurement.uncertainty_upper)
               END,
               nested.measurement.bound_semantics,
               'source_selected', {evidence_build}, {sql_literal(table_name)},
               nested.measurement.evidence_id, bundle.{bundle_id_field},
               bundle.source_record_id, {source_id}, {release},
               nested.measurement.method, nested.measurement.model,
               nested.measurement.reference_raw,
               d.decision_id, d.authority_rank, d.authority_reason,
               {policy_version}, nested.measurement.normalization_version,
               nested.measurement.quality_json
        FROM {source_alias}.{bundle_table} bundle
        JOIN evidence_object_bindings b
          ON b.source_id={source_id}
         AND b.source_record_id=bundle.source_record_id
         AND b.binding_status='accepted'
        CROSS JOIN UNNEST(bundle.{measurements_field}) AS nested(measurement)
        JOIN quantities q ON q.source_quantity=nested.measurement.quantity_key
        JOIN parameter_set_selection_decisions d
          ON d.object_type=b.object_type
         AND d.stable_object_key=b.stable_object_key
         AND d.quantity_group=q.quantity_group
         AND d.selected_parameter_set_id=bundle.{bundle_id_field}
        WHERE nested.measurement.normalized_value IS NOT NULL
           OR NULLIF(TRIM(nested.measurement.value_raw), '') IS NOT NULL
        """
    )


def insert_coherent_direct(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    set_table_name = str(source["parameter_set_table"])
    set_table = sql_identifier(set_table_name)
    set_id_field = sql_identifier(str(source.get("set_id_field") or "evidence_id"))
    values_field = sql_identifier(str(source.get("values_field") or "values_json"))
    specs = coherent_field_specs(con, source=source, source_alias=source_alias)
    policy_version = sql_literal(source["_policy_version"])
    source_id = sql_literal(source["source_id"])
    release = sql_literal(release_id)
    evidence_build = sql_literal(member["build_id"])

    duplicate_sets = int(
        con.execute(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT b.stable_object_key)
            FROM {source_alias}.{set_table} ps
            JOIN evidence_object_bindings b
              ON b.source_id={source_id}
             AND b.source_record_id=ps.source_record_id
             AND b.binding_status='accepted'
            """
        ).fetchone()[0]
    )
    if duplicate_sets:
        raise ValueError(
            f"authoritative-direct source has duplicate object parameter sets: "
            f"{source['source_id']}:{duplicate_sets}"
        )

    decision_selects: list[str] = []
    value_rows: list[str] = []
    for group in source["quantity_groups"]:
        rule = group["authorities"][0]
        condition = authority_condition(rule, "sr", "ps")
        selection_quality_score = quality_score_sql(
            rule, source_alias="sr", set_alias="ps"
        )
        present_terms: list[str] = []
        uncertainty_terms: list[str] = []
        for source_field, selected_spec in group["quantities"].items():
            if not isinstance(selected_spec, dict):
                selected_spec = {"quantity_key": str(selected_spec)}
            spec = specs[source_field]
            raw = f"json_extract_string(ps.{values_field}, '$[{spec['position']}]')"
            numeric = bool(selected_spec.get("numeric", True))
            normalized = f"try_cast({raw} AS DOUBLE)" if numeric else "NULL::DOUBLE"
            present_terms.append(f"CASE WHEN {raw} IS NOT NULL THEN 1 ELSE 0 END")
            uncertainty_field = selected_spec.get("uncertainty_field")
            if uncertainty_field:
                uncertainty_spec = specs[str(uncertainty_field)]
                uncertainty = (
                    f"try_cast(json_extract_string(ps.{values_field}, "
                    f"'$[{uncertainty_spec['position']}]') AS DOUBLE)"
                )
                uncertainty_lower = uncertainty
                uncertainty_upper = uncertainty
                bound_semantics = sql_literal("symmetric_error")
                uncertainty_terms.append(
                    f"CASE WHEN {raw} IS NOT NULL AND {uncertainty} IS NOT NULL THEN 1 ELSE 0 END"
                )
            else:
                uncertainty_lower = "NULL::DOUBLE"
                uncertainty_upper = "NULL::DOUBLE"
                bound_semantics = "NULL::VARCHAR"
            value_rows.append(
                "(" + ",".join(
                    [
                        sql_literal(group["group_key"]),
                        sql_literal(selected_spec["quantity_key"]),
                        raw,
                        normalized,
                        sql_literal(selected_spec.get("unit", spec["unit"]) or None),
                        uncertainty_lower,
                        uncertainty_upper,
                        bound_semantics,
                    ]
                ) + ")"
            )
        quantity_count = " + ".join(present_terms)
        uncertainty_count = " + ".join(uncertainty_terms) if uncertainty_terms else "0"
        group_key = sql_literal(group["group_key"])
        decision_selects.append(
            "SELECT "
            f"sha256(concat_ws('|', b.object_type, b.stable_object_key, {group_key}, "
            f"ps.{set_id_field}, {policy_version})), "
            "b.object_type, b.stable_object_key, b.system_stable_object_key, "
            f"{group_key}, ps.{set_id_field}, ps.source_record_id, {source_id}, "
            f"{release}, {evidence_build}, {int(rule['rank'])}, {sql_literal(rule['reason'])}, "
            f"{selection_quality_score}, "
            f"({quantity_count})::INTEGER, ({uncertainty_count})::INTEGER, 1, "
            f"NULL, NULL, NULL, {policy_version} "
            f"FROM {source_alias}.{set_table} ps "
            f"JOIN {source_alias}.source_records sr ON sr.source_record_id=ps.source_record_id "
            "JOIN evidence_object_bindings b "
            f"ON b.source_id={source_id} AND b.source_record_id=ps.source_record_id "
            f"AND b.binding_status='accepted' WHERE {condition}"
        )
    con.execute(
        "INSERT INTO parameter_set_selection_decisions " + " UNION ALL ".join(decision_selects)
    )

    con.execute(
        f"""
        INSERT INTO selected_facts
        SELECT sha256(concat_ws('|', b.object_type, b.stable_object_key, v.quantity_key,
                                ps.{set_id_field}, {policy_version})),
               b.object_type, b.stable_object_key, b.system_stable_object_key,
               v.quantity_group, v.quantity_key, v.value_raw, v.normalized_value,
               v.normalized_unit,
               CASE WHEN v.normalized_value IS NOT NULL AND v.uncertainty_lower IS NOT NULL
                    THEN v.normalized_value - abs(v.uncertainty_lower) END,
               CASE WHEN v.normalized_value IS NOT NULL AND v.uncertainty_upper IS NOT NULL
                    THEN v.normalized_value + abs(v.uncertainty_upper) END,
               v.bound_semantics, 'source_selected', {evidence_build},
               {sql_literal(set_table_name)}, ps.{set_id_field}, ps.{set_id_field},
               ps.source_record_id, {source_id}, {release}, ps.method, ps.model,
               ps.reference_raw,
               sha256(concat_ws('|', b.object_type, b.stable_object_key,
                                v.quantity_group, ps.{set_id_field}, {policy_version})),
               d.authority_rank, d.authority_reason, {policy_version},
               ps.normalization_version, ps.quality_json
        FROM {source_alias}.{set_table} ps
        JOIN evidence_object_bindings b
          ON b.source_id={source_id}
         AND b.source_record_id=ps.source_record_id
         AND b.binding_status='accepted'
        CROSS JOIN LATERAL (
          VALUES {','.join(value_rows)}
        ) v(quantity_group, quantity_key, value_raw, normalized_value,
            normalized_unit, uncertainty_lower, uncertainty_upper, bound_semantics)
        JOIN parameter_set_selection_decisions d
          ON d.object_type=b.object_type
         AND d.stable_object_key=b.stable_object_key
         AND d.quantity_group=v.quantity_group
         AND d.selected_parameter_set_id=ps.{set_id_field}
        WHERE v.value_raw IS NOT NULL
        """
    )


def insert_coherent_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    source: dict[str, Any],
    source_alias: str,
    member: dict[str, Any],
    release_id: str,
) -> None:
    set_table_name = str(source["parameter_set_table"])
    set_table = sql_identifier(set_table_name)
    set_id_field = sql_identifier(str(source.get("set_id_field") or "evidence_id"))
    values_field = sql_identifier(str(source.get("values_field") or "values_json"))
    specs = coherent_field_specs(con, source=source, source_alias=source_alias)
    rows: list[str] = []
    for group in source["quantity_groups"]:
        group_key = str(group["group_key"])
        for source_field, selected_spec in group["quantities"].items():
            if not isinstance(selected_spec, dict):
                selected_spec = {"quantity_key": str(selected_spec)}
            spec = specs[source_field]
            raw = f"json_extract_string(ps.{values_field}, '$[{spec['position']}]')"
            numeric = bool(selected_spec.get("numeric", True))
            normalized = f"try_cast({raw} AS DOUBLE)" if numeric else "NULL::DOUBLE"
            uncertainty_field = selected_spec.get("uncertainty_field")
            if uncertainty_field:
                uncertainty_spec = specs[str(uncertainty_field)]
                uncertainty = (
                    f"try_cast(json_extract_string(ps.{values_field}, "
                    f"'$[{uncertainty_spec['position']}]') AS DOUBLE)"
                )
                uncertainty_lower = uncertainty
                uncertainty_upper = uncertainty
                bound_semantics = sql_literal("symmetric_error")
            else:
                uncertainty_lower = "NULL::DOUBLE"
                uncertainty_upper = "NULL::DOUBLE"
                bound_semantics = "NULL::VARCHAR"
            rank_clauses = [
                f"WHEN {authority_condition(rule, 'sr', 'ps')} THEN {int(rule['rank'])}"
                for rule in group["authorities"]
            ]
            reason_clauses = [
                f"WHEN {authority_condition(rule, 'sr', 'ps')} THEN {sql_literal(rule['reason'])}"
                for rule in group["authorities"]
            ]
            quality_clauses = [
                f"WHEN {authority_condition(rule, 'sr', 'ps')} "
                f"THEN {quality_score_sql(rule, source_alias='sr', set_alias='ps')}"
                for rule in group["authorities"]
            ]
            rows.append(
                "SELECT "
                "b.object_type, b.stable_object_key, b.system_stable_object_key, "
                f"{sql_literal(group_key)} AS quantity_group, "
                f"{sql_literal(selected_spec['quantity_key'])} AS quantity_key, "
                f"{raw} AS value_raw, {normalized} AS normalized_value, "
                f"{sql_literal(selected_spec.get('unit', spec['unit']) or None)} AS normalized_unit, "
                f"{uncertainty_lower} AS uncertainty_lower, "
                f"{uncertainty_upper} AS uncertainty_upper, "
                f"{bound_semantics} AS bound_semantics, "
                f"{sql_literal(member['build_id'])} AS evidence_build_id, "
                f"{sql_literal(set_table_name)} AS evidence_table, "
                f"ps.{set_id_field} AS evidence_id, ps.{set_id_field} AS parameter_set_id, "
                "ps.source_record_id, "
                f"{sql_literal(source['source_id'])} AS source_id, "
                f"{sql_literal(release_id)} AS release_id, "
                "ps.method, ps.model, ps.reference_raw, "
                f"CASE {' '.join(rank_clauses)} ELSE NULL END AS authority_rank, "
                f"CASE {' '.join(reason_clauses)} ELSE NULL END AS authority_reason, "
                f"CASE {' '.join(quality_clauses)} ELSE NULL END AS selection_quality_score, "
                "ps.normalization_version, ps.quality_json "
                f"FROM {source_alias}.{set_table} ps "
                f"JOIN {source_alias}.source_records sr ON sr.source_record_id=ps.source_record_id "
                "JOIN evidence_object_bindings b "
                f"ON b.source_id={sql_literal(source['source_id'])} "
                "AND b.source_record_id=ps.source_record_id AND b.binding_status='accepted' "
                f"WHERE {raw} IS NOT NULL"
            )
    con.execute("INSERT INTO fact_candidates " + " UNION ALL ".join(rows))


def create_candidate_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE fact_candidates (
          object_type VARCHAR, stable_object_key VARCHAR, system_stable_object_key VARCHAR,
          quantity_group VARCHAR, quantity_key VARCHAR, value_raw VARCHAR,
          normalized_value DOUBLE, normalized_unit VARCHAR,
          uncertainty_lower DOUBLE, uncertainty_upper DOUBLE, bound_semantics VARCHAR,
          evidence_build_id VARCHAR, evidence_table VARCHAR, evidence_id VARCHAR,
          parameter_set_id VARCHAR, source_record_id VARCHAR, source_id VARCHAR,
          release_id VARCHAR, method VARCHAR, model VARCHAR, reference_raw VARCHAR,
          authority_rank INTEGER, authority_reason VARCHAR,
          selection_quality_score DOUBLE,
          normalization_version VARCHAR, quality_json JSON
        );
        """
    )


def select_parameter_sets(con: duckdb.DuckDBPyConnection, policy_version: str) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE candidate_sets AS
        SELECT object_type, stable_object_key, system_stable_object_key, quantity_group,
               parameter_set_id, source_record_id, source_id, release_id, evidence_build_id,
               MIN(authority_rank)::INTEGER AS authority_rank,
               MIN(authority_reason) AS authority_reason,
               MAX(selection_quality_score) AS selection_quality_score,
               COUNT(DISTINCT quantity_key)::INTEGER AS quantity_count,
               COUNT(DISTINCT CASE WHEN uncertainty_lower IS NOT NULL OR uncertainty_upper IS NOT NULL THEN quantity_key END)::INTEGER AS uncertainty_count,
               COUNT(DISTINCT CASE WHEN NULLIF(TRIM(reference_raw), '') IS NOT NULL THEN quantity_key END)::INTEGER AS reference_count
        FROM fact_candidates
        GROUP BY ALL;

        CREATE TEMP TABLE ranked_sets AS
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY object_type, stable_object_key, quantity_group
                 ORDER BY authority_rank, quantity_count DESC, uncertainty_count DESC,
                          reference_count DESC, selection_quality_score DESC NULLS LAST,
                          parameter_set_id
               ) AS selection_rank,
               COUNT(*) OVER (PARTITION BY object_type, stable_object_key, quantity_group)::INTEGER AS candidate_count,
               LEAD(parameter_set_id) OVER (
                 PARTITION BY object_type, stable_object_key, quantity_group
                 ORDER BY authority_rank, quantity_count DESC, uncertainty_count DESC,
                          reference_count DESC, selection_quality_score DESC NULLS LAST,
                          parameter_set_id
               ) AS runner_up_parameter_set_id,
               LEAD(authority_rank) OVER (
                 PARTITION BY object_type, stable_object_key, quantity_group
                 ORDER BY authority_rank, quantity_count DESC, uncertainty_count DESC,
                          reference_count DESC, selection_quality_score DESC NULLS LAST,
                          parameter_set_id
               )::INTEGER AS runner_up_authority_rank
               ,LEAD(selection_quality_score) OVER (
                 PARTITION BY object_type, stable_object_key, quantity_group
                 ORDER BY authority_rank, quantity_count DESC, uncertainty_count DESC,
                          reference_count DESC, selection_quality_score DESC NULLS LAST,
                          parameter_set_id
               ) AS runner_up_quality_score
        FROM candidate_sets;

        INSERT INTO parameter_set_selection_decisions
        SELECT sha256(concat_ws('|', object_type, stable_object_key, quantity_group,
                                parameter_set_id, {sql_literal(policy_version)})),
               object_type, stable_object_key, system_stable_object_key, quantity_group,
               parameter_set_id, source_record_id, source_id, release_id,
               evidence_build_id, authority_rank, authority_reason,
               selection_quality_score,
               quantity_count, uncertainty_count, candidate_count,
               runner_up_parameter_set_id, runner_up_authority_rank,
               runner_up_quality_score,
               {sql_literal(policy_version)}
        FROM ranked_sets WHERE selection_rank = 1;

        INSERT INTO selected_facts
        SELECT sha256(concat_ws('|', c.object_type, c.stable_object_key, c.quantity_key,
                                c.evidence_id, {sql_literal(policy_version)})),
               c.object_type, c.stable_object_key, c.system_stable_object_key,
               c.quantity_group, c.quantity_key, c.value_raw, c.normalized_value,
               c.normalized_unit,
               CASE
                 WHEN lower(coalesce(c.bound_semantics, '')) LIKE '%endpoint%' THEN c.uncertainty_lower
                 WHEN c.normalized_value IS NOT NULL AND c.uncertainty_lower IS NOT NULL
                   THEN c.normalized_value - abs(c.uncertainty_lower)
                 ELSE NULL
               END AS value_lower,
               CASE
                 WHEN lower(coalesce(c.bound_semantics, '')) LIKE '%endpoint%' THEN c.uncertainty_upper
                 WHEN c.normalized_value IS NOT NULL AND c.uncertainty_upper IS NOT NULL
                   THEN c.normalized_value + abs(c.uncertainty_upper)
                 ELSE NULL
               END AS value_upper,
               c.bound_semantics, 'source_selected', c.evidence_build_id,
               c.evidence_table, c.evidence_id, c.parameter_set_id,
               c.source_record_id, c.source_id, c.release_id, c.method, c.model,
               c.reference_raw, d.decision_id, c.authority_rank,
               c.authority_reason, {sql_literal(policy_version)},
               c.normalization_version, c.quality_json
        FROM fact_candidates c
        JOIN parameter_set_selection_decisions d
          ON d.object_type = c.object_type
         AND d.stable_object_key = c.stable_object_key
         AND d.quantity_group = c.quantity_group
         AND d.selected_parameter_set_id = c.parameter_set_id;
        """
    )


def derive_stellar_luminosity(
    con: duckdb.DuckDBPyConnection, policy: dict[str, Any]
) -> None:
    derivation = next(
        item for item in policy["derivations"]
        if item["derivation_key"] == "stellar_luminosity_stefan_boltzmann"
    )
    policy_version = str(policy["policy_version"])
    key = str(derivation["derivation_key"])
    version = str(derivation["version"])
    con.execute(
        f"""
        CREATE TEMP TABLE luminosity_derivations AS
        WITH radius AS (
          SELECT * FROM selected_facts
          WHERE object_type = 'star' AND quantity_key = 'radius_rsun'
        ), temperature AS (
          SELECT * FROM selected_facts
          WHERE object_type = 'star' AND quantity_key = 'teff_k'
        ), candidates AS (
          SELECT r.stable_object_key, r.system_stable_object_key,
                 r.selected_fact_id AS radius_fact_id,
                 t.selected_fact_id AS temperature_fact_id,
                 r.normalized_value * r.normalized_value
                   * pow(t.normalized_value / 5772.0, 4.0) AS value,
                 CASE WHEN r.value_lower > 0 AND t.value_lower > 0
                   THEN r.value_lower * r.value_lower * pow(t.value_lower / 5772.0, 4.0)
                   ELSE NULL END AS value_lower,
                 CASE WHEN r.value_upper > 0 AND t.value_upper > 0
                   THEN r.value_upper * r.value_upper * pow(t.value_upper / 5772.0, 4.0)
                   ELSE NULL END AS value_upper
          FROM radius r JOIN temperature t USING (object_type, stable_object_key)
          LEFT JOIN selected_facts direct
            ON direct.object_type = 'star'
           AND direct.stable_object_key = r.stable_object_key
           AND direct.quantity_key = 'luminosity_lsun'
          WHERE direct.selected_fact_id IS NULL
            AND r.normalized_value > 0 AND t.normalized_value > 0
        )
        SELECT sha256(concat_ws('|', stable_object_key, {sql_literal(key)}, {sql_literal(version)})) AS derivation_id,
               sha256(concat_ws('|', 'star', stable_object_key, 'luminosity_lsun',
                                {sql_literal(key)}, {sql_literal(version)}, {sql_literal(policy_version)})) AS selected_fact_id,
               *
        FROM candidates;

        INSERT INTO selected_facts
        SELECT selected_fact_id, 'star', stable_object_key, system_stable_object_key,
               'stellar_fundamental', 'luminosity_lsun', cast(value AS VARCHAR),
               value, 'solLum', value_lower, value_upper,
               'propagated_selected_interval_endpoints', 'derived',
               NULL, NULL, NULL, NULL, NULL, 'spacegate.derivation',
               {sql_literal(version)}, {sql_literal(key)}, NULL, NULL, NULL, NULL,
               NULL, {sql_literal(policy_version)}, {sql_literal(version)},
               json_object('solar_effective_temperature_k', 5772.0)
        FROM luminosity_derivations;

        INSERT INTO selected_fact_derivations
        SELECT derivation_id, selected_fact_id, stable_object_key, 'luminosity_lsun',
               {sql_literal(key)}, {sql_literal(version)},
               to_json([radius_fact_id, temperature_fact_id]),
               {sql_literal(derivation['applicability'])}, {sql_literal(derivation['formula'])},
               json_object('solar_effective_temperature_k', 5772.0),
               {sql_literal(derivation['uncertainty'])}, 'medium',
               {json_sql_literal(derivation['supersedes'])},
               {sql_literal(policy_version)}
        FROM luminosity_derivations;
        """
    )


def verify_keys(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    checks = {
        "duplicate_binding_ids": "SELECT COUNT(*) - COUNT(DISTINCT binding_id) FROM evidence_object_bindings",
        "invalid_binding_statuses": "SELECT COUNT(*) FROM evidence_object_bindings WHERE binding_status NOT IN ('accepted', 'missing', 'ambiguous')",
        "accepted_bindings_without_targets": "SELECT COUNT(*) FROM evidence_object_bindings WHERE binding_status='accepted' AND (canonical_object_node_key IS NULL OR stable_object_key IS NULL)",
        "unresolved_bindings_with_targets": "SELECT COUNT(*) FROM evidence_object_bindings WHERE binding_status<>'accepted' AND (canonical_object_node_key IS NOT NULL OR stable_object_key IS NOT NULL OR system_stable_object_key IS NOT NULL)",
        "duplicate_decision_ids": "SELECT COUNT(*) - COUNT(DISTINCT decision_id) FROM parameter_set_selection_decisions",
        "duplicate_selected_fact_ids": "SELECT COUNT(*) - COUNT(DISTINCT selected_fact_id) FROM selected_facts",
        "duplicate_object_quantities": "SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT COUNT(*) n FROM selected_facts GROUP BY object_type, stable_object_key, quantity_key HAVING COUNT(*) > 1)",
        "selected_source_facts_without_evidence": "SELECT COUNT(*) FROM selected_facts WHERE fact_status='source_selected' AND (evidence_build_id IS NULL OR evidence_id IS NULL OR parameter_set_id IS NULL)",
        "derived_facts_without_derivation": "SELECT COUNT(*) FROM selected_facts f LEFT JOIN selected_fact_derivations d ON d.output_selected_fact_id=f.selected_fact_id WHERE f.fact_status='derived' AND d.derivation_id IS NULL",
        "lower_authority_winner": "SELECT COUNT(*) FROM parameter_set_selection_decisions WHERE runner_up_authority_rank IS NOT NULL AND runner_up_authority_rank < authority_rank",
    }
    result = {name: int(con.execute(sql).fetchone()[0] or 0) for name, sql in checks.items()}
    failing = {name: count for name, count in result.items() if count}
    if failing:
        raise ValueError(f"selected-fact integrity checks failed: {failing}")
    return result


def table_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        table: int(con.execute(f"SELECT COUNT(*) FROM {sql_identifier(table)}").fetchone()[0])
        for table in [
            "selection_source_accounting", "evidence_object_bindings",
            "parameter_set_selection_decisions", "selected_facts",
            "selected_fact_derivations",
        ]
    }


def safe_partition_name(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9_]+", value):
        raise ValueError(f"unsafe selected-fact partition key: {value!r}")
    return value


def compile_selected_facts(
    *,
    state_dir: Path,
    policy_path: Path,
    dispositions_path: Path | None = None,
    artifact_root: Path | None = None,
    report_path: Path | None = None,
    memory_limit: str = "32GB",
    threads: int = 8,
    temp_directory: Path | None = None,
) -> dict[str, Any]:
    state_dir = state_dir.resolve()
    policy_path = policy_path.resolve()
    dispositions_path = (
        dispositions_path or policy_path.with_name(DEFAULT_DISPOSITIONS.name)
    ).resolve()
    policy = load_json(policy_path)
    release_manifest_path, release_manifest = release_set_paths(state_dir, policy)
    validate_policy(policy, release_manifest)
    dispositions = load_json(dispositions_path)
    disposition_audit = source_disposition_audit.audit(
        release_manifest, policy, dispositions
    )
    if disposition_audit["status"] == "fail":
        raise ValueError(
            "E5 source disposition audit failed: "
            + json.dumps(disposition_audit["checks"], sort_keys=True)
        )
    members = member_by_source(release_manifest)

    identity_dir = state_dir / "derived/evidence_lake_v2/identity" / str(policy["identity_graph_id"])
    identity_db = identity_dir / "identity_graph.duckdb"
    core_dir = state_dir / "out" / str(policy["canonical_reference_build_id"])
    core_db = core_dir / "core.duckdb"
    if not identity_db.is_file() or not core_db.is_file():
        raise ValueError("selection identity graph or canonical reference database is missing")
    identity_sha = file_sha256(identity_db)
    core_sha = file_sha256(core_db)
    compiler_sha = file_sha256(Path(__file__).resolve())
    policy_sha = file_sha256(policy_path)
    dispositions_sha = file_sha256(dispositions_path)
    disposition_audit_compiler_sha = file_sha256(
        Path(source_disposition_audit.__file__).resolve()
    )
    inputs = {
        "policy_sha256": policy_sha,
        "source_disposition_sha256": dispositions_sha,
        "source_disposition_version": dispositions["disposition_version"],
        "source_disposition_audit_compiler_sha256": disposition_audit_compiler_sha,
        "evidence_release_set_id": release_manifest["release_set_id"],
        "evidence_release_set_sha256": release_manifest["release_set_sha256"],
        "identity_graph_id": policy["identity_graph_id"],
        "identity_graph_sha256": identity_sha,
        "canonical_reference_build_id": policy["canonical_reference_build_id"],
        "canonical_reference_sha256": core_sha,
        "compiler_sha256": compiler_sha,
        "duckdb_version": duckdb.__version__,
    }
    build_sha = stable_sha256(inputs)
    build_id = build_sha[:24]
    artifact_root = (
        artifact_root or state_dir / "derived/evidence_lake_v2/selected_facts"
    ).resolve()
    final_dir = artifact_root / build_id
    final_manifest = final_dir / "manifest.json"
    if final_manifest.is_file():
        manifest = load_json(final_manifest)
        if manifest.get("build_sha256") != build_sha:
            raise ValueError(f"immutable selected-fact build collision: {build_id}")
        if report_path:
            atomic_json(report_path, manifest["report"])
        return manifest["report"]

    artifact_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=artifact_root))
    database = staging / "selected_facts.duckdb"
    temp_directory = (temp_directory or Path("/mnt/space/spacegate/e5-selection-spill")).resolve()
    temp_directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(
        str(database),
        config={
            "memory_limit": memory_limit,
            "threads": str(max(1, threads)),
            "temp_directory": str(temp_directory),
            "preserve_insertion_order": "false",
        },
    )
    try:
        create_schema(con)
        create_candidate_table(con)
        con.execute(f"ATTACH {sql_literal(str(identity_db))} AS identity (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(str(core_db))} AS core (READ_ONLY)")
        source_runtime: list[tuple[dict[str, Any], str, dict[str, Any], str, int, int]] = []
        for index, configured_source in enumerate(policy["selection_sources"]):
            source = dict(configured_source)
            source["_policy_version"] = str(policy["policy_version"])
            source_id = str(source["source_id"])
            member = members[source_id]
            alias = f"e4_{index}"
            artifact_path = state_dir / str(member["artifact_path"])
            db_path = artifact_path / str(member["database"])
            if file_sha256(artifact_path / "manifest.json") != member["manifest_sha256"]:
                raise ValueError(f"E4 member manifest changed: {source_id}")
            if db_path.stat().st_size != int(member["database_bytes"]):
                raise ValueError(f"E4 member database size changed: {source_id}")
            if file_sha256(db_path) != member["database_sha256"]:
                raise ValueError(f"E4 member database checksum changed: {source_id}")
            con.execute(f"ATTACH {sql_literal(str(db_path))} AS {sql_identifier(alias)} (READ_ONLY)")
            release_id = str(member["release_ids"][source_id])
            eligible, accepted = create_binding(
                con,
                source=source,
                source_alias=alias,
                member=member,
                release_id=release_id,
            )
            if eligible < int(source.get("minimum_eligible_records") or 1):
                raise ValueError(
                    f"selection source eligible-record floor failed: "
                    f"{source_id}:{eligible}<{source.get('minimum_eligible_records', 1)}"
                )
            if accepted < int(source.get("minimum_accepted_bindings") or 1):
                raise ValueError(
                    f"selection source accepted-binding floor failed: "
                    f"{source_id}:{accepted}<{source.get('minimum_accepted_bindings', 1)}"
                )
            insert_candidates(
                con,
                source=source,
                source_alias=alias,
                member=member,
                release_id=release_id,
            )
            source_runtime.append((source, alias, member, release_id, eligible, accepted))

        select_parameter_sets(con, str(policy["policy_version"]))
        derive_stellar_luminosity(con, policy)

        for source, _alias, member, release_id, eligible, accepted in source_runtime:
            source_id = str(source["source_id"])
            if str(source.get("selection_mode") or "ranked_candidates") == "authoritative_direct":
                candidate_sets = int(
                    con.execute(
                        "SELECT COUNT(*) FROM parameter_set_selection_decisions "
                        "WHERE selected_source_id=?",
                        [source_id],
                    ).fetchone()[0]
                )
            else:
                candidate_sets = int(
                    con.execute("SELECT COUNT(*) FROM candidate_sets WHERE source_id=?", [source_id]).fetchone()[0]
                )
            selected_sets = int(
                con.execute(
                    "SELECT COUNT(*) FROM parameter_set_selection_decisions WHERE selected_source_id=?",
                    [source_id],
                ).fetchone()[0]
            )
            selected = int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts WHERE source_id=? AND fact_status='source_selected'",
                    [source_id],
                ).fetchone()[0]
            )
            if selected < int(source.get("minimum_selected_facts") or 1):
                raise ValueError(
                    f"selection source selected-fact floor failed: "
                    f"{source_id}:{selected}<{source.get('minimum_selected_facts', 1)}"
                )
            con.execute(
                "INSERT INTO selection_source_accounting VALUES (?,?,?,?,?,?,?,?,?,?)",
                [
                    source_id, release_id, member["build_id"], source["object_type"],
                    eligible, accepted, eligible - accepted, candidate_sets,
                    selected_sets, selected,
                ],
            )

        checks = verify_keys(con)
        con.execute(
            "INSERT INTO evidence_build VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                build_id, policy["policy_version"], policy_sha,
                release_manifest["release_set_id"], release_manifest["release_set_sha256"],
                policy["identity_graph_id"], identity_sha,
                policy["canonical_reference_build_id"], core_sha,
                policy["compiler_version"], compiler_sha, utc_now(), "pass",
            ],
        )
        con.execute("CHECKPOINT")
        counts = table_counts(con)
        binding_outcomes: dict[str, dict[str, int]] = {}
        for source_id, binding_status, row_count in con.execute(
            "SELECT source_id, binding_status, COUNT(*) "
            "FROM evidence_object_bindings GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall():
            binding_outcomes.setdefault(str(source_id), {})[str(binding_status)] = int(
                row_count
            )
        fact_exports: dict[str, int] = {}
        decision_exports: dict[str, int] = {}
        for quantity_key, row_count in con.execute(
            "SELECT quantity_key, COUNT(*) FROM selected_facts "
            "GROUP BY quantity_key ORDER BY quantity_key"
        ).fetchall():
            partition = safe_partition_name(str(quantity_key))
            output = staging / f"selected_facts__{partition}.parquet"
            fact_exports[output.name] = int(row_count)
            con.execute(
                "COPY (SELECT * FROM selected_facts "
                f"WHERE quantity_key={sql_literal(quantity_key)} ORDER BY selected_fact_id) "
                f"TO {sql_literal(str(output))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        for quantity_group, row_count in con.execute(
            "SELECT quantity_group, COUNT(*) FROM parameter_set_selection_decisions "
            "GROUP BY quantity_group ORDER BY quantity_group"
        ).fetchall():
            partition = safe_partition_name(str(quantity_group))
            output = staging / f"selection_decisions__{partition}.parquet"
            decision_exports[output.name] = int(row_count)
            con.execute(
                "COPY (SELECT * FROM parameter_set_selection_decisions "
                f"WHERE quantity_group={sql_literal(quantity_group)} ORDER BY decision_id) "
                f"TO {sql_literal(str(output))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
        for table, order_key in [
            ("selected_fact_derivations", "derivation_id"),
            ("selection_source_accounting", "source_id"),
        ]:
            output = staging / f"{table}.parquet"
            con.execute(
                f"COPY (SELECT * FROM {sql_identifier(table)} ORDER BY {sql_identifier(order_key)}) "
                f"TO {sql_literal(str(output))} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            )
    except Exception:
        con.close()
        shutil.rmtree(staging, ignore_errors=True)
        raise
    else:
        con.close()

    expected_exports = {**fact_exports, **decision_exports}
    missing_exports = sorted(name for name in expected_exports if not (staging / name).is_file())
    if missing_exports:
        shutil.rmtree(staging, ignore_errors=True)
        raise ValueError(f"selected-fact partition exports missing: {missing_exports}")
    export_con = duckdb.connect(":memory:")
    try:
        export_counts = {
            name: int(
                export_con.execute(
                    f"SELECT COUNT(*) FROM read_parquet({sql_literal(str(staging / name))})"
                ).fetchone()[0]
            )
            for name in sorted(expected_exports)
        }
    finally:
        export_con.close()
    mismatched_exports = {
        name: {"expected": expected_exports[name], "actual": export_counts[name]}
        for name in expected_exports
        if expected_exports[name] != export_counts[name]
    }
    if mismatched_exports:
        shutil.rmtree(staging, ignore_errors=True)
        raise ValueError(f"selected-fact partition row accounting failed: {mismatched_exports}")

    files = {
        path.name: {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        for path in sorted(staging.iterdir()) if path.is_file()
    }
    logical_sha = stable_sha256({name: value["sha256"] for name, value in files.items() if name.endswith(".parquet")})
    report = {
        "schema_version": "spacegate.selected_fact_compile_report.v1",
        "status": "pass",
        "build_id": build_id,
        "build_sha256": build_sha,
        "policy_version": policy["policy_version"],
        "source_disposition_version": dispositions["disposition_version"],
        "source_disposition_status": disposition_audit["status"],
        "source_disposition_blockers": disposition_audit["checks"]["blocking_sources"],
        "evidence_release_set_id": release_manifest["release_set_id"],
        "identity_graph_id": policy["identity_graph_id"],
        "canonical_reference_build_id": policy["canonical_reference_build_id"],
        "table_counts": counts,
        "binding_outcomes": binding_outcomes,
        "integrity_checks": checks,
        "logical_content_sha256": logical_sha,
        "partition_exports": {
            "selected_facts": fact_exports,
            "selection_decisions": decision_exports,
        },
        "files": files,
    }
    manifest = {
        "schema_version": "spacegate.selected_fact_artifact.v1",
        "build_id": build_id,
        "build_sha256": build_sha,
        "inputs": inputs,
        "report": report,
    }
    atomic_json(staging / "manifest.json", manifest)
    os.replace(staging, final_dir)
    current_temp = artifact_root / f".current.{os.getpid()}.tmp"
    current_temp.unlink(missing_ok=True)
    current_temp.symlink_to(build_id)
    os.replace(current_temp, artifact_root / "current")
    if report_path:
        atomic_json(report_path, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--dispositions", type=Path, default=DEFAULT_DISPOSITIONS)
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--memory-limit", default="32GB")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--temp-directory", type=Path)
    args = parser.parse_args()
    report = compile_selected_facts(
        state_dir=args.state_dir,
        policy_path=args.policy,
        dispositions_path=args.dispositions,
        artifact_root=args.artifact_root,
        report_path=args.report,
        memory_limit=args.memory_limit,
        threads=args.threads,
        temp_directory=args.temp_directory,
    )
    print(
        f"E5 selected facts {report['build_id']} pass: "
        f"facts={report['table_counts']['selected_facts']} "
        f"decisions={report['table_counts']['parameter_set_selection_decisions']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
