#!/usr/bin/env python3
"""Compile source-native lake tables into immutable scientific evidence domains."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT = ROOT / "config" / "evidence_lake" / "e4_scientific_evidence.json"
DEFAULT_REGISTRY = ROOT / "config" / "evidence_lake" / "source_releases.json"
DEFAULT_STATE = Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state"))
BUILD_CONTRACT = "spacegate.scientific_evidence_build.v2"
LOGICAL_HASH_ALGORITHM = "sha256_bucketed_multiset_v1"


DOMAIN_TABLES = {
    "identifier_claim_evidence",
    "stellar_parameter_sets",
    "stellar_parameter_evidence",
    "stellar_classification_evidence",
    "astrometry_distance_evidence",
    "photometry_extinction_evidence",
    "spectra_product_index",
    "variability_activity_rotation_evidence",
    "relation_claim_evidence",
    "orbital_solution_evidence",
    "cluster_evidence",
    "cluster_membership_evidence",
    "planet_parameter_sets",
    "planet_parameter_evidence",
    "planet_lifecycle_evidence",
    "transit_observation_evidence",
    "radial_velocity_evidence",
    "compact_object_evidence",
    "extended_object_evidence",
    "citations",
    "evidence_citations",
    "observation_product_lineage",
}

EVIDENCE_REFERENCE_TABLES = {
    "stellar_parameter_evidence",
    "stellar_classification_evidence",
    "astrometry_distance_evidence",
    "photometry_extinction_evidence",
    "variability_activity_rotation_evidence",
    "relation_claim_evidence",
    "orbital_solution_evidence",
    "cluster_evidence",
    "cluster_membership_evidence",
    "planet_parameter_evidence",
    "planet_lifecycle_evidence",
    "transit_observation_evidence",
    "radial_velocity_evidence",
    "compact_object_evidence",
    "extended_object_evidence",
}


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9._-]+", "_", value.lower()).strip("._-")


def validate_contract(contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if contract.get("schema_version") != "spacegate.scientific_evidence_contract.v2":
        errors.append("unsupported scientific evidence contract")
    if set(contract.get("domain_tables") or []) != DOMAIN_TABLES:
        errors.append("domain_tables must exactly match the compiler contract")
    dispositions = set(contract.get("field_dispositions") or [])
    mapping_statuses = set(contract.get("mapping_statuses") or [])
    identifier_claims = contract.get("identifier_claims") or {}
    unit_normalizations = contract.get("unit_normalizations") or {}
    profiles = contract.get("field_profiles") or {}
    if not identifier_claims:
        errors.append("identifier_claims must not be empty")
    for field, claim in identifier_claims.items():
        if (
            not str(field).strip()
            or not str(claim.get("namespace") or "").strip()
            or not str(claim.get("claim_scope") or "").strip()
        ):
            errors.append(
                "identifier claim fields, namespaces, and scopes must be non-empty"
            )
        if any(not str(value).strip() for value in claim.get("excluded_values") or []):
            errors.append(f"identifier claim has an empty excluded value: {field}")
    if not unit_normalizations:
        errors.append("unit_normalizations must not be empty")
    for raw_unit, normalized_unit in unit_normalizations.items():
        if not str(raw_unit).strip() or not str(normalized_unit).strip():
            errors.append("unit normalization aliases must be non-empty")
    for profile_name, rules in profiles.items():
        if not rules:
            errors.append(f"field profile has no rules: {profile_name}")
            continue
        for index, rule in enumerate(rules):
            try:
                re.compile(str(rule.get("pattern") or ""))
            except re.error as exc:
                errors.append(f"invalid regex {profile_name}[{index}]: {exc}")
            if rule.get("disposition") not in dispositions:
                errors.append(f"invalid disposition {profile_name}[{index}]")
            destination = str(rule.get("destination") or "")
            if destination not in DOMAIN_TABLES | {"source_records"}:
                errors.append(f"invalid destination {profile_name}[{index}]: {destination}")
            if not rule.get("reason"):
                errors.append(f"missing reason {profile_name}[{index}]")
        if re.fullmatch(str(rules[-1].get("pattern") or ""), "unmatched_field") is None:
            errors.append(f"field profile lacks a final catch-all: {profile_name}")
    for source_id, adapter in (contract.get("source_adapters") or {}).items():
        if not adapter.get("adapter_version") or not adapter.get("tables"):
            errors.append(f"incomplete source adapter: {source_id}")
        for table_name, table in (adapter.get("tables") or {}).items():
            if not table.get("logical_key_fields"):
                errors.append(f"{source_id}.{table_name} lacks logical key fields")
            if table.get("field_profile") not in profiles:
                errors.append(f"{source_id}.{table_name} references an unknown field profile")
            if table.get("raw_artifact_name") is not None and not str(
                table.get("raw_artifact_name")
            ).strip():
                errors.append(f"{source_id}.{table_name} has an empty raw artifact name")
            for field, claim in (table.get("identifier_claims") or {}).items():
                if (
                    not str(field).strip()
                    or not str(claim.get("namespace") or "").strip()
                    or not str(claim.get("claim_scope") or "").strip()
                ):
                    errors.append(
                        f"{source_id}.{table_name} has an incomplete identifier claim"
                    )
                if any(
                    not str(value).strip()
                    for value in claim.get("excluded_values") or []
                ):
                    errors.append(
                        f"{source_id}.{table_name}.{field} has an empty excluded value"
                    )
            relation_claim = table.get("relation_claim")
            if relation_claim:
                prefix = f"{source_id}.{table_name}.relation_claim"
                required = {
                    "left_identifier_field",
                    "left_identifier_namespace",
                    "right_identifier_field",
                    "right_identifier_namespace",
                    "relation_kind",
                    "relation_scope",
                    "evidence_polarity",
                    "method",
                    "reference_raw",
                }
                missing = sorted(required - set(relation_claim))
                if missing:
                    errors.append(f"{prefix} lacks {missing}")
                probability_field = relation_claim.get("probability_field")
                probability_semantics = relation_claim.get("probability_semantics")
                if bool(probability_field) != bool(probability_semantics):
                    errors.append(
                        f"{prefix} must define probability field and semantics together"
                    )
                statistic_field = relation_claim.get("confidence_statistic_field")
                statistic_key = relation_claim.get("confidence_statistic_key")
                statistic_semantics = relation_claim.get(
                    "confidence_statistic_semantics"
                )
                if any((statistic_field, statistic_key, statistic_semantics)) and not all(
                    (statistic_field, statistic_key, statistic_semantics)
                ):
                    errors.append(
                        f"{prefix} confidence statistic requires field, key, and semantics"
                    )
            row_selection = table.get("row_selection")
            if row_selection:
                prefix = f"{source_id}.{table_name}.row_selection"
                missing = sorted(
                    {"policy_id", "sql_predicate", "reason"} - set(row_selection)
                )
                if missing:
                    errors.append(f"{prefix} lacks {missing}")
                if any(
                    not str(row_selection.get(field) or "").strip()
                    for field in ("policy_id", "sql_predicate", "reason")
                ):
                    errors.append(f"{prefix} values must be non-empty")
            orbital_solution = table.get("orbital_solution")
            if orbital_solution:
                prefix = f"{source_id}.{table_name}.orbital_solution"
                required = {
                    "solution_key_fields",
                    "parameter_fields",
                    "quality_fields",
                    "method",
                    "normalization_version",
                }
                missing = sorted(required - set(orbital_solution))
                if missing:
                    errors.append(f"{prefix} lacks {missing}")
                for field in required:
                    value = orbital_solution.get(field)
                    if value is None or value == "" or value == []:
                        errors.append(f"{prefix}.{field} must be non-empty")
            for index, parameter_set in enumerate(
                table.get("scoped_stellar_parameter_sets") or []
            ):
                prefix = (
                    f"{source_id}.{table_name}.scoped_stellar_parameter_sets[{index}]"
                )
                required = {
                    "parameter_set_kind",
                    "method",
                    "normalization_version",
                    "measurements",
                }
                missing = sorted(required - set(parameter_set))
                if missing:
                    errors.append(f"{prefix} lacks {missing}")
                for field in required:
                    value = parameter_set.get(field)
                    if value is None or value == "" or value == []:
                        errors.append(f"{prefix}.{field} must be non-empty")
                if not str(
                    parameter_set.get("scope_key")
                    or parameter_set.get("component_scope")
                    or ""
                ).strip():
                    errors.append(f"{prefix} requires scope_key or component_scope")
                for measurement_index, measurement in enumerate(
                    parameter_set.get("measurements") or []
                ):
                    measurement_prefix = f"{prefix}.measurements[{measurement_index}]"
                    for field in ("value_field", "quantity_key"):
                        if not str(measurement.get(field) or "").strip():
                            errors.append(f"{measurement_prefix}.{field} must be non-empty")
            for index, measurement in enumerate(
                table.get("photometry_measurements") or []
            ):
                prefix = f"{source_id}.{table_name}.photometry_measurements[{index}]"
                for field in ("value_field", "quantity_key"):
                    if not str(measurement.get(field) or "").strip():
                        errors.append(f"{prefix}.{field} must be non-empty")
            for index, claim in enumerate(table.get("lifecycle_claims") or []):
                prefix = f"{source_id}.{table_name}.lifecycle_claims[{index}]"
                if not claim.get("claim_role"):
                    errors.append(f"{prefix} lacks claim_role")
                if not claim.get("identifier_field"):
                    errors.append(f"{prefix} lacks identifier_field")
                elif claim["identifier_field"] not in identifier_claims:
                    errors.append(f"{prefix} identifier lacks a namespace")
                disposition_sources = sum(
                    bool(claim.get(field))
                    for field in ("disposition_field", "implicit_disposition")
                )
                if disposition_sources != 1:
                    errors.append(
                        f"{prefix} must define exactly one disposition source"
                    )
                for field in claim.get("context_fields") or []:
                    if not str(field).strip():
                        errors.append(f"{prefix} contains an empty context field")
    if mapping_statuses != {"materialized", "declared_pending", "excluded"}:
        errors.append("mapping_statuses do not match the compiler contract")
    return errors


def latest_manifest(root: Path, manifest_name: str) -> tuple[Path, dict[str, Any]]:
    candidates = list(root.rglob(manifest_name)) if root.exists() else []
    if not candidates:
        raise FileNotFoundError(f"no {manifest_name} under {root}")
    rows = [(path, load_json(path)) for path in candidates]
    return max(
        rows,
        key=lambda item: (
            str(item[1].get("created_at") or item[1].get("retrieved_at") or ""),
            item[0].as_posix(),
        ),
    )


def source_input(
    state_dir: Path,
    registry_source: dict[str, Any],
) -> dict[str, Any]:
    source_id = str(registry_source["source_id"])
    release_id = str(registry_source["release_id"])
    raw_root = state_dir / "raw" / "evidence_lake_v2" / slug(source_id) / slug(release_id)
    raw_path, raw_manifest = latest_manifest(raw_root, "snapshot_manifest.json")
    typed_root = (
        state_dir
        / "typed"
        / "evidence_lake_v2"
        / slug(source_id)
        / slug(release_id)
        / str(raw_manifest["snapshot_id"])
    )
    typed_path, typed_manifest = latest_manifest(typed_root, "typed_manifest.json")
    if typed_manifest.get("raw_content_sha256") != raw_manifest.get("content_sha256"):
        raise ValueError(f"raw/typed content mismatch for {source_id}")
    pending = [table for table in typed_manifest["tables"] if table["status"] != "typed"]
    if pending:
        raise ValueError(f"typed source contains pending tables: {source_id}")
    return {
        "source_id": source_id,
        "release_id": release_id,
        "raw_path": raw_path.parent,
        "raw_manifest": raw_manifest,
        "typed_path": typed_path.parent,
        "typed_manifest": typed_manifest,
    }


def deterministic_build_timestamp(inputs: list[dict[str, Any]]) -> str:
    timestamps = [
        str(artifact.get("retrieved_at") or "")
        for input_row in inputs
        for artifact in input_row["raw_manifest"]["artifacts"]
        if artifact.get("retrieved_at")
    ]
    if not timestamps:
        raise ValueError("scientific evidence inputs have no retrieval timestamps")
    return max(timestamps)


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table evidence_build (
          build_id varchar primary key,
          contract_version varchar not null,
          compiler_version varchar not null,
          input_fingerprint varchar not null,
          build_status varchar not null,
          created_at timestamp not null
        );
        create table evidence_sources (
          source_id varchar not null,
          release_id varchar not null,
          adapter_version varchar not null,
          raw_snapshot_id varchar not null,
          raw_content_sha256 varchar not null,
          typed_snapshot_id varchar not null,
          typed_content_sha256 varchar not null,
          primary key (source_id, release_id)
        );
        create table source_records (
          source_record_id varchar primary key,
          source_id varchar not null,
          release_id varchar not null,
          source_table varchar not null,
          object_scope varchar not null,
          logical_key_json json not null,
          source_context_json json not null,
          source_row_sha256 varchar not null,
          source_duplicate_count bigint not null,
          raw_snapshot_id varchar not null,
          typed_snapshot_id varchar not null,
          raw_artifact_sha256 varchar not null,
          typed_table_sha256 varchar not null,
          retrieved_at timestamp,
          unique (source_id, release_id, source_table, source_row_sha256)
        );
        create table source_field_dispositions (
          source_id varchar not null,
          release_id varchar not null,
          source_table varchar not null,
          source_field varchar not null,
          source_datatype varchar,
          source_unit varchar,
          source_ucd varchar,
          source_description varchar,
          disposition varchar not null,
          destination_table varchar not null,
          mapping_status varchar not null,
          reason varchar not null,
          adapter_version varchar not null,
          primary key (source_id, release_id, source_table, source_field)
        );
        create table object_binding_outcomes (
          binding_outcome_id varchar primary key,
          source_record_id varchar not null,
          binding_status varchar not null,
          binding_scope varchar not null,
          identity_node_id varchar,
          spacegate_object_type varchar,
          spacegate_object_id bigint,
          stable_object_key varchar,
          component_scope varchar,
          reason varchar not null,
          provenance_json json not null
        );
        create table identifier_claim_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          namespace varchar not null,
          identifier_raw varchar not null,
          identifier_normalized varchar,
          claim_scope varchar not null,
          component_scope varchar,
          reference_raw varchar,
          quality_json json
        );
        create table stellar_parameter_sets (
          parameter_set_id varchar primary key,
          source_record_id varchar not null,
          component_scope varchar,
          method varchar,
          model varchar,
          reference_raw varchar,
          epoch_raw varchar,
          frame_raw varchar,
          quality_json json
        );
        create table stellar_parameter_evidence (
          evidence_id varchar primary key,
          parameter_set_id varchar not null,
          source_record_id varchar not null,
          component_scope varchar,
          quantity_key varchar not null,
          value_raw varchar,
          unit_raw varchar,
          normalized_value double,
          normalized_unit varchar,
          uncertainty_lower double,
          uncertainty_upper double,
          bound_semantics varchar,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table stellar_classification_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          component_scope varchar,
          classification_scheme varchar not null,
          classification_raw varchar not null,
          classification_normalized varchar,
          probability double,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json
        );
        create table astrometry_distance_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          quantity_key varchar not null,
          value_raw varchar,
          unit_raw varchar,
          normalized_value double,
          normalized_unit varchar,
          uncertainty_lower double,
          uncertainty_upper double,
          bound_semantics varchar,
          frame_raw varchar,
          epoch_raw varchar,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table photometry_extinction_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          quantity_key varchar not null,
          bandpass varchar,
          value_raw varchar,
          unit_raw varchar,
          normalized_value double,
          normalized_unit varchar,
          uncertainty_lower double,
          uncertainty_upper double,
          bound_semantics varchar,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table spectra_product_index (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          survey varchar not null,
          product_key varchar not null,
          product_locator varchar,
          spectral_range_raw varchar,
          resolving_power_raw varchar,
          observation_epoch_raw varchar,
          quality_json json
        );
        create table variability_activity_rotation_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          evidence_kind varchar not null,
          quantity_key varchar,
          value_raw varchar,
          unit_raw varchar,
          normalized_value double,
          normalized_unit varchar,
          uncertainty_lower double,
          uncertainty_upper double,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table relation_claim_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          left_identity_namespace varchar not null,
          left_identity_raw varchar not null,
          right_identity_namespace varchar not null,
          right_identity_raw varchar not null,
          relation_kind varchar not null,
          relation_scope varchar not null,
          probability double,
          probability_semantics varchar,
          confidence_statistic_key varchar,
          confidence_statistic_value_raw varchar,
          confidence_statistic_value double,
          confidence_statistic_unit varchar,
          confidence_statistic_semantics varchar,
          evidence_polarity varchar not null,
          method varchar,
          reference_raw varchar,
          epoch_raw varchar,
          quality_json json
        );
        create table orbital_solution_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          relation_claim_id varchar,
          solution_key varchar not null,
          parameter_set_raw json,
          epoch_raw varchar,
          frame_raw varchar,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table cluster_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          cluster_identity_raw varchar not null,
          parameter_set_raw json,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table cluster_membership_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          cluster_identity_raw varchar not null,
          member_identity_raw varchar not null,
          membership_probability double,
          method varchar,
          reference_raw varchar,
          quality_json json
        );
        create table planet_parameter_sets (
          parameter_set_id varchar primary key,
          source_record_id varchar not null,
          parameter_set_kind varchar not null,
          method varchar,
          model varchar,
          reference_raw varchar,
          epoch_raw varchar,
          frame_raw varchar,
          quality_json json
        );
        create table planet_parameter_evidence (
          evidence_id varchar primary key,
          parameter_set_id varchar not null,
          source_record_id varchar not null,
          quantity_key varchar not null,
          value_raw varchar,
          unit_raw varchar,
          normalized_value double,
          normalized_unit varchar,
          uncertainty_lower double,
          uncertainty_upper double,
          bound_semantics varchar,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table planet_lifecycle_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          source_identifier_raw varchar not null,
          disposition_raw varchar not null,
          disposition_normalized varchar,
          evidence_polarity varchar not null,
          effective_at_raw varchar,
          supersedes_evidence_id varchar,
          reference_raw varchar,
          quality_json json
        );
        create table transit_observation_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          signal_identifier_raw varchar,
          quantity_key varchar not null,
          value_raw varchar,
          unit_raw varchar,
          normalized_value double,
          normalized_unit varchar,
          uncertainty_lower double,
          uncertainty_upper double,
          bound_semantics varchar,
          observation_epoch_raw varchar,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table radial_velocity_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          observation_identifier_raw varchar,
          quantity_key varchar not null,
          value_raw varchar,
          unit_raw varchar,
          normalized_value double,
          normalized_unit varchar,
          uncertainty_lower double,
          uncertainty_upper double,
          bound_semantics varchar,
          observation_epoch_raw varchar,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table compact_object_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          compact_kind varchar not null,
          parameter_set_key varchar,
          parameter_set_raw json,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table extended_object_evidence (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          extended_kind varchar not null,
          geometry_raw json,
          distance_raw json,
          method varchar,
          model varchar,
          reference_raw varchar,
          quality_json json,
          normalization_version varchar
        );
        create table citations (
          citation_id varchar primary key,
          source_id varchar not null,
          source_reference_key varchar,
          citation_text_raw varchar not null,
          citation_url varchar,
          bibcode varchar,
          doi varchar,
          publication_year integer,
          parsed_json json
        );
        create table evidence_citations (
          evidence_table varchar not null,
          evidence_id varchar not null,
          citation_id varchar not null,
          citation_role varchar not null,
          primary key (evidence_table, evidence_id, citation_id, citation_role)
        );
        create table observation_product_lineage (
          evidence_id varchar primary key,
          source_record_id varchar not null,
          product_kind varchar not null,
          product_key varchar not null,
          product_locator varchar,
          retrieval_policy varchar not null,
          checksum varchar,
          bytes bigint,
          observation_epoch_raw varchar,
          processing_level varchar,
          quality_json json
        );
        """
    )


def classify_field(
    field_name: str,
    rules: list[dict[str, str]],
) -> tuple[dict[str, str], int]:
    matches = [
        (index, rule)
        for index, rule in enumerate(rules)
        if re.fullmatch(rule["pattern"], field_name)
    ]
    if not matches:
        raise ValueError(f"field is not accounted by its profile: {field_name}")
    return matches[0][1], matches[0][0]


def artifact_by_name(raw_manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["source_name"]): row for row in raw_manifest["artifacts"]}


def typed_table_by_name(typed_manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["source_name"]): row for row in typed_manifest["tables"]}


def product_manifest(input_row: dict[str, Any], artifact: dict[str, Any]) -> dict[str, Any]:
    root = input_row["raw_path"] / str(artifact["artifact_path"])
    candidates = list(root.rglob("product_manifest.json"))
    if not candidates:
        return {}
    if len(candidates) != 1:
        raise ValueError(
            f"expected one product manifest for {artifact['source_name']}, found {len(candidates)}"
        )
    return load_json(candidates[0])


def source_field_metadata(
    typed_table: dict[str, Any],
    product: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return source-native field metadata for delimited, FITS, and document tables."""
    dispositions = product.get("field_dispositions") or []
    if dispositions:
        return list(dispositions)

    source_schema = typed_table.get("source_schema")
    if isinstance(source_schema, dict):
        source_schema = source_schema.get("source_schema")
    typed_columns = typed_table.get("columns") or []
    if not isinstance(source_schema, list):
        source_schema = typed_columns
    schema_by_name = {
        str(field.get("column_name") or field.get("name")): field
        for field in source_schema
        if field.get("column_name") or field.get("name")
    }
    ordered_schema = typed_columns or source_schema

    fields = []
    for typed_field in ordered_schema:
        name = typed_field.get("column_name") or typed_field.get("name")
        if not name:
            raise ValueError(
                f"source-native schema contains an unnamed field: {typed_table['source_name']}"
            )
        field = dict(typed_field)
        field.update(schema_by_name.get(str(name), {}))
        fields.append(
            {
                "column_name": str(name),
                "datatype": (
                    field.get("datatype")
                    or field.get("type")
                    or field.get("arrow_type")
                    or field.get("source_format")
                ),
                "unit": field.get("unit"),
                "ucd": field.get("ucd"),
                "description": field.get("description"),
            }
        )
    return fields


def logical_key_expression(fields: list[str]) -> str:
    members = ", ".join(
        f"{sql_identifier(field)} := {sql_identifier(field)}" for field in fields
    )
    return f"to_json(struct_pack({members}))"


def row_selection_predicate(table_contract: dict[str, Any]) -> str:
    selection = table_contract.get("row_selection") or {}
    return str(selection.get("sql_predicate") or "true")


def normalized_disposition_expression(value: str) -> str:
    normalized = f"upper(trim(cast({value} as varchar)))"
    return f"""
      case {normalized}
        when 'CP' then 'CONFIRMED'
        when 'KP' then 'CONFIRMED'
        when 'CONFIRMED' then 'CONFIRMED'
        when 'KNOWN PLANET' then 'CONFIRMED'
        when 'PC' then 'CANDIDATE'
        when 'APC' then 'CANDIDATE'
        when 'CANDIDATE' then 'CANDIDATE'
        when 'FP' then 'FALSE_POSITIVE'
        when 'FALSE POSITIVE' then 'FALSE_POSITIVE'
        when 'FA' then 'FALSE_ALARM'
        when 'FALSE ALARM' then 'FALSE_ALARM'
        else replace({normalized}, ' ', '_')
      end
    """


def lifecycle_polarity_expression(value: str) -> str:
    return f"""
      case {value}
        when 'CONFIRMED' then 'positive'
        when 'CANDIDATE' then 'candidate'
        when 'FALSE_POSITIVE' then 'negative'
        when 'FALSE_ALARM' then 'negative'
        when 'REFUTED' then 'negative'
        else 'ambiguous'
      end
    """


def materialize_identifier_claims(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    fields: list[str],
    claim_by_field: dict[str, dict[str, str]],
) -> set[str]:
    missing = sorted(set(fields) - set(claim_by_field))
    if missing:
        raise ValueError(f"identifier namespace mappings missing for {table_name}: {missing}")
    branches = []
    for field in fields:
        quoted = sql_identifier(field)
        claim = claim_by_field[field]
        namespace = claim["namespace"]
        claim_scope = claim["claim_scope"]
        component_scope = claim.get("component_scope")
        excluded_values = [str(value) for value in claim.get("excluded_values") or []]
        excluded_predicate = ""
        if excluded_values:
            values = ", ".join(sql_string(value) for value in excluded_values)
            excluded_predicate = f"and trim(cast({quoted} as varchar)) not in ({values})"
        evidence_namespace = f"identifier|{field}|"
        branches.append(
            f"""
            select
              sha256({sql_string(evidence_namespace)} || r.source_record_id || '|' || s.value_raw),
              r.source_record_id,
              {sql_string(namespace)},
              s.value_raw,
              trim(s.value_raw),
              {sql_string(claim_scope)},
              {nullable_sql_string(component_scope)},
              null,
              json_object('source_field', {sql_string(field)})
            from (
              select distinct
                sha256(to_json(t)) source_row_sha256,
                trim(cast({quoted} as varchar)) value_raw
              from read_parquet({sql_string(str(path))}) t
              where nullif(trim(cast({quoted} as varchar)), '') is not null
                {excluded_predicate}
            ) s
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=s.source_row_sha256
            """
        )
    if branches:
        con.execute(
            "insert into identifier_claim_evidence " + " union all ".join(branches)
        )
    return set(fields)


def optional_field_expression(field: str | None) -> str:
    return (
        f"cast({sql_identifier(field)} as varchar)"
        if field
        else "null::varchar"
    )


SCALAR_EVIDENCE_DESTINATIONS = {
    "stellar_parameter_evidence",
    "astrometry_distance_evidence",
    "photometry_extinction_evidence",
    "variability_activity_rotation_evidence",
    "planet_parameter_evidence",
    "transit_observation_evidence",
    "radial_velocity_evidence",
}

AUXILIARY_SUFFIXES = (
    ("_systemref", "system_reference"),
    ("_reflink", "reference"),
    ("_symerr", "symmetric_error_flag"),
    ("_err1", "error_upper"),
    ("_err2", "error_lower"),
    ("_err", "error_symmetric"),
    ("_lim", "limit"),
    ("_str", "formatted"),
    ("symerr", "symmetric_error_flag"),
    ("err1", "error_upper"),
    ("err2", "error_lower"),
    ("lim", "limit"),
    ("str", "formatted"),
)


def split_auxiliary_field(field: str) -> tuple[str, str] | None:
    for suffix, role in AUXILIARY_SUFFIXES:
        if field.endswith(suffix) and len(field) > len(suffix):
            return field[: -len(suffix)], role
    return None


def scalar_field_groups(
    destination_fields: list[str],
    available_fields: set[str],
) -> list[dict[str, Any]]:
    destination = set(destination_fields)
    auxiliary_fields: set[str] = set()
    for field in destination:
        split = split_auxiliary_field(field)
        if split and split[0] in destination:
            auxiliary_fields.add(field)
    groups = {
        field: {"base_field": field, "auxiliary": {}}
        for field in sorted(destination - auxiliary_fields)
    }
    for field in sorted(available_fields):
        split = split_auxiliary_field(field)
        if not split:
            continue
        base, role = split
        if base in groups:
            groups[base]["auxiliary"].setdefault(role, field)
    return list(groups.values())


def nullable_sql_string(value: Any) -> str:
    return (
        "null::varchar"
        if value is None or not str(value).strip()
        else sql_string(str(value))
    )


def text_expression(field: str | None) -> str:
    return (
        f"trim(cast({sql_identifier(field)} as varchar))"
        if field
        else "null::varchar"
    )


def double_expression(field: str | None, *, absolute: bool = False) -> str:
    if not field:
        return "null::double"
    expression = f"try_cast({sql_identifier(field)} as double)"
    return f"abs({expression})" if absolute else expression


def scalar_bound_expression(limit_field: str | None) -> str:
    if not limit_field:
        return "null::varchar"
    raw = f"try_cast({sql_identifier(limit_field)} as integer)"
    return f"""
      case {raw}
        when -1 then 'lower_limit'
        when 0 then 'measurement'
        when 1 then 'upper_limit'
        else 'source_limit_' || cast({raw} as varchar)
      end
    """


def scalar_reference_field(
    destination: str,
    auxiliary: dict[str, str],
    table_contract: dict[str, Any],
) -> str | None:
    if auxiliary.get("reference"):
        return auxiliary["reference"]
    if destination == "stellar_parameter_evidence":
        return (table_contract.get("stellar_parameter_set") or {}).get(
            "reference_field"
        )
    if destination in {
        "planet_parameter_evidence",
        "transit_observation_evidence",
        "radial_velocity_evidence",
    }:
        return (table_contract.get("planet_parameter_set") or {}).get(
            "reference_field"
        )
    return table_contract.get("system_reference_field")


def source_quantity_key(base_field: str) -> str:
    return f"nasa_exoplanet_archive.{base_field}"


def photometry_bandpass(base_field: str) -> str | None:
    match = re.search(r"(?:^|_)([a-z0-9]+)mag$", base_field)
    return match.group(1).upper() if match else None


def astrometry_frame(base_field: str) -> str | None:
    if base_field in {"glat", "glon"}:
        return "Galactic"
    if base_field in {"elat", "elon"}:
        return "Ecliptic"
    if base_field in {"ra", "dec"} or base_field.startswith(("sy_pm", "st_pm")):
        return "ICRS"
    return None


def signal_identifier_expression(table_contract: dict[str, Any]) -> str:
    fields = [str(field) for field in table_contract.get("signal_identifier_fields") or []]
    return logical_key_expression(fields) if fields else "null::varchar"


def scalar_quality_expression(
    base_field: str,
    auxiliary: dict[str, str],
    metadata: dict[str, Any],
) -> str:
    return f"""
      json_object(
        'source_field', {sql_string(base_field)},
        'source_description', {nullable_sql_string(metadata.get('description'))},
        'source_ucd', {nullable_sql_string(metadata.get('ucd'))},
        'error_lower_field', {nullable_sql_string(auxiliary.get('error_lower'))},
        'error_upper_field', {nullable_sql_string(auxiliary.get('error_upper'))},
        'error_symmetric_field', {nullable_sql_string(auxiliary.get('error_symmetric'))},
        'limit_field', {nullable_sql_string(auxiliary.get('limit'))},
        'limit_raw', {text_expression(auxiliary.get('limit'))},
        'formatted_field', {nullable_sql_string(auxiliary.get('formatted'))},
        'formatted_value_raw', {text_expression(auxiliary.get('formatted'))},
        'symmetric_error_flag_field', {nullable_sql_string(auxiliary.get('symmetric_error_flag'))},
        'symmetric_error_flag_raw', {text_expression(auxiliary.get('symmetric_error_flag'))},
        'system_reference_field', {nullable_sql_string(auxiliary.get('system_reference'))},
        'system_reference_raw', {text_expression(auxiliary.get('system_reference'))}
      )
    """


def parameter_set_id_expression(
    destination: str,
    table_contract: dict[str, Any],
) -> tuple[str, str]:
    config_key = (
        "stellar_parameter_set"
        if destination == "stellar_parameter_evidence"
        else "planet_parameter_set"
    )
    parameter_set = table_contract.get(config_key)
    if not parameter_set:
        raise ValueError(f"{destination} lacks {config_key} configuration")
    kind = str(parameter_set["kind"])
    namespace = (
        "stellar-parameter-set"
        if config_key.startswith("stellar")
        else "planet-parameter-set"
    )
    expression = f"sha256({sql_string(namespace + '|' + kind + '|')} || r.source_record_id)"
    return expression, kind


def scalar_select_branch(
    *,
    destination: str,
    group: dict[str, Any],
    metadata: dict[str, Any],
    table_contract: dict[str, Any],
    path: Path,
    source_id: str,
    release_id: str,
    table_name: str,
    unit_normalizations: dict[str, str],
) -> str:
    base = str(group["base_field"])
    auxiliary = group["auxiliary"]
    raw_value = text_expression(base)
    raw_unit = metadata.get("unit")
    unit = nullable_sql_string(raw_unit)
    normalized_unit = nullable_sql_string(
        unit_normalizations.get(str(raw_unit), raw_unit)
        if raw_unit is not None
        else None
    )
    normalized_value = double_expression(base)
    lower = double_expression(
        auxiliary.get("error_lower") or auxiliary.get("error_symmetric"),
        absolute=True,
    )
    upper = double_expression(
        auxiliary.get("error_upper") or auxiliary.get("error_symmetric"),
        absolute=True,
    )
    bound = scalar_bound_expression(auxiliary.get("limit"))
    reference = text_expression(
        scalar_reference_field(destination, auxiliary, table_contract)
    )
    quality = scalar_quality_expression(base, auxiliary, metadata)
    quantity = source_quantity_key(base)
    evidence_id = (
        f"sha256({sql_string('scalar|' + destination + '|' + base + '|')} "
        "|| r.source_record_id)"
    )
    common_from = f"""
      from read_parquet({sql_string(str(path))}) t
      join source_records r
        on r.source_id={sql_string(source_id)}
       and r.release_id={sql_string(release_id)}
       and r.source_table={sql_string(table_name)}
       and r.source_row_sha256=sha256(to_json(t))
      where nullif({raw_value}, '') is not null
    """
    normalization = sql_string("nasa_unit_alias_v1")
    if destination in {"stellar_parameter_evidence", "planet_parameter_evidence"}:
        parameter_set_id, _kind = parameter_set_id_expression(
            destination, table_contract
        )
        component_scope = "null::varchar, " if destination == "stellar_parameter_evidence" else ""
        return f"""
          select distinct
            {evidence_id}, {parameter_set_id}, r.source_record_id, {component_scope}
            {sql_string(quantity)}, {raw_value}, {unit}, {normalized_value}, {normalized_unit},
            {lower}, {upper}, {bound}, null, null, {reference}, {quality}, {normalization}
          {common_from}
        """
    if destination == "astrometry_distance_evidence":
        frame = nullable_sql_string(astrometry_frame(base))
        return f"""
          select distinct
            {evidence_id}, r.source_record_id, {sql_string(quantity)}, {raw_value},
            {unit}, {normalized_value}, {normalized_unit}, {lower}, {upper}, {bound},
            {frame}, null, null, null, {reference}, {quality}, {normalization}
          {common_from}
        """
    if destination == "photometry_extinction_evidence":
        bandpass = nullable_sql_string(photometry_bandpass(base))
        quantity_key = "magnitude" if photometry_bandpass(base) else quantity
        return f"""
          select distinct
            {evidence_id}, r.source_record_id, {sql_string(quantity_key)}, {bandpass},
            {raw_value}, {unit}, {normalized_value}, {normalized_unit}, {lower}, {upper},
            {bound}, null, null, {reference}, {quality}, {normalization}
          {common_from}
        """
    if destination == "variability_activity_rotation_evidence":
        kind = (
            "rotation_period"
            if "rotp" in base
            else "projected_rotation_velocity"
            if "vsin" in base
            else "variability_or_activity"
        )
        return f"""
          select distinct
            {evidence_id}, r.source_record_id, {sql_string(kind)},
            {sql_string(quantity)}, {raw_value}, {unit}, {normalized_value}, {normalized_unit},
            {lower}, {upper}, null, null, {reference}, {quality}, {normalization}
          {common_from}
        """
    if destination == "transit_observation_evidence":
        signal = signal_identifier_expression(table_contract)
        return f"""
          select distinct
            {evidence_id}, r.source_record_id, cast({signal} as varchar),
            {sql_string(quantity)}, {raw_value}, {unit}, {normalized_value}, {normalized_unit},
            {lower}, {upper}, {bound}, null, null, null, {reference},
            {quality}, {normalization}
          {common_from}
        """
    if destination == "radial_velocity_evidence":
        signal = signal_identifier_expression(table_contract)
        return f"""
          select distinct
            {evidence_id}, r.source_record_id, cast({signal} as varchar),
            {sql_string(quantity)}, {raw_value}, {unit}, {normalized_value}, {normalized_unit},
            {lower}, {upper}, {bound}, null, null, null, {reference},
            {quality}, {normalization}
          {common_from}
        """
    raise ValueError(f"unsupported scalar destination: {destination}")


def materialize_scalar_evidence(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    destination: str,
    fields: list[dict[str, Any]],
    available_fields: set[str],
    table_contract: dict[str, Any],
    unit_normalizations: dict[str, str],
) -> set[str]:
    metadata_by_field = {str(field["column_name"]): field for field in fields}
    groups = scalar_field_groups(list(metadata_by_field), available_fields)
    branches = [
        scalar_select_branch(
            destination=destination,
            group=group,
            metadata=metadata_by_field[str(group["base_field"])],
            table_contract=table_contract,
            path=path,
            source_id=source_id,
            release_id=release_id,
            table_name=table_name,
            unit_normalizations=unit_normalizations,
        )
        for group in groups
    ]
    if branches:
        con.execute(f"insert into {sql_identifier(destination)} " + " union all ".join(branches))
    consumed = set(metadata_by_field)
    for group in groups:
        consumed.update(group["auxiliary"].values())
    return consumed


def materialize_parameter_sets(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    table_contract: dict[str, Any],
) -> None:
    for destination, set_table, config_key in (
        ("planet_parameter_evidence", "planet_parameter_sets", "planet_parameter_set"),
        ("stellar_parameter_evidence", "stellar_parameter_sets", "stellar_parameter_set"),
    ):
        parameter_set = table_contract.get(config_key)
        if not parameter_set:
            continue
        kind = str(parameter_set["kind"])
        reference = (
            "any_value(e.reference_raw)"
            if parameter_set.get("reference_field")
            else "null::varchar"
        )
        if set_table == "planet_parameter_sets":
            select_columns = (
                f"e.parameter_set_id, e.source_record_id, {sql_string(kind)}, "
                f"null, null, {reference}, null, null, "
                f"json_object('parameter_set_kind', {sql_string(kind)})"
            )
        else:
            select_columns = (
                f"e.parameter_set_id, e.source_record_id, e.component_scope, "
                f"null, null, {reference}, "
                f"null, null, json_object('parameter_set_kind', {sql_string(kind)})"
            )
        con.execute(
            f"""
            insert into {sql_identifier(set_table)}
            select {select_columns}
            from {sql_identifier(destination)} e
            join source_records r using (source_record_id)
            where r.source_id={sql_string(source_id)}
              and r.release_id={sql_string(release_id)}
              and r.source_table={sql_string(table_name)}
            group by e.parameter_set_id, e.source_record_id{', e.component_scope' if set_table == 'stellar_parameter_sets' else ''}
            """
        )


def materialize_stellar_classifications(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    fields: list[dict[str, Any]],
    table_contract: dict[str, Any],
) -> set[str]:
    branches = []
    reference_field = (table_contract.get("stellar_parameter_set") or {}).get(
        "reference_field"
    )
    for field in fields:
        name = str(field["column_name"])
        raw = text_expression(name)
        branches.append(
            f"""
            select distinct
              sha256({sql_string('classification|' + name + '|')} || r.source_record_id),
              r.source_record_id, null, 'spectral_type', {raw}, null, null, null, null,
              {text_expression(reference_field)},
              json_object(
                'source_field', {sql_string(name)},
                'source_description', {nullable_sql_string(field.get('description'))}
              )
            from read_parquet({sql_string(str(path))}) t
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(t))
            where nullif({raw}, '') is not null
            """
        )
    if branches:
        con.execute(
            "insert into stellar_classification_evidence " + " union all ".join(branches)
        )
    return {str(field["column_name"]) for field in fields}


def configured_scoped_stellar_fields(table_contract: dict[str, Any]) -> set[str]:
    fields: set[str] = set()
    for parameter_set in table_contract.get("scoped_stellar_parameter_sets") or []:
        if parameter_set.get("classification_field"):
            fields.add(str(parameter_set["classification_field"]))
        if parameter_set.get("reference_field"):
            fields.add(str(parameter_set["reference_field"]))
        for measurement in parameter_set.get("measurements") or []:
            fields.add(str(measurement["value_field"]))
            if measurement.get("uncertainty_field"):
                fields.add(str(measurement["uncertainty_field"]))
    return fields


def missing_value_predicate(field: str, missing_values: list[Any]) -> str:
    raw = f"trim(cast({sql_identifier(field)} as varchar))"
    clauses = [f"nullif({raw}, '') is not null"]
    if missing_values:
        values = ", ".join(sql_string(str(value).strip().lower()) for value in missing_values)
        clauses.append(f"lower({raw}) not in ({values})")
    return " and ".join(clauses)


def nullable_measurement_double_expression(
    field: str | None,
    missing_values: list[Any],
    *,
    absolute: bool = False,
) -> str:
    if not field:
        return "null::double"
    value = f"try_cast({sql_identifier(field)} as double)"
    if absolute:
        value = f"abs({value})"
    return f"case when {missing_value_predicate(field, missing_values)} then {value} end"


def materialize_scoped_stellar_evidence(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    parameter_sets: list[dict[str, Any]],
    available_fields: set[str],
) -> set[str]:
    consumed: set[str] = set()
    for config in parameter_sets:
        component_scope = config.get("component_scope")
        scope = str(config.get("scope_key") or component_scope)
        component_scope_sql = nullable_sql_string(component_scope)
        kind = str(config["parameter_set_kind"])
        method = str(config["method"])
        normalization = str(config["normalization_version"])
        reference_field = config.get("reference_field")
        configured_fields = {
            str(value)
            for value in (config.get("classification_field"), reference_field)
            if value
        }
        for measurement in config["measurements"]:
            configured_fields.add(str(measurement["value_field"]))
            if measurement.get("uncertainty_field"):
                configured_fields.add(str(measurement["uncertainty_field"]))
        missing = sorted(configured_fields - available_fields)
        if missing:
            raise ValueError(
                f"scoped stellar fields missing from {table_name}.{scope}: {missing}"
            )
        consumed.update(configured_fields)
        reference = text_expression(reference_field)
        parameter_set_id = (
            f"sha256({sql_string('stellar-parameter-set|' + kind + '|' + scope + '|')} "
            "|| r.source_record_id)"
        )
        classification_field = config.get("classification_field")
        if classification_field:
            classification_field = str(classification_field)
            raw = text_expression(classification_field)
            predicate = missing_value_predicate(
                classification_field,
                list(config.get("classification_missing_values") or []),
            )
            con.execute(
                f"""
                insert into stellar_classification_evidence
                select distinct
                  sha256({sql_string('scoped-classification|' + scope + '|')} || r.source_record_id),
                  r.source_record_id,
                  {component_scope_sql},
                  {sql_string(str(config.get('classification_scheme') or 'spectral_type'))},
                  {raw}, null, null,
                  {sql_string(method)},
                  {nullable_sql_string(config.get('model'))},
                  {reference},
                  json_object(
                    'source_field', {sql_string(classification_field)},
                    'evidence_scope', {sql_string(scope)},
                    'component_scope', {component_scope_sql},
                    'missing_values', {sql_string(json.dumps(config.get('classification_missing_values') or []))}
                  )
                from read_parquet({sql_string(str(path))}) t
                join source_records r
                  on r.source_id={sql_string(source_id)}
                 and r.release_id={sql_string(release_id)}
                 and r.source_table={sql_string(table_name)}
                 and r.source_row_sha256=sha256(to_json(t))
                where {predicate}
                """
            )
        branches = []
        for measurement in config["measurements"]:
            field = str(measurement["value_field"])
            uncertainty_field = measurement.get("uncertainty_field")
            predicate = missing_value_predicate(
                field, list(measurement.get("missing_values") or [])
            )
            raw = text_expression(field)
            uncertainty = nullable_measurement_double_expression(
                str(uncertainty_field) if uncertainty_field else None,
                list(
                    measurement.get(
                        "uncertainty_missing_values",
                        measurement.get("missing_values") or [],
                    )
                ),
                absolute=True,
            )
            unit = nullable_sql_string(measurement.get("unit_raw"))
            normalized_unit = nullable_sql_string(
                measurement.get("normalized_unit", measurement.get("unit_raw"))
            )
            branches.append(
                f"""
                select distinct
                  sha256({sql_string('scoped-stellar|' + scope + '|' + field + '|')} || r.source_record_id),
                  {parameter_set_id}, r.source_record_id, {component_scope_sql},
                  {sql_string(str(measurement['quantity_key']))}, {raw}, {unit},
                  try_cast({sql_identifier(field)} as double), {normalized_unit},
                  {uncertainty}, {uncertainty}, 'measurement',
                  {sql_string(method)}, {nullable_sql_string(config.get('model'))},
                  {reference},
                  json_object(
                    'source_field', {sql_string(field)},
                    'uncertainty_field', {nullable_sql_string(uncertainty_field)},
                    'evidence_scope', {sql_string(scope)},
                    'component_scope', {component_scope_sql},
                    'parameter_set_kind', {sql_string(kind)},
                    'missing_values', {sql_string(json.dumps(measurement.get('missing_values') or []))}
                  ),
                  {sql_string(normalization)}
                from read_parquet({sql_string(str(path))}) t
                join source_records r
                  on r.source_id={sql_string(source_id)}
                 and r.release_id={sql_string(release_id)}
                 and r.source_table={sql_string(table_name)}
                 and r.source_row_sha256=sha256(to_json(t))
                where {predicate}
                """
            )
        if branches:
            con.execute(
                "insert into stellar_parameter_evidence " + " union all ".join(branches)
            )
            con.execute(
                f"""
                insert into stellar_parameter_sets
                select
                  e.parameter_set_id, e.source_record_id, e.component_scope,
                  {sql_string(method)}, {nullable_sql_string(config.get('model'))},
                  any_value(e.reference_raw), null, null,
                  json_object(
                    'parameter_set_kind', {sql_string(kind)},
                    'evidence_scope', {sql_string(scope)},
                    'component_scope', {component_scope_sql}
                  )
                from stellar_parameter_evidence e
                join source_records r using (source_record_id)
                where r.source_id={sql_string(source_id)}
                  and r.release_id={sql_string(release_id)}
                  and r.source_table={sql_string(table_name)}
                  and e.component_scope is not distinct from {component_scope_sql}
                  and e.parameter_set_id={parameter_set_id}
                group by e.parameter_set_id, e.source_record_id, e.component_scope
                """
            )
    return consumed


def materialize_configured_photometry(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    measurements: list[dict[str, Any]],
    available_fields: set[str],
) -> set[str]:
    consumed: set[str] = set()
    branches = []
    for measurement in measurements:
        field = str(measurement["value_field"])
        uncertainty_field = measurement.get("uncertainty_field")
        fields = {field} | ({str(uncertainty_field)} if uncertainty_field else set())
        missing = sorted(fields - available_fields)
        if missing:
            raise ValueError(f"photometry fields missing from {table_name}: {missing}")
        consumed.update(fields)
        raw = text_expression(field)
        uncertainty = nullable_measurement_double_expression(
            str(uncertainty_field) if uncertainty_field else None,
            list(
                measurement.get(
                    "uncertainty_missing_values",
                    measurement.get("missing_values") or [],
                )
            ),
            absolute=True,
        )
        unit = nullable_sql_string(measurement.get("unit_raw"))
        normalized_unit = nullable_sql_string(
            measurement.get("normalized_unit", measurement.get("unit_raw"))
        )
        predicate = missing_value_predicate(
            field, list(measurement.get("missing_values") or [])
        )
        branches.append(
            f"""
            select distinct
              sha256({sql_string('configured-photometry|' + field + '|')} || r.source_record_id),
              r.source_record_id, {sql_string(str(measurement['quantity_key']))},
              {nullable_sql_string(measurement.get('bandpass'))}, {raw}, {unit},
              try_cast({sql_identifier(field)} as double), {normalized_unit},
              {uncertainty}, {uncertainty}, 'measurement',
              {nullable_sql_string(measurement.get('method'))},
              {nullable_sql_string(measurement.get('model'))},
              {text_expression(measurement.get('reference_field'))},
              json_object(
                'source_field', {sql_string(field)},
                'missing_values', {sql_string(json.dumps(measurement.get('missing_values') or []))}
              ),
              {sql_string(str(measurement.get('normalization_version') or 'source_native_v1'))}
            from read_parquet({sql_string(str(path))}) t
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(t))
            where {predicate}
            """
        )
    if branches:
        con.execute(
            "insert into photometry_extinction_evidence " + " union all ".join(branches)
        )
    return consumed


def materialize_observation_products(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    fields: list[dict[str, Any]],
) -> set[str]:
    branches = []
    for field in fields:
        name = str(field["column_name"])
        raw = text_expression(name)
        product_kind = (
            "data_validation_report"
            if name.endswith("_dvr")
            else "data_validation_summary"
            if name.endswith("_dvs")
            else "archive_product"
        )
        branches.append(
            f"""
            select distinct
              sha256({sql_string('observation-product|' + name + '|')} || r.source_record_id),
              r.source_record_id, {sql_string(product_kind)}, {sql_string(name)},
              {raw}, 'on_demand', null, null, null, null,
              json_object(
                'source_field', {sql_string(name)},
                'source_description', {nullable_sql_string(field.get('description'))}
              )
            from read_parquet({sql_string(str(path))}) t
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(t))
            where nullif({raw}, '') is not null
            """
        )
    if branches:
        con.execute(
            "insert into observation_product_lineage " + " union all ".join(branches)
        )
    return {str(field["column_name"]) for field in fields}


def materialize_relation_claims(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    relation_claim: dict[str, Any],
    available_fields: set[str],
) -> set[str]:
    left_field = str(relation_claim["left_identifier_field"])
    right_field = str(relation_claim["right_identifier_field"])
    probability_field = relation_claim.get("probability_field")
    statistic_field = relation_claim.get("confidence_statistic_field")
    epoch_field = relation_claim.get("epoch_field")
    quality_fields = [str(field) for field in relation_claim.get("quality_fields") or []]
    required = {
        left_field,
        right_field,
        *(str(field) for field in (probability_field, statistic_field, epoch_field) if field),
        *quality_fields,
    }
    missing = sorted(required - available_fields)
    if missing:
        raise ValueError(f"relation fields missing from {table_name}: {missing}")

    left = text_expression(left_field)
    right = text_expression(right_field)
    probability = (
        f"try_cast({sql_identifier(str(probability_field))} as double)"
        if probability_field
        else "null::double"
    )
    probability_semantics = (
        sql_string(str(relation_claim["probability_semantics"]))
        if probability_field
        else "null::varchar"
    )
    statistic_raw = (
        text_expression(str(statistic_field))
        if statistic_field
        else "null::varchar"
    )
    statistic_value = (
        f"try_cast({sql_identifier(str(statistic_field))} as double)"
        if statistic_field
        else "null::double"
    )
    epoch = text_expression(str(epoch_field)) if epoch_field else "null::varchar"
    quality_members = [
        "'source_table'",
        sql_string(table_name),
        "'source_evidence_polarity'",
        sql_string(str(relation_claim["evidence_polarity"])),
    ]
    for field in quality_fields:
        quality_members.extend((sql_string(field), sql_identifier(field)))
    quality = "json_object(" + ", ".join(quality_members) + ")"
    evidence_namespace = (
        f"relation|{table_name}|{relation_claim['relation_kind']}|"
    )
    con.execute(
        f"""
        insert into relation_claim_evidence
        select distinct
          sha256({sql_string(evidence_namespace)} || r.source_record_id),
          r.source_record_id,
          {sql_string(str(relation_claim['left_identifier_namespace']))},
          {left},
          {sql_string(str(relation_claim['right_identifier_namespace']))},
          {right},
          {sql_string(str(relation_claim['relation_kind']))},
          {sql_string(str(relation_claim['relation_scope']))},
          {probability},
          {probability_semantics},
          {nullable_sql_string(relation_claim.get('confidence_statistic_key'))},
          {statistic_raw},
          {statistic_value},
          {nullable_sql_string(relation_claim.get('confidence_statistic_unit'))},
          {nullable_sql_string(relation_claim.get('confidence_statistic_semantics'))},
          {sql_string(str(relation_claim['evidence_polarity']))},
          {sql_string(str(relation_claim['method']))},
          {sql_string(str(relation_claim['reference_raw']))},
          {epoch},
          {quality}
        from read_parquet({sql_string(str(path))}) t
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(t))
        where nullif({left}, '') is not null
          and nullif({right}, '') is not null
        """
    )
    if probability_field:
        invalid = int(
            con.execute(
                """
                select count(*)
                from relation_claim_evidence e
                join source_records r using (source_record_id)
                where r.source_id=? and r.release_id=? and r.source_table=?
                  and (e.probability < 0 or e.probability > 1)
                """,
                [source_id, release_id, table_name],
            ).fetchone()[0]
        )
        if invalid:
            raise ValueError(
                f"strict relation probabilities outside [0, 1] in {table_name}: {invalid}"
            )
    return required


def materialize_orbital_solutions(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    fields: list[dict[str, Any]],
    orbital_solution: dict[str, Any],
    available_fields: set[str],
) -> set[str]:
    destination_fields = {str(field["column_name"]) for field in fields}
    key_fields = [str(field) for field in orbital_solution["solution_key_fields"]]
    parameter_fields = [str(field) for field in orbital_solution["parameter_fields"]]
    quality_fields = [str(field) for field in orbital_solution["quality_fields"]]
    optional_fields = [
        str(field)
        for field in (
            orbital_solution.get("epoch_field"),
            orbital_solution.get("frame_field"),
            orbital_solution.get("reference_field"),
        )
        if field
    ]
    consumed = set(key_fields + parameter_fields + quality_fields + optional_fields)
    missing = sorted(consumed - available_fields)
    if missing:
        raise ValueError(f"orbital solution fields missing from {table_name}: {missing}")
    unconsumed = sorted(destination_fields - consumed)
    if unconsumed:
        raise ValueError(
            f"orbital solution fields lack a typed role in {table_name}: {unconsumed}"
        )
    solution_key = f"cast({logical_key_expression(key_fields)} as varchar)"
    parameters = logical_key_expression(parameter_fields)
    quality = logical_key_expression(quality_fields)
    epoch = text_expression(orbital_solution.get("epoch_field"))
    frame = text_expression(orbital_solution.get("frame_field"))
    reference = text_expression(orbital_solution.get("reference_field"))
    model = nullable_sql_string(orbital_solution.get("model"))
    namespace = f"orbital-solution|{table_name}|"
    con.execute(
        f"""
        insert into orbital_solution_evidence
        select distinct
          sha256({sql_string(namespace)} || r.source_record_id),
          r.source_record_id,
          null,
          {solution_key},
          {parameters},
          {epoch},
          {frame},
          {sql_string(str(orbital_solution['method']))},
          {model},
          {reference},
          {quality},
          {sql_string(str(orbital_solution['normalization_version']))}
        from read_parquet({sql_string(str(path))}) t
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(t))
        """
    )
    return consumed


class ReferenceFragmentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.attributes: dict[str, str] = {}
        self.text_parts: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.lower() == "a" and not self.attributes:
            self.attributes = {key.lower(): value or "" for key, value in attrs}

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.text_parts.append(data.strip())


def parse_reference_fragment(raw: str) -> dict[str, Any]:
    parser = ReferenceFragmentParser()
    parser.feed(raw)
    parser.close()
    url = parser.attributes.get("href") or None
    reference_key = parser.attributes.get("refstr") or None
    display_text = " ".join(parser.text_parts).strip() or raw.strip()
    bibcode = None
    if url:
        match = re.search(r"/abs/([^/?#]+)", url)
        if match:
            bibcode = unquote(match.group(1))
    doi = None
    doi_match = re.search(r"10\.\d{4,9}/[^\s<>]+", unquote(url or raw), re.IGNORECASE)
    if doi_match:
        doi = doi_match.group(0).rstrip(".,;)")
    publication_year = None
    year_match = re.search(
        r"(?<!\d)((?:18|19|20)\d{2})(?!\d)", bibcode or display_text
    )
    if year_match:
        publication_year = int(year_match.group(1))
    return {
        "reference_key": reference_key,
        "display_text": display_text,
        "url": url,
        "bibcode": bibcode,
        "doi": doi,
        "publication_year": publication_year,
    }


def materialize_citations(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    reference_union = " union all ".join(
        f"""
        select r.source_id, e.reference_raw
        from {sql_identifier(table)} e
        join source_records r using (source_record_id)
        where nullif(trim(e.reference_raw), '') is not null
        """
        for table in sorted(EVIDENCE_REFERENCE_TABLES)
    )
    references = con.execute(
        f"select distinct source_id, reference_raw from ({reference_union}) "
        "order by source_id, reference_raw"
    ).fetchall()
    citation_rows = []
    for source_id, raw in references:
        parsed = parse_reference_fragment(str(raw))
        citation_id = hashlib.sha256(
            f"citation|{source_id}|{raw}".encode("utf-8")
        ).hexdigest()
        citation_rows.append(
            [
                citation_id,
                source_id,
                parsed["reference_key"] or str(raw),
                str(raw),
                parsed["url"],
                parsed["bibcode"],
                parsed["doi"],
                parsed["publication_year"],
                json.dumps(parsed, sort_keys=True),
            ]
        )
    if citation_rows:
        con.executemany(
            "insert into citations values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            citation_rows,
        )
    for table in sorted(EVIDENCE_REFERENCE_TABLES):
        con.execute(
            f"""
            insert into evidence_citations
            select
              {sql_string(table)}, e.evidence_id, c.citation_id, 'source_reference'
            from {sql_identifier(table)} e
            join source_records r using (source_record_id)
            join citations c
              on c.source_id=r.source_id
             and c.citation_text_raw=e.reference_raw
            where nullif(trim(e.reference_raw), '') is not null
            """
        )
    link_count = int(con.execute("select count(*) from evidence_citations").fetchone()[0])
    return {"citations": len(citation_rows), "evidence_links": link_count}


def materialize_lifecycle_claims(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    claims: list[dict[str, Any]],
    available_fields: set[str],
) -> set[str]:
    consumed: set[str] = set()
    for claim in claims:
        identifier_field = str(claim["identifier_field"])
        disposition_field = claim.get("disposition_field")
        effective_field = claim.get("effective_field")
        reference_field = claim.get("reference_field")
        context_fields = [str(field) for field in claim.get("context_fields") or []]
        required = {
            identifier_field,
            *(
                str(value)
                for value in (disposition_field, effective_field, reference_field)
                if value
            ),
            *context_fields,
        }
        missing = sorted(required - available_fields)
        if missing:
            raise ValueError(f"lifecycle fields missing from {table_name}: {missing}")
        consumed.update(required)
        disposition_source = (
            optional_field_expression(str(disposition_field))
            if disposition_field
            else sql_string(str(claim["implicit_disposition"]))
        )
        context_json = (
            logical_key_expression(context_fields) if context_fields else "'{}'::json"
        )
        normalized = normalized_disposition_expression("s.disposition_raw")
        polarity = lifecycle_polarity_expression("normalized_disposition")
        effective_expression = optional_field_expression(
            str(effective_field) if effective_field else None
        )
        reference_expression = optional_field_expression(
            str(reference_field) if reference_field else None
        )
        role = str(claim["claim_role"])
        con.execute(
            f"""
            insert into planet_lifecycle_evidence
            with source_claims as (
              select distinct
                sha256(to_json(t)) source_row_sha256,
                trim(cast({sql_identifier(identifier_field)} as varchar)) identifier_raw,
                {disposition_source} disposition_raw,
                {effective_expression} effective_raw,
                {reference_expression} reference_raw,
                {context_json} context_json
              from read_parquet({sql_string(str(path))}) t
            ), normalized as (
              select *, {normalized} normalized_disposition
              from source_claims s
              where nullif(identifier_raw, '') is not null
                and nullif(trim(cast(disposition_raw as varchar)), '') is not null
            )
            select
              sha256(
                {sql_string('lifecycle|' + role + '|')}
                || r.source_record_id || '|' || n.normalized_disposition
              ),
              r.source_record_id,
              n.identifier_raw,
              cast(n.disposition_raw as varchar),
              n.normalized_disposition,
              {polarity},
              n.effective_raw,
              null,
              n.reference_raw,
              json_object(
                'claim_role', {sql_string(role)},
                'source_context', n.context_json
              )
            from normalized n
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=n.source_row_sha256
            """
        )
    return consumed


def materialize_source(
    con: duckdb.DuckDBPyConnection,
    input_row: dict[str, Any],
    adapter: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    source_id = input_row["source_id"]
    release_id = input_row["release_id"]
    raw_manifest = input_row["raw_manifest"]
    typed_manifest = input_row["typed_manifest"]
    raw_artifacts = artifact_by_name(raw_manifest)
    typed_tables = typed_table_by_name(typed_manifest)
    configured_tables = set(adapter["tables"])
    configured_raw_artifacts = {
        str(table.get("raw_artifact_name") or table_name)
        for table_name, table in adapter["tables"].items()
    }
    if (
        set(typed_tables) != configured_tables
        or set(raw_artifacts) != configured_raw_artifacts
    ):
        raise ValueError(
            f"adapter table coverage mismatch for {source_id}: "
            f"typed_only={sorted(set(typed_tables) - configured_tables)} "
            f"configured_only={sorted(configured_tables - set(typed_tables))} "
            f"raw_only={sorted(set(raw_artifacts) - configured_raw_artifacts)} "
            f"raw_missing={sorted(configured_raw_artifacts - set(raw_artifacts))}"
        )

    con.execute(
        "insert into evidence_sources values (?, ?, ?, ?, ?, ?, ?)",
        [
            source_id,
            release_id,
            adapter["adapter_version"],
            raw_manifest["snapshot_id"],
            raw_manifest["content_sha256"],
            typed_manifest["typed_snapshot_id"],
            typed_manifest["content_sha256"],
        ],
    )
    report_tables: list[dict[str, Any]] = []
    disposition_rows: list[list[Any]] = []
    for table_name in sorted(configured_tables):
        table_contract = adapter["tables"][table_name]
        typed_table = typed_tables[table_name]
        raw_artifact_name = str(
            table_contract.get("raw_artifact_name") or table_name
        )
        artifact = raw_artifacts[raw_artifact_name]
        path = input_row["typed_path"] / typed_table["parquet_path"]
        if not path.exists():
            raise FileNotFoundError(path)
        columns = [
            str(row[0])
            for row in con.execute(
                f"describe select * from read_parquet({sql_string(str(path))})"
            ).fetchall()
        ]
        logical_fields = list(table_contract["logical_key_fields"])
        missing_keys = sorted(set(logical_fields) - set(columns))
        if missing_keys:
            raise ValueError(f"logical key fields missing from {table_name}: {missing_keys}")
        product = product_manifest(input_row, artifact)
        dispositions = source_field_metadata(typed_table, product)
        source_fields = [str(row["column_name"]) for row in dispositions]
        if columns != source_fields:
            raise ValueError(
                f"typed/product field order mismatch for {table_name}: "
                f"typed={len(columns)} product={len(source_fields)}"
            )
        rules = contract["field_profiles"][table_contract["field_profile"]]
        context_fields: list[str] = []
        classified_fields: list[tuple[dict[str, Any], dict[str, str], int]] = []
        for field in dispositions:
            rule, rule_index = classify_field(str(field["column_name"]), rules)
            classified_fields.append((field, rule, rule_index))
            if rule["destination"] == "source_records":
                context_fields.append(str(field["column_name"]))

        source_row_hash = "sha256(to_json(t))"
        key_json = logical_key_expression(logical_fields)
        context_json = (
            logical_key_expression(context_fields) if context_fields else "'{}'::json"
        )
        row_predicate = row_selection_predicate(table_contract)
        record_namespace = f"{source_id}|{release_id}|{table_name}|"
        retrieved_at = artifact.get("retrieved_at")
        con.execute(
            f"""
            insert into source_records
            with hashed as (
              select
                {source_row_hash} source_row_sha256,
                {key_json} logical_key_json,
                {context_json} source_context_json
              from read_parquet({sql_string(str(path))}) t
              where {row_predicate}
            )
            select
              sha256({sql_string(record_namespace)} || source_row_sha256),
              {sql_string(source_id)},
              {sql_string(release_id)},
              {sql_string(table_name)},
              {sql_string(str(table_contract['object_scope']))},
              any_value(logical_key_json)::json,
              any_value(source_context_json)::json,
              source_row_sha256,
              count(*)::bigint,
              {sql_string(str(raw_manifest['snapshot_id']))},
              {sql_string(str(typed_manifest['typed_snapshot_id']))},
              {sql_string(str(artifact['tree_sha256']))},
              {sql_string(str(typed_table['sha256']))},
              {sql_string(str(retrieved_at))}::timestamp
            from hashed
            group by source_row_sha256
            """
        )
        source_rows, records, duplicate_rows = con.execute(
            """
            select
              coalesce(sum(source_duplicate_count), 0)::bigint,
              count(*)::bigint,
              coalesce(sum(source_duplicate_count - 1), 0)::bigint
            from source_records
            where source_id=? and release_id=? and source_table=?
            """,
            [source_id, release_id, table_name],
        ).fetchone()
        typed_source_rows = int(typed_table["row_count"])
        if int(source_rows) > typed_source_rows:
            raise ValueError(f"source-record accounting overflow for {table_name}")
        if not table_contract.get("row_selection") and int(source_rows) != typed_source_rows:
            raise ValueError(f"source-record accounting mismatch for {table_name}")
        identifier_fields = [
            str(field["column_name"])
            for field, rule, _index in classified_fields
            if rule["destination"] == "identifier_claim_evidence"
        ]
        materialized_fields = set(context_fields)
        identifier_claims = dict(contract["identifier_claims"])
        identifier_claims.update(table_contract.get("identifier_claims") or {})
        materialized_fields.update(
            materialize_identifier_claims(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                fields=identifier_fields,
                claim_by_field=identifier_claims,
            )
        )
        materialized_fields.update(
            materialize_lifecycle_claims(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                claims=list(table_contract.get("lifecycle_claims") or []),
                available_fields=set(columns),
            )
        )
        fields_by_destination: dict[str, list[dict[str, Any]]] = {}
        for field, rule, _rule_index in classified_fields:
            if rule["disposition"] != "exclude":
                fields_by_destination.setdefault(rule["destination"], []).append(field)
        scoped_stellar_fields = configured_scoped_stellar_fields(table_contract)
        for destination in sorted(SCALAR_EVIDENCE_DESTINATIONS):
            destination_fields = [
                field
                for field in (fields_by_destination.get(destination) or [])
                if str(field["column_name"]) not in scoped_stellar_fields
                and not (
                    destination == "photometry_extinction_evidence"
                    and str(field["column_name"])
                    in {
                        str(row["value_field"])
                        for row in table_contract.get("photometry_measurements") or []
                    }
                )
            ]
            if destination_fields:
                materialized_fields.update(
                    materialize_scalar_evidence(
                        con,
                        source_id=source_id,
                        release_id=release_id,
                        table_name=table_name,
                        path=path,
                        destination=destination,
                        fields=destination_fields,
                        available_fields=set(columns),
                        table_contract=table_contract,
                        unit_normalizations=contract["unit_normalizations"],
                    )
                )
        classification_fields = fields_by_destination.get(
            "stellar_classification_evidence"
        ) or []
        classification_fields = [
            field
            for field in classification_fields
            if str(field["column_name"]) not in scoped_stellar_fields
        ]
        if classification_fields:
            materialized_fields.update(
                materialize_stellar_classifications(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    fields=classification_fields,
                    table_contract=table_contract,
                )
            )
        materialized_fields.update(
            materialize_scoped_stellar_evidence(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                parameter_sets=list(
                    table_contract.get("scoped_stellar_parameter_sets") or []
                ),
                available_fields=set(columns),
            )
        )
        materialized_fields.update(
            materialize_configured_photometry(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                measurements=list(table_contract.get("photometry_measurements") or []),
                available_fields=set(columns),
            )
        )
        observation_product_fields = fields_by_destination.get(
            "observation_product_lineage"
        ) or []
        if observation_product_fields:
            materialized_fields.update(
                materialize_observation_products(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    fields=observation_product_fields,
                )
            )
        relation_claim = table_contract.get("relation_claim")
        if relation_claim:
            materialized_fields.update(
                materialize_relation_claims(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    relation_claim=relation_claim,
                    available_fields=set(columns),
                )
            )
        orbital_solution_fields = fields_by_destination.get(
            "orbital_solution_evidence"
        ) or []
        orbital_solution = table_contract.get("orbital_solution")
        if orbital_solution_fields or orbital_solution:
            if not orbital_solution_fields or not orbital_solution:
                raise ValueError(
                    f"orbital solution profile/config mismatch for {table_name}"
                )
            materialized_fields.update(
                materialize_orbital_solutions(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    fields=orbital_solution_fields,
                    orbital_solution=orbital_solution,
                    available_fields=set(columns),
                )
            )
        materialize_parameter_sets(
            con,
            source_id=source_id,
            release_id=release_id,
            table_name=table_name,
            table_contract=table_contract,
        )
        for field, rule, rule_index in classified_fields:
            field_name = str(field["column_name"])
            mapping_status = (
                "excluded"
                if rule["disposition"] == "exclude"
                else "materialized"
                if field_name in materialized_fields and not rule.get("mapping_status")
                else str(rule.get("mapping_status") or "declared_pending")
            )
            disposition_rows.append(
                [
                    source_id,
                    release_id,
                    table_name,
                    field_name,
                    field.get("datatype"),
                    field.get("unit"),
                    field.get("ucd"),
                    field.get("description"),
                    rule["disposition"],
                    rule["destination"],
                    mapping_status,
                    f"rule[{rule_index}]: {rule['reason']}",
                    adapter["adapter_version"],
                ]
            )
        report_tables.append(
            {
                "source_table": table_name,
                "source_rows": int(source_rows),
                "input_source_rows": typed_source_rows,
                "excluded_by_row_selection": typed_source_rows - int(source_rows),
                "row_selection_policy": (
                    table_contract.get("row_selection") or {}
                ).get("policy_id"),
                "source_records": int(records),
                "exact_duplicate_rows": int(duplicate_rows),
                "source_fields": len(columns),
            }
        )

    con.executemany(
        "insert into source_field_dispositions values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        disposition_rows,
    )
    con.execute(
        """
        insert into object_binding_outcomes
        with binding_scopes as (
          select source_record_id, object_scope binding_scope, null::varchar component_scope
          from source_records
          where source_id=? and release_id=?
          union
          select i.source_record_id, i.claim_scope binding_scope, i.component_scope
          from identifier_claim_evidence i
          join source_records r using (source_record_id)
          where r.source_id=? and r.release_id=?
          union
          select e.source_record_id, 'stellar_component' binding_scope, e.component_scope
          from stellar_parameter_evidence e
          join source_records r using (source_record_id)
          where r.source_id=? and r.release_id=? and e.component_scope is not null
          union
          select e.source_record_id, 'stellar_component' binding_scope, e.component_scope
          from stellar_classification_evidence e
          join source_records r using (source_record_id)
          where r.source_id=? and r.release_id=? and e.component_scope is not null
        )
        select
          sha256('binding|unresolved|' || s.source_record_id || '|' || s.binding_scope
            || '|' || coalesce(s.component_scope, '')),
          s.source_record_id,
          'unresolved',
          s.binding_scope,
          null,
          null,
          null,
          null,
          s.component_scope,
          'awaiting release-scoped identity and component-scope binding',
          json_object(
            'source_id', r.source_id,
            'release_id', r.release_id,
            'source_table', r.source_table,
            'source_row_sha256', r.source_row_sha256
          )
        from binding_scopes s
        join source_records r using (source_record_id)
        """,
        [
            source_id,
            release_id,
            source_id,
            release_id,
            source_id,
            release_id,
            source_id,
            release_id,
        ],
    )
    return {
        "source_id": source_id,
        "release_id": release_id,
        "adapter_version": adapter["adapter_version"],
        "tables": report_tables,
        "source_rows": sum(row["source_rows"] for row in report_tables),
        "input_source_rows": sum(row["input_source_rows"] for row in report_tables),
        "excluded_by_row_selection": sum(
            row["excluded_by_row_selection"] for row in report_tables
        ),
        "source_records": sum(row["source_records"] for row in report_tables),
        "exact_duplicate_rows": sum(row["exact_duplicate_rows"] for row in report_tables),
        "source_fields": sum(row["source_fields"] for row in report_tables),
    }


def user_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [
        str(row[0])
        for row in con.execute(
            "select table_name from information_schema.tables "
            "where table_schema='main' and table_type='BASE TABLE' order by table_name"
        ).fetchall()
    ]


def table_logical_report(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> dict[str, Any]:
    quoted = sql_identifier(table_name)
    rows, digest = con.execute(
        f"""
        with row_hashes as (
          select sha256(to_json(t)) row_hash
          from {quoted} t
        ),
        bucket_hashes as (
          select
            substr(row_hash, 1, 2) bucket,
            count(*)::bigint row_count,
            sha256(string_agg(row_hash, '' order by row_hash)) bucket_hash
          from row_hashes
          group by bucket
        )
        select
          coalesce(sum(row_count), 0)::bigint,
          sha256(coalesce(
            string_agg(
              bucket || ':' || cast(row_count as varchar) || ':' || bucket_hash,
              '|' order by bucket
            ),
            ''
          ))
        from bucket_hashes
        """
    ).fetchone()
    return {
        "table": table_name,
        "row_count": int(rows),
        "logical_sha256": digest,
        "logical_hash_algorithm": LOGICAL_HASH_ALGORITHM,
    }


def compile_evidence(
    *,
    state_dir: Path,
    contract_path: Path,
    registry_path: Path,
    selected_source_ids: set[str],
    report_path: Path,
    artifact_root: Path | None = None,
) -> dict[str, Any]:
    contract = load_json(contract_path)
    errors = validate_contract(contract)
    if errors:
        raise ValueError("; ".join(errors))
    registry = load_json(registry_path)
    compiler_sha256 = file_hash(Path(__file__).resolve())
    registry_sha256 = file_hash(registry_path)
    runtime_versions = {
        "python": sys.version.split()[0],
        "duckdb": duckdb.__version__,
    }
    registry_sources = {
        str(source["source_id"]): source for source in registry["sources"]
    }
    adapter_ids = set(contract["source_adapters"])
    requested = selected_source_ids or adapter_ids
    unknown = requested - adapter_ids
    if unknown:
        raise ValueError(f"sources have no E4 adapter: {sorted(unknown)}")
    inputs = []
    for source_id in sorted(requested):
        if source_id not in registry_sources:
            raise ValueError(f"E4 adapter source is absent from registry: {source_id}")
        inputs.append(source_input(state_dir, registry_sources[source_id]))
    input_fingerprint = stable_hash(
        {
            "compiler_sha256": compiler_sha256,
            "contract_sha256": file_hash(contract_path),
            "registry_version": registry["registry_version"],
            "registry_sha256": registry_sha256,
            "runtime_versions": runtime_versions,
            "inputs": [
                {
                    "source_id": row["source_id"],
                    "release_id": row["release_id"],
                    "raw_content_sha256": row["raw_manifest"]["content_sha256"],
                    "typed_content_sha256": row["typed_manifest"]["content_sha256"],
                }
                for row in inputs
            ],
        }
    )
    build_id = input_fingerprint[:24]
    family_root = artifact_root or (
        state_dir
        / "derived"
        / "evidence_lake_v2"
        / contract["artifact_family"]
    )
    destination = family_root / build_id
    manifest_path = destination / "manifest.json"
    if manifest_path.exists():
        manifest = load_json(manifest_path)
        database_path = destination / str(manifest["database"])
        if not database_path.exists():
            raise ValueError(f"immutable evidence artifact lacks database: {destination}")
        if file_hash(database_path) != manifest.get("database_sha256"):
            raise ValueError(f"immutable evidence artifact checksum changed: {destination}")
        write_json(report_path, manifest["report"])
        return manifest["report"]
    if destination.exists():
        raise ValueError(f"immutable evidence artifact lacks manifest: {destination}")
    family_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=family_root))
    database_path = temporary / "scientific_evidence.duckdb"
    created_at = deterministic_build_timestamp(inputs)
    con = duckdb.connect(str(database_path))
    source_reports: list[dict[str, Any]] = []
    try:
        con.execute("set threads=1")
        con.execute("set preserve_insertion_order=true")
        create_schema(con)
        for input_row in inputs:
            source_reports.append(
                materialize_source(
                    con,
                    input_row,
                    contract["source_adapters"][input_row["source_id"]],
                    contract,
                )
            )
        citation_summary = materialize_citations(con)
        mapping_counts = {
            str(status): int(count)
            for status, count in con.execute(
                "select mapping_status, count(*) from source_field_dispositions "
                "group by mapping_status order by mapping_status"
            ).fetchall()
        }
        identifier_claim_counts = {
            str(namespace): int(count)
            for namespace, count in con.execute(
                "select namespace, count(*) from identifier_claim_evidence "
                "group by namespace order by namespace"
            ).fetchall()
        }
        identifier_claim_scope_counts = {
            str(scope): int(count)
            for scope, count in con.execute(
                "select claim_scope, count(*) from identifier_claim_evidence "
                "group by claim_scope order by claim_scope"
            ).fetchall()
        }
        binding_outcome_counts = {
            str(status): {
                str(scope): int(count)
                for scope, count in con.execute(
                    "select binding_scope, count(*) from object_binding_outcomes "
                    "where binding_status=? group by binding_scope order by binding_scope",
                    [status],
                ).fetchall()
            }
            for status in sorted(contract["binding_statuses"])
            if con.execute(
                "select count(*) from object_binding_outcomes where binding_status=?",
                [status],
            ).fetchone()[0]
        }
        lifecycle_claim_counts = {
            "by_disposition": {
                str(disposition): int(count)
                for disposition, count in con.execute(
                    "select disposition_normalized, count(*) "
                    "from planet_lifecycle_evidence "
                    "group by disposition_normalized order by disposition_normalized"
                ).fetchall()
            },
            "by_polarity": {
                str(polarity): int(count)
                for polarity, count in con.execute(
                    "select evidence_polarity, count(*) "
                    "from planet_lifecycle_evidence "
                    "group by evidence_polarity order by evidence_polarity"
                ).fetchall()
            },
        }
        relation_claim_counts = {
            "by_kind_and_polarity": {
                str(kind): {
                    str(polarity): int(count)
                    for polarity, count in con.execute(
                        "select evidence_polarity, count(*) "
                        "from relation_claim_evidence where relation_kind=? "
                        "group by evidence_polarity order by evidence_polarity",
                        [kind],
                    ).fetchall()
                }
                for (kind,) in con.execute(
                    "select distinct relation_kind from relation_claim_evidence "
                    "order by relation_kind"
                ).fetchall()
            },
            "with_strict_probability": int(
                con.execute(
                    "select count(*) from relation_claim_evidence "
                    "where probability is not null"
                ).fetchone()[0]
            ),
            "with_confidence_statistic": int(
                con.execute(
                    "select count(*) from relation_claim_evidence "
                    "where confidence_statistic_value is not null"
                ).fetchone()[0]
            ),
        }
        pending_fields = mapping_counts.get("declared_pending", 0)
        build_status = "pass" if not pending_fields else "in_progress"
        con.execute(
            "insert into evidence_build values (?, ?, ?, ?, ?, ?)",
            [
                build_id,
                contract["contract_version"],
                contract["compiler_version"],
                input_fingerprint,
                build_status,
                created_at,
            ],
        )
        con.execute("checkpoint")
        tables = [table_logical_report(con, table) for table in user_tables(con)]
    finally:
        con.close()
    logical_content_sha256 = stable_hash(tables)
    report = {
        "schema_version": "spacegate.scientific_evidence_report.v1",
        "build_id": build_id,
        "contract_version": contract["contract_version"],
        "compiler_version": contract["compiler_version"],
        "compiler_sha256": compiler_sha256,
        "registry_sha256": registry_sha256,
        "runtime_versions": runtime_versions,
        "input_fingerprint": input_fingerprint,
        "status": "pass" if not mapping_counts.get("declared_pending", 0) else "in_progress",
        "sources": source_reports,
        "mapping_status_counts": mapping_counts,
        "identifier_claim_counts_by_namespace": identifier_claim_counts,
        "identifier_claim_counts_by_scope": identifier_claim_scope_counts,
        "binding_outcome_counts_by_status_and_scope": binding_outcome_counts,
        "lifecycle_claim_counts": lifecycle_claim_counts,
        "relation_claim_counts": relation_claim_counts,
        "citation_summary": citation_summary,
        "logical_content_sha256": logical_content_sha256,
        "logical_hash_algorithm": LOGICAL_HASH_ALGORITHM,
        "tables": tables,
        "created_at": created_at,
    }
    manifest = {
        "schema_version": BUILD_CONTRACT,
        "build_id": build_id,
        "input_fingerprint": input_fingerprint,
        "contract_path": str(contract_path.relative_to(ROOT)),
        "contract_sha256": file_hash(contract_path),
        "registry_version": registry["registry_version"],
        "registry_sha256": registry_sha256,
        "compiler_sha256": compiler_sha256,
        "runtime_versions": runtime_versions,
        "database": "scientific_evidence.duckdb",
        "database_bytes": database_path.stat().st_size,
        "database_sha256": file_hash(database_path),
        "logical_content_sha256": logical_content_sha256,
        "report": report,
    }
    write_json(temporary / "manifest.json", manifest)
    os.replace(temporary, destination)
    write_json(report_path, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument(
        "--artifact-root",
        type=Path,
        help="Override the immutable artifact family root for clean reproduction",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_STATE
        / "reports"
        / "evidence_lake_v2"
        / "e4_scientific_evidence_report.json",
    )
    parser.add_argument("--validate-contract", action="store_true")
    args = parser.parse_args()
    contract = load_json(args.contract)
    errors = validate_contract(contract)
    if args.validate_contract:
        if errors:
            for error in errors:
                print(f"ERROR: {error}")
            return 1
        print(
            f"Scientific evidence contract OK: {len(contract['domain_tables'])} domains, "
            f"{len(contract['source_adapters'])} adapters"
        )
        return 0
    report = compile_evidence(
        state_dir=args.state_dir,
        contract_path=args.contract,
        registry_path=args.registry,
        selected_source_ids=set(args.source),
        report_path=args.report,
        artifact_root=args.artifact_root,
    )
    print(
        f"scientific evidence {report['build_id']} {report['status']}: "
        f"sources={len(report['sources'])} "
        f"pending_fields={report['mapping_status_counts'].get('declared_pending', 0):,}"
    )
    return 0 if report["status"] in {"pass", "in_progress"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
