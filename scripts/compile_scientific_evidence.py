#!/usr/bin/env python3
"""Compile source-native lake tables into immutable scientific evidence domains."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT = ROOT / "config" / "evidence_lake" / "e4_scientific_evidence.json"
DEFAULT_REGISTRY = ROOT / "config" / "evidence_lake" / "source_releases.json"
DEFAULT_STATE = Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state"))
BUILD_CONTRACT = "spacegate.scientific_evidence_build.v1"


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
    if contract.get("schema_version") != "spacegate.scientific_evidence_contract.v1":
        errors.append("unsupported scientific evidence contract")
    if set(contract.get("domain_tables") or []) != DOMAIN_TABLES:
        errors.append("domain_tables must exactly match the compiler contract")
    dispositions = set(contract.get("field_dispositions") or [])
    mapping_statuses = set(contract.get("mapping_statuses") or [])
    identifier_claims = contract.get("identifier_claims") or {}
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
          left_identity_raw varchar not null,
          right_identity_raw varchar not null,
          relation_kind varchar not null,
          relation_scope varchar not null,
          probability double,
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
    if len(candidates) != 1:
        raise ValueError(
            f"expected one product manifest for {artifact['source_name']}, found {len(candidates)}"
        )
    return load_json(candidates[0])


def logical_key_expression(fields: list[str]) -> str:
    members = ", ".join(
        f"{sql_identifier(field)} := {sql_identifier(field)}" for field in fields
    )
    return f"to_json(struct_pack({members}))"


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
              null,
              null,
              json_object('source_field', {sql_string(field)})
            from (
              select distinct
                sha256(to_json(t)) source_row_sha256,
                trim(cast({quoted} as varchar)) value_raw
              from read_parquet({sql_string(str(path))}) t
              where nullif(trim(cast({quoted} as varchar)), '') is not null
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
            *(str(value) for value in (disposition_field, effective_field, reference_field) if value),
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
        role = str(claim["claim_role"])
        con.execute(
            f"""
            insert into planet_lifecycle_evidence
            with source_claims as (
              select distinct
                sha256(to_json(t)) source_row_sha256,
                trim(cast({sql_identifier(identifier_field)} as varchar)) identifier_raw,
                {disposition_source} disposition_raw,
                {optional_field_expression(str(effective_field) if effective_field else None)} effective_raw,
                {optional_field_expression(str(reference_field) if reference_field else None)} reference_raw,
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
    if set(typed_tables) != configured_tables or set(raw_artifacts) != configured_tables:
        raise ValueError(
            f"adapter table coverage mismatch for {source_id}: "
            f"typed_only={sorted(set(typed_tables) - configured_tables)} "
            f"configured_only={sorted(configured_tables - set(typed_tables))}"
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
        artifact = raw_artifacts[table_name]
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
        dispositions = product.get("field_dispositions") or []
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
        if int(source_rows) != int(typed_table["row_count"]):
            raise ValueError(f"source-record accounting mismatch for {table_name}")
        identifier_fields = [
            str(field["column_name"])
            for field, rule, _index in classified_fields
            if rule["destination"] == "identifier_claim_evidence"
        ]
        materialized_fields = set(context_fields)
        materialized_fields.update(
            materialize_identifier_claims(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                fields=identifier_fields,
                claim_by_field=contract["identifier_claims"],
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
          select source_record_id, object_scope binding_scope
          from source_records
          where source_id=? and release_id=?
          union
          select i.source_record_id, i.claim_scope binding_scope
          from identifier_claim_evidence i
          join source_records r using (source_record_id)
          where r.source_id=? and r.release_id=?
        )
        select
          sha256('binding|unresolved|' || s.source_record_id || '|' || s.binding_scope),
          s.source_record_id,
          'unresolved',
          s.binding_scope,
          null,
          null,
          null,
          null,
          null,
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
        [source_id, release_id, source_id, release_id],
    )
    return {
        "source_id": source_id,
        "release_id": release_id,
        "adapter_version": adapter["adapter_version"],
        "tables": report_tables,
        "source_rows": sum(row["source_rows"] for row in report_tables),
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
        select
          count(*)::bigint,
          sha256(coalesce(string_agg(sha256(to_json(t)), '' order by to_json(t)), ''))
        from {quoted} t
        """
    ).fetchone()
    return {"table": table_name, "row_count": int(rows), "logical_sha256": digest}


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
            "contract_sha256": file_hash(contract_path),
            "registry_version": registry["registry_version"],
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
        "input_fingerprint": input_fingerprint,
        "status": "pass" if not mapping_counts.get("declared_pending", 0) else "in_progress",
        "sources": source_reports,
        "mapping_status_counts": mapping_counts,
        "identifier_claim_counts_by_namespace": identifier_claim_counts,
        "identifier_claim_counts_by_scope": identifier_claim_scope_counts,
        "binding_outcome_counts_by_status_and_scope": binding_outcome_counts,
        "lifecycle_claim_counts": lifecycle_claim_counts,
        "logical_content_sha256": logical_content_sha256,
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
