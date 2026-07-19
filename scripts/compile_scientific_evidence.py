#!/usr/bin/env python3
"""Compile source-native lake tables into immutable scientific evidence domains."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import resource
import shutil
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
LOGICAL_HASH_BATCH_SIZE = 65_536
LOGICAL_HASH_MEMORY_LIMIT = os.environ.get(
    "SPACEGATE_E4_HASH_MEMORY_LIMIT", "16GB"
)
MATERIALIZATION_MEMORY_LIMIT = os.environ.get(
    "SPACEGATE_E4_MEMORY_LIMIT", "16GB"
)
LOGICAL_HASH_THREADS = max(
    1,
    min(int(os.environ.get("SPACEGATE_E4_HASH_THREADS", "4")), os.cpu_count() or 1),
)
CITATION_LINK_BUCKET_COUNT = 32


DOMAIN_TABLES = {
    "identifier_claim_evidence",
    "stellar_parameter_sets",
    "stellar_parameter_evidence",
    "stellar_classification_evidence",
    "astrometry_distance_evidence",
    "astrometry_distance_evidence_bundles",
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


def create_duckdb_temporary_directory(
    artifact_temporary: Path, build_id: str
) -> tuple[Path, str]:
    configured_root = os.environ.get("SPACEGATE_E4_TEMP_DIRECTORY")
    if not configured_root:
        path = artifact_temporary / ".duckdb_tmp"
        path.mkdir()
        return path, "artifact_family_staging"
    root = Path(configured_root).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    path = Path(
        tempfile.mkdtemp(prefix=f"scientific-evidence-{build_id}.", dir=root)
    )
    return path, "external_operator_scratch"


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


def resolve_table_contract(
    adapter: dict[str, Any],
    table_name: str,
    stack: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Resolve a table contract that reuses another table's scientific mapping."""
    tables = adapter.get("tables") or {}
    if table_name not in tables:
        raise ValueError(f"unknown table contract reference: {table_name}")
    if table_name in stack:
        cycle = " -> ".join((*stack, table_name))
        raise ValueError(f"cyclic table contract reference: {cycle}")
    table = tables[table_name]
    reference = table.get("table_contract_ref")
    if not reference:
        return dict(table)
    reference_name = str(reference)
    inherited = resolve_table_contract(
        adapter,
        reference_name,
        (*stack, table_name),
    )
    overrides = {
        key: value for key, value in table.items() if key != "table_contract_ref"
    }
    return {**inherited, **overrides}


def validate_relation_claim_contract(
    relation_claim: dict[str, Any],
    *,
    prefix: str,
) -> list[str]:
    errors: list[str] = []
    required = {
        "left_identifier_namespace",
        "left_component_scope",
        "right_identifier_namespace",
        "right_component_scope",
        "relation_kind",
        "relation_scope",
        "method",
        "reference_raw",
    }
    missing = sorted(required - set(relation_claim))
    if missing:
        errors.append(f"{prefix} lacks {missing}")
    for side in ("left", "right"):
        field = relation_claim.get(f"{side}_identifier_field")
        fields = relation_claim.get(f"{side}_identifier_fields")
        if bool(field) == bool(fields):
            errors.append(
                f"{prefix}.{side}_identifier requires exactly one field or fields"
            )
        if fields is not None and (
            not isinstance(fields, list)
            or not fields
            or any(not str(value).strip() for value in fields)
        ):
            errors.append(
                f"{prefix}.{side}_identifier_fields must be a non-empty list"
            )
    if bool(relation_claim.get("evidence_polarity")) == bool(
        relation_claim.get("evidence_polarity_sql")
    ):
        errors.append(
            f"{prefix} requires exactly one static or dynamic evidence polarity"
        )
    probability_field = relation_claim.get("probability_field")
    probability_semantics = relation_claim.get("probability_semantics")
    if bool(probability_field) != bool(probability_semantics):
        errors.append(f"{prefix} must define probability field and semantics together")
    statistic_field = relation_claim.get("confidence_statistic_field")
    statistic_key = relation_claim.get("confidence_statistic_key")
    statistic_semantics = relation_claim.get("confidence_statistic_semantics")
    if any((statistic_field, statistic_key, statistic_semantics)) and not all(
        (statistic_field, statistic_key, statistic_semantics)
    ):
        errors.append(
            f"{prefix} confidence statistic requires field, key, and semantics"
        )
    if relation_claim.get("sql_predicate") is not None and not str(
        relation_claim.get("sql_predicate") or ""
    ).strip():
        errors.append(f"{prefix}.sql_predicate must be non-empty")
    return errors


def validate_contract(contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if contract.get("schema_version") != "spacegate.scientific_evidence_contract.v3":
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
        if adapter.get("table_order") is not None:
            table_order = [str(value) for value in adapter.get("table_order") or []]
            if len(table_order) != len(set(table_order)):
                errors.append(f"{source_id}.table_order contains duplicates")
            if set(table_order) != set(adapter.get("tables") or {}):
                errors.append(f"{source_id}.table_order must cover every table exactly")
        for table_name in adapter.get("tables") or {}:
            try:
                table = resolve_table_contract(adapter, table_name)
            except ValueError as error:
                errors.append(f"{source_id}.{table_name}: {error}")
                continue
            if not table.get("logical_key_fields"):
                errors.append(f"{source_id}.{table_name} lacks logical key fields")
            if table.get("field_profile") not in profiles:
                errors.append(f"{source_id}.{table_name} references an unknown field profile")
            if table.get("raw_artifact_name") is not None and not str(
                table.get("raw_artifact_name")
            ).strip():
                errors.append(f"{source_id}.{table_name} has an empty raw artifact name")
            unit_overrides = table.get("unit_overrides")
            if unit_overrides is not None and (
                not isinstance(unit_overrides, dict)
                or not unit_overrides
                or any(
                    not str(field).strip() or not str(unit).strip()
                    for field, unit in unit_overrides.items()
                )
            ):
                errors.append(
                    f"{source_id}.{table_name}.unit_overrides must be a non-empty "
                    "field-to-unit object"
                )
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
                if claim.get("normalization") not in {
                    None,
                    "trim_v1",
                    "strip_literal_prefix_v1",
                    "strip_trailing_hash_footnote_v1",
                    "unsigned_integer_decimal_v1",
                }:
                    errors.append(
                        f"{source_id}.{table_name}.{field} has an unsupported identifier normalization"
                    )
                normalization_prefix = claim.get("normalization_prefix")
                if claim.get("normalization") == "strip_literal_prefix_v1":
                    if not str(normalization_prefix or "").strip():
                        errors.append(
                            f"{source_id}.{table_name}.{field} requires a non-empty "
                            "normalization_prefix"
                        )
                elif normalization_prefix is not None:
                    errors.append(
                        f"{source_id}.{table_name}.{field} normalization_prefix is only "
                        "valid with strip_literal_prefix_v1"
                    )
            relation_claim = table.get("relation_claim")
            relation_claims = table.get("relation_claims")
            if relation_claim and relation_claims:
                errors.append(
                    f"{source_id}.{table_name} cannot define both relation_claim "
                    "and relation_claims"
                )
            relation_contracts = (
                list(relation_claims)
                if relation_claims
                else [relation_claim]
                if relation_claim
                else []
            )
            relation_evidence_keys: list[str] = []
            for index, relation_contract in enumerate(relation_contracts):
                if relation_claims:
                    evidence_key = str(
                        relation_contract.get("evidence_key") or ""
                    ).strip()
                    if not evidence_key:
                        errors.append(
                            f"{source_id}.{table_name}.relation_claims[{index}]."
                            "evidence_key must be non-empty"
                        )
                    relation_evidence_keys.append(evidence_key)
                    prefix = f"{source_id}.{table_name}.relation_claims[{index}]"
                else:
                    prefix = f"{source_id}.{table_name}.relation_claim"
                errors.extend(
                    validate_relation_claim_contract(
                        relation_contract,
                        prefix=prefix,
                    )
                )
            if len(relation_evidence_keys) != len(set(relation_evidence_keys)):
                errors.append(
                    f"{source_id}.{table_name}.relation_claims contains duplicate "
                    "evidence_key values"
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
                for index, membership in enumerate(
                    row_selection.get("cross_table_memberships") or []
                ):
                    membership_prefix = (
                        f"{prefix}.cross_table_memberships[{index}]"
                    )
                    required = {"local_field", "target_table", "target_field"}
                    missing = sorted(required - set(membership))
                    if missing:
                        errors.append(f"{membership_prefix} lacks {missing}")
                        continue
                    if any(
                        not str(membership.get(field) or "").strip()
                        for field in required
                    ):
                        errors.append(
                            f"{membership_prefix} values must be non-empty"
                        )
                    if membership.get("target_table") not in adapter["tables"]:
                        errors.append(
                            f"{membership_prefix}.target_table is not in adapter"
                        )
                    if membership.get("target_sql_predicate") is not None and not str(
                        membership.get("target_sql_predicate") or ""
                    ).strip():
                        errors.append(
                            f"{membership_prefix}.target_sql_predicate must be non-empty"
                        )
            cluster_context = table.get("cluster_context")
            if cluster_context:
                prefix = f"{source_id}.{table_name}.cluster_context"
                required = {
                    "cluster_identity_field",
                    "method",
                    "normalization_version",
                }
                missing = sorted(required - set(cluster_context))
                if missing:
                    errors.append(f"{prefix} lacks {missing}")
            cluster_membership = table.get("cluster_membership")
            cluster_memberships = table.get("cluster_memberships")
            if cluster_membership and cluster_memberships:
                errors.append(
                    f"{source_id}.{table_name} cannot define both "
                    "cluster_membership and cluster_memberships"
                )
            membership_contracts = (
                list(cluster_memberships)
                if cluster_memberships
                else [cluster_membership]
                if cluster_membership
                else []
            )
            evidence_keys: list[str] = []
            for index, membership in enumerate(membership_contracts):
                prefix = (
                    f"{source_id}.{table_name}.cluster_memberships[{index}]"
                    if cluster_memberships
                    else f"{source_id}.{table_name}.cluster_membership"
                )
                required = {
                    "cluster_identity_field",
                    "member_identity_field",
                    "method",
                }
                missing = sorted(required - set(membership))
                if missing:
                    errors.append(f"{prefix} lacks {missing}")
                if cluster_memberships:
                    evidence_key = str(membership.get("evidence_key") or "").strip()
                    if not evidence_key:
                        errors.append(f"{prefix}.evidence_key must be non-empty")
                    evidence_keys.append(evidence_key)
            if len(evidence_keys) != len(set(evidence_keys)):
                errors.append(
                    f"{source_id}.{table_name}.cluster_memberships "
                    "contains duplicate evidence_key values"
                )
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
                relation_link = orbital_solution.get("relation_link")
                if relation_link:
                    for field in ("source_table", "key_fields"):
                        if not relation_link.get(field):
                            errors.append(f"{prefix}.relation_link.{field} must be non-empty")
                    if not isinstance(relation_link.get("key_fields"), dict):
                        errors.append(f"{prefix}.relation_link.key_fields must be an object")
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
                for field in required - {"measurements"}:
                    value = parameter_set.get(field)
                    if value is None or value == "" or value == []:
                        errors.append(f"{prefix}.{field} must be non-empty")
                measurements = parameter_set.get("measurements")
                if not isinstance(measurements, list):
                    errors.append(f"{prefix}.measurements must be a list")
                    measurements = []
                if not measurements and not str(
                    parameter_set.get("classification_field") or ""
                ).strip():
                    errors.append(
                        f"{prefix} requires a classification field or at least one measurement"
                    )
                if not str(
                    parameter_set.get("scope_key")
                    or parameter_set.get("component_scope")
                    or ""
                ).strip():
                    errors.append(f"{prefix} requires scope_key or component_scope")
                dynamic_scope = parameter_set.get(
                    "component_scope_field"
                ) or parameter_set.get("component_scope_fields")
                if parameter_set.get("component_scope") and dynamic_scope:
                    errors.append(
                        f"{prefix} cannot define constant and dynamic component scope"
                    )
                if parameter_set.get("component_scope_fields") is not None and (
                    not isinstance(parameter_set["component_scope_fields"], list)
                    or not parameter_set["component_scope_fields"]
                    or any(
                        not str(value).strip()
                        for value in parameter_set["component_scope_fields"]
                    )
                ):
                    errors.append(
                        f"{prefix}.component_scope_fields must be a non-empty list"
                    )
                for measurement_index, measurement in enumerate(
                    parameter_set.get("measurements") or []
                ):
                    measurement_prefix = f"{prefix}.measurements[{measurement_index}]"
                    for field in ("value_field", "quantity_key"):
                        if not str(measurement.get(field) or "").strip():
                            errors.append(f"{measurement_prefix}.{field} must be non-empty")
                    if measurement.get("zero_is_missing") not in (None, False, True):
                        errors.append(
                            f"{measurement_prefix}.zero_is_missing must be boolean"
                        )
                    if measurement.get("normalize_numeric") not in (None, False, True):
                        errors.append(
                            f"{measurement_prefix}.normalize_numeric must be boolean"
                        )
                    if measurement.get("uncertainty_field") and (
                        measurement.get("uncertainty_lower_field")
                        or measurement.get("uncertainty_upper_field")
                    ):
                        errors.append(
                            f"{measurement_prefix} cannot combine symmetric and "
                            "asymmetric uncertainty fields"
                        )
                    for bound in (
                        "uncertainty_minimum_value",
                        "uncertainty_maximum_value",
                    ):
                        value = measurement.get(bound)
                        if value is not None and (
                            isinstance(value, bool) or not isinstance(value, (int, float))
                        ):
                            errors.append(f"{measurement_prefix}.{bound} must be numeric")
            for index, measurement in enumerate(
                table.get("photometry_measurements") or []
            ):
                prefix = f"{source_id}.{table_name}.photometry_measurements[{index}]"
                for field in ("value_field", "quantity_key"):
                    if not str(measurement.get(field) or "").strip():
                        errors.append(f"{prefix}.{field} must be non-empty")
                if measurement.get("bandpass") and measurement.get("bandpass_field"):
                    errors.append(
                        f"{prefix} cannot define both bandpass and bandpass_field"
                    )
                if measurement.get("zero_is_missing") not in (None, False, True):
                    errors.append(f"{prefix}.zero_is_missing must be boolean")
                if measurement.get("uncertainty_field") and (
                    measurement.get("uncertainty_lower_field")
                    or measurement.get("uncertainty_upper_field")
                ):
                    errors.append(
                        f"{prefix} cannot combine symmetric and asymmetric uncertainty fields"
                    )
                for bound in (
                    "uncertainty_minimum_value",
                    "uncertainty_maximum_value",
                ):
                    value = measurement.get(bound)
                    if value is not None and (
                        isinstance(value, bool) or not isinstance(value, (int, float))
                    ):
                        errors.append(f"{prefix}.{bound} must be numeric")
                minimum = measurement.get("minimum_value")
                maximum = measurement.get("maximum_value")
                if minimum is not None and (
                    isinstance(minimum, bool) or not isinstance(minimum, (int, float))
                ):
                    errors.append(f"{prefix}.minimum_value must be numeric")
                if maximum is not None and (
                    isinstance(maximum, bool) or not isinstance(maximum, (int, float))
                ):
                    errors.append(f"{prefix}.maximum_value must be numeric")
                if (
                    isinstance(minimum, (int, float))
                    and not isinstance(minimum, bool)
                    and isinstance(maximum, (int, float))
                    and not isinstance(maximum, bool)
                    and minimum > maximum
                ):
                    errors.append(f"{prefix}.minimum_value exceeds maximum_value")
            for index, measurement in enumerate(
                table.get("configured_domain_measurements") or []
            ):
                prefix = (
                    f"{source_id}.{table_name}.configured_domain_measurements[{index}]"
                )
                for field in ("destination", "value_field", "quantity_key"):
                    if not str(measurement.get(field) or "").strip():
                        errors.append(f"{prefix}.{field} must be non-empty")
                if measurement.get("destination") not in {
                    "astrometry_distance_evidence",
                    "variability_activity_rotation_evidence",
                }:
                    errors.append(f"{prefix}.destination is unsupported")
                if measurement.get("normalize_numeric") not in (None, False, True):
                    errors.append(f"{prefix}.normalize_numeric must be boolean")
                if measurement.get("zero_is_missing") not in (None, False, True):
                    errors.append(f"{prefix}.zero_is_missing must be boolean")
                if measurement.get("uncertainty_field") and (
                    measurement.get("uncertainty_lower_field")
                    or measurement.get("uncertainty_upper_field")
                ):
                    errors.append(
                        f"{prefix} cannot combine symmetric and asymmetric uncertainty fields"
                    )
                for bound in (
                    "uncertainty_minimum_value",
                    "uncertainty_maximum_value",
                ):
                    value = measurement.get(bound)
                    if value is not None and (
                        isinstance(value, bool) or not isinstance(value, (int, float))
                    ):
                        errors.append(f"{prefix}.{bound} must be numeric")
                if measurement.get("epoch_field") and measurement.get("epoch_raw"):
                    errors.append(f"{prefix} cannot define both epoch_field and epoch_raw")
                minimum = measurement.get("minimum_value")
                maximum = measurement.get("maximum_value")
                if minimum is not None and (
                    isinstance(minimum, bool) or not isinstance(minimum, (int, float))
                ):
                    errors.append(f"{prefix}.minimum_value must be numeric")
                if maximum is not None and (
                    isinstance(maximum, bool) or not isinstance(maximum, (int, float))
                ):
                    errors.append(f"{prefix}.maximum_value must be numeric")
                if (
                    isinstance(minimum, (int, float))
                    and not isinstance(minimum, bool)
                    and isinstance(maximum, (int, float))
                    and not isinstance(maximum, bool)
                    and minimum > maximum
                ):
                    errors.append(f"{prefix}.minimum_value exceeds maximum_value")
            for index, measurement in enumerate(
                table.get("configured_coordinate_measurements") or []
            ):
                prefix = (
                    f"{source_id}.{table_name}.configured_coordinate_measurements[{index}]"
                )
                for field in ("quantity_key", "coordinate_kind", "component_fields"):
                    value = measurement.get(field)
                    if value is None or value == "" or value == []:
                        errors.append(f"{prefix}.{field} must be non-empty")
                kind = measurement.get("coordinate_kind")
                components = measurement.get("component_fields") or []
                expected_components = {
                    "right_ascension_hms": 3,
                    "declination_dms": 4,
                }
                if kind not in expected_components:
                    errors.append(f"{prefix}.coordinate_kind is unsupported")
                elif len(components) != expected_components[kind]:
                    errors.append(
                        f"{prefix}.component_fields must contain "
                        f"{expected_components[kind]} fields"
                    )
                if measurement.get("epoch_field") and measurement.get("epoch_raw"):
                    errors.append(
                        f"{prefix} cannot define both epoch_field and epoch_raw"
                    )
            product_missing_values = table.get("observation_product_missing_values")
            if product_missing_values is not None:
                prefix = f"{source_id}.{table_name}.observation_product_missing_values"
                if not isinstance(product_missing_values, dict):
                    errors.append(f"{prefix} must be an object")
                else:
                    for field, values in product_missing_values.items():
                        if not str(field).strip() or not isinstance(values, list):
                            errors.append(f"{prefix} entries must map fields to lists")
                        elif any(not str(value).strip() for value in values):
                            errors.append(f"{prefix}.{field} contains an empty value")
            configured_storage = table.get("configured_domain_storage") or {}
            configured_destinations = {
                str(row.get("destination"))
                for row in table.get("configured_domain_measurements") or []
            }
            for destination, storage in configured_storage.items():
                prefix = f"{source_id}.{table_name}.configured_domain_storage"
                if destination not in configured_destinations:
                    errors.append(
                        f"{prefix}.{destination} has no configured measurements"
                    )
                if (destination, storage) != (
                    "astrometry_distance_evidence",
                    "typed_measurement_bundle_v1",
                ):
                    errors.append(
                        f"{prefix}.{destination} storage mode is unsupported"
                    )
            for index, claim in enumerate(
                table.get("composite_identifier_claims") or []
            ):
                prefix = f"{source_id}.{table_name}.composite_identifier_claims[{index}]"
                for field in ("fields", "namespace", "claim_scope"):
                    value = claim.get(field)
                    if value is None or value == "" or value == []:
                        errors.append(f"{prefix}.{field} must be non-empty")
                if claim.get("sql_predicate") is not None and not str(
                    claim.get("sql_predicate") or ""
                ).strip():
                    errors.append(f"{prefix}.sql_predicate must be non-empty")
            for index, claim in enumerate(
                table.get("conditional_identifier_claims") or []
            ):
                prefix = f"{source_id}.{table_name}.conditional_identifier_claims[{index}]"
                for field in ("value_field", "namespace", "claim_scope", "sql_predicate"):
                    if not str(claim.get(field) or "").strip():
                        errors.append(f"{prefix}.{field} must be non-empty")
                if claim.get("strip_prefix") is not None and not str(
                    claim.get("strip_prefix") or ""
                ):
                    errors.append(f"{prefix}.strip_prefix must be non-empty")
                if claim.get("normalization") not in {
                    None,
                    "trim_v1",
                    "unsigned_integer_decimal_v1",
                }:
                    errors.append(f"{prefix}.normalization is unsupported")
            citation_catalog = table.get("citation_catalog")
            if citation_catalog:
                prefix = f"{source_id}.{table_name}.citation_catalog"
                for field in ("reference_key_field", "citation_text_field"):
                    if not str(citation_catalog.get(field) or "").strip():
                        errors.append(f"{prefix}.{field} must be non-empty")
                if any(
                    not str(value).strip()
                    for value in citation_catalog.get("excluded_values") or []
                ):
                    errors.append(f"{prefix}.excluded_values contains an empty value")
                aggregate_lines = citation_catalog.get("aggregate_repeated_key_lines")
                if aggregate_lines not in (None, False, True):
                    errors.append(
                        f"{prefix}.aggregate_repeated_key_lines must be boolean"
                    )
                if aggregate_lines and not str(
                    citation_catalog.get("line_order_field") or ""
                ).strip():
                    errors.append(
                        f"{prefix}.line_order_field is required for line aggregation"
                    )
                if aggregate_lines and any(
                    citation_catalog.get(value)
                    for value in (
                        "citation_url_field",
                        "bibcode_field",
                        "doi_field",
                        "publication_year_field",
                        "context_fields",
                    )
                ):
                    errors.append(
                        f"{prefix} line aggregation does not support ancillary fields"
                    )
                separator = citation_catalog.get("line_separator")
                if separator is not None and not str(separator):
                    errors.append(f"{prefix}.line_separator must be non-empty")
            for index, link in enumerate(table.get("source_citation_links") or []):
                prefix = f"{source_id}.{table_name}.source_citation_links[{index}]"
                for field in (
                    "identifier_claim_field",
                    "reference_key_field",
                    "citation_role",
                ):
                    if not str(link.get(field) or "").strip():
                        errors.append(f"{prefix}.{field} must be non-empty")
                if any(
                    not str(value).strip()
                    for value in link.get("excluded_reference_values") or []
                ):
                    errors.append(
                        f"{prefix}.excluded_reference_values contains an empty value"
                    )
            extended_object = table.get("extended_object")
            if extended_object:
                prefix = f"{source_id}.{table_name}.extended_object"
                required = {
                    "extended_kind",
                    "identity_key_fields",
                    "geometry_fields",
                    "parameter_fields",
                    "quality_fields",
                    "method",
                    "normalization_version",
                }
                missing = sorted(required - set(extended_object))
                if missing:
                    errors.append(f"{prefix} lacks {missing}")
                for field in required:
                    value = extended_object.get(field)
                    if value is None or value == "" or value == []:
                        errors.append(f"{prefix}.{field} must be non-empty")
            compact_objects = list(table.get("compact_object_parameter_sets") or [])
            if table.get("compact_object") and compact_objects:
                errors.append(
                    f"{source_id}.{table_name} cannot define both compact-object contract forms"
                )
            if table.get("compact_object"):
                compact_objects = [table["compact_object"]]
            compact_kinds: list[str] = []
            for compact_index, compact_object in enumerate(compact_objects):
                prefix = (
                    f"{source_id}.{table_name}.compact_object_parameter_sets[{compact_index}]"
                    if table.get("compact_object_parameter_sets")
                    else f"{source_id}.{table_name}.compact_object"
                )
                required = {
                    "compact_kind",
                    "parameter_set_key_fields",
                    "parameter_fields",
                    "quality_fields",
                    "method",
                    "normalization_version",
                }
                missing = sorted(required - set(compact_object))
                if missing:
                    errors.append(f"{prefix} lacks {missing}")
                for field in required:
                    value = compact_object.get(field)
                    if value is None or value == "" or value == []:
                        errors.append(f"{prefix}.{field} must be non-empty")
                compact_kinds.append(str(compact_object.get("compact_kind") or ""))
                if compact_object.get("sql_predicate") is not None and not str(
                    compact_object.get("sql_predicate") or ""
                ).strip():
                    errors.append(f"{prefix}.sql_predicate must be non-empty")
            if len(compact_kinds) != len(set(compact_kinds)):
                errors.append(
                    f"{source_id}.{table_name} compact-object kinds must be unique"
                )
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
        create table identifier_normalization_rejections (
          rejection_id varchar primary key,
          source_record_id varchar not null,
          source_field varchar not null,
          requested_namespace varchar not null,
          identifier_raw varchar not null,
          normalization varchar not null,
          reason varchar not null
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
        create table astrometry_distance_evidence_bundles (
          bundle_id varchar primary key,
          source_record_id varchar not null,
          bundle_semantics varchar not null,
          measurements struct(
            evidence_id varchar,
            quantity_key varchar,
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
          )[] not null
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
          left_component_scope varchar not null,
          right_identity_namespace varchar not null,
          right_identity_raw varchar not null,
          right_component_scope varchar not null,
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
          parameter_set_raw json,
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
        selected_fields = product.get("selected_fields") or []
        typed_columns = typed_table.get("columns") or []
        if selected_fields and all(isinstance(row, dict) for row in selected_fields):
            disposition_by_source = {
                str(row["column_name"]): row for row in dispositions
            }
            selected_by_output = {
                str(row["output_name"]): row for row in selected_fields
            }
            typed_names = [
                str(row.get("column_name") or row.get("name"))
                for row in typed_columns
            ]
            if len(disposition_by_source) != len(dispositions):
                raise ValueError("source product has duplicate field dispositions")
            if len(selected_by_output) != len(selected_fields):
                raise ValueError("source product has duplicate selected output fields")
            if set(typed_names) != set(selected_by_output):
                raise ValueError(
                    "typed columns do not match selected product output fields: "
                    f"typed_only={sorted(set(typed_names) - set(selected_by_output))} "
                    f"selected_only={sorted(set(selected_by_output) - set(typed_names))}"
                )
            fields = []
            for typed_name in typed_names:
                selected = selected_by_output[typed_name]
                source_name = str(selected["source_name"])
                source = disposition_by_source.get(source_name)
                if source is None:
                    raise ValueError(
                        f"selected source field lacks disposition: {source_name}"
                    )
                field = dict(source)
                field["source_column_name"] = source_name
                field["column_name"] = typed_name
                fields.append(field)
            return fields
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


def logical_key_expression(
    fields: list[str],
    table_alias: str | None = None,
) -> str:
    def source(field: str) -> str:
        identifier = sql_identifier(field)
        return f"{sql_identifier(table_alias)}.{identifier}" if table_alias else identifier

    members = ", ".join(
        f"{sql_identifier(field)} := {source(field)}" for field in fields
    )
    return f"to_json(struct_pack({members}))"


def row_selection_predicate(
    table_contract: dict[str, Any],
    *,
    typed_tables: dict[str, dict[str, Any]] | None = None,
    typed_root: Path | None = None,
    available_fields: set[str] | None = None,
) -> str:
    selection = table_contract.get("row_selection") or {}
    predicates = [f"({str(selection.get('sql_predicate') or 'true')})"]
    memberships = list(selection.get("cross_table_memberships") or [])
    if memberships and (typed_tables is None or typed_root is None):
        raise ValueError("cross-table row selection requires typed table context")
    for membership in memberships:
        local_field = str(membership["local_field"])
        target_table = str(membership["target_table"])
        target_field = str(membership["target_field"])
        if available_fields is not None and local_field not in available_fields:
            raise ValueError(f"row-selection local field missing: {local_field}")
        assert typed_tables is not None and typed_root is not None
        target = typed_tables.get(target_table)
        if target is None:
            raise ValueError(f"row-selection target table missing: {target_table}")
        target_fields = {str(column["name"]) for column in target["columns"]}
        if target_field not in target_fields:
            raise ValueError(
                f"row-selection target field missing from {target_table}: {target_field}"
            )
        target_path = typed_root / str(target["parquet_path"])
        if not target_path.exists():
            raise FileNotFoundError(target_path)
        target_predicate = str(membership.get("target_sql_predicate") or "true")
        predicates.append(
            "exists (select 1 from read_parquet("
            + sql_string(str(target_path))
            + ") membership_target where ("
            + target_predicate
            + ") and "
            + f"trim(cast(membership_target.{sql_identifier(target_field)} as varchar)) = "
            + f"trim(cast(source_row.{sql_identifier(local_field)} as varchar)))"
        )
    return " and ".join(predicates)


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
    rejection_branches = []
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
        normalization = str(claim.get("normalization") or "trim_v1")
        normalization_prefix = claim.get("normalization_prefix")
        normalized = (
            "cast(try_cast(s.value_raw as ubigint) as varchar)"
            if normalization == "unsigned_integer_decimal_v1"
            else (
                f"case when starts_with(trim(s.value_raw), {sql_string(str(normalization_prefix))}) "
                f"then trim(substr(trim(s.value_raw), {len(str(normalization_prefix)) + 1})) end"
            )
            if normalization == "strip_literal_prefix_v1"
            else "regexp_replace(trim(s.value_raw), '\\s+#+\\s*$', '')"
            if normalization == "strip_trailing_hash_footnote_v1"
            else "trim(s.value_raw)"
        )
        source_rows = f"""
          from (
            select distinct
              sha256(to_json(source_row)) source_row_sha256,
              trim(cast({quoted} as varchar)) value_raw
            from read_parquet({sql_string(str(path))}) source_row
            where nullif(trim(cast({quoted} as varchar)), '') is not null
              {excluded_predicate}
          ) s
          join source_records r
            on r.source_id={sql_string(source_id)}
           and r.release_id={sql_string(release_id)}
           and r.source_table={sql_string(table_name)}
           and r.source_row_sha256=s.source_row_sha256
        """
        branches.append(
            f"""
            select
              sha256({sql_string(evidence_namespace)} || r.source_record_id || '|' || s.value_raw),
              r.source_record_id,
              {sql_string(namespace)},
              s.value_raw,
              {normalized},
              {sql_string(claim_scope)},
              {nullable_sql_string(component_scope)},
              null,
              json_object(
                'source_field', {sql_string(field)},
                'normalization', {sql_string(normalization)},
                'normalization_prefix', {nullable_sql_string(normalization_prefix)}
              )
            {source_rows}
            where nullif({normalized}, '') is not null
            """
        )
        rejection_branches.append(
            f"""
            select
              sha256({sql_string('identifier-normalization-rejection|' + field + '|')} || r.source_record_id || '|' || s.value_raw),
              r.source_record_id, {sql_string(field)}, {sql_string(namespace)},
              s.value_raw, {sql_string(normalization)},
              'normalization did not produce a usable identifier'
            {source_rows}
            where nullif({normalized}, '') is null
            """
        )
    if rejection_branches:
        con.execute(
            "insert into identifier_normalization_rejections "
            + " union all ".join(rejection_branches)
        )
    if branches:
        con.execute(
            "insert into identifier_claim_evidence " + " union all ".join(branches)
        )
    return set(fields)


def materialize_composite_identifier_claims(
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
    branches = []
    for index, claim in enumerate(claims):
        fields = [str(field) for field in claim["fields"]]
        missing = sorted(set(fields) - available_fields)
        if missing:
            raise ValueError(
                f"composite identifier fields missing from {table_name}: {missing}"
            )
        consumed.update(fields)
        delimiter = str(claim.get("delimiter") or "")
        parts = [sql_string(str(claim.get("prefix") or ""))]
        for field_index, field in enumerate(fields):
            if field_index:
                parts.append(sql_string(delimiter))
            parts.append(f"trim(cast({sql_identifier(field)} as varchar))")
        parts.append(sql_string(str(claim.get("suffix") or "")))
        value = " || ".join(parts)
        nonblank = " and ".join(
            f"nullif(trim(cast({sql_identifier(field)} as varchar)), '') is not null"
            for field in fields
        )
        namespace = str(claim["namespace"])
        predicate = str(claim.get("sql_predicate") or "true")
        branches.append(
            f"""
            select distinct
              sha256({sql_string('composite-identifier|' + namespace + '|' + str(index) + '|')} || r.source_record_id),
              r.source_record_id, {sql_string(namespace)}, {value}, {value},
              {sql_string(str(claim['claim_scope']))},
              {nullable_sql_string(claim.get('component_scope'))}, null,
              json_object(
                'source_fields', {sql_string(json.dumps(fields))},
                'construction', 'literal_prefix_delimiter_suffix_v1'
              )
            from read_parquet({sql_string(str(path))}) source_row
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(source_row))
            where ({predicate}) and {nonblank}
            """
        )
    if branches:
        con.execute(
            "insert into identifier_claim_evidence " + " union all ".join(branches)
        )
    return consumed


def materialize_conditional_identifier_claims(
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
    branches = []
    rejection_branches = []
    for index, claim in enumerate(claims):
        field = str(claim["value_field"])
        if field not in available_fields:
            raise ValueError(
                f"conditional identifier field missing from {table_name}: {field}"
            )
        consumed.add(field)
        raw = text_expression(field)
        normalized = raw
        strip_prefix = claim.get("strip_prefix")
        if strip_prefix:
            normalized = f"trim(substr({raw}, {len(str(strip_prefix)) + 1}))"
        normalization = str(claim.get("normalization") or "trim_v1")
        if normalization == "unsigned_integer_decimal_v1":
            normalized = f"cast(try_cast({normalized} as ubigint) as varchar)"
        namespace = str(claim["namespace"])
        branches.append(
            f"""
            select distinct
              sha256({sql_string('conditional-identifier|' + namespace + '|' + str(index) + '|')} || r.source_record_id),
              r.source_record_id, {sql_string(namespace)}, {raw}, {normalized},
              {sql_string(str(claim['claim_scope']))},
              {nullable_sql_string(claim.get('component_scope'))}, null,
              json_object(
                'source_field', {sql_string(field)},
                'predicate', {sql_string(str(claim['sql_predicate']))},
                'strip_prefix', {nullable_sql_string(strip_prefix)},
                'normalization', {sql_string(normalization)}
              )
            from read_parquet({sql_string(str(path))}) source_row
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(source_row))
            where ({str(claim['sql_predicate'])})
              and nullif({raw}, '') is not null
              and nullif({normalized}, '') is not null
            """
        )
        rejection_branches.append(
            f"""
            select distinct
              sha256({sql_string('conditional-identifier-normalization-rejection|' + namespace + '|' + str(index) + '|')} || r.source_record_id),
              r.source_record_id, {sql_string(field)}, {sql_string(namespace)},
              {raw}, {sql_string(normalization)},
              'normalization did not produce a usable identifier'
            from read_parquet({sql_string(str(path))}) source_row
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(source_row))
            where ({str(claim['sql_predicate'])})
              and nullif({raw}, '') is not null
              and nullif({normalized}, '') is null
            """
        )
    if rejection_branches:
        con.execute(
            "insert into identifier_normalization_rejections "
            + " union all ".join(rejection_branches)
        )
    if branches:
        con.execute(
            "insert into identifier_claim_evidence " + " union all ".join(branches)
        )
    return consumed


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


def qualified_sql_identifier(field: str, table_alias: str | None = None) -> str:
    identifier = sql_identifier(field)
    return f"{sql_identifier(table_alias)}.{identifier}" if table_alias else identifier


def text_expression(
    field: str | None,
    *,
    table_alias: str | None = None,
) -> str:
    return (
        f"trim(cast({qualified_sql_identifier(field, table_alias)} as varchar))"
        if field
        else "null::varchar"
    )


def configured_epoch_expression(measurement: dict[str, Any]) -> str:
    epoch_field = measurement.get("epoch_field")
    if epoch_field:
        return text_expression(str(epoch_field))
    return nullable_sql_string(measurement.get("epoch_raw"))


def configured_text_expression(
    config: dict[str, Any],
    *,
    key: str,
    available_fields: set[str],
    table_alias: str | None = None,
) -> tuple[str, set[str]]:
    """Build a trusted contract-defined scalar or composite text key."""
    scalar = config.get(f"{key}_field")
    composite = config.get(f"{key}_fields")
    if scalar and composite:
        raise ValueError(f"{key} cannot declare both a field and fields")
    fields = (
        [str(value) for value in composite]
        if composite
        else [str(scalar)]
        if scalar
        else []
    )
    if not fields:
        raise ValueError(f"{key} requires a field or non-empty fields")
    missing = sorted(set(fields) - available_fields)
    if missing:
        raise ValueError(f"{key} fields missing: {missing}")
    delimiter = str(config.get(f"{key}_delimiter") or "")
    parts = [sql_string(str(config.get(f"{key}_prefix") or ""))]
    for index, field in enumerate(fields):
        if index:
            parts.append(sql_string(delimiter))
        parts.append(text_expression(field, table_alias=table_alias))
    parts.append(sql_string(str(config.get(f"{key}_suffix") or "")))
    return " || ".join(parts), set(fields)


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
    return logical_key_expression(fields, "source_row") if fields else "null::varchar"


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
    raw_unit = (table_contract.get("unit_overrides") or {}).get(
        base, metadata.get("unit")
    )
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
      from read_parquet({sql_string(str(path))}) source_row
      join source_records r
        on r.source_id={sql_string(source_id)}
       and r.release_id={sql_string(release_id)}
       and r.source_table={sql_string(table_name)}
       and r.source_row_sha256=sha256(to_json(source_row))
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
            from read_parquet({sql_string(str(path))}) source_row
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(source_row))
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
        if parameter_set.get("component_scope_field"):
            fields.add(str(parameter_set["component_scope_field"]))
        fields.update(
            str(field) for field in parameter_set.get("component_scope_fields") or []
        )
        if parameter_set.get("classification_field"):
            fields.add(str(parameter_set["classification_field"]))
        if parameter_set.get("reference_field"):
            fields.add(str(parameter_set["reference_field"]))
        fields.update(str(field) for field in parameter_set.get("quality_fields") or [])
        for measurement in parameter_set.get("measurements") or []:
            fields.add(str(measurement["value_field"]))
            fields.update(
                str(value)
                for value in (
                    measurement.get("uncertainty_field"),
                    measurement.get("uncertainty_lower_field"),
                    measurement.get("uncertainty_upper_field"),
                )
                if value
            )
    return fields


def missing_value_predicate(
    field: str,
    missing_values: list[Any],
    *,
    zero_is_missing: bool = False,
) -> str:
    raw = f"trim(cast({sql_identifier(field)} as varchar))"
    clauses = [f"nullif({raw}, '') is not null"]
    if missing_values:
        values = ", ".join(sql_string(str(value).strip().lower()) for value in missing_values)
        clauses.append(f"lower({raw}) not in ({values})")
    if zero_is_missing:
        clauses.append(f"coalesce(try_cast({sql_identifier(field)} as double), 1)<>0")
    return " and ".join(clauses)


def configured_measurement_predicate(
    field: str,
    measurement: dict[str, Any],
) -> str:
    clauses = [
        missing_value_predicate(
            field,
            list(measurement.get("missing_values") or []),
            zero_is_missing=bool(measurement.get("zero_is_missing")),
        )
    ]
    value = f"try_cast({sql_identifier(field)} as double)"
    if measurement.get("normalize_numeric", True):
        clauses.append(f"isfinite({value})")
    if measurement.get("minimum_value") is not None:
        clauses.append(f"{value}>={float(measurement['minimum_value'])}")
    if measurement.get("maximum_value") is not None:
        clauses.append(f"{value}<={float(measurement['maximum_value'])}")
    return " and ".join(clauses)


def nullable_measurement_double_expression(
    field: str | None,
    missing_values: list[Any],
    *,
    absolute: bool = False,
    minimum_value: float | None = None,
    maximum_value: float | None = None,
) -> str:
    if not field:
        return "null::double"
    source_value = f"try_cast({sql_identifier(field)} as double)"
    clauses = [missing_value_predicate(field, missing_values), f"isfinite({source_value})"]
    if minimum_value is not None:
        clauses.append(f"{source_value}>={float(minimum_value)}")
    if maximum_value is not None:
        clauses.append(f"{source_value}<={float(maximum_value)}")
    value = source_value
    if absolute:
        value = f"abs({value})"
    return f"case when {' and '.join(clauses)} then {value} end"


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
        dynamic_scope = config.get("component_scope_field") or config.get(
            "component_scope_fields"
        )
        if component_scope and dynamic_scope:
            raise ValueError(
                "scoped stellar evidence cannot declare both constant and dynamic component scope"
            )
        if dynamic_scope:
            component_scope_sql, component_scope_fields = configured_text_expression(
                config,
                key="component_scope",
                available_fields=available_fields,
            )
        else:
            component_scope_sql = nullable_sql_string(component_scope)
            component_scope_fields = set()
        scope = str(config.get("scope_key") or component_scope or "")
        if not scope:
            raise ValueError("dynamic component scope requires a stable scope_key")
        kind = str(config["parameter_set_kind"])
        method = str(config["method"])
        normalization = str(config["normalization_version"])
        reference_field = config.get("reference_field")
        quality_fields = [str(field) for field in config.get("quality_fields") or []]
        configured_fields = {
            str(value)
            for value in (config.get("classification_field"), reference_field)
            if value
        }
        configured_fields.update(component_scope_fields)
        configured_fields.update(quality_fields)
        for measurement in config.get("measurements") or []:
            configured_fields.add(str(measurement["value_field"]))
            configured_fields.update(
                str(value)
                for value in (
                    measurement.get("uncertainty_field"),
                    measurement.get("uncertainty_lower_field"),
                    measurement.get("uncertainty_upper_field"),
                )
                if value
            )
        missing = sorted(configured_fields - available_fields)
        if missing:
            raise ValueError(
                f"scoped stellar fields missing from {table_name}.{scope}: {missing}"
            )
        consumed.update(configured_fields)
        reference = text_expression(reference_field)
        source_quality = (
            logical_key_expression(quality_fields, "source_row")
            if quality_fields
            else "'{}'::json"
        )
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
                    'missing_values', {sql_string(json.dumps(config.get('classification_missing_values') or []))},
                    'source_quality', {source_quality}
                  )
                from read_parquet({sql_string(str(path))}) source_row
                join source_records r
                  on r.source_id={sql_string(source_id)}
                 and r.release_id={sql_string(release_id)}
                 and r.source_table={sql_string(table_name)}
                 and r.source_row_sha256=sha256(to_json(source_row))
                where {predicate}
                """
            )
        branches = []
        for measurement in config.get("measurements") or []:
            field = str(measurement["value_field"])
            uncertainty_field = measurement.get("uncertainty_field")
            uncertainty_lower_field = measurement.get(
                "uncertainty_lower_field", uncertainty_field
            )
            uncertainty_upper_field = measurement.get(
                "uncertainty_upper_field", uncertainty_field
            )
            predicate = configured_measurement_predicate(field, measurement)
            raw = text_expression(field)
            uncertainty_lower = nullable_measurement_double_expression(
                str(uncertainty_lower_field) if uncertainty_lower_field else None,
                list(
                    measurement.get(
                        "uncertainty_missing_values",
                        measurement.get("missing_values") or [],
                    )
                ),
                absolute=True,
                minimum_value=measurement.get("uncertainty_minimum_value"),
                maximum_value=measurement.get("uncertainty_maximum_value"),
            )
            uncertainty_upper = nullable_measurement_double_expression(
                str(uncertainty_upper_field) if uncertainty_upper_field else None,
                list(
                    measurement.get(
                        "uncertainty_missing_values",
                        measurement.get("missing_values") or [],
                    )
                ),
                absolute=True,
                minimum_value=measurement.get("uncertainty_minimum_value"),
                maximum_value=measurement.get("uncertainty_maximum_value"),
            )
            unit = nullable_sql_string(measurement.get("unit_raw"))
            normalized_unit = nullable_sql_string(
                measurement.get("normalized_unit", measurement.get("unit_raw"))
            )
            normalized_value = (
                f"try_cast({sql_identifier(field)} as double)"
                if measurement.get("normalize_numeric", True)
                else "null::double"
            )
            branches.append(
                f"""
                select distinct
                  sha256({sql_string('scoped-stellar|' + scope + '|' + field + '|')} || r.source_record_id),
                  {parameter_set_id}, r.source_record_id, {component_scope_sql},
                  {sql_string(str(measurement['quantity_key']))}, {raw}, {unit},
                  {normalized_value}, {normalized_unit},
                  {uncertainty_lower}, {uncertainty_upper}, 'measurement',
                  {sql_string(method)}, {nullable_sql_string(config.get('model'))},
                  {reference},
                  json_object(
                    'source_field', {sql_string(field)},
                    'uncertainty_field', {nullable_sql_string(uncertainty_field)},
                    'uncertainty_lower_field', {nullable_sql_string(uncertainty_lower_field)},
                    'uncertainty_upper_field', {nullable_sql_string(uncertainty_upper_field)},
                    'evidence_scope', {sql_string(scope)},
                    'component_scope', {component_scope_sql},
                    'parameter_set_kind', {sql_string(kind)},
                    'missing_values', {sql_string(json.dumps(measurement.get('missing_values') or []))},
                    'source_quality', {source_quality}
                  ),
                  {sql_string(normalization)}
                from read_parquet({sql_string(str(path))}) source_row
                join source_records r
                  on r.source_id={sql_string(source_id)}
                 and r.release_id={sql_string(release_id)}
                 and r.source_table={sql_string(table_name)}
                 and r.source_row_sha256=sha256(to_json(source_row))
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
                    'component_scope', e.component_scope,
                    'quality_fields', {sql_string(json.dumps(quality_fields))}
                  )
                from stellar_parameter_evidence e
                join source_records r using (source_record_id)
                where r.source_id={sql_string(source_id)}
                  and r.release_id={sql_string(release_id)}
                  and r.source_table={sql_string(table_name)}
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
        uncertainty_lower_field = measurement.get(
            "uncertainty_lower_field", uncertainty_field
        )
        uncertainty_upper_field = measurement.get(
            "uncertainty_upper_field", uncertainty_field
        )
        bandpass_field = measurement.get("bandpass_field")
        reference_field = measurement.get("reference_field")
        quality_fields = [str(value) for value in measurement.get("quality_fields") or []]
        fields = {
            field,
            *(
                str(value)
                for value in (
                    uncertainty_field,
                    uncertainty_lower_field,
                    uncertainty_upper_field,
                    bandpass_field,
                    reference_field,
                )
                if value
            ),
            *quality_fields,
        }
        missing = sorted(fields - available_fields)
        if missing:
            raise ValueError(f"photometry fields missing from {table_name}: {missing}")
        consumed.update(fields)
        raw = text_expression(field)
        uncertainty_lower = nullable_measurement_double_expression(
            str(uncertainty_lower_field) if uncertainty_lower_field else None,
            list(
                measurement.get(
                    "uncertainty_missing_values",
                    measurement.get("missing_values") or [],
                )
            ),
            absolute=True,
            minimum_value=measurement.get("uncertainty_minimum_value"),
            maximum_value=measurement.get("uncertainty_maximum_value"),
        )
        uncertainty_upper = nullable_measurement_double_expression(
            str(uncertainty_upper_field) if uncertainty_upper_field else None,
            list(
                measurement.get(
                    "uncertainty_missing_values",
                    measurement.get("missing_values") or [],
                )
            ),
            absolute=True,
            minimum_value=measurement.get("uncertainty_minimum_value"),
            maximum_value=measurement.get("uncertainty_maximum_value"),
        )
        unit = nullable_sql_string(measurement.get("unit_raw"))
        normalized_unit = nullable_sql_string(
            measurement.get("normalized_unit", measurement.get("unit_raw"))
        )
        bandpass = (
            text_expression(str(bandpass_field))
            if bandpass_field
            else nullable_sql_string(measurement.get("bandpass"))
        )
        quality_members = [
            "'source_field'",
            sql_string(field),
            "'uncertainty_field'",
            nullable_sql_string(uncertainty_field),
            "'uncertainty_lower_field'",
            nullable_sql_string(uncertainty_lower_field),
            "'uncertainty_upper_field'",
            nullable_sql_string(uncertainty_upper_field),
            "'bandpass_field'",
            nullable_sql_string(bandpass_field),
            "'missing_values'",
            sql_string(json.dumps(measurement.get("missing_values") or [])),
        ]
        for quality_field in quality_fields:
            quality_members.extend(
                (sql_string(quality_field), sql_identifier(quality_field))
            )
        quality = "json_object(" + ", ".join(quality_members) + ")"
        predicate = configured_measurement_predicate(field, measurement)
        branches.append(
            f"""
            select distinct
              sha256({sql_string('configured-photometry|' + field + '|')} || r.source_record_id),
              r.source_record_id, {sql_string(str(measurement['quantity_key']))},
              {bandpass}, {raw}, {unit},
              try_cast({sql_identifier(field)} as double), {normalized_unit},
              {uncertainty_lower}, {uncertainty_upper}, 'measurement',
              {nullable_sql_string(measurement.get('method'))},
              {nullable_sql_string(measurement.get('model'))},
              {text_expression(reference_field)},
              {quality},
              {sql_string(str(measurement.get('normalization_version') or 'source_native_v1'))}
            from read_parquet({sql_string(str(path))}) source_row
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(source_row))
            where {predicate}
            """
        )
    if branches:
        con.execute(
            "insert into photometry_extinction_evidence " + " union all ".join(branches)
        )
    return consumed


def configured_photometry_fields(
    measurements: list[dict[str, Any]],
) -> set[str]:
    fields: set[str] = set()
    for measurement in measurements:
        fields.add(str(measurement["value_field"]))
        fields.update(
            str(value)
            for value in (
                measurement.get("uncertainty_field"),
                measurement.get("uncertainty_lower_field"),
                measurement.get("uncertainty_upper_field"),
                measurement.get("bandpass_field"),
                measurement.get("reference_field"),
            )
            if value
        )
        fields.update(str(value) for value in measurement.get("quality_fields") or [])
    return fields


def configured_domain_measurement_fields(
    measurements: list[dict[str, Any]],
) -> set[str]:
    fields: set[str] = set()
    for measurement in measurements:
        fields.add(str(measurement["value_field"]))
        fields.update(
            str(value)
            for value in (
                measurement.get("uncertainty_field"),
                measurement.get("uncertainty_lower_field"),
                measurement.get("uncertainty_upper_field"),
                measurement.get("epoch_field"),
                measurement.get("reference_field"),
            )
            if value
        )
        fields.update(str(value) for value in measurement.get("quality_fields") or [])
    return fields


def configured_coordinate_measurement_fields(
    measurements: list[dict[str, Any]],
) -> set[str]:
    fields: set[str] = set()
    for measurement in measurements:
        fields.update(str(value) for value in measurement["component_fields"])
        fields.update(str(value) for value in measurement.get("quality_fields") or [])
        fields.update(
            str(value)
            for value in (
                measurement.get("epoch_field"),
                measurement.get("reference_field"),
            )
            if value
        )
    return fields


def materialize_configured_coordinate_measurements(
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
    branches: list[str] = []
    for index, measurement in enumerate(measurements):
        kind = str(measurement["coordinate_kind"])
        component_fields = [
            str(value) for value in measurement["component_fields"]
        ]
        quality_fields = [
            str(value) for value in measurement.get("quality_fields") or []
        ]
        epoch_field = measurement.get("epoch_field")
        reference_field = measurement.get("reference_field")
        configured_fields = {
            *component_fields,
            *quality_fields,
            *(
                str(value)
                for value in (epoch_field, reference_field)
                if value
            ),
        }
        missing = sorted(configured_fields - available_fields)
        if missing:
            raise ValueError(
                f"configured coordinate fields missing from {table_name}: {missing}"
            )
        consumed.update(configured_fields)
        component_predicate = " and ".join(
            f"nullif(trim(cast({sql_identifier(field)} as varchar)), '') is not null"
            for field in component_fields
        )
        raw_value = f"cast({logical_key_expression(component_fields, 'source_row')} as varchar)"
        if kind == "right_ascension_hms":
            hours, minutes, seconds = map(sql_identifier, component_fields)
            valid = (
                f"try_cast({hours} as double) >= 0 and "
                f"try_cast({hours} as double) < 24 and "
                f"try_cast({minutes} as double) between 0 and 59 and "
                f"try_cast({seconds} as double) >= 0 and "
                f"try_cast({seconds} as double) < 60"
            )
            normalized = (
                f"try_cast({hours} as double) * 15.0 + "
                f"try_cast({minutes} as double) / 4.0 + "
                f"try_cast({seconds} as double) / 240.0"
            )
        elif kind == "declination_dms":
            sign, degrees, minutes, seconds = map(sql_identifier, component_fields)
            valid = (
                f"trim(cast({sign} as varchar)) in ('+', '-') and "
                f"abs(try_cast({degrees} as double)) between 0 and 90 and "
                f"try_cast({minutes} as double) between 0 and 59 and "
                f"try_cast({seconds} as double) >= 0 and "
                f"try_cast({seconds} as double) < 60"
            )
            unsigned = (
                f"abs(try_cast({degrees} as double)) + "
                f"try_cast({minutes} as double) / 60.0 + "
                f"try_cast({seconds} as double) / 3600.0"
            )
            normalized = (
                f"case when trim(cast({degrees} as varchar)) like '-%' then -1.0 "
                f"when trim(cast({degrees} as varchar)) like '+%' then 1.0 "
                f"when trim(cast({sign} as varchar))='-' "
                f"then -1.0 else 1.0 end * ({unsigned})"
            )
        else:
            raise ValueError(f"unsupported coordinate kind: {kind}")
        source_quality = (
            logical_key_expression(quality_fields, "source_row")
            if quality_fields
            else "'{}'::json"
        )
        epoch = (
            text_expression(str(epoch_field))
            if epoch_field
            else nullable_sql_string(measurement.get("epoch_raw"))
        )
        evidence_id = (
            f"sha256({sql_string('configured-coordinate|' + kind + '|' + str(index) + '|')} "
            "|| r.source_record_id)"
        )
        branches.append(
            f"""
            select distinct
              {evidence_id}, r.source_record_id,
              {sql_string(str(measurement['quantity_key']))}, {raw_value},
              {nullable_sql_string(measurement.get('unit_raw'))},
              case when {valid} then {normalized} end,
              {nullable_sql_string(measurement.get('normalized_unit') or 'deg')},
              null::double, null::double,
              case when {valid} then 'measurement' else 'invalid_source_coordinate' end,
              {nullable_sql_string(measurement.get('frame_raw'))}, {epoch},
              {nullable_sql_string(measurement.get('method'))},
              {nullable_sql_string(measurement.get('model'))},
              {text_expression(str(reference_field) if reference_field else None)},
              json_object(
                'coordinate_kind', {sql_string(kind)},
                'component_fields', {sql_string(json.dumps(component_fields))},
                'source_quality', {source_quality},
                'embedded_degree_sign', case
                  when {sql_string(kind)}='declination_dms'
                  then regexp_full_match(trim(cast({sql_identifier(component_fields[1] if kind == 'declination_dms' else component_fields[0])} as varchar)), '[+-].*')
                  else false
                end,
                'normalization_valid', ({valid})
              ),
              {sql_string(str(measurement.get('normalization_version') or 'sexagesimal_degrees_v1'))}
            from read_parquet({sql_string(str(path))}) source_row
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(source_row))
            where {component_predicate}
            """
        )
    if branches:
        con.execute(
            "insert into astrometry_distance_evidence " + " union all ".join(branches)
        )
    return consumed


def materialize_configured_domain_measurements(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    measurements: list[dict[str, Any]],
    available_fields: set[str],
    storage_modes: dict[str, str] | None = None,
) -> set[str]:
    consumed: set[str] = set()
    branches_by_destination: dict[str, list[str]] = {}
    bundle_structs_by_destination: dict[str, list[str]] = {}
    storage_modes = storage_modes or {}
    for measurement in measurements:
        destination = str(measurement["destination"])
        field = str(measurement["value_field"])
        uncertainty_field = measurement.get("uncertainty_field")
        uncertainty_lower_field = measurement.get(
            "uncertainty_lower_field", uncertainty_field
        )
        uncertainty_upper_field = measurement.get(
            "uncertainty_upper_field", uncertainty_field
        )
        epoch_field = measurement.get("epoch_field")
        reference_field = measurement.get("reference_field")
        quality_fields = [str(value) for value in measurement.get("quality_fields") or []]
        configured_fields = {
            field,
            *(
                str(value)
                for value in (
                    uncertainty_field,
                    uncertainty_lower_field,
                    uncertainty_upper_field,
                    epoch_field,
                    reference_field,
                )
                if value
            ),
            *quality_fields,
        }
        missing = sorted(configured_fields - available_fields)
        if missing:
            raise ValueError(
                f"configured domain fields missing from {table_name}: {missing}"
            )
        consumed.update(configured_fields)
        raw = text_expression(field)
        missing_values = list(measurement.get("missing_values") or [])
        predicate = configured_measurement_predicate(field, measurement)
        uncertainty_lower = nullable_measurement_double_expression(
            str(uncertainty_lower_field) if uncertainty_lower_field else None,
            list(
                measurement.get("uncertainty_missing_values", missing_values)
            ),
            absolute=True,
            minimum_value=measurement.get("uncertainty_minimum_value"),
            maximum_value=measurement.get("uncertainty_maximum_value"),
        )
        uncertainty_upper = nullable_measurement_double_expression(
            str(uncertainty_upper_field) if uncertainty_upper_field else None,
            list(measurement.get("uncertainty_missing_values", missing_values)),
            absolute=True,
            minimum_value=measurement.get("uncertainty_minimum_value"),
            maximum_value=measurement.get("uncertainty_maximum_value"),
        )
        unit = nullable_sql_string(measurement.get("unit_raw"))
        normalized_unit = nullable_sql_string(
            measurement.get("normalized_unit", measurement.get("unit_raw"))
        )
        evidence_id = (
            f"sha256({sql_string('configured-domain|' + destination + '|' + field + '|')} "
            "|| r.source_record_id)"
        )
        common_from = f"""
          from read_parquet({sql_string(str(path))}) source_row
          join source_records r
            on r.source_id={sql_string(source_id)}
           and r.release_id={sql_string(release_id)}
           and r.source_table={sql_string(table_name)}
           and r.source_row_sha256=sha256(to_json(source_row))
        """
        quality_members = [
            "'source_field'",
            sql_string(field),
            "'uncertainty_field'",
            nullable_sql_string(uncertainty_field),
            "'uncertainty_lower_field'",
            nullable_sql_string(uncertainty_lower_field),
            "'uncertainty_upper_field'",
            nullable_sql_string(uncertainty_upper_field),
            "'missing_values'",
            sql_string(json.dumps(missing_values)),
        ]
        for quality_field in quality_fields:
            quality_members.extend(
                (sql_string(quality_field), sql_identifier(quality_field))
            )
        quality = "json_object(" + ", ".join(quality_members) + ")"
        normalization = sql_string(
            str(measurement.get("normalization_version") or "source_native_v1")
        )
        epoch = configured_epoch_expression(measurement)
        normalized_value = (
            f"try_cast({sql_identifier(field)} as double)"
            if measurement.get("normalize_numeric", True)
            else "null::double"
        )
        if destination == "astrometry_distance_evidence":
            if storage_modes.get(destination) == "typed_measurement_bundle_v1":
                bundle_structs_by_destination.setdefault(destination, []).append(
                    f"""
                    case when {predicate} then struct_pack(
                      evidence_id := {evidence_id},
                      quantity_key := {sql_string(str(measurement['quantity_key']))},
                      value_raw := {raw},
                      unit_raw := {unit},
                      normalized_value := try_cast({sql_identifier(field)} as double),
                      normalized_unit := {normalized_unit},
                      uncertainty_lower := {uncertainty_lower},
                      uncertainty_upper := {uncertainty_upper},
                      bound_semantics := 'measurement',
                      frame_raw := {nullable_sql_string(measurement.get('frame_raw'))},
                      epoch_raw := {epoch},
                      method := {nullable_sql_string(measurement.get('method'))},
                      model := {nullable_sql_string(measurement.get('model'))},
                      reference_raw := {text_expression(reference_field)},
                      quality_json := {quality},
                      normalization_version := {normalization}
                    ) else null end
                    """
                )
                continue
            branch = f"""
              select distinct
                {evidence_id}, r.source_record_id,
                {sql_string(str(measurement['quantity_key']))}, {raw}, {unit},
                {normalized_value}, {normalized_unit},
                {uncertainty_lower}, {uncertainty_upper}, 'measurement',
                {nullable_sql_string(measurement.get('frame_raw'))},
                {epoch},
                {nullable_sql_string(measurement.get('method'))},
                {nullable_sql_string(measurement.get('model'))},
                {text_expression(reference_field)},
                {quality}, {normalization}
              {common_from}
              where {predicate}
            """
        elif destination == "variability_activity_rotation_evidence":
            branch = f"""
              select distinct
                {evidence_id}, r.source_record_id,
                {sql_string(str(measurement.get('evidence_kind') or 'variability'))},
                {sql_string(str(measurement['quantity_key']))}, {raw}, {unit},
                {normalized_value}, {normalized_unit},
                {uncertainty_lower}, {uncertainty_upper},
                {nullable_sql_string(measurement.get('method'))},
                {nullable_sql_string(measurement.get('model'))},
                {text_expression(reference_field)},
                {quality}, {normalization}
              {common_from}
              where {predicate}
            """
        else:
            raise ValueError(f"unsupported configured domain destination: {destination}")
        branches_by_destination.setdefault(destination, []).append(branch)
    for destination, branches in branches_by_destination.items():
        con.execute(
            f"insert into {sql_identifier(destination)} " + " union all ".join(branches)
        )
    for destination, structs in bundle_structs_by_destination.items():
        if destination != "astrometry_distance_evidence":
            raise ValueError(f"unsupported bundle destination: {destination}")
        bundle_values = "[" + ",".join(structs) + "]"
        con.execute(
            f"""
            insert into astrometry_distance_evidence_bundles
            with bundled as (
              select distinct
                sha256({sql_string('configured-domain-bundle|' + destination + '|' + table_name + '|')} || r.source_record_id) bundle_id,
                r.source_record_id,
                'storage_group_only_no_parameter_coherence' bundle_semantics,
                list_filter({bundle_values}, measurement -> measurement is not null) measurements
              from read_parquet({sql_string(str(path))}) source_row
              join source_records r
                on r.source_id={sql_string(source_id)}
               and r.release_id={sql_string(release_id)}
               and r.source_table={sql_string(table_name)}
               and r.source_row_sha256=sha256(to_json(source_row))
            )
            select * from bundled where len(measurements)>0
            """
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
    missing_values_by_field: dict[str, list[Any]] | None = None,
) -> set[str]:
    missing_values_by_field = missing_values_by_field or {}
    branches = []
    for field in fields:
        name = str(field["column_name"])
        raw = text_expression(name)
        predicate = missing_value_predicate(
            name, list(missing_values_by_field.get(name) or [])
        )
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
            from read_parquet({sql_string(str(path))}) source_row
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(source_row))
            where {predicate}
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
    left, left_fields = configured_text_expression(
        relation_claim,
        key="left_identifier",
        available_fields=available_fields,
        table_alias="source_row",
    )
    right, right_fields = configured_text_expression(
        relation_claim,
        key="right_identifier",
        available_fields=available_fields,
        table_alias="source_row",
    )
    probability_field = relation_claim.get("probability_field")
    statistic_field = relation_claim.get("confidence_statistic_field")
    epoch_field = relation_claim.get("epoch_field")
    quality_fields = [str(field) for field in relation_claim.get("quality_fields") or []]
    required = {
        *left_fields,
        *right_fields,
        *(str(field) for field in (probability_field, statistic_field, epoch_field) if field),
        *quality_fields,
    }
    missing = sorted(required - available_fields)
    if missing:
        raise ValueError(f"relation fields missing from {table_name}: {missing}")

    static_polarity = relation_claim.get("evidence_polarity")
    dynamic_polarity = relation_claim.get("evidence_polarity_sql")
    if bool(static_polarity) == bool(dynamic_polarity):
        raise ValueError(
            "relation claim requires exactly one of evidence_polarity or evidence_polarity_sql"
        )
    evidence_polarity = (
        str(dynamic_polarity)
        if dynamic_polarity
        else sql_string(str(static_polarity))
    )
    probability = (
        f"try_cast({qualified_sql_identifier(str(probability_field), 'source_row')} as double)"
        if probability_field
        else "null::double"
    )
    probability_semantics = (
        sql_string(str(relation_claim["probability_semantics"]))
        if probability_field
        else "null::varchar"
    )
    statistic_raw = (
        text_expression(str(statistic_field), table_alias="source_row")
        if statistic_field
        else "null::varchar"
    )
    statistic_value = (
        f"try_cast({qualified_sql_identifier(str(statistic_field), 'source_row')} as double)"
        if statistic_field
        else "null::double"
    )
    epoch = (
        text_expression(str(epoch_field), table_alias="source_row")
        if epoch_field
        else "null::varchar"
    )
    quality_members = [
        "'source_table'",
        sql_string(table_name),
        "'source_evidence_polarity'",
        evidence_polarity,
    ]
    for field in quality_fields:
        quality_members.extend(
            (
                sql_string(field),
                qualified_sql_identifier(field, "source_row"),
            )
        )
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
          {sql_string(str(relation_claim['left_component_scope']))},
          {sql_string(str(relation_claim['right_identifier_namespace']))},
          {right},
          {sql_string(str(relation_claim['right_component_scope']))},
          {sql_string(str(relation_claim['relation_kind']))},
          {sql_string(str(relation_claim['relation_scope']))},
          {probability},
          {probability_semantics},
          {nullable_sql_string(relation_claim.get('confidence_statistic_key'))},
          {statistic_raw},
          {statistic_value},
          {nullable_sql_string(relation_claim.get('confidence_statistic_unit'))},
          {nullable_sql_string(relation_claim.get('confidence_statistic_semantics'))},
          {evidence_polarity},
          {sql_string(str(relation_claim['method']))},
          {sql_string(str(relation_claim['reference_raw']))},
          {epoch},
          {quality}
        from read_parquet({sql_string(str(path))}) source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where nullif({left}, '') is not null
          and nullif({right}, '') is not null
          and ({str(relation_claim.get('sql_predicate') or 'true')})
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
            orbital_solution.get("model_field"),
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
    solution_key = f"cast({logical_key_expression(key_fields, 'source_row')} as varchar)"
    parameters = logical_key_expression(parameter_fields, "source_row")
    quality = logical_key_expression(quality_fields, "source_row")
    epoch = text_expression(orbital_solution.get("epoch_field"))
    frame = (
        text_expression(orbital_solution.get("frame_field"))
        if orbital_solution.get("frame_field")
        else nullable_sql_string(orbital_solution.get("frame"))
    )
    reference = (
        text_expression(orbital_solution.get("reference_field"))
        if orbital_solution.get("reference_field")
        else nullable_sql_string(orbital_solution.get("reference_raw"))
    )
    model = (
        text_expression(orbital_solution.get("model_field"))
        if orbital_solution.get("model_field")
        else nullable_sql_string(orbital_solution.get("model"))
    )
    relation_link = orbital_solution.get("relation_link") or {}
    relation_claim_id = "null::varchar"
    if relation_link:
        key_fields_map = {
            str(local_field): str(relation_field)
            for local_field, relation_field in relation_link["key_fields"].items()
        }
        missing_link_fields = sorted(set(key_fields_map) - available_fields)
        if missing_link_fields:
            raise ValueError(
                f"orbital relation-link fields missing from {table_name}: {missing_link_fields}"
            )
        consumed.update(key_fields_map)
        predicates = " and ".join(
            f"trim(cast(rr.logical_key_json->>{sql_string(relation_field)} as varchar))="
            f"trim(cast(source_row.{sql_identifier(local_field)} as varchar))"
            for local_field, relation_field in key_fields_map.items()
        )
        relation_claim_id = f"""
          (
            select case when count(*)=1 then min(rc.evidence_id) end
            from source_records rr
            join relation_claim_evidence rc using (source_record_id)
            where rr.source_id={sql_string(source_id)}
              and rr.release_id={sql_string(release_id)}
              and rr.source_table={sql_string(str(relation_link['source_table']))}
              and {predicates}
          )
        """
    namespace = f"orbital-solution|{table_name}|"
    con.execute(
        f"""
        insert into orbital_solution_evidence
        select distinct
          sha256({sql_string(namespace)} || r.source_record_id),
          r.source_record_id,
          {relation_claim_id},
          {solution_key},
          {parameters},
          {epoch},
          {frame},
          {sql_string(str(orbital_solution['method']))},
          {model},
          {reference},
          {quality},
          {sql_string(str(orbital_solution['normalization_version']))}
        from read_parquet({sql_string(str(path))}) source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where {str(orbital_solution.get('sql_predicate') or 'true')}
        """
    )
    if relation_link.get("required"):
        unresolved = int(
            con.execute(
                """
                select count(*)
                from orbital_solution_evidence o
                join source_records r using (source_record_id)
                where r.source_id=? and r.release_id=? and r.source_table=?
                  and o.relation_claim_id is null
                """,
                [source_id, release_id, table_name],
            ).fetchone()[0]
        )
        if unresolved:
            raise ValueError(
                f"required orbital relation links unresolved in {table_name}: {unresolved}"
            )
    return consumed


def materialize_extended_objects(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    fields: list[dict[str, Any]],
    extended_object: dict[str, Any],
    available_fields: set[str],
) -> set[str]:
    destination_fields = {str(field["column_name"]) for field in fields}
    key_fields = [str(field) for field in extended_object["identity_key_fields"]]
    geometry_fields = [str(field) for field in extended_object["geometry_fields"]]
    distance_fields = [str(field) for field in extended_object.get("distance_fields") or []]
    parameter_fields = [str(field) for field in extended_object["parameter_fields"]]
    quality_fields = [str(field) for field in extended_object["quality_fields"]]
    reference_field = extended_object.get("reference_field")
    optional_fields = [str(reference_field)] if reference_field else []
    consumed = set(
        key_fields
        + geometry_fields
        + distance_fields
        + parameter_fields
        + quality_fields
        + optional_fields
    )
    missing = sorted(consumed - available_fields)
    if missing:
        raise ValueError(f"extended-object fields missing from {table_name}: {missing}")
    unconsumed = sorted(destination_fields - consumed)
    if unconsumed:
        raise ValueError(
            f"extended-object fields lack a typed role in {table_name}: {unconsumed}"
        )
    identity_key = logical_key_expression(key_fields, "source_row")
    geometry = logical_key_expression(geometry_fields, "source_row")
    distance = (
        logical_key_expression(distance_fields, "source_row")
        if distance_fields
        else "null::json"
    )
    parameters = logical_key_expression(parameter_fields, "source_row")
    quality = logical_key_expression(quality_fields, "source_row")
    reference = (
        text_expression(str(reference_field))
        if reference_field
        else nullable_sql_string(extended_object.get("reference_raw"))
    )
    namespace = f"extended-object|{table_name}|"
    con.execute(
        f"""
        insert into extended_object_evidence
        select distinct
          sha256({sql_string(namespace)} || r.source_record_id),
          r.source_record_id,
          {sql_string(str(extended_object['extended_kind']))},
          {geometry}, {distance}, {parameters},
          {sql_string(str(extended_object['method']))},
          {nullable_sql_string(extended_object.get('model'))},
          {reference}, {quality},
          {sql_string(str(extended_object['normalization_version']))}
        from read_parquet({sql_string(str(path))}) source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where nullif(cast({identity_key} as varchar), '') is not null
        """
    )
    return consumed


def materialize_compact_objects(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    fields: list[dict[str, Any]],
    compact_object: dict[str, Any],
    available_fields: set[str],
) -> set[str]:
    destination_fields = {str(field["column_name"]) for field in fields}
    key_fields = [str(field) for field in compact_object["parameter_set_key_fields"]]
    parameter_fields = [str(field) for field in compact_object["parameter_fields"]]
    quality_fields = [str(field) for field in compact_object["quality_fields"]]
    optional_fields = [
        str(field)
        for field in (compact_object.get("reference_field"),)
        if field
    ]
    consumed = set(key_fields + parameter_fields + quality_fields + optional_fields)
    missing = sorted(consumed - available_fields)
    if missing:
        raise ValueError(f"compact-object fields missing from {table_name}: {missing}")
    parameter_set_key = logical_key_expression(key_fields, "source_row")
    parameters = logical_key_expression(parameter_fields, "source_row")
    quality = logical_key_expression(quality_fields, "source_row")
    reference_field = compact_object.get("reference_field")
    reference = (
        text_expression(reference_field)
        if reference_field
        else nullable_sql_string(compact_object.get("reference_raw"))
    )
    if reference_field and compact_object.get("reference_catalog_validated"):
        candidate = text_expression(str(reference_field))
        reference = f"""
          case when exists (
            select 1 from citations c
            where c.source_id={sql_string(source_id)}
              and c.source_reference_key={candidate}
          ) then {candidate} end
        """
    compact_kind = str(compact_object["compact_kind"])
    namespace = f"compact-object|{table_name}|{compact_kind}|"
    predicate = str(compact_object.get("sql_predicate") or "true")
    con.execute(
        f"""
        insert into compact_object_evidence
        select distinct
          sha256({sql_string(namespace)} || r.source_record_id),
          r.source_record_id,
          {sql_string(compact_kind)},
          cast({parameter_set_key} as varchar), {parameters},
          {sql_string(str(compact_object['method']))},
          {nullable_sql_string(compact_object.get('model'))},
          {reference}, {quality},
          {sql_string(str(compact_object['normalization_version']))}
        from read_parquet({sql_string(str(path))}) source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where {predicate}
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
    if bibcode is None and re.fullmatch(
        r"(?:18|19|20)\d{2}\S{15}", display_text
    ):
        bibcode = display_text
        url = f"https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract"
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


def materialize_citation_catalog(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    citation_catalog: dict[str, Any],
    fields: list[dict[str, Any]],
    available_fields: set[str],
) -> set[str]:
    destination_fields = {str(field["column_name"]) for field in fields}
    key_field = str(citation_catalog["reference_key_field"])
    text_field = str(citation_catalog["citation_text_field"])
    context_fields = [str(field) for field in citation_catalog.get("context_fields") or []]
    url_field = citation_catalog.get("citation_url_field")
    bibcode_field = citation_catalog.get("bibcode_field")
    doi_field = citation_catalog.get("doi_field")
    publication_year_field = citation_catalog.get("publication_year_field")
    aggregate_lines = bool(citation_catalog.get("aggregate_repeated_key_lines"))
    line_order_field = citation_catalog.get("line_order_field")
    line_separator = str(citation_catalog.get("line_separator") or " ")
    excluded_values = [
        str(value) for value in citation_catalog.get("excluded_values") or []
    ]
    consumed = {key_field, text_field, *context_fields}
    consumed.update(
        str(field)
        for field in (
            url_field,
            bibcode_field,
            doi_field,
            publication_year_field,
        )
        if field
    )
    if line_order_field:
        consumed.add(str(line_order_field))
    missing = sorted(consumed - available_fields)
    if missing:
        raise ValueError(f"citation-catalog fields missing from {table_name}: {missing}")
    unconsumed = sorted(destination_fields - consumed)
    if unconsumed:
        raise ValueError(
            f"citation-catalog fields lack a typed role in {table_name}: {unconsumed}"
        )
    context = logical_key_expression(context_fields, "source_row") if context_fields else "'{}'::json"
    excluded_predicate = ""
    if excluded_values:
        excluded_predicate = (
            "and trim(cast(source_row."
            + sql_identifier(key_field)
            + " as varchar)) not in ("
            + ", ".join(sql_string(value) for value in excluded_values)
            + ")"
        )
    if aggregate_lines:
        rows_sql = f"""
        select
          trim(cast(source_row.{sql_identifier(key_field)} as varchar)),
          string_agg(
            trim(cast(source_row.{sql_identifier(text_field)} as varchar)),
            {sql_string(line_separator)}
            order by source_row.{sql_identifier(str(line_order_field))}
          ),
          null::varchar, null::varchar, null::varchar, null::varchar,
          cast(json_object(
            'aggregation', 'repeated_reference_key_lines_v1',
            'line_count', count(*),
            'line_order_field', {sql_string(str(line_order_field))},
            'first_line', min(source_row.{sql_identifier(str(line_order_field))}),
            'last_line', max(source_row.{sql_identifier(str(line_order_field))})
          ) as varchar)
        from read_parquet({sql_string(str(path))}) source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where nullif(trim(cast(source_row.{sql_identifier(key_field)} as varchar)), '') is not null
          and nullif(trim(cast(source_row.{sql_identifier(text_field)} as varchar)), '') is not null
          {excluded_predicate}
        group by trim(cast(source_row.{sql_identifier(key_field)} as varchar))
        order by 1
        """
    else:
        rows_sql = f"""
        select distinct
          trim(cast(source_row.{sql_identifier(key_field)} as varchar)),
          trim(cast(source_row.{sql_identifier(text_field)} as varchar)),
          {text_expression(str(url_field) if url_field else None)},
          {text_expression(str(bibcode_field) if bibcode_field else None)},
          {text_expression(str(doi_field) if doi_field else None)},
          {text_expression(str(publication_year_field) if publication_year_field else None)},
          cast({context} as varchar)
        from read_parquet({sql_string(str(path))}) source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where nullif(trim(cast(source_row.{sql_identifier(key_field)} as varchar)), '') is not null
          and nullif(trim(cast(source_row.{sql_identifier(text_field)} as varchar)), '') is not null
          {excluded_predicate}
        order by 1, 2
        """
    rows = con.execute(rows_sql).fetchall()
    inserts = []
    for (
        reference_key,
        citation_text,
        citation_url,
        source_bibcode,
        source_doi,
        source_year,
        context_json,
    ) in rows:
        parsed = parse_reference_fragment(str(citation_text))
        parsed["source_context"] = json.loads(str(context_json))
        bibcode = str(source_bibcode).strip() if source_bibcode else parsed["bibcode"]
        doi = str(source_doi).strip() if source_doi else parsed["doi"]
        publication_year = (
            int(source_year)
            if source_year and str(source_year).strip().isdigit()
            else parsed["publication_year"]
        )
        url = str(citation_url).strip() if citation_url else parsed["url"]
        if not url and bibcode:
            url = f"https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract"
        citation_id = hashlib.sha256(
            f"citation-catalog|{source_id}|{reference_key}|{citation_text}".encode()
        ).hexdigest()
        inserts.append(
            [
                citation_id,
                source_id,
                str(reference_key),
                str(citation_text),
                url,
                bibcode,
                doi,
                publication_year,
                json.dumps(parsed, sort_keys=True),
            ]
        )
    if inserts:
        con.executemany("insert into citations values (?, ?, ?, ?, ?, ?, ?, ?, ?)", inserts)
    return consumed


def materialize_source_citation_links(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    links: list[dict[str, Any]],
    available_fields: set[str],
) -> set[str]:
    consumed: set[str] = set()
    for link in links:
        claim_field = str(link["identifier_claim_field"])
        reference_field = str(link["reference_key_field"])
        required_fields = {claim_field, reference_field}
        missing = sorted(required_fields - available_fields)
        if missing:
            raise ValueError(f"source citation-link fields missing from {table_name}: {missing}")
        consumed.update(required_fields)
        excluded_values = [
            str(value) for value in link.get("excluded_reference_values") or []
        ]
        excluded_predicate = ""
        if excluded_values:
            excluded_predicate = (
                f"and trim(cast(source_row.{sql_identifier(reference_field)} as varchar)) not in ("
                + ", ".join(sql_string(value) for value in excluded_values)
                + ")"
            )
        evidence_id = (
            f"sha256({sql_string('identifier|' + claim_field + '|')} || "
            f"r.source_record_id || '|' || trim(cast(source_row.{sql_identifier(claim_field)} as varchar)))"
        )
        before = int(con.execute("select count(*) from evidence_citations").fetchone()[0])
        con.execute(
            f"""
            insert into evidence_citations
            select distinct
              'identifier_claim_evidence', e.evidence_id, c.citation_id,
              {sql_string(str(link['citation_role']))}
            from read_parquet({sql_string(str(path))}) source_row
            join source_records r
              on r.source_id={sql_string(source_id)}
             and r.release_id={sql_string(release_id)}
             and r.source_table={sql_string(table_name)}
             and r.source_row_sha256=sha256(to_json(source_row))
            join identifier_claim_evidence e on e.evidence_id={evidence_id}
            join citations c
              on c.source_id={sql_string(source_id)}
             and c.source_reference_key=trim(cast(source_row.{sql_identifier(reference_field)} as varchar))
            where nullif(trim(cast(source_row.{sql_identifier(claim_field)} as varchar)), '') is not null
              and nullif(trim(cast(source_row.{sql_identifier(reference_field)} as varchar)), '') is not null
              {excluded_predicate}
            """
        )
        if link.get("required"):
            expected = int(
                con.execute(
                    f"""
                    select count(distinct r.source_record_id)
                    from read_parquet({sql_string(str(path))}) source_row
                    join source_records r
                      on r.source_id={sql_string(source_id)}
                     and r.release_id={sql_string(release_id)}
                     and r.source_table={sql_string(table_name)}
                     and r.source_row_sha256=sha256(to_json(source_row))
                    where nullif(trim(cast(source_row.{sql_identifier(claim_field)} as varchar)), '') is not null
                      and nullif(trim(cast(source_row.{sql_identifier(reference_field)} as varchar)), '') is not null
                      {excluded_predicate}
                    """
                ).fetchone()[0]
            )
            inserted = int(con.execute("select count(*) from evidence_citations").fetchone()[0]) - before
            if inserted != expected:
                raise ValueError(
                    f"required source citation links unresolved in {table_name}: "
                    f"inserted={inserted} expected={expected}"
                )
    return consumed


def materialize_cluster_context(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    fields: list[dict[str, Any]],
    cluster_context: dict[str, Any],
    available_fields: set[str],
) -> set[str]:
    identity_field = str(cluster_context["cluster_identity_field"])
    reference_field = cluster_context.get("reference_field")
    quality_fields = [str(value) for value in cluster_context.get("quality_fields") or []]
    parameter_fields = [str(field["column_name"]) for field in fields]
    configured_fields = {
        identity_field,
        *parameter_fields,
        *quality_fields,
        *(str(reference_field) for _ in [0] if reference_field),
    }
    missing = sorted(configured_fields - available_fields)
    if missing:
        raise ValueError(f"cluster context fields missing from {table_name}: {missing}")
    reference = (
        text_expression(str(reference_field))
        if reference_field
        else nullable_sql_string(cluster_context.get("reference_raw"))
    )
    parameters = (
        logical_key_expression(parameter_fields, "source_row")
        if parameter_fields
        else "'{}'::json"
    )
    source_quality = (
        logical_key_expression(quality_fields, "source_row")
        if quality_fields
        else "'{}'::json"
    )
    predicate = str(cluster_context.get("sql_predicate") or "true")
    con.execute(
        f"""
        insert into cluster_evidence
        select distinct
          sha256({sql_string('cluster-context|' + table_name + '|')} || r.source_record_id),
          r.source_record_id,
          trim(cast({sql_identifier(identity_field)} as varchar)),
          {parameters},
          {sql_string(str(cluster_context['method']))},
          {nullable_sql_string(cluster_context.get('model'))},
          {reference},
          json_object(
            'source_table', {sql_string(table_name)},
            'source_quality', {source_quality},
            'row_selection_policy', {nullable_sql_string((cluster_context.get('selection_context') or {}).get('policy_id'))}
          ),
          {sql_string(str(cluster_context['normalization_version']))}
        from read_parquet({sql_string(str(path))}) source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where nullif(trim(cast({sql_identifier(identity_field)} as varchar)), '') is not null
          and ({predicate})
        """
    )
    return configured_fields


def materialize_cluster_memberships(
    con: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    release_id: str,
    table_name: str,
    path: Path,
    fields: list[dict[str, Any]],
    cluster_membership: dict[str, Any],
    available_fields: set[str],
    evidence_key: str | None = None,
) -> set[str]:
    cluster_field = str(cluster_membership["cluster_identity_field"])
    member_field = str(cluster_membership["member_identity_field"])
    probability_field = cluster_membership.get("membership_probability_field")
    probability_field = str(probability_field) if probability_field else None
    reference_field = cluster_membership.get("reference_field")
    quality_fields = list(
        dict.fromkeys(
            [str(field["column_name"]) for field in fields]
            + [str(field) for field in cluster_membership.get("quality_fields") or []]
        )
    )
    configured_fields = {
        cluster_field,
        member_field,
        *quality_fields,
        *(probability_field for _ in [0] if probability_field),
        *(str(reference_field) for _ in [0] if reference_field),
    }
    missing = sorted(configured_fields - available_fields)
    if missing:
        raise ValueError(f"cluster membership fields missing from {table_name}: {missing}")
    invalid_probability_count = 0
    if probability_field:
        invalid_probability_count = int(
            con.execute(
                f"""
                select count(*)
                from read_parquet({sql_string(str(path))}) source_row
                join source_records r
                  on r.source_id={sql_string(source_id)}
                 and r.release_id={sql_string(release_id)}
                 and r.source_table={sql_string(table_name)}
                 and r.source_row_sha256=sha256(to_json(source_row))
                where try_cast({sql_identifier(probability_field)} as double) is not null
                  and try_cast({sql_identifier(probability_field)} as double) not between 0 and 1
                """
            ).fetchone()[0]
        )
    if invalid_probability_count:
        raise ValueError(
            f"cluster membership probability outside [0,1] in {table_name}: "
            f"{invalid_probability_count}"
        )
    reference = (
        text_expression(str(reference_field))
        if reference_field
        else nullable_sql_string(cluster_membership.get("reference_raw"))
    )
    source_quality = (
        logical_key_expression(quality_fields, "source_row")
        if quality_fields
        else "'{}'::json"
    )
    predicate = str(cluster_membership.get("sql_predicate") or "true")
    evidence_prefix = f"cluster-membership|{table_name}|"
    if evidence_key:
        evidence_prefix += f"{evidence_key}|"
    probability = (
        f"try_cast({sql_identifier(probability_field)} as double)"
        if probability_field
        else "null::double"
    )
    con.execute(
        f"""
        insert into cluster_membership_evidence
        select distinct
          sha256({sql_string(evidence_prefix)} || r.source_record_id),
          r.source_record_id,
          trim(cast({sql_identifier(cluster_field)} as varchar)),
          trim(cast({sql_identifier(member_field)} as varchar)),
          {probability},
          {sql_string(str(cluster_membership['method']))},
          {reference},
          json_object(
            'source_table', {sql_string(table_name)},
            'evidence_key', {nullable_sql_string(evidence_key)},
            'probability_semantics', {nullable_sql_string(cluster_membership.get('probability_semantics'))},
            'source_membership_record', {source_quality}
          )
        from read_parquet({sql_string(str(path))}) source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(table_name)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where nullif(trim(cast({sql_identifier(cluster_field)} as varchar)), '') is not null
          and nullif(trim(cast({sql_identifier(member_field)} as varchar)), '') is not null
          and ({predicate})
        """
    )
    return configured_fields


def materialize_citations(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    reference_branches = [
        f"""
        select r.source_id, e.reference_raw
        from {sql_identifier(table)} e
        join source_records r using (source_record_id)
        where nullif(trim(e.reference_raw), '') is not null
        """
        for table in sorted(EVIDENCE_REFERENCE_TABLES)
    ]
    reference_branches.append(
        """
        select r.source_id, measurement.reference_raw
        from astrometry_distance_evidence_bundles b
        join source_records r using (source_record_id),
        unnest(b.measurements) as nested(measurement)
        where nullif(trim(measurement.reference_raw), '') is not null
        """
    )
    reference_union = " union all ".join(reference_branches)
    references = con.execute(
        f"""
        with source_references as (
          select distinct source_id, reference_raw from ({reference_union})
        )
        select r.source_id, r.reference_raw
        from source_references r
        where not exists (
          select 1 from citations c
          where c.source_id=r.source_id
            and c.source_reference_key=r.reference_raw
        )
          and not exists (
            select 1 from citations c
            where c.source_id=r.source_id
              and c.citation_text_raw=r.reference_raw
          )
        order by r.source_id, r.reference_raw
        """
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
    con.execute(
        """
        create temp table citation_match_keys as
        select source_id, source_reference_key match_key, citation_id
        from citations
        where nullif(trim(source_reference_key), '') is not null
        union all
        select source_id, citation_text_raw match_key, citation_id
        from citations
        where nullif(trim(citation_text_raw), '') is not null
          and citation_text_raw is distinct from source_reference_key
        """
    )
    for table in sorted(EVIDENCE_REFERENCE_TABLES):
        con.execute(
            f"""
            insert into evidence_citations
            select
              {sql_string(table)}, e.evidence_id, c.citation_id, 'source_reference'
            from {sql_identifier(table)} e
            join source_records r using (source_record_id)
            join citation_match_keys c
              on c.source_id=r.source_id
             and c.match_key=e.reference_raw
            where nullif(trim(e.reference_raw), '') is not null
            """
        )
    for bucket in range(CITATION_LINK_BUCKET_COUNT):
        con.execute(
            f"""
            insert into evidence_citations
            select
              'astrometry_distance_evidence_bundles', measurement.evidence_id,
              c.citation_id, 'source_reference'
            from (
              select source_record_id, measurements
              from astrometry_distance_evidence_bundles
              where hash(source_record_id) % {CITATION_LINK_BUCKET_COUNT} = {bucket}
            ) b
            join source_records r using (source_record_id),
            unnest(b.measurements) as nested(measurement)
            join citation_match_keys c
              on c.source_id=r.source_id
             and c.match_key=measurement.reference_raw
            where nullif(trim(measurement.reference_raw), '') is not null
            """
        )
    link_count = int(con.execute("select count(*) from evidence_citations").fetchone()[0])
    con.execute("drop table citation_match_keys")
    return {
        "citations": int(con.execute("select count(*) from citations").fetchone()[0]),
        "evidence_links": link_count,
    }


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
            logical_key_expression(context_fields, "source_row")
            if context_fields
            else "'{}'::json"
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
                sha256(to_json(source_row)) source_row_sha256,
                trim(cast({sql_identifier(identifier_field)} as varchar)) identifier_raw,
                {disposition_source} disposition_raw,
                {effective_expression} effective_raw,
                {reference_expression} reference_raw,
                {context_json} context_json
              from read_parquet({sql_string(str(path))}) source_row
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
        for table_name in adapter["tables"]
        for table in [resolve_table_contract(adapter, table_name)]
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
    table_order = [
        str(value)
        for value in (adapter.get("table_order") or sorted(configured_tables))
    ]
    for table_name in table_order:
        table_contract = resolve_table_contract(adapter, table_name)
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

        source_row_hash = "sha256(to_json(source_row))"
        key_json = logical_key_expression(logical_fields, "source_row")
        context_json = (
            logical_key_expression(context_fields, "source_row")
            if context_fields
            else "'{}'::json"
        )
        row_predicate = row_selection_predicate(
            table_contract,
            typed_tables=typed_tables,
            typed_root=input_row["typed_path"],
            available_fields=set(columns),
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
              from read_parquet({sql_string(str(path))}) source_row
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
            materialize_composite_identifier_claims(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                claims=list(table_contract.get("composite_identifier_claims") or []),
                available_fields=set(columns),
            )
        )
        materialized_fields.update(
            materialize_conditional_identifier_claims(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                claims=list(table_contract.get("conditional_identifier_claims") or []),
                available_fields=set(columns),
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
        configured_domain_fields = configured_domain_measurement_fields(
            list(table_contract.get("configured_domain_measurements") or [])
        )
        configured_coordinate_fields = configured_coordinate_measurement_fields(
            list(table_contract.get("configured_coordinate_measurements") or [])
        )
        configured_photometry = configured_photometry_fields(
            list(table_contract.get("photometry_measurements") or [])
        )
        for destination in sorted(SCALAR_EVIDENCE_DESTINATIONS):
            destination_fields = [
                field
                for field in (fields_by_destination.get(destination) or [])
                if str(field["column_name"]) not in scoped_stellar_fields
                and str(field["column_name"]) not in configured_domain_fields
                and str(field["column_name"]) not in configured_coordinate_fields
                and not (
                    destination == "photometry_extinction_evidence"
                    and str(field["column_name"]) in configured_photometry
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
        materialized_fields.update(
                materialize_configured_domain_measurements(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                    measurements=list(
                        table_contract.get("configured_domain_measurements") or []
                    ),
                    available_fields=set(columns),
                    storage_modes=dict(
                        table_contract.get("configured_domain_storage") or {}
                    ),
                )
        )
        materialized_fields.update(
            materialize_configured_coordinate_measurements(
                con,
                source_id=source_id,
                release_id=release_id,
                table_name=table_name,
                path=path,
                measurements=list(
                    table_contract.get("configured_coordinate_measurements") or []
                ),
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
                    missing_values_by_field=dict(
                        table_contract.get("observation_product_missing_values") or {}
                    ),
                )
            )
        relation_claim = table_contract.get("relation_claim")
        relation_claims = table_contract.get("relation_claims")
        relation_contracts = (
            list(relation_claims)
            if relation_claims
            else [relation_claim]
            if relation_claim
            else []
        )
        for relation_contract in relation_contracts:
            materialized_fields.update(
                materialize_relation_claims(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    relation_claim=relation_contract,
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
        cluster_fields = fields_by_destination.get("cluster_evidence") or []
        cluster_context = table_contract.get("cluster_context")
        if cluster_fields or cluster_context:
            if not cluster_fields or not cluster_context:
                raise ValueError(
                    f"cluster-context profile/config mismatch for {table_name}"
                )
            materialized_fields.update(
                materialize_cluster_context(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    fields=cluster_fields,
                    cluster_context=cluster_context,
                    available_fields=set(columns),
                )
            )
        membership_fields = fields_by_destination.get(
            "cluster_membership_evidence"
        ) or []
        cluster_membership = table_contract.get("cluster_membership")
        cluster_memberships = table_contract.get("cluster_memberships")
        membership_contracts = (
            [(None, cluster_membership)]
            if cluster_membership
            else [
                (str(row["evidence_key"]), row)
                for row in (cluster_memberships or [])
            ]
        )
        if membership_fields or membership_contracts:
            if not membership_fields or not membership_contracts:
                raise ValueError(
                    f"cluster-membership profile/config mismatch for {table_name}"
                )
            for evidence_key, membership_contract in membership_contracts:
                materialized_fields.update(
                    materialize_cluster_memberships(
                        con,
                        source_id=source_id,
                        release_id=release_id,
                        table_name=table_name,
                        path=path,
                        fields=membership_fields,
                        cluster_membership=membership_contract,
                        available_fields=set(columns),
                        evidence_key=evidence_key,
                    )
                )
        extended_object_fields = fields_by_destination.get(
            "extended_object_evidence"
        ) or []
        extended_object = table_contract.get("extended_object")
        if extended_object_fields or extended_object:
            if not extended_object_fields or not extended_object:
                raise ValueError(
                    f"extended-object profile/config mismatch for {table_name}"
                )
            materialized_fields.update(
                materialize_extended_objects(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    fields=extended_object_fields,
                    extended_object=extended_object,
                    available_fields=set(columns),
                )
            )
        compact_object_fields = fields_by_destination.get(
            "compact_object_evidence"
        ) or []
        compact_objects = list(
            table_contract.get("compact_object_parameter_sets") or []
        )
        if table_contract.get("compact_object"):
            compact_objects = [table_contract["compact_object"]]
        if compact_object_fields or compact_objects:
            if not compact_object_fields or not compact_objects:
                raise ValueError(
                    f"compact-object profile/config mismatch for {table_name}"
                )
            for compact_object in compact_objects:
                materialized_fields.update(
                    materialize_compact_objects(
                        con,
                        source_id=source_id,
                        release_id=release_id,
                        table_name=table_name,
                        path=path,
                        fields=compact_object_fields,
                        compact_object=compact_object,
                        available_fields=set(columns),
                    )
                )
        citation_fields = fields_by_destination.get("citations") or []
        citation_catalog = table_contract.get("citation_catalog")
        if citation_fields or citation_catalog:
            if not citation_fields or not citation_catalog:
                raise ValueError(
                    f"citation-catalog profile/config mismatch for {table_name}"
                )
            materialized_fields.update(
                materialize_citation_catalog(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    citation_catalog=citation_catalog,
                    fields=citation_fields,
                    available_fields=set(columns),
                )
            )
        source_citation_fields = fields_by_destination.get("evidence_citations") or []
        source_citation_links = list(table_contract.get("source_citation_links") or [])
        if source_citation_fields or source_citation_links:
            if source_citation_fields and not source_citation_links:
                raise ValueError(
                    f"source citation-link profile/config mismatch for {table_name}"
                )
            materialized_fields.update(
                materialize_source_citation_links(
                    con,
                    source_id=source_id,
                    release_id=release_id,
                    table_name=table_name,
                    path=path,
                    links=source_citation_links,
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
    cursor = con.execute(
        f"select sha256(to_json(source_row)) row_hash from {quoted} source_row order by row_hash"
    )
    bucket_parts: list[str] = []
    current_bucket: str | None = None
    bucket_rows = 0
    total_rows = 0
    bucket_digest = hashlib.sha256()
    while batch := cursor.fetchmany(LOGICAL_HASH_BATCH_SIZE):
        for (row_hash_value,) in batch:
            row_hash = str(row_hash_value)
            bucket = row_hash[:2]
            if current_bucket is None:
                current_bucket = bucket
            elif bucket != current_bucket:
                bucket_parts.append(
                    f"{current_bucket}:{bucket_rows}:{bucket_digest.hexdigest()}"
                )
                current_bucket = bucket
                bucket_rows = 0
                bucket_digest = hashlib.sha256()
            bucket_digest.update(row_hash.encode("ascii"))
            bucket_rows += 1
            total_rows += 1
    if current_bucket is not None:
        bucket_parts.append(
            f"{current_bucket}:{bucket_rows}:{bucket_digest.hexdigest()}"
        )
    digest = hashlib.sha256("|".join(bucket_parts).encode("ascii")).hexdigest()
    return {
        "table": table_name,
        "row_count": total_rows,
        "logical_sha256": digest,
        "logical_hash_algorithm": LOGICAL_HASH_ALGORITHM,
    }


def database_block_report(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    row = con.execute(
        "select database_size,block_size,total_blocks,used_blocks,free_blocks "
        "from pragma_database_size()"
    ).fetchone()
    return {
        "database_size": str(row[0]),
        "block_size": int(row[1]),
        "total_blocks": int(row[2]),
        "used_blocks": int(row[3]),
        "free_blocks": int(row[4]),
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
    contract_sha256 = file_hash(contract_path)
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
            "contract_sha256": contract_sha256,
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
    duckdb_temporary, temporary_storage_policy = create_duckdb_temporary_directory(
        temporary, build_id
    )
    created_at = deterministic_build_timestamp(inputs)
    con = duckdb.connect(str(database_path))
    source_reports: list[dict[str, Any]] = []
    try:
        con.execute(f"set temp_directory={sql_string(str(duckdb_temporary))}")
        con.execute(f"set memory_limit={sql_string(MATERIALIZATION_MEMORY_LIMIT)}")
        con.execute("set threads=1")
        con.execute("set preserve_insertion_order=false")
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
        identifier_normalization_rejections = {
            "total": int(
                con.execute(
                    "select count(*) from identifier_normalization_rejections"
                ).fetchone()[0]
            ),
            "by_source_table_field_namespace": [
                {
                    "source_id": str(source_id),
                    "release_id": str(release_id),
                    "source_table": str(source_table),
                    "source_field": str(source_field),
                    "requested_namespace": str(namespace),
                    "count": int(count),
                }
                for source_id, release_id, source_table, source_field, namespace, count in con.execute(
                    "select r.source_id, r.release_id, r.source_table, x.source_field, "
                    "x.requested_namespace, count(*) "
                    "from identifier_normalization_rejections x "
                    "join source_records r using (source_record_id) "
                    "group by all order by all"
                ).fetchall()
            ],
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
        database_blocks_before_hash = database_block_report(con)
        con.execute(f"set memory_limit={sql_string(LOGICAL_HASH_MEMORY_LIMIT)}")
        con.execute(f"set threads={LOGICAL_HASH_THREADS}")
        tables = [table_logical_report(con, table) for table in user_tables(con)]
        database_blocks_after_hash = database_block_report(con)
        if database_blocks_after_hash != database_blocks_before_hash:
            raise ValueError(
                "logical hashing changed persistent database allocation: "
                f"before={database_blocks_before_hash} "
                f"after={database_blocks_after_hash}"
            )
    finally:
        con.close()
        shutil.rmtree(duckdb_temporary, ignore_errors=True)
    logical_content_sha256 = stable_hash(tables)
    report = {
        "schema_version": "spacegate.scientific_evidence_report.v1",
        "build_id": build_id,
        "contract_version": contract["contract_version"],
        "compiler_version": contract["compiler_version"],
        "contract_sha256": contract_sha256,
        "compiler_sha256": compiler_sha256,
        "registry_sha256": registry_sha256,
        "runtime_versions": runtime_versions,
        "input_fingerprint": input_fingerprint,
        "status": "pass" if not mapping_counts.get("declared_pending", 0) else "in_progress",
        "sources": source_reports,
        "mapping_status_counts": mapping_counts,
        "identifier_claim_counts_by_namespace": identifier_claim_counts,
        "identifier_claim_counts_by_scope": identifier_claim_scope_counts,
        "identifier_normalization_rejections": identifier_normalization_rejections,
        "binding_outcome_counts_by_status_and_scope": binding_outcome_counts,
        "lifecycle_claim_counts": lifecycle_claim_counts,
        "relation_claim_counts": relation_claim_counts,
        "citation_summary": citation_summary,
        "logical_content_sha256": logical_content_sha256,
        "logical_hash_algorithm": LOGICAL_HASH_ALGORITHM,
        "materialization_execution": {
            "memory_limit": MATERIALIZATION_MEMORY_LIMIT,
            "threads": 1,
            "preserve_insertion_order": False,
            "astrometry_citation_link_bucket_count": CITATION_LINK_BUCKET_COUNT,
            "process_peak_rss_bytes": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            * 1024,
            "temporary_storage_policy": temporary_storage_policy,
            "temporary_storage_removed": not duckdb_temporary.exists(),
        },
        "logical_hash_execution": {
            "implementation": "ordered_stream_v1",
            "batch_size": LOGICAL_HASH_BATCH_SIZE,
            "memory_limit": LOGICAL_HASH_MEMORY_LIMIT,
            "threads": LOGICAL_HASH_THREADS,
            "database_blocks_before": database_blocks_before_hash,
            "database_blocks_after": database_blocks_after_hash,
        },
        "tables": tables,
        "created_at": created_at,
    }
    manifest = {
        "schema_version": BUILD_CONTRACT,
        "build_id": build_id,
        "input_fingerprint": input_fingerprint,
        "contract_path": str(contract_path.relative_to(ROOT)),
        "contract_sha256": contract_sha256,
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
