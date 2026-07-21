#!/usr/bin/env python3
"""Deterministic schema and SQL helpers for coherent scientific parameter sets."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


SCHEMA_VERSION = "spacegate.coherent_parameter_set_schema.v1"
MASKED_VECTOR_ENCODING = "source_masked_numeric_vector_to_nullable_double_array_v1"


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def nullable_sql_string(value: Any) -> str:
    return "null::varchar" if value is None else sql_string(str(value))


def source_relation(path: Any) -> str:
    text = str(path)
    prefix = "duckdb-table:"
    if text.startswith(prefix):
        return sql_identifier(text[len(prefix) :])
    return f"read_parquet({sql_string(text)})"


def resolve_masked_vectors(
    value_fields: list[str],
    rules: list[dict[str, Any]] | None,
    *,
    expected_count: int | None = None,
) -> dict[str, dict[str, Any]]:
    resolved: dict[str, dict[str, Any]] = {}
    for index, rule in enumerate(rules or []):
        try:
            pattern = re.compile(str(rule.get("pattern") or ""))
        except re.error as error:
            raise ValueError(f"invalid masked-vector pattern at index {index}: {error}") from error
        length_field = str(rule.get("length_field") or "")
        if length_field not in value_fields:
            raise ValueError(
                f"masked-vector cardinality field is absent from value fields: {length_field}"
            )
        mask_token = str(rule.get("mask_token") or "")
        if not mask_token:
            raise ValueError(f"masked-vector mask token is empty at index {index}")
        for field in value_fields:
            if pattern.fullmatch(field) is None:
                continue
            if field in resolved:
                raise ValueError(f"masked-vector field matches multiple rules: {field}")
            resolved[field] = {
                "length_field": length_field,
                "mask_token": mask_token,
                "value_type": str(rule.get("value_type") or "double"),
            }
    if expected_count is not None and len(resolved) != expected_count:
        raise ValueError(
            f"masked-vector field count mismatch: expected={expected_count} actual={len(resolved)}"
        )
    return resolved


def build_parameter_schema(
    *,
    source_id: str,
    release_id: str,
    source_table: str,
    destination: str,
    parameter_set_kind: str,
    value_fields: list[str],
    metadata_by_field: dict[str, dict[str, Any]],
    masked_vectors: dict[str, dict[str, Any]] | None = None,
) -> tuple[str, str]:
    vectors = masked_vectors or {}
    if not value_fields or len(value_fields) != len(set(value_fields)):
        raise ValueError("coherent parameter-set value fields must be non-empty and unique")
    missing = sorted(set(value_fields) - set(metadata_by_field))
    if missing:
        raise ValueError(f"coherent parameter-set metadata missing fields: {missing}")
    unknown_vectors = sorted(set(vectors) - set(value_fields))
    if unknown_vectors:
        raise ValueError(f"masked-vector fields are absent from value fields: {unknown_vectors}")

    fields: list[dict[str, Any]] = []
    for position, field in enumerate(value_fields):
        source = metadata_by_field[field]
        row = {
            "position": position,
            "name": field,
            "source_column_name": source.get("source_column_name") or field,
            "datatype": source.get("datatype"),
            "unit": source.get("unit"),
            "ucd": source.get("ucd"),
            "description": source.get("description"),
            "encoding": "source_native_typed_scalar_v1",
        }
        if field in vectors:
            vector = vectors[field]
            if vector.get("value_type", "double") != "double":
                raise ValueError(f"unsupported masked-vector value type for {field}")
            length_field = str(vector.get("length_field") or "")
            if length_field not in value_fields:
                raise ValueError(
                    f"masked-vector cardinality field is absent from value fields: {field}"
                )
            mask_token = str(vector.get("mask_token") or "")
            if not mask_token:
                raise ValueError(f"masked-vector mask token is empty: {field}")
            row.update(
                {
                    "encoding": MASKED_VECTOR_ENCODING,
                    "source_mask_token": mask_token,
                    "cardinality_field": length_field,
                    "normalized_datatype": "DOUBLE[]",
                }
            )
        fields.append(row)

    schema = {
        "schema_version": SCHEMA_VERSION,
        "source_id": source_id,
        "release_id": release_id,
        "source_table": source_table,
        "destination": destination,
        "parameter_set_kind": parameter_set_kind,
        "value_encoding": "ordered_json_array_v1",
        "fields": fields,
    }
    schema_json = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    schema_id = hashlib.sha256(
        ("coherent-parameter-schema|" + schema_json).encode("utf-8")
    ).hexdigest()
    return schema_id, schema_json


def masked_vector_sql(field: str, config: dict[str, Any]) -> str:
    if config.get("value_type", "double") != "double":
        raise ValueError(f"unsupported masked-vector value type for {field}")
    mask_token = str(config.get("mask_token") or "")
    if not mask_token:
        raise ValueError(f"masked-vector mask token is empty: {field}")
    source = f"cast(source_row.{sql_identifier(field)} as varchar)"
    content = f"trim(both '[]' from {source})"
    escaped_mask = mask_token.replace("'", "''")
    return (
        f"case when source_row.{sql_identifier(field)} is null then null "
        f"when {content}='' then []::double[] else "
        f"list_transform(regexp_split_to_array({content}, '\\s+'), "
        f"token -> case when token='{escaped_mask}' then null "
        f"else cast(token as double) end) end"
    )


def parameter_values_sql(
    value_fields: list[str],
    masked_vectors: dict[str, dict[str, Any]] | None = None,
) -> str:
    vectors = masked_vectors or {}
    unknown_vectors = sorted(set(vectors) - set(value_fields))
    if unknown_vectors:
        raise ValueError(f"masked-vector fields are absent from value fields: {unknown_vectors}")
    expressions = [
        masked_vector_sql(field, vectors[field])
        if field in vectors
        else f"source_row.{sql_identifier(field)}"
        for field in value_fields
    ]
    return "json_array(" + ", ".join(expressions) + ")"


def json_object_sql(fields: list[str]) -> str:
    if not fields:
        return "'{}'::json"
    arguments = ", ".join(
        f"{sql_string(field)}, source_row.{sql_identifier(field)}" for field in fields
    )
    return f"json_object({arguments})"


def materialize_coherent_parameter_set(
    con: Any,
    *,
    source_id: str,
    release_id: str,
    source_table: str,
    path: Any,
    destination_fields: list[dict[str, Any]],
    available_fields: set[str],
    config: dict[str, Any],
) -> set[str]:
    destination = str(config["destination"])
    kind = str(config["parameter_set_kind"])
    value_fields = [str(field["column_name"]) for field in destination_fields]
    if not value_fields:
        raise ValueError(f"coherent parameter set has no destination fields: {source_table}")
    vector_rules = list(config.get("masked_vector_rules") or [])
    vectors = resolve_masked_vectors(
        value_fields,
        vector_rules,
        expected_count=config.get("expected_masked_vector_count"),
    )
    metadata_by_field = {
        str(field["column_name"]): field for field in destination_fields
    }
    schema_id, schema_json = build_parameter_schema(
        source_id=source_id,
        release_id=release_id,
        source_table=source_table,
        destination=destination,
        parameter_set_kind=kind,
        value_fields=value_fields,
        metadata_by_field=metadata_by_field,
        masked_vectors=vectors,
    )
    extra_fields = {
        str(field)
        for field in (
            config.get("epoch_field"),
            config.get("reference_field"),
            *(config.get("quality_fields") or []),
        )
        if field
    }
    missing = sorted((set(value_fields) | extra_fields) - available_fields)
    if missing:
        raise ValueError(f"coherent parameter-set fields missing from {source_table}: {missing}")
    predicate = str(config.get("sql_predicate") or "true")
    if not predicate.strip():
        raise ValueError(f"coherent parameter-set predicate is empty: {source_table}")

    if vectors:
        mismatch_terms = []
        for field, vector in sorted(vectors.items()):
            length = f"source_row.{sql_identifier(str(vector['length_field']))}"
            parsed = masked_vector_sql(field, vector)
            mismatch_terms.append(
                "count(*) filter (where "
                f"source_row.{sql_identifier(field)} is not null and "
                f"array_length({parsed}) is distinct from cast({length} as bigint))"
            )
        mismatch_count = int(
            con.execute(
                "select " + " + ".join(mismatch_terms) + " from "
                + source_relation(path)
                + " source_row where "
                + predicate
            ).fetchone()[0]
        )
        if mismatch_count:
            raise ValueError(
                f"coherent parameter-set vector cardinality mismatch in "
                f"{source_table}: {mismatch_count}"
            )

    con.execute(
        "insert into coherent_parameter_set_schemas values (?, ?, ?, ?, ?, ?, ?)",
        [
            schema_id,
            source_id,
            release_id,
            source_table,
            destination,
            kind,
            schema_json,
        ],
    )

    epoch = (
        f"cast(source_row.{sql_identifier(str(config['epoch_field']))} as varchar)"
        if config.get("epoch_field")
        else nullable_sql_string(config.get("epoch_raw"))
    )
    reference = (
        f"cast(source_row.{sql_identifier(str(config['reference_field']))} as varchar)"
        if config.get("reference_field")
        else nullable_sql_string(config.get("reference_raw"))
    )
    quality = json_object_sql([str(field) for field in config.get("quality_fields") or []])
    values = parameter_values_sql(value_fields, vectors)
    con.execute(
        f"""
        insert into {sql_identifier(destination)}
        select
          sha256({sql_string('coherent-parameter-set|' + destination + '|' + kind + '|')}
                 || r.source_record_id),
          {sql_string(schema_id)}, r.source_record_id,
          {nullable_sql_string(config.get('component_scope'))},
          {sql_string(kind)}, {values}, {epoch},
          {sql_string(str(config['method']))},
          {nullable_sql_string(config.get('model'))},
          {reference}, {quality},
          {sql_string(str(config['normalization_version']))}
        from {source_relation(path)} source_row
        join source_records r
          on r.source_id={sql_string(source_id)}
         and r.release_id={sql_string(release_id)}
         and r.source_table={sql_string(source_table)}
         and r.source_row_sha256=sha256(to_json(source_row))
        where {predicate}
        """
    )
    return set(value_fields) | extra_fields
