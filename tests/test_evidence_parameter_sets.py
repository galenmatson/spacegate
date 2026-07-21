from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from evidence_parameter_sets import (  # noqa: E402
    MASKED_VECTOR_ENCODING,
    build_parameter_schema,
    materialize_coherent_parameter_set,
    parameter_values_sql,
    resolve_masked_vectors,
)


def test_parameter_schema_is_ordered_typed_and_deterministic() -> None:
    metadata = {
        "num_segments": {"datatype": "short", "description": "Segment count"},
        "segments_period": {
            "datatype": "char",
            "unit": "d",
            "ucd": "time.period",
            "description": "Masked periods",
        },
    }
    arguments = {
        "source_id": "gaia.dr3.variability",
        "release_id": "dr3",
        "source_table": "rotation",
        "destination": "variability_activity_rotation_parameter_sets",
        "parameter_set_kind": "gaia_rotation_solution",
        "value_fields": ["num_segments", "segments_period"],
        "metadata_by_field": metadata,
        "masked_vectors": {
            "segments_period": {
                "length_field": "num_segments",
                "mask_token": "--",
                "value_type": "double",
            }
        },
    }
    first_id, first_json = build_parameter_schema(**arguments)
    second_id, second_json = build_parameter_schema(**arguments)
    assert (first_id, first_json) == (second_id, second_json)
    schema = json.loads(first_json)
    assert [field["position"] for field in schema["fields"]] == [0, 1]
    assert schema["fields"][1]["encoding"] == MASKED_VECTOR_ENCODING
    assert schema["fields"][1]["cardinality_field"] == "num_segments"
    assert schema["fields"][1]["normalized_datatype"] == "DOUBLE[]"


def test_parameter_values_sql_preserves_masks_and_whitespace() -> None:
    expression = parameter_values_sql(
        ["num_segments", "segments_period"],
        {
            "segments_period": {
                "length_field": "num_segments",
                "mask_token": "--",
                "value_type": "double",
            }
        },
    )
    con = duckdb.connect()
    con.execute("create table source_row(num_segments integer, segments_period varchar)")
    con.execute("insert into source_row values (?, ?)", [3, "[1.0\n --   2.5]"])
    value = con.execute(f"select {expression} from source_row").fetchone()[0]
    assert json.loads(value) == [3, [1.0, None, 2.5]]


def test_parameter_values_sql_fails_closed_on_malformed_vector_tokens() -> None:
    expression = parameter_values_sql(
        ["segments_period"],
        {
            "segments_period": {
                "length_field": "segments_period",
                "mask_token": "--",
            }
        },
    )
    con = duckdb.connect()
    with pytest.raises(duckdb.ConversionException):
        con.execute(
            f"select {expression} from (values ('[1.0 invalid]')) "
            "source_row(segments_period)"
        ).fetchone()


def test_masked_vector_rules_are_exhaustive_and_non_overlapping() -> None:
    fields = ["num_segments", "num_outliers", "segments_period", "outliers_time"]
    resolved = resolve_masked_vectors(
        fields,
        [
            {"pattern": "^segments_.*$", "length_field": "num_segments", "mask_token": "--"},
            {"pattern": "^outliers_time$", "length_field": "num_outliers", "mask_token": "--"},
        ],
        expected_count=2,
    )
    assert resolved["segments_period"]["length_field"] == "num_segments"
    assert resolved["outliers_time"]["length_field"] == "num_outliers"
    with pytest.raises(ValueError, match="multiple rules"):
        resolve_masked_vectors(
            fields,
            [
                {"pattern": ".*", "length_field": "num_segments", "mask_token": "--"},
                {"pattern": "^segments_.*$", "length_field": "num_segments", "mask_token": "--"},
            ],
        )


def test_materialize_coherent_parameter_set_links_schema_and_source_rows(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "rotation.parquet"
    con = duckdb.connect()
    con.execute(
        "create table input(source_id bigint, num_segments integer, "
        "segments_period varchar, quality_flag varchar)"
    )
    con.executemany(
        "insert into input values (?, ?, ?, ?)",
        [(1, 3, "[1.0 -- 2.5]", "A"), (2, 0, None, "B")],
    )
    con.execute("copy input to ? (format parquet)", [str(parquet)])
    source_hashes = con.execute(
        "select source_id, sha256(to_json(input)) from input order by source_id"
    ).fetchall()
    con.execute(
        "create table source_records(source_record_id varchar, source_id varchar, "
        "release_id varchar, source_table varchar, source_row_sha256 varchar)"
    )
    con.executemany(
        "insert into source_records values (?, 'test.source', 'r1', 'rotation', ?)",
        [(f"record-{source_id}", row_hash) for source_id, row_hash in source_hashes],
    )
    con.execute(
        "create table coherent_parameter_set_schemas("
        "parameter_schema_id varchar, source_id varchar, release_id varchar, "
        "source_table varchar, destination varchar, parameter_set_kind varchar, "
        "schema_json json)"
    )
    con.execute(
        "create table variability_activity_rotation_parameter_sets("
        "evidence_id varchar, parameter_schema_id varchar, "
        "source_record_id varchar, component_scope varchar, parameter_set_kind varchar, "
        "values_json json, epoch_raw varchar, method varchar, model varchar, "
        "reference_raw varchar, quality_json json, normalization_version varchar)"
    )
    consumed = materialize_coherent_parameter_set(
        con,
        source_id="test.source",
        release_id="r1",
        source_table="rotation",
        path=parquet,
        destination_fields=[
            {"column_name": "num_segments", "datatype": "int"},
            {"column_name": "segments_period", "datatype": "char", "unit": "d"},
        ],
        available_fields={"source_id", "num_segments", "segments_period", "quality_flag"},
        config={
            "destination": "variability_activity_rotation_parameter_sets",
            "parameter_set_kind": "test_rotation_solution",
            "masked_vector_rules": [
                {
                    "pattern": "^segments_.*$",
                    "length_field": "num_segments",
                    "mask_token": "--",
                }
            ],
            "expected_masked_vector_count": 1,
            "quality_fields": ["quality_flag"],
            "method": "test_method",
            "reference_raw": "test_reference",
            "normalization_version": "test_v1",
        },
    )
    assert consumed == {"num_segments", "segments_period", "quality_flag"}
    rows = con.execute(
        "select source_record_id, values_json, quality_json "
        "from variability_activity_rotation_parameter_sets order by source_record_id"
    ).fetchall()
    assert [(row[0], json.loads(row[1]), json.loads(row[2])) for row in rows] == [
        ("record-1", [3, [1.0, None, 2.5]], {"quality_flag": "A"}),
        ("record-2", [0, None], {"quality_flag": "B"}),
    ]
    schema = con.execute(
        "select parameter_schema_id, schema_json from coherent_parameter_set_schemas"
    ).fetchone()
    assert con.execute(
        "select count(*) from variability_activity_rotation_parameter_sets "
        "where parameter_schema_id=?",
        [schema[0]],
    ).fetchone()[0] == 2
    assert json.loads(schema[1])["value_encoding"] == "ordered_json_array_v1"


def test_materialize_coherent_parameter_set_rejects_vector_length_mismatch(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "bad.parquet"
    con = duckdb.connect()
    con.execute(
        "copy (select 2 num_segments, '[1.0]' segments_period) to ? (format parquet)",
        [str(parquet)],
    )
    row_hash = con.execute(
        "select sha256(to_json(source_row)) from read_parquet(?) source_row",
        [str(parquet)],
    ).fetchone()[0]
    con.execute(
        "create table source_records(source_record_id varchar, source_id varchar, "
        "release_id varchar, source_table varchar, source_row_sha256 varchar)"
    )
    con.execute(
        "insert into source_records values ('record', 'test.source', 'r1', 'bad', ?)",
        [row_hash],
    )
    con.execute(
        "create table coherent_parameter_set_schemas("
        "parameter_schema_id varchar, source_id varchar, release_id varchar, "
        "source_table varchar, destination varchar, parameter_set_kind varchar, "
        "schema_json json)"
    )
    con.execute(
        "create table variability_activity_rotation_parameter_sets("
        "evidence_id varchar, parameter_schema_id varchar, "
        "source_record_id varchar, component_scope varchar, parameter_set_kind varchar, "
        "values_json json, epoch_raw varchar, method varchar, model varchar, "
        "reference_raw varchar, quality_json json, normalization_version varchar)"
    )
    with pytest.raises(ValueError, match="vector cardinality mismatch"):
        materialize_coherent_parameter_set(
            con,
            source_id="test.source",
            release_id="r1",
            source_table="bad",
            path=parquet,
            destination_fields=[
                {"column_name": "num_segments"},
                {"column_name": "segments_period"},
            ],
            available_fields={"num_segments", "segments_period"},
            config={
                "destination": "variability_activity_rotation_parameter_sets",
                "parameter_set_kind": "bad",
                "masked_vector_rules": [
                    {
                        "pattern": "^segments_period$",
                        "length_field": "num_segments",
                        "mask_token": "--",
                    }
                ],
                "expected_masked_vector_count": 1,
                "method": "test",
                "normalization_version": "test",
            },
        )
