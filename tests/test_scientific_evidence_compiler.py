from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_scientific_evidence as compiler  # noqa: E402
import verify_scientific_evidence_reproduction as reproduction  # noqa: E402


CONTRACT_PATH = ROOT / "config" / "evidence_lake" / "e4_scientific_evidence.json"


def test_checked_in_scientific_evidence_contract_is_complete_and_valid() -> None:
    contract = compiler.load_json(CONTRACT_PATH)
    assert compiler.validate_contract(contract) == []
    assert set(contract["domain_tables"]) == compiler.DOMAIN_TABLES
    nasa_adapter = contract["source_adapters"][
        "nasa_exoplanet_archive.planetary_systems"
    ]
    assert len(nasa_adapter["tables"]) == 12


def test_scientific_evidence_schema_has_bounded_domain_tables() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        tables = set(compiler.user_tables(con))
    assert compiler.DOMAIN_TABLES <= tables
    assert {
        "evidence_build",
        "evidence_sources",
        "source_records",
        "source_field_dispositions",
        "object_binding_outcomes",
    } <= tables


def test_source_record_compilation_is_deterministic_and_accounts_duplicates(
    tmp_path: Path,
) -> None:
    raw_path = tmp_path / "raw"
    typed_path = tmp_path / "typed"
    artifact_path = raw_path / "artifacts" / "test_rows"
    tables_path = typed_path / "tables"
    artifact_path.mkdir(parents=True)
    tables_path.mkdir(parents=True)
    (artifact_path / "product_manifest.json").write_text(
        json.dumps(
            {
                "field_dispositions": [
                    {
                        "column_name": "source_id",
                        "datatype": "long",
                        "unit": None,
                        "ucd": "meta.id",
                        "description": "source identifier",
                    },
                    {
                        "column_name": "note",
                        "datatype": "char",
                        "unit": None,
                        "ucd": None,
                        "description": "source context",
                    },
                    {
                        "column_name": "disposition",
                        "datatype": "char",
                        "unit": None,
                        "ucd": None,
                        "description": "candidate disposition",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    parquet = tables_path / "test_rows.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            f"('1','alpha','PC'),('1','alpha','PC'),('1','beta','FP')) "
            f"t(source_id,note,disposition)) to '{parquet}' "
            f"(format parquet, compression zstd)"
        )

    input_row = {
        "source_id": "test.catalog",
        "release_id": "r1",
        "raw_path": raw_path,
        "raw_manifest": {
            "snapshot_id": "raw1",
            "content_sha256": "raw-content",
            "artifacts": [
                {
                    "source_name": "test_rows",
                    "artifact_path": "artifacts/test_rows",
                    "tree_sha256": "raw-tree",
                    "retrieved_at": "2026-07-19T00:00:00Z",
                }
            ],
        },
        "typed_path": typed_path,
        "typed_manifest": {
            "typed_snapshot_id": "typed1",
            "content_sha256": "typed-content",
            "tables": [
                {
                    "source_name": "test_rows",
                    "status": "typed",
                    "parquet_path": "tables/test_rows.parquet",
                    "row_count": 3,
                    "sha256": "typed-table",
                }
            ],
        },
    }
    adapter = {
        "adapter_version": "test_adapter_v1",
        "tables": {
            "test_rows": {
                "logical_key_fields": ["source_id"],
                "object_scope": "object",
                "field_profile": "test",
                "lifecycle_claims": [
                    {
                        "claim_role": "test_disposition",
                        "identifier_field": "source_id",
                        "disposition_field": "disposition",
                        "context_fields": ["note"],
                    }
                ],
            }
        },
    }
    contract = {
        "identifier_claims": {
            "source_id": {"namespace": "test_id", "claim_scope": "host"}
        },
        "field_profiles": {
            "test": [
                {
                    "pattern": "source_id",
                    "disposition": "identity",
                    "destination": "identifier_claim_evidence",
                    "reason": "source identity",
                },
                {
                    "pattern": "disposition",
                    "disposition": "domain",
                    "destination": "planet_lifecycle_evidence",
                    "reason": "candidate lifecycle",
                },
                {
                    "pattern": "note",
                    "disposition": "context",
                    "destination": "source_records",
                    "reason": "source context",
                },
            ]
        }
    }

    snapshots = []
    for _ in range(2):
        with duckdb.connect() as con:
            compiler.create_schema(con)
            report = compiler.materialize_source(
                con, input_row, adapter, contract
            )
            records = con.execute(
                "select source_record_id, source_row_sha256, source_duplicate_count, "
                "source_context_json::varchar from source_records order by source_record_id"
            ).fetchall()
            dispositions = con.execute(
                "select source_field, mapping_status from source_field_dispositions "
                "order by source_field"
            ).fetchall()
            binding_count = con.execute(
                "select count(*) from object_binding_outcomes"
            ).fetchone()[0]
            binding_scopes = con.execute(
                "select distinct binding_scope from object_binding_outcomes order by 1"
            ).fetchall()
            identifier_claims = con.execute(
                "select identifier_normalized from identifier_claim_evidence "
                "order by evidence_id"
            ).fetchall()
            lifecycle = con.execute(
                "select disposition_normalized, evidence_polarity "
                "from planet_lifecycle_evidence order by disposition_normalized"
            ).fetchall()
        snapshots.append(records)
        assert report["source_rows"] == 3
        assert report["source_records"] == 2
        assert report["exact_duplicate_rows"] == 1
        assert sorted(row[2] for row in records) == [1, 2]
        assert {json.loads(row[3])["note"] for row in records} == {"alpha", "beta"}
        assert dispositions == [
            ("disposition", "materialized"),
            ("note", "materialized"),
            ("source_id", "materialized"),
        ]
        assert identifier_claims == [("1",), ("1",)]
        assert lifecycle == [
            ("CANDIDATE", "candidate"),
            ("FALSE_POSITIVE", "negative"),
        ]
        assert binding_count == 4
        assert binding_scopes == [("host",), ("object",)]
    assert snapshots[0] == snapshots[1]


def test_reproduction_comparison_uses_logical_content_not_runtime_database_bytes() -> None:
    report = {
        "build_id": "build",
        "contract_version": "contract",
        "compiler_version": "compiler",
        "input_fingerprint": "inputs",
        "status": "in_progress",
        "sources": [],
        "mapping_status_counts": {"declared_pending": 1},
        "identifier_claim_counts_by_namespace": {"test_id": 1},
        "identifier_claim_counts_by_scope": {"host": 1},
        "binding_outcome_counts_by_status_and_scope": {
            "unresolved": {"host": 1, "object": 1}
        },
        "lifecycle_claim_counts": {
            "by_disposition": {"CANDIDATE": 1},
            "by_polarity": {"candidate": 1},
        },
        "logical_content_sha256": "logical",
        "tables": [{"table": "source_records", "row_count": 1, "logical_sha256": "a"}],
        "created_at": "2026-07-19T00:00:00Z",
        "database_sha256": "runtime-only-a",
    }
    reproduced = dict(report)
    reproduced["database_sha256"] = "runtime-only-b"
    assert reproduction.compare_reports(report, reproduced) == []
    reproduced["logical_content_sha256"] = "changed"
    assert reproduction.compare_reports(report, reproduced) == [
        "logical_content_sha256"
    ]


def test_refuted_planet_claim_is_negative_evidence() -> None:
    expression = compiler.lifecycle_polarity_expression("disposition")
    with duckdb.connect() as con:
        polarity = con.execute(
            f"select {expression} from (select 'REFUTED' disposition)"
        ).fetchone()[0]
    assert polarity == "negative"
