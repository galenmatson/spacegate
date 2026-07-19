from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_scientific_evidence as compiler  # noqa: E402
import verify_scientific_evidence_artifact as artifact_audit  # noqa: E402
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
    wide_adapter = contract["source_adapters"][
        "multiplicity.el_badry_2021_wide_binary"
    ]
    assert set(wide_adapter["tables"]) == {
        "el_badry_finder_code",
        "el_badry_neighbor_code",
        "el_badry_shifted_control_catalog",
        "el_badry_wide_binary_catalog",
    }


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

    selected_adapter = json.loads(json.dumps(adapter))
    selected_adapter["tables"]["test_rows"]["row_selection"] = {
        "policy_id": "alpha_only_v1",
        "sql_predicate": "note = 'alpha'",
        "reason": "test selection",
    }
    with duckdb.connect() as con:
        compiler.create_schema(con)
        selected = compiler.materialize_source(
            con, input_row, selected_adapter, contract
        )
        assert con.execute("select count(*) from source_records").fetchone()[0] == 1
    assert selected["input_source_rows"] == 3
    assert selected["source_rows"] == 2
    assert selected["excluded_by_row_selection"] == 1
    assert selected["tables"][0]["row_selection_policy"] == "alpha_only_v1"


def test_reproduction_comparison_uses_logical_content_not_runtime_database_bytes() -> None:
    report = {
        "build_id": "build",
        "contract_version": "contract",
        "compiler_version": "compiler",
        "compiler_sha256": "compiler-sha",
        "registry_sha256": "registry-sha",
        "runtime_versions": {"python": "3.14", "duckdb": "1.4"},
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
        "relation_claim_counts": {
            "by_kind_and_polarity": {},
            "with_strict_probability": 0,
            "with_confidence_statistic": 0,
        },
        "citation_summary": {"citations": 1, "evidence_links": 1},
        "logical_content_sha256": "logical",
        "logical_hash_algorithm": "sha256_bucketed_multiset_v1",
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


def test_bucketed_logical_hash_is_order_independent_and_duplicate_sensitive() -> None:
    with duckdb.connect() as con:
        con.execute("create table first_rows (id integer, value varchar)")
        con.execute("create table second_rows (id integer, value varchar)")
        con.execute("insert into first_rows values (1, 'a'), (2, 'b')")
        con.execute("insert into second_rows values (2, 'b'), (1, 'a')")
        first = compiler.table_logical_report(con, "first_rows")
        second = compiler.table_logical_report(con, "second_rows")
        assert first["logical_sha256"] == second["logical_sha256"]
        assert first["logical_hash_algorithm"] == compiler.LOGICAL_HASH_ALGORITHM
        con.execute("insert into second_rows values (1, 'a')")
        duplicated = compiler.table_logical_report(con, "second_rows")
    assert duplicated["row_count"] == 3
    assert duplicated["logical_sha256"] != first["logical_sha256"]


def test_artifact_audit_rejects_invalid_relation_probability() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        assert artifact_audit.audit_evidence(con)["status"] == "pass"
        con.execute(
            """
            insert into relation_claim_evidence (
              evidence_id, source_record_id,
              left_identity_namespace, left_identity_raw,
              right_identity_namespace, right_identity_raw,
              relation_kind, relation_scope, probability, probability_semantics,
              evidence_polarity
            ) values (
              'evidence', 'record', 'test', 'left', 'test', 'right',
              'candidate_test', 'pair', 2.0, 'strict probability', 'candidate'
            )
            """
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["strict_probabilities_outside_unit_interval"] == 1


def test_refuted_planet_claim_is_negative_evidence() -> None:
    expression = compiler.lifecycle_polarity_expression("disposition")
    with duckdb.connect() as con:
        polarity = con.execute(
            f"select {expression} from (select 'REFUTED' disposition)"
        ).fetchone()[0]
    assert polarity == "negative"


def test_source_field_metadata_falls_back_to_source_native_fits_schema() -> None:
    fields = compiler.source_field_metadata(
        {
            "source_name": "fits_rows",
            "source_schema": {
                "source_schema": [
                    {
                        "name": "source_id1",
                        "arrow_type": "int64",
                        "source_format": "K",
                        "unit": None,
                    },
                    {
                        "name": "sep_AU",
                        "arrow_type": "double",
                        "source_format": "D",
                        "unit": "AU",
                    },
                ]
            },
        },
        {},
    )
    assert fields == [
        {
            "column_name": "source_id1",
            "datatype": "int64",
            "unit": None,
            "ucd": None,
            "description": None,
        },
        {
            "column_name": "sep_AU",
            "datatype": "double",
            "unit": "AU",
            "ucd": None,
            "description": None,
        },
    ]


def test_relation_claim_preserves_non_probability_statistic_and_control_polarity(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "wide_pairs.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "(11::bigint,12::bigint,0.1::double,100::double),"
            "(21::bigint,22::bigint,4.2::double,200::double)) "
            "t(source_id1,source_id2,R_chance_align,sep_AU)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'test.wide', 'r1', 'control', 'control_relation', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [
                [f"record-{index}", row_hash]
                for index, (row_hash,) in enumerate(rows)
            ],
        )
        consumed = compiler.materialize_relation_claims(
            con,
            source_id="test.wide",
            release_id="r1",
            table_name="control",
            path=parquet,
            relation_claim={
                "left_identifier_field": "source_id1",
                "left_identifier_namespace": "gaia_edr3_source_id",
                "right_identifier_field": "source_id2",
                "right_identifier_namespace": "gaia_edr3_source_id",
                "relation_kind": "shifted_control",
                "relation_scope": "stellar_pair_control",
                "evidence_polarity": "negative_control",
                "method": "test_kde",
                "reference_raw": "test reference",
                "confidence_statistic_field": "R_chance_align",
                "confidence_statistic_key": "chance_alignment_density_ratio",
                "confidence_statistic_unit": "dimensionless",
                "confidence_statistic_semantics": "not a strict probability",
                "quality_fields": ["sep_AU", "R_chance_align"],
            },
            available_fields={
                "source_id1",
                "source_id2",
                "R_chance_align",
                "sep_AU",
            },
        )
        evidence = con.execute(
            "select left_identity_raw, right_identity_raw, probability, "
            "confidence_statistic_value, evidence_polarity, quality_json::varchar "
            "from relation_claim_evidence order by left_identity_raw"
        ).fetchall()
    assert consumed == {
        "source_id1",
        "source_id2",
        "R_chance_align",
        "sep_AU",
    }
    assert [(row[0], row[1], row[2], row[3], row[4]) for row in evidence] == [
        ("11", "12", None, 0.1, "negative_control"),
        ("21", "22", None, 4.2, "negative_control"),
    ]
    assert all(json.loads(row[5])["sep_AU"] in {100.0, 200.0} for row in evidence)


def test_scalar_grouping_preserves_measurement_companions() -> None:
    groups = compiler.scalar_field_groups(
        [
            "pl_rade",
            "pl_radeerr1",
            "pl_radeerr2",
            "pl_radelim",
            "pl_radestr",
        ],
        {"pl_rade", "pl_radeerr1", "pl_radeerr2", "pl_radelim", "pl_radestr"},
    )
    assert groups == [
        {
            "base_field": "pl_rade",
            "auxiliary": {
                "error_upper": "pl_radeerr1",
                "error_lower": "pl_radeerr2",
                "limit": "pl_radelim",
                "formatted": "pl_radestr",
            },
        }
    ]


def test_planet_scalar_materialization_preserves_units_errors_bounds_and_reference(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "planet.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            f"('Planet b','Reference A',1.75,0.20,-0.10,1,'< 1.75')) "
            f"t(pl_name,pl_refname,pl_rade,pl_radeerr1,pl_radeerr2,pl_radelim,pl_radestr)) "
            f"to '{parquet}' (format parquet)"
        )
        source_row_sha256 = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.catalog','r1','planet_rows','planet',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_row_sha256],
        )
        fields = [
            {
                "column_name": name,
                "unit": "Rearth" if name.startswith("pl_rade") else None,
                "ucd": None,
                "description": "planet radius",
            }
            for name in (
                "pl_rade",
                "pl_radeerr1",
                "pl_radeerr2",
                "pl_radelim",
                "pl_radestr",
            )
        ]
        table_contract = {
            "planet_parameter_set": {
                "kind": "reference_specific",
                "reference_field": "pl_refname",
            },
            "signal_identifier_fields": ["pl_name"],
        }
        consumed = compiler.materialize_scalar_evidence(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="planet_rows",
            path=parquet,
            destination="planet_parameter_evidence",
            fields=fields,
            available_fields={
                "pl_name",
                "pl_refname",
                "pl_rade",
                "pl_radeerr1",
                "pl_radeerr2",
                "pl_radelim",
                "pl_radestr",
            },
            table_contract=table_contract,
            unit_normalizations={"Rearth": "R_earth"},
        )
        compiler.materialize_parameter_sets(
            con,
            source_id="test.catalog",
            release_id="r1",
            table_name="planet_rows",
            table_contract=table_contract,
        )
        citation_summary = compiler.materialize_citations(con)
        evidence = con.execute(
            "select quantity_key,value_raw,unit_raw,normalized_value,normalized_unit,"
            "uncertainty_lower,uncertainty_upper,bound_semantics,reference_raw,"
            "quality_json->>'formatted_value_raw' from planet_parameter_evidence"
        ).fetchone()
        set_row = con.execute(
            "select parameter_set_kind,reference_raw from planet_parameter_sets"
        ).fetchone()
        citation = con.execute(
            "select source_reference_key,citation_text_raw from citations"
        ).fetchone()
    assert consumed == {
        "pl_rade",
        "pl_radeerr1",
        "pl_radeerr2",
        "pl_radelim",
        "pl_radestr",
    }
    assert evidence == (
        "nasa_exoplanet_archive.pl_rade",
        "1.75",
        "Rearth",
        1.75,
        "R_earth",
        0.1,
        0.2,
        "upper_limit",
        "Reference A",
        "< 1.75",
    )
    assert set_row == ("reference_specific", "Reference A")
    assert citation_summary == {"citations": 1, "evidence_links": 1}
    assert citation == ("Reference A", "Reference A")


def test_nasa_reference_fragment_parser_preserves_lineage_and_parses_ads() -> None:
    raw = (
        "<a refstr=HOLCZER_ET_AL__2016 "
        "href=https://ui.adsabs.harvard.edu/abs/2016ApJS..225....9H/abstract "
        "target=ref>Holczer et al. 2016</a>"
    )
    assert compiler.parse_reference_fragment(raw) == {
        "reference_key": "HOLCZER_ET_AL__2016",
        "display_text": "Holczer et al. 2016",
        "url": "https://ui.adsabs.harvard.edu/abs/2016ApJS..225....9H/abstract",
        "bibcode": "2016ApJS..225....9H",
        "doi": None,
        "publication_year": 2016,
    }
