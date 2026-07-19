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
    magnetar_adapter = contract["source_adapters"]["compact.mcgill_magnetar"]
    assert len(magnetar_adapter["tables"]["TabO1"]["compact_object_parameter_sets"]) == 5
    assert set(contract["source_adapters"]["multiplicity.sb9"]["tables"]) == {
        "sb9_readme",
        "sb9_main",
        "sb9_alias",
        "sb9_orbits",
    }
    sbx_adapter = contract["source_adapters"]["multiplicity.sbx"]
    assert set(sbx_adapter["tables"]) == {
        "sbx_systems",
        "sbx_alias",
        "sbx_configurations",
        "sbx_orbits",
    }
    assert sbx_adapter["tables"]["sbx_orbits"]["orbital_solution"][
        "relation_link"
    ]["required"] is True


def test_contract_table_order_must_cover_each_table_exactly_once() -> None:
    contract = compiler.load_json(CONTRACT_PATH)
    adapter = contract["source_adapters"]["compact.atnf"]
    original = list(adapter["table_order"])

    adapter["table_order"] = [*original, original[0]]
    errors = compiler.validate_contract(contract)
    assert "compact.atnf.table_order contains duplicates" in errors

    adapter["table_order"] = original[:-1]
    errors = compiler.validate_contract(contract)
    assert "compact.atnf.table_order must cover every table exactly" in errors


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
                    "source_name": "raw_rows",
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
                "raw_artifact_name": "raw_rows",
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
              left_identity_namespace, left_identity_raw, left_component_scope,
              right_identity_namespace, right_identity_raw, right_component_scope,
              relation_kind, relation_scope, probability, probability_semantics,
              evidence_polarity
            ) values (
              'evidence', 'record', 'test', 'left', 'left', 'test', 'right', 'right',
              'candidate_test', 'pair', 2.0, 'strict probability', 'candidate'
            )
            """
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["strict_probabilities_outside_unit_interval"] == 1


def test_artifact_audit_rejects_empty_or_orphaned_orbital_solutions() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into orbital_solution_evidence "
            "(evidence_id,source_record_id,relation_claim_id,solution_key) "
            "values ('orbit','record','missing-relation','solution')"
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["empty_orbital_solution_parameter_sets"] == 1
    assert report["checks"]["orphan_orbital_solution_relations"] == 1


def test_artifact_audit_rejects_extended_object_without_geometry() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into extended_object_evidence "
            "(evidence_id,source_record_id,extended_kind) "
            "values ('extended','record','test')"
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["empty_extended_object_geometry"] == 1


def test_artifact_audit_rejects_compact_object_without_parameters() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into compact_object_evidence "
            "(evidence_id,source_record_id,compact_kind) "
            "values ('compact','record','test')"
        )
        report = artifact_audit.audit_evidence(con)
    assert report["status"] == "fail"
    assert report["checks"]["empty_compact_object_parameter_sets"] == 1


def test_logical_key_expression_qualifies_source_id_against_lineage_alias() -> None:
    expression = compiler.logical_key_expression(["source_id"], "t")
    with duckdb.connect() as con:
        value = con.execute(
            f"select {expression} from (values ('catalog-id')) t(source_id) "
            "cross join (values ('lineage-id')) r(source_id)"
        ).fetchone()[0]
    assert json.loads(value) == {"source_id": "catalog-id"}


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
    fields_with_lineage = compiler.source_field_metadata(
        {
            "source_name": "source_rows",
            "source_schema": {"source_schema": [{"name": "value", "unit": "K"}]},
            "columns": [
                {"name": "source_line_number", "type": "BIGINT"},
                {"name": "value", "type": "VARCHAR"},
                {"name": "raw_row", "type": "VARCHAR"},
            ],
        },
        {},
    )
    assert [field["column_name"] for field in fields_with_lineage] == [
        "source_line_number",
        "value",
        "raw_row",
    ]
    assert fields_with_lineage[1]["unit"] == "K"


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
                "left_component_scope": "left",
                "right_identifier_field": "source_id2",
                "right_identifier_namespace": "gaia_edr3_source_id",
                "right_component_scope": "right",
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


def test_relation_claim_predicate_retains_source_rows_but_bounds_claims(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "hierarchy.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('1','2'),('2',null)) t(sn,parent)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t order by sn"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'test.hierarchy', 'r1', 'configurations', 'hierarchy_claim', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [[f"record-{index}", row_hash] for index, (row_hash,) in enumerate(rows)],
        )
        compiler.materialize_relation_claims(
            con,
            source_id="test.hierarchy",
            release_id="r1",
            table_name="configurations",
            path=parquet,
            relation_claim={
                "left_identifier_field": "sn",
                "left_identifier_namespace": "source_component",
                "left_component_scope": "subsystem",
                "right_identifier_field": "parent",
                "right_identifier_namespace": "source_component",
                "right_component_scope": "parent_subsystem",
                "relation_kind": "hierarchical_parent",
                "relation_scope": "source_configuration",
                "evidence_polarity": "positive",
                "method": "source_configuration",
                "reference_raw": "source reference",
                "sql_predicate": "parent is not null",
            },
            available_fields={"sn", "parent"},
        )
        assert con.execute("select count(*) from source_records").fetchone()[0] == 2
        assert con.execute(
            "select left_identity_raw,right_identity_raw from relation_claim_evidence"
        ).fetchall() == [("1", "2")]


def test_relation_audit_accepts_source_native_primary_secondary_scopes() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record', 'multiplicity.test', 'r1', 'systems', 'binary', "
            "'{}', '{}', 'row-hash', 1, 'raw', 'typed', 'raw-tree', "
            "'typed-table', timestamp '2026-07-19 00:00:00')"
        )
        con.execute(
            "insert into relation_claim_evidence ("
            "evidence_id,source_record_id,left_identity_namespace,left_identity_raw,"
            "left_component_scope,right_identity_namespace,right_identity_raw,"
            "right_component_scope,relation_kind,relation_scope,evidence_polarity) "
            "values ('relation','record','component','system:primary','primary',"
            "'component','system:secondary','secondary','binary','pair','positive')"
        )
        con.execute(
            "insert into identifier_claim_evidence "
            "(evidence_id,source_record_id,namespace,identifier_raw,"
            "identifier_normalized,claim_scope,component_scope) values "
            "('primary-claim','record','component','system:primary',"
            "'system:primary','star','primary'),"
            "('secondary-claim','record','component','system:secondary',"
            "'system:secondary','star','secondary')"
        )
        con.execute(
            "insert into object_binding_outcomes "
            "(binding_outcome_id,source_record_id,binding_status,binding_scope,"
            "component_scope,reason,provenance_json) values "
            "('primary-binding','record','unresolved','star','primary',"
            "'test unresolved','{}'),"
            "('secondary-binding','record','unresolved','star','secondary',"
            "'test unresolved','{}')"
        )
        report = artifact_audit.audit_evidence(con)
    assert report["checks"]["relation_endpoints_without_identifier_claims"] == 0
    assert report["checks"]["relation_endpoints_without_binding_scopes"] == 0
    assert report["status"] == "pass"


def test_orbital_solution_preserves_one_coherent_source_parameter_set(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "orbit.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select '00021-6817' wds_id, 'I 699AB' pair_designation, "
            "'290.0' period_raw, '1884.54' epoch_raw, 'Zir2013d' reference_code, "
            "'2' grade_raw) "
            f"to '{parquet}' (format parquet)"
        )
        source_row_sha256 = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.orbits','r1','orbits','orbital_solution',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_row_sha256],
        )
        consumed = compiler.materialize_orbital_solutions(
            con,
            source_id="test.orbits",
            release_id="r1",
            table_name="orbits",
            path=parquet,
            fields=[
                {"column_name": "period_raw"},
                {"column_name": "epoch_raw"},
                {"column_name": "reference_code"},
                {"column_name": "grade_raw"},
            ],
            orbital_solution={
                "solution_key_fields": [
                    "wds_id",
                    "pair_designation",
                    "reference_code",
                ],
                "parameter_fields": ["period_raw", "epoch_raw"],
                "quality_fields": ["grade_raw"],
                "epoch_field": "epoch_raw",
                "reference_field": "reference_code",
                "method": "published_visual_orbit_solution",
                "normalization_version": "source_native_v1",
            },
            available_fields={
                "wds_id",
                "pair_designation",
                "period_raw",
                "epoch_raw",
                "reference_code",
                "grade_raw",
            },
        )
        row = con.execute(
            "select relation_claim_id,solution_key,parameter_set_raw::varchar,"
            "epoch_raw,reference_raw,quality_json::varchar "
            "from orbital_solution_evidence"
        ).fetchone()
    assert consumed == {
        "wds_id",
        "pair_designation",
        "period_raw",
        "epoch_raw",
        "reference_code",
        "grade_raw",
    }
    assert row[0] is None
    assert json.loads(row[1]) == {
        "wds_id": "00021-6817",
        "pair_designation": "I 699AB",
        "reference_code": "Zir2013d",
    }
    assert json.loads(row[2]) == {"period_raw": "290.0", "epoch_raw": "1884.54"}
    assert row[3:5] == ("1884.54", "Zir2013d")
    assert json.loads(row[5]) == {"grade_raw": "2"}


def test_orbital_solution_links_exactly_one_relation_by_source_logical_key(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "orbits.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('1','2','5.0','reference')) "
            "t(Seq,o,Per,Ref)) "
            f"to '{parquet}' (format parquet)"
        )
        orbit_hash = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('system-record','multiplicity.test','r1','systems','binary',"
            "'{\"Seq\":\"1\"}','{}','system-hash',1,'raw','typed',"
            "'raw-tree','typed-table',timestamp '2026-07-19 00:00:00'),"
            "('orbit-record','multiplicity.test','r1','orbits','orbit',"
            "'{\"Seq\":\"1\",\"o\":\"2\"}','{}',?,1,'raw','typed',"
            "'raw-tree','typed-table',timestamp '2026-07-19 00:00:00')",
            [orbit_hash],
        )
        con.execute(
            "insert into relation_claim_evidence ("
            "evidence_id,source_record_id,left_identity_namespace,left_identity_raw,"
            "left_component_scope,right_identity_namespace,right_identity_raw,"
            "right_component_scope,relation_kind,relation_scope,evidence_polarity) "
            "values ('relation','system-record','component','system:primary','primary',"
            "'component','system:secondary','secondary','binary','pair','positive')"
        )
        compiler.materialize_orbital_solutions(
            con,
            source_id="multiplicity.test",
            release_id="r1",
            table_name="orbits",
            path=parquet,
            fields=[{"column_name": "Per"}, {"column_name": "Ref"}],
            orbital_solution={
                "solution_key_fields": ["Seq", "o"],
                "parameter_fields": ["Per"],
                "quality_fields": ["o"],
                "reference_field": "Ref",
                "relation_link": {
                    "source_table": "systems",
                    "key_fields": {"Seq": "Seq"},
                    "required": True,
                },
                "method": "source_orbit",
                "normalization_version": "source_native_v1",
            },
            available_fields={"Seq", "o", "Per", "Ref"},
        )
        linked = con.execute(
            "select relation_claim_id from orbital_solution_evidence"
        ).fetchone()[0]
    assert linked == "relation"


def test_scoped_stellar_parameter_sets_keep_binary_components_separate(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "components.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select 'M4V' spectral_1, 'none' spectral_2, 'DA2' spectral_3, "
            "'-0.65' log_mass_1, '0.01' log_mass_error_1, "
            "'-9.99' log_mass_2, '-9.99' log_mass_error_2) "
            f"to '{parquet}' (format parquet)"
        )
        source_row_sha256 = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.binary','r1','components','binary_system',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_row_sha256],
        )
        consumed = compiler.materialize_scoped_stellar_evidence(
            con,
            source_id="test.binary",
            release_id="r1",
            table_name="components",
            path=parquet,
            parameter_sets=[
                {
                    "component_scope": "primary",
                    "parameter_set_kind": "dynamical_component",
                    "classification_field": "spectral_1",
                    "classification_missing_values": ["none"],
                    "method": "published_binary_solution",
                    "normalization_version": "source_native_v1",
                    "measurements": [
                        {
                            "value_field": "log_mass_1",
                            "uncertainty_field": "log_mass_error_1",
                            "quantity_key": "log10_mass",
                            "unit_raw": "dex(M_sun)",
                            "missing_values": ["-9.99"],
                        }
                    ],
                },
                {
                    "component_scope": "secondary",
                    "parameter_set_kind": "dynamical_component",
                    "classification_field": "spectral_2",
                    "classification_missing_values": ["none"],
                    "method": "published_binary_solution",
                    "normalization_version": "source_native_v1",
                    "measurements": [
                        {
                            "value_field": "log_mass_2",
                            "uncertainty_field": "log_mass_error_2",
                            "quantity_key": "log10_mass",
                            "unit_raw": "dex(M_sun)",
                            "missing_values": ["-9.99"],
                        }
                    ],
                },
                {
                    "component_scope": "tertiary",
                    "parameter_set_kind": "published_component_classification",
                    "classification_field": "spectral_3",
                    "method": "published_binary_classification",
                    "normalization_version": "source_native_v1",
                    "measurements": [],
                },
            ],
            available_fields={
                "spectral_1",
                "spectral_2",
                "spectral_3",
                "log_mass_1",
                "log_mass_error_1",
                "log_mass_2",
                "log_mass_error_2",
            },
        )
        evidence = con.execute(
            "select component_scope,quantity_key,value_raw,uncertainty_lower "
            "from stellar_parameter_evidence"
        ).fetchall()
        classifications = con.execute(
            "select component_scope,classification_raw "
            "from stellar_classification_evidence"
        ).fetchall()
        parameter_sets = con.execute(
            "select component_scope from stellar_parameter_sets"
        ).fetchall()
    assert consumed == {
        "spectral_1",
        "spectral_2",
        "spectral_3",
        "log_mass_1",
        "log_mass_error_1",
        "log_mass_2",
        "log_mass_error_2",
    }
    assert evidence == [("primary", "log10_mass", "-0.65", 0.01)]
    assert classifications == [("primary", "M4V"), ("tertiary", "DA2")]
    assert parameter_sets == [("primary",)]


def test_missing_uncertainty_sentinel_does_not_become_large_uncertainty() -> None:
    expression = compiler.nullable_measurement_double_expression(
        "uncertainty", ["-9.99", "-9.9900"], absolute=True
    )
    with duckdb.connect() as con:
        rows = con.execute(
            f"select {expression} from (values ('-9.9900'),('-0.25')) t(uncertainty)"
        ).fetchall()
    assert rows == [(None,), (0.25,)]


def test_configured_photometry_preserves_dynamic_band_reference_and_quality(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "photometry.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select '8.4' mag, 'V' band, '2024A&A...1A' reference, "
            f"'primary' component) to '{parquet}' (format parquet)"
        )
        source_row_sha256 = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchone()[0]
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('record','test.photometry','r1','systems','binary_system',"
            "'{}','{}',?,1,'raw','typed','raw-tree','typed-table',"
            "timestamp '2026-07-19 00:00:00')",
            [source_row_sha256],
        )
        consumed = compiler.materialize_configured_photometry(
            con,
            source_id="test.photometry",
            release_id="r1",
            table_name="systems",
            path=parquet,
            measurements=[
                {
                    "value_field": "mag",
                    "quantity_key": "apparent_magnitude",
                    "bandpass_field": "band",
                    "reference_field": "reference",
                    "quality_fields": ["component"],
                    "unit_raw": "mag",
                }
            ],
            available_fields={"mag", "band", "reference", "component"},
        )
        row = con.execute(
            "select bandpass,reference_raw,quality_json::varchar "
            "from photometry_extinction_evidence"
        ).fetchone()
    assert consumed == {"mag", "band", "reference", "component"}
    assert row[:2] == ("V", "2024A&A...1A")
    assert json.loads(row[2])["component"] == "primary"


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


def test_conditional_identifier_claims_apply_only_to_matching_rows(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "parameters.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('PSRJ','J1234+5678'),('P0','1.25')) "
            "t(parameter_name,value_raw)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'compact.test', 'r1', 'parameters', 'compact_object', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        consumed = compiler.materialize_conditional_identifier_claims(
            con,
            source_id="compact.test",
            release_id="r1",
            table_name="parameters",
            path=parquet,
            claims=[
                {
                    "value_field": "value_raw",
                    "namespace": "psrj",
                    "claim_scope": "compact_object",
                    "sql_predicate": "parameter_name='PSRJ'",
                }
            ],
            available_fields={"parameter_name", "value_raw"},
        )
        claims = con.execute(
            "select namespace,identifier_raw,claim_scope,quality_json->>'predicate' "
            "from identifier_claim_evidence"
        ).fetchall()
    assert consumed == {"value_raw"}
    assert claims == [("psrj", "J1234+5678", "compact_object", "parameter_name='PSRJ'")]


def test_conditional_identifier_claim_strips_prefix_before_numeric_normalization(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "aliases.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values ('GAIADR3 000123'),('HIP 7')) t(Name)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'multiplicity.test', 'r1', 'aliases', 'system_alias', '{}', '{}', "
            "?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        compiler.materialize_conditional_identifier_claims(
            con,
            source_id="multiplicity.test",
            release_id="r1",
            table_name="aliases",
            path=parquet,
            claims=[
                {
                    "value_field": "Name",
                    "namespace": "gaia_dr3_source_id",
                    "claim_scope": "star",
                    "sql_predicate": "Name like 'GAIADR3 %'",
                    "strip_prefix": "GAIADR3 ",
                    "normalization": "unsigned_integer_decimal_v1",
                }
            ],
            available_fields={"Name"},
        )
        claim = con.execute(
            "select identifier_raw,identifier_normalized from identifier_claim_evidence"
        ).fetchone()
    assert claim == ("GAIADR3 000123", "123")


def test_identifier_normalization_strips_only_trailing_hash_footnotes(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "names.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('AX J1818.8-1559 #'),('PSR J1846-0258 ##'),('Name#Internal')) "
            "t(Name)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'compact.test', 'r1', 'names', 'compact_object', '{}', '{}', ?, "
            "1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        compiler.materialize_identifier_claims(
            con,
            source_id="compact.test",
            release_id="r1",
            table_name="names",
            path=parquet,
            fields=["Name"],
            claim_by_field={
                "Name": {
                    "namespace": "compact_name",
                    "claim_scope": "compact_object",
                    "normalization": "strip_trailing_hash_footnote_v1",
                }
            },
        )
        names = con.execute(
            "select identifier_raw,identifier_normalized "
            "from identifier_claim_evidence order by identifier_raw"
        ).fetchall()
    assert names == [
        ("AX J1818.8-1559 #", "AX J1818.8-1559"),
        ("Name#Internal", "Name#Internal"),
        ("PSR J1846-0258 ##", "PSR J1846-0258"),
    ]


def test_multiple_compact_parameter_sets_have_distinct_ids_and_predicates(
    tmp_path: Path,
) -> None:
    parquet = tmp_path / "compact.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('Object A','5.0','timing-ref','0.4','xray-ref'),"
            "('Object B','7.0','timing-ref',null,null)) "
            "t(Name,Period,Ref_Time,kT,Ref_Xray)) "
            f"to '{parquet}' (format parquet)"
        )
        rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.executemany(
            "insert into source_records values "
            "(?, 'compact.test', 'r1', 'compact', 'compact_object', '{}', '{}', ?, "
            "1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [(f"record-{index}", row[0]) for index, row in enumerate(rows)],
        )
        common = {
            "source_id": "compact.test",
            "release_id": "r1",
            "table_name": "compact",
            "path": parquet,
            "fields": [
                {"column_name": name}
                for name in ("Name", "Period", "Ref_Time", "kT", "Ref_Xray")
            ],
            "available_fields": {"Name", "Period", "Ref_Time", "kT", "Ref_Xray"},
        }
        timing_fields = compiler.materialize_compact_objects(
            con,
            compact_object={
                "compact_kind": "timing",
                "parameter_set_key_fields": ["Name", "Ref_Time"],
                "parameter_fields": ["Period", "Ref_Time"],
                "quality_fields": ["Name"],
                "reference_field": "Ref_Time",
                "sql_predicate": "nullif(trim(Period), '') is not null",
                "method": "source_timing",
                "normalization_version": "source_native_v1",
            },
            **common,
        )
        xray_fields = compiler.materialize_compact_objects(
            con,
            compact_object={
                "compact_kind": "xray",
                "parameter_set_key_fields": ["Name", "Ref_Xray"],
                "parameter_fields": ["kT", "Ref_Xray"],
                "quality_fields": ["Name"],
                "reference_field": "Ref_Xray",
                "sql_predicate": "nullif(trim(kT), '') is not null",
                "method": "source_xray",
                "normalization_version": "source_native_v1",
            },
            **common,
        )
        rows = con.execute(
            "select compact_kind,count(*),count(distinct evidence_id) "
            "from compact_object_evidence group by 1 order by 1"
        ).fetchall()
    assert timing_fields == {"Name", "Period", "Ref_Time"}
    assert xray_fields == {"Name", "kT", "Ref_Xray"}
    assert rows == [("timing", 2, 2), ("xray", 1, 1)]


def test_authoritative_citation_catalog_validates_compact_references(
    tmp_path: Path,
) -> None:
    citation_parquet = tmp_path / "references.parquet"
    parameter_parquet = tmp_path / "parameters.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values "
            "('REF1','Author et al. 2025','source block')) "
            "t(reference_code,citation_text,raw_block)) "
            f"to '{citation_parquet}' (format parquet)"
        )
        con.execute(
            f"copy (select * from (values "
            "('PSR A','P0','1.25','REF1'),"
            "('PSR B','P0','2.50','not-a-reference')) "
            "t(pulsar_name,parameter_name,value_raw,reference_raw)) "
            f"to '{parameter_parquet}' (format parquet)"
        )
        citation_sha = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{citation_parquet}') t"
        ).fetchone()[0]
        parameter_rows = con.execute(
            f"select sha256(to_json(t)) from read_parquet('{parameter_parquet}') t"
        ).fetchall()
        compiler.create_schema(con)
        con.execute(
            "insert into source_records values "
            "('citation-record', 'compact.test', 'r1', 'references', "
            "'source_reference', '{}', '{}', ?, 1, 'raw', 'typed', "
            "'raw-tree', 'typed-table', timestamp '2026-07-19 00:00:00')",
            [citation_sha],
        )
        con.executemany(
            "insert into source_records values "
            "(?, 'compact.test', 'r1', 'parameters', 'compact_object', "
            "'{}', '{}', ?, 1, 'raw', 'typed', 'raw-tree', 'typed-table', "
            "timestamp '2026-07-19 00:00:00')",
            [
                (f"parameter-record-{index}", row[0])
                for index, row in enumerate(parameter_rows)
            ],
        )
        citation_fields = [
            {"column_name": name}
            for name in ("reference_code", "citation_text", "raw_block")
        ]
        compiler.materialize_citation_catalog(
            con,
            source_id="compact.test",
            release_id="r1",
            table_name="references",
            path=citation_parquet,
            citation_catalog={
                "reference_key_field": "reference_code",
                "citation_text_field": "citation_text",
                "context_fields": ["raw_block"],
            },
            fields=citation_fields,
            available_fields={"reference_code", "citation_text", "raw_block"},
        )
        compiler.materialize_compact_objects(
            con,
            source_id="compact.test",
            release_id="r1",
            table_name="parameters",
            path=parameter_parquet,
            fields=[
                {"column_name": name}
                for name in (
                    "pulsar_name",
                    "parameter_name",
                    "value_raw",
                    "reference_raw",
                )
            ],
            compact_object={
                "compact_kind": "pulsar_parameter",
                "parameter_set_key_fields": ["pulsar_name", "parameter_name"],
                "parameter_fields": ["parameter_name", "value_raw", "reference_raw"],
                "quality_fields": ["pulsar_name"],
                "reference_field": "reference_raw",
                "reference_catalog_validated": True,
                "method": "source_parameter",
                "normalization_version": "source_native_v1",
            },
            available_fields={
                "pulsar_name",
                "parameter_name",
                "value_raw",
                "reference_raw",
            },
        )
        summary = compiler.materialize_citations(con)
        evidence = con.execute(
            "select quality_json->>'pulsar_name', reference_raw, "
            "parameter_set_raw->>'reference_raw' "
            "from compact_object_evidence order by 1"
        ).fetchall()
        citation = con.execute(
            "select source_reference_key,citation_text_raw,"
            "parsed_json->'source_context'->>'raw_block' from citations"
        ).fetchone()
    assert evidence == [
        ("PSR A", "REF1", "REF1"),
        ("PSR B", None, "not-a-reference"),
    ]
    assert citation == ("REF1", "Author et al. 2025", "source block")
    assert summary == {"citations": 1, "evidence_links": 1}


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


def test_reference_fragment_parser_recognizes_direct_ads_bibcode() -> None:
    assert compiler.parse_reference_fragment("1926PDAO....3..341H") == {
        "reference_key": None,
        "display_text": "1926PDAO....3..341H",
        "url": "https://ui.adsabs.harvard.edu/abs/1926PDAO....3..341H/abstract",
        "bibcode": "1926PDAO....3..341H",
        "doi": None,
        "publication_year": 1926,
    }
