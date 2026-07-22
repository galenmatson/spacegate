from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_selected_extended_object_artifact as artifact_audit  # noqa: E402
import compile_selected_extended_object_evidence as compiler  # noqa: E402


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def make_source(path: Path, rows: list[tuple], evidence: list[tuple]) -> None:
    path.parent.mkdir(parents=True)
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE source_records (
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          source_table VARCHAR, object_scope VARCHAR, logical_key_json JSON
        );
        CREATE TABLE extended_object_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, extended_kind VARCHAR,
          geometry_raw JSON, distance_raw JSON, parameter_set_raw JSON,
          method VARCHAR, model VARCHAR, reference_raw VARCHAR, quality_json JSON,
          normalization_version VARCHAR
        );
        """
    )
    con.executemany("INSERT INTO source_records VALUES (?,?,?,?,?,?)", rows)
    con.executemany("INSERT INTO extended_object_evidence VALUES (?,?,?,?,?,?,?,?,?,?,?)", evidence)
    con.close()


def make_fixture(state: Path, policy_path: Path) -> None:
    canonical_build = "canonical-test"
    core_dir = state / "out" / canonical_build
    core_dir.mkdir(parents=True)
    con = duckdb.connect(str(core_dir / "core.duckdb"))
    con.execute(
        """
        CREATE TABLE extended_objects (
          extended_object_id BIGINT, stable_object_key VARCHAR,
          canonical_name VARCHAR, object_family VARCHAR, object_type VARCHAR
        );
        CREATE TABLE extended_object_source_reconciliation (
          extended_object_reconciliation_id BIGINT, source_record_key VARCHAR,
          extended_object_id DOUBLE, outcome VARCHAR, reason VARCHAR
        );
        INSERT INTO extended_objects VALUES
          (100,'extended:snr','Test SNR','remnant','supernova_remnant'),
          (200,'extended:ic1','IC 1','nebula','emission_nebula'),
          (201,'extended:ldn7','LDN 7','nebula','dark_nebula');
        INSERT INTO extended_object_source_reconciliation VALUES
          (1,'green_snr:snr:g+1.0-0.1',100.0,'accepted','identity_master'),
          (2,'openngc:ic0001',200.0,'accepted','identity_master'),
          (3,'ldn:seq-7',201.0,'accepted','identity_master'),
          (4,'openngc:ic0002',NULL,'excluded_stellar_domain','stellar'),
          (5,'openngc:ic0003',NULL,'quarantine_unclassified','unknown'),
          (6,'openngc:ic0004',NULL,'redirect','redirect');
        """
    )
    con.close()

    green_build = "green-test"
    make_source(
        state / "derived/evidence_lake_v2/scientific_evidence" / green_build / "scientific_evidence.duckdb",
        [("g1", "extended.green_snr", "green-release", "green_snr_catalogue", "supernova_remnant", '{"galactic_longitude":"1.0","galactic_latitude":"-0.1"}')],
        [("ge1", "g1", "supernova_remnant", "{}", None, "{}", "test", None, "ref", "{}", "v1")],
    )

    open_build = "open-test"
    make_source(
        state / "derived/evidence_lake_v2/scientific_evidence" / open_build / "scientific_evidence.duckdb",
        [
            ("o1", "extended.openngc_and_nebulae", "open-release", "openngc_ngc", "extended_object", '{"Name":"IC0001"}'),
            ("o2", "extended.openngc_and_nebulae", "open-release", "openngc_ngc", "extended_object", '{"Name":"IC0002"}'),
            ("o3", "extended.openngc_and_nebulae", "open-release", "openngc_ngc", "extended_object", '{"Name":"IC0003"}'),
            ("o4", "extended.openngc_and_nebulae", "open-release", "openngc_ngc", "extended_object", '{"Name":"IC0004"}'),
            ("o5", "extended.openngc_and_nebulae", "open-release", "ldn_vii_7a", "dark_nebula", '{"LDN":null}'),
        ],
        [
            ("oe1", "o1", "openngc", "{}", None, "{}", "test", None, "ref", "{}", "v1"),
            ("oe2", "o2", "openngc", "{}", None, "{}", "test", None, "ref", "{}", "v1"),
            ("oe3", "o3", "openngc", "{}", None, "{}", "test", None, "ref", "{}", "v1"),
            ("oe4", "o4", "openngc", "{}", None, "{}", "test", None, "ref", "{}", "v1"),
            ("oe5", "o5", "dark_nebula", "{}", None, '{"Seq":"7"}', "test", None, "ref", "{}", "v1"),
        ],
    )

    write_json(
        policy_path,
        {
            "schema_version": "spacegate.e5_extended_object_policies.v1",
            "policy_version": "test-extended-v1",
            "compiler_version": "test-extended-compiler-v1",
            "canonical_reference_build_id": canonical_build,
            "reconciliation_contract": "test-reconciliation",
            "canonical_id_join": "unique_double_encoded_reconciliation_id_to_bigint_v1",
            "sources": [
                {
                    "source_id": "extended.green_snr",
                    "release_id": "green-release",
                    "evidence_build_id": green_build,
                    "source_key_strategy": "green_snr_galactic_coordinate_key_v1",
                    "authority_role": "test-green",
                    "accepted_reconciliation_outcomes": ["accepted", "reconciled", "redirected"],
                    "stellar_fact_projection": False,
                    "acceptance": {
                        "expected_bindings": 1,
                        "expected_bindings_accepted": 1,
                        "expected_bindings_excluded": 0,
                        "expected_bindings_quarantined": 0,
                        "expected_bindings_unresolved": 0,
                        "expected_evidence": 1,
                        "expected_evidence_eligible": 1,
                        "expected_canonical_candidate_ambiguities": 0,
                        "expected_stellar_fact_rows": 0,
                    },
                },
                {
                    "source_id": "extended.openngc_and_nebulae",
                    "release_id": "open-release",
                    "evidence_build_id": open_build,
                    "source_key_strategy": "extended_catalog_logical_key_v1",
                    "authority_role": "test-open",
                    "accepted_reconciliation_outcomes": ["accepted", "reconciled", "redirected"],
                    "stellar_fact_projection": False,
                    "acceptance": {
                        "expected_bindings": 5,
                        "expected_bindings_accepted": 2,
                        "expected_bindings_excluded": 1,
                        "expected_bindings_quarantined": 1,
                        "expected_bindings_unresolved": 1,
                        "expected_evidence": 5,
                        "expected_evidence_eligible": 2,
                        "expected_canonical_candidate_ambiguities": 0,
                        "expected_stellar_fact_rows": 0,
                    },
                },
            ],
        },
    )


def test_extended_object_scope_accounting_and_determinism(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy = tmp_path / "policy.json"
    output = tmp_path / "output"
    make_fixture(state, policy)
    first = compiler.compile_extended_objects(
        policy_path=policy, state=state, output_root=output, report_path=tmp_path / "first.json"
    )
    second = compiler.compile_extended_objects(
        policy_path=policy, state=state, output_root=output, report_path=tmp_path / "second.json"
    )
    assert first["deterministic_files"] == second["deterministic_files"]
    assert first["verification"] == {key: 0 for key in first["verification"]}

    con = duckdb.connect(str(Path(first["artifact_path"]) / "selected_extended_objects.duckdb"), read_only=True)
    assert dict(con.execute(
        "SELECT binding_status,count(*) FROM extended_object_bindings WHERE source_id='extended.openngc_and_nebulae' GROUP BY 1"
    ).fetchall()) == {"accepted": 2, "excluded": 1, "quarantined": 1, "unresolved": 1}
    assert con.execute(
        "SELECT source_record_key FROM extended_object_bindings WHERE evidence_id='oe5'"
    ).fetchone()[0] == "ldn:seq-7"
    assert con.execute(
        "SELECT count(*) FROM extended_object_evidence_projection WHERE stellar_fact_projection"
    ).fetchone()[0] == 0
    con.close()

    audited = artifact_audit.audit(artifact=Path(first["artifact_path"]), policy_path=policy)
    assert audited["status"] == "pass"
    assert audited["failing_checks"] == {}
