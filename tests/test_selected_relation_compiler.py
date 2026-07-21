from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_selected_relations as compiler  # noqa: E402
import audit_selected_relation_artifact as artifact_audit  # noqa: E402


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def make_fixture(state: Path, policy_path: Path) -> None:
    identity_id = "identity-test"
    identity_dir = state / "derived/evidence_lake_v2/identity" / identity_id
    identity_dir.mkdir(parents=True)
    con = duckdb.connect(str(identity_dir / "identity_graph.duckdb"))
    con.execute(
        """
        CREATE TABLE canonical_identifier_bindings (
          namespace VARCHAR, id_value_norm VARCHAR, object_node_key VARCHAR,
          stable_object_key VARCHAR, system_stable_object_key VARCHAR
        );
        CREATE TABLE canonical_object_nodes (
          object_node_key VARCHAR, stable_object_key VARCHAR,
          system_stable_object_key VARCHAR, object_type VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('gaia_dr3','123','node-123','star-123','system-1'),
          ('gaia_dr3','124','node-124','star-124','system-2'),
          ('gaia_dr3','888','node-888-a','star-888-a','system-3'),
          ('gaia_dr3','888','node-888-b','star-888-b','system-4');
        INSERT INTO canonical_object_nodes VALUES
          ('node-123','star-123','system-1','star'),
          ('node-124','star-124','system-2','star'),
          ('node-888-a','star-888-a','system-3','star'),
          ('node-888-b','star-888-b','system-4','star');
        """
    )
    con.close()
    write_json(identity_dir / "identity_graph_report.json", {"graph_id": identity_id, "status": "pass"})

    evidence_id = "evidence-test"
    evidence_dir = state / "derived/evidence_lake_v2/scientific_evidence" / evidence_id
    evidence_dir.mkdir(parents=True)
    con = duckdb.connect(str(evidence_dir / "scientific_evidence.duckdb"))
    con.execute(
        """
        CREATE TABLE relation_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR,
          left_identity_namespace VARCHAR, left_identity_raw VARCHAR,
          right_identity_namespace VARCHAR, right_identity_raw VARCHAR,
          relation_kind VARCHAR, relation_scope VARCHAR, probability DOUBLE,
          probability_semantics VARCHAR, confidence_statistic_key VARCHAR,
          confidence_statistic_value_raw VARCHAR, confidence_statistic_value DOUBLE,
          confidence_statistic_unit VARCHAR, confidence_statistic_semantics VARCHAR,
          evidence_polarity VARCHAR, method VARCHAR, reference_raw VARCHAR,
          epoch_raw VARCHAR, quality_json JSON
        );
        INSERT INTO relation_claim_evidence VALUES
          ('r-high','sr-high','gaia_edr3_source_id','123','gaia_edr3_source_id','124',
           'wide_binary','pair',NULL,NULL,'chance_alignment_density_ratio','0.01',0.01,
           'dimensionless','not a strict probability','candidate','test','ref','2016.0','{}'),
          ('r-missing','sr-missing','gaia_edr3_source_id','123','gaia_edr3_source_id','125',
           'wide_binary','pair',NULL,NULL,'chance_alignment_density_ratio','0.02',0.02,
           'dimensionless','not a strict probability','candidate','test','ref','2016.0','{}'),
          ('r-negative','sr-negative','gaia_edr3_source_id','123','gaia_edr3_source_id','124',
           'shifted_control','control',NULL,NULL,'chance_alignment_density_ratio','0.01',0.01,
           'dimensionless','not a strict probability','negative_control','test','ref','2016.0','{}'),
          ('r-ambiguous','sr-ambiguous','gaia_edr3_source_id','888','gaia_edr3_source_id','124',
           'wide_binary','pair',NULL,NULL,'chance_alignment_density_ratio','0.5',0.5,
           'dimensionless','not a strict probability','candidate','test','ref','2016.0','{}'),
          ('r-namespace','sr-namespace','wds_id','00001+0001','gaia_edr3_source_id','124',
           'wide_binary','pair',NULL,NULL,'chance_alignment_density_ratio','0.5',0.5,
           'dimensionless','not a strict probability','candidate','test','ref','2016.0','{}');
        """
    )
    con.close()

    write_json(
        policy_path,
        {
            "schema_version": "spacegate.e5_relation_policies.v1",
            "policy_version": "test-relations-v1",
            "compiler_version": "test-compiler-v1",
            "identity_graph_id": identity_id,
            "sources": [
                {
                    "source_id": "multiplicity.test",
                    "release_id": "test-release",
                    "evidence_build_id": evidence_id,
                    "relation_table": "relation_claim_evidence",
                    "object_type": "star",
                    "endpoint_binding": {
                        "source_namespace": "gaia_edr3_source_id",
                        "canonical_namespace": "gaia_dr3",
                        "normalization": "unsigned_decimal",
                        "method": "test-equivalence",
                        "reason": "test unique target",
                        "citation_url": "https://example.test/identity",
                    },
                    "projection": {
                        "candidate_polarity": "candidate",
                        "negative_polarity": "negative_control",
                        "confidence_statistic_key": "chance_alignment_density_ratio",
                        "high_confidence_operator": "lt",
                        "high_confidence_threshold": 0.1,
                        "high_confidence_semantics": "test high confidence",
                        "citation_url": "https://example.test/paper",
                        "canonical_containment_promotion": False,
                    },
                    "acceptance": {
                        "expected_relation_claims": 5,
                        "expected_endpoint_bindings": 10,
                        "expected_both_endpoints_accepted": 2,
                        "expected_high_confidence_relations": 1,
                        "expected_negative_controls_with_bound_endpoints": 1,
                    },
                }
            ],
        },
    )


def test_relation_endpoint_accounting_and_determinism(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy = tmp_path / "policy.json"
    output = tmp_path / "output"
    make_fixture(state, policy)

    first = compiler.compile_relations(
        policy_path=policy,
        state=state,
        output_root=output,
        report_path=tmp_path / "first.json",
    )
    second = compiler.compile_relations(
        policy_path=policy,
        state=state,
        output_root=output,
        report_path=tmp_path / "second.json",
    )
    assert first["deterministic_files"] == second["deterministic_files"]
    assert first["verification"] == {key: 0 for key in first["verification"]}

    database = Path(first["artifact_path"]) / "selected_relations.duckdb"
    con = duckdb.connect(str(database), read_only=True)
    assert dict(con.execute("SELECT binding_status,COUNT(*) FROM relation_endpoint_bindings GROUP BY 1").fetchall()) == {
        "accepted": 7,
        "ambiguous": 1,
        "excluded": 1,
        "missing": 1,
    }
    assert dict(con.execute("SELECT projection_status,COUNT(*) FROM relation_evidence_projection GROUP BY 1").fetchall()) == {
        "high_confidence_relation_evidence": 1,
        "negative_control_evidence": 1,
        "unresolved_endpoint_evidence": 3,
    }
    assert con.execute("SELECT COUNT(*) FROM relation_evidence_projection WHERE probability IS NOT NULL").fetchone()[0] == 0
    con.close()
    audit = artifact_audit.audit(artifact=Path(first["artifact_path"]), policy_path=policy)
    assert audit["status"] == "pass"
    assert audit["failing_checks"] == {}
