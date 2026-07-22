from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_selected_cluster_artifact as artifact_audit  # noqa: E402
import compile_selected_cluster_evidence as compiler  # noqa: E402


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
          namespace VARCHAR, id_value_norm VARCHAR,
          stable_object_key VARCHAR, system_stable_object_key VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('gaia_dr3','100','star-100','system-100');
        """
    )
    con.close()
    write_json(identity_dir / "identity_graph_report.json", {"graph_id": identity_id, "status": "pass"})

    canonical_build = "canonical-test"
    core_dir = state / "out" / canonical_build
    core_dir.mkdir(parents=True)
    con = duckdb.connect(str(core_dir / "core.duckdb"))
    con.execute(
        """
        CREATE TABLE open_clusters (
          cluster_id BIGINT, stable_object_key VARCHAR, cluster_name VARCHAR
        );
        INSERT INTO open_clusters VALUES
          (1,'cluster-alpha','Alpha Cluster'),
          (2,'cluster-beta','Beta-Cluster');
        """
    )
    con.close()

    source_build = "cluster-source-test"
    source_dir = state / "derived/evidence_lake_v2/scientific_evidence" / source_build
    source_dir.mkdir(parents=True)
    con = duckdb.connect(str(source_dir / "scientific_evidence.duckdb"))
    con.execute(
        """
        CREATE TABLE source_records (
          source_record_id VARCHAR, source_table VARCHAR
        );
        CREATE TABLE identifier_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR,
          namespace VARCHAR, identifier_raw VARCHAR
        );
        CREATE TABLE cluster_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR,
          cluster_identity_raw VARCHAR, parameter_set_raw JSON
        );
        CREATE TABLE cluster_membership_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR,
          cluster_identity_raw VARCHAR, member_identity_raw VARCHAR,
          membership_probability DOUBLE
        );
        INSERT INTO source_records VALUES
          ('c1','hunt_reffert_2024_clusters'),
          ('c2','hunt_reffert_2024_clusters'),
          ('c3','hunt_reffert_2024_clusters'),
          ('c4','hunt_reffert_2024_clusters'),
          ('x1','hunt_reffert_2024_crossmatch'),
          ('m1','hunt_reffert_2024_members'),
          ('m2','hunt_reffert_2024_members'),
          ('m3','hunt_reffert_2024_members'),
          ('m4','hunt_reffert_2024_members');
        INSERT INTO identifier_claim_evidence VALUES
          ('i1','c1','cluster_name','Alpha Cluster'),
          ('i2','c2','cluster_name','Beta Cluster'),
          ('i3','c3','cluster_name','Beta-Cluster'),
          ('i4','c4','cluster_name','Missing Cluster'),
          ('ix','x1','cluster_literature_designation','Alpha-Cluster');
        INSERT INTO cluster_evidence VALUES
          ('e1','c1','1','{"distance":10}'),
          ('e2','c2','2','{"distance":20}'),
          ('e3','c3','3','{"distance":21}'),
          ('e4','c4','4','{"distance":30}'),
          ('ex','x1','1','{"source":"literature"}');
        INSERT INTO cluster_membership_evidence VALUES
          ('me1','m1','1','100',0.9),
          ('me2','m2','1','999',0.5),
          ('me3','m3','2','100',0.8),
          ('me4','m4','4','999',0.4);
        """
    )
    con.close()

    write_json(
        policy_path,
        {
            "schema_version": "spacegate.e5_cluster_policies.v1",
            "policy_version": "test-cluster-policy-v1",
            "compiler_version": "test-cluster-compiler-v1",
            "identity_graph_id": identity_id,
            "canonical_reference_build_id": canonical_build,
            "sources": [
                {
                    "source_id": "clusters.hunt_reffert_2024",
                    "release_id": "test-release",
                    "evidence_build_id": source_build,
                    "cluster_binding_method": "test-exact-designation",
                    "cluster_name_namespaces": ["cluster_name", "cluster_literature_designation"],
                    "designation_normalization": "lower_trim_space_hyphen_to_underscore_v1",
                    "cluster_authority": "test-cluster-authority",
                    "membership_authority": "test-membership-authority",
                    "canonical_containment_promotion": False,
                    "acceptance": {
                        "expected_cluster_bindings": 4,
                        "expected_clusters_accepted": 1,
                        "expected_clusters_missing": 1,
                        "expected_clusters_ambiguous": 2,
                        "expected_cluster_evidence": 5,
                        "expected_cluster_characterizations": 4,
                        "expected_cluster_characterizations_eligible": 1,
                        "expected_crossmatch_context": 1,
                        "expected_memberships": 4,
                        "expected_member_endpoints_accepted": 2,
                        "expected_member_endpoints_missing": 2,
                        "expected_member_endpoints_ambiguous": 0,
                        "expected_memberships_both_accepted": 1,
                        "expected_memberships_cluster_accepted_member_missing": 1,
                        "expected_memberships_cluster_ambiguous_member_accepted": 1,
                        "expected_memberships_cluster_ambiguous_member_missing": 0,
                        "expected_memberships_cluster_missing_member_accepted": 0,
                        "expected_memberships_both_missing": 1,
                        "expected_canonical_containment_rows": 0,
                    },
                }
            ],
        },
    )


def test_cluster_scope_accounting_collision_safeguard_and_determinism(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy = tmp_path / "policy.json"
    output = tmp_path / "output"
    make_fixture(state, policy)

    first = compiler.compile_clusters(
        policy_path=policy,
        state=state,
        output_root=output,
        report_path=tmp_path / "first.json",
    )
    second = compiler.compile_clusters(
        policy_path=policy,
        state=state,
        output_root=output,
        report_path=tmp_path / "second.json",
    )
    assert first["deterministic_files"] == second["deterministic_files"]
    assert first["verification"] == {key: 0 for key in first["verification"]}

    database = Path(first["artifact_path"]) / "selected_clusters.duckdb"
    con = duckdb.connect(str(database), read_only=True)
    assert dict(con.execute(
        "SELECT binding_status,count(*) FROM cluster_identity_bindings GROUP BY 1"
    ).fetchall()) == {"accepted": 1, "ambiguous": 2, "missing": 1}
    assert con.execute(
        "SELECT count(*) FROM cluster_identity_bindings WHERE binding_status='ambiguous' AND source_cluster_collision_count=2"
    ).fetchone()[0] == 2
    assert con.execute(
        "SELECT count(*) FROM cluster_evidence_projection WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0] == 1
    assert con.execute(
        "SELECT count(*) FROM cluster_membership_projection WHERE projection_status='probability_bearing_membership_evidence' AND canonical_containment_promotion=false"
    ).fetchone()[0] == 1
    con.close()

    audited = artifact_audit.audit(artifact=Path(first["artifact_path"]), policy_path=policy)
    assert audited["status"] == "pass"
    assert audited["failing_checks"] == {}
