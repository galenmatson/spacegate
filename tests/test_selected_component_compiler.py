from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_selected_component_artifact as artifact_audit  # noqa: E402
import compile_selected_component_evidence as compiler  # noqa: E402


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
          namespace VARCHAR, id_value_raw VARCHAR, system_stable_object_key VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('wds','00001+0001','system-1');
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
        CREATE TABLE systems (
          system_id BIGINT, stable_object_key VARCHAR, system_name VARCHAR, wds_id VARCHAR
        );
        CREATE TABLE system_search_terms (
          system_id BIGINT, term_norm VARCHAR, term_priority INTEGER
        );
        INSERT INTO systems VALUES
          (1,'system-1','Test Binary','00001+0001'),
          (2,'system-2','Background Object',NULL);
        INSERT INTO system_search_terms VALUES
          (1,'test binary',0),
          (2,'test binary',500);
        """
    )
    con.close()

    msc_id = "msc-test"
    msc_dir = state / "derived/evidence_lake_v2/scientific_evidence" / msc_id
    msc_dir.mkdir(parents=True)
    con = duckdb.connect(str(msc_dir / "scientific_evidence.duckdb"))
    con.execute(
        """
        CREATE TABLE relation_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR,
          left_identity_namespace VARCHAR, left_identity_raw VARCHAR,
          right_identity_namespace VARCHAR, right_identity_raw VARCHAR,
          relation_kind VARCHAR, relation_scope VARCHAR, evidence_polarity VARCHAR,
          method VARCHAR, reference_raw VARCHAR, quality_json JSON
        );
        CREATE TABLE orbital_solution_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, parameter_set_raw JSON
        );
        INSERT INTO relation_claim_evidence VALUES
          ('m-r1','m-s1','msc_component','00001+0001:A','msc_component','00001+0001:B',
           'binary','pair','positive','test','ref','{}'),
          ('m-self','m-self-source','msc_component','00001+0001:A','msc_component','00001+0001:A',
           'binary','pair','positive','test','ref','{}'),
          ('m-missing','m-missing-source','msc_component','99999+9999:A','msc_component','99999+9999:B',
           'binary','pair','positive','test','ref','{}');
        INSERT INTO orbital_solution_evidence VALUES
          ('m-o1','m-s1','{"P":"2.0","Punit":"d"}'),
          ('m-o2','m-self-source','{"P":"3.0","Punit":"d"}'),
          ('m-o3','m-missing-source','{"P":"4.0","Punit":"d"}');
        """
    )
    con.close()

    debcat_id = "debcat-test"
    debcat_dir = state / "derived/evidence_lake_v2/scientific_evidence" / debcat_id
    debcat_dir.mkdir(parents=True)
    con = duckdb.connect(str(debcat_dir / "scientific_evidence.duckdb"))
    con.execute(
        """
        CREATE TABLE identifier_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, namespace VARCHAR, identifier_raw VARCHAR
        );
        CREATE TABLE orbital_solution_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, parameter_set_raw JSON
        );
        CREATE TABLE stellar_parameter_sets (
          parameter_set_id VARCHAR, source_record_id VARCHAR, component_scope VARCHAR
        );
        CREATE TABLE stellar_parameter_evidence (
          evidence_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
          component_scope VARCHAR, quantity_key VARCHAR
        );
        CREATE TABLE stellar_classification_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, component_scope VARCHAR
        );
        CREATE TABLE photometry_extinction_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR
        );
        INSERT INTO identifier_claim_evidence VALUES
          ('d-i1','d1','debcat_system_name','Test_Binary'),
          ('d-i2','d2','debcat_system_name','Absent_Binary'),
          ('d-i3','d3','debcat_system_name','Test_Binary');
        INSERT INTO orbital_solution_evidence VALUES
          ('d-o1','d1','{"period_days_raw":"2.001"}'),
          ('d-o2','d2','{"period_days_raw":"1.0"}'),
          ('d-o3','d3','{"period_days_raw":"9.0"}');
        INSERT INTO stellar_parameter_sets VALUES
          ('ps-primary','d1','primary'),
          ('ps-secondary','d1','secondary'),
          ('ps-system','d1',NULL),
          ('ps-missing','d2','primary');
        INSERT INTO stellar_parameter_evidence VALUES
          ('p-primary','ps-primary','d1','primary','log10_mass'),
          ('p-secondary','ps-secondary','d1','secondary','log10_radius'),
          ('p-system','ps-system','d1',NULL,'metallicity_m_h'),
          ('p-missing','ps-missing','d2','primary','log10_mass');
        INSERT INTO stellar_classification_evidence VALUES
          ('c-primary','d1','primary'),
          ('c-secondary','d1','secondary'),
          ('c-missing','d2','primary');
        INSERT INTO photometry_extinction_evidence VALUES
          ('ph-accepted','d1'),
          ('ph-missing','d2');
        """
    )
    con.close()

    write_json(
        policy_path,
        {
            "schema_version": "spacegate.e5_component_scope_policies.v1",
            "policy_version": "test-component-policy-v1",
            "compiler_version": "test-component-compiler-v1",
            "identity_graph_id": identity_id,
            "canonical_reference_build_id": canonical_build,
            "msc": {
                "source_id": "multiplicity.msc",
                "release_id": "msc-test-release",
                "evidence_build_id": msc_id,
                "component_namespace": "msc_component",
                "system_namespace": "wds",
                "system_binding_method": "test-exact-wds",
                "canonical_containment_promotion": False,
                "acceptance": {
                    "expected_system_bindings": 2,
                    "expected_systems_accepted": 1,
                    "expected_systems_missing": 1,
                    "expected_systems_ambiguous": 0,
                    "expected_identity_graph_systems_accepted": 1,
                    "expected_identity_graph_systems_missing": 1,
                    "expected_identity_graph_systems_ambiguous": 0,
                    "expected_component_entities": 4,
                    "expected_components_accepted": 2,
                    "expected_components_missing": 2,
                    "expected_components_ambiguous": 0,
                    "expected_relation_claims": 3,
                    "expected_relations_accepted": 1,
                    "expected_relations_unresolved": 1,
                    "expected_relations_invalid_self": 1,
                },
            },
            "debcat": {
                "source_id": "multiplicity.debcat",
                "release_id": "debcat-test-release",
                "evidence_build_id": debcat_id,
                "name_binding_method": "test-best-priority-name",
                "relation_binding_method": "test-system-period",
                "period_unit": "d",
                "period_absolute_tolerance_days": 0.01,
                "period_relative_tolerance": 0.01,
                "component_parameter_authority": {
                    "log10_mass": "test-mass",
                    "log10_radius": "test-radius",
                    "metallicity_m_h": "test-metallicity",
                },
                "classification_authority": "test-classification",
                "photometry_authority": "test-photometry",
                "orbit_authority": "test-orbit",
                "canonical_containment_promotion": False,
                "acceptance": {
                    "expected_system_bindings": 3,
                    "expected_systems_accepted": 2,
                    "expected_systems_missing": 1,
                    "expected_systems_ambiguous": 0,
                    "expected_relation_bindings": 3,
                    "expected_relations_accepted": 1,
                    "expected_relations_missing_system": 1,
                    "expected_relations_no_period_match": 1,
                    "expected_relations_ambiguous": 0,
                    "expected_parameter_sets": 4,
                    "expected_parameter_sets_eligible": 3,
                    "expected_parameter_evidence": 4,
                    "expected_parameter_evidence_eligible": 3,
                    "expected_classification_evidence": 3,
                    "expected_classification_evidence_eligible": 2,
                    "expected_photometry_evidence": 2,
                    "expected_photometry_evidence_eligible": 1,
                    "expected_orbital_solutions": 3,
                    "expected_orbital_solutions_eligible": 1,
                },
            },
        },
    )


def test_component_scope_accounting_and_determinism(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy = tmp_path / "policy.json"
    output = tmp_path / "output"
    make_fixture(state, policy)

    first = compiler.compile_components(
        policy_path=policy,
        state=state,
        output_root=output,
        report_path=tmp_path / "first.json",
    )
    second = compiler.compile_components(
        policy_path=policy,
        state=state,
        output_root=output,
        report_path=tmp_path / "second.json",
    )
    assert first["deterministic_files"] == second["deterministic_files"]
    assert first["verification"] == {key: 0 for key in first["verification"]}

    database = Path(first["artifact_path"]) / "selected_components.duckdb"
    con = duckdb.connect(str(database), read_only=True)
    assert dict(con.execute("SELECT projection_status,count(*) FROM msc_relation_evidence_projection GROUP BY 1").fetchall()) == {
        "accepted_relation_evidence": 1,
        "invalid_self_relation_evidence": 1,
        "unresolved_endpoint_evidence": 1,
    }
    assert dict(con.execute("SELECT binding_status,count(*) FROM debcat_relation_bindings GROUP BY 1").fetchall()) == {
        "accepted": 1,
        "missing_system": 1,
        "no_period_match": 1,
    }
    accepted_name = con.execute(
        "SELECT canonical_system_stable_object_key FROM debcat_system_bindings WHERE source_record_id='d1'"
    ).fetchone()[0]
    assert accepted_name == "system-1"
    assert con.execute(
        "SELECT count(*) FROM debcat_parameter_set_bindings WHERE component_scope='system' AND target_scope='canonical_system' AND binding_status='accepted'"
    ).fetchone()[0] == 1
    assert con.execute(
        "SELECT count(*) FROM debcat_parameter_set_bindings WHERE component_scope IN ('primary','secondary') AND target_scope='msc_source_component' AND binding_status='accepted'"
    ).fetchone()[0] == 2
    con.close()

    audited = artifact_audit.audit(artifact=Path(first["artifact_path"]), policy_path=policy)
    assert audited["status"] == "pass"
    assert audited["failing_checks"] == {}
