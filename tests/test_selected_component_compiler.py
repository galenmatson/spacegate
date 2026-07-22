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
          namespace VARCHAR, id_value_raw VARCHAR, id_value_norm VARCHAR,
          system_stable_object_key VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('wds','00001+0001','00001 0001','system-1'),
          ('gaia_dr3','123','123','system-1');
        CREATE TABLE dr2_release_outcomes (
          dr2_source_id VARCHAR, outcome VARCHAR,
          canonical_system_stable_object_key VARCHAR
        );
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
        CREATE TABLE source_records (source_record_id VARCHAR, source_table VARCHAR);
        CREATE TABLE identifier_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, namespace VARCHAR,
          identifier_raw VARCHAR, component_scope VARCHAR
        );
        CREATE TABLE stellar_parameter_sets (
          parameter_set_id VARCHAR, source_record_id VARCHAR, component_scope VARCHAR
        );
        CREATE TABLE stellar_parameter_evidence (
          evidence_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
          quantity_key VARCHAR
        );
        CREATE TABLE stellar_classification_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, component_scope VARCHAR
        );
        CREATE TABLE photometry_extinction_evidence (evidence_id VARCHAR, source_record_id VARCHAR);
        CREATE TABLE astrometry_distance_evidence (evidence_id VARCHAR, source_record_id VARCHAR);
        INSERT INTO relation_claim_evidence VALUES
          ('m-r1','m-s1','msc_component','00001+0001:A','msc_component','00001+0001:B',
           'binary','pair','positive','test','ref','{"Comment":"SB9_1"}'),
          ('m-self','m-self-source','msc_component','00001+0001:A','msc_component','00001+0001:A',
           'binary','pair','positive','test','ref','{"Comment":"SB9_3"}'),
          ('m-missing','m-missing-source','msc_component','99999+9999:A','msc_component','99999+9999:B',
           'binary','pair','positive','test','ref','{"Comment":"SB9_3"}');
        INSERT INTO orbital_solution_evidence VALUES
          ('m-o1','m-s1','{"P":"2.0","Punit":"d"}'),
          ('m-o2','m-self-source','{"P":"3.0","Punit":"d"}'),
          ('m-o3','m-missing-source','{"P":"4.0","Punit":"d"}');
        INSERT INTO source_records VALUES
          ('m-s1','msc_sys'),('m-self-source','msc_sys'),
          ('m-missing-source','msc_sys'),('m-comp1','msc_comp');
        INSERT INTO identifier_claim_evidence VALUES
          ('m-i-a','m-s1','msc_component','00001+0001:A','primary_endpoint'),
          ('m-i-b','m-s1','msc_component','00001+0001:B','secondary_endpoint'),
          ('m-i-ma','m-missing-source','msc_component','99999+9999:A','primary_endpoint'),
          ('m-i-mb','m-missing-source','msc_component','99999+9999:B','secondary_endpoint'),
          ('m-i-source','m-comp1','msc_component','00001+0001:A','source_component');
        INSERT INTO stellar_parameter_sets VALUES ('m-ps1','m-comp1','00001+0001:A');
        INSERT INTO stellar_parameter_evidence VALUES ('m-p1','m-ps1','m-comp1','mass');
        INSERT INTO stellar_classification_evidence VALUES ('m-c1','m-comp1','00001+0001:A');
        INSERT INTO photometry_extinction_evidence VALUES ('m-ph1','m-comp1');
        INSERT INTO astrometry_distance_evidence VALUES ('m-a1','m-comp1');
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

    sb9_id = "sb9-test"
    sb9_dir = state / "derived/evidence_lake_v2/scientific_evidence" / sb9_id
    sb9_dir.mkdir(parents=True)
    con = duckdb.connect(str(sb9_dir / "scientific_evidence.duckdb"))
    con.execute(
        """
        CREATE TABLE identifier_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, namespace VARCHAR,
          identifier_normalized VARCHAR
        );
        CREATE TABLE relation_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR
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
        CREATE TABLE orbital_solution_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, relation_claim_id VARCHAR
        );
        INSERT INTO relation_claim_evidence VALUES
          ('sb-r1','sb-s1'),('sb-r2','sb-s2'),('sb-r3','sb-s3');
        INSERT INTO identifier_claim_evidence VALUES
          ('sb-i1','sb-s1','sb9_sequence','1'),
          ('sb-i2','sb-s2','sb9_sequence','2'),
          ('sb-i3','sb-s3','sb9_sequence','3');
        INSERT INTO stellar_parameter_sets VALUES
          ('sb-ps1','sb-s1','primary'),
          ('sb-ps2','sb-s1','secondary'),
          ('sb-ps3','sb-s2','primary');
        INSERT INTO stellar_parameter_evidence VALUES
          ('sb-p1','sb-ps1','sb-s1','primary','sb9.apparent_magnitude'),
          ('sb-p2','sb-ps2','sb-s1','secondary','sb9.apparent_magnitude'),
          ('sb-p3','sb-ps3','sb-s2','primary','sb9.apparent_magnitude');
        INSERT INTO stellar_classification_evidence VALUES
          ('sb-c1','sb-s1','primary'),
          ('sb-c2','sb-s1','secondary'),
          ('sb-c3','sb-s2','primary');
        INSERT INTO orbital_solution_evidence VALUES
          ('sb-o1','sb-o-source1','sb-r1'),
          ('sb-o2','sb-o-source2','sb-r1'),
          ('sb-o3','sb-o-source3','sb-r2'),
          ('sb-o4','sb-o-source4','sb-r3');
        """
    )
    con.close()

    orb6_id = "orb6-test"
    orb6_dir = state / "derived/evidence_lake_v2/scientific_evidence" / orb6_id
    orb6_dir.mkdir(parents=True)
    con = duckdb.connect(str(orb6_dir / "scientific_evidence.duckdb"))
    con.execute(
        """
        CREATE TABLE identifier_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, namespace VARCHAR,
          identifier_raw VARCHAR
        );
        CREATE TABLE orbital_solution_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR
        );
        INSERT INTO identifier_claim_evidence VALUES
          ('o-w1','o1','wds_id','00001+0001'),
          ('o-d1','o1','wds_discoverer_designation','TST   1AB'),
          ('o-w2','o2','wds_id','00001+0001'),
          ('o-d2','o2','wds_discoverer_designation','TST   2Aa1,2'),
          ('o-w3','o3','wds_id','00001+0001'),
          ('o-d3','o3','wds_discoverer_designation','TST   3');
        INSERT INTO orbital_solution_evidence VALUES
          ('o-orbit1','o1'),('o-orbit2','o2'),('o-orbit3','o3');
        """
    )
    con.close()

    sbx_id = "sbx-test"
    sbx_dir = state / "derived/evidence_lake_v2/scientific_evidence" / sbx_id
    sbx_dir.mkdir(parents=True)
    con = duckdb.connect(str(sbx_dir / "scientific_evidence.duckdb"))
    con.execute(
        """
        CREATE TABLE source_records (
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          source_table VARCHAR, logical_key_json JSON
        );
        CREATE TABLE identifier_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, namespace VARCHAR,
          identifier_raw VARCHAR, identifier_normalized VARCHAR
        );
        CREATE TABLE relation_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR,
          left_identity_namespace VARCHAR, left_identity_raw VARCHAR,
          left_component_scope VARCHAR, right_identity_namespace VARCHAR,
          right_identity_raw VARCHAR, right_component_scope VARCHAR,
          relation_kind VARCHAR, relation_scope VARCHAR, probability DOUBLE,
          probability_semantics VARCHAR, confidence_statistic_key VARCHAR,
          confidence_statistic_value_raw VARCHAR, confidence_statistic_value DOUBLE,
          confidence_statistic_unit VARCHAR, confidence_statistic_semantics VARCHAR,
          evidence_polarity VARCHAR, method VARCHAR, reference_raw VARCHAR,
          epoch_raw VARCHAR, quality_json JSON
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
        CREATE TABLE orbital_solution_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, relation_claim_id VARCHAR
        );
        CREATE TABLE astrometry_distance_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, quantity_key VARCHAR
        );
        INSERT INTO source_records VALUES
          ('sx-s1','multiplicity.sbx','sbx-test-release','sbx_systems','{"sn":"1"}'),
          ('sx-s2','multiplicity.sbx','sbx-test-release','sbx_systems','{"sn":"2"}'),
          ('sx-a1','multiplicity.sbx','sbx-test-release','sbx_alias','{"sn":"1"}'),
          ('sx-a2','multiplicity.sbx','sbx-test-release','sbx_alias','{"sn":"2"}'),
          ('sx-o1','multiplicity.sbx','sbx-test-release','sbx_orbits','{"sn":"1","on":"1"}'),
          ('sx-c1','multiplicity.sbx','sbx-test-release','sbx_configurations','{"sn":"1"}');
        INSERT INTO identifier_claim_evidence VALUES
          ('sx-seq1','sx-s1','sbx_sequence','1','1'),
          ('sx-sys1','sx-s1','sbx_system','SBX_1','SBX_1'),
          ('sx-p1','sx-s1','sbx_component','SBX_1:primary','SBX_1:primary'),
          ('sx-q1','sx-s1','sbx_component','SBX_1:secondary','SBX_1:secondary'),
          ('sx-seq2','sx-s2','sbx_sequence','2','2'),
          ('sx-sys2','sx-s2','sbx_system','SBX_2','SBX_2'),
          ('sx-p2','sx-s2','sbx_component','SBX_2:primary','SBX_2:primary'),
          ('sx-q2','sx-s2','sbx_component','SBX_2:secondary','SBX_2:secondary'),
          ('sx-gaia','sx-a1','gaia_dr3_source_id','123','123'),
          ('sx-seqa1','sx-a1','sbx_sequence','1','1'),
          ('sx-seqa2','sx-a2','sbx_sequence','2','2');
        INSERT INTO relation_claim_evidence VALUES
          ('sx-r1','sx-s1','sbx_component','SBX_1:primary','primary',
           'sbx_component','SBX_1:secondary','secondary','spectroscopic_binary',
           'pair',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'positive','test','ref',NULL,'{}'),
          ('sx-r2','sx-s2','sbx_component','SBX_2:primary','primary',
           'sbx_component','SBX_2:secondary','secondary','spectroscopic_binary',
           'pair',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'positive','test','ref',NULL,'{}'),
          ('sx-h1','sx-c1','sbx_system','SBX_1','child_subsystem',
           'sbx_system','SBX_2','parent_subsystem','hierarchical_parent',
           'system',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'positive','test','ref',NULL,'{}');
        INSERT INTO stellar_parameter_sets VALUES
          ('sx-ps1','sx-s1','primary'),('sx-ps2','sx-s2','primary');
        INSERT INTO stellar_parameter_evidence VALUES
          ('sx-pe1','sx-ps1','sx-s1','primary','sbx.apparent_magnitude'),
          ('sx-pe2','sx-ps2','sx-s2','primary','sbx.apparent_magnitude');
        INSERT INTO stellar_classification_evidence VALUES
          ('sx-cl1','sx-s1','primary'),('sx-cl2','sx-s2','primary');
        INSERT INTO orbital_solution_evidence VALUES
          ('sx-or1','sx-o1','sx-r1');
        INSERT INTO astrometry_distance_evidence VALUES
          ('sx-as1','sx-s1','parallax'),('sx-as2','sx-s2','parallax');
        """
    )
    con.close()

    wds_id = "wds-test"
    wds_dir = state / "derived/evidence_lake_v2/scientific_evidence" / wds_id
    wds_dir.mkdir(parents=True)
    con = duckdb.connect(str(wds_dir / "scientific_evidence.duckdb"))
    con.execute(
        """
        CREATE TABLE source_records (
          source_record_id VARCHAR, source_id VARCHAR, release_id VARCHAR,
          source_table VARCHAR, source_context_json JSON
        );
        CREATE TABLE identifier_claim_evidence (
          evidence_id VARCHAR, source_record_id VARCHAR, namespace VARCHAR,
          identifier_raw VARCHAR
        );
        INSERT INTO source_records VALUES
          ('w1','multiplicity.wds','wds-test-release','wdsweb_summ2','{"components":"AB"}'),
          ('w2','multiplicity.wds','wds-test-release','wdsweb_summ2','{"components":"Aa1,2"}');
        INSERT INTO identifier_claim_evidence VALUES
          ('w-w1','w1','wds_id','00001+0001'),
          ('w-d1','w1','wds_discoverer_designation','TST   1'),
          ('w-w2','w2','wds_id','00001+0001'),
          ('w-d2','w2','wds_discoverer_designation','TST   2');
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
                "component_parameter_authority": {"mass": "test-msc-mass"},
                "context_only_parameter_quantities": ["separation_from_main_component"],
                "classification_authority": "test-msc-classification",
                "photometry_authority": "test-msc-photometry",
                "astrometry_authority": "test-msc-astrometry",
                "hierarchy_orbit_authority": "test-msc-hierarchy-orbit",
                "orbit_table_authority": "test-msc-orbit-table",
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
                    "expected_parameter_sets": 1,
                    "expected_parameter_sets_bound": 1,
                    "expected_parameter_evidence": 1,
                    "expected_parameter_evidence_eligible": 1,
                    "expected_parameter_evidence_context_only": 0,
                    "expected_classification_evidence": 1,
                    "expected_classification_evidence_eligible": 1,
                    "expected_photometry_evidence": 1,
                    "expected_photometry_evidence_eligible": 1,
                    "expected_astrometry_evidence": 1,
                    "expected_astrometry_evidence_eligible": 1,
                    "expected_orbital_solutions": 3,
                    "expected_orbital_solutions_eligible": 1,
                    "expected_orbits_unresolved_msc_relation": 1,
                    "expected_orbits_invalid_msc_relation": 1,
                    "expected_orbits_missing_msc_relation": 0,
                    "expected_orbits_ambiguous_msc_relation": 0,
                    "expected_orbits_unparsed_pair": 0,
                    "expected_orbits_missing_pair_identity": 0,
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
            "sb9": {
                "source_id": "multiplicity.sb9",
                "release_id": "sb9-test-release",
                "evidence_build_id": sb9_id,
                "relation_binding_method": "test-exact-msc-sequence",
                "reference_pattern": "SB9_([0-9]+)",
                "parameter_authority": "test-sb9-parameter",
                "classification_authority": "test-sb9-classification",
                "orbit_authority": "test-sb9-orbit",
                "canonical_containment_promotion": False,
                "acceptance": {
                    "expected_relation_bindings": 3,
                    "expected_relations_accepted": 1,
                    "expected_relations_missing_reference": 1,
                    "expected_relations_ambiguous_reference": 1,
                    "expected_relations_unresolved_msc": 0,
                    "expected_parameter_sets": 3,
                    "expected_parameter_sets_eligible": 2,
                    "expected_parameter_evidence": 3,
                    "expected_parameter_evidence_eligible": 2,
                    "expected_classification_evidence": 3,
                    "expected_classification_evidence_eligible": 2,
                    "expected_orbital_solutions": 4,
                    "expected_orbital_solutions_eligible": 2,
                },
            },
            "orb6": {
                "source_id": "multiplicity.orb6",
                "release_id": "orb6-test-release",
                "evidence_build_id": orb6_id,
                "wds_source_id": "multiplicity.wds",
                "wds_release_id": "wds-test-release",
                "wds_evidence_build_id": wds_id,
                "wds_source_table": "wdsweb_summ2",
                "relation_binding_method": "test-exact-orb6-wds-msc-pair",
                "orbit_authority": "test-orb6-orbit",
                "canonical_containment_promotion": False,
                "acceptance": {
                    "expected_relation_bindings": 3,
                    "expected_relations_accepted": 1,
                    "expected_relations_missing_wds_pair": 1,
                    "expected_relations_ambiguous_wds_pair": 0,
                    "expected_relations_unparsed_wds_pair": 0,
                    "expected_relations_missing_msc_relation": 1,
                    "expected_relations_ambiguous_msc_relation": 0,
                    "expected_orbital_solutions": 3,
                    "expected_orbital_solutions_eligible": 1,
                },
            },
            "sbx": {
                "source_id": "multiplicity.sbx",
                "release_id": "sbx-test-release",
                "evidence_build_id": sbx_id,
                "system_binding_method": "test-sbx-exact-identifier-consensus",
                "component_binding_method": "test-sbx-release-component",
                "parameter_authority": "test-sbx-parameter",
                "classification_authority": "test-sbx-classification",
                "orbit_authority": "test-sbx-orbit",
                "astrometry_authority": "test-sbx-astrometry-context",
                "canonical_containment_promotion": False,
                "acceptance": {
                    "expected_system_bindings": 2,
                    "expected_systems_accepted": 1,
                    "expected_systems_missing": 1,
                    "expected_systems_ambiguous": 0,
                    "expected_component_entities": 4,
                    "expected_components_accepted": 2,
                    "expected_components_missing": 2,
                    "expected_components_ambiguous": 0,
                    "expected_binary_relations": 2,
                    "expected_binary_relations_accepted": 1,
                    "expected_binary_relations_unresolved": 1,
                    "expected_hierarchy_relations": 1,
                    "expected_hierarchy_relations_accepted": 0,
                    "expected_hierarchy_relations_unresolved": 1,
                    "expected_parameter_sets": 2,
                    "expected_parameter_sets_eligible": 1,
                    "expected_parameter_evidence": 2,
                    "expected_parameter_evidence_eligible": 1,
                    "expected_classification_evidence": 2,
                    "expected_classification_evidence_eligible": 1,
                    "expected_orbital_solutions": 1,
                    "expected_orbital_solutions_eligible": 1,
                    "expected_astrometry_evidence": 2,
                    "expected_astrometry_context_only": 1,
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
    assert con.execute(
        "SELECT count(*) FROM msc_stellar_parameter_projection WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0] == 1
    assert con.execute(
        "SELECT count(*) FROM msc_photometry_projection WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0] == 1
    assert dict(con.execute(
        "SELECT binding_status,count(*) FROM msc_orbit_solution_bindings GROUP BY 1"
    ).fetchall()) == {
        "accepted": 1,
        "invalid_msc_relation": 1,
        "unresolved_msc_relation": 1,
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
    assert dict(con.execute(
        "SELECT binding_status,count(*) FROM sb9_relation_bindings GROUP BY 1"
    ).fetchall()) == {
        "accepted": 1,
        "ambiguous_reference": 1,
        "missing_reference": 1,
    }
    assert con.execute(
        "SELECT count(*) FROM sb9_classification_projection WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0] == 2
    assert con.execute(
        "SELECT count(*) FROM sb9_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0] == 2
    assert dict(con.execute(
        "SELECT binding_status,count(*) FROM orb6_relation_bindings GROUP BY 1"
    ).fetchall()) == {
        "accepted": 1,
        "missing_msc_relation": 1,
        "missing_wds_pair": 1,
    }
    assert con.execute(
        "SELECT secondary_component_label FROM orb6_relation_bindings WHERE source_record_id='o2'"
    ).fetchone()[0] == "Aa2"
    assert con.execute(
        "SELECT count(*) FROM orb6_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0] == 1
    assert dict(con.execute(
        "SELECT binding_status,count(*) FROM sbx_system_bindings GROUP BY 1"
    ).fetchall()) == {"accepted": 1, "missing": 1}
    assert dict(con.execute(
        "SELECT projection_status,count(*) FROM sbx_relation_evidence_projection GROUP BY 1"
    ).fetchall()) == {
        "accepted_relation_evidence": 1,
        "unresolved_endpoint_evidence": 2,
    }
    assert con.execute(
        "SELECT count(*) FROM sbx_stellar_parameter_projection WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0] == 1
    assert con.execute(
        "SELECT count(*) FROM sbx_astrometry_projection WHERE projection_status='context_only_evidence'"
    ).fetchone()[0] == 1
    assert con.execute(
        "SELECT count(*) FROM sbx_astrometry_projection WHERE projection_status='eligible_for_quantity_selection'"
    ).fetchone()[0] == 0
    con.close()

    audited = artifact_audit.audit(artifact=Path(first["artifact_path"]), policy_path=policy)
    assert audited["status"] == "pass"
    assert audited["failing_checks"] == {}
