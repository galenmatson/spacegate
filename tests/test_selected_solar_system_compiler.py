from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_selected_solar_system_evidence as compiler  # noqa: E402


def fixture_policy() -> dict:
    return {
        "schema_version": "spacegate.e5_solar_system_policies.v1",
        "policy_version": "test-solar-v1",
        "compiler_version": "test",
        "canonical_reference_build_id": "fixture",
        "source": {
            "source_id": "solar_system.jpl_horizons_authority",
            "release_id": "release",
            "evidence_build_id": "e4",
            "object_source_table": "objects",
            "target_namespace": "jpl_horizons_target",
            "target_claim_scope": "natural_solar_system_target",
            "relation_kind": "jpl_horizons_orbit_center",
            "component_source_catalog": "sol_authority",
            "canonical_identifier_json_key": "jpl_horizons_command",
            "external_reference_origins": {"0": "solar_system_barycenter"},
            "required_solution_contract": {
                "frame_raw": "ICRF/ecliptic/TDB/AU-D",
                "method": "elements",
                "model": "osculating",
                "normalization_version": "norm-v1",
            },
            "physical_parameter_set_kind": "physical",
            "physical_normalization_version": "physical-v1",
            "acceptance": {},
        },
    }


def make_fixture() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE SCHEMA evidence; CREATE SCHEMA core; CREATE SCHEMA arm")
    con.execute(
        """
        CREATE TABLE evidence.source_records (
          source_record_id VARCHAR,source_id VARCHAR,release_id VARCHAR,
          source_table VARCHAR,logical_key_json JSON
        );
        CREATE TABLE evidence.identifier_claim_evidence (
          evidence_id VARCHAR,source_record_id VARCHAR,namespace VARCHAR,
          identifier_normalized VARCHAR,claim_scope VARCHAR
        );
        CREATE TABLE evidence.relation_claim_evidence (
          evidence_id VARCHAR,source_record_id VARCHAR,relation_kind VARCHAR,
          left_identity_raw VARCHAR,right_identity_raw VARCHAR
        );
        CREATE TABLE evidence.orbital_solution_evidence (
          evidence_id VARCHAR,source_record_id VARCHAR,relation_claim_id VARCHAR,
          solution_key JSON,parameter_set_raw JSON,epoch_raw VARCHAR,frame_raw VARCHAR,
          method VARCHAR,model VARCHAR,reference_raw VARCHAR,quality_json JSON,
          normalization_version VARCHAR
        );
        CREATE TABLE evidence.solar_system_object_parameter_sets (
          evidence_id VARCHAR,parameter_schema_id VARCHAR,source_record_id VARCHAR,
          component_scope VARCHAR,parameter_set_kind VARCHAR,values_json JSON,
          epoch_raw VARCHAR,method VARCHAR,model VARCHAR,reference_raw VARCHAR,
          quality_json JSON,normalization_version VARCHAR
        );
        CREATE TABLE core.stars (
          star_id BIGINT,catalog_ids_json JSON
        );
        CREATE TABLE arm.component_entities (
          component_entity_id BIGINT,stable_component_key VARCHAR,
          component_type VARCHAR,core_object_type VARCHAR,core_object_id BIGINT,
          source_catalog VARCHAR,source_pk VARCHAR
        );
        """
    )
    con.execute(
        """
        INSERT INTO evidence.source_records VALUES
          ('s1','solar_system.jpl_horizons_authority','release','objects','{"source_pk":"1"}'),
          ('s2','solar_system.jpl_horizons_authority','release','objects','{"source_pk":"2"}');
        INSERT INTO evidence.identifier_claim_evidence VALUES
          ('i1','s1','jpl_horizons_target','10','natural_solar_system_target'),
          ('i2','s2','jpl_horizons_target','20','natural_solar_system_target');
        INSERT INTO evidence.relation_claim_evidence VALUES
          ('r1','s1','jpl_horizons_orbit_center','10','20'),
          ('r2','s2','jpl_horizons_orbit_center','20','0');
        INSERT INTO evidence.orbital_solution_evidence VALUES
          ('o1','s1','r1','{}','{"orbital_period_days":"10","semi_major_axis_au":"1","eccentricity":"0.1","periapsis_distance_au":"0.9","inclination_deg":"2","longitude_ascending_node_deg":"3","argument_periapsis_deg":"4","time_periapsis_tdb_jd":"2449999","mean_motion_deg_day":"36","mean_anomaly_deg":"5","true_anomaly_deg":"6","apoapsis_distance_au":"1.1"}','2450000.5','ICRF/ecliptic/TDB/AU-D','elements','osculating','ref','{}','norm-v1'),
          ('o2','s2','r2','{}','{"orbital_period_days":"20","semi_major_axis_au":"2","eccentricity":"0.2","periapsis_distance_au":"1.6","inclination_deg":"3","longitude_ascending_node_deg":"4","argument_periapsis_deg":"5","time_periapsis_tdb_jd":"2449998","mean_motion_deg_day":"18","mean_anomaly_deg":"6","true_anomaly_deg":"7","apoapsis_distance_au":"2.4"}','2450000.5','ICRF/ecliptic/TDB/AU-D','elements','osculating','ref','{}','norm-v1');
        INSERT INTO evidence.solar_system_object_parameter_sets VALUES
          ('p1','schema','s1','natural_solar_system_target','physical','[1000.0,2e20]',NULL,'elements',NULL,'ref','{}','physical-v1');
        INSERT INTO core.stars VALUES (22,'{"jpl_horizons_command":"20"}');
        INSERT INTO arm.component_entities VALUES
          (1,'component:a','planet','planet',11,'sol_authority','1'),
          (2,'component:b','main_sequence','star',22,'sol_authority','canonical-star');
        """
    )
    compiler.create_schema(con)
    return con


def test_solar_projection_binds_targets_centers_and_reference_origin() -> None:
    con = make_fixture()
    observed = compiler.materialize(con, policy=fixture_policy())
    assert observed == {
        "target_bindings": 2,
        "targets_accepted": 2,
        "targets_missing": 0,
        "targets_ambiguous": 0,
        "relation_bindings": 2,
        "relations_accepted": 1,
        "relations_reference_origin": 1,
        "relations_missing": 0,
        "relations_ambiguous": 0,
        "orbital_solutions": 2,
        "orbital_solutions_complete_elements": 2,
        "orbital_solutions_eligible": 1,
        "orbital_solutions_reference_context": 1,
        "physical_parameter_sets": 1,
        "physical_parameter_sets_eligible": 1,
        "radius_values": 1,
        "mass_values": 1,
        "canonical_relation_promotions": 0,
    }
    assert compiler.verify(con) == {
        "duplicate_target_binding_ids": 0,
        "duplicate_source_target_bindings": 0,
        "accepted_targets_without_one_candidate": 0,
        "accepted_targets_without_components": 0,
        "unaccepted_targets_with_components": 0,
        "duplicate_relation_binding_ids": 0,
        "accepted_relations_without_two_components": 0,
        "reference_relations_without_declared_origins": 0,
        "eligible_orbits_without_two_components": 0,
        "reference_orbits_without_origins": 0,
        "eligible_orbits_with_invalid_contract": 0,
        "eligible_orbits_without_complete_elements": 0,
        "eligible_physical_sets_without_targets": 0,
        "canonical_relation_promotions": 0,
    }
    methods = dict(con.execute(
        "SELECT target_command,binding_method FROM solar_target_bindings"
    ).fetchall())
    assert methods == {
        "10": "exact_sol_authority_source_key",
        "20": "exact_canonical_jpl_command",
    }
    con.close()
