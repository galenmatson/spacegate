from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_clean_runtime_arm", ROOT / "scripts/compile_e7_clean_runtime_arm.py"
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def test_checked_in_policy_is_bounded_and_forbids_stability_authority() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_clean_runtime_arm.json")
    COMPILER.validate_policy(policy)

    assert policy["rules"]["open_stability_databases"] is False
    assert policy["rules"]["source_relation_claims_create_containment"] is False
    assert policy["rules"]["context_only_orbits_create_renderable_edges"] is False
    assert policy["runtime_graph_status"]["cross_source_orbit_selection"].startswith("pending_")


def test_policy_rejects_relation_claim_containment() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_clean_runtime_arm.json")
    policy["rules"]["source_relation_claims_create_containment"] = True

    try:
        COMPILER.validate_policy(policy)
    except ValueError as exc:
        assert "unsafe clean runtime ARM rules" in str(exc)
    else:
        raise AssertionError("unsafe containment rule was accepted")


def test_component_keys_are_stable_across_runtime_numeric_ids() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE nodes(
          node_kind VARCHAR, canonical_key VARCHAR, wds_id VARCHAR,
          hierarchy_node_key VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO nodes VALUES
          ('system','canon:system:stable:system:sol',NULL,'canon:system:stable:system:sol'),
          ('star','canon:star:stable:star:sol:sun',NULL,'canon:star:stable:star:sol:sun'),
          ('planet','canon:planet:nasa_source:4',NULL,'canon:planet:nasa_source:4'),
          ('inferred_star_leaf',NULL,'07346+3153','canon:leaf:msc:07346+3153:ab')
        """
    )
    rows = con.execute(
        f"SELECT {COMPILER.component_key_sql('n')} FROM nodes n ORDER BY hierarchy_node_key"
    ).fetchall()
    assert {row[0] for row in rows} == {
        "comp:system:canon:system:stable:system:sol",
        "comp:star:canon:star:stable:star:sol:sun",
        "comp:planet:canon:planet:nasa_source:4",
        "comp:msc:wds:07346+3153:ab",
    }


def test_runtime_distance_adapter_uses_explicit_light_year_conversion() -> None:
    assert COMPILER.PARSECS_TO_LIGHT_YEARS == 3.26156
    source = (ROOT / "scripts/compile_e7_clean_runtime_arm.py").read_text(encoding="utf-8")
    assert "PARSECS_TO_LIGHT_YEARS" in source
    assert "sys.dist_pc" not in source


def test_component_graph_projects_each_node_kind_without_cross_product_joins() -> None:
    con = duckdb.connect(":memory:")
    con.execute("ATTACH ':memory:' AS core")
    con.execute("ATTACH ':memory:' AS hierarchy")
    con.execute(
        """
        CREATE TABLE core.systems(
          system_id HUGEINT, stable_object_key VARCHAR, wds_id VARCHAR,
          ra_deg DOUBLE, dec_deg DOUBLE, dist_ly DOUBLE
        );
        CREATE TABLE core.stars(
          star_id HUGEINT, system_id HUGEINT, stable_object_key VARCHAR,
          ra_deg DOUBLE, dec_deg DOUBLE, dist_ly DOUBLE, component VARCHAR
        );
        CREATE TABLE core.planets(
          planet_id HUGEINT, system_id HUGEINT, stable_object_key VARCHAR
        );
        CREATE TABLE hierarchy.hierarchy_nodes(
          hierarchy_node_key VARCHAR,node_kind VARCHAR,component_family VARCHAR,
          component_type VARCHAR,canonical_key VARCHAR,display_name VARCHAR,
          wds_id VARCHAR,member_role VARCHAR,source_basis VARCHAR
        );
        CREATE TABLE hierarchy.hierarchy_edges(
          hierarchy_edge_id BIGINT,source_hierarchy_edge_id BIGINT,
          parent_node_key VARCHAR,child_node_key VARCHAR,edge_kind VARCHAR,
          member_role VARCHAR,source_basis VARCHAR,confidence_score DOUBLE,
          supporting_edge_count BIGINT
        );
        INSERT INTO core.systems VALUES
          (1,'canon:system:test','12345+6789',10,20,32.6156),
          (4,'canon:system:context','12345+6789',10,20,32.6156);
        INSERT INTO core.stars VALUES
          (2,1,'canon:star:test',10,20,32.6156,'A');
        INSERT INTO core.planets VALUES
          (3,1,'canon:planet:test');
        INSERT INTO hierarchy.hierarchy_nodes VALUES
          ('canon:system:test','system','system','system','canon:system:test','Test','12345+6789',NULL,'canonical_system'),
          ('canon:system:context','system','system','system','canon:system:context','Context','12345+6789',NULL,'canonical_system'),
          ('canon:star:test','star','star','star','canon:star:test','Test A','12345+6789','A','canonical_star'),
          ('canon:planet:test','planet','planet','planet','canon:planet:test','Test b',NULL,'planet','canonical_planet'),
          ('canon:leaf:msc:12345+6789:b','inferred_star_leaf','star','star',NULL,'Test B','12345+6789','b','msc_inferred_leaf');
        INSERT INTO hierarchy.hierarchy_edges VALUES
          (1,1,'canon:system:test','canon:star:test','contains','star','canonical_root_star',1,1),
          (2,2,'canon:star:test','canon:leaf:msc:12345+6789:b','contains','star','msc_role_leaf',0.9,1),
          (3,3,'canon:system:context','canon:star:test','associated','star','wds_context',0.8,1);
        """
    )

    COMPILER.create_component_graph(con, "test-build")

    assert con.execute("SELECT count(*) FROM component_entities").fetchone()[0] == 5
    assert con.execute("SELECT count(*) FROM system_hierarchy_edges").fetchone()[0] == 3
    row = con.execute(
        "SELECT system_id,dist_pc FROM runtime_component_nodes WHERE node_kind='system'"
    ).fetchone()
    assert row[0] == 1
    assert abs(row[1] - 10.0) < 1e-9
    assert con.execute(
        "SELECT system_id,distinct_system_count FROM runtime_node_system_bindings "
        "WHERE hierarchy_node_key='canon:leaf:msc:12345+6789:b'"
    ).fetchone() == (1, 1)

    con.execute("ATTACH ':memory:' AS science")
    con.execute(
        """
        CREATE TABLE selected_stellar_display_classifications(
          selected_display_classification_id BIGINT,build_id VARCHAR,star_id HUGEINT,
          system_id HUGEINT,stable_object_key VARCHAR,classification_value VARCHAR,
          classification_status VARCHAR,evidence_basis VARCHAR,selected_fact_id VARCHAR,
          source_value VARCHAR,confidence_score DOUBLE,lineage_kind VARCHAR,lineage_id VARCHAR,
          distinct_candidate_class_count INTEGER,candidate_classes_json VARCHAR,
          distinct_direct_class_count INTEGER,direct_classes_json VARCHAR,
          has_classification_conflict BOOLEAN,has_alternative_disagreement BOOLEAN,
          projection_version VARCHAR
        );
        INSERT INTO selected_stellar_display_classifications VALUES
          (1,'science',2,1,'canon:star:test','G','source','selected_test','fact-1',
           'G2V',0.99,'selected_fact','fact-1',1,'[\"G\"]',1,'[\"G\"]',false,false,'v1');
        CREATE TABLE science.evidence_component_msc_component_entities(
          component_entity_id VARCHAR,source_id VARCHAR,release_id VARCHAR,
          binding_status VARCHAR,canonical_system_stable_object_key VARCHAR,
          component_label_normalized VARCHAR
        );
        CREATE TABLE science.evidence_component_msc_classification_projection(
          component_entity_id VARCHAR,projection_status VARCHAR,classification_raw VARCHAR,
          classification_normalized VARCHAR,evidence_id VARCHAR
        );
        CREATE TABLE science.evidence_component_msc_stellar_parameter_projection(
          component_entity_id VARCHAR,projection_status VARCHAR,quantity_key VARCHAR,
          normalized_value DOUBLE,evidence_id VARCHAR,value_raw VARCHAR
        );
        """
    )
    COMPILER.create_leaf_classifications(con, "test-build")
    rows = con.execute(
        "SELECT hierarchy_node_key,classification_value FROM stellar_leaf_display_classifications ORDER BY 1"
    ).fetchall()
    assert rows == [("canon:leaf:msc:12345+6789:b", "UNKNOWN")]


def test_compiler_has_no_stability_or_named_object_inputs() -> None:
    source = (ROOT / "scripts/compile_e7_clean_runtime_arm.py").read_text(encoding="utf-8")
    lowered = source.lower()

    assert "stability_reference_build_id" not in lowered
    assert "served/current" not in lowered
    assert "20260717t0614z" not in lowered
    assert "castor" not in lowered
    assert "sirius" not in lowered
    assert "nu sco" not in lowered


def test_policy_json_is_canonical_object() -> None:
    value = json.loads((ROOT / "config/evidence_lake/e7_clean_runtime_arm.json").read_text())
    assert isinstance(value, dict)
    assert set(value["inputs"]) == {"clean_runtime_core", "clean_science", "clean_wise"}


def test_independent_verifier_contract_matches_compiler_policy() -> None:
    verifier_path = ROOT / "scripts/verify_e7_clean_runtime_arm.py"
    spec = importlib.util.spec_from_file_location("verify_e7_clean_runtime_arm", verifier_path)
    assert spec and spec.loader
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_clean_runtime_arm.json")

    assert set(policy["selected_science_tables"]) == verifier.SCIENCE_TABLES
    assert set(policy["clean_wise_tables"]) == verifier.WISE_TABLES
    assert verifier.EXPECTED_VIEWS == {
        "e6_selected_planet_parameters",
        "e6_selected_stellar_display_classifications",
        "e6_selected_stellar_parameters",
    }
