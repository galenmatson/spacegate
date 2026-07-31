from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "audit_msc_component_identity_bridge",
    ROOT / "scripts/audit_msc_component_identity_bridge.py",
)
assert SPEC and SPEC.loader
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


def make_arm(path: Path, *, candidate: bool, unrelated_change: bool = False) -> None:
    con = duckdb.connect(str(path))
    for table in ("component_entities", "system_hierarchy_edges", "orbit_edges", "orbital_solutions"):
        con.execute(f"CREATE TABLE {table}(id INTEGER)")
        con.execute(f"INSERT INTO {table} VALUES (1)")
    con.execute(
        """
        CREATE TABLE stellar_leaf_display_classifications(
          hierarchy_node_key VARCHAR,classification_value VARCHAR,
          classification_status VARCHAR,evidence_basis VARCHAR,selected_fact_id VARCHAR,
          source_catalog VARCHAR,source_value VARCHAR,system_id BIGINT,display_name VARCHAR
        );
        CREATE TABLE msc_runtime_leaf_bindings(
          binding_id BIGINT,component_entity_id VARCHAR,wds_id_raw VARCHAR,
          component_label_raw VARCHAR,component_label_normalized VARCHAR,
          runtime_binding_status VARCHAR,runtime_binding_reason VARCHAR,
          hierarchy_node_key VARCHAR,runtime_component_key VARCHAR,
          source_component_key VARCHAR,source_candidate_count BIGINT,
          runtime_identity_bridge_id VARCHAR,runtime_identity_bridge_build_id VARCHAR,
          runtime_identity_bridge_policy_version VARCHAR,canonical_containment BOOLEAN
        );
        CREATE TABLE stellar_orbit_endpoint_bindings(
          endpoint_binding_id VARCHAR,component_entity_id VARCHAR,endpoint_kind VARCHAR,
          binding_status VARCHAR,hierarchy_node_key VARCHAR
        );
        """
    )
    if candidate:
        con.execute(
            """
            INSERT INTO stellar_leaf_display_classifications VALUES
              ('canon:leaf:msc:00001+0001:ab','M','source',
               'selected_sb9_component_spectral_type','fact-ab','multiplicity.sb9',
               'dM1e',1,'Test Ab');
            INSERT INTO msc_runtime_leaf_bindings VALUES
              (1,'leaf-ab','00001+0001','Ab','Ab','accepted',
               'exact_release_scoped_leaf_identity_bridge',
               'canon:leaf:msc:00001+0001:ab','comp:msc:wds:00001+0001:ab',
               'comp:msc:release:00001+0001:Ab',2,'bridge-ab','bridge-build',
               'bridge-policy',false),
              (2,'group-ab','00001+0001','AB','AB','ambiguous',
               'case_significant_source_collision',NULL,NULL,
               'comp:msc:release:00001+0001:AB',2,NULL,NULL,NULL,false);
            INSERT INTO stellar_orbit_endpoint_bindings VALUES
              ('bridge-ab','leaf-ab','leaf','accepted','canon:leaf:msc:00001+0001:ab');
            """
        )
    else:
        con.execute(
            """
            INSERT INTO stellar_leaf_display_classifications VALUES
              ('canon:leaf:msc:00001+0001:ab','UNKNOWN','missing',
               'no_selected_leaf_classification',NULL,NULL,NULL,1,'Test Ab');
            INSERT INTO msc_runtime_leaf_bindings VALUES
              (1,'leaf-ab','00001+0001','Ab','Ab','ambiguous',
               'case_significant_source_collision',NULL,NULL,
               'comp:msc:release:00001+0001:Ab',2,NULL,NULL,NULL,false),
              (2,'group-ab','00001+0001','AB','AB','ambiguous',
               'case_significant_source_collision',NULL,NULL,
               'comp:msc:release:00001+0001:AB',2,NULL,NULL,NULL,false);
            """
        )
    if unrelated_change:
        con.execute(
            """
            INSERT INTO stellar_leaf_display_classifications VALUES
              ('canon:leaf:msc:00002+0002:b','M','source','unrelated','fact-b',
               'catalog','M3V',2,'Unrelated B')
            """
        )
    con.close()


def test_exact_identity_bridge_recovery_passes(tmp_path: Path) -> None:
    reference = tmp_path / "reference.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    make_arm(reference, candidate=False)
    make_arm(candidate, candidate=True)

    report = AUDIT.audit(candidate_arm=candidate, reference_arm=reference)

    assert report["status"] == "pass"
    assert report["counts"]["binding_deltas"] == 1
    assert report["counts"]["unknown_to_known"] == 1
    assert report["checks"] == {key: 0 for key in report["checks"]}


def test_change_outside_identity_bridge_fails(tmp_path: Path) -> None:
    reference = tmp_path / "reference.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    make_arm(reference, candidate=False)
    make_arm(candidate, candidate=True, unrelated_change=True)

    report = AUDIT.audit(candidate_arm=candidate, reference_arm=reference)

    assert report["status"] == "fail"
    assert report["checks"]["classification_changes_outside_exact_bridge"] == 1
    assert report["checks"]["removed_or_added_leaf_rows"] == 1
