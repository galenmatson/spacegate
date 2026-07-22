from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_permanent_identity_seed as compiler  # noqa: E402
import verify_permanent_identity_seed_reproduction as reproduction  # noqa: E402


def test_checked_in_policy_is_identity_only() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    compiler.validate_policy(policy)
    assert policy["scientific_authority"] is False
    assert policy["rules"]["future_builds_may_read_stability_databases"] is False


def test_policy_rejects_scientific_columns() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    policy["tables"]["hierarchy_nodes"].append("mass_msun")
    with pytest.raises(ValueError, match="scientific columns"):
        compiler.validate_policy(policy)


def test_compile_small_identity_seed(tmp_path: Path) -> None:
    state = tmp_path / "state"
    identity_id = "fixture_identity"
    hierarchy_build = "fixture_shadow"
    identity_dir = state / "derived/evidence_lake_v2/identity" / identity_id
    hierarchy_dir = state / "out" / hierarchy_build
    identity_dir.mkdir(parents=True)
    hierarchy_dir.mkdir(parents=True)
    identity_db = identity_dir / "identity_graph.duckdb"
    con = duckdb.connect(str(identity_db))
    con.execute("CREATE TABLE canonical_object_nodes(object_node_key VARCHAR,object_type VARCHAR,stable_object_key VARCHAR,canonical_row_id VARCHAR,system_stable_object_key VARCHAR,display_name VARCHAR,canonical_reference_build_id VARCHAR)")
    con.execute("INSERT INTO canonical_object_nodes VALUES ('object:system','system','system:1','1','system:1','System 1','reference'),('object:star','star','star:1','1','system:1','Star 1','reference')")
    con.close()
    hierarchy_db = hierarchy_dir / "canonical_hierarchy.duckdb"
    con = duckdb.connect(str(hierarchy_db))
    con.execute("CREATE TABLE hierarchy_nodes(hierarchy_node_key VARCHAR,node_kind VARCHAR,component_family VARCHAR,component_type VARCHAR,canonical_key VARCHAR,display_name VARCHAR,wds_id VARCHAR,member_role VARCHAR,source_basis VARCHAR)")
    con.execute("CREATE TABLE hierarchy_edges(hierarchy_edge_id BIGINT,parent_node_key VARCHAR,child_node_key VARCHAR,edge_kind VARCHAR,member_role VARCHAR,source_basis VARCHAR,confidence_score DOUBLE,supporting_edge_count BIGINT)")
    con.execute("INSERT INTO hierarchy_nodes VALUES ('system:1','system','system','system','system:1','System 1',NULL,NULL,'canonical_system'),('star:1','star','star','star','star:1','Star 1',NULL,NULL,'canonical_star')")
    con.execute("INSERT INTO hierarchy_edges VALUES (1,'system:1','star:1','contains',NULL,'canonical_root_star',1.0,1)")
    con.close()
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    policy["identity_graph_id"] = identity_id
    policy["hierarchy_source_build_id"] = hierarchy_build
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    manifest = compiler.compile_seed(policy_path, state)

    assert manifest["status"] == "pass"
    assert manifest["scientific_authority"] is False
    assert manifest["verification"] == {
        "canonical_objects_missing_nodes": 0,
        "duplicate_edge_relationships": 0,
        "duplicate_edge_ids": 0,
        "duplicate_node_keys": 0,
        "edges_with_missing_child": 0,
        "edges_with_missing_parent": 0,
        "source_edge_id_collision_rows": 0,
    }
    assert manifest["products"]["hierarchy_nodes"]["row_count"] == 2
    assert manifest["products"]["hierarchy_edges"]["row_count"] == 1


def test_reproduction_comparison_ignores_timestamps_and_timings() -> None:
    products = {"hierarchy_nodes": {"sha256": "a", "row_count": 2}}
    reference = {
        "seed_id": "seed",
        "policy_sha256": "policy",
        "identity_graph_sha256": "identity",
        "hierarchy_source_sha256": "hierarchy",
        "products": products,
        "verification": {"duplicate_node_keys": 0},
        "scientific_authority": False,
        "created_at": "first",
        "timings": {"export": {"wall_seconds": 1}},
    }
    reproduced = dict(reference, created_at="second", timings={"export": {"wall_seconds": 2}})

    report = reproduction.compare(reference, reproduced)

    assert report["status"] == "pass"
    assert report["failing_checks"] == []
