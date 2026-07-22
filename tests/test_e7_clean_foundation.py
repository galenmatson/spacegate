from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compile_e7_clean_foundation import compile_foundation, file_sha256  # noqa: E402


def _write_parquet(path: Path, sql: str) -> None:
    con = duckdb.connect()
    try:
        con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def test_clean_foundation_compiles_without_stability_database(tmp_path: Path) -> None:
    state = tmp_path / "state"
    inputs = state / "inputs"
    inputs.mkdir(parents=True)
    identity = inputs / "identity.duckdb"
    con = duckdb.connect(str(identity))
    con.execute(
        """
        CREATE TABLE canonical_object_nodes(
          object_node_key VARCHAR,object_type VARCHAR,stable_object_key VARCHAR,
          canonical_row_id VARCHAR,system_stable_object_key VARCHAR,
          display_name VARCHAR,canonical_reference_build_id VARCHAR
        );
        INSERT INTO canonical_object_nodes VALUES
          ('os1','system','sys:1','1','sys:1','Alpha','migration'),
          ('os2','system','sys:2','2','sys:2','Beta','migration'),
          ('ot1','star','star:1','11','sys:1','Alpha A','migration'),
          ('ot2','star','star:2','12','sys:1','Alpha B','migration'),
          ('ot3','star','star:3','13','sys:2','Beta A','migration'),
          ('op1','planet','planet:1','21','sys:1','Alpha b','migration');
        CREATE TABLE canonical_identifier_bindings(
          binding_key VARCHAR,identifier_node_key VARCHAR,object_node_key VARCHAR,
          stable_object_key VARCHAR,system_stable_object_key VARCHAR,namespace VARCHAR,
          id_value_raw VARCHAR,id_value_norm VARCHAR,identifier_source_id VARCHAR,
          identifier_release_id VARCHAR,is_canonical BOOLEAN,resolution_method VARCHAR,
          resolution_confidence DOUBLE,source_catalog VARCHAR,source_version VARCHAR,
          source_record_id VARCHAR,evidence_json VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('b1','i1','ot1','star:1','sys:1','tic','123','123','tic','r1',true,
           'authoritative',1.0,'tic','r1','123','{}');
        CREATE TABLE identity_quarantine(
          quarantine_key VARCHAR,quarantine_kind VARCHAR,outcome VARCHAR,reason VARCHAR,
          subject_node_key VARCHAR,candidate_nodes_json JSON,evidence_json VARCHAR
        );
        """
    )
    con.close()

    hierarchy_nodes = inputs / "hierarchy_nodes.parquet"
    hierarchy_edges = inputs / "hierarchy_edges.parquet"
    aliases = inputs / "aliases.parquet"
    placements = inputs / "placements.parquet"
    _write_parquet(
        hierarchy_nodes,
        """
        SELECT * FROM (VALUES
          ('sys:1','system','system','system','sys:1','Alpha',NULL,NULL,'identity'),
          ('star:1','star','stellar','star','star:1','Alpha A',NULL,NULL,'identity'),
          ('planet:1','planet','planetary','planet','planet:1','Alpha b',NULL,NULL,'identity')
        ) t(hierarchy_node_key,node_kind,component_family,component_type,canonical_key,
            display_name,wds_id,member_role,source_basis)
        """,
    )
    _write_parquet(
        hierarchy_edges,
        """
        SELECT * FROM (VALUES
          (1::BIGINT,1::BIGINT,'sys:1','star:1','contains',NULL,'identity',1.0,1::BIGINT),
          (2::BIGINT,2::BIGINT,'star:1','planet:1','contains',NULL,'identity',1.0,1::BIGINT)
        ) t(hierarchy_edge_id,source_hierarchy_edge_id,parent_node_key,child_node_key,
            edge_kind,member_role,source_basis,confidence_score,supporting_edge_count)
        """,
    )
    _write_parquet(
        aliases,
        """
        SELECT 1::BIGINT alias_seed_id,'star:1' stable_object_key,'sys:1' system_stable_object_key,
               'star' target_type,'Common Alpha' alias_raw,'common alpha' alias_norm,
               'proper_name' alias_kind,5::INTEGER alias_priority,true is_primary,
               'names' source_catalog,'r1' source_version,1::BIGINT source_pk
        """,
    )
    _write_parquet(
        placements,
        """
        SELECT * FROM (VALUES
          ('sys:1','star:1',10.0,20.0,5.0,200.0,16.3078,1.0,2.0,3.0,'ICRS','J2016.0','selected','test','selected','p1'),
          ('sys:2','star:3',30.0,40.0,10.0,100.0,32.6156,4.0,5.0,6.0,'ICRS','J2016.0','selected','test','selected','p1')
        ) t(system_stable_object_key,representative_object_key,ra_deg,dec_deg,distance_pc,
            parallax_mas,dist_ly,x_helio_ly,y_helio_ly,z_helio_ly,coordinate_frame,
            coordinate_epoch,placement_source,placement_method,placement_status,policy_version)
        """,
    )
    specs = {}
    for name, path in {
        "identity_graph": identity,
        "hierarchy_nodes": hierarchy_nodes,
        "hierarchy_edges": hierarchy_edges,
        "aliases": aliases,
        "system_placements": placements,
    }.items():
        specs[name] = {
            "id": name,
            "relative_path": str(path.relative_to(state)),
            "sha256": file_sha256(path),
        }
    policy = {
        "schema_version": "spacegate.e7_clean_foundation_policy.v1",
        "policy_version": "test.1",
        "compiler_version": "test.1",
        "rules": {
            "open_stability_databases": False,
            "scientific_authority_from_identity_seed": False,
            "require_every_canonical_system_placed": True,
            "require_every_alias_bound": True,
            "require_every_identifier_bound": True,
            "semantic_search_duplicates_allowed": False,
        },
        "inputs": specs,
        "identifier_display_prefixes": {"tic": "TIC"},
    }
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    manifest = compile_foundation(
        policy_path, state, tmp_path / "out", link_into_state=False
    )
    assert manifest["status"] == "pass"
    assert manifest["stability_databases_opened"] == []
    assert manifest["counts"]["systems"] == 2
    assert manifest["counts"]["stars"] == 3
    assert manifest["counts"]["planets"] == 1
    db = tmp_path / "out" / manifest["build_id"] / "clean_core_foundation.duckdb"
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("SELECT star_count,planet_count FROM systems WHERE system_id=1").fetchone() == (2, 1)
    assert con.execute("SELECT count(*) FROM system_search_terms WHERE term_norm='tic 123'").fetchone()[0] == 1
    assert con.execute("SELECT star_id FROM planets WHERE planet_id=21").fetchone()[0] == 11
    con.close()
