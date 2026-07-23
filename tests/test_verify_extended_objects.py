from __future__ import annotations

from pathlib import Path
import sys

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from verify_extended_objects import verify  # noqa: E402


def test_clean_projection_uses_general_gates_and_optional_named_diagnostics(
    tmp_path: Path,
) -> None:
    core_path = tmp_path / "core.duckdb"
    arm_path = tmp_path / "arm.duckdb"
    con = duckdb.connect(str(core_path))
    con.execute(
        """
        create table extended_objects(
          extended_object_id bigint,stable_object_key varchar,canonical_name varchar,
          display_name varchar,object_type varchar,dist_ly double,map_domain varchar,
          nominal_radius_tier_ly double,retrieval_checksum varchar,retrieved_at varchar,
          dist_pc double,x_helio_ly double,y_helio_ly double,z_helio_ly double,
          distance_method varchar,distance_confidence varchar,distance_evidence_json varchar,
          source_catalog varchar,source_version varchar,source_pk varchar,
          source_row_hash varchar,transform_version varchar
        );
        insert into extended_objects values
          (1,'extended:test','Test','Test','nebula',10.0,'local_3d',100.0,null,null,
           3.0,1.0,2.0,3.0,'selected_distance','high','{}','catalog','v1','row-1',
           'hash','e7_clean_extended_objects_compiler_v3');
        create table extended_object_aliases(extended_object_id bigint,alias varchar);
        insert into extended_object_aliases values (1,'Test');
        create table extended_object_identifiers(extended_object_id bigint,namespace varchar);
        insert into extended_object_identifiers values (1,'TEST');
        create table extended_object_search_terms(extended_object_id bigint,term_norm varchar);
        insert into extended_object_search_terms values (1,'test');
        create table extended_object_source_reconciliation(
          extended_object_id bigint,outcome varchar,reason varchar
        );
        insert into extended_object_source_reconciliation values (1,'accepted','identity');
        create table extended_object_identity_quarantine(reason varchar);
        """
    )
    con.close()
    duckdb.connect(str(arm_path)).close()

    report = verify(core_path, arm_path, None)

    assert report["status"] == "pass"
    assert report["named_object_gates"] is False
    assert report["checks"]["arm_evidence_contract"]["passed"] is True
    assert report["checks"]["arm_evidence_contract"]["details"]["contract"] == (
        "clean_selected_core_projection"
    )
