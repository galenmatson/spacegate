from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import verify_e5_gaia_variability_selection as verification


def test_checked_in_policy_has_one_gaia_variability_source() -> None:
    policy = verification.compiler.load_json(verification.compiler.DEFAULT_POLICY)
    source = verification.selected_source(policy)
    assert source["storage"] == "coherent_array"
    assert source["selection_mode"] == "authoritative_direct"
    assert {group["group_key"] for group in source["quantity_groups"]} == {
        "stellar_rotation_modulation",
        "stellar_variability_summary",
        "stellar_variability_classification_membership",
    }
    assert all(group.get("parameter_set_kinds") for group in source["quantity_groups"])


def test_table_fingerprint_is_order_independent() -> None:
    con = verification.duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE rows(id VARCHAR,value INTEGER)")
        con.execute("INSERT INTO rows VALUES ('b',2),('a',1)")
        first = verification.table_fingerprint(
            con,
            table="rows",
            identity_column="id",
            hash_columns=["id", "value"],
        )
        con.execute("CREATE TABLE reordered AS SELECT * FROM rows ORDER BY id")
        second = verification.table_fingerprint(
            con,
            table="reordered",
            identity_column="id",
            hash_columns=["id", "value"],
        )
    finally:
        con.close()
    assert first == second
