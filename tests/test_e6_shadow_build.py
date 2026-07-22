from __future__ import annotations

import copy
import shutil
import sys
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_e6_shadow_build as compiler  # noqa: E402
import verify_e6_shadow_reproduction as reproduction  # noqa: E402


def test_e6_policy_maps_only_projected_numeric_quantities() -> None:
    policy = compiler.load_json(ROOT / "config/evidence_lake/e6_shadow_build.json")
    compiler.validate_policy(policy)

    invalid = copy.deepcopy(policy)
    invalid["core_scalar_updates"][0]["quantity"] = "not_projected"
    with pytest.raises(ValueError, match="CORE mapping is not projected"):
        compiler.validate_policy(invalid)

    invalid = copy.deepcopy(policy)
    invalid["selected_artifacts"][0]["acceptance_mode"] = "accept_anything"
    with pytest.raises(ValueError, match="artifact acceptance mode"):
        compiler.validate_policy(invalid)


def test_e6_projection_preserves_lineage_and_updates_without_inventory_change(
    tmp_path: Path,
) -> None:
    database = tmp_path / "arm.duckdb"
    core = tmp_path / "core.duckdb"
    base = tmp_path / "base.duckdb"
    selected = tmp_path / "selected.duckdb"

    con = duckdb.connect(str(core))
    con.execute(
        """
        CREATE TABLE stars(
          star_id BIGINT,system_id BIGINT,stable_object_key VARCHAR,teff_k DOUBLE
        );
        CREATE TABLE planets(
          planet_id BIGINT,system_id BIGINT,stable_object_key VARCHAR,star_id BIGINT,
          radius_earth DOUBLE
        );
        CREATE TABLE aliases(
          alias_id BIGINT,target_type VARCHAR,target_id BIGINT,system_id BIGINT,
          star_id BIGINT,alias_raw VARCHAR,alias_norm VARCHAR,alias_kind VARCHAR,
          alias_priority INTEGER,is_primary BOOLEAN,source_catalog VARCHAR,
          source_version VARCHAR,source_pk BIGINT
        );
        INSERT INTO stars VALUES (1,10,'star:one',4000.0),(2,20,'star:two',NULL);
        INSERT INTO planets VALUES (3,10,'planet:one',1,1.0);
        """
    )
    con.close()
    shutil.copy2(core, base)

    con = duckdb.connect(str(selected))
    con.execute(
        """
        CREATE TABLE selected_facts(
          selected_fact_id VARCHAR,object_type VARCHAR,stable_object_key VARCHAR,
          quantity_key VARCHAR,value_raw VARCHAR,normalized_value DOUBLE,
          value_lower DOUBLE,value_upper DOUBLE
        );
        INSERT INTO selected_facts VALUES
          ('fact-teff-1','star','star:one','teff_k','4100',4100,4050,4150),
          ('fact-teff-2','star','star:two','teff_k','3200',3200,3100,3300),
          ('fact-name','star','star:one','official_proper_name','Test Star',NULL,NULL,NULL),
          ('fact-variable','star','star:one','variable','false',NULL,NULL,NULL),
          ('fact-radius','planet','planet:one','radius_earth','1.1',1.1,1.0,1.2);
        """
    )
    con.close()

    con = duckdb.connect(str(database))
    con.execute(f"ATTACH '{core}' AS core")
    con.execute(f"ATTACH '{base}' AS base (READ_ONLY)")
    con.execute(f"ATTACH '{selected}' AS selected (READ_ONLY)")
    assert compiler.create_wide_projection(
        con,
        output_table="stellar_projection",
        object_type="star",
        quantities=["teff_k", "official_proper_name", "variable"],
        categorical={"official_proper_name"},
        boolean={"variable"},
        selected_alias="selected",
    ) == 2
    assert compiler.create_wide_projection(
        con,
        output_table="planet_projection",
        object_type="planet",
        quantities=["radius_earth"],
        categorical=set(),
        boolean=set(),
        selected_alias="selected",
    ) == 1
    assert con.execute(
        "SELECT teff_k,teff_k_fact_id,official_proper_name_fact_id,variable,variable_fact_id "
        "FROM stellar_projection WHERE star_id=1"
    ).fetchone() == (4100.0, "fact-teff-1", "fact-name", False, "fact-variable")

    policy = {
        "core_scalar_updates": [
            {
                "object_type": "star",
                "quantity": "teff_k",
                "column": "teff_k",
                "absolute_tolerance": 0.1,
            },
            {
                "object_type": "planet",
                "quantity": "radius_earth",
                "column": "radius_earth",
                "absolute_tolerance": 0.01,
            },
        ],
        "official_name_alias_policy": {
            "enabled": True,
            "alias_kind": "iau_official_proper_name",
            "alias_priority": 5,
            "is_primary": False,
        },
    }
    updates = compiler.core_update_report(
        con,
        policy=policy,
        projection_by_quantity={
            ("star", "teff_k"): "stellar_projection",
            ("planet", "radius_earth"): "planet_projection",
        },
    )
    assert [row["scientifically_changed_rows"] for row in updates] == [1, 1]
    assert [row["filled_rows"] for row in updates] == [1, 0]
    assert all(row["post_update_mismatches"] == 0 for row in updates)
    assert compiler.add_official_name_aliases(
        con,
        policy=policy,
        classification_table="stellar_projection",
    ) == 1
    assert con.execute("SELECT COUNT(*) FROM core.stars").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM core.planets").fetchone()[0] == 1
    assert con.execute(
        "SELECT alias_raw,is_primary FROM core.aliases"
    ).fetchone() == ("Test Star", False)
    con.close()


def test_e6_logical_hash_is_order_independent_and_duplicate_sensitive(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.duckdb"
    second = tmp_path / "second.duckdb"
    for database, values in (
        (first, "(1,'a'),(2,'b')"),
        (second, "(2,'b'),(1,'a')"),
    ):
        con = duckdb.connect(str(database))
        con.execute("CREATE TABLE facts(id INTEGER,value VARCHAR)")
        con.execute(f"INSERT INTO facts VALUES {values}")
        con.close()
    first_hash = reproduction.table_logical_hash(first, "facts", tmp_path / "scratch-a")
    second_hash = reproduction.table_logical_hash(second, "facts", tmp_path / "scratch-b")
    assert first_hash == second_hash

    con = duckdb.connect(str(second))
    con.execute("INSERT INTO facts VALUES (1,'a')")
    con.close()
    duplicate_hash = reproduction.table_logical_hash(
        second, "facts", tmp_path / "scratch-c"
    )
    assert duplicate_hash["row_count"] == 3
    assert duplicate_hash["logical_sha256"] != first_hash["logical_sha256"]
