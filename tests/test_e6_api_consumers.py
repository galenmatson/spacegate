from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app.queries import _enrich_hierarchy_star_nodes  # noqa: E402


def test_hierarchy_quick_facts_prefer_e6_selected_parameters(tmp_path: Path) -> None:
    core_db = tmp_path / "core.duckdb"
    arm_db = tmp_path / "arm.duckdb"
    core = duckdb.connect(str(core_db))
    core.execute(
        """
        CREATE TABLE stars(
          star_id BIGINT,spectral_type_raw VARCHAR,spectral_class VARCHAR,
          teff_k DOUBLE,vmag DOUBLE,dist_ly DOUBLE
        );
        INSERT INTO stars VALUES (1,'G2 V','G',5800,4.0,10.0);
        """
    )
    core.close()
    arm = duckdb.connect(str(arm_db))
    arm.execute(
        """
        CREATE TABLE stellar_parameters(
          stellar_parameter_id BIGINT,star_id BIGINT,parameter_source VARCHAR,
          mass_msun DOUBLE,radius_rsun DOUBLE,luminosity_log10_lsun DOUBLE,
          teff_k DOUBLE
        );
        INSERT INTO stellar_parameters VALUES (1,1,'gaia_dr3_backbone',9,9,9,9000);
        CREATE TABLE e6_selected_stellar_parameters(
          star_id BIGINT,mass_msun DOUBLE,radius_rsun DOUBLE,
          luminosity_log10_lsun DOUBLE
        );
        INSERT INTO e6_selected_stellar_parameters VALUES (1,1.0,1.1,0.2);
        """
    )
    arm.close()

    con = duckdb.connect(str(core_db), read_only=True)
    con.execute(f"ATTACH '{arm_db}' AS arm_db (READ_ONLY)")
    node_map = {
        "star:one": {
            "component_family": "star",
            "core_object_type": "star",
            "core_object_id": 1,
        }
    }
    _enrich_hierarchy_star_nodes(con, node_map=node_map, arm_attached=True)
    con.close()

    assert node_map["star:one"]["quick_facts"]["mass_msun"] == 1.0
    assert node_map["star:one"]["quick_facts"]["radius_rsun"] == 1.1
    assert node_map["star:one"]["quick_facts"]["luminosity_log10_lsun"] == 0.2
