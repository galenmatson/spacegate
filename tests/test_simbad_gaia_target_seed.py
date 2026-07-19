from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from build_simbad_gaia_target_seed import compile_seed_tables  # noqa: E402


def test_target_seed_reports_present_missing_and_multi_id_objects(tmp_path: Path) -> None:
    gaia = tmp_path / "gaia.parquet"
    bridge = tmp_path / "bridge.parquet"
    basic = tmp_path / "basic.parquet"
    output = tmp_path / "output"
    with duckdb.connect() as con:
        con.execute(
            f"copy (select * from (values (1), (2), (3)) t(source_id)) "
            f"to '{gaia}' (format parquet)"
        )
        con.execute(
            f"copy (select * from (values "
            f"(10, 'Gaia DR3 1'), (20, 'Gaia DR3 2'), "
            f"(20, 'Gaia DR3 3'), (30, 'Gaia DR3 4')) t(oidref,id)) "
            f"to '{bridge}' (format parquet)"
        )
        con.execute(
            f"copy (select * from (values (10), (99)) t(oid)) "
            f"to '{basic}' (format parquet)"
        )
    report = compile_seed_tables(
        gaia_tables={"gaia_dr3_source_envelope_v2": gaia},
        bridge_path=bridge,
        basic_path=basic,
        output_dir=output,
    )
    assert report["summary"] == {
        "gaia_envelope_rows": 3,
        "simbad_bridge_rows": 4,
        "simbad_basic_rows": 2,
        "target_bridge_rows": 3,
        "target_object_oids": 2,
        "target_present_basic": 1,
        "target_missing_basic": 1,
        "basic_outside_gaia_target": 1,
        "target_oids_with_multiple_gaia_ids": 1,
    }
    assert report["missing_oids"] == [20]
    assert json.loads((output / "missing_oids.json").read_text())["simbad_oids"] == [20]
    with duckdb.connect() as con:
        rows = con.execute(
            f"select gaia_dr3_source_id,simbad_oid,base_basic_present "
            f"from read_parquet('{output / 'target_seed.parquet'}') order by 1"
        ).fetchall()
    assert rows == [(1, 10, True), (2, 20, False), (3, 20, False)]
