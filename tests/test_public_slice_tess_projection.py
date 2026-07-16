from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ingest" / "build_public_slice.py"
SPEC = importlib.util.spec_from_file_location("build_public_slice", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def projection_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("attach ':memory:' as src")
    con.execute("attach ':memory:' as core")
    con.execute("create table core.systems(system_id bigint)")
    con.execute("create table core.stars(star_id bigint)")
    con.execute("create table core.planets(planet_id bigint)")
    con.execute("insert into core.systems values (10)")
    con.execute("insert into core.stars values (20)")
    con.execute("insert into core.planets values (30)")

    con.execute(
        "create table src.tess_target_identity("
        "tess_identity_id bigint, ingested_at varchar, resolution_status varchar, "
        "star_id bigint, system_id bigint, payload varchar)"
    )
    con.execute(
        "insert into src.tess_target_identity values "
        "(1, 'source', 'accepted', 20, 10, 'retained'), "
        "(2, 'source', 'accepted', 21, 11, 'trimmed'), "
        "(3, 'source', 'ambiguous', 21, 11, 'preserved')"
    )
    con.execute(
        "create table tess_target_identity as "
        "select * from src.tess_target_identity where payload <> 'trimmed'"
    )

    con.execute("create table src.tess_missing_object_audit(audit_id bigint, payload varchar)")
    con.execute("insert into src.tess_missing_object_audit values (1, 'all')")
    con.execute("create table tess_missing_object_audit as select * from src.tess_missing_object_audit")

    con.execute(
        "create table src.toi_current_evidence("
        "toi_evidence_id bigint, ingested_at varchar, system_id bigint, star_id bigint, "
        "planet_id bigint, payload varchar)"
    )
    con.execute(
        "insert into src.toi_current_evidence values "
        "(1, 'source', 10, 20, 30, 'retained'), "
        "(2, 'source', 11, 21, null, 'trimmed'), "
        "(3, 'source', null, null, null, 'unbound')"
    )
    con.execute(
        "create table toi_current_evidence as "
        "select * from src.toi_current_evidence where payload <> 'trimmed'"
    )

    con.execute(
        "create table src.toi_disposition_history("
        "history_id bigint, ingested_at varchar, payload varchar)"
    )
    con.execute("insert into src.toi_disposition_history values (1, 'source', 'all')")
    con.execute("create table toi_disposition_history as select * from src.toi_disposition_history")
    return con


def test_sliced_tess_projection_accepts_exact_canonical_subset() -> None:
    con = projection_connection()
    try:
        report = MODULE.verify_sliced_tess_projection(con)
        assert all(values == {"unexpected": 0, "missing": 0} for values in report.values())
    finally:
        con.close()


def test_sliced_tess_projection_rejects_changed_identity() -> None:
    con = projection_connection()
    try:
        con.execute("update tess_target_identity set payload='changed' where tess_identity_id=1")
        with pytest.raises(RuntimeError, match="tess_target_identity"):
            MODULE.verify_sliced_tess_projection(con)
    finally:
        con.close()
