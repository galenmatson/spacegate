from __future__ import annotations

from pathlib import Path
import sys

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app.queries import fetch_tess_identifier_resolution  # noqa: E402
from app.utils import parse_tess_identifier_query  # noqa: E402


def test_parse_tess_identifier_query_accepts_canonical_variants() -> None:
    assert parse_tess_identifier_query("TIC 307210830") == {
        "namespace": "tic",
        "valid": True,
        "raw": "TIC 307210830",
        "value": 307210830,
        "identifier": "TIC 307210830",
        "term_norm": "tic 307210830",
    }
    assert parse_tess_identifier_query("TOI-700.1") == {
        "namespace": "toi",
        "valid": True,
        "raw": "TOI-700.1",
        "host_number": 700,
        "component": 1,
        "value": "700.01",
        "identifier": "TOI-700.01",
        "term_norm": "toi 700 01",
    }
    assert parse_tess_identifier_query("Castor") is None
    assert parse_tess_identifier_query("toilet 3") is None


def test_parse_tess_identifier_query_rejects_malformed_values() -> None:
    assert parse_tess_identifier_query("TIC abc")["valid"] is False
    assert parse_tess_identifier_query("TOI nope")["valid"] is False
    assert parse_tess_identifier_query("TIC 0")["reason"] == "identifier_out_of_range"
    assert parse_tess_identifier_query("TOI-700.00")["reason"] == "identifier_out_of_range"


def _arm_fixture(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE tess_target_identity(
          tess_identity_id BIGINT,
          tic_id BIGINT,
          resolution_status VARCHAR,
          resolution_reason VARCHAR,
          system_id BIGINT
        )
        """
    )
    con.executemany(
        "INSERT INTO tess_target_identity VALUES (?,?,?,?,?)",
        [
            (1, 100, "accepted", "bound", 10),
            (2, 200, "missing", "no_identity_route", None),
            (3, 300, "ambiguous", "collision", None),
            (4, 400, "excluded", "artifact", None),
        ],
    )
    con.execute(
        """
        CREATE TABLE toi_current_evidence(
          toi_evidence_id BIGINT,
          toi VARCHAR,
          toi_prefix VARCHAR,
          host_resolution_status VARCHAR,
          host_resolution_reason VARCHAR,
          system_id BIGINT
        )
        """
    )
    con.executemany(
        "INSERT INTO toi_current_evidence VALUES (?,?,?,?,?,?)",
        [
            (1, "700.01", "700", "accepted", "bound", 10),
            (2, "700.02", "700", "accepted", "bound", 10),
            (3, "800.01", "800", "missing", "deferred", None),
            (4, "900.01", "900", "accepted", "bound", 20),
            (5, "900.02", "900", "accepted", "bound", 21),
        ],
    )
    con.close()


def test_tess_resolution_preserves_dispositions_and_blocks_collisions(tmp_path: Path) -> None:
    arm_path = tmp_path / "arm.duckdb"
    _arm_fixture(arm_path)
    con = duckdb.connect()
    try:
        accepted = fetch_tess_identifier_resolution(
            con,
            identifier_query=parse_tess_identifier_query("TIC 100"),
            arm_db_path=str(arm_path),
        )
        assert accepted["match_status"] == "exact_match"
        assert accepted["bound_system_ids"] == [10]

        missing = fetch_tess_identifier_resolution(
            con,
            identifier_query=parse_tess_identifier_query("TIC 200"),
            arm_db_path=str(arm_path),
        )
        assert missing["match_status"] == "exact_no_match"
        assert missing["resolution_status"] == "missing"
        assert missing["deferred"] is True

        excluded = fetch_tess_identifier_resolution(
            con,
            identifier_query=parse_tess_identifier_query("TIC 400"),
            arm_db_path=str(arm_path),
        )
        assert excluded["resolution_status"] == "excluded"
        assert excluded["deferred"] is False

        host = fetch_tess_identifier_resolution(
            con,
            identifier_query=parse_tess_identifier_query("TOI-700"),
            arm_db_path=str(arm_path),
        )
        assert host["match_status"] == "exact_match"
        assert host["evidence_record_count"] == 2
        assert host["bound_system_ids"] == [10]

        collision = fetch_tess_identifier_resolution(
            con,
            identifier_query=parse_tess_identifier_query("TOI-900"),
            arm_db_path=str(arm_path),
        )
        assert collision["match_status"] == "exact_no_match"
        assert collision["resolution_status"] == "ambiguous"
        assert collision["deferred"] is True
        assert collision["bound_system_ids"] == [20, 21]
    finally:
        con.close()
