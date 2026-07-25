from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app import narration  # noqa: E402


def test_absent_disc_narrative_table_is_fingerprint_cached(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database = tmp_path / "disc.duckdb"
    con = duckdb.connect(str(database))
    con.execute("CREATE TABLE other(value INTEGER)")
    con.close()

    real_connect = narration.duckdb.connect
    calls = 0

    def counted_connect(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real_connect(*args, **kwargs)

    narration._disc_has_narrative_table.cache_clear()
    monkeypatch.setattr(narration.duckdb, "connect", counted_connect)
    for _ in range(2):
        assert narration.fetch_disc_system_narrative_blocks(
            disc_db_path=str(database),
            system_id=1,
            stable_object_key="system:1",
        ) == []
    assert calls == 1
