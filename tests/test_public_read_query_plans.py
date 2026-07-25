from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from profile_public_read_plans import includes, query_plan  # noqa: E402


def test_plan_helpers_report_indexed_sqlite_operations(tmp_path: Path) -> None:
    import sqlite3

    con = sqlite3.connect(tmp_path / "read.sqlite")
    con.execute("CREATE TABLE systems(system_id INTEGER PRIMARY KEY,name TEXT)")
    con.execute("INSERT INTO systems VALUES (1,'Sol')")
    plan = query_plan(
        con,
        "SELECT * FROM systems WHERE system_id=?",
        [1],
    )
    assert includes(plan, "INTEGER PRIMARY KEY")
    assert not includes(plan, "SCAN unrelated")
    con.close()
