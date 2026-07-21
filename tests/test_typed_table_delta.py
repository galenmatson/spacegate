from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from report_typed_table_delta import compare  # noqa: E402


def write_table(path: Path, rows: list[tuple[int, str, str]]) -> None:
    con = duckdb.connect()
    try:
        con.execute("create table source(id bigint, value varchar, raw_row varchar)")
        con.executemany("insert into source values (?, ?, ?)", rows)
        con.execute("copy source to ? (format parquet)", [str(path)])
    finally:
        con.close()


def test_typed_delta_separates_identity_science_and_lineage(tmp_path: Path) -> None:
    old = tmp_path / "old.parquet"
    new = tmp_path / "new.parquet"
    write_table(old, [(1, "same", "old"), (2, "before", "row"), (3, "gone", "row")])
    write_table(new, [(1, "same", "new"), (2, "after", "row"), (4, "added", "row")])

    report = compare(
        source_id="test.source",
        old_path=old,
        new_path=new,
        keys=["id"],
        lineage_fields=["raw_row"],
        sample_limit=10,
    )

    assert report["status"] == "pass"
    assert report["identity_delta"]["added_count"] == 1
    assert report["identity_delta"]["removed_count"] == 1
    assert report["identity_delta"]["common_count"] == 2
    assert report["value_delta"]["changed_any_common_row_count"] == 2
    assert report["value_delta"]["changed_scientific_common_row_count"] == 1
    assert report["value_delta"]["changed_lineage_common_row_count"] == 1
    assert report["value_delta"]["scientific_field_change_counts"] == {"value": 1}
    assert report["value_delta"]["lineage_field_change_counts"] == {"raw_row": 1}
    assert report["value_delta"]["scientifically_changed_key_sample"] == [{"id": "2"}]
    assert report["identity_delta"]["added_sample"] == [{"id": "4"}]
    assert report["identity_delta"]["removed_sample"] == [{"id": "3"}]
