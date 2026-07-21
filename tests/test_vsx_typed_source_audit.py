from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_vsx_typed_source as vsx_audit  # noqa: E402


def test_vsx_audit_separates_identity_from_public_name_collisions(tmp_path: Path) -> None:
    table_dir = tmp_path / "tables"
    table_dir.mkdir()
    path = table_dir / "vsx_dat.parquet"
    con = duckdb.connect()
    columns = list(vsx_audit.REQUIRED_OBJECT_FIELDS)
    values = {name: "" for name in columns}
    values.update(
        {
            "source_line_number": 1,
            "OID": "1",
            "Name": "Same Name",
            "V": "0",
            "RAdeg": "10.5",
            "DEdeg": "-20.5",
            "Type": "ROT",
            "Period": "2.5",
            "Sp": "G2V",
            "raw_row": "row one",
        }
    )
    second = dict(values, source_line_number=2, OID="2", raw_row="row two")
    names = sorted(columns)
    placeholders = ",".join("?" for _ in names)
    con.execute(
        "create table source(" + ",".join(f'\"{name}\" varchar' for name in names) + ")"
    )
    con.executemany(
        f"insert into source values ({placeholders})",
        [[str(row[name]) for name in names] for row in (values, second)],
    )
    con.execute("copy source to ? (format parquet)", [str(path)])
    con.close()
    manifest = {
        "release_id": "test_release",
        "snapshot_id": "raw",
        "typed_snapshot_id": "typed",
        "content_sha256": "content",
        "tables": [
            {
                "source_name": "vsx_dat",
                "status": "typed",
                "row_count": 2,
                "parquet_path": "tables/vsx_dat.parquet",
                "columns": [{"name": name, "type": "VARCHAR"} for name in names],
            },
            {
                "source_name": "vsx_readme",
                "status": "typed",
                "row_count": 1,
                "parquet_path": "tables/vsx_readme.parquet",
                "columns": [
                    {"name": "source_line_number", "type": "BIGINT"},
                    {"name": "text", "type": "VARCHAR"},
                ],
            },
        ],
    }
    report = vsx_audit.audit(tmp_path, manifest)
    assert report["status"] == "incomplete"
    assert report["checks"]["duplicate_oid_excess"] == 0
    assert report["summaries"]["identity"]["duplicate_public_name_excess"] == 1
    assert report["summaries"]["public_name_collisions"] == [
        {"normalized_name": "SAME NAME", "row_count": 2, "vsx_oids": ["1", "2"]}
    ]
    assert report["incomplete_checks"]["missing_source_bibliography_table"] is True
