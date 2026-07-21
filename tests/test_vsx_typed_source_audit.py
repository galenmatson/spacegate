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


def test_vsx_audit_preserves_partial_non_ads_bibliography(tmp_path: Path) -> None:
    table_dir = tmp_path / "tables"
    table_dir.mkdir()
    con = duckdb.connect()
    object_fields = sorted(vsx_audit.REQUIRED_OBJECT_FIELDS)
    object_values = {name: "" for name in object_fields}
    object_values.update(
        {
            "source_line_number": "1",
            "OID": "1",
            "Name": "Variable One",
            "V": "0",
            "RAdeg": "10.0",
            "DEdeg": "20.0",
            "Type": "ROT",
            "Period": "2.0",
            "raw_row": "object",
        }
    )
    con.execute(
        "create table objects("
        + ",".join(f'\"{name}\" varchar' for name in object_fields)
        + ")"
    )
    con.execute(
        "insert into objects values ("
        + ",".join("?" for _ in object_fields)
        + ")",
        [object_values[name] for name in object_fields],
    )
    con.execute("copy objects to ? (format parquet)", [str(table_dir / "vsx_dat.parquet")])
    con.execute(
        "create table refs(source_line_number bigint,OID varchar,Bibcode varchar,raw_row varchar)"
    )
    con.execute(
        "insert into refs values (1,'1','2020ApJ...123..456A','object'),"
        "(2,'2','BAVJ, 22, 2018','historical orphan')"
    )
    con.execute(
        "copy refs to ? (format parquet)",
        [str(table_dir / "vsx_references.parquet")],
    )
    con.close()
    object_columns = [
        {"name": name, "type": "VARCHAR"} for name in object_fields
    ]
    manifest = {
        "release_id": "test_release",
        "snapshot_id": "raw",
        "typed_snapshot_id": "typed",
        "content_sha256": "content",
        "tables": [
            {
                "source_name": "vsx_dat",
                "status": "typed",
                "row_count": 1,
                "parquet_path": "tables/vsx_dat.parquet",
                "columns": object_columns,
            },
            {
                "source_name": "vsx_readme",
                "status": "typed",
                "row_count": 1,
                "parquet_path": "tables/vsx_readme.parquet",
                "columns": [],
            },
            {
                "source_name": "vsx_references",
                "status": "typed",
                "row_count": 2,
                "parquet_path": "tables/vsx_references.parquet",
                "columns": [
                    {"name": name, "type": kind}
                    for name, kind in (
                        ("source_line_number", "BIGINT"),
                        ("OID", "VARCHAR"),
                        ("Bibcode", "VARCHAR"),
                        ("raw_row", "VARCHAR"),
                    )
                ],
            },
        ],
    }
    report = vsx_audit.audit(tmp_path, manifest)
    assert report["status"] == "pass"
    assert report["incomplete_checks"]["missing_source_bibliography_table"] is False
    assert report["checks"]["bibliography_duplicate_pair_excess"] == 0
    assert report["summaries"]["bibliography"]["oid_not_in_current_object_table"] == 1
    assert report["summaries"]["bibliography"]["noncanonical_reference_rows"] == 1
