from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from evidence_lake_store import (  # noqa: E402
    build_raw_snapshot,
    build_typed_snapshot,
    manifest_entries,
    verify_snapshot,
    write_mast_json_parquet,
)


def source_contract() -> dict:
    return {
        "source_id": "test.catalog",
        "release_id": "r1",
        "registry_version": "test.1",
        "authority_roles": {"inventory": "test"},
        "license": {"name": "Test", "url": "https://example.test/license"},
        "citation_url": "https://example.test/citation",
        "identity_namespaces": ["test_id"],
        "frame_epoch": "J2000",
        "schema_policy": {
            "kind": "delimited_header",
            "drift": "fail_until_reviewed",
            "default_disposition": "preserve",
        },
        "manifest_entries": [
            {"manifest": "test_manifest.json", "source_name": "test_rows"}
        ],
    }


def test_raw_snapshot_is_independent_and_typed_cook_is_deterministic(tmp_path: Path) -> None:
    state = tmp_path / "state"
    raw = state / "raw" / "test"
    manifests = state / "reports" / "manifests"
    raw.mkdir(parents=True)
    manifests.mkdir(parents=True)
    source_path = raw / "rows.csv"
    source_path.write_text("source_id,value,quality\n1,2.5,A\n2,3.5,B\n", encoding="utf-8")

    import hashlib

    source_sha = hashlib.sha256(source_path.read_bytes()).hexdigest()
    (manifests / "test_manifest.json").write_text(
        json.dumps(
            [
                {
                    "source_name": "test_rows",
                    "source_version": "r1",
                    "dest_path": "raw/test/rows.csv",
                    "sha256": source_sha,
                    "row_count": 2,
                    "url": "https://example.test/rows.csv",
                    "retrieved_at": "2026-07-18T00:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    source = source_contract()
    entries = manifest_entries(manifests)
    raw_root = state / "raw" / "evidence_lake_v2"
    raw_manifest = build_raw_snapshot(source, entries, state, raw_root)
    snapshot_dir = raw_root / "test.catalog" / "r1" / raw_manifest["snapshot_id"]
    snapshotted = snapshot_dir / raw_manifest["artifacts"][0]["artifact_path"] / "rows.csv"

    assert snapshotted.read_bytes() == source_path.read_bytes()
    assert snapshotted.stat().st_ino != source_path.stat().st_ino
    assert raw_manifest["artifacts"][0]["materialization_counts"] == {"copy": 1, "hardlink": 0}

    source_path.write_text("source_id,value,quality\n9,9.9,Z\n", encoding="utf-8")
    assert snapshotted.read_text(encoding="utf-8").endswith("2,3.5,B\n")

    typed_root = state / "typed" / "evidence_lake_v2"
    typed_manifest = build_typed_snapshot(source, snapshot_dir, typed_root)
    typed_dir = (
        typed_root
        / "test.catalog"
        / "r1"
        / raw_manifest["snapshot_id"]
        / typed_manifest["typed_snapshot_id"]
    )
    table = typed_manifest["tables"][0]
    assert table["status"] == "typed"
    assert table["row_count"] == 2
    assert [column["name"] for column in table["columns"]] == [
        "source_id",
        "value",
        "quality",
    ]
    with duckdb.connect() as con:
        assert con.execute(
            f"select sum(cast(value as double)) from read_parquet('{typed_dir / table['parquet_path']}')"
        ).fetchone()[0] == 6.0

    repeated = build_typed_snapshot(source, snapshot_dir, typed_root)
    assert repeated["content_sha256"] == typed_manifest["content_sha256"]
    verification = verify_snapshot(snapshot_dir, typed_dir)
    assert verification["status"] == "pass"


def test_parser_pending_preserves_raw_contract(tmp_path: Path) -> None:
    state = tmp_path / "state"
    raw = state / "raw" / "test"
    manifests = state / "reports" / "manifests"
    raw.mkdir(parents=True)
    manifests.mkdir(parents=True)
    source_path = raw / "rows.dat"
    source_path.write_text("fixed width source row\n", encoding="utf-8")

    import hashlib

    (manifests / "test_manifest.json").write_text(
        json.dumps(
            [
                {
                    "source_name": "test_rows",
                    "dest_path": "raw/test/rows.dat",
                    "sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
                }
            ]
        ),
        encoding="utf-8",
    )
    source = source_contract()
    source["schema_policy"]["kind"] = "documented_fixed_width"
    raw_root = state / "raw" / "evidence_lake_v2"
    raw_manifest = build_raw_snapshot(source, manifest_entries(manifests), state, raw_root)
    snapshot_dir = raw_root / "test.catalog" / "r1" / raw_manifest["snapshot_id"]
    typed = build_typed_snapshot(source, snapshot_dir, state / "typed" / "evidence_lake_v2")
    assert typed["tables"][0]["status"] == "parser_pending"
    assert typed["tables"][0]["raw_tree_sha256"]


def test_mast_json_uses_declared_union_schema_across_null_batches(tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    fields = [
        {"name": "ID", "type": "string"},
        {"name": "KIC", "type": "int"},
        {"name": "Tmag", "type": "float"},
    ]
    first.write_text(
        json.dumps({"fields": fields, "data": [{"ID": 123, "KIC": None, "Tmag": 10}]}),
        encoding="utf-8",
    )
    second.write_text(
        json.dumps({"fields": fields, "data": [{"ID": "456", "KIC": 789, "Tmag": 11.5}]}),
        encoding="utf-8",
    )
    output = tmp_path / "mast.parquet"
    schema = write_mast_json_parquet([first, second], output)
    assert schema == [
        {"name": "ID", "source_type": "string"},
        {"name": "KIC", "source_type": "int"},
        {"name": "Tmag", "source_type": "float"},
    ]
    with duckdb.connect() as con:
        assert con.execute(
            f"select ID, KIC, Tmag from read_parquet('{output}') order by ID"
        ).fetchall() == [("123", None, 10.0), ("456", 789, 11.5)]


def test_delimited_continuation_uses_one_header_and_accounts_for_all_rows(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    snapshot = state / "raw" / "snapshot"
    first_dir = snapshot / "artifacts" / "part_1"
    second_dir = snapshot / "artifacts" / "part_2"
    first_dir.mkdir(parents=True)
    second_dir.mkdir(parents=True)
    (first_dir / "part1.csv").write_text("id,value\n1,a\n", encoding="utf-8")
    (second_dir / "part2.csv").write_text("2,b\n3,c\n", encoding="utf-8")
    source = source_contract()
    source["source_id"] = "test.continuation"
    source["schema_policy"]["artifact_layout"] = {
        "kind": "delimited_continuation",
        "table_name": "combined",
        "header_artifact": "part_1",
        "continuation_artifacts": ["part_2"],
    }
    artifacts = []
    import hashlib

    for name, relative, filename in (
        ("part_1", "artifacts/part_1", "part1.csv"),
        ("part_2", "artifacts/part_2", "part2.csv"),
    ):
        path = snapshot / relative / filename
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        artifacts.append(
            {
                "source_name": name,
                "artifact_path": relative,
                "tree_sha256": digest,
                "files": [{"path": filename, "sha256": digest, "bytes": path.stat().st_size}],
            }
        )
    raw_manifest = {
        "snapshot_id": "raw1",
        "content_sha256": "abc",
        "artifacts": artifacts,
    }
    (snapshot / "snapshot_manifest.json").write_text(
        json.dumps(raw_manifest), encoding="utf-8"
    )
    typed = build_typed_snapshot(source, snapshot, state / "typed")
    table = typed["tables"][0]
    assert table["status"] == "typed"
    assert table["row_count"] == 3
    assert [row["row_count"] for row in table["source_row_accounting"]] == [1, 2]
