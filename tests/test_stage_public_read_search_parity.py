from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import stage_public_read_search_parity as stage  # noqa: E402


def test_refresh_manifest_accounting_replaces_intermediate_counts() -> None:
    manifest = {
        "counts": {
            "singleton_scene_seeds": 12,
        },
        "verification": {
            "counts": {
                "singleton_scene_seeds": 12,
            },
            "expected_counts": {
                "singleton_scene_seeds": 12,
            },
            "count_mismatches": {
                "singleton_scene_seeds": {"expected": 12, "observed": 10},
                "systems": {"expected": 3, "observed": 2},
            },
        },
    }
    stage.refresh_manifest_accounting(
        manifest,
        singleton_seed_count=10,
        overlay_rows=7,
    )
    assert manifest["counts"]["singleton_scene_seeds"] == 10
    assert manifest["counts"]["stellar_badge_overlays"] == 7
    assert manifest["verification"]["counts"]["singleton_scene_seeds"] == 10
    assert manifest["verification"]["counts"]["stellar_badge_overlays"] == 7
    assert manifest["verification"]["expected_counts"]["singleton_scene_seeds"] == 10
    assert manifest["verification"]["expected_counts"]["stellar_badge_overlays"] == 7
    assert manifest["verification"]["count_mismatches"] == {
        "systems": {"expected": 3, "observed": 2}
    }


def test_refreshes_every_logical_hash_modified_by_parity_upgrade() -> None:
    con = sqlite3.connect(":memory:")
    con.executescript(
        """
        CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
        INSERT INTO metadata VALUES ('build_id','build-1');
        CREATE TABLE systems(
          system_id INTEGER PRIMARY KEY,
          stable_object_key TEXT,
          system_name_norm TEXT,
          star_count INTEGER,
          planet_count INTEGER
        );
        INSERT INTO systems VALUES (1,'system:1','one',1,0);
        CREATE TABLE stellar_badge_overlays(
          system_id INTEGER,
          badge_order INTEGER,
          leaf_component_key TEXT,
          classification_value TEXT
        );
        INSERT INTO stellar_badge_overlays VALUES (1,0,'leaf:a','G');
        CREATE TABLE singleton_scene_seeds(system_id INTEGER PRIMARY KEY);
        INSERT INTO singleton_scene_seeds VALUES (1);
        """
    )
    initial = stage.refresh_modified_logical_hashes(con, {"logical_hashes": {}})
    con.execute("UPDATE systems SET star_count=2 WHERE system_id=1")
    con.execute(
        "INSERT INTO stellar_badge_overlays VALUES (1,1,'leaf:b','M')"
    )
    changed = stage.refresh_modified_logical_hashes(
        con,
        {"logical_hashes": initial},
    )
    assert changed["metadata"] == initial["metadata"]
    assert changed["systems"] != initial["systems"]
    assert (
        changed["stellar_badge_overlays"]
        != initial["stellar_badge_overlays"]
    )
    con.close()


def test_refresh_existing_finalizes_schema_and_hash(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    database = staging / "public_read.sqlite"
    con = sqlite3.connect(database)
    con.executescript(
        """
        CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
        INSERT INTO metadata VALUES ('build_id','build-1');
        CREATE TABLE systems(
          system_id INTEGER PRIMARY KEY,
          stable_object_key TEXT,
          system_name_norm TEXT,
          star_count INTEGER,
          planet_count INTEGER
        );
        INSERT INTO systems VALUES (1,'system:1','one',1,0);
        CREATE TABLE stellar_badge_overlays(
          system_id INTEGER,
          badge_order INTEGER,
          leaf_component_key TEXT,
          classification_value TEXT
        );
        INSERT INTO stellar_badge_overlays VALUES (1,0,'leaf:a','G');
        CREATE TABLE singleton_scene_seeds(system_id INTEGER PRIMARY KEY);
        INSERT INTO singleton_scene_seeds VALUES (1);
        """
    )
    con.close()
    (staging / "manifest.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "build_id": "build-1",
                "artifact": {
                    "path": "public_read.sqlite",
                    "sha256": None,
                    "hash_status": "pending",
                },
                "counts": {
                    "singleton_scene_seeds": 2,
                    "stellar_badge_overlays": 2,
                },
                "verification": {
                    "counts": {
                        "singleton_scene_seeds": 2,
                        "stellar_badge_overlays": 2,
                    },
                    "expected_counts": {
                        "singleton_scene_seeds": 2,
                        "stellar_badge_overlays": 2,
                    },
                    "count_mismatches": {},
                },
                "logical_hashes": {},
            }
        ),
        encoding="utf-8",
    )
    report_dir = tmp_path / "reports"
    result = stage.refresh_existing(
        argparse.Namespace(
            staging_dir=str(staging),
            report_dir=str(report_dir),
        )
    )
    manifest = json.loads((staging / "manifest.json").read_text(encoding="utf-8"))
    con = sqlite3.connect(database)
    metadata = dict(con.execute("SELECT key,value FROM metadata").fetchall())
    con.close()
    assert result["status"] == "pass"
    assert manifest["artifact"]["hash_status"] == "verified"
    assert manifest["artifact"]["sha256"] == stage.builder.sha256_file(database)
    assert manifest["counts"]["singleton_scene_seeds"] == 1
    assert manifest["counts"]["stellar_badge_overlays"] == 1
    assert (
        metadata["stellar_badge_overlay_schema_version"]
        == "spacegate.stellar_badge_overlay.v1"
    )
    assert (report_dir / "search_parity_refresh.json").is_file()


def test_canonicalization_normalizes_sqlite_change_counters(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.sqlite"
    con = sqlite3.connect(first)
    con.execute("CREATE TABLE values_table(id INTEGER PRIMARY KEY,value TEXT)")
    con.execute("INSERT INTO values_table VALUES (1,'same')")
    con.commit()
    con.close()
    second = tmp_path / "second.sqlite"
    second.write_bytes(first.read_bytes())
    con = sqlite3.connect(second)
    con.execute("UPDATE values_table SET value='changed' WHERE id=1")
    con.execute("UPDATE values_table SET value='same' WHERE id=1")
    con.commit()
    con.close()
    assert stage.builder.sha256_file(first) != stage.builder.sha256_file(second)
    stage.canonicalize_database(first)
    stage.canonicalize_database(second)
    assert stage.builder.sha256_file(first) == stage.builder.sha256_file(second)
