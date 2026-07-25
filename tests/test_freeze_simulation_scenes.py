from __future__ import annotations

import gzip
import json
import sqlite3
import sys
from argparse import Namespace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import freeze_simulation_scenes as freezer  # noqa: E402


def write_scene(path: Path, *, build_id: str, system_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "spacegate.simulation_scene.v1",
        "system_id": system_id,
        "materialization": {
            "materialized": True,
            "materializer_version": "simulation_scene_artifact_v5",
            "build_id": build_id,
            "deterministic": True,
        },
    }
    path.write_bytes(
        gzip.compress(
            json.dumps(payload, sort_keys=True).encode("utf-8"),
            mtime=0,
        )
    )


def test_frozen_scene_archive_is_complete_and_deterministic(tmp_path: Path) -> None:
    build_id = "build-test"
    public_read = tmp_path / "public-read"
    public_read.mkdir()
    con = sqlite3.connect(public_read / "public_read.sqlite")
    con.execute(
        "CREATE TABLE systems(system_id INTEGER PRIMARY KEY, scene_representation TEXT)"
    )
    con.executemany(
        "INSERT INTO systems VALUES (?,?)",
        [(1, "full_scene"), (2, "full_scene"), (3, "singleton_seed")],
    )
    con.commit()
    con.close()
    cache = tmp_path / "cache"
    write_scene(cache / "system_1.json.gz", build_id=build_id, system_id=1)
    write_scene(cache / "system_2.json.gz", build_id=build_id, system_id=2)

    reports = []
    for name in ["first", "second"]:
        reports.append(
            freezer.run(
                Namespace(
                    state_dir=str(tmp_path),
                    build_id=build_id,
                    public_read_dir=str(public_read),
                    cache_dir=str(cache),
                    output_dir=str(tmp_path / name),
                )
            )
        )
    assert reports[0]["scene_count"] == 2
    assert reports[0]["verification"]["status"] == "pass"
    assert reports[0]["archive"]["sha256"] == reports[1]["archive"]["sha256"]
    assert reports[0]["verification"]["deterministic_rebuild_match"] is None
    assert reports[1]["verification"]["deterministic_rebuild_match"] is None
    assert (tmp_path / "first" / "simulation_scenes.tar.gz").read_bytes() == (
        tmp_path / "second" / "simulation_scenes.tar.gz"
    ).read_bytes()


def test_repeated_freeze_records_deterministic_match(tmp_path: Path) -> None:
    build_id = "build-test"
    public_read = tmp_path / "public-read"
    public_read.mkdir()
    con = sqlite3.connect(public_read / "public_read.sqlite")
    con.execute(
        "CREATE TABLE systems(system_id INTEGER PRIMARY KEY, scene_representation TEXT)"
    )
    con.execute("INSERT INTO systems VALUES (1,'full_scene')")
    con.commit()
    con.close()
    cache = tmp_path / "cache"
    write_scene(cache / "system_1.json.gz", build_id=build_id, system_id=1)
    args = Namespace(
        state_dir=str(tmp_path),
        build_id=build_id,
        public_read_dir=str(public_read),
        cache_dir=str(cache),
        output_dir=str(tmp_path / "frozen"),
    )
    first = freezer.run(args)
    second = freezer.run(args)
    assert first["verification"]["previous_archive_sha256"] is None
    assert second["verification"]["deterministic_rebuild_match"] is True
