from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from materialize_simulation_scenes import (  # noqa: E402
    MATERIALIZER_VERSION,
    _scene_artifact_reusable,
    _state_dir_for_explicit_build,
    _write_scene as _write_materialized_scene,
    run,
)


def _write_scene(path: Path, *, version: str, build_id: str | None) -> None:
    materialization = {"materializer_version": version}
    if build_id is not None:
        materialization["build_id"] = build_id
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump({"materialization": materialization}, handle)


def test_scene_reuse_requires_current_version_and_target_build(tmp_path: Path) -> None:
    artifact = tmp_path / "scene.json.gz"

    _write_scene(artifact, version="simulation_scene_artifact_v1", build_id="candidate")
    assert not _scene_artifact_reusable(artifact, build_id="candidate")

    _write_scene(artifact, version=MATERIALIZER_VERSION, build_id="source")
    assert not _scene_artifact_reusable(artifact, build_id="candidate")

    _write_scene(artifact, version=MATERIALIZER_VERSION, build_id="candidate")
    assert _scene_artifact_reusable(artifact, build_id="candidate")


def test_corrupt_scene_is_not_reusable(tmp_path: Path) -> None:
    artifact = tmp_path / "scene.json.gz"
    artifact.write_bytes(b"not gzip")
    assert not _scene_artifact_reusable(artifact, build_id="candidate")


def test_explicit_out_build_infers_external_state_root(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("SPACEGATE_STATE_DIR", raising=False)
    monkeypatch.delenv("SPACEGATE_DATA_DIR", raising=False)
    build_dir = tmp_path / "state" / "out" / "candidate.tmp"
    assert _state_dir_for_explicit_build(ROOT, build_dir) == tmp_path / "state"


def test_configured_state_root_wins_over_build_inference(tmp_path: Path, monkeypatch) -> None:
    configured = tmp_path / "configured"
    monkeypatch.setenv("SPACEGATE_STATE_DIR", str(configured))
    build_dir = tmp_path / "state" / "out" / "candidate.tmp"
    assert _state_dir_for_explicit_build(ROOT, build_dir) == configured


def test_runtime_cache_mode_does_not_mutate_immutable_build(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / "state"
    build_id = "candidate"
    build_dir = state_dir / "out" / build_id
    build_dir.mkdir(parents=True)
    con = duckdb.connect(str(build_dir / "core.duckdb"))
    con.execute(
        """
        CREATE TABLE systems (
          system_id BIGINT,
          stable_object_key VARCHAR,
          system_name VARCHAR,
          dist_ly DOUBLE
        )
        """
    )
    con.execute("INSERT INTO systems VALUES (42, 'system:42', 'Test System', 4.2)")
    con.close()
    monkeypatch.setenv("SPACEGATE_STATE_DIR", str(state_dir))
    monkeypatch.setattr(
        "materialize_simulation_scenes._load_scene_builder",
        lambda _root, _build: lambda system_id, build_id: {"system_id": system_id, "build_id": build_id},
    )
    args = argparse.Namespace(
        build_dir=str(build_dir),
        build_id=build_id,
        system_id=[42],
        limit=1,
        sort="distance",
        priority_profile="none",
        top_coolness_limit=0,
        min_dist_ly=None,
        max_dist_ly=100.0,
        min_star_count=None,
        min_planet_count=None,
        force=False,
        output_mode="runtime-cache",
    )

    report = run(args)

    cache_file = state_dir / "cache" / "simulation_scenes" / build_id / "system_42.json.gz"
    assert report["ok"] is True
    assert report["params"]["output_mode"] == "runtime-cache"
    assert cache_file.exists()
    assert not (build_dir / "disc" / "simulation_scenes").exists()
    with gzip.open(cache_file, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["materialization"]["output_mode"] == "runtime-cache"
    assert payload["materialization"]["build_id"] == build_id

    args.system_id = [99]
    with pytest.raises(SystemExit, match="absent from build"):
        run(args)


def test_scene_write_atomically_replaces_existing_payload(tmp_path: Path) -> None:
    artifact = tmp_path / "system_42.json.gz"
    _write_materialized_scene(artifact, {"version": 1})
    _write_materialized_scene(artifact, {"version": 2})

    with gzip.open(artifact, "rt", encoding="utf-8") as handle:
        assert json.load(handle) == {"version": 2}
    assert list(tmp_path.glob(".*.tmp.*")) == []
