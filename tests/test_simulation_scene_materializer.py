from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from materialize_simulation_scenes import (  # noqa: E402
    MATERIALIZER_VERSION,
    _scene_artifact_reusable,
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
