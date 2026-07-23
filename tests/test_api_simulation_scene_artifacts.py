from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app import main  # noqa: E402


def _scene(build_id: str) -> dict[str, object]:
    return {
        "build_id": build_id,
        "system": {"requested_name_style": "public_full"},
        "render_scene": {
            "diagnostics": {
                "membership_reconciliation": {
                    "membership_gate": "source_hierarchy_leaves"
                }
            }
        },
        "materialization": {
            "build_id": build_id,
            "materializer_version": main.SIMULATION_SCENE_ARTIFACT_VERSION,
        },
    }


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)


def test_scene_compatibility_requires_exact_build_identity(tmp_path: Path) -> None:
    artifact = tmp_path / "scene.json.gz"
    _write(artifact, _scene("served-build"))
    assert main._simulation_scene_artifact_compatible(
        artifact, expected_build_id="served-build"
    )
    assert not main._simulation_scene_artifact_compatible(
        artifact, expected_build_id="candidate-build"
    )


def test_candidate_lookup_skips_stale_served_scene(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(main, "_state_dir", lambda: tmp_path)
    served = tmp_path / "served/current/disc/simulation_scenes/system_7.json.gz"
    candidate = tmp_path / "out/candidate-build/disc/simulation_scenes/system_7.json.gz"
    _write(served, _scene("served-build"))
    _write(candidate, _scene("candidate-build"))
    assert main._simulation_scene_artifact_path("candidate-build", 7) == candidate.resolve()


def test_runtime_scene_artifact_carries_build_contract(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(main, "_state_dir", lambda: tmp_path)
    monkeypatch.setattr(main, "_prune_simulation_scene_runtime_cache", lambda **_: None)
    main._write_simulation_scene_runtime_artifact(
        "candidate-build", 9, {**_scene("candidate-build"), "materialization": {}}
    )
    artifact = main._simulation_scene_runtime_artifact_path("candidate-build", 9)
    assert main._simulation_scene_artifact_compatible(
        artifact, expected_build_id="candidate-build"
    )
    with gzip.open(artifact, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["materialization"]["output_mode"] == "runtime-cache"
    assert payload["materialization"]["build_id"] == "candidate-build"
