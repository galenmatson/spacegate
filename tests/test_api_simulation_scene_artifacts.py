from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

from fastapi import Response


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app import main  # noqa: E402


def _scene(build_id: str, *, name_style: str = "public_full") -> dict[str, object]:
    return {
        "build_id": build_id,
        "system": {"requested_name_style": name_style},
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


def test_candidate_lookup_treats_inaccessible_scene_as_unavailable(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(main, "_state_dir", lambda: tmp_path)
    artifact = (
        tmp_path
        / "cache/simulation_scenes/candidate-build/system_7.json.gz"
    )
    _write(artifact, _scene("candidate-build"))
    artifact.parent.chmod(0o000)
    try:
        assert main._simulation_scene_artifact_path("candidate-build", 7) is None
    finally:
        artifact.parent.chmod(0o700)


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


def test_runtime_scene_name_styles_use_separate_artifacts(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(main, "_state_dir", lambda: tmp_path)
    monkeypatch.setattr(main, "_prune_simulation_scene_runtime_cache", lambda **_: None)
    public_payload = {
        **_scene("candidate-build"),
        "materialization": {},
    }
    technical_payload = {
        **_scene("candidate-build", name_style="source_technical"),
        "materialization": {},
    }
    main._write_simulation_scene_runtime_artifact(
        "candidate-build",
        9,
        public_payload,
        name_style="public_full",
    )
    main._write_simulation_scene_runtime_artifact(
        "candidate-build",
        9,
        technical_payload,
        name_style="source_technical",
    )
    public_path = main._simulation_scene_runtime_artifact_path(
        "candidate-build", 9
    )
    technical_path = main._simulation_scene_runtime_artifact_path(
        "candidate-build",
        9,
        name_style="source_technical",
    )
    assert public_path != technical_path
    assert main._simulation_scene_artifact_path(
        "candidate-build",
        9,
        name_style="public_full",
    ) == public_path.resolve()
    assert main._simulation_scene_artifact_path(
        "candidate-build",
        9,
        name_style="source_technical",
    ) == technical_path.resolve()


def test_scene_response_survives_optional_runtime_cache_write_failure(
    monkeypatch,
) -> None:
    payload = _scene("candidate-build")
    completed: list[tuple[str, int]] = []
    monkeypatch.setattr(main.db, "build_id", lambda: "candidate-build")
    monkeypatch.setattr(main, "_simulation_scene_artifact_path", lambda *_, **__: None)
    monkeypatch.setattr(
        main, "_projected_singleton_simulation_scene", lambda *_, **__: None
    )
    monkeypatch.setattr(main, "_simulation_scene_cache_get", lambda *_, **__: None)
    monkeypatch.setattr(main, "_simulation_scene_cache_set", lambda *_, **__: None)
    monkeypatch.setattr(
        main, "_simulation_scene_build_role", lambda *_, **__: (True, object())
    )
    monkeypatch.setattr(
        main,
        "_simulation_scene_build_complete",
        lambda build_id, system_id: completed.append((build_id, system_id)),
    )
    monkeypatch.setattr(
        main, "_system_simulation_scene_payload", lambda *_, **__: payload
    )

    def fail_write(*_, **__) -> None:
        raise PermissionError("read-only runtime cache")

    monkeypatch.setattr(main, "_write_simulation_scene_runtime_artifact", fail_write)
    response = Response()

    assert main.system_simulation_scene(9, response, "public_full") == payload
    assert (
        response.headers["X-Spacegate-Simulation-Scene-Cache"]
        == "miss-write-failed"
    )
    assert completed == [("candidate-build:public_full", 9)]


def test_simulation_prefers_selected_luminosity_and_preserves_derivation_status() -> None:
    fields = main._star_simulation_fields(
        {
            "star_id": 7,
            "spectral_type_raw": "G2 V",
            "spectral_class": "G",
        },
        {
            "teff_k": 5772.0,
            "radius_rsun": 1.0,
            "luminosity_lsun": 0.98,
            "luminosity_lsun_status": "derived",
            "luminosity_lsun_basis": "stellar_luminosity_stefan_boltzmann",
        },
        {},
    )
    luminosity = next(
        field for field in fields["fields"] if field["key"] == "luminosity_lsun"
    )
    assert luminosity["value"] == 0.98
    assert luminosity["status"] == "derived"
    assert luminosity["basis"] == "arm stellar_luminosity_stefan_boltzmann"
    assert luminosity.get("generator_version") is None
