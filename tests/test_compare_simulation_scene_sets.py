from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compare_simulation_scene_sets import compare  # noqa: E402


def _write_scene(root: Path, system_id: int, build_id: str, mass: float = 1.0) -> None:
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "build_id": build_id,
        "materialization": {"build_id": build_id, "materializer_version": "v19"},
        "render_scene": {
            "assumptions": [{"build_id": build_id}],
            "bodies": {"stars": [{"mass_msun": mass, "source_build_id": "arm-stable"}]},
        },
    }
    with gzip.open(root / f"system_{system_id}.json.gz", "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)


def test_public_build_identity_is_the_only_normalized_value(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _write_scene(before, 1, "public-a")
    _write_scene(after, 1, "public-b")

    report = compare(before, after, expected_count=1)

    assert report["status"] == "pass"
    assert report["before_logical_set_sha256"] == report["after_logical_set_sha256"]


def test_scientific_difference_fails_comparison(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _write_scene(before, 1, "public-a", mass=1.0)
    _write_scene(after, 1, "public-b", mass=1.1)

    report = compare(before, after, expected_count=1)

    assert report["status"] == "fail"
    assert report["differing_scene_count"] == 1
