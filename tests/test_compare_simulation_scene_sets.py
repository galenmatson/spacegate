from __future__ import annotations

import gzip
import hashlib
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


def _build_keyed_assumption(build_id: str, value: float = 0.25) -> dict:
    record = {
        "build_id": build_id,
        "object_type": "orbit",
        "system_id": 1,
        "star_id": None,
        "planet_id": None,
        "orbit_edge_id": 7,
        "stable_object_key": None,
        "stable_component_key": "comp:test",
        "render_key": "orbit:7",
        "parameter_key": "inclination_deg",
        "value_json": json.dumps(value),
        "assumption_version": "test_v1",
        "input_context_json": "{}",
        "replacement_target": "source inclination",
    }
    payload = {key: record.get(key) for key in record if key != "assumption_key"}
    record["assumption_key"] = hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return record


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


def test_verified_build_keyed_assumption_identity_is_normalized(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _write_scene(before, 1, "public-a")
    _write_scene(after, 1, "public-b")
    for root, build_id in ((before, "public-a"), (after, "public-b")):
        path = root / "system_1.json.gz"
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["render_scene"]["assumptions"] = [_build_keyed_assumption(build_id)]
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle)

    report = compare(before, after, expected_count=1)

    assert report["status"] == "pass"


def test_unverified_assumption_key_difference_is_not_normalized(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _write_scene(before, 1, "public-a")
    _write_scene(after, 1, "public-b")
    for root, build_id, key in (
        (before, "public-a", "not-the-documented-hash-a"),
        (after, "public-b", "not-the-documented-hash-b"),
    ):
        path = root / "system_1.json.gz"
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        assumption = _build_keyed_assumption(build_id)
        assumption["assumption_key"] = key
        payload["render_scene"]["assumptions"] = [assumption]
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle)

    report = compare(before, after, expected_count=1)

    assert report["status"] == "fail"
    assert report["differing_scene_count"] == 1


def test_set_like_arm_diagnostic_order_is_canonicalized(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _write_scene(before, 1, "public-a")
    _write_scene(after, 1, "public-b")
    components = [
        {"component_type": "planet", "display_name": "b", "stable_component_key": "planet:b"},
        {"component_type": "planet", "display_name": "c", "stable_component_key": "planet:c"},
    ]
    edges = [
        {"confidence_score": 1.0, "parent_component_key": "star:a", "child_component_key": "planet:b"},
        {"confidence_score": 1.0, "parent_component_key": "star:a", "child_component_key": "planet:c"},
    ]
    for root, component_rows, edge_rows in (
        (before, components, edges),
        (after, list(reversed(components)), list(reversed(edges))),
    ):
        path = root / "system_1.json.gz"
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["arm"] = {
            "components": {"count": 2, "items": component_rows},
            "hierarchy_edges": {"count": 2, "items": edge_rows},
        }
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle)

    report = compare(before, after, expected_count=1)

    assert report["status"] == "pass"


def test_set_like_arm_diagnostic_content_difference_still_fails(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _write_scene(before, 1, "public-a")
    _write_scene(after, 1, "public-b")
    for root, child in ((before, "planet:b"), (after, "planet:c")):
        path = root / "system_1.json.gz"
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["arm"] = {
            "components": {"count": 0, "items": []},
            "hierarchy_edges": {
                "count": 1,
                "items": [{
                    "confidence_score": 1.0,
                    "parent_component_key": "star:a",
                    "child_component_key": child,
                }],
            },
        }
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle)

    report = compare(before, after, expected_count=1)

    assert report["status"] == "fail"
