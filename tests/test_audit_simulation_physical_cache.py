from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from audit_simulation_physical_cache import audit  # noqa: E402


def _write_scene(
    path: Path,
    *,
    applicability: str = "physical",
    root_radius_au: float | None = 2.0,
    root_status: str = "complete",
) -> None:
    axis = {"value": 1.0, "unit": "au"} if applicability == "physical" else None
    payload = {
        "materialization": {"materializer_version": "simulation_scene_artifact_v19"},
        "render_scene": {
            "physical_scale": {"schema_version": "simulation_physical_scale_v2"},
            "focus_graph": {
                "schema_version": "simulation_focus_graph_v2",
                "root_focus_key": "focus:root",
                "nodes": {
                    "focus:root": {
                        "physical_bounds": {
                            "radius_au": root_radius_au,
                            "view_radius_au": root_radius_au,
                            "status": root_status,
                            "view_applicability": "physical_layout" if root_radius_au else "unavailable",
                        }
                    },
                },
            },
            "visual_scale": {"schema_version": "visual_scale_v2"},
            "orbits": [
                {
                    "endpoint_kind": "stellar_pair",
                    "physical_extent": {
                        "applicability": applicability,
                        "semi_major_axis_au": axis,
                        "presentation_radius_excluded": True,
                    },
                }
            ],
            "bodies": {"planets": []},
        },
    }
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)


def test_audit_accepts_complete_physical_contract(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    _write_scene(cache / "system_1.json.gz")

    report = audit(cache, expected_count=1)

    assert report["status"] == "pass"
    assert report["applicability_counts"] == {"physical": 1}
    assert report["stellar_orbit_count"] == 1


def test_audit_rejects_nonphysical_extent_with_axis(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    _write_scene(cache / "system_1.json.gz", applicability="unavailable")
    with gzip.open(cache / "system_1.json.gz", "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["render_scene"]["orbits"][0]["physical_extent"]["semi_major_axis_au"] = {
        "value": 1.0,
        "unit": "au",
    }
    with gzip.open(cache / "system_1.json.gz", "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)

    report = audit(cache, expected_count=1)

    assert report["status"] == "fail"
    assert report["error_count"] == 1


def test_audit_accepts_explicit_partial_root_without_invented_radius(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    _write_scene(
        cache / "system_1.json.gz",
        applicability="unavailable",
        root_radius_au=None,
        root_status="partial",
    )

    report = audit(cache, expected_count=1)

    assert report["status"] == "pass"
    assert report["root_bound_status_counts"] == {"partial": 1}
