from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("rate_report", ROOT / "scripts/report_simulation_rate_policy.py")
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_scene_rate_review_rejects_assumed_periods_and_keeps_epoch_out() -> None:
    scene = {
        "system": {"system_id": 7, "display_name": "Policy Test"},
        "render_scene": {
            "bodies": {"planets": [
                {"fields": {"orbital_period_days": {"status": "source", "value": 17.5}}},
                {"fields": {"orbital_period_days": {"status": "assumed", "value": 1}}},
            ]},
            "orbits": [
                {
                    "fields": {"period_days": {"status": "source", "value": 4200}},
                    "primary_child_body_keys": ["a", "b"],
                    "secondary_child_body_keys": ["c"],
                },
                {
                    "fields": {"period_days": {"status": "assumed", "value": 2}},
                    "primary_child_body_keys": ["a"],
                    "secondary_child_body_keys": ["b"],
                },
            ],
        },
    }
    result = MODULE.analyze_scene(scene)
    assert result["planet_periods"] == [17.5]
    assert result["stellar_periods"] == [4200.0]
    assert result["planet_policy"]["nearest_manual_rate"] == 5
    assert "simulation_days" not in result
