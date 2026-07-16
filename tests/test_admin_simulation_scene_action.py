from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app.admin_actions import (  # noqa: E402
    ACTION_SPECS,
    ActionValidationError,
    _build_command_materialize_simulation_scenes,
)


def test_simulation_scene_admin_action_is_bounded_runtime_cache_job() -> None:
    command = _build_command_materialize_simulation_scenes(
        {
            "build_id": "20260716T1905Z_ad13e39_side",
            "limit": 1200,
            "top_coolness_limit": 400,
            "max_dist_ly": "250",
            "sort": "coolness",
            "force": False,
        }
    )

    assert ACTION_SPECS["materialize_simulation_scenes"].risk_level == "medium"
    assert command[0] == sys.executable
    assert "--output-mode" in command
    assert command[command.index("--output-mode") + 1] == "runtime-cache"
    assert "--build-dir" not in command
    assert command[command.index("--limit") + 1] == "1200"


@pytest.mark.parametrize("limit", [0, 10001])
def test_simulation_scene_admin_action_rejects_unbounded_limits(limit: int) -> None:
    with pytest.raises(ActionValidationError):
        _build_command_materialize_simulation_scenes({"limit": limit})
