from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import run_public_read_capacity_campaign as campaign_runner  # noqa: E402


def test_checked_in_campaign_matches_workload_build() -> None:
    campaign, workload = campaign_runner.load_campaign(
        campaign_runner.DEFAULT_CAMPAIGN
    )
    workload_payload = json.loads(workload.read_text(encoding="utf-8"))
    assert campaign["build_id"] == workload_payload["build_id"]
    assert campaign["staircase"]["steps"] == [1, 2, 4, 6, 8, 12]
    assert {
        profile["acceptance"]["max_aggregate_memory_bytes"]
        for profile in workload_payload["profiles"].values()
    } == {8 * 1024**3}
    restarted = {
        run["label"]: run["restart_containers_before"]
        for run in campaign["runs"]
        if run.get("restart_containers_before")
    }
    assert restarted == {
        "constrained_scene_dynamic_diverse": ["spacegate-capacity-api-1"],
        "constrained_scene_coalesced_miss": ["spacegate-capacity-api-1"],
    }


def test_rejects_duplicate_labels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workload = tmp_path / "workload.json"
    workload.write_text(json.dumps({"build_id": "build-1"}), encoding="utf-8")
    campaign = tmp_path / "campaign.json"
    campaign.write_text(
        json.dumps(
            {
                "build_id": "build-1",
                "workload_manifest": str(workload),
                "runs": [{"label": "same"}, {"label": "same"}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(campaign_runner, "ROOT", Path("/"))
    with pytest.raises(SystemExit, match="unique"):
        campaign_runner.load_campaign(campaign)


def test_rejects_restart_outside_isolated_capacity_stack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workload = tmp_path / "workload.json"
    workload.write_text(json.dumps({"build_id": "build-1"}), encoding="utf-8")
    campaign = tmp_path / "campaign.json"
    campaign.write_text(
        json.dumps(
            {
                "build_id": "build-1",
                "workload_manifest": str(workload),
                "runs": [
                    {
                        "label": "unsafe",
                        "restart_containers_before": ["app-api-1"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(campaign_runner, "ROOT", Path("/"))
    with pytest.raises(SystemExit, match="isolated capacity stack"):
        campaign_runner.load_campaign(campaign)
