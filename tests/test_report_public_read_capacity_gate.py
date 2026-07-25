from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import report_public_read_capacity_gate as reporter  # noqa: E402


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_compiles_conditional_streamed_deployment_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    build_id = "build-1"
    controls = tmp_path / "controls"
    campaign = tmp_path / "campaign"
    summary = {
        "schema_version": "spacegate.runtime_capacity_report.v1",
        "profile": "mixed",
        "label": "test",
        "environment": {"resource_model": {"profile": "antiproton_like"}},
        "workload": {"cache_state": "warm", "concurrency": 1, "duration_seconds": 1},
        "requests": {
            "request_count": 1,
            "throughput_rps": 1,
            "latency_ms": {"p95": 1},
            "queue_delay_ms": {"p95": 0},
            "error_rate_pct": 0,
            "timeout_count": 0,
            "scene_cache_counts": {},
        },
        "resources": {"aggregate_cgroup_memory_peak_bytes": 1},
        "database_runtime": {"numeric_delta": {}},
        "gates": {"final_health_ok": True},
    }
    write_json(controls / "retained_control" / "summary.json", summary)
    write_json(controls / "diagnostic_smoke" / "summary.json", summary)
    write_json(campaign / "warm" / "summary.json", summary)
    write_json(
        campaign / "warm" / "capacity_slo_report.json",
        {"status": "pass", "gates": {"passed": True}},
    )
    write_json(
        campaign / "campaign_run.json",
        {
            "schema_version": "spacegate.public_read_capacity_campaign_run.v1",
            "status": "pass",
            "started_at_utc": "2026-07-25T00:00:00Z",
            "completed_at_utc": "2026-07-25T00:01:00Z",
            "campaign": {"sha256": "campaign"},
            "workload": {"sha256": "workload"},
            "runs": [{"label": "test", "status": "pass"}],
            "staircase_returncode": 0,
        },
    )

    build_dir = tmp_path / build_id
    build_dir.mkdir()
    (build_dir / "core.duckdb").write_bytes(b"x")
    build_archive = tmp_path / "build.tar.gz"
    build_archive.write_bytes(b"a" * 100)
    public_dir = tmp_path / "public"
    public_dir.mkdir()
    (public_dir / "public_read.sqlite").write_bytes(b"db")
    write_json(
        public_dir / "manifest.json",
        {
            "build_id": build_id,
            "artifact": {"path": "public_read.sqlite"},
        },
    )
    scene_dir = tmp_path / "scenes"
    scene_dir.mkdir()
    (scene_dir / "simulation_scenes.tar.gz").write_bytes(b"scene")
    write_json(
        scene_dir / "manifest.json",
        {
            "build_id": build_id,
            "archive": {"path": "simulation_scenes.tar.gz"},
        },
    )
    reserve = 15 * 1024**3
    available_after_cleanup = (
        reserve + reporter.allocated_bytes(build_dir) + 2 + 5 + 50
    )
    prior_gate = tmp_path / "prior.json"
    write_json(
        prior_gate,
        {
            "build_id": build_id,
            "deployment_storage": {
                "available_after_candidate_cleanup_bytes": available_after_cleanup,
                "remote_observed": {
                    "observed_at_utc": "2026-07-24T00:00:00Z",
                    "available_bytes": reserve,
                    "current_build_bytes": 1,
                },
            },
        },
    )
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report_public_read_capacity_gate.py",
            "--campaign-dir",
            str(campaign),
            "--control-dir",
            str(controls),
            "--build-dir",
            str(build_dir),
            "--build-archive",
            str(build_archive),
            "--public-read-manifest",
            str(public_dir / "manifest.json"),
            "--scene-manifest",
            str(scene_dir / "manifest.json"),
            "--prior-capacity-gate",
            str(prior_gate),
            "--output",
            str(output),
        ],
    )
    assert reporter.main() == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["decision"]["recommendation"] == "conditional_go"
    assert report["campaign"]["status"] == "pass"
    assert sorted(report["controls"]) == ["retained_control"]
    transfer_seconds = report["deployment_storage"][
        "transfer_seconds_at_effective_rate"
    ]
    assert sorted(transfer_seconds) == ["10", "100", "20", "250", "50"]
    assert transfer_seconds["10"] >= transfer_seconds["250"]
