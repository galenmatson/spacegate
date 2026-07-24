from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/check_profile_slo.py"
SPEC = importlib.util.spec_from_file_location("check_profile_slo", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
SLO = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SLO
SPEC.loader.exec_module(SLO)


def _capacity_report() -> dict:
    return {
        "schema_version": "spacegate.runtime_capacity_report.v1",
        "status": "pass",
        "profile": "mixed",
        "workload": {
            "build_id": "build-1",
            "acceptance": {
                "max_error_rate_pct": 1.0,
                "max_p95_latency_ms": 3000,
                "max_aggregate_memory_bytes": 8 * 1024**3,
                "require_no_timeouts": True,
                "require_no_oom": True,
            },
        },
        "environment": {
            "resource_model": {"profile": "antiproton_like"}
        },
        "requests": {
            "request_count": 100,
            "error_rate_pct": 0.0,
            "timeout_count": 0,
            "latency_ms": {"p95": 120.0},
        },
        "resources": {
            "aggregate_cgroup_memory_peak_bytes": 512 * 1024**2,
            "containers": {"api": {"oom_kill_delta": 0}},
        },
        "gates": {
            "build_identity_match": True,
            "no_safety_stop": True,
        },
    }


def test_capacity_slo_uses_embedded_acceptance_contract(tmp_path: Path) -> None:
    source = tmp_path / "summary.json"
    source.write_text(json.dumps(_capacity_report()), encoding="utf-8")

    report = SLO.evaluate_capacity_report(source)

    assert report["status"] == "pass"
    assert report["gates"]["passed"] is True
    assert report["measurements"]["aggregate_cgroup_memory_peak_bytes"] == (
        512 * 1024**2
    )


def test_capacity_slo_fails_latency_regression(tmp_path: Path) -> None:
    payload = _capacity_report()
    payload["requests"]["latency_ms"]["p95"] = 4000.0
    source = tmp_path / "summary.json"
    source.write_text(json.dumps(payload), encoding="utf-8")

    report = SLO.evaluate_capacity_report(source)

    assert report["status"] == "fail"
    assert report["gates"]["p95_latency_ok"] is False
