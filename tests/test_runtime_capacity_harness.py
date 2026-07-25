from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/runtime_capacity_harness.py"
SPEC = importlib.util.spec_from_file_location("runtime_capacity_harness", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
HARNESS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = HARNESS
SPEC.loader.exec_module(HARNESS)


def test_percentile_interpolates() -> None:
    assert HARNESS.percentile([], 95) == 0.0
    assert HARNESS.percentile([7], 95) == 7.0
    assert HARNESS.percentile([0, 10], 50) == 5.0
    assert HARNESS.percentile([1, 2, 3, 4], 95) == pytest.approx(3.85)


def test_cpu_quota_cores_handles_bounded_and_unbounded_values() -> None:
    assert HARNESS.cpu_quota_cores("550000 100000") == 5.5
    assert HARNESS.cpu_quota_cores("max 100000") is None
    assert HARNESS.cpu_quota_cores(None) is None
    assert HARNESS.cpu_quota_cores("invalid") is None


def test_validate_manifest_rejects_invalid_profile_contracts() -> None:
    manifest = {
        "schema_version": "spacegate.runtime_capacity_workload.v1",
        "endpoints": {"health": {"path": "/api/v1/health"}},
        "profiles": {
            "ok": {
                "duration_seconds": 10,
                "concurrency": 1,
                "entries": [["health", 1]],
            }
        },
    }
    assert HARNESS.validate_manifest(manifest, "ok")["concurrency"] == 1

    broken = json.loads(json.dumps(manifest))
    broken["profiles"]["ok"]["entries"] = [["missing", 1]]
    with pytest.raises(ValueError, match="unknown endpoint"):
        HARNESS.validate_manifest(broken, "ok")

    broken = json.loads(json.dumps(manifest))
    broken["profiles"]["ok"]["duration_seconds"] = 0
    with pytest.raises(ValueError, match="invalid duration"):
        HARNESS.validate_manifest(broken, "ok")


def test_resolve_artifacts_is_confined_to_active_build(tmp_path: Path) -> None:
    state = tmp_path / "state"
    active = state / "out/build"
    active.mkdir(parents=True)
    (active / "core.duckdb").write_bytes(b"core")
    (state / "served").mkdir()
    (state / "served/current").symlink_to(active)

    resolved = HARNESS.resolve_artifacts(
        state, {"artifacts": ["served/current/core.duckdb"]}
    )
    assert resolved == [
        ("served/current/core.duckdb", (active / "core.duckdb").resolve())
    ]

    outside = tmp_path / "outside"
    outside.write_bytes(b"outside")
    with pytest.raises(ValueError, match="escapes state directory"):
        HARNESS.resolve_artifacts(
            state, {"artifacts": ["../outside"]}
        )


def test_resolve_artifacts_allows_only_registered_build_derived_files(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    active = state / "out/build-1"
    active.mkdir(parents=True)
    (active / "core.duckdb").write_bytes(b"core")
    (state / "served").mkdir()
    (state / "served/current").symlink_to(active)
    derived = state / "derived/public_read/build-1"
    derived.mkdir(parents=True)
    public_read = derived / "public_read.sqlite"
    public_read.write_bytes(b"sqlite")
    unrelated = derived / "unrelated.sqlite"
    unrelated.write_bytes(b"unrelated")

    resolved = HARNESS.resolve_artifacts(
        state,
        {
            "build_id": "build-1",
            "artifacts": [
                "derived/public_read/build-1/public_read.sqlite",
            ],
        },
    )
    assert resolved == [
        (
            "derived/public_read/build-1/public_read.sqlite",
            public_read.resolve(),
        )
    ]

    with pytest.raises(ValueError, match="outside the active build artifact set"):
        HARNESS.resolve_artifacts(
            state,
            {
                "build_id": "build-1",
                "artifacts": [
                    "derived/public_read/build-1/unrelated.sqlite",
                ],
            },
        )


def test_targeted_eviction_never_requests_global_cache_drop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "artifact.bin"
    artifact.write_bytes(b"x" * 4096)
    monkeypatch.setattr(HARNESS, "fincore", lambda _path: {"res": 4096})

    report = HARNESS.evict_artifact_pages([("artifact.bin", artifact)])

    assert report["method"] == "targeted_posix_fadvise_dontneed"
    assert report["global_cache_drop"] is False
    assert report["files"][0]["status"] == "pass"


def _sample(
    *,
    cpu_total: int,
    cpu_idle: int,
    rx_bytes: int,
    memory_current: int,
    page_cache: int,
    dirty: int,
    shmem: int,
    io_read: int,
    pressure: int,
) -> dict:
    return {
        "host": {
            "cpu": {"total": cpu_total, "idle_total": cpu_idle},
            "memory": {"MemAvailable": 10_000, "Cached": 3_000},
            "pressure": {
                resource: {
                    level: {"total": pressure}
                    for level in ("some", "full")
                }
                for resource in ("cpu", "memory", "io")
            },
            "network": {
                "lo": {
                    "rx_bytes": rx_bytes,
                    "rx_packets": 1,
                    "rx_errors": 0,
                    "rx_drops": 0,
                    "tx_bytes": rx_bytes,
                    "tx_packets": 1,
                    "tx_errors": 0,
                    "tx_drops": 0,
                }
            },
        },
        "containers": [
            {
                "name": "api",
                "process": {"VmRSS_bytes": memory_current // 2},
                "cgroup": {
                    "memory_current_bytes": memory_current,
                    "page_cache_bytes": page_cache,
                    "pids_current": 3,
                    "memory_stat": {"file_dirty": dirty, "shmem": shmem},
                    "cpu": {
                        "usage_usec": cpu_total,
                        "user_usec": cpu_total,
                        "system_usec": 0,
                        "throttled_usec": 0,
                        "nr_throttled": 0,
                    },
                    "io": {"rbytes": io_read, "wbytes": 0},
                    "memory_events": {
                        "low": 0,
                        "high": 0,
                        "max": 0,
                        "oom": 0,
                        "oom_kill": 0,
                    },
                    **{
                        f"{resource}_pressure": {
                            level: {"total": pressure}
                            for level in ("some", "full")
                        }
                        for resource in ("cpu", "memory", "io")
                    },
                },
            }
        ],
    }


def test_resource_summary_emits_deltas_and_temporary_allocation() -> None:
    first = _sample(
        cpu_total=100,
        cpu_idle=80,
        rx_bytes=1_000,
        memory_current=2_000,
        page_cache=400,
        dirty=10,
        shmem=20,
        io_read=100,
        pressure=5,
    )
    last = _sample(
        cpu_total=300,
        cpu_idle=180,
        rx_bytes=3_000,
        memory_current=4_000,
        page_cache=800,
        dirty=50,
        shmem=60,
        io_read=600,
        pressure=25,
    )

    summary = HARNESS.summarize_resources([first, last])
    api = summary["containers"]["api"]

    assert summary["host"]["cpu_busy_pct"] == 50.0
    assert summary["host"]["network"]["delta"]["lo"]["rx_bytes"] == 2_000
    assert api["io_read_bytes_delta"] == 500
    assert api["file_dirty_peak_bytes"] == 50
    assert api["shmem_peak_bytes"] == 60
    assert api["pressure_total_usec_delta"]["cpu"]["some"] == 20
