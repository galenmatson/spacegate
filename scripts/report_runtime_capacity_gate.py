#!/usr/bin/env python3
"""Compile the M8.3d measurements into one reviewable machine report."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any


BUILD_ID = "e7_24cb15211f430a37f199f462_full_public"
RUNS = {
    "unconstrained_mixed": "20260724T181215Z_unconstrained_mixed_control_mixed",
    "unconstrained_scene_misses": (
        "20260724T181402Z_unconstrained_scene_diverse_control_scene_dynamic_diverse"
    ),
    "constrained_cold_single": "20260724T181445Z_constrained_cold_single_mixed",
    "constrained_baseline": "20260724T181542Z_constrained_warm_mixed_before_mixed",
    "constrained_scene_misses_baseline": (
        "20260724T181712Z_constrained_scene_diverse_before_scene_dynamic_diverse"
    ),
    "constrained_pool6": "20260724T182918Z_constrained_warm_pool6_threads1_mixed",
    "constrained_pool6_health_cache": (
        "20260724T1910Z_constrained_warm_pool6_health_cache_mixed"
    ),
    "static": "20260724T183256Z_constrained_static_pool6_static",
    "prebuilt_scenes": "20260724T183402Z_constrained_scene_prebuilt_pool6_scene_prebuilt",
    "dynamic_scene_misses": (
        "20260724T183518Z_constrained_scene_diverse_pool6_scene_dynamic_diverse"
    ),
    "burst": "20260724T183912Z_constrained_burst_3rps_mixed",
    "sustained": "20260724T184035Z_constrained_sustained_c2_pool6_mixed",
    "idle": "20260724T184548Z_constrained_idle_5m_pool6_idle",
    "scene_coalesced": (
        "20260724T185409Z_constrained_scene_coalesced_pool6_scene_coalesced"
    ),
    "scene_hits": "20260724T185426Z_constrained_scene_hits_pool6_scene_coalesced",
}


def utc_now() -> str:
    return (
        dt.datetime.now(dt.UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def report_ref(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


def absolute_ref(path: Path) -> dict[str, Any]:
    path = path.resolve(strict=True)
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


def request_metrics(report: dict[str, Any]) -> dict[str, Any]:
    requests = report["requests"]
    resources = report["resources"]
    return {
        "elapsed_seconds": report["elapsed_seconds"],
        "request_count": requests["request_count"],
        "throughput_rps": requests["throughput_rps"],
        "latency_ms": requests["latency_ms"],
        "queue_delay_ms": requests["queue_delay_ms"],
        "error_rate_pct": requests["error_rate_pct"],
        "timeout_count": requests["timeout_count"],
        "scene_cache_counts": requests["scene_cache_counts"],
        "aggregate_cgroup_memory_peak_bytes": resources[
            "aggregate_cgroup_memory_peak_bytes"
        ],
        "gates": report["gates"],
        "database_runtime_delta": report.get("database_runtime", {}).get(
            "numeric_delta", {}
        ),
    }


def pct_change(before: float, after: float) -> float:
    return round(((after - before) / before) * 100.0, 3)


def transfer_seconds(byte_count: int, megabits_per_second: float) -> int:
    # Allow 15% for TCP/SSH/rsync overhead and real uplink variation.
    effective_bits_per_second = megabits_per_second * 1_000_000 * 0.85
    return round(byte_count * 8 / effective_bits_per_second)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--remote-total-bytes", type=int, required=True)
    parser.add_argument("--remote-used-bytes", type=int, required=True)
    parser.add_argument("--remote-available-bytes", type=int, required=True)
    parser.add_argument("--remote-observed-at-utc", required=True)
    parser.add_argument("--remote-current-build", required=True)
    parser.add_argument("--remote-current-build-bytes", type=int, required=True)
    parser.add_argument("--remote-standby-build-bytes", type=int, required=True)
    parser.add_argument("--remote-current-archive-bytes", type=int, required=True)
    parser.add_argument("--remote-standby-archive-bytes", type=int, required=True)
    parser.add_argument("--remote-cache-bytes", type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.report_root.resolve(strict=True)
    build_dir = args.build_dir.resolve(strict=True)
    archive = args.archive.resolve(strict=True)
    if build_dir.name != BUILD_ID:
        raise SystemExit(f"Unexpected build: {build_dir.name}")

    reports: dict[str, dict[str, Any]] = {}
    refs: dict[str, dict[str, Any]] = {}
    for label, directory in RUNS.items():
        path = root / directory / "summary.json"
        reports[label] = load_json(path)
        refs[label] = report_ref(path, root)
        if reports[label]["workload"]["active_build_id"] != BUILD_ID:
            raise SystemExit(f"Build mismatch in {path}")

    staircase_path = root / "20260724T183618Z_staircase_mixed/staircase_summary.json"
    staircase = load_json(staircase_path)
    refs["staircase"] = report_ref(staircase_path, root)
    browser_path = (
        root / "browser/20260724T1851Z-radius1000/constrained-pool6.json"
    )
    browser = load_json(browser_path)
    refs["browser_map"] = report_ref(browser_path, root)
    map_playwright_path = (
        root
        / "browser/20260724T1853Z-capacity-1000/playwright-report.json"
    )
    correctness_playwright_path = (
        root
        / "browser/20260724T1854Z-capacity-correctness/playwright-report.json"
    )
    map_playwright = load_json(map_playwright_path)
    correctness_playwright = load_json(correctness_playwright_path)
    refs["map_playwright"] = report_ref(map_playwright_path, root)
    refs["correctness_playwright"] = report_ref(
        correctness_playwright_path, root
    )
    nested_orbit_runtime_path = root / "nested_orbit_runtime_verification.json"
    nested_orbit_runtime = load_json(nested_orbit_runtime_path)
    refs["nested_orbit_runtime"] = report_ref(nested_orbit_runtime_path, root)
    orbit_compiler_verification_path = Path(
        "/data/spacegate/state/reports/evidence_lake_v2/"
        "e7_clean_runtime_arm_group_orbit_repair_final_verification.json"
    )
    orbit_compiler_verification = load_json(orbit_compiler_verification_path)
    refs["nested_orbit_compiler_verification"] = absolute_ref(
        orbit_compiler_verification_path
    )

    archive_bytes = archive.stat().st_size
    logical_bytes = sum(
        path.stat().st_size for path in build_dir.rglob("*") if path.is_file()
    )
    allocated_bytes = sum(
        path.stat().st_blocks * 512
        for path in [build_dir, *build_dir.rglob("*")]
    )
    cleanup_candidate_bytes = (
        args.remote_standby_build_bytes
        + args.remote_current_archive_bytes
        + args.remote_standby_archive_bytes
    )
    available_after_reviewed_cleanup = (
        args.remote_available_bytes + cleanup_candidate_bytes
    )
    deployment_peak_addition = allocated_bytes + archive_bytes
    available_at_peak = available_after_reviewed_cleanup - deployment_peak_addition
    available_after_archive_retirement = available_at_peak + archive_bytes

    before = request_metrics(reports["constrained_baseline"])
    after = request_metrics(reports["constrained_pool6"])
    optimizations = {
        "connection_pool": {
            "configuration": {
                "pool_size": 6,
                "duckdb_threads_per_connection": 1,
                "acquire_timeout_seconds": 30,
            },
            "before": before,
            "after": after,
            "throughput_change_pct": pct_change(
                before["throughput_rps"], after["throughput_rps"]
            ),
            "p95_latency_change_pct": pct_change(
                before["latency_ms"]["p95"], after["latency_ms"]["p95"]
            ),
            "peak_memory_change_pct": pct_change(
                before["aggregate_cgroup_memory_peak_bytes"],
                after["aggregate_cgroup_memory_peak_bytes"],
            ),
        },
        "fingerprint_cached_health_identity": {
            "before_health": reports["constrained_pool6"]["by_category"][
                "health"
            ],
            "after_health": reports["constrained_pool6_health_cache"][
                "by_category"
            ]["health"],
            "health_p95_latency_change_pct": pct_change(
                reports["constrained_pool6"]["by_category"]["health"][
                    "latency_ms"
                ]["p95"],
                reports["constrained_pool6_health_cache"]["by_category"][
                    "health"
                ]["latency_ms"]["p95"],
            ),
            "mixed_throughput_change_pct": pct_change(
                reports["constrained_pool6"]["requests"]["throughput_rps"],
                reports["constrained_pool6_health_cache"]["requests"][
                    "throughput_rps"
                ],
            ),
        },
    }
    transfer_rates = [10, 20, 50, 100, 250]
    transfer_times = {
        str(rate): transfer_seconds(archive_bytes, rate) for rate in transfer_rates
    }

    map_results = []
    for result in browser["results"]:
        map_results.append(
            {
                "profile": result["profile"],
                "usable_ms": result["usable_ms"],
                "visible_region_settle_ms": result["visible_region_settle_ms"],
                "search_result_ms": result["search_result_ms"],
                "selection_ms": result["selection_ms"],
                "network_requests": result["network"]["requests"],
                "network_encoded_bytes": result["network"]["encodedBytes"],
                "network_failed": result["network"]["failed"],
                "visible_points": int(
                    result["renderer"]["mapTileRenderedSystems"]
                ),
                "visible_labels": int(result["renderer"]["mapLabelCount"]),
                "tile_failures": int(result["renderer"]["mapTileFailures"]),
                "javascript_heap_bytes": result["heap"]["usedJSHeapSize"],
                "frame_time_ms": result["frame_time_ms"],
            }
        )

    payload = {
        "schema_version": "spacegate.runtime_capacity_gate.v1",
        "generated_at_utc": utc_now(),
        "build_id": BUILD_ID,
        "decision": {
            "recommendation": "no_go",
            "scope": "complete Evidence Lake runtime on existing antiproton",
            "reasons": [
                "mixed request p95 exceeds the 3000 ms gate at concurrency 1",
                "mixed throughput saturates near 3 requests/s at concurrency 6",
                "search and detail scans dominate CPU and worker queueing",
                "current antiproton disk cannot stage the unpruned payload safely",
            ],
            "scientific_content_weakened": False,
            "deployment_performed": False,
            "antiproton_load_tested": False,
            "proton_mutated": False,
        },
        "resource_model": reports["constrained_pool6"]["environment"][
            "resource_model"
        ],
        "correctness": {
            "tess_browser_tests": {
                "expected": correctness_playwright["stats"]["expected"],
                "unexpected": correctness_playwright["stats"]["unexpected"],
                "status": (
                    "pass"
                    if correctness_playwright["stats"]["unexpected"] == 0
                    else "fail"
                ),
            },
            "map_browser_tests": {
                "expected": map_playwright["stats"]["expected"],
                "unexpected": map_playwright["stats"]["unexpected"],
                "status": (
                    "pass"
                    if map_playwright["stats"]["unexpected"] == 0
                    else "fail"
                ),
            },
            "nested_orbits": {
                "status": nested_orbit_runtime["status"],
                "case_count": nested_orbit_runtime["case_count"],
                "warnings": nested_orbit_runtime["warnings"],
                "compiler_status": orbit_compiler_verification["status"],
                "compiler_failing_checks": orbit_compiler_verification[
                    "failing_checks"
                ],
                "runtime_graph_status": orbit_compiler_verification[
                    "runtime_graph_status"
                ],
            },
        },
        "runs": {label: request_metrics(report) for label, report in reports.items()},
        "staircase": {
            "first_slo_failure_concurrency": staircase[
                "first_slo_failure_concurrency"
            ],
            "highest_completed_concurrency": staircase[
                "highest_completed_concurrency"
            ],
            "highest_slo_concurrency": staircase["highest_slo_concurrency"],
            "steps": staircase["steps"],
            "recovery": staircase["recovery"],
        },
        "optimizations": optimizations,
        "browser_1000ly": map_results,
        "deployment_storage": {
            "candidate_logical_bytes": logical_bytes,
            "candidate_allocated_bytes": allocated_bytes,
            "candidate_archive_bytes": archive_bytes,
            "candidate_archive_sha256": sha256(archive),
            "compression_ratio": round(archive_bytes / logical_bytes, 6),
            "remote_observed": {
                "method": "read-only SSH inventory; no state mutation",
                "observed_at_utc": args.remote_observed_at_utc,
                "total_bytes": args.remote_total_bytes,
                "used_bytes": args.remote_used_bytes,
                "available_bytes": args.remote_available_bytes,
                "current_build_id": args.remote_current_build,
                "current_build_bytes": args.remote_current_build_bytes,
                "standby_build_bytes": args.remote_standby_build_bytes,
                "current_archive_bytes": args.remote_current_archive_bytes,
                "standby_archive_bytes": args.remote_standby_archive_bytes,
                "cache_bytes": args.remote_cache_bytes,
            },
            "pretransfer_cleanup_candidate": {
                "bytes": cleanup_candidate_bytes,
                "performed": False,
                "requires_operator_review": True,
                "contents": [
                    "superseded extracted standby build",
                    "current and superseded compressed archives after hash review",
                ],
            },
            "available_after_candidate_cleanup_bytes": (
                available_after_reviewed_cleanup
            ),
            "candidate_staging_peak_addition_bytes": deployment_peak_addition,
            "available_at_staging_peak_bytes": available_at_peak,
            "available_after_candidate_archive_retirement_bytes": (
                available_after_archive_retirement
            ),
            "transfer_seconds_at_effective_rate": transfer_times,
            "transfer_rate_unit": "nominal Mbps with 85% payload efficiency",
        },
        "operational_thresholds": {
            "mixed_supported_concurrency_at_3000ms_p95": 0,
            "measured_mixed_saturation_concurrency": 6,
            "measured_mixed_saturation_rps": 3.058508,
            "max_api_pool_connections": 6,
            "max_duckdb_threads_per_connection": 1,
            "max_aggregate_service_memory_bytes": 8 * 1024**3,
            "alert_api_rss_bytes": 6 * 1024**3,
            "alert_p95_latency_ms": 3000,
            "alert_error_rate_pct": 1.0,
            "alert_queue_delay_ms": 1000,
            "alert_disk_available_bytes": 15 * 1024**3,
        },
        "required_follow_up": [
            "build an immutable indexed search projection and bounded detail summaries",
            "repeat this exact constrained campaign against the new consumer contract",
            "review and retire only the measured antiproton standby/archive set",
            "retain the current extracted build as rollback through public verification",
            "upgrade or separate API compute if the new projection still misses budgets",
        ],
        "evidence": refs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
