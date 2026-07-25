#!/usr/bin/env python3
"""Compile M8.3e controls, capacity runs, and deployment sizing."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "spacegate.public_read_capacity_gate.v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def file_ref(path: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    return {
        "path": str(resolved),
        "bytes": resolved.stat().st_size,
        "sha256": sha256(resolved),
    }


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def request_metrics(report: dict[str, Any]) -> dict[str, Any]:
    requests = report.get("requests") or {}
    resources = report.get("resources") or {}
    return {
        "profile": report.get("profile"),
        "label": report.get("label"),
        "environment_profile": (
            (report.get("environment") or {}).get("resource_model") or {}
        ).get("profile"),
        "cache_state": (report.get("workload") or {}).get("cache_state"),
        "concurrency": (report.get("workload") or {}).get("concurrency"),
        "duration_seconds": (report.get("workload") or {}).get("duration_seconds"),
        "request_count": requests.get("request_count"),
        "throughput_rps": requests.get("throughput_rps"),
        "latency_ms": requests.get("latency_ms"),
        "queue_delay_ms": requests.get("queue_delay_ms"),
        "error_rate_pct": requests.get("error_rate_pct"),
        "timeout_count": requests.get("timeout_count"),
        "scene_cache_counts": requests.get("scene_cache_counts"),
        "aggregate_cgroup_memory_peak_bytes": resources.get(
            "aggregate_cgroup_memory_peak_bytes"
        ),
        "database_runtime_delta": (
            report.get("database_runtime") or {}
        ).get("numeric_delta"),
        "harness_gates": report.get("gates") or {},
    }


def collect_campaign(
    campaign_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    campaign_run_path = campaign_dir / "campaign_run.json"
    if not campaign_run_path.is_file():
        raise SystemExit(
            f"campaign completion manifest is missing: {campaign_run_path}"
        )
    campaign_run = load_json(campaign_run_path)
    if (
        campaign_run.get("schema_version")
        != "spacegate.public_read_capacity_campaign_run.v1"
    ):
        raise SystemExit("campaign completion manifest has an incompatible schema")
    runs: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = [file_ref(campaign_run_path)]
    for summary_path in sorted(campaign_dir.glob("*/summary.json")):
        report = load_json(summary_path)
        if report.get("schema_version") != "spacegate.runtime_capacity_report.v1":
            continue
        slo_path = summary_path.with_name("capacity_slo_report.json")
        if not slo_path.is_file():
            raise SystemExit(f"capacity run lacks SLO report: {summary_path}")
        slo = load_json(slo_path)
        runs.append(
            {
                "run_dir": str(summary_path.parent),
                "metrics": request_metrics(report),
                "slo_status": slo.get("status"),
                "slo_gates": slo.get("gates") or {},
            }
        )
        evidence.extend([file_ref(summary_path), file_ref(slo_path)])
    staircase_paths = sorted(campaign_dir.rglob("staircase_summary.json"))
    staircases = []
    for path in staircase_paths:
        staircases.append(load_json(path))
        evidence.append(file_ref(path))
    if not runs:
        raise SystemExit(f"campaign contains no capacity summaries: {campaign_dir}")
    expected_labels = {
        str(row.get("label") or "") for row in campaign_run.get("runs") or []
    }
    observed_labels = {str(row["metrics"].get("label") or "") for row in runs}
    if not expected_labels or expected_labels != observed_labels:
        raise SystemExit(
            "campaign completion manifest and capacity summaries disagree"
        )
    status = (
        "pass"
        if campaign_run.get("status") == "pass"
        and all(row["slo_status"] == "pass" for row in runs)
        else "fail"
    )
    return (
        {
            "status": status,
            "completion": campaign_run,
            "run_count": len(runs),
            "staircase_count": len(staircases),
            "runs": runs,
            "staircases": staircases,
        },
        evidence,
        runs,
    )


def collect_controls(control_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    controls: dict[str, Any] = {}
    evidence: list[dict[str, Any]] = []
    for path in sorted(control_dir.glob("*/summary.json")):
        if not path.parent.name.endswith("_control"):
            continue
        report = load_json(path)
        controls[path.parent.name] = request_metrics(report)
        evidence.append(file_ref(path))
    if not controls:
        raise SystemExit(f"no retained controls found: {control_dir}")
    return controls, evidence


def allocated_bytes(root: Path) -> int:
    return sum(
        path.stat().st_blocks * 512
        for path in [root, *root.rglob("*")]
        if path.exists()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-dir", required=True, type=Path)
    parser.add_argument("--control-dir", required=True, type=Path)
    parser.add_argument("--build-dir", required=True, type=Path)
    parser.add_argument("--build-archive", required=True, type=Path)
    parser.add_argument("--public-read-manifest", required=True, type=Path)
    parser.add_argument("--scene-manifest", required=True, type=Path)
    parser.add_argument("--prior-capacity-gate", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    campaign_dir = args.campaign_dir.resolve(strict=True)
    controls, control_evidence = collect_controls(
        args.control_dir.resolve(strict=True)
    )
    campaign, campaign_evidence, _runs = collect_campaign(campaign_dir)
    build_dir = args.build_dir.resolve(strict=True)
    build_archive = args.build_archive.resolve(strict=True)
    public_manifest_path = args.public_read_manifest.resolve(strict=True)
    public_manifest = load_json(public_manifest_path)
    public_database = public_manifest_path.parent / str(
        (public_manifest.get("artifact") or {}).get("path")
    )
    scene_manifest_path = args.scene_manifest.resolve(strict=True)
    scene_manifest = load_json(scene_manifest_path)
    scene_archive = scene_manifest_path.parent / str(
        (scene_manifest.get("archive") or {}).get("path")
    )
    prior_gate_path = args.prior_capacity_gate.resolve(strict=True)
    prior_gate = load_json(prior_gate_path)

    build_id = str(public_manifest.get("build_id") or "")
    if not build_id or build_dir.name != build_id:
        raise SystemExit("scientific build and public-read build identities differ")
    if scene_manifest.get("build_id") != build_id:
        raise SystemExit("scene and public-read build identities differ")
    if (prior_gate.get("build_id") or "") != build_id:
        raise SystemExit("prior capacity inventory belongs to another build")
    for path in (public_database, scene_archive):
        if not path.is_file():
            raise SystemExit(f"missing deployment artifact: {path}")

    scientific_logical = sum(
        path.stat().st_size for path in build_dir.rglob("*") if path.is_file()
    )
    scientific_allocated = allocated_bytes(build_dir)
    public_read_bytes = public_database.stat().st_size
    scene_bytes = scene_archive.stat().st_size
    transfer_payload = build_archive.stat().st_size + public_read_bytes + scene_bytes
    streamed_peak_addition = scientific_allocated + public_read_bytes + scene_bytes
    staged_archive_peak_addition = streamed_peak_addition + build_archive.stat().st_size

    prior_storage = prior_gate.get("deployment_storage") or {}
    remote = prior_storage.get("remote_observed") or {}
    available_after_cleanup = int(
        prior_storage.get("available_after_candidate_cleanup_bytes") or 0
    )
    streamed_reserve = available_after_cleanup - streamed_peak_addition
    staged_reserve = available_after_cleanup - staged_archive_peak_addition
    transfer_seconds = {
        str(rate_mbps): round(
            transfer_payload * 8 / (rate_mbps * 1_000_000 * 0.85)
        )
        for rate_mbps in (10, 20, 50, 100, 250)
    }
    runtime_status = campaign["status"]
    minimum_reserve = 15 * 1024**3
    if runtime_status != "pass":
        recommendation = "no_go"
        reasons = ["one or more constrained runtime SLO profiles failed"]
    elif streamed_reserve < minimum_reserve:
        recommendation = "no_go"
        reasons = [
            "even streamed extraction leaves less than the 15-GiB operational disk reserve"
        ]
    elif staged_reserve < minimum_reserve:
        recommendation = "conditional_go"
        reasons = [
            "runtime SLOs pass",
            "deployment requires reviewed prior cleanup and streamed extraction because retaining the scientific archive at peak violates the disk reserve",
        ]
    else:
        recommendation = "go"
        reasons = ["runtime SLOs and staged deployment reserve pass"]

    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "build_id": build_id,
        "decision": {
            "recommendation": recommendation,
            "reasons": reasons,
            "scientific_content_weakened": False,
            "deployment_performed": False,
            "antiproton_contacted": False,
            "proton_mutated": False,
        },
        "controls": controls,
        "campaign": campaign,
        "artifacts": {
            "scientific_build_logical_bytes": scientific_logical,
            "scientific_build_allocated_bytes": scientific_allocated,
            "scientific_build_archive": file_ref(build_archive),
            "public_read_database": file_ref(public_database),
            "simulation_scene_archive": file_ref(scene_archive),
            "exact_transfer_payload_bytes": transfer_payload,
        },
        "deployment_storage": {
            "inventory_source": "retained M8.3d read-only antiproton inventory",
            "inventory_observed_at_utc": remote.get("observed_at_utc"),
            "remote_available_before_cleanup_bytes": remote.get("available_bytes"),
            "remote_available_after_reviewed_cleanup_bytes": available_after_cleanup,
            "current_extracted_build_retained_as_rollback_bytes": remote.get(
                "current_build_bytes"
            ),
            "streamed_extraction_peak_addition_bytes": streamed_peak_addition,
            "streamed_extraction_reserve_bytes": streamed_reserve,
            "archive_staged_peak_addition_bytes": staged_archive_peak_addition,
            "archive_staged_reserve_bytes": staged_reserve,
            "minimum_operational_reserve_bytes": minimum_reserve,
            "transfer_seconds_at_effective_rate": transfer_seconds,
            "transfer_rate_unit": "nominal Mbps with 85% payload efficiency",
            "rollback_policy": (
                "retain the current extracted build until public verification; "
                "retire only separately reviewed standby/archive artifacts"
            ),
        },
        "evidence": [
            *control_evidence,
            *campaign_evidence,
            file_ref(public_manifest_path),
            file_ref(scene_manifest_path),
            file_ref(prior_gate_path),
        ],
    }
    atomic_json(args.output.resolve(), payload)
    print(
        json.dumps(
            {
                "status": runtime_status,
                "recommendation": recommendation,
                "campaign_runs": campaign["run_count"],
                "transfer_payload_bytes": transfer_payload,
                "streamed_reserve_bytes": streamed_reserve,
                "output": str(args.output.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if runtime_status == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
