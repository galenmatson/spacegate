#!/usr/bin/env python3
"""Run a bounded Spacegate concurrency staircase and stop at saturation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = (
    ROOT / "config/runtime_capacity/workload_e7_24cb15211f430a37.json"
)
DEFAULT_STATE = Path(
    os.getenv("SPACEGATE_STATE_DIR")
    or os.getenv("SPACEGATE_DATA_DIR")
    or "/data/spacegate/state"
)


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write_json(path: Path, payload: Any) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def parse_steps(raw: str) -> list[int]:
    values = [int(value.strip()) for value in raw.split(",") if value.strip()]
    if not values or any(value <= 0 for value in values):
        raise argparse.ArgumentTypeError("steps must be positive integers")
    if values != sorted(set(values)):
        raise argparse.ArgumentTypeError("steps must be unique and increasing")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--profile", default="mixed")
    parser.add_argument("--steps", type=parse_steps, default=parse_steps("1,2,4,8,12,16,24,32"))
    parser.add_argument("--duration-seconds", type=float, default=45.0)
    parser.add_argument("--base-url", default="http://127.0.0.1:18081")
    parser.add_argument("--state-dir", default=str(DEFAULT_STATE))
    parser.add_argument("--output-dir", default="")
    parser.add_argument(
        "--containers",
        default="spacegate-capacity-api-1,spacegate-capacity-web-1",
    )
    parser.add_argument(
        "--environment-profile",
        choices=("antiproton_like", "unconstrained_photon"),
        default="antiproton_like",
    )
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    parser.add_argument("--recovery-seconds", type=float, default=20.0)
    parser.add_argument(
        "--continue-through-slo-fail",
        action="store_true",
        help="Continue measuring higher steps until a harness safety gate fails.",
    )
    parser.add_argument("--insecure-tls", action="store_true")
    return parser.parse_args()


def run_command(command: list[str]) -> int:
    completed = subprocess.run(command, check=False)
    return int(completed.returncode)


def main() -> int:
    args = parse_args()
    if args.duration_seconds <= 0 or args.recovery_seconds <= 0:
        raise SystemExit("duration and recovery values must be positive")

    manifest_path = Path(args.manifest).resolve(strict=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    profile = (manifest.get("profiles") or {}).get(args.profile)
    if not profile:
        raise SystemExit(f"unknown workload profile: {args.profile}")
    build_id = str(manifest.get("build_id") or "")
    if not build_id:
        raise SystemExit("workload manifest has no build id")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(args.state_dir)
        / "reports/runtime_capacity_gate"
        / build_id
        / f"{run_id}_staircase_{args.profile}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    python = ROOT / ".venv/bin/python"
    if not python.is_file():
        python = Path(sys.executable)
    harness = ROOT / "scripts/runtime_capacity_harness.py"
    slo = ROOT / "scripts/check_profile_slo.py"
    steps: list[dict[str, Any]] = []
    stop_reason = "steps_exhausted"
    first_slo_failure: int | None = None
    started_at = utc_now()

    for concurrency in args.steps:
        step_dir = output_dir / f"c{concurrency:03d}"
        harness_command = [
            str(python),
            str(harness),
            "--manifest",
            str(manifest_path),
            "--profile",
            args.profile,
            "--base-url",
            args.base_url,
            "--state-dir",
            args.state_dir,
            "--output-dir",
            str(step_dir),
            "--duration-seconds",
            str(args.duration_seconds),
            "--concurrency",
            str(concurrency),
            "--containers",
            args.containers,
            "--environment-profile",
            args.environment_profile,
            "--cache-state",
            "warm",
            "--label",
            f"staircase_c{concurrency}",
            "--timeout-seconds",
            str(args.timeout_seconds),
        ]
        if args.insecure_tls:
            harness_command.append("--insecure-tls")
        harness_returncode = run_command(harness_command)
        summary_path = step_dir / "summary.json"
        if not summary_path.is_file():
            stop_reason = "missing_step_report"
            steps.append(
                {
                    "concurrency": concurrency,
                    "harness_returncode": harness_returncode,
                    "status": "missing_report",
                }
            )
            break

        slo_path = step_dir / "capacity_slo_report.json"
        slo_returncode = run_command(
            [
                str(python),
                str(slo),
                "--capacity-report",
                str(summary_path),
                "--report-path",
                str(slo_path),
            ]
        )
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        slo_report = json.loads(slo_path.read_text(encoding="utf-8"))
        step = {
            "concurrency": concurrency,
            "harness_returncode": harness_returncode,
            "slo_returncode": slo_returncode,
            "status": "pass" if slo_returncode == 0 else "fail",
            "summary_path": str(summary_path),
            "summary_sha256": sha256(summary_path),
            "slo_path": str(slo_path),
            "slo_sha256": sha256(slo_path),
            "throughput_rps": (summary.get("requests") or {}).get(
                "throughput_rps"
            ),
            "p95_latency_ms": (
                (summary.get("requests") or {}).get("latency_ms") or {}
            ).get("p95"),
            "error_rate_pct": (summary.get("requests") or {}).get(
                "error_rate_pct"
            ),
            "aggregate_cgroup_memory_peak_bytes": (
                summary.get("resources") or {}
            ).get("aggregate_cgroup_memory_peak_bytes"),
            "stop_reasons": (summary.get("stop") or {}).get("reasons") or [],
        }
        steps.append(step)
        if harness_returncode != 0:
            stop_reason = "harness_safety_gate"
            break
        if slo_returncode != 0:
            if first_slo_failure is None:
                first_slo_failure = concurrency
            if not args.continue_through_slo_fail:
                stop_reason = "profile_slo_saturation"
                break

    sustainable = [
        int(step["concurrency"])
        for step in steps
        if step.get("status") == "pass"
    ]
    recovery_dir = output_dir / "recovery"
    recovery_command = [
            str(python),
            str(harness),
            "--manifest",
            str(manifest_path),
            "--profile",
            args.profile,
            "--base-url",
            args.base_url,
            "--state-dir",
            args.state_dir,
            "--output-dir",
            str(recovery_dir),
            "--duration-seconds",
            str(args.recovery_seconds),
            "--concurrency",
            "1",
            "--containers",
            args.containers,
            "--environment-profile",
            args.environment_profile,
            "--cache-state",
            "warm",
            "--label",
            "staircase_recovery",
            "--timeout-seconds",
            str(args.timeout_seconds),
        ]
    if args.insecure_tls:
        recovery_command.append("--insecure-tls")
    recovery_returncode = run_command(recovery_command)
    recovery_summary = recovery_dir / "summary.json"
    harness_failure = any(
        int(step.get("harness_returncode") or 0) != 0
        or step.get("status") == "missing_report"
        for step in steps
    )
    report = {
        "schema_version": "spacegate.runtime_capacity_staircase.v1",
        "status": (
            "pass"
            if steps and not harness_failure and recovery_returncode == 0
            else "fail"
        ),
        "started_at_utc": started_at,
        "completed_at_utc": utc_now(),
        "build_id": build_id,
        "environment_profile": args.environment_profile,
        "profile": args.profile,
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256(manifest_path),
        "planned_steps": args.steps,
        "duration_seconds_per_step": args.duration_seconds,
        "steps": steps,
        "stop_reason": stop_reason,
        "first_slo_failure_concurrency": first_slo_failure,
        "highest_slo_concurrency": max(sustainable, default=None),
        "highest_completed_concurrency": max(
            (int(step["concurrency"]) for step in steps),
            default=None,
        ),
        "recovery": {
            "returncode": recovery_returncode,
            "summary_path": (
                str(recovery_summary) if recovery_summary.is_file() else None
            ),
            "summary_sha256": (
                sha256(recovery_summary) if recovery_summary.is_file() else None
            ),
        },
    }
    atomic_write_json(output_dir / "staircase_summary.json", report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "highest_slo_concurrency": report[
                    "highest_slo_concurrency"
                ],
                "highest_completed_concurrency": report[
                    "highest_completed_concurrency"
                ],
                "first_slo_failure_concurrency": report[
                    "first_slo_failure_concurrency"
                ],
                "stop_reason": stop_reason,
                "output_dir": str(output_dir),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
