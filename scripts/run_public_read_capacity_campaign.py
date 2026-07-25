#!/usr/bin/env python3
"""Run the pinned M8.3e capacity campaign and its concurrency staircase."""

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
DEFAULT_CAMPAIGN = ROOT / "config/runtime_capacity/campaign_public_read_v2.json"
DEFAULT_STATE = Path(
    os.getenv("SPACEGATE_STATE_DIR")
    or os.getenv("SPACEGATE_DATA_DIR")
    or "/data/spacegate/state"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def load_campaign(path: Path) -> tuple[dict[str, Any], Path]:
    campaign = json.loads(path.read_text(encoding="utf-8"))
    workload = ROOT / str(campaign.get("workload_manifest") or "")
    workload = workload.resolve(strict=True)
    workload_payload = json.loads(workload.read_text(encoding="utf-8"))
    if campaign.get("build_id") != workload_payload.get("build_id"):
        raise SystemExit("campaign and workload build identities differ")
    labels = [str(row.get("label") or "") for row in campaign.get("runs") or []]
    if not labels or any(not value for value in labels):
        raise SystemExit("campaign has an unlabeled run")
    if len(labels) != len(set(labels)):
        raise SystemExit("campaign labels must be unique")
    return campaign, workload


def run_command(command: list[str], *, dry_run: bool) -> int:
    print("+ " + " ".join(command), flush=True)
    if dry_run:
        return 0
    return int(subprocess.run(command, check=False).returncode)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, default=DEFAULT_CAMPAIGN)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--unconstrained-url", default="http://127.0.0.1")
    parser.add_argument("--constrained-url", default="http://127.0.0.1:18081")
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    parser.add_argument("--start-at-label", default="")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started_at = utc_now()
    campaign_path = args.campaign.resolve(strict=True)
    campaign, workload = load_campaign(campaign_path)
    state_dir = args.state_dir.resolve(strict=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else state_dir
        / "reports/runtime_capacity_gate/public_read_v2"
        / f"final_{run_id}"
    )
    if output_dir.exists() and not args.skip_existing and not args.dry_run:
        raise SystemExit(f"output directory already exists: {output_dir}")
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    python = ROOT / ".venv/bin/python"
    if not python.is_file():
        python = Path(sys.executable)
    harness = ROOT / "scripts/runtime_capacity_harness.py"
    slo = ROOT / "scripts/check_profile_slo.py"
    results: list[dict[str, Any]] = []
    started = not bool(args.start_at_label)

    for run in campaign["runs"]:
        label = str(run["label"])
        if not started:
            started = label == args.start_at_label
            if not started:
                continue
        environment = str(run["environment_profile"])
        unconstrained = environment == "unconstrained_photon"
        base_url = (
            args.unconstrained_url if unconstrained else args.constrained_url
        )
        containers = (
            "spacegate-api-1,spacegate-web-1"
            if unconstrained
            else "spacegate-capacity-api-1,spacegate-capacity-web-1"
        )
        run_dir = output_dir / label
        if (run_dir / "summary.json").is_file() and args.skip_existing:
            results.append({"label": label, "status": "retained_existing"})
            continue
        command = [
            str(python),
            str(harness),
            "--manifest",
            str(workload),
            "--profile",
            str(run["profile"]),
            "--base-url",
            base_url,
            "--state-dir",
            str(state_dir),
            "--output-dir",
            str(run_dir),
            "--duration-seconds",
            str(run["duration_seconds"]),
            "--concurrency",
            str(run["concurrency"]),
            "--containers",
            containers,
            "--environment-profile",
            environment,
            "--cache-state",
            str(run["cache_state"]),
            "--timeout-seconds",
            str(args.timeout_seconds),
            "--label",
            label,
        ]
        if run.get("target_rps") is not None:
            command.extend(["--target-rps", str(run["target_rps"])])
        if run.get("request_limit") is not None:
            command.extend(["--request-limit", str(run["request_limit"])])
        if run.get("evict_file_cache"):
            command.append("--evict-file-cache")
        harness_code = run_command(command, dry_run=args.dry_run)
        slo_code = 0
        if not args.dry_run and (run_dir / "summary.json").is_file():
            slo_code = run_command(
                [
                    str(python),
                    str(slo),
                    "--capacity-report",
                    str(run_dir / "summary.json"),
                    "--report-path",
                    str(run_dir / "capacity_slo_report.json"),
                ],
                dry_run=False,
            )
        results.append(
            {
                "label": label,
                "harness_returncode": harness_code,
                "slo_returncode": slo_code,
                "status": (
                    "planned"
                    if args.dry_run
                    else "pass"
                    if harness_code == 0 and slo_code == 0
                    else "fail"
                ),
            }
        )
        if harness_code != 0 and not args.dry_run:
            break

    if args.start_at_label and not started:
        raise SystemExit(f"unknown --start-at-label: {args.start_at_label}")

    staircase = campaign["staircase"]
    staircase_dir = output_dir / "staircase"
    staircase_command = [
        str(python),
        str(ROOT / "scripts/run_runtime_capacity_staircase.py"),
        "--manifest",
        str(workload),
        "--profile",
        str(staircase["profile"]),
        "--steps",
        ",".join(str(value) for value in staircase["steps"]),
        "--duration-seconds",
        str(staircase["duration_seconds_per_step"]),
        "--recovery-seconds",
        str(staircase["recovery_seconds"]),
        "--base-url",
        args.constrained_url,
        "--state-dir",
        str(state_dir),
        "--output-dir",
        str(staircase_dir),
        "--containers",
        "spacegate-capacity-api-1,spacegate-capacity-web-1",
        "--environment-profile",
        str(staircase["environment_profile"]),
    ]
    if staircase.get("continue_through_slo_fail"):
        staircase_command.append("--continue-through-slo-fail")
    staircase_code = (
        0
        if (staircase_dir / "staircase_summary.json").is_file()
        and args.skip_existing
        else run_command(staircase_command, dry_run=args.dry_run)
    )
    status = (
        "planned"
        if args.dry_run
        else "pass"
        if all(row["status"] in {"pass", "retained_existing"} for row in results)
        and staircase_code == 0
        else "fail"
    )
    report = {
        "schema_version": "spacegate.public_read_capacity_campaign_run.v1",
        "status": status,
        "campaign_id": campaign.get("campaign_id"),
        "build_id": campaign.get("build_id"),
        "started_at_utc": started_at,
        "completed_at_utc": utc_now(),
        "campaign": {
            "path": str(campaign_path),
            "sha256": sha256(campaign_path),
        },
        "workload": {"path": str(workload), "sha256": sha256(workload)},
        "runs": results,
        "staircase_returncode": staircase_code,
        "output_dir": str(output_dir),
    }
    if not args.dry_run:
        atomic_json(output_dir / "campaign_run.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if status in {"pass", "planned"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
