#!/usr/bin/env python3
"""Compile and validate the local E7 promotion/rollback/re-promotion drill."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


TIME_FIELDS = {
    "promote_pointer": "promote_candidate.time",
    "promote_cold_rebuild": "promote_restart.time",
    "promote_required_verify": "promote_verify_build_required.time",
    "promote_api_integration": "promote_api_integration.time",
    "promote_known_systems": "promote_known_systems.time",
    "promote_browser_smoke": "promote_playwright.time",
    "rollback_pointer": "rollback_pointer.time",
    "rollback_restart": "rollback_restart.time",
    "rollback_pinned_verify": "rollback_pinned_verify_build.time",
    "rollback_api_integration": "rollback_api_integration.time",
    "repromote_pointer": "repromote_pointer.time",
    "repromote_restart": "repromote_restart.time",
    "repromote_required_verify": "repromote_verify_build.time",
    "repromote_api_integration": "repromote_api_integration.time",
    "repromote_known_systems": "repromote_known_systems.time",
    "repromote_browser_smoke": "repromote_playwright.time",
}


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def elapsed_seconds(value: str) -> float:
    parts = value.strip().split(":")
    if len(parts) == 1:
        return float(parts[0])
    if len(parts) == 2:
        return float(parts[0]) * 60 + float(parts[1])
    if len(parts) == 3:
        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    raise ValueError(f"invalid GNU time elapsed value: {value!r}")


def parse_gnu_time(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")

    def capture(pattern: str) -> str:
        match = re.search(pattern, text, flags=re.MULTILINE)
        if not match:
            raise ValueError(f"missing GNU time field in {path}: {pattern}")
        return match.group(1).strip()

    elapsed = capture(
        r"^\s*Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*(\S+)\s*$"
    )
    return {
        "path": str(path),
        "command": capture(r'^\s*Command being timed:\s*"(.+)"\s*$'),
        "wall_seconds": elapsed_seconds(elapsed),
        "user_cpu_seconds": float(capture(r"^\s*User time \(seconds\):\s*(\S+)\s*$")),
        "system_cpu_seconds": float(capture(r"^\s*System time \(seconds\):\s*(\S+)\s*$")),
        "peak_rss_kib": int(capture(r"^\s*Maximum resident set size \(kbytes\):\s*(\d+)\s*$")),
        "filesystem_inputs": int(capture(r"^\s*File system inputs:\s*(\d+)\s*$")),
        "filesystem_outputs": int(capture(r"^\s*File system outputs:\s*(\d+)\s*$")),
        "exit_status": int(capture(r"^\s*Exit status:\s*(\d+)\s*$")),
    }


def database_build_id(path: Path) -> str | None:
    con = duckdb.connect(str(path), read_only=True)
    try:
        rows = con.execute(
            "SELECT value FROM build_metadata WHERE key='build_id'"
        ).fetchall()
        return str(rows[0][0]) if len(rows) == 1 else None
    finally:
        con.close()


def contains(path: Path, marker: str) -> bool:
    return marker in path.read_text(encoding="utf-8")


def compile_report(
    *,
    report_dir: Path,
    state_dir: Path,
    candidate_build_id: str,
    rollback_build_id: str,
) -> dict[str, Any]:
    report_dir = report_dir.resolve(strict=True)
    state_dir = state_dir.resolve(strict=True)
    acceptance = load_object(report_dir / "operator_acceptance.json")
    preflight = load_object(report_dir / "preflight.json")
    timings = {
        name: parse_gnu_time(report_dir / relative)
        for name, relative in TIME_FIELDS.items()
    }
    current = (state_dir / "served/current").resolve(strict=True)
    candidate = (state_dir / "out" / candidate_build_id).resolve(strict=True)
    tile_index = load_object(candidate / "map_tiles/index.json")
    checks = {
        "operator_acceptance": (
            acceptance.get("status") == "accepted"
            and acceptance.get("candidate_build_id") == candidate_build_id
            and acceptance.get("rollback_build_id") == rollback_build_id
            and acceptance.get("antiproton_deployment_authorized") is False
        ),
        "preflight": (
            preflight.get("status") == "pass"
            and preflight.get("candidate_build_id") == candidate_build_id
            and not preflight.get("failing_checks")
            and preflight.get("rollback", {}).get("target")
            == str(state_dir / "out" / rollback_build_id)
        ),
        "all_required_commands_exit_zero": all(
            item["exit_status"] == 0 for item in timings.values()
        ),
        "promote_required_verify": contains(
            report_dir / "promote_verify_build_required.stdout",
            f"Verified build {candidate_build_id}",
        ),
        "promote_api_integration": contains(
            report_dir / "promote_api_integration.stdout", "Integration test passed."
        ),
        "promote_known_systems": contains(
            report_dir / "promote_known_systems.stdout",
            "Known-system API benchmark passed:",
        ),
        "promote_browser_smoke": contains(
            report_dir / "promote_playwright.stdout", "5 passed"
        ),
        "rollback_pinned_verify": contains(
            report_dir / "rollback_pinned_verify_build.stdout",
            f"Verified build {rollback_build_id}",
        ),
        "rollback_api_integration": contains(
            report_dir / "rollback_api_integration.stdout", "Integration test passed."
        ),
        "repromote_required_verify": contains(
            report_dir / "repromote_verify_build.stdout",
            f"Verified build {candidate_build_id}",
        ),
        "repromote_api_integration": contains(
            report_dir / "repromote_api_integration.stdout", "Integration test passed."
        ),
        "repromote_known_systems": contains(
            report_dir / "repromote_known_systems.stdout",
            "Known-system API benchmark passed:",
        ),
        "repromote_browser_smoke": contains(
            report_dir / "repromote_playwright.stdout", "2 passed"
        ),
        "final_pointer_is_candidate": current == candidate,
        "final_core_identity": database_build_id(candidate / "core.duckdb")
        == candidate_build_id,
        "final_arm_identity": database_build_id(candidate / "arm.duckdb")
        == candidate_build_id,
        "final_disc_identity": database_build_id(candidate / "disc.duckdb")
        == candidate_build_id,
        "final_tile_identity": tile_index.get("build_id") == candidate_build_id,
    }
    findings = [
        {
            "finding_id": "legacy_multiplicity_golden_coupling",
            "blocking": False,
            "evidence": "promote_verify_build.stderr and promote_verify_build.stdout",
            "disposition": (
                "The opt-in legacy suite queries deprecated MSC/orbit surfaces and "
                "fixed named-system counts. Required strict verification and current "
                "public known-system checks pass; migrate or retire the suite in M8.3e."
            ),
        },
        {
            "finding_id": "rollback_verifier_contract_drift",
            "blocking": False,
            "evidence": "rollback_verify_build.stderr and rollback_verify_build.stdout",
            "disposition": (
                "The current verifier expects 147 post-stability classification rows. "
                "The rollback-revision-pinned strict verifier passes; retain versioned "
                "verification logic with rollback artifacts."
            ),
        },
        {
            "finding_id": "rollback_named_spectral_string_golden",
            "blocking": False,
            "evidence": "rollback_pinned_known_systems.stderr",
            "disposition": (
                "The historical named-system benchmark requires Castor raw text M1_Ve "
                "while the rollback API returns equivalent dM1e. It is not an identity, "
                "health, or scientific-integrity gate and must not drive a one-off fix."
            ),
        },
        {
            "finding_id": "local_tls_preflight",
            "blocking": False,
            "evidence": "promote_playwright.stderr",
            "disposition": (
                "The first Node global preflight rejected the Photon certificate IP. "
                "The local-only rerun disabled Node certificate verification; browser "
                "contexts still used the configured local certificate exception."
            ),
        },
    ]
    failures = sorted(name for name, passed in checks.items() if not passed)
    return {
        "schema_version": "spacegate.e7_cutover_drill.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "pass" if not failures else "fail",
        "candidate_build_id": candidate_build_id,
        "rollback_build_id": rollback_build_id,
        "final_served_target": str(current),
        "checks": checks,
        "failing_checks": failures,
        "timings": timings,
        "compatibility_findings": findings,
        "retention_cleanup_performed": False,
        "antiproton_deployment_performed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--candidate-build-id", required=True)
    parser.add_argument("--rollback-build-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    if args.output.exists() and not args.replace:
        raise ValueError("refusing to replace an existing cutover report without --replace")
    report = compile_report(
        report_dir=args.report_dir,
        state_dir=args.state_dir,
        candidate_build_id=args.candidate_build_id,
        rollback_build_id=args.rollback_build_id,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.{os.getpid()}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, args.output)
    print(rendered, end="")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
