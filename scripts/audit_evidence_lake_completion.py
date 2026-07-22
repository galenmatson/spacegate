#!/usr/bin/env python3
"""Audit Evidence Lake E0-E7 checkpoint evidence without overstating cutover."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT = ROOT / "config/evidence_lake/e0_e7_acceptance.json"
DEFAULT_STATE = Path("/data/spacegate/state")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def audit(contract_path: Path, state_dir: Path) -> dict[str, Any]:
    contract = load_object(contract_path)
    reports_dir = state_dir / "reports/evidence_lake_v2"
    checks: list[dict[str, Any]] = []

    for requirement in contract.get("report_checks") or []:
        relative = str(requirement.get("path") or "")
        report_path = reports_dir / relative
        failures: list[dict[str, Any]] = []
        if not report_path.is_file():
            failures.append({"field": "$file", "expected": "present", "actual": "missing"})
        else:
            report = load_object(report_path)
            for field, expected in (requirement.get("expect") or {}).items():
                actual = report.get(field)
                if actual != expected:
                    failures.append({"field": field, "expected": expected, "actual": actual})
        checks.append({
            "kind": "report",
            "stage": requirement.get("stage"),
            "path": relative,
            "passed": not failures,
            "failures": failures,
        })

    for relative in contract.get("required_artifacts") or []:
        artifact_path = state_dir / str(relative)
        checks.append({
            "kind": "artifact",
            "path": str(relative),
            "passed": artifact_path.is_file(),
            "failures": [] if artifact_path.is_file() else [{
                "field": "$file", "expected": "present", "actual": "missing"
            }],
        })

    failing = [item for item in checks if not item["passed"]]
    open_gates = contract.get("open_gates") or []
    checkpoint_status = "pass" if not failing else "fail"
    completion_status = "complete" if not failing and not open_gates else (
        "incomplete" if not failing else "fail"
    )
    return {
        "schema_version": "spacegate.evidence_lake_completion_audit.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "contract_version": contract.get("contract_version"),
        "contract_sha256": stable_hash(contract),
        "candidate_build_id": contract.get("candidate_build_id"),
        "shadow_build_id": contract.get("shadow_build_id"),
        "verified_checkpoint_status": checkpoint_status,
        "completion_status": completion_status,
        "check_count": len(checks),
        "failing_checks": failing,
        "open_gate_count": len(open_gates),
        "open_gates": open_gates,
        "checks": checks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = audit(args.contract.resolve(), args.state_dir.resolve())
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(1 if report["verified_checkpoint_status"] == "fail" else 0)


if __name__ == "__main__":
    main()
