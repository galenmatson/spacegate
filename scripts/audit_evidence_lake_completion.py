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


def nested_value(value: Any, field: str) -> Any:
    current = value
    for part in field.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def artifact_path(
    requirement: Any, *, state_dir: Path, artifact_roots: dict[str, Path]
) -> tuple[str, Path]:
    if isinstance(requirement, str):
        relative = requirement
        root_name = "state"
    elif isinstance(requirement, dict):
        relative = str(requirement.get("path") or "")
        root_name = str(requirement.get("root") or "state")
    else:
        raise ValueError(f"invalid artifact requirement: {requirement!r}")
    relative_path = Path(relative)
    if not relative or relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"artifact path must be relative and bounded: {relative!r}")
    if root_name == "state":
        root = state_dir
    else:
        try:
            root = artifact_roots[root_name]
        except KeyError as exc:
            raise ValueError(f"unknown artifact root: {root_name}") from exc
    return f"{root_name}:{relative}", root / relative_path


def audit(contract_path: Path, state_dir: Path) -> dict[str, Any]:
    contract = load_object(contract_path)
    report_roots = [state_dir / "reports/evidence_lake_v2"] + [
        Path(str(path)).resolve() for path in (contract.get("additional_report_roots") or [])
    ]
    artifact_roots = {
        str(name): Path(str(path)).resolve()
        for name, path in (contract.get("artifact_roots") or {}).items()
    }
    checks: list[dict[str, Any]] = []

    for requirement in contract.get("report_checks") or []:
        relative = str(requirement.get("path") or "")
        relative_path = Path(relative)
        if not relative or relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError(f"report path must be relative and bounded: {relative!r}")
        present_paths = [root / relative_path for root in report_roots if (root / relative_path).is_file()]
        report_path = present_paths[0] if len(present_paths) == 1 else report_roots[0] / relative_path
        failures: list[dict[str, Any]] = []
        if len(present_paths) > 1:
            failures.append({
                "field": "$file",
                "expected": "present in exactly one registered report root",
                "actual": [str(path) for path in present_paths],
            })
        elif not present_paths:
            failures.append({"field": "$file", "expected": "present", "actual": "missing"})
        else:
            report = load_object(report_path)
            expected_sha256 = requirement.get("sha256")
            if expected_sha256:
                actual_sha256 = hashlib.sha256(report_path.read_bytes()).hexdigest()
                if actual_sha256 != expected_sha256:
                    failures.append({
                        "field": "$sha256",
                        "expected": expected_sha256,
                        "actual": actual_sha256,
                    })
            for field, expected in (requirement.get("expect") or {}).items():
                actual = nested_value(report, field)
                if actual != expected:
                    failures.append({"field": field, "expected": expected, "actual": actual})
        checks.append({
            "kind": "report",
            "stage": requirement.get("stage"),
            "path": relative,
            "resolved_path": str(report_path),
            "passed": not failures,
            "failures": failures,
        })

    for requirement in contract.get("required_artifacts") or []:
        label, required_path = artifact_path(
            requirement, state_dir=state_dir, artifact_roots=artifact_roots
        )
        checks.append({
            "kind": "artifact",
            "path": label,
            "resolved_path": str(required_path),
            "passed": required_path.is_file(),
            "failures": [] if required_path.is_file() else [{
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
