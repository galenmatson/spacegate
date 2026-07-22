from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_evidence_lake_completion as completion  # noqa: E402


def test_checked_in_completion_contract_reports_verified_but_incomplete() -> None:
    report = completion.audit(completion.DEFAULT_CONTRACT, completion.DEFAULT_STATE)

    assert report["verified_checkpoint_status"] == "pass"
    assert report["completion_status"] == "incomplete"
    assert report["failing_checks"] == []
    assert report["check_count"] == 38
    assert report["open_gate_count"] == 6


def test_missing_required_report_fails_checkpoint(tmp_path: Path) -> None:
    contract = {
        "schema_version": "spacegate.evidence_lake_acceptance_contract.v1",
        "contract_version": "test",
        "report_checks": [
            {"stage": "E0", "path": "missing.json", "expect": {"status": "pass"}}
        ],
        "required_artifacts": [],
        "open_gates": [],
    }
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    report = completion.audit(contract_path, tmp_path / "state")

    assert report["verified_checkpoint_status"] == "fail"
    assert report["completion_status"] == "fail"
    assert report["failing_checks"][0]["path"] == "missing.json"


def test_no_open_gates_allows_complete_status(tmp_path: Path) -> None:
    reports = tmp_path / "state/reports/evidence_lake_v2"
    reports.mkdir(parents=True)
    (reports / "pass.json").write_text(json.dumps({"status": "pass"}), encoding="utf-8")
    contract = {
        "schema_version": "spacegate.evidence_lake_acceptance_contract.v1",
        "contract_version": "test",
        "report_checks": [
            {"stage": "E7", "path": "pass.json", "expect": {"status": "pass"}}
        ],
        "required_artifacts": [],
        "open_gates": [],
    }
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    report = completion.audit(contract_path, tmp_path / "state")

    assert report["verified_checkpoint_status"] == "pass"
    assert report["completion_status"] == "complete"
