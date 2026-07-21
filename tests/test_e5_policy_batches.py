from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_e5_policy_batches as batch_audit


def test_checked_in_batches_account_every_blocking_source_once() -> None:
    report = batch_audit.audit(
        batch_audit.load(batch_audit.DEFAULT_DISPOSITIONS),
        batch_audit.load(batch_audit.DEFAULT_BATCHES),
        batch_audit.load(batch_audit.DEFAULT_SELECTION),
    )
    assert report["status"] == "pass"
    assert report["blocking_source_count"] == 17
    assert report["completed_source_count"] == 9
    assert report["batch_count"] == 7
    assert report["failing_checks"] == {}


def test_batch_audit_rejects_duplicate_and_missing_sources() -> None:
    dispositions = {
        "disposition_version": "test-v1",
        "explicit_dispositions": {
            "source.a": {"blocks_e5": True},
            "source.b": {"blocks_e5": True},
        },
    }
    batches = {
        "schema_version": "spacegate.e5_policy_batches.v1",
        "batch_version": "test-v1",
        "disposition_version": "test-v1",
        "batches": [
            {
                "batch_id": "batch",
                "status": "planned",
                "output_contract": "projection",
                "depends_on": [],
                "sources": ["source.a", "source.a"],
            }
        ],
    }
    selection = {"selection_sources": []}
    report = batch_audit.audit(dispositions, batches, selection)
    assert report["status"] == "fail"
    assert report["checks"]["duplicate_source_assignments"] == ["source.a"]
    assert report["checks"]["missing_blocking_sources"] == ["source.b"]


def test_batch_audit_requires_completed_sources_to_be_resolved() -> None:
    dispositions = {
        "disposition_version": "test-v1",
        "explicit_dispositions": {
            "source.active": {"blocks_e5": True},
        },
    }
    batches = {
        "schema_version": "spacegate.e5_policy_batches.v1",
        "batch_version": "test-v1",
        "disposition_version": "test-v1",
        "batches": [
            {
                "batch_id": "batch",
                "status": "in_progress",
                "output_contract": "projection",
                "depends_on": [],
                "sources": ["source.active"],
                "completed_sources": ["source.unresolved"],
            }
        ],
    }
    report = batch_audit.audit(
        dispositions,
        batches,
        {"selection_sources": []},
    )
    assert report["status"] == "fail"
    assert report["checks"]["unresolved_completed_sources"] == [
        "source.unresolved"
    ]
