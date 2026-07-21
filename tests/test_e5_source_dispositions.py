from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_e5_source_dispositions as disposition_audit  # noqa: E402


def test_checked_in_e5_dispositions_account_every_accepted_source() -> None:
    report = disposition_audit.audit(
        json.loads(disposition_audit.DEFAULT_RELEASE_SET.read_text()),
        json.loads(disposition_audit.DEFAULT_SELECTION.read_text()),
        json.loads(disposition_audit.DEFAULT_DISPOSITIONS.read_text()),
    )
    assert report["status"] == "in_progress"
    assert report["summaries"]["accepted_e4_sources"] == 38
    assert report["summaries"]["selected_sources"] == 14
    assert report["summaries"]["explicit_dispositions"] == 24
    assert report["checks"]["selection_disposition_conflicts"] == []
    assert report["checks"]["metadata_errors"] == []
    assert report["checks"]["duplicate_selection_sources"] == []
    assert report["checks"]["stale_explicit_dispositions"] == []
    assert report["checks"]["unknown_selection_sources"] == []
    assert report["checks"]["unaccounted_sources"] == []
    assert report["checks"]["invalid_dispositions"] == []
    assert report["checks"]["incomplete_dispositions"] == []
    assert len(report["checks"]["blocking_sources"]) == 15


def test_e5_disposition_audit_fails_conflicts_and_omissions() -> None:
    release_set = {
        "schema_version": "spacegate.scientific_evidence_release_set.v1",
        "release_set_id": "set1",
        "status": "pass",
        "members": [
            {
                "source_ids": ["selected", "missing"],
                "release_ids": {"selected": "r1", "missing": "r2"},
            }
        ],
    }
    selection = {
        "schema_version": "spacegate.selected_fact_policy.v1",
        "policy_version": "p1",
        "selection_sources": [
            {"source_id": "selected", "quantity_groups": [{"group_key": "g"}]}
        ],
    }
    dispositions = {
        "schema_version": "spacegate.e5_source_dispositions.v1",
        "disposition_version": "d1",
        "explicit_dispositions": {
            "selected": {
                "disposition": "evidence_only",
                "owner": "E5",
                "blocks_e5": False,
                "reason": "conflict",
            }
        },
    }
    report = disposition_audit.audit(release_set, selection, dispositions)
    assert report["status"] == "fail"
    assert report["checks"]["selection_disposition_conflicts"] == ["selected"]
    assert report["checks"]["unaccounted_sources"] == ["missing"]


def test_e5_disposition_audit_rejects_invalid_or_incomplete_rows() -> None:
    release_set = {
        "schema_version": "spacegate.scientific_evidence_release_set.v1",
        "release_set_id": "set1",
        "status": "pass",
        "members": [
            {"source_ids": ["source"], "release_ids": {"source": "r1"}}
        ],
    }
    selection = {
        "schema_version": "spacegate.selected_fact_policy.v1",
        "policy_version": "p1",
        "selection_sources": [],
    }
    dispositions = {
        "schema_version": "spacegate.e5_source_dispositions.v1",
        "disposition_version": "d1",
        "explicit_dispositions": {
            "source": {"disposition": "invented", "blocks_e5": "no"}
        },
    }
    report = disposition_audit.audit(release_set, selection, dispositions)
    assert report["status"] == "fail"
    assert report["checks"]["invalid_dispositions"] == ["source"]
    assert report["checks"]["incomplete_dispositions"] == ["source"]


def test_e5_disposition_audit_requires_relation_artifact_lineage() -> None:
    release_set = {
        "schema_version": "spacegate.scientific_evidence_release_set.v1",
        "release_set_id": "set1",
        "status": "pass",
        "members": [{"source_ids": ["source"], "release_ids": {"source": "r1"}}],
    }
    selection = {
        "schema_version": "spacegate.selected_fact_policy.v1",
        "policy_version": "p1",
        "selection_sources": [],
    }
    dispositions = {
        "schema_version": "spacegate.e5_source_dispositions.v1",
        "disposition_version": "d1",
        "explicit_dispositions": {
            "source": {
                "disposition": "selected_relation_evidence_projection",
                "owner": "E5",
                "blocks_e5": False,
                "reason": "missing required artifact lineage",
            }
        },
    }
    report = disposition_audit.audit(release_set, selection, dispositions)
    assert report["status"] == "fail"
    assert report["checks"]["incomplete_dispositions"] == ["source"]


def test_e5_disposition_audit_requires_component_artifact_lineage() -> None:
    release_set = {
        "schema_version": "spacegate.scientific_evidence_release_set.v1",
        "release_set_id": "set1",
        "status": "pass",
        "members": [{"source_ids": ["source"], "release_ids": {"source": "r1"}}],
    }
    selection = {
        "schema_version": "spacegate.selected_fact_policy.v1",
        "policy_version": "p1",
        "selection_sources": [],
    }
    dispositions = {
        "schema_version": "spacegate.e5_source_dispositions.v1",
        "disposition_version": "d1",
        "explicit_dispositions": {
            "source": {
                "disposition": "selected_component_evidence_projection",
                "owner": "E5",
                "blocks_e5": False,
                "reason": "missing required artifact lineage",
            }
        },
    }
    report = disposition_audit.audit(release_set, selection, dispositions)
    assert report["status"] == "fail"
    assert report["checks"]["incomplete_dispositions"] == ["source"]
