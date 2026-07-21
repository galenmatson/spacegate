from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_e4_source_scope as scope_audit  # noqa: E402


def test_checked_in_e4_source_scope_accounts_every_registered_source() -> None:
    report = scope_audit.audit(
        json.loads(scope_audit.DEFAULT_REGISTRY.read_text()),
        json.loads(scope_audit.DEFAULT_CONTRACT.read_text()),
        json.loads(scope_audit.DEFAULT_SCOPE.read_text()),
    )
    assert report["status"] == "in_progress"
    assert report["summaries"]["registered_sources"] == 44
    assert report["summaries"]["e4_adapters"] == 34
    assert report["summaries"]["explicit_boundary_dispositions"] == 10
    assert report["checks"]["unaccounted_sources"] == []
    assert report["checks"]["adapter_disposition_conflicts"] == []
    assert report["checks"]["stale_explicit_dispositions"] == []
    assert report["checks"]["unregistered_adapters"] == []
    assert report["checks"]["blocking_sources"] == [
        "gaia.dr3.gaia_source",
    ]


def test_e4_source_scope_fails_unaccounted_and_conflicting_sources() -> None:
    registry = {
        "registry_version": "r1",
        "sources": [
            {"source_id": "adapted", "release_id": "a1", "state": "active"},
            {"source_id": "missing", "release_id": "m1", "state": "active"},
        ],
    }
    contract = {
        "contract_version": "c1",
        "source_adapters": {"adapted": {}},
    }
    scope = {
        "scope_version": "s1",
        "explicit_dispositions": {
            "adapted": {
                "disposition": "identity_graph_only",
                "owner": "E2",
                "blocks_e4": False,
                "reason": "conflict",
            }
        },
    }
    report = scope_audit.audit(registry, contract, scope)
    assert report["status"] == "fail"
    assert report["checks"]["adapter_disposition_conflicts"] == ["adapted"]
    assert report["checks"]["unaccounted_sources"] == ["missing"]
