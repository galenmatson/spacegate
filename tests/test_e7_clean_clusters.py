from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_e7_clean_clusters as compiler  # noqa: E402


POLICY = ROOT / "config/evidence_lake/e7_clean_clusters.json"


def test_checked_in_clean_cluster_policy_is_source_general_and_fail_closed() -> None:
    policy = json.loads(POLICY.read_text())
    compiler.validate_policy(policy)
    assert policy["canonical_containment_promotion"] is False
    assert {source["member_bridge"] for source in policy["sources"]} == {
        "canonical_identifier_bindings_v1",
        "official_dr2_to_dr3_outcomes_v1",
    }
    assert all(source["acceptance"] for source in policy["sources"])


def test_clean_cluster_policy_rejects_containment_promotion() -> None:
    policy = json.loads(POLICY.read_text())
    policy["canonical_containment_promotion"] = True
    with pytest.raises(ValueError, match="containment"):
        compiler.validate_policy(policy)
