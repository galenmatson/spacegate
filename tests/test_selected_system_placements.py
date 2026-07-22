from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_selected_system_placements as compiler  # noqa: E402
import verify_selected_system_placement_reproduction as reproduction  # noqa: E402


def test_checked_in_policy_is_complete_and_safe() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    compiler.validate_policy(policy)
    assert sum(policy["expected_winner_counts"].values()) == 5_869_091
    assert policy["name_anchor"]["promote_physical_identity"] is False
    assert policy["coordinate_contract"]["preserve_source_epoch"] is True


def test_policy_rejects_nonunique_name_anchor() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    policy["name_anchor"]["require_unique_source_name"] = False
    with pytest.raises(ValueError, match="unsafe system-context name anchor"):
        compiler.validate_policy(policy)


def test_policy_rejects_duplicate_precedence_rank() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    policy["placement_precedence"][1]["rank"] = 10
    with pytest.raises(ValueError, match="precedence must be unique"):
        compiler.validate_policy(policy)


def test_reproduction_comparison_ignores_timestamps_and_timings() -> None:
    products = {"selected_system_placements": {"sha256": "placement"}}
    reference = {
        "build_id": "build",
        "policy_sha256": "policy",
        "compiler_sha256": "compiler",
        "input_sha256": {"input": "hash"},
        "input_attestation": {"registered_product_checks": {"ra": True}},
        "products": products,
        "winner_counts": {"selected_star": 2},
        "verification": {"duplicate_systems": 0},
        "created_at": "first",
        "timings": {"compile": {"wall_seconds": 1}},
    }
    reproduced = dict(
        reference,
        created_at="second",
        timings={"compile": {"wall_seconds": 2}},
    )

    report = reproduction.compare(reference, reproduced, {"status": "pass"})

    assert report["status"] == "pass"
    assert report["failing_checks"] == []


def test_sbx_lineage_uses_selected_component_release_and_source_epoch() -> None:
    source = Path(compiler.__file__).read_text(encoding="utf-8")
    assert "sbx_v2026_07_21" not in source
    assert "b.source_id,b.release_id" in source
    assert "'J'||position_epoch_raw coordinate_epoch" in source
