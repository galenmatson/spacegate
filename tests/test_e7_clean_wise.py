from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_e7_clean_wise as compiler  # noqa: E402


def test_checked_in_clean_wise_policy_is_fail_closed() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    compiler.validate_policy(policy)
    assert policy["rules"]["open_stability_databases"] is False
    assert policy["rules"]["copy_legacy_wise_csv"] is False
    assert policy["rules"]["no_core_inventory_promotion"] is True
    assert policy["rules"]["accept_only_unique_nearest_source"] is True


def test_policy_rejects_core_promotion() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    policy["rules"]["no_core_inventory_promotion"] = False
    with pytest.raises(ValueError, match="unsafe E7 clean WISE rules"):
        compiler.validate_policy(policy)
