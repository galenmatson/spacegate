from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_permanent_identity_vocabulary as compiler  # noqa: E402


def test_checked_in_policy_is_identity_only() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    compiler.validate_policy(policy)
    assert policy["scientific_authority"] is False
    assert policy["rules"]["future_clean_build_reads_migration_core"] is False


def test_policy_rejects_scientific_columns() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    policy["output_columns"].append("teff_k")
    with pytest.raises(ValueError, match="prohibited scientific columns"):
        compiler.validate_policy(policy)
