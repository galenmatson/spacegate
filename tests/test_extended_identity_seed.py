from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_extended_identity_seed as compiler  # noqa: E402


def policy() -> dict:
    return json.loads(compiler.DEFAULT_POLICY.read_text(encoding="utf-8"))


def test_checked_in_policy_is_identity_only() -> None:
    value = policy()
    compiler.validate_policy(value)
    columns = {
        column
        for spec in value["tables"].values()
        for column in spec["columns"]
    }
    assert value["scientific_authority"] is False
    assert not columns.intersection(value["prohibited_scientific_columns"])


def test_policy_rejects_scientific_geometry() -> None:
    value = copy.deepcopy(policy())
    value["tables"]["extended_identity_nodes"]["columns"].append("ra_deg")
    with pytest.raises(ValueError, match="scientific columns"):
        compiler.validate_policy(value)


def test_policy_rejects_future_migration_core_reads() -> None:
    value = copy.deepcopy(policy())
    value["rules"]["future_clean_build_reads_migration_core"] = True
    with pytest.raises(ValueError, match="unsafe extended identity"):
        compiler.validate_policy(value)
