from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_selected_planet_evidence as compiler  # noqa: E402


POLICY_PATH = ROOT / "config/evidence_lake/e5_planet_evidence_policies.json"


def test_checked_in_planet_policy_is_exhaustive_and_inventory_safe() -> None:
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    compiler.validate_policy(policy)
    acceptance = policy["acceptance"]

    assert acceptance["supplemental_planet_objects"] == sum(
        acceptance[f"{source}_planets_{status}"]
        for source in ("eu", "hwc", "oec")
        for status in ("accepted", "missing", "ambiguous")
    )
    assert acceptance["tess_target_bindings"] == sum(
        acceptance[f"tess_targets_{status}"]
        for status in ("accepted", "missing", "excluded", "ambiguous")
    )
    assert acceptance["tess_candidates"] == sum(
        acceptance[key]
        for key in (
            "tess_confirmed",
            "tess_candidate_evidence",
            "tess_negative_evidence",
            "tess_unclassified_evidence",
        )
    )
    assert acceptance["canonical_inventory_mutations"] == 0
    assert policy["authority_policy"]["hwc_parameters"] == (
        "derived_comparison_never_measurement_authority"
    )


def test_planet_projection_verifier_rejects_inventory_mutation() -> None:
    with duckdb.connect() as con:
        compiler.create_schema(con)
        assert not any(compiler.verify(con).values())
        con.execute(
            "INSERT INTO planet_lifecycle_projection "
            "(evidence_id,binding_id,evidence_role,canonical_inventory_mutation) "
            "VALUES ('e','b','candidate_evidence',true)"
        )
        with pytest.raises(ValueError, match="canonical_inventory_mutations"):
            compiler.verify(con)


def test_planet_policy_rejects_candidate_inventory_authority() -> None:
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    policy["authority_policy"]["toi_candidate"] = "canonical_inventory"
    with pytest.raises(ValueError, match="candidate inventory boundary"):
        compiler.validate_policy(policy)
