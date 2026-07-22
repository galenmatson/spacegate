from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_solar_identity", ROOT / "scripts/compile_e7_solar_identity.py"
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def test_checked_in_policy_is_identity_only_and_bounded() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_solar_identity.json")
    COMPILER.validate_policy(policy)

    assert policy["rules"]["scientific_authority"] is False
    assert policy["rules"]["name_only_core_binding"] is False
    assert policy["rules"]["source_relations_create_canonical_containment"] is False
    assert policy["acceptance"]["identities"] == 71


def test_policy_rejects_containment_promotion() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_solar_identity.json")
    policy["rules"]["source_relations_create_canonical_containment"] = True
    try:
        COMPILER.validate_policy(policy)
    except ValueError as exc:
        assert "unsafe Solar identity rules" in str(exc)
    else:
        raise AssertionError("unsafe Solar containment rule was accepted")


def test_compiler_has_no_stability_or_named_target_branches() -> None:
    source = (ROOT / "scripts/compile_e7_solar_identity.py").read_text(encoding="utf-8").lower()
    assert "served/current" not in source
    assert "20260717t0614z" not in source
    assert "voyager" not in source
    assert "jupiter" not in source
    assert "moon:sol:moon" not in source


def test_verifier_prohibits_scientific_identity_columns() -> None:
    verifier_path = ROOT / "scripts/verify_e7_solar_identity.py"
    spec = importlib.util.spec_from_file_location("verify_e7_solar_identity", verifier_path)
    assert spec and spec.loader
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)

    assert {"mass", "radius", "orbital_period_days", "semi_major_axis_au"}.issubset(
        verifier.PROHIBITED_IDENTITY_COLUMNS
    )
