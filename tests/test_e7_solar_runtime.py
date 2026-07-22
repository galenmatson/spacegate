from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_solar_runtime", ROOT / "scripts/compile_e7_solar_runtime.py"
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def test_policy_requires_complete_noncontainment_contract() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_solar_runtime.json")
    COMPILER.validate_policy(policy)
    assert policy["rules"]["source_relations_create_canonical_containment"] is False
    assert policy["rules"]["reference_origin_solutions_renderable"] is False
    assert policy["acceptance"]["orbital_solutions"] == 71


def test_policy_rejects_renderable_reference_origin() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_solar_runtime.json")
    policy["rules"]["reference_origin_solutions_renderable"] = True
    try:
        COMPILER.validate_policy(policy)
    except ValueError as exc:
        assert "unsafe Solar runtime rules" in str(exc)
    else:
        raise AssertionError("renderable reference-origin policy was accepted")


def test_compiler_does_not_read_stability_or_branch_on_named_targets() -> None:
    source = (ROOT / "scripts/compile_e7_solar_runtime.py").read_text(encoding="utf-8").lower()
    assert "served/current" not in source
    assert "20260717t0614z" not in source
    assert "voyager" not in source
    assert "jupiter" not in source
