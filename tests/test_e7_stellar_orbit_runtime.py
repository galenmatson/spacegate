from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_stellar_orbit_runtime",
    ROOT / "scripts/compile_e7_stellar_orbit_runtime.py",
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def test_checked_in_policy_separates_authority_by_applicability() -> None:
    policy = COMPILER.load_object(
        ROOT / "config/evidence_lake/e7_stellar_orbit_runtime.json"
    )
    COMPILER.validate_policy(policy)

    assert policy["simulation_authority"] == ["orb6", "msc_orb"]
    assert policy["source_roles"]["sb9"] == "spectroscopic_dynamics_context"
    assert policy["source_roles"]["gaia_nss"].startswith("deferred_")
    assert policy["rules"]["fieldwise_cross_source_composites"] is False
    assert policy["rules"]["source_relations_create_containment"] is False


def test_policy_rejects_context_only_runtime_edges() -> None:
    policy = COMPILER.load_object(
        ROOT / "config/evidence_lake/e7_stellar_orbit_runtime.json"
    )
    policy["rules"]["context_only_solutions_create_runtime_edges"] = True

    try:
        COMPILER.validate_policy(policy)
    except ValueError as exc:
        assert "unsafe stellar orbit runtime rules" in str(exc)
    else:
        raise AssertionError("context-only orbit promotion was accepted")


def test_compiler_has_no_named_system_or_stability_database_branch() -> None:
    source = (
        ROOT / "scripts/compile_e7_stellar_orbit_runtime.py"
    ).read_text(encoding="utf-8").lower()

    assert "served/current" not in source
    assert "stability_reference_build_id" not in source
    assert "castor" not in source
    assert "sirius" not in source
    assert "nu sco" not in source


def test_policy_json_is_canonical_object() -> None:
    value = json.loads(
        (ROOT / "config/evidence_lake/e7_stellar_orbit_runtime.json").read_text()
    )
    assert isinstance(value, dict)
    assert value["acceptance"]["selected_solutions"] == 17170
    assert value["acceptance"]["preferred_simulation_solutions"] == 1959
