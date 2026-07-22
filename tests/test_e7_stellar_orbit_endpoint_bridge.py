from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
SPEC=importlib.util.spec_from_file_location("bridge",ROOT/"scripts/compile_e7_stellar_orbit_endpoint_bridge.py")
assert SPEC and SPEC.loader
COMPILER=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(COMPILER)


def test_checked_in_bridge_policy_fails_closed()->None:
    policy=COMPILER.load_object(ROOT/"config/evidence_lake/e7_stellar_orbit_endpoint_bridge.json")
    COMPILER.validate_policy(policy)
    assert policy["rules"]["name_or_coordinate_endpoint_matching"] is False
    assert policy["rules"]["casefold_matching_requires_unique_source_and_runtime_leaf"] is True
    assert policy["rules"]["unresolved_endpoints_create_runtime_components"] is False
    assert policy["rules"]["unresolved_relations_create_runtime_edges"] is False


def test_bridge_policy_rejects_unresolved_runtime_components()->None:
    policy=COMPILER.load_object(ROOT/"config/evidence_lake/e7_stellar_orbit_endpoint_bridge.json")
    policy["rules"]["unresolved_endpoints_create_runtime_components"]=True
    try:COMPILER.validate_policy(policy)
    except ValueError as exc:assert "unsafe endpoint bridge rules" in str(exc)
    else:raise AssertionError("unsafe endpoint policy was accepted")


def test_bridge_compiler_has_no_named_object_or_stability_branch()->None:
    source=(ROOT/"scripts/compile_e7_stellar_orbit_endpoint_bridge.py").read_text(encoding="utf-8").lower()
    for forbidden in ("served/current","castor","sirius","nu sco"):
        assert forbidden not in source
