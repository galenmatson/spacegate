from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_selected_hierarchy", ROOT / "scripts/compile_e7_selected_hierarchy.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_msc_leaf_label_preserves_component_case() -> None:
    assert MODULE.leaf_label("Aa") == "Aa"
    assert MODULE.leaf_label("Ab2") == "Ab2"
    assert MODULE.leaf_label("Ba") == "Ba"
    assert MODULE.leaf_label("A") is None
    assert MODULE.leaf_label("AB") is None
    assert MODULE.leaf_label("ABC") is None
    assert MODULE.leaf_label("Aab") is None


def test_group_endpoint_cannot_support_casefold_colliding_leaf() -> None:
    assert MODULE.source_component_parts("comp:msc:release:13473+1727:Ab") == (
        "13473+1727",
        "ab",
    )
    assert MODULE.source_component_parts("comp:msc:release:13473+1727:AB") is None


def test_selected_hierarchy_policy_preserves_canonical_identity_without_names() -> None:
    policy = MODULE.load_json(MODULE.DEFAULT_POLICY)
    MODULE.validate_policy(policy)
    assert policy["rules"]["preserve_all_canonical_nodes"] is True
    assert policy["rules"]["preserve_all_canonical_edges"] is True
    assert policy["rules"]["allow_named_object_conditions"] is False
