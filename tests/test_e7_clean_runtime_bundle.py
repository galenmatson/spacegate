from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_clean_runtime_bundle",
    ROOT / "scripts/compile_e7_clean_runtime_bundle.py",
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def policy() -> dict:
    return json.loads(
        (ROOT / "config/evidence_lake/e7_clean_runtime_bundle.json").read_text(encoding="utf-8")
    )


def test_bundle_policy_is_pinned_and_not_directly_served() -> None:
    value = policy()
    COMPILER.validate_policy(value)
    assert value["rules"]["open_stability_databases"] is False
    assert value["rules"]["mutate_component_artifacts"] is False
    assert value["rules"]["bundle_is_served_directly"] is False


def test_bundle_policy_rejects_path_traversal_and_missing_products() -> None:
    value = policy()
    value["inputs"]["arm"]["relative_path"] = "../../tmp/e3e82312eaa3cab931e9e756"
    try:
        COMPILER.validate_policy(value)
    except ValueError as exc:
        assert "invalid bounded input path" in str(exc)
    else:
        raise AssertionError("bundle accepted path traversal")

    value = policy()
    value["inputs"]["disc"]["products"] = []
    try:
        COMPILER.validate_policy(value)
    except ValueError as exc:
        assert "invalid product contract" in str(exc)
    else:
        raise AssertionError("bundle accepted an incomplete product set")
