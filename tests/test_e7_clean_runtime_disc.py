from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_clean_runtime_disc", ROOT / "scripts/compile_e7_clean_runtime_disc.py"
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def policy() -> dict:
    return json.loads(
        (ROOT / "config/evidence_lake/e7_clean_runtime_disc.json").read_text(encoding="utf-8")
    )


def test_checked_in_policy_is_strict_and_pinned() -> None:
    value = policy()
    COMPILER.validate_policy(value)
    assert value["rules"]["open_stability_databases"] is False
    assert value["rules"]["require_selected_stellar_surfaces"] is True
    assert value["rules"]["allow_core_classification_fallback"] is False
    assert value["rules"]["luminosity_proxy_is_presentation_assumption"] is True


def test_policy_rejects_core_classification_fallback() -> None:
    value = policy()
    value["rules"]["allow_core_classification_fallback"] = True
    try:
        COMPILER.validate_policy(value)
    except ValueError as exc:
        assert "unsafe clean runtime DISC rules" in str(exc)
    else:
        raise AssertionError("unsafe CORE classification fallback was accepted")


def test_policy_rejects_missing_weight_and_path_traversal() -> None:
    value = policy()
    del value["coolness_profile"]["weights"]["exotic_star"]
    try:
        COMPILER.validate_policy(value)
    except ValueError as exc:
        assert "weight set is incomplete" in str(exc)
    else:
        raise AssertionError("incomplete coolness weights were accepted")

    value = policy()
    value["inputs"]["clean_runtime_core"]["relative_path"] = (
        "../../../tmp/92da8d31dc0e7dbd4d4d70a5"
    )
    try:
        COMPILER.validate_policy(value)
    except ValueError as exc:
        assert "invalid bounded input path" in str(exc)
    else:
        raise AssertionError("input path traversal was accepted")


def test_compiler_invokes_strict_scoring_mode() -> None:
    source = (ROOT / "scripts/compile_e7_clean_runtime_disc.py").read_text(encoding="utf-8")
    assert "require_selected_surfaces=True" in source
    assert "allow_core_classification_fallback=False" in source
    assert "resolve_input(state," in source
