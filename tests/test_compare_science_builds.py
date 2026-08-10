from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "compare_science_builds", ROOT / "scripts" / "compare_science_builds.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_accepted_supplements_disabled_supports_clean_runtime_contract() -> None:
    assert MODULE.accepted_supplements_disabled(
        {
            "build_kind": "e7_clean_runtime_core",
            "stability_database_opened": "0",
        },
        accepted_supplement_count=0,
    )
    assert not MODULE.accepted_supplements_disabled(
        {
            "build_kind": "e7_clean_runtime_core",
            "stability_database_opened": "0",
        },
        accepted_supplement_count=1,
    )


def test_accepted_supplements_disabled_preserves_legacy_explicit_gate() -> None:
    assert MODULE.accepted_supplements_disabled(
        {"accepted_supplements_enabled": "0"},
        accepted_supplement_count=0,
    )
    assert not MODULE.accepted_supplements_disabled(
        {"accepted_supplements_enabled": "1"},
        accepted_supplement_count=0,
    )
