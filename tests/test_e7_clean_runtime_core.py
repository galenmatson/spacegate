from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_clean_runtime_core", ROOT / "scripts/compile_e7_clean_runtime_core.py"
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def test_checked_in_policy_is_fail_closed() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_clean_runtime_core.json")
    COMPILER.validate_policy(policy)
    assert policy["rules"]["open_stability_databases"] is False
    assert policy["rules"]["cluster_membership_creates_containment"] is False
    assert set(policy["inputs"]) == {
        "clean_foundation", "clean_science", "clean_clusters", "clean_extended_objects"
    }


def test_policy_rejects_stability_and_unbounded_paths() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_clean_runtime_core.json")
    policy["rules"]["open_stability_databases"] = True
    with pytest.raises(ValueError, match="unsafe"):
        COMPILER.validate_policy(policy)
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_clean_runtime_core.json")
    policy["inputs"]["clean_science"]["relative_path"] = "../stability"
    with pytest.raises(ValueError, match="unbounded"):
        COMPILER.validate_policy(policy)


def test_runtime_core_exports_required_public_tables() -> None:
    assert {"systems", "stars", "planets", "compact_objects", "eclipsing_binaries"}.issubset(
        COMPILER.EXPORT_ORDER
    )
    assert {"open_clusters", "open_cluster_memberships", "extended_objects"}.issubset(
        COMPILER.EXPORT_ORDER
    )


def test_scientific_rows_do_not_use_compile_wall_clock() -> None:
    source = (ROOT / "scripts/compile_e7_clean_runtime_core.py").read_text(encoding="utf-8")
    assert "sql_literal(utc_now())" not in source
    assert "stability_reference" not in source


def test_runtime_core_names_checkpoint_and_hashing_phases() -> None:
    source = (ROOT / "scripts/compile_e7_clean_runtime_core.py").read_text(encoding="utf-8")
    assert 'timing.run("core_checkpoint"' in source
    assert '"hierarchy_metadata_checkpoint"' in source
    assert '"database_hashing"' in source
