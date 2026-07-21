from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_legacy_derivation_inventory as inventory_audit  # noqa: E402


def test_checked_in_legacy_derivation_inventory_accounts_source_paths() -> None:
    report = inventory_audit.audit(
        repo_root=ROOT,
        inventory_path=inventory_audit.DEFAULT_INVENTORY,
        policy_path=inventory_audit.DEFAULT_POLICY,
        build_dir=None,
    )

    assert report["status"] == "pass"
    assert report["validation"]["path_count"] == 24
    assert report["validation"]["unreferenced_policy_derivation_keys"] == []
    assert report["source_audit"]["unaccounted_versioned_markers"] == []


def test_legacy_derivation_inventory_rejects_unregistered_materialized_method(
    tmp_path: Path,
) -> None:
    build = tmp_path / "test-build"
    build.mkdir()
    con = duckdb.connect(str(build / "arm.duckdb"))
    con.execute(
        """
        CREATE TABLE derived_physical_parameters (
          derivation_method VARCHAR, review_status VARCHAR
        );
        INSERT INTO derived_physical_parameters VALUES
          ('unregistered_science_shortcut', 'candidate');
        CREATE TABLE derived_stellar_classifications (
          derivation_method VARCHAR, classification_status VARCHAR
        );
        CREATE TABLE stellar_leaf_display_classifications (
          evidence_basis VARCHAR, classification_status VARCHAR
        );
        """
    )
    con.close()
    duckdb.connect(str(build / "disc.duckdb")).close()

    report = inventory_audit.audit(
        repo_root=ROOT,
        inventory_path=inventory_audit.DEFAULT_INVENTORY,
        policy_path=inventory_audit.DEFAULT_POLICY,
        build_dir=build,
    )

    assert report["status"] == "fail"
    assert report["failing_checks"] == {"unaccounted_materialized_markers": 1}
    assert report["materialized_audit"]["unaccounted_materialized_markers"] == [
        "derived_physical_parameters.derivation_method=unregistered_science_shortcut"
    ]
