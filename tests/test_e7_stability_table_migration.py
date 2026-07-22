from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_e7_stability_table_migration as migration  # noqa: E402


def test_checked_in_stability_table_contract_is_complete() -> None:
    contract = migration.load_object(migration.DEFAULT_CONTRACT)
    assert migration.validate_contract(contract) == []
    assert contract["rules"]["stability_scientific_values_may_enter_clean_build"] is False
    assert contract["rules"]["clean_compiler_must_not_open_stability_databases"] is True


def test_current_stability_tables_are_owned_once() -> None:
    report = migration.audit(migration.DEFAULT_CONTRACT, migration.DEFAULT_STATE)
    assert report["status"] == "pass"
    assert report["failing_checks"] == {}
    assert report["completion_status"] == "incomplete"
    assert report["open_replacement_count"] > 0
