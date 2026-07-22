#!/usr/bin/env python3
"""Verify the pre-promotion E7 retirement ledger and Gaia DR4 adapter plan."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEGACY = ROOT / "config/evidence_lake/e7_legacy_path_inventory.json"
DEFAULT_DR4 = ROOT / "config/evidence_lake/gaia_dr4_adapter_plan.json"


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def verify(repo_root: Path, legacy_path: Path, dr4_path: Path) -> dict[str, Any]:
    legacy = load_object(legacy_path)
    dr4 = load_object(dr4_path)
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    entries = legacy.get("entries") or []
    path_ids = [str(item.get("path_id") or "") for item in entries]
    check("legacy_schema", legacy.get("schema_version") == "spacegate.e7_legacy_path_inventory.v1", legacy.get("schema_version"))
    check("legacy_pre_promotion", legacy.get("pre_promotion") is True, legacy.get("pre_promotion"))
    check("legacy_entries_present", len(entries) >= 7, len(entries))
    check("legacy_path_ids_unique", len(path_ids) == len(set(path_ids)) and all(path_ids), path_ids)
    check("legacy_no_premature_retirement", all(item.get("current_state") != "retired" for item in entries), [item.get("current_state") for item in entries])
    missing_paths = sorted(
        path
        for item in entries
        for path in (
            (item.get("paths") or [])
            + (item.get("verified_replacement_paths") or [])
        )
        if not (repo_root / str(path)).is_file()
    )
    check("legacy_paths_exist", not missing_paths, missing_paths)
    incomplete = sorted(
        item.get("path_id")
        for item in entries
        if not item.get("category")
        or not item.get("current_state")
        or not item.get("target_state")
        or not item.get("action")
        or not item.get("retirement_gates")
        or not item.get("retention")
    )
    check("legacy_entries_complete", not incomplete, incomplete)
    permanent = [item for item in entries if item.get("category") == "permanent_identity"]
    check(
        "permanent_identity_retained",
        len(permanent) == 1 and str(permanent[0].get("target_state")).startswith("retained_"),
        [item.get("path_id") for item in permanent],
    )
    rules = legacy.get("rules") or {}
    check("raw_typed_never_retired", rules.get("retire_raw_or_typed_source_evidence") is False, rules)
    check("rollback_required", rules.get("require_atomic_promotion_and_rollback") is True, rules)

    identity = dr4.get("identity_contract") or {}
    namespaces = identity.get("source_namespaces") or []
    phases = dr4.get("phases") or []
    phase_ids = [str(item.get("phase") or "") for item in phases]
    check("dr4_schema", dr4.get("schema_version") == "spacegate.gaia_release_adapter_plan.v1", dr4.get("schema_version"))
    check("dr4_status_planned", (dr4.get("release") or {}).get("status") == "planned_not_acquired", dr4.get("release"))
    check("dr4_permanent_ids_independent", identity.get("permanent_spacegate_ids_independent_of_gaia") is True, identity)
    check("dr3_dr4_not_interchangeable", identity.get("gaia_dr3_and_dr4_ids_interchangeable") is False, identity)
    check("dr3_dr4_namespaces_distinct", namespaces == ["gaia_dr3_source_id", "gaia_dr4_source_id"], namespaces)
    check("official_transition_edge_required", str(identity.get("required_transition_edge") or "").startswith("official_gaia_dr3_to_dr4"), identity.get("required_transition_edge"))
    check("dr4_phases_complete", phase_ids == [
        "D0_registry_preflight",
        "D1_bounded_acquisition",
        "D2_source_native_typed",
        "D3_identity_transition",
        "D4_evidence_and_selection",
        "D5_shadow_ab_and_cutover",
    ], phase_ids)
    check("dr4_phase_requirements", all(len(item.get("requirements") or []) >= 3 for item in phases), phase_ids)
    forbidden = dr4.get("forbidden_shortcuts") or []
    check("dr4_forbidden_shortcuts", len(forbidden) >= 7, forbidden)

    failing = [item for item in checks if not item["passed"]]
    return {
        "schema_version": "spacegate.e7_cutover_plan_verification.v1",
        "status": "pass" if not failing else "fail",
        "legacy_inventory_hash": stable_hash(legacy),
        "gaia_dr4_plan_hash": stable_hash(dr4),
        "checks": checks,
        "failing_checks": failing,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--legacy", type=Path, default=DEFAULT_LEGACY)
    parser.add_argument("--dr4", type=Path, default=DEFAULT_DR4)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.repo_root.resolve(), args.legacy.resolve(), args.dr4.resolve())
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
