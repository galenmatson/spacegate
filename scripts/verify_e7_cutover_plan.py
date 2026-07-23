#!/usr/bin/env python3
"""Verify the phased E7 retirement ledger and Gaia DR4 adapter plan."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEGACY = ROOT / "config/evidence_lake/e7_legacy_path_inventory.json"
DEFAULT_DR4 = ROOT / "config/evidence_lake/gaia_dr4_adapter_plan.json"
DEFAULT_RECOVERY = ROOT / "docs/INGEST_RECOVERY.md"


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def verify(
    repo_root: Path,
    legacy_path: Path,
    dr4_path: Path,
    recovery_path: Path | None = None,
) -> dict[str, Any]:
    legacy = load_object(legacy_path)
    dr4 = load_object(dr4_path)
    recovery_path = recovery_path or repo_root / "docs/INGEST_RECOVERY.md"
    recovery_text = recovery_path.read_text(encoding="utf-8")
    recovery_lower = recovery_text.lower()
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    entries = legacy.get("entries") or []
    path_ids = [str(item.get("path_id") or "") for item in entries]
    check("legacy_schema", legacy.get("schema_version") == "spacegate.e7_legacy_path_inventory.v1", legacy.get("schema_version"))
    pre_promotion = legacy.get("pre_promotion")
    check("legacy_phase_declared", isinstance(pre_promotion, bool), pre_promotion)
    check("legacy_entries_present", len(entries) >= 7, len(entries))
    check("legacy_path_ids_unique", len(path_ids) == len(set(path_ids)) and all(path_ids), path_ids)
    states = [str(item.get("current_state") or "") for item in entries]
    if pre_promotion:
        lifecycle_ok = all(state != "retired" for state in states)
    else:
        lifecycle_ok = (
            legacy.get("authoritative_build_path") == "evidence_lake_v2"
            and legacy.get("formal_deprecation_complete") is True
            and all(
                not state.startswith("transitional_")
                and not state.endswith("_pending_cutover")
                for state in states
            )
        )
    check("legacy_lifecycle_state", lifecycle_ok, states)
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
    cutover = legacy.get("cutover_evidence") or {}
    cutover_ok = (
        pre_promotion is True
        or (
            cutover.get("status") == "pass"
            and cutover.get("final_served_build_id") == legacy.get("candidate_build_id")
            and cutover.get("rollback_build_id") == legacy.get("stability_build_id")
            and len(str(cutover.get("report_sha256") or "")) == 64
            and len(str(cutover.get("operator_acceptance_sha256") or "")) == 64
            and cutover.get("artifacts_deleted") is False
            and cutover.get("antiproton_deployed") is False
        )
    )
    check("cutover_evidence", cutover_ok, cutover)
    recovery_authority_markers = [
        "not the authoritative scientific refresh path",
        "legacy differential refresh entry point",
        "scripts/run_e7_timed_pipeline.py",
    ]
    missing_recovery_markers = [
        marker for marker in recovery_authority_markers if marker not in recovery_lower
    ]
    check(
        "recovery_authority_separated",
        not missing_recovery_markers
        and "use scripts/refresh_core.sh for normal refresh operations" not in recovery_lower,
        {
            "path": str(recovery_path),
            "missing_markers": missing_recovery_markers,
        },
    )
    recovery_safety_markers = [
        "--mode verify",
        "does not promote",
        "promotion remains a separate operator-approved action",
    ]
    missing_safety_markers = [
        marker for marker in recovery_safety_markers if marker not in recovery_lower
    ]
    check(
        "recovery_verification_cannot_imply_promotion",
        not missing_safety_markers,
        {
            "path": str(recovery_path),
            "missing_markers": missing_safety_markers,
        },
    )

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
        "recovery_document_sha256": hashlib.sha256(
            recovery_text.encode("utf-8")
        ).hexdigest(),
        "checks": checks,
        "failing_checks": failing,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--legacy", type=Path, default=DEFAULT_LEGACY)
    parser.add_argument("--dr4", type=Path, default=DEFAULT_DR4)
    parser.add_argument("--recovery", type=Path, default=DEFAULT_RECOVERY)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(
        args.repo_root.resolve(),
        args.legacy.resolve(),
        args.dr4.resolve(),
        args.recovery.resolve(),
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if report["status"] == "pass" else 1)


if __name__ == "__main__":
    main()
