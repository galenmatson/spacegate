from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import prune_e6_shadow_artifacts as retention  # noqa: E402


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, str, str, Path, Path, Path]:
    state = tmp_path / "state"
    out = state / "out"
    reports = state / "reports/evidence_lake_v2"
    candidate_id = "e6_" + "a" * 24 + "_shadow"
    replacement_id = "e6_" + "b" * 24 + "_shadow"
    candidate = out / candidate_id
    replacement = out / replacement_id
    candidate.mkdir(parents=True)
    replacement.mkdir(parents=True)
    _write_json(
        candidate / "manifest.json",
        {
            "build_id": candidate_id,
            "report": {
                "status": "pass",
                "promotion_status": "unpromoted",
                "policy_version": "old",
                "compiler_version": "old",
                "product_files": {},
            },
        },
    )
    product = replacement / "core.duckdb"
    product.write_bytes(b"verified replacement")
    declared = {"bytes": product.stat().st_size, "sha256": _sha(product)}
    _write_json(
        replacement / "manifest.json",
        {
            "build_id": replacement_id,
            "report": {
                "status": "pass",
                "promotion_status": "unpromoted",
                "product_files": {"core.duckdb": declared},
            },
        },
    )
    manifest_sha = _sha(replacement / "manifest.json")
    audit = reports / "replacement-audit.json"
    reproduction = reports / "replacement-reproduction.json"
    reference = reports / "candidate-reference.json"
    _write_json(
        audit,
        {
            "status": "pass",
            "build_id": replacement_id,
            "manifest_sha256": manifest_sha,
            "failing_checks": {},
        },
    )
    _write_json(
        reproduction,
        {
            "status": "pass",
            "build_id": replacement_id,
            "reference_manifest_sha256": manifest_sha,
            "reproduced_audit_status": "pass",
            "checks": {"logical": 0},
        },
    )
    _write_json(reference, {"build_id": candidate_id, "result": "historical"})
    old = 1_700_000_000
    for path in [candidate / "manifest.json", candidate]:
        os.utime(path, (old, old))
    return state, candidate_id, replacement_id, audit, reproduction, reference


def test_exact_superseded_candidate_report_and_pointer_guard(tmp_path: Path) -> None:
    state, candidate_id, replacement_id, audit, reproduction, reference = _fixture(
        tmp_path
    )
    output = state / "reports/evidence_lake_v2/retention.json"
    report = retention.retention_report(
        state_dir=state,
        candidate_ids=[candidate_id],
        replacement_build_id=replacement_id,
        replacement_audit=audit,
        replacement_reproduction=reproduction,
        acknowledged_reports={candidate_id: {reference.resolve()}},
        reason="replacement is independently verified",
        minimum_age_minutes=1,
        output_report=output,
    )
    assert report["status"] == "pass"
    assert report["candidate_count"] == 1
    assert report["candidate_set_sha256"]

    _write_json(
        state / "reports/evidence_lake_v2/prior-retention.json",
        {
            "schema_version": retention.CONTRACT,
            "status": "pass",
            "candidates": [{"build_id": candidate_id}],
        },
    )
    repeated = retention.retention_report(
        state_dir=state,
        candidate_ids=[candidate_id],
        replacement_build_id=replacement_id,
        replacement_audit=audit,
        replacement_reproduction=reproduction,
        acknowledged_reports={candidate_id: {reference.resolve()}},
        reason="retention audit reports are not artifact dependencies",
        minimum_age_minutes=1,
        output_report=output,
    )
    assert repeated["candidate_set_sha256"] == report["candidate_set_sha256"]

    with pytest.raises(ValueError, match="exact acknowledgement"):
        retention.retention_report(
            state_dir=state,
            candidate_ids=[candidate_id],
            replacement_build_id=replacement_id,
            replacement_audit=audit,
            replacement_reproduction=reproduction,
            acknowledged_reports={},
            reason="missing acknowledgement",
            minimum_age_minutes=1,
            output_report=output,
        )

    served = state / "served"
    served.mkdir()
    (served / "current").symlink_to(Path("../out") / candidate_id)
    with pytest.raises(ValueError, match="linked as current"):
        retention.retention_report(
            state_dir=state,
            candidate_ids=[candidate_id],
            replacement_build_id=replacement_id,
            replacement_audit=audit,
            replacement_reproduction=reproduction,
            acknowledged_reports={candidate_id: {reference.resolve()}},
            reason="pointer guard",
            minimum_age_minutes=1,
            output_report=output,
        )
