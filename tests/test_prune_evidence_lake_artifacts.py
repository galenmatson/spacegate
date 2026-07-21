from __future__ import annotations

import os
import hashlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prune_evidence_lake_artifacts as retention  # noqa: E402


def temporary_artifact(root: Path, name: str = ".0123456789abcdef01234567.test") -> Path:
    artifact = root / name
    artifact.mkdir(parents=True)
    (artifact / "scientific_evidence.duckdb").write_bytes(b"diagnostic")
    return artifact


def failed_artifact(root: Path, tmp_path: Path) -> tuple[Path, Path]:
    artifact = root / "0123456789abcdef01234567"
    artifact.mkdir(parents=True)
    database = artifact / "scientific_evidence.duckdb"
    database.write_bytes(b"failed diagnostic")
    database_sha256 = hashlib.sha256(database.read_bytes()).hexdigest()
    (artifact / "manifest.json").write_text(
        json.dumps(
            {
                "build_id": artifact.name,
                "database": database.name,
                "database_sha256": database_sha256,
            }
        )
    )
    audit = tmp_path / "audit.json"
    audit.write_text(
        json.dumps(
            {
                "schema_version": "spacegate.scientific_evidence_artifact_audit.v1",
                "status": "fail",
                "build_id": artifact.name,
                "database": str(database),
                "checks": {"blank_identifier_claims": 2, "other": 0},
            }
        )
    )
    return artifact, audit


def test_retention_dry_run_and_hash_gated_whole_artifact_apply(tmp_path: Path) -> None:
    root = tmp_path / "scientific_evidence"
    artifact = temporary_artifact(root)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    report = retention.retention_report(
        root,
        [artifact.name],
        reason="confirmed interrupted diagnostic",
        minimum_age_minutes=0,
        proc_root=proc_root,
    )
    assert report["status"] == "pass"
    assert report["candidate_count"] == 1
    assert report["reclaimable_bytes"] > 0
    assert report["candidates"][0]["name"] == artifact.name
    assert len(report["candidate_set_sha256"]) == 64

    report_again = retention.retention_report(
        root,
        [artifact.name],
        reason="confirmed interrupted diagnostic",
        minimum_age_minutes=0,
        proc_root=proc_root,
    )
    assert report_again["candidate_set_sha256"] == report["candidate_set_sha256"]
    try:
        retention.apply_retention(report_again, "wrong")
    except ValueError as error:
        assert "exact current" in str(error)
    else:
        raise AssertionError("retention apply accepted a stale candidate hash")
    applied = retention.apply_retention(
        report_again,
        report_again["candidate_set_sha256"],
    )
    assert applied["action"] == "applied"
    assert not artifact.exists()


def test_retention_rejects_manifest_symlink_and_shared_file(tmp_path: Path) -> None:
    root = tmp_path / "scientific_evidence"
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    manifested = temporary_artifact(root, ".0123456789abcdef01234567.manifested")
    (manifested / "manifest.json").write_text("{}")
    try:
        retention.inspect_candidate(
            root,
            manifested.name,
            minimum_age_minutes=0,
            proc_root=proc_root,
        )
    except ValueError as error:
        assert "manifest-bearing" in str(error)
    else:
        raise AssertionError("manifest-bearing artifact was accepted")

    linked = temporary_artifact(root, ".0123456789abcdef01234567.linked")
    (linked / "link").symlink_to(linked / "scientific_evidence.duckdb")
    try:
        retention.inspect_candidate(
            root,
            linked.name,
            minimum_age_minutes=0,
            proc_root=proc_root,
        )
    except ValueError as error:
        assert "symlinks" in str(error)
    else:
        raise AssertionError("symlink-bearing artifact was accepted")

    shared = temporary_artifact(root, ".0123456789abcdef01234567.shared")
    os.link(shared / "scientific_evidence.duckdb", shared / "shared.duckdb")
    try:
        retention.inspect_candidate(
            root,
            shared.name,
            minimum_age_minutes=0,
            proc_root=proc_root,
        )
    except ValueError as error:
        assert "shared files" in str(error)
    else:
        raise AssertionError("hardlinked artifact was accepted")


def test_retention_rejects_live_artifact_and_detects_tree_change(tmp_path: Path) -> None:
    root = tmp_path / "scientific_evidence"
    artifact = temporary_artifact(root)
    proc_root = tmp_path / "proc"
    descriptors = proc_root / "123" / "fd"
    descriptors.mkdir(parents=True)
    active_descriptor = descriptors / "3"
    active_descriptor.symlink_to(artifact / "scientific_evidence.duckdb")
    try:
        retention.inspect_candidate(
            root,
            artifact.name,
            minimum_age_minutes=0,
            proc_root=proc_root,
        )
    except ValueError as error:
        assert "open by live processes" in str(error)
    else:
        raise AssertionError("live compiler artifact was accepted")

    active_descriptor.unlink()
    before = retention.retention_report(
        root,
        [artifact.name],
        reason="test tree identity",
        minimum_age_minutes=0,
        proc_root=proc_root,
    )
    (artifact / "scientific_evidence.duckdb").write_bytes(b"changed diagnostic")
    after = retention.retention_report(
        root,
        [artifact.name],
        reason="test tree identity",
        minimum_age_minutes=0,
        proc_root=proc_root,
    )
    assert before["candidate_set_sha256"] != after["candidate_set_sha256"]


def test_retention_accepts_only_independently_audit_failed_immutable_build(
    tmp_path: Path,
) -> None:
    root = tmp_path / "scientific_evidence"
    artifact, audit = failed_artifact(root, tmp_path)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    report = retention.retention_report(
        root,
        [],
        failed_audits={artifact.name: audit},
        reason="independent audit failed immutable diagnostic",
        minimum_age_minutes=0,
        proc_root=proc_root,
    )
    candidate = report["candidates"][0]
    assert candidate["artifact_state"] == "immutable_independently_audit_failed"
    assert candidate["failed_checks"] == {"blank_identifier_claims": 2}
    assert candidate["database_sha256"] == hashlib.sha256(
        (artifact / "scientific_evidence.duckdb").read_bytes()
    ).hexdigest()
    applied = retention.apply_retention(report, report["candidate_set_sha256"])
    assert applied["action"] == "applied"
    assert not artifact.exists()


def test_retention_rejects_failed_artifact_referenced_by_release_set(
    tmp_path: Path,
) -> None:
    root = tmp_path / "derived/evidence_lake_v2/scientific_evidence"
    root.mkdir(parents=True)
    artifact, audit = failed_artifact(root, tmp_path)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    release_set = (
        root.parent
        / "scientific_evidence_sets"
        / ("a" * 24)
        / "manifest.json"
    )
    release_set.parent.mkdir(parents=True)
    release_set.write_text(
        json.dumps({"members": [{"build_id": artifact.name}]})
    )

    try:
        retention.inspect_failed_artifact(
            root,
            artifact.name,
            audit,
            minimum_age_minutes=0,
            proc_root=proc_root,
        )
    except ValueError as error:
        assert "referenced by an E4 release set" in str(error)
    else:
        raise AssertionError("release-set member was accepted for retention")


def test_failed_immutable_retention_rejects_passing_or_mismatched_audit(
    tmp_path: Path,
) -> None:
    root = tmp_path / "scientific_evidence"
    artifact, audit = failed_artifact(root, tmp_path)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    payload = json.loads(audit.read_text())
    payload["status"] = "pass"
    audit.write_text(json.dumps(payload))
    try:
        retention.inspect_failed_artifact(
            root,
            artifact.name,
            audit,
            minimum_age_minutes=0,
            proc_root=proc_root,
        )
    except ValueError as error:
        assert "does not fail" in str(error)
    else:
        raise AssertionError("passing audit authorized immutable retention")

    payload["status"] = "fail"
    payload["database"] = str(tmp_path / "other.duckdb")
    audit.write_text(json.dumps(payload))
    try:
        retention.inspect_failed_artifact(
            root,
            artifact.name,
            audit,
            minimum_age_minutes=0,
            proc_root=proc_root,
        )
    except ValueError as error:
        assert "database does not match" in str(error)
    else:
        raise AssertionError("mismatched audit database authorized retention")


def test_failed_immutable_retention_accepts_allowlisted_source_audit(
    tmp_path: Path,
) -> None:
    root = tmp_path / "scientific_evidence"
    artifact, audit = failed_artifact(root, tmp_path)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    payload = json.loads(audit.read_text())
    payload["schema_version"] = "spacegate.gcvs_scientific_evidence_audit.v1"
    payload["checks"] = {"invalid_normalized_coordinates": 1020}
    audit.write_text(json.dumps(payload))

    inspected = retention.inspect_failed_artifact(
        root,
        artifact.name,
        audit,
        minimum_age_minutes=0,
        proc_root=proc_root,
    )
    assert inspected["audit_schema_version"] == payload["schema_version"]
    assert inspected["failed_checks"] == {"invalid_normalized_coordinates": 1020}

    payload["schema_version"] = "spacegate.unreviewed_source_audit.v1"
    audit.write_text(json.dumps(payload))
    try:
        retention.inspect_failed_artifact(
            root,
            artifact.name,
            audit,
            minimum_age_minutes=0,
            proc_root=proc_root,
        )
    except ValueError as error:
        assert "unsupported failed-artifact audit contract" in str(error)
    else:
        raise AssertionError("unreviewed source audit authorized retention")

    assert "spacegate.hunt_reffert_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.extended_catalog_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.msc_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.wds_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.gaia_ucd_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.ultracoolsheet_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.tess_targeted_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.bailer_jones_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.apogee_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.galah_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
    assert "spacegate.lamost_scientific_evidence_audit.v1" in (
        retention.SUPPORTED_FAILED_ARTIFACT_AUDITS
    )
