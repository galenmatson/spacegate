from __future__ import annotations

import os
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
