from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import archive_storage_generations as archive
import storage_generation_policy as policy_module


def test_checked_in_policy_excludes_evidence_and_identity_roots() -> None:
    policy = policy_module.load_object(
        ROOT / "config/evidence_lake/storage_retention_roots.json"
    )
    excluded = set(policy["excluded_roots"])

    assert "$STATE_DIR/raw" in excluded
    assert "$STATE_DIR/typed" in excluded
    assert "$STATE_DIR/derived/evidence_lake_v2/identity" in excluded
    assert "$STATE_DIR/derived/evidence_lake_v2/permanent_identity_seed" in excluded
    assert "$STATE_DIR/reports" in excluded


def test_evidence_release_members_are_union_of_pinned_sets(tmp_path: Path) -> None:
    root = tmp_path / "derived/evidence_lake_v2/scientific_evidence_sets"
    for release_set_id, members in (("a", ["one", "two"]), ("b", ["two", "three"])):
        directory = root / release_set_id
        directory.mkdir(parents=True)
        (directory / "manifest.json").write_text(
            json.dumps(
                {
                    "release_set_id": release_set_id,
                    "members": [{"build_id": value} for value in members],
                }
            ),
            encoding="utf-8",
        )

    assert policy_module.evidence_release_members(tmp_path, ["a", "b"]) == {
        "one",
        "two",
        "three",
    }


def test_content_manifest_is_location_and_mtime_independent(tmp_path: Path) -> None:
    source = tmp_path / "source"
    copied = tmp_path / "copied"
    source.mkdir()
    (source / "nested").mkdir()
    (source / "nested/value.txt").write_text("evidence\n", encoding="utf-8")
    shutil.copytree(source, copied)

    source_manifest = archive.content_manifest(source)
    copied_manifest = archive.content_manifest(copied)

    assert source_manifest["content_sha256"] == copied_manifest["content_sha256"]
    (copied / "nested/value.txt").write_text("changed\n", encoding="utf-8")
    assert (
        archive.content_manifest(copied)["content_sha256"]
        != source_manifest["content_sha256"]
    )


def test_archive_destination_mirrors_absolute_source(tmp_path: Path) -> None:
    source = tmp_path / "state/out/build"
    source.mkdir(parents=True)
    destination = archive.archive_destination(source, tmp_path / "archive", "photon")

    assert destination == (
        tmp_path / "archive/hosts/photon" / source.relative_to("/")
    )


def test_archive_mount_selects_nfs_layer(
    tmp_path: Path, monkeypatch
) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir()

    def fake_run(*args, **kwargs):
        assert "-t" in args[0]
        assert "nfs4" in args[0]
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout="/mnt/proton 192.168.252.2:/ nfs4\n",
            stderr="",
        )

    monkeypatch.setattr(archive.subprocess, "run", fake_run)
    assert archive.archive_mount(archive_root) == {
        "target": "/mnt/proton",
        "source": "192.168.252.2:/",
        "fstype": "nfs4",
    }


def test_rsync_copy_preserves_content_and_modes(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir(mode=0o750)
    nested = source / "nested"
    nested.mkdir(mode=0o700)
    value = nested / "value.txt"
    value.write_text("archived\n", encoding="utf-8")
    value.chmod(0o640)

    archive.run_rsync(source, destination)

    assert (
        archive.content_manifest(source)["content_sha256"]
        == archive.content_manifest(destination)["content_sha256"]
    )


def test_retirement_reverifies_archive_before_removing_source(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    bulk = tmp_path / "bulk"
    report = tmp_path / "retirement.json"
    copy_report = tmp_path / "copy.json"
    policy_path = tmp_path / "policy.json"
    archive_root = tmp_path / "archive"
    for build in ("served", "deployed", "rollback", "candidate"):
        directory = state / "out" / build
        directory.mkdir(parents=True)
        (directory / "value.txt").write_text(f"{build}\n", encoding="utf-8")
    (state / "served").mkdir()
    (state / "served/current").symlink_to(state / "out/served")
    (state / "raw").mkdir()
    bulk.mkdir()

    source = state / "out/candidate"
    archived = archive_root / "hosts/photon" / source.relative_to("/")
    shutil.copytree(source, archived)
    source_identity = policy_module.inspect_tree(
        state_dir=state,
        label="candidate",
        family="state_output_generations",
        candidate=source,
        root=state / "out",
    )
    archived_content = archive.content_manifest(archived)
    candidate_hash = "candidate-set-hash"
    policy = {
        "schema_version": "spacegate.storage_retention_roots.v1",
        "policy_version": "test",
        "served_build": "served",
        "deployed_public_build": "deployed",
        "immediate_rollback_build": "rollback",
        "excluded_roots": ["$STATE_DIR/raw"],
    }
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    copy_report.write_text(
        json.dumps(
            {
                "schema_version": archive.COPY_CONTRACT,
                "status": "pass",
                "candidate_set_sha256": candidate_hash,
                "policy_sha256": archive.file_hash(policy_path),
                "archive_root": str(archive_root),
                "candidates": [
                    {
                        **source_identity,
                        "archive_path": str(archived),
                        "content_sha256": archived_content["content_sha256"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = archive.retire_copy(
        copy_report_path=copy_report,
        expected_hash=candidate_hash,
        policy_path=policy_path,
        policy=policy,
        state_dir=state,
        bulk_dir=bulk,
        report_path=report,
        reason="test verified retirement",
    )

    assert result["status"] == "pass"
    assert result["retired_candidate_count"] == 1
    assert not source.exists()
    assert (archived / "value.txt").read_text(encoding="utf-8") == "candidate\n"
    assert json.loads(report.read_text(encoding="utf-8"))["status"] == "pass"
