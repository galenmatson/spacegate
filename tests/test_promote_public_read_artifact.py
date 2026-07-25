from __future__ import annotations

import json
import sqlite3
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import promote_public_read_artifact as promoter  # noqa: E402


def write_artifact(root: Path, *, build_id: str, marker: str) -> str:
    root.mkdir()
    database = root / "public_read.sqlite"
    con = sqlite3.connect(database)
    con.execute("CREATE TABLE marker(value TEXT NOT NULL)")
    con.execute("INSERT INTO marker VALUES (?)", (marker,))
    con.commit()
    con.close()
    digest = promoter.sha256_file(database)
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "build_id": build_id,
                "artifact": {"sha256": digest},
            }
        ),
        encoding="utf-8",
    )
    return digest


def marker(root: Path) -> str:
    con = sqlite3.connect(root / "public_read.sqlite")
    try:
        return str(con.execute("SELECT value FROM marker").fetchone()[0])
    finally:
        con.close()


def test_promotes_verified_artifact_and_retains_rollback(tmp_path: Path) -> None:
    active = tmp_path / "active"
    staging = tmp_path / "staging"
    write_artifact(active, build_id="build-1", marker="old")
    staged_digest = write_artifact(staging, build_id="build-1", marker="new")
    report = promoter.run(
        Namespace(
            active_dir=str(active),
            staging_dir=str(staging),
            backup_suffix="before",
            report=str(tmp_path / "report.json"),
        )
    )
    assert report["promoted_sha256"] == staged_digest
    assert marker(active) == "new"
    rollback = sqlite3.connect(active / "public_read.rollback.before.sqlite")
    try:
        assert rollback.execute("SELECT value FROM marker").fetchone()[0] == "old"
    finally:
        rollback.close()
    assert (active / "manifest.rollback.before.json").is_file()


def test_refuses_staged_hash_mismatch_without_touching_active(
    tmp_path: Path,
) -> None:
    active = tmp_path / "active"
    staging = tmp_path / "staging"
    write_artifact(active, build_id="build-1", marker="old")
    write_artifact(staging, build_id="build-1", marker="new")
    manifest_path = staging / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifact"]["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SystemExit, match="hash disagrees"):
        promoter.run(
            Namespace(
                active_dir=str(active),
                staging_dir=str(staging),
                backup_suffix="before",
                report=None,
            )
        )
    assert marker(active) == "old"
    assert not (active / "public_read.rollback.before.sqlite").exists()
