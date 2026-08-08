from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "public_edge_cold_storage.py"
SPEC = importlib.util.spec_from_file_location("public_edge_cold_storage", SCRIPT)
assert SPEC and SPEC.loader
cold = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cold)


VOLUME_ID = "a243664c-231f-4cf8-8487-bb39f82d555d"
BUILD_ID = "20260717T0614Z_f452835_side"


def fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    volume = tmp_path / "volume"
    cold_root = volume / "spacegate"
    cold_root.mkdir(parents=True)
    (volume / cold.MARKER_NAME).write_text(VOLUME_ID + "\n", encoding="utf-8")
    state = tmp_path / "hot"
    build = state / "out" / BUILD_ID
    (build / "map_tiles").mkdir(parents=True)
    (build / "core.duckdb").write_bytes(b"core")
    (build / "map_tiles" / "index.json").write_text("{}\n", encoding="utf-8")
    active = state / "out" / "active"
    active.mkdir()
    (active / "core.duckdb").write_bytes(b"active")
    (state / "served").mkdir()
    (state / "served" / "current").symlink_to(active)
    archive = tmp_path / f"{BUILD_ID}.7z"
    archive.write_bytes(b"archive")
    return cold_root, state, archive


def direct_args(cold_root: Path, state: Path, archive: Path | None = None) -> object:
    return type(
        "Args",
        (),
        {
            "cold_root": cold_root,
            "hot_state_dir": state,
            "volume_id": VOLUME_ID,
            "build_id": BUILD_ID,
            "archive": archive,
            "minimum_free_bytes": 0,
        },
    )()


def allow_test_volume(monkeypatch: pytest.MonkeyPatch) -> None:
    original = cold.verify_cold_root

    def wrapped(
        cold_root,
        volume_id,
        *,
        hot_root=None,
        require_mount=True,
        require_distinct_filesystem=True,
    ):
        return original(
            cold_root,
            volume_id,
            hot_root=hot_root,
            require_mount=False,
            require_distinct_filesystem=False,
        )

    monkeypatch.setattr(cold, "verify_cold_root", wrapped)


def test_snapshot_verify_retire_and_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cold_root, state, archive = fixture(tmp_path)
    allow_test_volume(monkeypatch)
    args = direct_args(cold_root, state, archive)

    created = cold.command_snapshot(args)
    assert created["reused"] is False
    snapshot = cold.snapshot_path(cold_root, BUILD_ID)
    manifest = json.loads(snapshot.read_text())
    assert manifest["schema_version"] == cold.SCHEMA_VERSION
    assert created["file_count"] == 2

    checked = cold.command_verify_snapshot(args)
    assert checked["logical_sha256"] == created["logical_sha256"]

    retired = cold.command_retire_hot(args)
    assert retired["status"] == "pass"
    assert not (state / "out" / BUILD_ID).exists()
    assert not archive.exists()

    restored = cold.command_restore(args)
    assert restored["restored"] is True
    assert (state / "out" / BUILD_ID / "core.duckdb").read_bytes() == b"core"


def test_snapshot_rejects_current_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cold_root, state, archive = fixture(tmp_path)
    allow_test_volume(monkeypatch)
    current = state / "served" / "current"
    current.unlink()
    current.symlink_to(state / "out" / BUILD_ID)
    with pytest.raises(ValueError, match="currently served"):
        cold.command_snapshot(direct_args(cold_root, state, archive))


def test_volume_marker_must_match(tmp_path: Path) -> None:
    cold_root, state, _ = fixture(tmp_path)
    with pytest.raises(ValueError, match="marker mismatch"):
        cold.verify_cold_root(
            cold_root,
            "11111111-1111-1111-1111-111111111111",
            hot_root=state,
            require_mount=False,
        )


def test_verify_detects_cold_corruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cold_root, state, archive = fixture(tmp_path)
    allow_test_volume(monkeypatch)
    args = direct_args(cold_root, state, archive)
    cold.command_snapshot(args)
    copied = cold_root / "rollbacks" / BUILD_ID / "out" / BUILD_ID / "core.duckdb"
    copied.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="file inventory mismatch"):
        cold.command_verify_snapshot(args)


def test_verify_detects_unmanifested_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cold_root, state, archive = fixture(tmp_path)
    allow_test_volume(monkeypatch)
    args = direct_args(cold_root, state, archive)
    cold.command_snapshot(args)
    build = cold_root / "rollbacks" / BUILD_ID / "out" / BUILD_ID
    (build / "unexpected.txt").write_text("unexpected\n", encoding="utf-8")
    with pytest.raises(ValueError, match="file inventory mismatch"):
        cold.command_verify_snapshot(args)
