from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "prune_compiler_staging", ROOT / "scripts/prune_compiler_staging.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_inspect_accepts_old_manifestless_database_staging(
    monkeypatch, tmp_path: Path
) -> None:
    allowed = tmp_path / "spacegate"
    root = allowed / "family"
    candidate = root / ".0123456789abcdef01234567.fixture"
    candidate.mkdir(parents=True)
    (candidate / "work.duckdb").write_bytes(b"fixture")
    old = 1_600_000_000
    os.utime(candidate / "work.duckdb", (old, old))
    os.utime(candidate, (old, old))
    monkeypatch.setattr(MODULE, "ALLOWED_ROOTS", (allowed,))
    monkeypatch.setattr(MODULE, "open_processes", lambda _: [])

    row = MODULE.inspect(root, candidate.name, 60)

    assert row["artifact_state"] == "interrupted_manifestless_compiler_staging"
    assert row["database_files"] == ["work.duckdb"]


def test_inspect_rejects_manifest_bearing_artifact(
    monkeypatch, tmp_path: Path
) -> None:
    allowed = tmp_path / "spacegate"
    root = allowed / "family"
    candidate = root / ".0123456789abcdef01234567.fixture"
    candidate.mkdir(parents=True)
    (candidate / "work.duckdb").write_bytes(b"fixture")
    (candidate / "manifest.json").write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(MODULE, "ALLOWED_ROOTS", (allowed,))

    try:
        MODULE.inspect(root, candidate.name, 0)
    except ValueError as exc:
        assert "manifest-bearing" in str(exc)
    else:
        raise AssertionError("manifest-bearing staging was accepted")
