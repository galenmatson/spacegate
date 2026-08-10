from __future__ import annotations

import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import prune_rejected_build_artifacts as module


def test_plan_accepts_unlinked_rejected_workspace(monkeypatch, tmp_path: Path) -> None:
    candidate = tmp_path / "rejected/candidate"
    candidate.mkdir(parents=True)
    payload = candidate / "artifact.sqlite"
    payload.write_bytes(b"rejected")
    old = 1_600_000_000
    os.utime(payload, (old, old))
    os.utime(candidate, (old, old))
    monkeypatch.setattr(module, "open_processes", lambda _: [])

    report = module.plan(
        tmp_path,
        ["candidate"],
        minimum_age_minutes=60,
        reason="failed required hierarchy coverage",
    )

    assert report["candidate_count"] == 1
    assert report["reclaimable_bytes"] > 0


def test_inspect_rejects_state_link(monkeypatch, tmp_path: Path) -> None:
    candidate = tmp_path / "rejected/candidate"
    candidate.mkdir(parents=True)
    (candidate / "artifact.sqlite").write_bytes(b"rejected")
    (tmp_path / "current").symlink_to(candidate)
    monkeypatch.setattr(module, "open_processes", lambda _: [])

    try:
        module.inspect(tmp_path, "candidate", 0)
    except ValueError as exc:
        assert "state-linked" in str(exc)
    else:
        raise AssertionError("state-linked rejected workspace was accepted")
