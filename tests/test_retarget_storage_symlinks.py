from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import retarget_storage_symlinks as module


def test_inspect_and_apply_retargets_only_source_links(tmp_path: Path) -> None:
    state = tmp_path / "state"
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    unrelated = tmp_path / "unrelated"
    for root in (state, source, destination, unrelated):
        root.mkdir()
    (source / "family/build").mkdir(parents=True)
    (destination / "family/build").mkdir(parents=True)
    (unrelated / "build").mkdir()
    migrated = state / "migrated"
    untouched = state / "untouched"
    broken_unrelated = state / "broken-unrelated"
    migrated.symlink_to(source / "family/build")
    untouched.symlink_to(unrelated / "build")
    broken_unrelated.symlink_to("missing")

    report = module.inspect(state, source, destination)

    assert report["candidate_count"] == 1
    applied = module.apply(report, report["candidate_set_sha256"])
    assert applied["action"] == "applied"
    assert migrated.resolve() == destination / "family/build"
    assert untouched.resolve() == unrelated / "build"


def test_inspect_rejects_missing_destination(tmp_path: Path) -> None:
    state = tmp_path / "state"
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    (source / "build").mkdir(parents=True)
    state.mkdir()
    destination.mkdir()
    (state / "linked").symlink_to(source / "build")

    try:
        module.inspect(state, source, destination)
    except ValueError as exc:
        assert "destination target is missing" in str(exc)
    else:
        raise AssertionError("missing destination was accepted")
