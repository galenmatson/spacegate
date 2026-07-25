from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compare_public_read_artifacts as comparator  # noqa: E402


def write_artifact(root: Path) -> None:
    root.mkdir()
    database = root / "public_read.sqlite"
    database.write_bytes(b"deterministic")
    manifest = {
        "status": "pass",
        "build_id": "build-1",
        "projection_schema_version": "projection-v2",
        "search_schema_version": "search-v2",
        "stellar_badge_overlay_schema_version": "stellar-overlay-v1",
        "policy": {"sha256": "policy"},
        "source_artifacts": {"core": "core"},
        "counts": {"systems": 1},
        "representation_counts": {"singleton": 1},
        "logical_hashes": {"systems": "logical"},
        "artifact": {"sha256": comparator.sha256(database)},
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_requires_byte_identical_complete_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reference = tmp_path / "reference"
    reproduced = tmp_path / "reproduced"
    write_artifact(reference)
    shutil.copytree(reference, reproduced)
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_public_read_artifacts.py",
            "--reference-dir",
            str(reference),
            "--reproduced-dir",
            str(reproduced),
            "--output",
            str(output),
        ],
    )
    assert comparator.main() == 0
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "pass"
