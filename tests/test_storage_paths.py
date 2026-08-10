from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import storage_paths


def test_bulk_root_defaults_to_internal_nvme(monkeypatch) -> None:
    monkeypatch.delenv("SPACEGATE_BULK_DIR", raising=False)

    assert storage_paths.bulk_root() == Path("/space/spacegate")
    assert storage_paths.bulk_path("cache", "survey") == Path(
        "/space/spacegate/cache/survey"
    )


def test_storage_roots_accept_operator_overrides(monkeypatch, tmp_path: Path) -> None:
    bulk = tmp_path / "bulk"
    archive = tmp_path / "archive"
    monkeypatch.setenv("SPACEGATE_BULK_DIR", str(bulk))
    monkeypatch.setenv("SPACEGATE_COLD_ARCHIVE_DIR", str(archive))

    assert storage_paths.bulk_root() == bulk
    assert storage_paths.bulk_path("evidence") == bulk / "evidence"
    assert storage_paths.cold_archive_root() == archive


def test_empty_storage_environment_does_not_resolve_to_working_directory(
    monkeypatch,
) -> None:
    monkeypatch.setenv("SPACEGATE_BULK_DIR", "  ")
    monkeypatch.setenv("SPACEGATE_COLD_ARCHIVE_DIR", "")

    assert storage_paths.bulk_root() == storage_paths.DEFAULT_BULK_ROOT
    assert storage_paths.cold_archive_root() == storage_paths.DEFAULT_COLD_ARCHIVE_ROOT
