from __future__ import annotations

import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "bootstrap_core_db.sh"


def resolve_local(meta_url: str, base_url: str, artifact_url: str) -> subprocess.CompletedProcess[str]:
    command = (
        f"source {SCRIPT!s}; "
        f"PYTHON_BIN={sys.executable!s}; "
        'resolve_local_artifact "$1" "$2" "$3"'
    )
    return subprocess.run(
        ["bash", "-c", command, "bootstrap-test", meta_url, base_url, artifact_url],
        check=False,
        capture_output=True,
        text=True,
    )


def test_local_artifact_must_stay_under_file_base(tmp_path: pathlib.Path) -> None:
    download_root = tmp_path / "dl"
    archive = download_root / "db" / "build.7z"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"archive")
    metadata = download_root / "current.json"
    metadata.write_text("{}")

    result = resolve_local(metadata.as_uri(), download_root.as_uri() + "/", archive.as_uri())

    assert result.returncode == 0, result.stderr
    assert pathlib.Path(result.stdout.strip()) == archive.resolve()


def test_local_artifact_rejects_escape(tmp_path: pathlib.Path) -> None:
    download_root = tmp_path / "dl"
    download_root.mkdir()
    metadata = download_root / "current.json"
    metadata.write_text("{}")
    archive = tmp_path / "outside.7z"
    archive.write_bytes(b"archive")

    result = resolve_local(metadata.as_uri(), download_root.as_uri() + "/", archive.as_uri())

    assert result.returncode != 0
    assert "escapes bootstrap base directory" in result.stderr


def test_local_artifact_rejects_non_file_metadata(tmp_path: pathlib.Path) -> None:
    download_root = tmp_path / "dl"
    archive = download_root / "db" / "build.7z"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"archive")

    result = resolve_local(
        "https://example.invalid/current.json",
        download_root.as_uri() + "/",
        archive.as_uri(),
    )

    assert result.returncode != 0
    assert "non-file metadata" in result.stderr


def test_bootstrap_help_documents_immutable_promotion() -> None:
    result = subprocess.run(
        [str(SCRIPT), "--help"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--skip-auto-score" in result.stdout
    assert "immutable published builds" in result.stdout
