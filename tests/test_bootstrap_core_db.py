from __future__ import annotations

import pathlib
import json
import hashlib
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


def read_report_metadata(metadata: pathlib.Path) -> subprocess.CompletedProcess[str]:
    command = (
        f"source {SCRIPT!s}; "
        f"PYTHON_BIN={sys.executable!s}; "
        'read_report_metadata "$1"'
    )
    return subprocess.run(
        ["bash", "-c", command, "bootstrap-report-test", str(metadata)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_report_metadata_accepts_bounded_checksummed_json(tmp_path: pathlib.Path) -> None:
    metadata = tmp_path / "current.json"
    metadata.write_text(json.dumps({
        "reports": {
            "qc_report": {
                "path": "reports/build-1/qc_report.json",
                "bytes": 42,
                "sha256": "a" * 64,
            }
        }
    }))

    result = read_report_metadata(metadata)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"reports/build-1/qc_report.json\t42\t{'a' * 64}"


def test_report_metadata_rejects_path_traversal(tmp_path: pathlib.Path) -> None:
    metadata = tmp_path / "current.json"
    metadata.write_text(json.dumps({
        "reports": {
            "qc_report": {
                "path": "../outside.json",
                "bytes": 2,
                "sha256": "b" * 64,
            }
        }
    }))

    result = read_report_metadata(metadata)

    assert result.returncode != 0
    assert "Unsafe report path" in result.stderr


def test_install_published_reports_from_bounded_file_metadata(tmp_path: pathlib.Path) -> None:
    download_root = tmp_path / "dl"
    source_report = download_root / "reports" / "build-1" / "qc_report.json"
    source_report.parent.mkdir(parents=True)
    report_bytes = b'{"build_id":"build-1","status":"ok"}\n'
    source_report.write_bytes(report_bytes)
    metadata = download_root / "current.json"
    metadata.write_text(json.dumps({
        "reports": {
            "qc_report": {
                "path": "reports/build-1/qc_report.json",
                "bytes": len(report_bytes),
                "sha256": hashlib.sha256(report_bytes).hexdigest(),
            }
        }
    }))
    state_dir = tmp_path / "state"
    command = (
        f"source {SCRIPT!s}; "
        f"PYTHON_BIN={sys.executable!s}; "
        'STATE_DIR="$1"; install_published_reports "$2" "$3" "$4" build-1 0'
    )

    result = subprocess.run(
        [
            "bash",
            "-c",
            command,
            "bootstrap-report-install-test",
            str(state_dir),
            str(metadata),
            metadata.as_uri(),
            download_root.as_uri() + "/",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (state_dir / "reports" / "build-1" / "qc_report.json").read_bytes() == report_bytes
