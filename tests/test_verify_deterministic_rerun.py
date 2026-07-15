from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "verify_deterministic_rerun.py"


def write_report(
    state_dir: Path,
    build_id: str,
    *,
    transform_fingerprint: str,
    xor_hash: str,
) -> None:
    report_dir = state_dir / "reports" / build_id
    report_dir.mkdir(parents=True)
    table_fingerprint = {
        "row_count": 1,
        "xor_hash_hex": xor_hash,
        "min_hash_uint64": 1,
        "max_hash_uint64": 1,
    }
    payload = {
        "build_id": build_id,
        "generated_at": f"2026-07-15T00:00:0{len(build_id)}Z",
        "source_inputs_fingerprint": "canonical:fixture",
        "transform_version": "ingest_canonical_build",
        "transform_fingerprint": transform_fingerprint,
        "build_layer": "core",
        "slice_profile_id": "",
        "slice_profile_version": "",
        "table_fingerprints": {
            table: table_fingerprint for table in ("stars", "systems", "planets")
        },
    }
    (report_dir / "determinism_report.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )


def run_checker(state_dir: Path, build_id: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--state-dir",
            str(state_dir),
            "--build-id",
            build_id,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_different_transform_fingerprint_is_not_a_comparable_baseline(tmp_path: Path) -> None:
    write_report(tmp_path, "old", transform_fingerprint="oldsha", xor_hash="old")
    write_report(tmp_path, "new", transform_fingerprint="newsha", xor_hash="new")

    result = run_checker(tmp_path, "new")

    assert result.returncode == 0
    assert "No comparable baseline" in result.stdout


def test_same_transform_fingerprint_must_match_table_hashes(tmp_path: Path) -> None:
    write_report(tmp_path, "first", transform_fingerprint="same", xor_hash="first")
    write_report(tmp_path, "second", transform_fingerprint="same", xor_hash="second")

    result = run_checker(tmp_path, "second")

    assert result.returncode == 1
    assert "Determinism fingerprint mismatch" in result.stdout
