from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "prune_state_scratch.py"


def run_retention(
    state: Path,
    report: Path,
    *extra: str,
    scratch_scope: str = "state",
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--state-dir",
            str(state),
            "--scratch-scope",
            scratch_scope,
            "--candidate",
            "old-diagnostic",
            "--minimum-age-minutes",
            "0",
            "--reason",
            "unit-test disposable diagnostic",
            "--report",
            str(report),
            *extra,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_state_scratch_retention_requires_exact_candidate_hash(tmp_path: Path) -> None:
    state = tmp_path / "state"
    candidate = state / "tmp/old-diagnostic"
    candidate.mkdir(parents=True)
    (candidate / "scratch.duckdb").write_bytes(b"disposable diagnostic")
    protected = state / "protected.duckdb"
    protected.write_bytes(b"must remain")
    (candidate / "current.duckdb").symlink_to(protected)
    report = state / "reports/evidence_lake_v2/scratch-retention.json"

    dry_run = run_retention(state, report)
    assert dry_run.returncode == 0, dry_run.stderr
    dry_report = json.loads(report.read_text(encoding="utf-8"))
    assert dry_report["action"] == "dry_run"
    assert dry_report["candidate_count"] == 1

    refused = run_retention(
        state,
        report,
        "--apply",
        "--expected-candidate-set-sha256",
        "0" * 64,
    )
    assert refused.returncode != 0
    assert candidate.exists()

    applied = run_retention(
        state,
        report,
        "--apply",
        "--expected-candidate-set-sha256",
        dry_report["candidate_set_sha256"],
    )
    assert applied.returncode == 0, applied.stderr
    assert not candidate.exists()
    assert protected.read_bytes() == b"must remain"


def test_state_scratch_retention_supports_bounded_host_scratch(tmp_path: Path) -> None:
    state = tmp_path / "state"
    (state / "tmp").mkdir(parents=True)
    candidate = tmp_path / "tmp/old-diagnostic"
    candidate.mkdir(parents=True)
    (candidate / "scratch.bin").write_bytes(b"disposable host scratch")
    report = state / "reports/evidence_lake_v2/host-scratch-retention.json"

    dry_run = run_retention(state, report, scratch_scope="host")
    assert dry_run.returncode == 0, dry_run.stderr
    dry_report = json.loads(report.read_text(encoding="utf-8"))
    assert dry_report["scratch_scope"] == "host"
    assert dry_report["scratch_root"] == str(tmp_path / "tmp")

    applied = run_retention(
        state,
        report,
        "--apply",
        "--expected-candidate-set-sha256",
        dry_report["candidate_set_sha256"],
        scratch_scope="host",
    )
    assert applied.returncode == 0, applied.stderr
    assert not candidate.exists()
