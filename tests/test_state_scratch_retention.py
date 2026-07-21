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
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--state-dir",
            str(state),
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
