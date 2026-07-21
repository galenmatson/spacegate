from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "prune_selected_fact_artifacts.py"


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def make_failed_artifact(state: Path, build_id: str) -> tuple[Path, Path]:
    artifact = state / "derived/evidence_lake_v2/selected_facts" / build_id
    artifact.mkdir(parents=True)
    database = artifact / "selected_facts.duckdb"
    database.write_bytes(b"independently rejected selected-fact artifact")
    database_sha = hashlib.sha256(database.read_bytes()).hexdigest()
    write_json(
        artifact / "manifest.json",
        {
            "build_id": build_id,
            "report": {"files": {database.name: {"sha256": database_sha}}},
        },
    )
    audit = state / "external-audits" / f"{build_id}.json"
    write_json(
        audit,
        {
            "schema_version": "spacegate.selected_fact_artifact_audit.v1",
            "status": "fail",
            "build_id": build_id,
            "database": str(database),
            "database_sha256": database_sha,
            "failing_checks": {"missing_fact_partition_files": 1},
        },
    )
    return artifact, audit


def run_retention(
    state: Path,
    build_id: str,
    audit: Path,
    report: Path,
    *extra: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--state-dir",
            str(state),
            "--failed-audit",
            f"{build_id}={audit}",
            "--minimum-age-minutes",
            "0",
            "--reason",
            "unit-test rejected artifact",
            "--report",
            str(report),
            *extra,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_selected_fact_retention_requires_exact_dry_run_hash(tmp_path: Path) -> None:
    state = tmp_path / "state"
    build_id = "a" * 24
    artifact, audit = make_failed_artifact(state, build_id)
    report = state / "reports/evidence_lake_v2/retention.json"

    dry_run = run_retention(state, build_id, audit, report)
    assert dry_run.returncode == 0, dry_run.stderr
    dry_report = json.loads(report.read_text(encoding="utf-8"))
    assert dry_report["status"] == "pass"
    assert dry_report["action"] == "dry_run"
    candidate_hash = dry_report["candidate_set_sha256"]

    refused = run_retention(
        state,
        build_id,
        audit,
        report,
        "--apply",
        "--expected-candidate-set-sha256",
        "0" * 64,
    )
    assert refused.returncode != 0
    assert artifact.is_dir()

    applied = run_retention(
        state,
        build_id,
        audit,
        report,
        "--apply",
        "--expected-candidate-set-sha256",
        candidate_hash,
    )
    assert applied.returncode == 0, applied.stderr
    assert not artifact.exists()
    applied_report = json.loads(report.read_text(encoding="utf-8"))
    assert applied_report["action"] == "applied"
    assert applied_report["candidate_set_sha256"] == candidate_hash


def test_selected_fact_retention_requires_exact_report_acknowledgement(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    build_id = "c" * 24
    artifact, audit = make_failed_artifact(state, build_id)
    retained = state / "reports/evidence_lake_v2/historical.json"
    write_json(retained, {"historical_build_id": build_id})
    report = state / "reports/evidence_lake_v2/retention.json"

    refused = run_retention(state, build_id, audit, report)
    assert refused.returncode != 0
    assert artifact.exists()

    accepted = run_retention(
        state,
        build_id,
        audit,
        report,
        "--acknowledged-report",
        f"{build_id}={retained}",
    )
    assert accepted.returncode == 0, accepted.stderr
    value = json.loads(report.read_text(encoding="utf-8"))
    assert value["candidates"][0]["retained_report_references"] == [
        {
            "path": str(retained.resolve()),
            "sha256": hashlib.sha256(retained.read_bytes()).hexdigest(),
        }
    ]


def test_interrupted_selected_fact_staging_requires_exact_hash(tmp_path: Path) -> None:
    state = tmp_path / "state"
    root = state / "derived/evidence_lake_v2/selected_facts"
    root.mkdir(parents=True)
    staging = root / f".{('b' * 24)}.interrupted"
    staging.mkdir()
    (staging / "selected_facts.duckdb").write_bytes(b"partial")
    report = state / "reports/evidence_lake_v2/interrupted-retention.json"

    command = [
        sys.executable,
        str(SCRIPT),
        "--state-dir",
        str(state),
        "--interrupted-staging",
        staging.name,
        "--minimum-age-minutes",
        "0",
        "--reason",
        "unit-test interrupted compiler",
        "--report",
        str(report),
    ]
    dry_run = subprocess.run(command, check=False, capture_output=True, text=True)
    assert dry_run.returncode == 0, dry_run.stderr
    dry_report = json.loads(report.read_text(encoding="utf-8"))
    assert dry_report["candidates"][0]["artifact_state"] == (
        "interrupted_manifestless_selected_fact_staging"
    )

    applied = subprocess.run(
        [
            *command,
            "--apply",
            "--expected-candidate-set-sha256",
            dry_report["candidate_set_sha256"],
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert applied.returncode == 0, applied.stderr
    assert not staging.exists()
