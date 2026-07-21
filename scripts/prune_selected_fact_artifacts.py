#!/usr/bin/env python3
"""Fail-closed retention for independently rejected E5 selected-fact artifacts."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from prune_evidence_lake_artifacts import (
    BUILD_NAME,
    TEMPORARY_NAME,
    allocated_bytes,
    current_pointer_references,
    file_hash,
    open_processes,
    stable_hash,
    tree_identity,
    utc_now,
    write_json,
)


DEFAULT_STATE = Path("/data/spacegate/state")
CONTRACT = "spacegate.selected_fact_artifact_retention.v1"
AUDIT_CONTRACT = "spacegate.selected_fact_artifact_audit.v1"
DEFAULT_SPILL = Path("/mnt/space/spacegate/e5-selection-spill")


def report_references(
    state_dir: Path,
    build_id: str,
    *,
    ignored: set[Path],
) -> list[str]:
    references: list[str] = []
    report_root = state_dir / "reports/evidence_lake_v2"
    for path in sorted(report_root.glob("*.json")):
        resolved = path.resolve()
        if resolved in ignored:
            continue
        try:
            text = path.read_text(encoding="utf-8")
            if build_id in text:
                references.append(str(path))
        except (OSError, UnicodeDecodeError) as error:
            raise ValueError(f"cannot prove selected-fact report references: {path}: {error}") from error
    return references


def inspect_failed_artifact(
    state_dir: Path,
    build_id: str,
    audit_path: Path,
    *,
    minimum_age_minutes: float,
    ignored_reports: set[Path],
) -> dict[str, Any]:
    root = (state_dir / "derived/evidence_lake_v2/selected_facts").resolve(strict=True)
    candidate = (root / build_id).resolve(strict=True)
    if candidate.parent != root or not BUILD_NAME.fullmatch(candidate.name):
        raise ValueError(f"candidate must be a direct selected-fact build: {build_id}")
    if candidate.is_symlink() or not candidate.is_dir():
        raise ValueError(f"candidate must be a real directory: {build_id}")
    manifest_path = candidate / "manifest.json"
    database = candidate / "selected_facts.duckdb"
    if not manifest_path.is_file() or not database.is_file():
        raise ValueError(f"candidate lacks selected-fact manifest/database: {build_id}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("build_id") != build_id:
        raise ValueError(f"candidate manifest identity mismatch: {build_id}")
    expected_database_sha = (
        manifest.get("report", {}).get("files", {}).get(database.name, {}).get("sha256")
    )
    actual_database_sha = file_hash(database)
    if expected_database_sha != actual_database_sha:
        raise ValueError(f"candidate database checksum mismatch: {build_id}")

    audit_path = audit_path.resolve(strict=True)
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    if (
        audit.get("schema_version") != AUDIT_CONTRACT
        or audit.get("status") != "fail"
        or audit.get("build_id") != build_id
        or Path(str(audit.get("database") or "")).resolve() != database
        or audit.get("database_sha256") != actual_database_sha
    ):
        raise ValueError(f"audit does not independently fail candidate: {build_id}")
    failing = {
        str(name): int(value)
        for name, value in (audit.get("failing_checks") or {}).items()
        if int(value)
    }
    if not failing:
        raise ValueError(f"failed audit has no nonzero checks: {build_id}")

    identity = tree_identity(candidate)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError(f"candidate contains symlinks: {build_id}")
    if any(row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity):
        raise ValueError(f"candidate contains shared files: {build_id}")
    active = open_processes(candidate)
    if active:
        raise ValueError(f"candidate is open by live processes: {build_id}:{active}")
    pointers = current_pointer_references(root, candidate)
    if pointers:
        raise ValueError(f"candidate is current: {build_id}:{pointers}")
    references = report_references(
        state_dir,
        build_id,
        ignored={audit_path, *ignored_reports},
    )
    if references:
        raise ValueError(f"candidate is referenced by retained reports: {build_id}:{references}")

    newest_mtime_ns = max(int(row["mtime_ns"]) for row in identity)
    age_seconds = max(
        0.0,
        datetime.now(timezone.utc).timestamp() - newest_mtime_ns / 1e9,
    )
    if age_seconds < minimum_age_minutes * 60:
        raise ValueError(
            f"candidate is newer than minimum age: {build_id}:"
            f"{age_seconds:.1f}s<{minimum_age_minutes * 60:.1f}s"
        )
    return {
        "name": build_id,
        "path": str(candidate),
        "artifact_state": "immutable_independently_audit_failed",
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_identity_sha256": stable_hash(identity),
        "tree_entry_count": len(identity),
        "manifest_sha256": file_hash(manifest_path),
        "database_sha256": actual_database_sha,
        "audit_report": str(audit_path),
        "audit_sha256": file_hash(audit_path),
        "failed_checks": failing,
    }


def inspect_interrupted_staging(
    state_dir: Path,
    name: str,
    *,
    minimum_age_minutes: float,
) -> dict[str, Any]:
    root = (state_dir / "derived/evidence_lake_v2/selected_facts").resolve(strict=True)
    candidate = (root / name).resolve(strict=True)
    if candidate.parent != root or not TEMPORARY_NAME.fullmatch(candidate.name):
        raise ValueError(f"candidate must be a direct interrupted staging tree: {name}")
    if candidate.is_symlink() or not candidate.is_dir():
        raise ValueError(f"staging candidate must be a real directory: {name}")
    if (candidate / "manifest.json").exists():
        raise ValueError(f"manifest-bearing staging candidate is protected: {name}")
    if not (candidate / "selected_facts.duckdb").is_file():
        raise ValueError(f"staging candidate lacks selected_facts.duckdb: {name}")
    pointers = current_pointer_references(root, candidate)
    if pointers:
        raise ValueError(f"staging candidate is current: {name}:{pointers}")
    return inspect_interrupted_tree(
        candidate,
        name=name,
        artifact_state="interrupted_manifestless_selected_fact_staging",
        minimum_age_minutes=minimum_age_minutes,
    )


def inspect_interrupted_tree(
    candidate: Path,
    *,
    name: str,
    artifact_state: str,
    minimum_age_minutes: float,
) -> dict[str, Any]:
    identity = tree_identity(candidate)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError(f"interrupted candidate contains symlinks: {name}")
    if any(row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity):
        raise ValueError(f"interrupted candidate contains shared files: {name}")
    active = open_processes(candidate)
    if active:
        raise ValueError(f"interrupted candidate is open by live processes: {name}:{active}")
    newest_mtime_ns = max(int(row["mtime_ns"]) for row in identity)
    age_seconds = max(
        0.0,
        datetime.now(timezone.utc).timestamp() - newest_mtime_ns / 1e9,
    )
    if age_seconds < minimum_age_minutes * 60:
        raise ValueError(
            f"interrupted candidate is newer than minimum age: {name}:"
            f"{age_seconds:.1f}s<{minimum_age_minutes * 60:.1f}s"
        )
    return {
        "name": name,
        "path": str(candidate),
        "artifact_state": artifact_state,
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_identity_sha256": stable_hash(identity),
        "tree_entry_count": len(identity),
    }


def inspect_interrupted_spill(
    value: Path,
    *,
    minimum_age_minutes: float,
) -> dict[str, Any]:
    configured = value.resolve(strict=True)
    allowed = DEFAULT_SPILL.resolve(strict=True)
    if configured != allowed:
        raise ValueError(f"spill candidate must be the E5 compiler spill root: {configured}")
    if configured.is_symlink() or not configured.is_dir():
        raise ValueError(f"spill candidate must be a real directory: {configured}")
    return inspect_interrupted_tree(
        configured,
        name=configured.name,
        artifact_state="interrupted_selected_fact_spill",
        minimum_age_minutes=minimum_age_minutes,
    )


def retention_report(
    state_dir: Path,
    failed_audits: dict[str, Path],
    *,
    interrupted_staging: list[str],
    spill: Path | None,
    reason: str,
    minimum_age_minutes: float,
    output_report: Path,
) -> dict[str, Any]:
    if not failed_audits and not interrupted_staging and spill is None:
        raise ValueError("at least one explicit candidate is required")
    if spill is not None and not interrupted_staging:
        raise ValueError("spill cleanup requires an explicitly named interrupted staging tree")
    if not reason.strip():
        raise ValueError("an explicit retention reason is required")
    rows = [
        inspect_failed_artifact(
            state_dir,
            build_id,
            audit,
            minimum_age_minutes=minimum_age_minutes,
            ignored_reports={output_report.resolve()},
        )
        for build_id, audit in sorted(failed_audits.items())
    ]
    rows.extend(
        inspect_interrupted_staging(
            state_dir,
            name,
            minimum_age_minutes=minimum_age_minutes,
        )
        for name in sorted(interrupted_staging)
    )
    if spill is not None:
        rows.append(
            inspect_interrupted_spill(
                spill,
                minimum_age_minutes=minimum_age_minutes,
            )
        )
    rows.sort(key=lambda row: (str(row["artifact_state"]), str(row["path"])))
    candidate_hash = stable_hash(
        [
            {
                "name": row["name"],
                "allocated_bytes": row["allocated_bytes"],
                "tree_identity_sha256": row["tree_identity_sha256"],
                "audit_sha256": row.get("audit_sha256"),
            }
            for row in rows
        ]
    )
    return {
        "schema_version": CONTRACT,
        "status": "pass",
        "action": "dry_run",
        "artifact_root": str(
            (state_dir / "derived/evidence_lake_v2/selected_facts").resolve()
        ),
        "reason": reason.strip(),
        "minimum_age_minutes": minimum_age_minutes,
        "candidate_set_sha256": candidate_hash,
        "candidate_count": len(rows),
        "reclaimable_bytes": sum(int(row["allocated_bytes"]) for row in rows),
        "candidates": rows,
        "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--failed-audit", action="append", default=[], metavar="BUILD_ID=REPORT")
    parser.add_argument("--interrupted-staging", action="append", default=[], metavar="NAME")
    parser.add_argument("--spill", type=Path)
    parser.add_argument("--minimum-age-minutes", type=float, default=60.0)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-candidate-set-sha256")
    args = parser.parse_args()
    failed_audits: dict[str, Path] = {}
    for specification in args.failed_audit:
        if specification.count("=") != 1:
            raise ValueError("failed audit must use BUILD_ID=REPORT")
        build_id, report = specification.split("=", 1)
        if not build_id or not report or build_id in failed_audits:
            raise ValueError("failed audit must identify one unique build and report")
        failed_audits[build_id] = Path(report)
    report = retention_report(
        args.state_dir.resolve(),
        failed_audits,
        interrupted_staging=args.interrupted_staging,
        spill=args.spill,
        reason=args.reason,
        minimum_age_minutes=args.minimum_age_minutes,
        output_report=args.report,
    )
    if args.apply:
        if args.expected_candidate_set_sha256 != report["candidate_set_sha256"]:
            raise ValueError("apply requires the exact current dry-run candidate-set hash")
        for row in report["candidates"]:
            shutil.rmtree(row["path"])
        report = {**report, "action": "applied", "applied_at": utc_now()}
    write_json(args.report, report)
    print(
        f"{report['action']}: candidates={report['candidate_count']} "
        f"bytes={report['reclaimable_bytes']} hash={report['candidate_set_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
