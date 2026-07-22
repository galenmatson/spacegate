#!/usr/bin/env python3
"""Fail-closed retention for explicitly superseded E6 shadow products."""

from __future__ import annotations

import argparse
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from prune_evidence_lake_artifacts import (
    allocated_bytes,
    file_hash,
    open_processes,
    stable_hash,
    tree_identity,
    utc_now,
    write_json,
)


DEFAULT_STATE = Path("/data/spacegate/state")
CONTRACT = "spacegate.e6_shadow_retention.v1"
E6_BUILD_NAME = re.compile(r"^e6_[0-9a-f]{24}_shadow$")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read retention evidence {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"retention evidence is not a JSON object: {path}")
    return value


def pointer_references(state_dir: Path, candidate: Path) -> list[str]:
    references: list[str] = []
    roots = [
        state_dir / "served",
        state_dir / "rollback",
        state_dir / "published",
        state_dir / "out",
    ]
    for root in roots:
        if not root.is_dir():
            continue
        for pointer in sorted(root.iterdir()):
            try:
                if pointer.is_symlink() and pointer.resolve(strict=True) == candidate:
                    references.append(str(pointer))
                elif (
                    pointer.is_file()
                    and pointer.stat().st_size < 4096
                    and pointer.read_text(encoding="utf-8").strip() == candidate.name
                ):
                    references.append(str(pointer))
            except (FileNotFoundError, PermissionError, UnicodeDecodeError):
                continue
    return references


def manifest_references(
    state_dir: Path, build_id: str, *, ignored_builds: set[str]
) -> list[str]:
    references: list[str] = []
    for manifest_path in sorted((state_dir / "out").glob("*/manifest.json")):
        if manifest_path.parent.name in ignored_builds:
            continue
        try:
            if build_id in manifest_path.read_text(encoding="utf-8"):
                references.append(str(manifest_path))
        except (OSError, UnicodeDecodeError) as error:
            raise ValueError(
                f"cannot prove E6 manifest references: {manifest_path}: {error}"
            ) from error
    return references


def report_references(
    state_dir: Path, build_id: str, *, ignored: set[Path]
) -> list[Path]:
    references: list[Path] = []
    root = state_dir / "reports/evidence_lake_v2"
    for report_path in sorted(root.glob("*.json")):
        resolved = report_path.resolve()
        if resolved in ignored:
            continue
        try:
            text = report_path.read_text(encoding="utf-8")
            if build_id not in text:
                continue
            parsed = json.loads(text)
            if isinstance(parsed, dict) and parsed.get("schema_version") == CONTRACT:
                continue
            references.append(resolved)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                f"cannot prove E6 report references: {report_path}: {error}"
            ) from error
    return references


def validate_replacement(
    state_dir: Path,
    build_id: str,
    audit_path: Path,
    reproduction_path: Path,
) -> dict[str, Any]:
    if not E6_BUILD_NAME.fullmatch(build_id):
        raise ValueError(f"invalid E6 replacement build id: {build_id}")
    build_dir = state_dir / "out" / build_id
    manifest_path = build_dir / "manifest.json"
    if not build_dir.is_dir() or not manifest_path.is_file():
        raise ValueError(f"replacement build is incomplete: {build_dir}")
    manifest_sha = file_hash(manifest_path)
    manifest = load_json(manifest_path)
    if manifest.get("build_id") != build_id:
        raise ValueError("replacement manifest identity mismatch")
    report = manifest.get("report") or {}
    if report.get("status") != "pass" or report.get("promotion_status") != "unpromoted":
        raise ValueError("replacement is not a passing unpromoted E6 shadow")
    product_files = report.get("product_files") or {}
    if not product_files:
        raise ValueError("replacement manifest has no product integrity set")
    observed_products: dict[str, dict[str, Any]] = {}
    for filename, declared in sorted(product_files.items()):
        if Path(filename).name != filename:
            raise ValueError(f"unsafe replacement product path: {filename}")
        product = build_dir / filename
        observed = {"bytes": product.stat().st_size, "sha256": file_hash(product)}
        if observed != declared:
            raise ValueError(f"replacement product integrity mismatch: {filename}")
        observed_products[filename] = observed

    audit_path = audit_path.resolve(strict=True)
    audit = load_json(audit_path)
    if (
        audit.get("status") != "pass"
        or audit.get("build_id") != build_id
        or audit.get("manifest_sha256") != manifest_sha
        or audit.get("failing_checks")
    ):
        raise ValueError("replacement independent audit is not a matching pass")
    reproduction_path = reproduction_path.resolve(strict=True)
    reproduction = load_json(reproduction_path)
    if (
        reproduction.get("status") != "pass"
        or reproduction.get("build_id") != build_id
        or reproduction.get("reference_manifest_sha256") != manifest_sha
        or reproduction.get("reproduced_audit_status") != "pass"
        or any(int(value) for value in (reproduction.get("checks") or {}).values())
    ):
        raise ValueError("replacement reproduction is not a matching logical pass")
    return {
        "build_id": build_id,
        "path": str(build_dir.resolve()),
        "manifest_sha256": manifest_sha,
        "audit_path": str(audit_path),
        "audit_sha256": file_hash(audit_path),
        "reproduction_path": str(reproduction_path),
        "reproduction_sha256": file_hash(reproduction_path),
        "product_files": observed_products,
    }


def inspect_candidate(
    state_dir: Path,
    build_id: str,
    *,
    replacement_build_id: str,
    minimum_age_minutes: float,
    ignored_builds: set[str],
    ignored_reports: set[Path],
    acknowledged_reports: set[Path],
) -> dict[str, Any]:
    if not E6_BUILD_NAME.fullmatch(build_id) or build_id == replacement_build_id:
        raise ValueError(f"invalid superseded E6 candidate: {build_id}")
    out_root = (state_dir / "out").resolve(strict=True)
    raw_candidate = out_root / build_id
    if raw_candidate.is_symlink() or not raw_candidate.is_dir():
        raise ValueError(f"candidate must be a real E6 directory: {build_id}")
    candidate = raw_candidate.resolve(strict=True)
    if candidate.parent != out_root:
        raise ValueError(f"candidate escapes E6 output root: {build_id}")
    manifest_path = candidate / "manifest.json"
    manifest = load_json(manifest_path)
    if manifest.get("build_id") != build_id:
        raise ValueError(f"candidate manifest identity mismatch: {build_id}")
    if (manifest.get("report") or {}).get("promotion_status") != "unpromoted":
        raise ValueError(f"candidate is not explicitly unpromoted: {build_id}")

    identity = tree_identity(candidate)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError(f"candidate contains symlinks: {build_id}")
    if any(row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity):
        raise ValueError(f"candidate contains shared files: {build_id}")
    active = open_processes(candidate)
    if active:
        raise ValueError(f"candidate is open by live processes: {build_id}:{active}")
    pointers = pointer_references(state_dir, candidate)
    if pointers:
        raise ValueError(f"candidate is linked as current/rollback/published: {pointers}")
    manifest_refs = manifest_references(
        state_dir, build_id, ignored_builds=ignored_builds
    )
    if manifest_refs:
        raise ValueError(f"candidate is referenced by retained build manifests: {manifest_refs}")

    references = set(
        report_references(state_dir, build_id, ignored=ignored_reports)
    )
    unexpected = references - acknowledged_reports
    stale = acknowledged_reports - references
    if unexpected or stale:
        raise ValueError(
            f"candidate report references require an exact acknowledgement set: {build_id}:"
            f"unexpected={sorted(map(str, unexpected))}:stale={sorted(map(str, stale))}"
        )
    retained_reports = [
        {"path": str(path), "sha256": file_hash(path)} for path in sorted(references)
    ]
    newest_mtime_ns = max(int(row["mtime_ns"]) for row in identity)
    age_seconds = max(
        0.0, datetime.now(timezone.utc).timestamp() - newest_mtime_ns / 1e9
    )
    if age_seconds < minimum_age_minutes * 60:
        raise ValueError(f"candidate is newer than minimum age: {build_id}")
    declared_products = (manifest.get("report") or {}).get("product_files") or {}
    observed_product_metadata = {
        filename: {
            "bytes": (candidate / filename).stat().st_size,
            "declared": declared,
        }
        for filename, declared in sorted(declared_products.items())
        if Path(filename).name == filename and (candidate / filename).is_file()
    }
    return {
        "build_id": build_id,
        "path": str(candidate),
        "artifact_state": "explicitly_superseded_unpromoted_e6_shadow",
        "allocated_bytes": allocated_bytes(candidate),
        "age_seconds": round(age_seconds, 3),
        "tree_entry_count": len(identity),
        "tree_identity_sha256": stable_hash(identity),
        "manifest_sha256": file_hash(manifest_path),
        "policy_version": (manifest.get("report") or {}).get("policy_version"),
        "compiler_version": (manifest.get("report") or {}).get("compiler_version"),
        "declared_product_files": observed_product_metadata,
        "retained_report_references": retained_reports,
    }


def retention_report(
    *,
    state_dir: Path,
    candidate_ids: list[str],
    replacement_build_id: str,
    replacement_audit: Path,
    replacement_reproduction: Path,
    acknowledged_reports: dict[str, set[Path]],
    reason: str,
    minimum_age_minutes: float,
    output_report: Path,
) -> dict[str, Any]:
    state_dir = state_dir.resolve(strict=True)
    if not candidate_ids or len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("one or more unique explicit E6 candidates are required")
    if not reason.strip():
        raise ValueError("an explicit retention reason is required")
    if set(acknowledged_reports) - set(candidate_ids):
        raise ValueError("report acknowledgements name non-candidates")
    replacement = validate_replacement(
        state_dir, replacement_build_id, replacement_audit, replacement_reproduction
    )
    ignored_reports = {
        output_report.resolve(),
        replacement_audit.resolve(),
        replacement_reproduction.resolve(),
    }
    ignored_builds = set(candidate_ids)
    candidates = [
        inspect_candidate(
            state_dir,
            build_id,
            replacement_build_id=replacement_build_id,
            minimum_age_minutes=minimum_age_minutes,
            ignored_builds=ignored_builds,
            ignored_reports=ignored_reports,
            acknowledged_reports=acknowledged_reports.get(build_id, set()),
        )
        for build_id in sorted(candidate_ids)
    ]
    candidate_set_sha256 = stable_hash(
        [
            {
                "build_id": row["build_id"],
                "allocated_bytes": row["allocated_bytes"],
                "tree_identity_sha256": row["tree_identity_sha256"],
                "manifest_sha256": row["manifest_sha256"],
                "retained_report_references": row["retained_report_references"],
            }
            for row in candidates
        ]
    )
    return {
        "schema_version": CONTRACT,
        "status": "pass",
        "action": "dry_run",
        "reason": reason.strip(),
        "minimum_age_minutes": minimum_age_minutes,
        "replacement": replacement,
        "candidate_count": len(candidates),
        "candidate_set_sha256": candidate_set_sha256,
        "reclaimable_bytes": sum(int(row["allocated_bytes"]) for row in candidates),
        "candidates": candidates,
        "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--candidate", action="append", default=[])
    parser.add_argument("--replacement-build", required=True)
    parser.add_argument("--replacement-audit", type=Path, required=True)
    parser.add_argument("--replacement-reproduction", type=Path, required=True)
    parser.add_argument(
        "--acknowledged-report", action="append", default=[], metavar="BUILD_ID=REPORT"
    )
    parser.add_argument("--minimum-age-minutes", type=float, default=60.0)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-candidate-set-sha256")
    args = parser.parse_args()
    acknowledgements: dict[str, set[Path]] = {}
    for specification in args.acknowledged_report:
        if specification.count("=") != 1:
            raise ValueError("acknowledged report must use BUILD_ID=REPORT")
        build_id, raw_path = specification.split("=", 1)
        path = Path(raw_path).resolve(strict=True)
        reports = acknowledgements.setdefault(build_id, set())
        if path in reports:
            raise ValueError("acknowledged report list contains duplicates")
        reports.add(path)
    report = retention_report(
        state_dir=args.state_dir,
        candidate_ids=args.candidate,
        replacement_build_id=args.replacement_build,
        replacement_audit=args.replacement_audit,
        replacement_reproduction=args.replacement_reproduction,
        acknowledged_reports=acknowledgements,
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
