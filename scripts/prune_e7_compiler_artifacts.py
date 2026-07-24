#!/usr/bin/env python3
"""Fail-closed retirement of unreachable E7 compiler generations."""

from __future__ import annotations

import argparse
import json
import re
import shutil
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
DEFAULT_BULK = Path("/mnt/space/spacegate")
CONTRACT = "spacegate.e7_compiler_retention.v1"
BUILD_ID = re.compile(r"^[0-9a-f]{24}$")
PUBLIC_BUILD = re.compile(
    r"^e7_([0-9a-f]{24})(?:_[a-z0-9][a-z0-9_-]*)?_public$"
)
FAMILIES = {
    "clean_foundation": "e7-clean-foundation",
    "clean_science": "e7-clean-science",
    "clean_runtime_core": "e7-clean-runtime-core",
    "clean_runtime_arm": "e7-clean-runtime-arm",
    "clean_runtime_disc": "e7-clean-runtime-disc",
}


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read JSON evidence {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"JSON evidence is not an object: {path}")
    return value


def artifact_root(state_dir: Path, bulk_dir: Path, family: str) -> Path:
    if family in {"clean_runtime_core", "clean_runtime_disc"}:
        internal = state_dir.parent / FAMILIES[family]
        if internal.is_dir():
            return internal
    return bulk_dir / FAMILIES[family]


def locate_artifact(
    state_dir: Path, bulk_dir: Path, family: str, build_id: str
) -> Path:
    candidates = [
        bulk_dir / FAMILIES[family] / build_id,
        state_dir.parent / FAMILIES[family] / build_id,
    ]
    found = [path for path in candidates if path.is_dir()]
    if len(found) != 1:
        raise ValueError(
            f"expected one {family}/{build_id} artifact, found {len(found)}"
        )
    return found[0].resolve(strict=True)


def retained_bundle_ids(state_dir: Path, bulk_dir: Path) -> set[str]:
    retained: set[str] = set()
    roots = [state_dir / "out", bulk_dir / "e7-public-state/out"]
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.iterdir():
            match = PUBLIC_BUILD.fullmatch(path.name)
            if match and path.is_dir():
                retained.add(match.group(1))
    served = state_dir / "served/current"
    if served.is_symlink():
        match = PUBLIC_BUILD.fullmatch(served.resolve(strict=True).name)
        if not match:
            raise ValueError(f"served pointer is not an E7 public build: {served}")
        retained.add(match.group(1))
    return retained


def dependency_closure(
    state_dir: Path, bulk_dir: Path, bundle_ids: set[str]
) -> tuple[set[tuple[str, str]], dict[str, dict[str, Any]]]:
    protected: set[tuple[str, str]] = set()
    bundle_manifests: dict[str, dict[str, Any]] = {}
    bundle_root = bulk_dir / "e7-clean-runtime-bundle"
    for bundle_id in sorted(bundle_ids):
        if not BUILD_ID.fullmatch(bundle_id):
            raise ValueError(f"invalid retained E7 bundle id: {bundle_id}")
        manifest_path = bundle_root / bundle_id / "manifest.json"
        manifest = load_json(manifest_path)
        if manifest.get("build_id") != bundle_id or manifest.get("status") != "pass":
            raise ValueError(f"retained bundle is not a passing artifact: {bundle_id}")
        bundle_manifests[bundle_id] = {
            "path": str(manifest_path.resolve()),
            "sha256": file_hash(manifest_path),
        }
        inputs = manifest.get("inputs") or {}
        for family, input_name in (
            ("clean_runtime_core", "core"),
            ("clean_runtime_arm", "arm"),
            ("clean_runtime_disc", "disc"),
        ):
            build_id = str((inputs.get(input_name) or {}).get("build_id") or "")
            if not BUILD_ID.fullmatch(build_id):
                raise ValueError(f"bundle {bundle_id} has invalid {input_name} input")
            protected.add((family, build_id))

    pending = list(protected)
    while pending:
        family, build_id = pending.pop()
        if family not in {"clean_runtime_core", "clean_runtime_arm"}:
            continue
        manifest_path = locate_artifact(
            state_dir, bulk_dir, family, build_id
        ) / "manifest.json"
        manifest = load_json(manifest_path)
        inputs = manifest.get("inputs") or {}
        dependencies: list[tuple[str, str]] = []
        if family == "clean_runtime_core":
            dependencies.extend(
                [
                    (
                        "clean_foundation",
                        str((inputs.get("clean_foundation") or {}).get("build_id") or ""),
                    ),
                    (
                        "clean_science",
                        str((inputs.get("clean_science") or {}).get("build_id") or ""),
                    ),
                ]
            )
        else:
            dependencies.extend(
                [
                    (
                        "clean_runtime_core",
                        str(
                            (inputs.get("clean_runtime_core") or {}).get("build_id")
                            or ""
                        ),
                    ),
                    (
                        "clean_science",
                        str((inputs.get("clean_science") or {}).get("build_id") or ""),
                    ),
                ]
            )
        for dependency in dependencies:
            if not BUILD_ID.fullmatch(dependency[1]):
                raise ValueError(
                    f"{family}/{build_id} has invalid {dependency[0]} dependency"
                )
            if dependency not in protected:
                protected.add(dependency)
                pending.append(dependency)
    return protected, bundle_manifests


def report_references(state_dir: Path, build_id: str) -> list[dict[str, str]]:
    references: list[dict[str, str]] = []
    root = state_dir / "reports/evidence_lake_v2"
    if not root.is_dir():
        return references
    for path in sorted(root.rglob("*.json")):
        try:
            if build_id in path.read_text(encoding="utf-8"):
                references.append(
                    {"path": str(path.resolve()), "sha256": file_hash(path)}
                )
        except (OSError, UnicodeDecodeError) as error:
            raise ValueError(f"cannot inspect retained report {path}: {error}") from error
    return references


def inspect_candidate(
    state_dir: Path,
    bulk_dir: Path,
    family: str,
    build_id: str,
    *,
    protected: set[tuple[str, str]],
) -> dict[str, Any]:
    if family not in FAMILIES or not BUILD_ID.fullmatch(build_id):
        raise ValueError(f"invalid E7 candidate: {family}/{build_id}")
    if (family, build_id) in protected:
        raise ValueError(f"candidate is reachable from a retained public build: {family}/{build_id}")
    candidate = locate_artifact(state_dir, bulk_dir, family, build_id)
    manifest_path = candidate / "manifest.json"
    manifest = load_json(manifest_path)
    if manifest.get("build_id") != build_id or manifest.get("status") != "pass":
        raise ValueError(f"candidate is not a passing immutable artifact: {family}/{build_id}")
    identity = tree_identity(candidate)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError(f"candidate contains symlinks: {family}/{build_id}")
    if any(row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity):
        raise ValueError(f"candidate contains shared files: {family}/{build_id}")
    active = open_processes(candidate)
    if active:
        raise ValueError(f"candidate is open by live processes: {family}/{build_id}:{active}")

    state_link = (
        state_dir
        / "derived/evidence_lake_v2"
        / family
        / build_id
    )
    if not state_link.is_symlink() or state_link.resolve(strict=True) != candidate:
        raise ValueError(f"candidate state link is missing or mismatched: {state_link}")
    return {
        "family": family,
        "build_id": build_id,
        "path": str(candidate),
        "state_link": str(state_link),
        "allocated_bytes": allocated_bytes(candidate),
        "manifest_sha256": file_hash(manifest_path),
        "tree_entry_count": len(identity),
        "tree_identity_sha256": stable_hash(identity),
        "retained_report_references": report_references(state_dir, build_id),
    }


def parse_candidate(value: str) -> tuple[str, str]:
    try:
        family, build_id = value.split(":", 1)
    except ValueError as error:
        raise argparse.ArgumentTypeError("candidate must be FAMILY:BUILD_ID") from error
    if family not in FAMILIES or not BUILD_ID.fullmatch(build_id):
        raise argparse.ArgumentTypeError(f"invalid E7 candidate: {value}")
    return family, build_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--bulk-dir", type=Path, default=DEFAULT_BULK)
    parser.add_argument("--candidate", action="append", type=parse_candidate, required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply-candidate-hash")
    args = parser.parse_args()

    state_dir = args.state_dir.resolve(strict=True)
    bulk_dir = args.bulk_dir.resolve(strict=True)
    candidates = sorted(set(args.candidate))
    if len(candidates) != len(args.candidate):
        raise ValueError("candidate list contains duplicates")

    bundle_ids = retained_bundle_ids(state_dir, bulk_dir)
    protected, bundles = dependency_closure(state_dir, bulk_dir, bundle_ids)
    inspected = [
        inspect_candidate(
            state_dir, bulk_dir, family, build_id, protected=protected
        )
        for family, build_id in candidates
    ]
    candidate_hash = stable_hash(
        [
            {
                "family": row["family"],
                "build_id": row["build_id"],
                "path": row["path"],
                "state_link": row["state_link"],
                "manifest_sha256": row["manifest_sha256"],
                "tree_identity_sha256": row["tree_identity_sha256"],
            }
            for row in inspected
        ]
    )
    apply = args.apply_candidate_hash is not None
    if apply and args.apply_candidate_hash != candidate_hash:
        raise ValueError(
            f"candidate hash mismatch: expected {args.apply_candidate_hash}, observed {candidate_hash}"
        )

    reclaimed = 0
    if apply:
        for row in inspected:
            link = Path(row["state_link"])
            candidate = Path(row["path"])
            link.unlink()
            shutil.rmtree(candidate)
            reclaimed += int(row["allocated_bytes"])

    report = {
        "schema_version": CONTRACT,
        "generated_at": utc_now(),
        "status": "pass",
        "mode": "apply" if apply else "dry_run",
        "reason": args.reason,
        "state_dir": str(state_dir),
        "bulk_dir": str(bulk_dir),
        "retained_public_bundle_ids": sorted(bundle_ids),
        "retained_bundle_manifests": bundles,
        "protected_compiler_artifacts": [
            {"family": family, "build_id": build_id}
            for family, build_id in sorted(protected)
        ],
        "candidates": inspected,
        "candidate_set_sha256": candidate_hash,
        "reclaimable_allocated_bytes": sum(
            int(row["allocated_bytes"]) for row in inspected
        ),
        "reclaimed_allocated_bytes": reclaimed,
    }
    write_json(args.report, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
