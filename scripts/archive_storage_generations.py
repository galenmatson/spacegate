#!/usr/bin/env python3
"""Copy, verify, manifest, and retire whole Spacegate storage generations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

from prune_evidence_lake_artifacts import file_hash, stable_hash, utc_now, write_json
from storage_generation_policy import (
    build_plan,
    inspect_tree,
    load_object,
    validate_policy,
)


DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_BULK = Path("/mnt/space/spacegate")
DEFAULT_POLICY = Path("config/evidence_lake/storage_retention_roots.json")
DEFAULT_ARCHIVE = Path("/mnt/proton/spacegate-archive/v1")
DEFAULT_HOST_ID = "photon"
DEFAULT_RESERVE_BYTES = 100 * 1024**3
DRY_RUN_CONTRACT = "spacegate.storage_archive_dry_run.v1"
COPY_CONTRACT = "spacegate.storage_archive_copy.v1"
RETIRE_CONTRACT = "spacegate.storage_archive_retirement.v1"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_manifest(root: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    entries: list[dict[str, Any]] = []
    logical_bytes = 0
    file_count = 0
    for child in [root, *sorted(root.rglob("*"))]:
        metadata = child.lstat()
        relative = "." if child == root else child.relative_to(root).as_posix()
        if child.is_symlink():
            raise ValueError(f"archive content contains symlink: {child}")
        if child.is_dir():
            entries.append(
                {
                    "path": relative,
                    "kind": "dir",
                    "mode": stat.S_IMODE(metadata.st_mode),
                }
            )
            continue
        if not child.is_file():
            raise ValueError(f"archive content contains unsupported entry: {child}")
        digest = sha256_file(child)
        entries.append(
            {
                "path": relative,
                "kind": "file",
                "mode": stat.S_IMODE(metadata.st_mode),
                "bytes": metadata.st_size,
                "sha256": digest,
            }
        )
        logical_bytes += metadata.st_size
        file_count += 1
    return {
        "content_sha256": stable_hash(entries),
        "entry_count": len(entries),
        "file_count": file_count,
        "logical_bytes": logical_bytes,
        "entries": entries,
    }


def archive_destination(source: Path, archive_root: Path, host_id: str) -> Path:
    source = source.resolve(strict=True)
    if not source.is_absolute():
        raise ValueError(f"archive source is not absolute: {source}")
    if not host_id or "/" in host_id or host_id in {".", ".."}:
        raise ValueError(f"invalid archive host id: {host_id}")
    destination = archive_root / "hosts" / host_id / source.relative_to("/")
    destination.relative_to(archive_root)
    return destination


def archive_mount(archive_root: Path) -> dict[str, str]:
    probe = archive_root
    while not probe.exists():
        if probe.parent == probe:
            raise ValueError(f"archive path has no existing parent: {archive_root}")
        probe = probe.parent
    result = subprocess.run(
        [
            "findmnt",
            "-T",
            str(probe),
            "-t",
            "nfs4",
            "-n",
            "-o",
            "TARGET,SOURCE,FSTYPE",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    rows = [line.split() for line in result.stdout.splitlines() if line.strip()]
    if len(rows) != 1:
        raise ValueError(f"unexpected findmnt output for {archive_root}: {result.stdout}")
    fields = rows[0]
    if len(fields) != 3:
        raise ValueError(f"unexpected findmnt output for {archive_root}: {result.stdout}")
    target, source, fstype = fields
    if fstype != "nfs4" or source != "192.168.252.2:/":
        raise ValueError(
            f"archive root is not on the dedicated proton NFS export: "
            f"target={target} source={source} fstype={fstype}"
        )
    return {"target": target, "source": source, "fstype": fstype}


def decorate_plan(
    plan: dict[str, Any], archive_root: Path, host_id: str
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for candidate in plan["candidates"]:
        if candidate["state_links"]:
            raise ValueError(
                f"initial archive migration refuses linked generation: "
                f"{candidate['path']}:{candidate['state_links']}"
            )
        source = Path(candidate["path"])
        destination = archive_destination(source, archive_root, host_id)
        rows.append({**candidate, "archive_path": str(destination)})
    candidate_set_sha256 = stable_hash(
        [
            {
                "path": row["path"],
                "archive_path": row["archive_path"],
                "allocated_bytes": row["allocated_bytes"],
                "tree_identity_sha256": row["tree_identity_sha256"],
                "manifest_sha256": row["manifest_sha256"],
            }
            for row in rows
        ]
    )
    return {
        **plan,
        "candidates": rows,
        "candidate_set_sha256": candidate_set_sha256,
    }


def report_base(
    *,
    contract: str,
    mode: str,
    reason: str,
    policy_path: Path,
    policy: dict[str, Any],
    state_dir: Path,
    bulk_dir: Path,
    archive_root: Path,
    host_id: str,
    families: set[str],
    mount: dict[str, str],
    plan: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": contract,
        "generated_at": utc_now(),
        "status": "pass",
        "mode": mode,
        "reason": reason,
        "policy_path": str(policy_path),
        "policy_sha256": file_hash(policy_path),
        "policy_version": policy["policy_version"],
        "state_dir": str(state_dir),
        "bulk_dir": str(bulk_dir),
        "archive_root": str(archive_root),
        "archive_host_id": host_id,
        "archive_mount": mount,
        "families": sorted(families),
        **plan,
    }


def run_rsync(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "rsync",
            "-a",
            "--no-owner",
            "--no-group",
            "--numeric-ids",
            "--delete",
            "--partial",
            "--sparse",
            "--",
            f"{source}/",
            f"{destination}/",
        ],
        check=True,
    )


def copy_plan(
    *,
    plan: dict[str, Any],
    state_dir: Path,
    archive_root: Path,
    report: dict[str, Any],
    report_path: Path,
) -> dict[str, Any]:
    manifest_root = archive_root / "manifests" / plan["candidate_set_sha256"]
    copied: list[dict[str, Any]] = []
    for candidate in plan["candidates"]:
        source = Path(candidate["path"])
        destination = Path(candidate["archive_path"])
        staging = destination.with_name(
            f".{destination.name}.partial-{plan['candidate_set_sha256'][:16]}"
        )
        print(f"hashing source: {source}", file=sys.stderr, flush=True)
        source_content = content_manifest(source)
        if destination.exists():
            print(
                f"verifying existing archive: {destination}",
                file=sys.stderr,
                flush=True,
            )
            archived_content = content_manifest(destination)
        else:
            print(f"copying: {source} -> {staging}", file=sys.stderr, flush=True)
            run_rsync(source, staging)
            current = inspect_tree(
                state_dir=state_dir,
                label=str(candidate["label"]),
                family=str(candidate["family"]),
                candidate=source,
                root=Path(candidate["root"]),
                reconstruct_from=candidate.get("reconstruct_from"),
            )
            if current["tree_identity_sha256"] != candidate["tree_identity_sha256"]:
                raise ValueError(f"source changed while copying: {source}")
            print(f"hashing archive: {staging}", file=sys.stderr, flush=True)
            archived_content = content_manifest(staging)
            if archived_content["content_sha256"] != source_content["content_sha256"]:
                raise ValueError(f"archive checksum mismatch: {source} -> {staging}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging, destination)
        if archived_content["content_sha256"] != source_content["content_sha256"]:
            raise ValueError(f"existing archive checksum mismatch: {destination}")

        candidate_manifest = {
            "schema_version": "spacegate.storage_archive_candidate.v1",
            "generated_at": utc_now(),
            "candidate_set_sha256": plan["candidate_set_sha256"],
            "family": candidate["family"],
            "label": candidate["label"],
            "source_path": str(source),
            "archive_path": str(destination),
            "source_tree_identity_sha256": candidate["tree_identity_sha256"],
            "source_manifest_sha256": candidate["manifest_sha256"],
            **archived_content,
        }
        candidate_manifest_path = (
            manifest_root / str(candidate["family"]) / f"{candidate['label']}.json"
        )
        write_json(candidate_manifest_path, candidate_manifest)
        print(
            f"verified candidate: {candidate['family']}/{candidate['label']}",
            file=sys.stderr,
            flush=True,
        )
        copied.append(
            {
                **candidate,
                "content_sha256": archived_content["content_sha256"],
                "logical_bytes": archived_content["logical_bytes"],
                "file_count": archived_content["file_count"],
                "archive_manifest_path": str(candidate_manifest_path),
                "archive_manifest_sha256": file_hash(candidate_manifest_path),
            }
        )
        progress = {
            **report,
            "status": "running",
            "candidates": copied,
            "copied_candidate_count": len(copied),
        }
        write_json(report_path, progress)

    complete = {
        **report,
        "status": "pass",
        "candidates": copied,
        "copied_candidate_count": len(copied),
        "verified_logical_bytes": sum(int(row["logical_bytes"]) for row in copied),
    }
    write_json(report_path, complete)
    write_json(manifest_root / "copy_report.json", complete)
    return complete


def retire_copy(
    *,
    copy_report_path: Path,
    expected_hash: str,
    policy_path: Path,
    policy: dict[str, Any],
    state_dir: Path,
    bulk_dir: Path,
    report_path: Path,
    reason: str,
) -> dict[str, Any]:
    copied = load_object(copy_report_path)
    if copied.get("schema_version") != COPY_CONTRACT or copied.get("status") != "pass":
        raise ValueError("retirement requires a passing archive copy report")
    if copied.get("candidate_set_sha256") != expected_hash:
        raise ValueError("archive copy report candidate hash mismatch")
    if copied.get("policy_sha256") != file_hash(policy_path):
        raise ValueError("archive policy changed after copy verification")
    validate_policy(policy, state_dir, bulk_dir)
    protected = {
        str(policy["served_build"]),
        str(policy["deployed_public_build"]),
        str(policy["immediate_rollback_build"]),
    }

    journal: dict[str, Any]
    if report_path.exists():
        journal = load_object(report_path)
        if journal.get("candidate_set_sha256") != expected_hash:
            raise ValueError("retirement journal candidate hash mismatch")
    else:
        journal = {
            "schema_version": RETIRE_CONTRACT,
            "generated_at": utc_now(),
            "status": "running",
            "mode": "retire",
            "reason": reason,
            "copy_report_path": str(copy_report_path),
            "copy_report_sha256": file_hash(copy_report_path),
            "candidate_set_sha256": expected_hash,
            "policy_path": str(policy_path),
            "policy_sha256": file_hash(policy_path),
            "candidates": [
                {
                    "family": row["family"],
                    "label": row["label"],
                    "source_path": row["path"],
                    "archive_path": row["archive_path"],
                    "content_sha256": row["content_sha256"],
                    "source_tree_identity_sha256": row["tree_identity_sha256"],
                    "status": "pending",
                }
                for row in copied["candidates"]
            ],
        }
        write_json(report_path, journal)

    copy_by_source = {str(row["path"]): row for row in copied["candidates"]}
    for row in journal["candidates"]:
        source = Path(row["source_path"])
        archived = Path(row["archive_path"])
        copied_row = copy_by_source[str(source)]
        if source.name in protected:
            raise ValueError(f"retirement candidate became protected: {source}")
        archived_content = content_manifest(archived)
        if archived_content["content_sha256"] != row["content_sha256"]:
            raise ValueError(f"archive changed before retirement: {archived}")
        if source.exists():
            current = inspect_tree(
                state_dir=state_dir,
                label=str(copied_row["label"]),
                family=str(copied_row["family"]),
                candidate=source,
                root=Path(copied_row["root"]),
                reconstruct_from=copied_row.get("reconstruct_from"),
            )
            if current["tree_identity_sha256"] != row["source_tree_identity_sha256"]:
                raise ValueError(f"source changed before retirement: {source}")
            if current["state_links"]:
                raise ValueError(f"source acquired state links before retirement: {source}")
        elif row["status"] not in {"archive_verified", "retired"}:
            raise ValueError(f"source disappeared before archive verification: {source}")
        row["status"] = "archive_verified"
        row["archive_verified_at"] = utc_now()
        write_json(report_path, journal)

    retired_bytes = 0
    for row in journal["candidates"]:
        source = Path(row["source_path"])
        copied_row = copy_by_source[str(source)]
        if source.exists():
            shutil.rmtree(source)
            retired_bytes += int(copied_row["allocated_bytes"])
        row["status"] = "retired"
        row["retired_at"] = utc_now()
        write_json(report_path, journal)

    journal["status"] = "pass"
    journal["completed_at"] = utc_now()
    journal["retired_candidate_count"] = len(journal["candidates"])
    journal["retired_allocated_bytes"] = retired_bytes
    write_json(report_path, journal)
    archive_report = (
        Path(copied["archive_root"])
        / "manifests"
        / expected_hash
        / "retirement_report.json"
    )
    write_json(archive_report, journal)
    return journal


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--bulk-dir", type=Path, default=DEFAULT_BULK)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--host-id", default=DEFAULT_HOST_ID)
    parser.add_argument("--family", action="append", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--expected-candidate-set-sha256")
    parser.add_argument("--copy", action="store_true")
    parser.add_argument("--retire-copy-report", type=Path)
    parser.add_argument("--minimum-free-after-bytes", type=int, default=DEFAULT_RESERVE_BYTES)
    args = parser.parse_args()

    state_dir = args.state_dir.resolve(strict=True)
    bulk_dir = args.bulk_dir.resolve(strict=True)
    policy_path = args.policy.resolve(strict=True)
    archive_root = args.archive_root.absolute()
    policy = load_object(policy_path)
    families = set(args.family)
    mount = archive_mount(archive_root)

    if args.retire_copy_report:
        if not args.expected_candidate_set_sha256:
            raise ValueError("retirement requires --expected-candidate-set-sha256")
        result = retire_copy(
            copy_report_path=args.retire_copy_report.resolve(strict=True),
            expected_hash=args.expected_candidate_set_sha256,
            policy_path=policy_path,
            policy=policy,
            state_dir=state_dir,
            bulk_dir=bulk_dir,
            report_path=args.report.absolute(),
            reason=args.reason,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    plan = decorate_plan(
        build_plan(
            policy,
            state_dir,
            bulk_dir,
            include_families=families,
        ),
        archive_root,
        args.host_id,
    )
    free_bytes = shutil.disk_usage(Path(mount["target"])).free
    free_after = free_bytes - int(plan["candidate_allocated_bytes"])
    if free_after < args.minimum_free_after_bytes:
        raise ValueError(
            f"archive capacity gate failed: free_after={free_after}, "
            f"minimum={args.minimum_free_after_bytes}"
        )
    mode = "copy" if args.copy else "dry_run"
    report = report_base(
        contract=COPY_CONTRACT if args.copy else DRY_RUN_CONTRACT,
        mode=mode,
        reason=args.reason,
        policy_path=policy_path,
        policy=policy,
        state_dir=state_dir,
        bulk_dir=bulk_dir,
        archive_root=archive_root,
        host_id=args.host_id,
        families=families,
        mount=mount,
        plan=plan,
    )
    report["archive_free_bytes_before"] = free_bytes
    report["archive_free_bytes_after_estimate"] = free_after
    if args.copy:
        if args.expected_candidate_set_sha256 != plan["candidate_set_sha256"]:
            raise ValueError(
                f"candidate hash mismatch: expected="
                f"{args.expected_candidate_set_sha256}, "
                f"observed={plan['candidate_set_sha256']}"
            )
        archive_root.mkdir(parents=True, exist_ok=True)
        result = copy_plan(
            plan=plan,
            state_dir=state_dir,
            archive_root=archive_root,
            report=report,
            report_path=args.report.absolute(),
        )
    else:
        result = report
        write_json(args.report.absolute(), result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
