#!/usr/bin/env python3
"""Compose accepted immutable E4 source shards into one release-set identity."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e4_accepted_artifacts.json"
DEFAULT_CONTRACT = ROOT / "config/evidence_lake/e4_scientific_evidence.json"
DEFAULT_SCOPE = ROOT / "config/evidence_lake/e4_source_scope.json"
DEFAULT_REGISTRY = ROOT / "config/evidence_lake/source_releases.json"


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def contained_child(
    root: Path,
    child: Path,
    *,
    allowed_external_roots: tuple[Path, ...] = (),
) -> Path:
    resolved_root = root.resolve()
    if child.parent.resolve() != resolved_root:
        raise ValueError(f"artifact path is not an immediate child of {resolved_root}: {child}")
    resolved_child = child.resolve()
    allowed_parents = {resolved_root, *(path.resolve() for path in allowed_external_roots)}
    if resolved_child.parent not in allowed_parents:
        raise ValueError(f"artifact must be an immediate child of {resolved_root}: {child}")
    return resolved_child


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def atomic_symlink(target_name: str, pointer: Path) -> None:
    pointer.parent.mkdir(parents=True, exist_ok=True)
    temp = pointer.parent / f".{pointer.name}.{os.getpid()}.tmp"
    temp.unlink(missing_ok=True)
    temp.symlink_to(target_name)
    os.replace(temp, pointer)


def configured_adapter_sources(contract: dict[str, Any]) -> set[str]:
    adapters = contract.get("source_adapters")
    if not isinstance(adapters, dict) or not adapters:
        raise ValueError("scientific evidence contract has no source_adapters")
    return set(adapters)


def explicit_boundary_sources(scope: dict[str, Any]) -> set[str]:
    values = scope.get("explicit_dispositions")
    if not isinstance(values, dict):
        raise ValueError("source scope has no explicit_dispositions object")
    return set(values)


def registered_sources(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    values = registry.get("sources")
    if not isinstance(values, list):
        raise ValueError("source registry has no sources array")
    result: dict[str, dict[str, Any]] = {}
    for source in values:
        source_id = str(source.get("source_id") or "")
        if not source_id or source_id in result:
            raise ValueError(f"missing or duplicate registry source_id: {source_id!r}")
        result[source_id] = source
    return result


def compile_release_set(
    *,
    state_dir: Path,
    policy_path: Path,
    contract_path: Path,
    scope_path: Path,
    registry_path: Path,
    output_root: Path | None = None,
    artifact_bulk_roots: tuple[Path, ...] = (),
    promote: bool = True,
    verify_database_checksums: bool = False,
) -> dict[str, Any]:
    policy = load_json(policy_path)
    contract = load_json(contract_path)
    scope = load_json(scope_path)
    registry = load_json(registry_path)

    members_policy = policy.get("members")
    if not isinstance(members_policy, dict) or not members_policy:
        raise ValueError("accepted-artifact policy has no members")
    members_policy = {str(key): str(value) for key, value in members_policy.items()}

    adapter_sources = configured_adapter_sources(contract)
    boundary_sources = explicit_boundary_sources(scope)
    registry_by_id = registered_sources(registry)
    registered = set(registry_by_id)
    if adapter_sources & boundary_sources:
        raise ValueError(f"adapter/boundary source conflict: {sorted(adapter_sources & boundary_sources)}")
    if adapter_sources | boundary_sources != registered:
        raise ValueError(
            "contract/scope does not exhaust the registry: "
            f"missing={sorted(registered - adapter_sources - boundary_sources)} "
            f"extra={sorted((adapter_sources | boundary_sources) - registered)}"
        )
    if set(members_policy) != adapter_sources:
        raise ValueError(
            "accepted artifacts do not exhaust E4 adapters: "
            f"missing={sorted(adapter_sources - set(members_policy))} "
            f"extra={sorted(set(members_policy) - adapter_sources)}"
        )

    artifact_root = (
        state_dir / "derived/evidence_lake_v2/scientific_evidence"
    ).resolve()
    release_root = (
        output_root or state_dir / "derived/evidence_lake_v2/scientific_evidence_sets"
    ).resolve()

    build_to_expected_sources: dict[str, set[str]] = defaultdict(set)
    for source_id, build_id in members_policy.items():
        if not build_id or "/" in build_id or build_id.startswith("."):
            raise ValueError(f"unsafe accepted build id for {source_id}: {build_id!r}")
        build_to_expected_sources[build_id].add(source_id)

    members: list[dict[str, Any]] = []
    table_shards: dict[str, list[dict[str, Any]]] = defaultdict(list)
    total_database_bytes = 0
    source_row_total = 0
    deterministic_dates: list[str] = []

    for build_id in sorted(build_to_expected_sources):
        logical_build_dir = artifact_root / build_id
        build_dir = contained_child(
            artifact_root,
            logical_build_dir,
            allowed_external_roots=artifact_bulk_roots,
        )
        manifest_path = build_dir / "manifest.json"
        if not manifest_path.is_file():
            raise ValueError(f"accepted artifact lacks manifest: {build_id}")
        manifest = load_json(manifest_path)
        if manifest.get("build_id") != build_id:
            raise ValueError(f"artifact manifest/build mismatch: {build_id}")
        report = manifest.get("report")
        if not isinstance(report, dict) or report.get("status") != "pass":
            raise ValueError(f"accepted artifact is not pass: {build_id}")

        source_rows = report.get("sources")
        if not isinstance(source_rows, list) or not source_rows:
            raise ValueError(f"accepted artifact has no source report: {build_id}")
        actual_sources = {str(row.get("source_id") or "") for row in source_rows}
        expected_sources = build_to_expected_sources[build_id]
        if actual_sources != expected_sources:
            raise ValueError(
                f"accepted artifact source mismatch for {build_id}: "
                f"expected={sorted(expected_sources)} actual={sorted(actual_sources)}"
            )
        release_by_source = {
            str(row["source_id"]): str(row.get("release_id") or "") for row in source_rows
        }
        for source_id in actual_sources:
            registered_release = str(registry_by_id[source_id].get("release_id") or "")
            if release_by_source[source_id] != registered_release:
                raise ValueError(
                    f"accepted artifact release mismatch for {source_id}: "
                    f"registry={registered_release!r} artifact={release_by_source[source_id]!r}"
                )

        database_name = str(manifest.get("database") or "")
        database_path = build_dir / database_name
        if not database_name or database_path.parent.resolve() != build_dir or not database_path.is_file():
            raise ValueError(f"accepted artifact database is missing or unsafe: {build_id}")
        database_bytes = database_path.stat().st_size
        expected_bytes = int(manifest.get("database_bytes") or -1)
        if database_bytes != expected_bytes:
            raise ValueError(
                f"accepted artifact database size mismatch for {build_id}: "
                f"manifest={expected_bytes} actual={database_bytes}"
            )
        if verify_database_checksums:
            actual_database_sha = sha256_file(database_path)
            if actual_database_sha != manifest.get("database_sha256"):
                raise ValueError(f"accepted artifact database checksum mismatch: {build_id}")

        manifest_sha = sha256_file(manifest_path)
        table_rows = report.get("tables")
        if not isinstance(table_rows, list):
            raise ValueError(f"accepted artifact has no table report: {build_id}")
        for row in table_rows:
            row_count = int(row.get("row_count") or 0)
            if row_count <= 0:
                continue
            table_name = str(row.get("table") or "")
            table_shards[table_name].append(
                {
                    "build_id": build_id,
                    "row_count": row_count,
                    "logical_sha256": row.get("logical_sha256"),
                    "logical_hash_algorithm": row.get("logical_hash_algorithm"),
                }
            )

        created_at = str(report.get("created_at") or "")
        if created_at:
            deterministic_dates.append(created_at)
        source_row_total += sum(int(row.get("source_records") or 0) for row in source_rows)
        total_database_bytes += database_bytes
        members.append(
            {
                "build_id": build_id,
                "source_ids": sorted(actual_sources),
                "release_ids": {key: release_by_source[key] for key in sorted(actual_sources)},
                "artifact_path": str(logical_build_dir.relative_to(state_dir.resolve())),
                "manifest_sha256": manifest_sha,
                "database": database_name,
                "database_bytes": database_bytes,
                "database_sha256": manifest.get("database_sha256"),
                "logical_content_sha256": manifest.get("logical_content_sha256"),
                "scientific_content_sha256": report.get("scientific_content_sha256"),
                "compiler_version": report.get("compiler_version"),
                "contract_version": report.get("contract_version"),
            }
        )

    identity_payload = {
        "schema_version": "spacegate.scientific_evidence_release_set.v1",
        "policy_version": policy.get("policy_version"),
        "policy_sha256": sha256_file(policy_path),
        "contract_sha256": sha256_file(contract_path),
        "scope_sha256": sha256_file(scope_path),
        "registry_sha256": sha256_file(registry_path),
        "members": members,
    }
    release_set_sha256 = sha256_bytes(canonical_json(identity_payload).encode("utf-8"))
    release_set_id = release_set_sha256[:24]
    manifest = {
        **identity_payload,
        "release_set_id": release_set_id,
        "release_set_sha256": release_set_sha256,
        "created_at": max(deterministic_dates) if deterministic_dates else None,
        "adapter_source_count": len(adapter_sources),
        "boundary_source_count": len(boundary_sources),
        "artifact_count": len(members),
        "source_record_count": source_row_total,
        "total_database_bytes": total_database_bytes,
        "table_shards": {key: table_shards[key] for key in sorted(table_shards)},
        "status": "pass",
    }

    release_dir = release_root / release_set_id
    manifest_path = release_dir / "manifest.json"
    if manifest_path.exists():
        existing = load_json(manifest_path)
        if existing != manifest:
            raise ValueError(f"immutable release-set collision: {release_set_id}")
    else:
        release_dir.mkdir(parents=True, exist_ok=False)
        atomic_write_json(manifest_path, manifest)
    if promote:
        atomic_symlink(release_set_id, release_root / "current")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--scope", type=Path, default=DEFAULT_SCOPE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--artifact-bulk-root", type=Path, action="append", default=[])
    parser.add_argument("--report", type=Path)
    parser.add_argument("--no-promote", action="store_true")
    parser.add_argument("--verify-database-checksums", action="store_true")
    args = parser.parse_args()

    manifest = compile_release_set(
        state_dir=args.state_dir,
        policy_path=args.policy,
        contract_path=args.contract,
        scope_path=args.scope,
        registry_path=args.registry,
        output_root=args.output_root,
        artifact_bulk_roots=tuple(args.artifact_bulk_root),
        promote=not args.no_promote,
        verify_database_checksums=args.verify_database_checksums,
    )
    if args.report:
        atomic_write_json(args.report, manifest)
    print(
        f"E4 release set {manifest['release_set_id']} pass: "
        f"sources={manifest['adapter_source_count']} artifacts={manifest['artifact_count']} "
        f"bytes={manifest['total_database_bytes']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
