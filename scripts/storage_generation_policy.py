#!/usr/bin/env python3
"""Build fail-closed plans for whole Spacegate storage generations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from prune_evidence_lake_artifacts import (
    allocated_bytes,
    file_hash,
    open_processes,
    stable_hash,
    tree_identity,
)


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value


def expand_path(value: str, state_dir: Path, bulk_dir: Path) -> Path:
    expanded = value.replace("$STATE_DIR", str(state_dir)).replace(
        "$BULK_DIR", str(bulk_dir)
    )
    path = Path(expanded)
    if not path.is_absolute():
        raise ValueError(f"storage path is not absolute after expansion: {value}")
    return path


def evidence_release_members(state_dir: Path, release_set_ids: list[str]) -> set[str]:
    members: set[str] = set()
    root = state_dir / "derived/evidence_lake_v2/scientific_evidence_sets"
    for release_set_id in release_set_ids:
        manifest_path = root / release_set_id / "manifest.json"
        manifest = load_object(manifest_path)
        if manifest.get("release_set_id") != release_set_id:
            raise ValueError(f"evidence release-set identity mismatch: {manifest_path}")
        rows = manifest.get("members")
        if not isinstance(rows, list) or not rows:
            raise ValueError(f"evidence release set has no members: {manifest_path}")
        for row in rows:
            build_id = str((row or {}).get("build_id") or "")
            if not build_id:
                raise ValueError(f"evidence release set has invalid member: {manifest_path}")
            members.add(build_id)
    return members


def state_links_to(state_dir: Path, candidate: Path) -> list[Path]:
    root = state_dir / "derived/evidence_lake_v2"
    links: list[Path] = []
    if not root.is_dir():
        return links
    for link in root.rglob("*"):
        if not link.is_symlink():
            continue
        try:
            if link.resolve(strict=True) == candidate:
                links.append(link)
        except FileNotFoundError:
            continue
    return sorted(links)


def inspect_tree(
    *,
    state_dir: Path,
    label: str,
    family: str,
    candidate: Path,
    root: Path,
    reconstruct_from: str | None = None,
) -> dict[str, Any]:
    if candidate.is_symlink() or not candidate.is_dir():
        raise ValueError(f"candidate is not a real directory: {candidate}")
    resolved = candidate.resolve(strict=True)
    resolved_root = root.resolve(strict=True)
    if resolved.parent != resolved_root and resolved != resolved_root:
        raise ValueError(f"candidate escapes declared root: {candidate}")
    active = open_processes(resolved)
    if active:
        raise ValueError(f"candidate is open by live processes: {candidate}:{active}")
    identity = tree_identity(resolved)
    if any(row["kind"] == "symlink" for row in identity):
        raise ValueError(f"candidate contains symlinks: {candidate}")
    if any(
        row["kind"] == "file" and int(row["link_count"]) > 1 for row in identity
    ):
        raise ValueError(f"candidate contains shared files: {candidate}")
    manifest = resolved / "manifest.json"
    return {
        "label": label,
        "family": family,
        "path": str(resolved),
        "root": str(resolved_root),
        "allocated_bytes": allocated_bytes(resolved),
        "tree_entry_count": len(identity),
        "tree_identity_sha256": stable_hash(identity),
        "manifest_sha256": file_hash(manifest) if manifest.is_file() else None,
        "state_links": [str(path) for path in state_links_to(state_dir, resolved)],
        "reconstruct_from": reconstruct_from,
    }


def validate_policy(
    policy: dict[str, Any], state_dir: Path, bulk_dir: Path
) -> tuple[set[str], list[dict[str, Any]]]:
    if policy.get("schema_version") != "spacegate.storage_retention_roots.v1":
        raise ValueError("unsupported storage-generation policy")
    served = (state_dir / "served/current").resolve(strict=True).name
    if served != policy.get("served_build"):
        raise ValueError(
            f"served build changed: policy={policy.get('served_build')} actual={served}"
        )
    output_protected = {
        str(policy.get("served_build") or ""),
        str(policy.get("deployed_public_build") or ""),
        str(policy.get("immediate_rollback_build") or ""),
    }
    if "" in output_protected:
        raise ValueError("served/deployed/rollback roots must all be explicit")
    for name in output_protected:
        if not (state_dir / "out" / name).is_dir():
            raise ValueError(f"protected output build is missing: {name}")

    excluded = [
        expand_path(value, state_dir, bulk_dir).resolve(strict=True)
        for value in policy.get("excluded_roots", [])
    ]
    if not excluded:
        raise ValueError("storage policy must define excluded roots")
    return output_protected, [{"path": str(path)} for path in excluded]


def build_plan(
    policy: dict[str, Any],
    state_dir: Path,
    bulk_dir: Path,
    *,
    include_families: set[str] | None = None,
) -> dict[str, Any]:
    output_protected, excluded = validate_policy(policy, state_dir, bulk_dir)
    release_members = evidence_release_members(
        state_dir, list(policy.get("protected_evidence_release_sets") or [])
    )
    candidates: list[dict[str, Any]] = []
    protected: list[dict[str, Any]] = []
    known_families = {
        str(family["name"]) for family in policy.get("families") or []
    } | {"standalone_regenerable"}
    if include_families:
        unknown = include_families - known_families
        if unknown:
            raise ValueError(f"unknown storage families: {sorted(unknown)}")

    for family in policy.get("families") or []:
        name = str(family["name"])
        if include_families and name not in include_families:
            continue
        root = expand_path(str(family["root"]), state_dir, bulk_dir).resolve(
            strict=True
        )
        protect_names = set(str(value) for value in family.get("protected_names", []))
        if family.get("protected_names_from_evidence_release_sets"):
            protect_names.update(release_members)
        if name == "state_output_generations" and protect_names != output_protected:
            raise ValueError(
                "state-output family must exactly match served/deployed/rollback roots"
            )
        for child in sorted(root.iterdir()):
            if child.is_symlink() or not child.is_dir():
                continue
            if child.name in protect_names:
                protected.append(
                    {"family": name, "name": child.name, "path": str(child.resolve())}
                )
                continue
            candidates.append(
                inspect_tree(
                    state_dir=state_dir,
                    label=child.name,
                    family=name,
                    candidate=child,
                    root=root,
                )
            )
        missing = sorted(
            value for value in protect_names if not (root / value).is_dir()
        )
        if missing:
            raise ValueError(f"protected {name} generations are missing: {missing}")

    if not include_families or "standalone_regenerable" in include_families:
        for row in policy.get("standalone_candidates") or []:
            candidate = expand_path(str(row["path"]), state_dir, bulk_dir)
            candidates.append(
                inspect_tree(
                    state_dir=state_dir,
                    label=str(row["name"]),
                    family="standalone_regenerable",
                    candidate=candidate,
                    root=candidate,
                    reconstruct_from=str(row["reconstruct_from"]),
                )
            )

    candidate_paths = {row["path"] for row in candidates}
    if len(candidate_paths) != len(candidates):
        raise ValueError("storage plan contains duplicate candidate paths")
    protected_paths = {row["path"] for row in protected}
    if candidate_paths & protected_paths:
        raise ValueError("storage candidate is also protected")

    candidate_set_sha256 = stable_hash(
        [
            {
                "path": row["path"],
                "allocated_bytes": row["allocated_bytes"],
                "tree_identity_sha256": row["tree_identity_sha256"],
                "manifest_sha256": row["manifest_sha256"],
                "state_links": row["state_links"],
            }
            for row in candidates
        ]
    )
    return {
        "protected": protected,
        "excluded": excluded,
        "candidates": candidates,
        "candidate_set_sha256": candidate_set_sha256,
        "candidate_allocated_bytes": sum(
            int(row["allocated_bytes"]) for row in candidates
        ),
    }
