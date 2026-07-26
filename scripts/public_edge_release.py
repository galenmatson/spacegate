#!/usr/bin/env python3
"""Build, stage, verify, activate, and roll back a public edge release."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


SCHEMA_VERSION = "spacegate.public_edge_release.v1"
INSTALL_MARKER_SCHEMA = "spacegate.public_edge_install_marker.v1"
ACTIVATION_SCHEMA = "spacegate.public_edge_activation.v1"
BUILD_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,159}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
MINIMUM_FREE_BYTES = 15 * 1024**3


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_build_id(value: str) -> str:
    if BUILD_ID_RE.fullmatch(value) is None:
        raise ValueError(f"unsafe build id: {value!r}")
    return value


def validate_sha256(value: Any, label: str) -> str:
    normalized = str(value or "").lower()
    if SHA256_RE.fullmatch(normalized) is None:
        raise ValueError(f"invalid sha256 for {label}")
    return normalized


def bounded_path(root: Path, candidate: Path, *, must_exist: bool = False) -> Path:
    root = root.resolve(strict=True)
    resolved = candidate.resolve(strict=must_exist)
    if not resolved.is_relative_to(root):
        raise ValueError(f"path escapes bounded root {root}: {candidate}")
    return resolved


def artifact_ref(path: Path, transfer_filename: str) -> dict[str, Any]:
    source = path.resolve(strict=True)
    if not source.is_file():
        raise ValueError(f"artifact is not a regular file: {source}")
    if PurePosixPath(transfer_filename).name != transfer_filename:
        raise ValueError(f"unsafe transfer filename: {transfer_filename}")
    return {
        "source_path": str(source),
        "transfer_filename": transfer_filename,
        "bytes": source.stat().st_size,
        "sha256": sha256_file(source),
    }


def installed_file_ref(path: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError(f"required installed file is missing: {resolved}")
    return {
        "bytes": resolved.stat().st_size,
        "sha256": sha256_file(resolved),
    }


def apparent_size(root: Path) -> int:
    return sum(
        path.stat().st_size
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    )


def validate_release(value: dict[str, Any]) -> dict[str, Any]:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("incompatible public edge release schema")
    build_id = validate_build_id(str(value.get("build_id") or ""))
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("release artifacts must be an object")
    for role in ("scientific_build", "public_read", "simulation_scenes"):
        artifact = artifacts.get(role)
        if not isinstance(artifact, dict):
            raise ValueError(f"release is missing artifact role: {role}")
        filename = str(artifact.get("transfer_filename") or "")
        if PurePosixPath(filename).name != filename or not filename:
            raise ValueError(f"unsafe transfer filename for {role}")
        if not isinstance(artifact.get("bytes"), int) or artifact["bytes"] < 1:
            raise ValueError(f"invalid byte count for {role}")
        validate_sha256(artifact.get("sha256"), role)
    public_manifest = value.get("public_read_manifest")
    if (
        not isinstance(public_manifest, dict)
        or public_manifest.get("build_id") != build_id
        or public_manifest.get("status") != "pass"
        or public_manifest.get("sample_limit") is not None
        or public_manifest.get("projection_schema_version")
        != "spacegate.public_read.v2"
        or public_manifest.get("search_schema_version") != "spacegate.search.v2"
    ):
        raise ValueError("public-read manifest is incompatible with release")
    public_artifact = public_manifest.get("artifact") or {}
    if (
        public_artifact.get("bytes") != artifacts["public_read"]["bytes"]
        or str(public_artifact.get("sha256") or "").lower()
        != artifacts["public_read"]["sha256"]
    ):
        raise ValueError("public-read artifact and embedded manifest disagree")
    scenes = value.get("simulation_scene_manifest")
    if (
        not isinstance(scenes, dict)
        or scenes.get("build_id") != build_id
        or (scenes.get("verification") or {}).get("status") != "pass"
        or scenes.get("required_count") != scenes.get("scene_count")
    ):
        raise ValueError("simulation-scene manifest is incompatible with release")
    scene_archive = scenes.get("archive") or {}
    if (
        scene_archive.get("bytes") != artifacts["simulation_scenes"]["bytes"]
        or str(scene_archive.get("sha256") or "").lower()
        != artifacts["simulation_scenes"]["sha256"]
    ):
        raise ValueError("scene artifact and embedded manifest disagree")
    scientific = value.get("scientific_build") or {}
    required_files = scientific.get("required_files")
    if not isinstance(required_files, dict) or not required_files:
        raise ValueError("scientific build has no required-file contract")
    for relative, spec in required_files.items():
        path = PurePosixPath(str(relative))
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError(f"unsafe required scientific path: {relative!r}")
        if (
            not isinstance(spec, dict)
            or not isinstance(spec.get("bytes"), int)
            or spec["bytes"] < 1
        ):
            raise ValueError(f"invalid required scientific file: {relative!r}")
        validate_sha256(spec.get("sha256"), f"scientific_build:{relative}")
    return value


def command_create(args: argparse.Namespace) -> dict[str, Any]:
    build_id = validate_build_id(args.build_id)
    build_dir = args.build_dir.resolve(strict=True)
    if build_dir.name != build_id or not (build_dir / "core.duckdb").is_file():
        raise ValueError("scientific build directory does not match build id")
    public_manifest = load_json(args.public_read_manifest.resolve(strict=True))
    scene_manifest = load_json(args.simulation_scene_manifest.resolve(strict=True))
    scientific = artifact_ref(
        args.scientific_archive, f"{build_id}.scientific.7z"
    )
    public_read = artifact_ref(args.public_read, "public_read.sqlite")
    scenes = artifact_ref(args.simulation_scenes, "simulation_scenes.tar.gz")
    scientific_apparent_bytes = apparent_size(build_dir)
    scene_payload_bytes = int(scene_manifest.get("total_scene_bytes") or 0)
    minimum_start_free_bytes = max(
        scientific["bytes"] + scientific_apparent_bytes + MINIMUM_FREE_BYTES,
        scientific_apparent_bytes
        + public_read["bytes"]
        + scenes["bytes"]
        + scene_payload_bytes
        + MINIMUM_FREE_BYTES,
    )
    required_paths = (
        "core.duckdb",
        "arm.duckdb",
        "canonical_hierarchy.duckdb",
        "disc.duckdb",
        "map_tiles/index.json",
    )
    release = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "build_id": build_id,
        "artifacts": {
            "scientific_build": scientific,
            "public_read": public_read,
            "simulation_scenes": scenes,
        },
        "scientific_build": {
            "apparent_bytes": scientific_apparent_bytes,
            "required_files": {
                relative: installed_file_ref(build_dir / relative)
                for relative in required_paths
            },
        },
        "public_read_manifest": public_manifest,
        "simulation_scene_manifest": scene_manifest,
        "runtime_contract": {
            "duckdb_memory_limit": "5GB",
            "duckdb_threads": 1,
            "db_pool_size": 6,
            "db_acquire_timeout_seconds": 30,
            "public_read_compatibility_fallback": False,
        },
        "transfer": {
            "sequence": [
                "scientific_build",
                "remove_scientific_transfer_file",
                "public_read",
                "simulation_scenes",
            ],
            "total_bytes": sum(
                row["bytes"] for row in (scientific, public_read, scenes)
            ),
            "minimum_free_bytes_after_stage": MINIMUM_FREE_BYTES,
            "minimum_start_free_bytes": minimum_start_free_bytes,
        },
    }
    validate_release(release)
    atomic_json(args.output.resolve(), release)
    return {
        "status": "pass",
        "manifest": str(args.output.resolve()),
        "build_id": build_id,
        "transfer_bytes": release["transfer"]["total_bytes"],
    }


def verify_artifact(path: Path, spec: dict[str, Any], role: str) -> None:
    if not path.is_file():
        raise ValueError(f"missing {role} artifact: {path}")
    actual_bytes = path.stat().st_size
    if actual_bytes != spec["bytes"]:
        raise ValueError(
            f"{role} byte mismatch: expected {spec['bytes']}, got {actual_bytes}"
        )
    actual_sha = sha256_file(path)
    if actual_sha != spec["sha256"]:
        raise ValueError(
            f"{role} sha256 mismatch: expected {spec['sha256']}, got {actual_sha}"
        )


def source_path(release: dict[str, Any], role: str) -> Path:
    value = str(release["artifacts"][role].get("source_path") or "")
    if not value:
        raise ValueError(f"release omits local source path for {role}")
    return Path(value).resolve(strict=True)


def command_verify_source(args: argparse.Namespace) -> dict[str, Any]:
    release = validate_release(load_json(args.manifest.resolve(strict=True)))
    for role, spec in release["artifacts"].items():
        verify_artifact(source_path(release, role), spec, role)
    return {
        "status": "pass",
        "build_id": release["build_id"],
        "verified_roles": sorted(release["artifacts"]),
    }


def incoming_path(
    release: dict[str, Any], role: str, incoming_dir: Path
) -> Path:
    root = incoming_dir.resolve(strict=True)
    filename = release["artifacts"][role]["transfer_filename"]
    return bounded_path(root, root / filename)


def list_7z_members(archive: Path) -> list[PurePosixPath]:
    completed = subprocess.run(
        ["7z", "l", "-slt", str(archive)],
        check=True,
        capture_output=True,
        text=True,
    )
    members: list[PurePosixPath] = []
    for line in completed.stdout.splitlines():
        if not line.startswith("Path = "):
            continue
        raw = line.removeprefix("Path = ").strip().replace("\\", "/")
        if raw == str(archive):
            continue
        path = PurePosixPath(raw)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError(f"unsafe scientific archive member: {raw!r}")
        members.append(path)
    if not members:
        raise ValueError("scientific archive contains no members")
    return members


def reject_extracted_links(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"extracted artifact contains a symbolic link: {path}")
        bounded_path(root, path, must_exist=True)


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def require_reserve(path: Path, reserve: int = MINIMUM_FREE_BYTES) -> None:
    available = free_bytes(path)
    if available < reserve:
        raise ValueError(
            f"free-space reserve violated at {path}: "
            f"available={available} required={reserve}"
        )


def command_stage_scientific(args: argparse.Namespace) -> dict[str, Any]:
    release = validate_release(load_json(args.manifest.resolve(strict=True)))
    build_id = release["build_id"]
    state = args.state_dir.resolve(strict=True)
    incoming = incoming_path(
        release, "scientific_build", args.incoming_dir.resolve(strict=True)
    )
    verify_artifact(incoming, release["artifacts"]["scientific_build"], "scientific_build")
    members = list_7z_members(incoming)
    if any(member.parts[0] != build_id for member in members):
        raise ValueError("scientific archive has content outside its build root")
    target = state / "out" / build_id
    marker = target / ".public_edge_release.json"
    if target.exists():
        if not marker.is_file():
            raise ValueError(f"unmanaged scientific build target already exists: {target}")
        installed = load_json(marker)
        if (
            installed.get("build_id") != build_id
            or installed.get("artifact_sha256")
            != release["artifacts"]["scientific_build"]["sha256"]
        ):
            raise ValueError("existing scientific build marker does not match release")
        return {"status": "pass", "stage": "scientific_build", "reused": True}
    (state / "out").mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(prefix=f".{build_id}.", dir=state / "out")
    )
    try:
        subprocess.run(
            ["7z", "x", "-y", f"-o{staging_root}", str(incoming)],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        extracted = staging_root / build_id
        reject_extracted_links(extracted)
        for relative, spec in release["scientific_build"]["required_files"].items():
            verify_artifact(
                extracted / relative, spec, f"scientific_build:{relative}"
            )
        atomic_json(
            extracted / ".public_edge_release.json",
            {
                "schema_version": INSTALL_MARKER_SCHEMA,
                "build_id": build_id,
                "artifact_sha256": release["artifacts"]["scientific_build"]["sha256"],
                "installed_at_utc": utc_now(),
            },
        )
        os.replace(extracted, target)
        require_reserve(state)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
    return {"status": "pass", "stage": "scientific_build", "reused": False}


def read_public_build_id(path: Path) -> str:
    uri = f"file:{path.resolve()}?mode=ro&immutable=1"
    con = sqlite3.connect(uri, uri=True)
    try:
        row = con.execute(
            "SELECT value FROM metadata WHERE key='build_id'"
        ).fetchone()
        integrity = con.execute("PRAGMA quick_check").fetchone()
    finally:
        con.close()
    if row is None or integrity is None or integrity[0] != "ok":
        raise ValueError("public-read SQLite metadata or quick-check failed")
    return str(row[0])


def command_stage_public_read(args: argparse.Namespace) -> dict[str, Any]:
    release = validate_release(load_json(args.manifest.resolve(strict=True)))
    build_id = release["build_id"]
    state = args.state_dir.resolve(strict=True)
    incoming = incoming_path(
        release, "public_read", args.incoming_dir.resolve(strict=True)
    )
    verify_artifact(incoming, release["artifacts"]["public_read"], "public_read")
    if read_public_build_id(incoming) != build_id:
        raise ValueError("public-read SQLite build identity mismatch")
    target_dir = state / "derived" / "public_read" / build_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "public_read.sqlite"
    if target.is_file():
        verify_artifact(target, release["artifacts"]["public_read"], "public_read")
        reused = True
        incoming.unlink(missing_ok=True)
    else:
        os.replace(incoming, target)
        reused = False
    atomic_json(target_dir / "manifest.json", release["public_read_manifest"])
    require_reserve(state)
    return {"status": "pass", "stage": "public_read", "reused": reused}


def safe_scene_member(member: tarfile.TarInfo) -> PurePosixPath:
    path = PurePosixPath(member.name)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError(f"unsafe scene archive member: {member.name!r}")
    if not member.isfile():
        raise ValueError(f"unsupported scene archive member type: {member.name!r}")
    if path != PurePosixPath("manifest.json") and not re.fullmatch(
        r"scenes/system_[0-9]+\.json\.gz", path.as_posix()
    ):
        raise ValueError(f"unexpected scene archive member: {member.name!r}")
    return path


def command_stage_scenes(args: argparse.Namespace) -> dict[str, Any]:
    release = validate_release(load_json(args.manifest.resolve(strict=True)))
    build_id = release["build_id"]
    state = args.state_dir.resolve(strict=True)
    incoming = incoming_path(
        release, "simulation_scenes", args.incoming_dir.resolve(strict=True)
    )
    verify_artifact(
        incoming, release["artifacts"]["simulation_scenes"], "simulation_scenes"
    )
    cache_root = state / "cache" / "simulation_scenes"
    cache_root.mkdir(parents=True, exist_ok=True)
    target = cache_root / build_id
    expected_entries = {
        str(row["path"]): row
        for row in release["simulation_scene_manifest"].get("entries") or []
    }
    installed = 0
    reused = target.exists()
    if reused:
        installed_manifest = load_json(target / "manifest.json")
        installed = sum(1 for _ in target.glob("system_*.json.gz"))
        if (
            installed_manifest.get("build_id") != build_id
            or installed != release["simulation_scene_manifest"]["scene_count"]
        ):
            raise ValueError("existing scene cache does not match release")
    else:
        staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=cache_root))
        try:
            with tarfile.open(incoming, mode="r:gz") as archive:
                members = archive.getmembers()
                paths = [safe_scene_member(member) for member in members]
                if len(paths) != len(set(paths)):
                    raise ValueError("scene archive contains duplicate members")
                embedded = archive.extractfile("manifest.json")
                if embedded is None:
                    raise ValueError("scene archive lacks embedded manifest")
                embedded_manifest = json.loads(embedded.read())
                if (
                    embedded_manifest.get("build_id") != build_id
                    or embedded_manifest.get("scene_count")
                    != release["simulation_scene_manifest"]["scene_count"]
                ):
                    raise ValueError("embedded scene manifest does not match release")
                for member in members:
                    path = safe_scene_member(member)
                    if path == PurePosixPath("manifest.json"):
                        continue
                    source = archive.extractfile(member)
                    if source is None:
                        raise ValueError(f"unable to read scene member: {member.name}")
                    content = source.read()
                    spec = expected_entries.get(path.as_posix())
                    if (
                        spec is None
                        or len(content) != spec["bytes"]
                        or hashlib.sha256(content).hexdigest() != spec["sha256"]
                    ):
                        raise ValueError(
                            f"scene member verification failed: {member.name}"
                        )
                    destination = staging / path.name
                    destination.write_bytes(content)
                    installed += 1
            if installed != release["simulation_scene_manifest"]["scene_count"]:
                raise ValueError("installed scene count does not match release")
            atomic_json(
                staging / "manifest.json", release["simulation_scene_manifest"]
            )
            os.replace(staging, target)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
    frozen = state / "derived" / "simulation_scenes" / build_id
    frozen.mkdir(parents=True, exist_ok=True)
    archive_target = frozen / "simulation_scenes.tar.gz"
    if archive_target.is_file():
        verify_artifact(
            archive_target,
            release["artifacts"]["simulation_scenes"],
            "simulation_scenes",
        )
        incoming.unlink(missing_ok=True)
    else:
        os.replace(incoming, archive_target)
    atomic_json(frozen / "manifest.json", release["simulation_scene_manifest"])
    require_reserve(state)
    return {
        "status": "pass",
        "stage": "simulation_scenes",
        "scene_count": installed,
        "reused": reused,
    }


def verify_installed(release: dict[str, Any], state: Path) -> dict[str, Any]:
    build_id = release["build_id"]
    build = state / "out" / build_id
    marker = load_json(build / ".public_edge_release.json")
    if (
        marker.get("schema_version") != INSTALL_MARKER_SCHEMA
        or marker.get("build_id") != build_id
        or marker.get("artifact_sha256")
        != release["artifacts"]["scientific_build"]["sha256"]
    ):
        raise ValueError("scientific build install marker mismatch")
    for relative, spec in release["scientific_build"]["required_files"].items():
        verify_artifact(build / relative, spec, f"scientific_build:{relative}")
    public = state / "derived" / "public_read" / build_id / "public_read.sqlite"
    verify_artifact(public, release["artifacts"]["public_read"], "public_read")
    if read_public_build_id(public) != build_id:
        raise ValueError("installed public-read build identity mismatch")
    public_manifest = load_json(public.parent / "manifest.json")
    if canonical_json(public_manifest) != canonical_json(
        release["public_read_manifest"]
    ):
        raise ValueError("installed public-read manifest mismatch")
    frozen = (
        state
        / "derived"
        / "simulation_scenes"
        / build_id
        / "simulation_scenes.tar.gz"
    )
    verify_artifact(
        frozen, release["artifacts"]["simulation_scenes"], "simulation_scenes"
    )
    scene_cache = state / "cache" / "simulation_scenes" / build_id
    manifest = load_json(scene_cache / "manifest.json")
    entries = release["simulation_scene_manifest"].get("entries") or []
    count = 0
    for entry in entries:
        relative = PurePosixPath(str(entry.get("path") or ""))
        if not re.fullmatch(r"scenes/system_[0-9]+\.json\.gz", relative.as_posix()):
            raise ValueError("release scene manifest contains an unsafe entry")
        verify_artifact(
            scene_cache / relative.name, entry, f"scene:{relative.name}"
        )
        count += 1
    extra_count = sum(1 for _ in scene_cache.glob("system_*.json.gz")) - count
    if (
        manifest.get("build_id") != build_id
        or count != release["simulation_scene_manifest"]["scene_count"]
        or extra_count != 0
    ):
        raise ValueError("installed simulation scene cache mismatch")
    return {
        "status": "pass",
        "build_id": build_id,
        "scene_count": count,
        "free_bytes": free_bytes(state),
    }


def command_verify_installed(args: argparse.Namespace) -> dict[str, Any]:
    release = validate_release(load_json(args.manifest.resolve(strict=True)))
    return verify_installed(release, args.state_dir.resolve(strict=True))


def atomic_symlink(link: Path, target: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    relative = os.path.relpath(target, link.parent)
    temporary = link.with_name(f".{link.name}.tmp.{os.getpid()}")
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(relative)
    os.replace(temporary, link)


def command_activate(args: argparse.Namespace) -> dict[str, Any]:
    release = validate_release(load_json(args.manifest.resolve(strict=True)))
    state = args.state_dir.resolve(strict=True)
    verified = verify_installed(release, state)
    link = state / "served" / "current"
    previous = str(link.resolve(strict=True)) if link.exists() else None
    target = state / "out" / release["build_id"]
    atomic_symlink(link, target)
    activation = {
        "schema_version": ACTIVATION_SCHEMA,
        "activated_at_utc": utc_now(),
        "build_id": release["build_id"],
        "target": str(target),
        "previous_target": previous,
        "release_manifest_sha256": sha256_file(args.manifest.resolve(strict=True)),
    }
    atomic_json(
        state / "deployments" / release["build_id"] / "activation.json",
        activation,
    )
    return {**verified, "activated": True, "previous_target": previous}


def command_rollback(args: argparse.Namespace) -> dict[str, Any]:
    build_id = validate_build_id(args.build_id)
    state = args.state_dir.resolve(strict=True)
    activation_path = state / "deployments" / build_id / "activation.json"
    activation = load_json(activation_path)
    previous_raw = activation.get("previous_target")
    if activation.get("schema_version") != ACTIVATION_SCHEMA or not previous_raw:
        raise ValueError("activation record has no rollback target")
    previous = bounded_path(state / "out", Path(str(previous_raw)), must_exist=True)
    if not (previous / "core.duckdb").is_file():
        raise ValueError("rollback target is not a usable build")
    atomic_symlink(state / "served" / "current", previous)
    rollback = {
        "schema_version": "spacegate.public_edge_rollback.v1",
        "rolled_back_at_utc": utc_now(),
        "from_build_id": build_id,
        "target": str(previous),
    }
    atomic_json(
        state / "deployments" / build_id / "rollback.json", rollback
    )
    return {"status": "pass", **rollback}


def dotenv_values(paths: list[Path]) -> dict[str, str]:
    values: dict[str, str] = {}
    for path in paths:
        if not path.is_file():
            continue
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip("\"'")
    return values


def expected_runtime_env(release: dict[str, Any]) -> dict[str, str]:
    runtime = release["runtime_contract"]
    return {
        "SPACEGATE_API_DUCKDB_MEMORY_LIMIT": str(runtime["duckdb_memory_limit"]),
        "SPACEGATE_API_DUCKDB_THREADS": str(runtime["duckdb_threads"]),
        "SPACEGATE_API_DB_POOL_SIZE": str(runtime["db_pool_size"]),
        "SPACEGATE_API_DB_ACQUIRE_TIMEOUT_SECONDS": str(
            runtime["db_acquire_timeout_seconds"]
        ),
    }


def command_verify_runtime_env(args: argparse.Namespace) -> dict[str, Any]:
    release = validate_release(load_json(args.manifest.resolve(strict=True)))
    paths = [path.resolve() for path in args.env_file]
    values = dotenv_values(paths)
    expected = expected_runtime_env(release)
    mismatches = {
        key: {"expected": wanted, "actual": values.get(key, "<unset>")}
        for key, wanted in expected.items()
        if values.get(key) != wanted
    }
    fallback = values.get("SPACEGATE_PUBLIC_READ_COMPATIBILITY_FALLBACK", "")
    if fallback.lower() in {"1", "true", "yes"}:
        mismatches["SPACEGATE_PUBLIC_READ_COMPATIBILITY_FALLBACK"] = {
            "expected": "unset or disabled",
            "actual": "enabled",
        }
    if mismatches:
        raise ValueError(f"runtime environment mismatch: {mismatches}")
    return {
        "status": "pass",
        "build_id": release["build_id"],
        "checked_keys": sorted(expected),
        "compatibility_fallback": "disabled",
    }


def command_configure_runtime_env(args: argparse.Namespace) -> dict[str, Any]:
    release = validate_release(load_json(args.manifest.resolve(strict=True)))
    path = args.env_file.resolve()
    expected = expected_runtime_env(release)
    expected["SPACEGATE_PUBLIC_READ_COMPATIBILITY_FALLBACK"] = "0"
    original = path.read_text(encoding="utf-8") if path.is_file() else ""
    retained: list[str] = []
    managed = set(expected)
    for raw in original.splitlines():
        stripped = raw.strip()
        key = stripped.split("=", 1)[0].strip() if "=" in stripped else ""
        if key not in managed:
            retained.append(raw)
    if retained and retained[-1] != "":
        retained.append("")
    retained.append("# M8.3e measured public edge runtime contract")
    retained.extend(f"{key}={value}" for key, value in expected.items())
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text("\n".join(retained) + "\n", encoding="utf-8")
    if path.exists():
        temporary.chmod(path.stat().st_mode & 0o777)
    else:
        temporary.chmod(0o600)
    os.replace(temporary, path)
    return {
        "status": "pass",
        "build_id": release["build_id"],
        "updated_file": str(path),
        "managed_keys": sorted(expected),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create")
    create.add_argument("--build-id", required=True)
    create.add_argument("--build-dir", required=True, type=Path)
    create.add_argument("--scientific-archive", required=True, type=Path)
    create.add_argument("--public-read", required=True, type=Path)
    create.add_argument("--public-read-manifest", required=True, type=Path)
    create.add_argument("--simulation-scenes", required=True, type=Path)
    create.add_argument("--simulation-scene-manifest", required=True, type=Path)
    create.add_argument("--output", required=True, type=Path)
    create.set_defaults(handler=command_create)

    verify_source = sub.add_parser("verify-source")
    verify_source.add_argument("--manifest", required=True, type=Path)
    verify_source.set_defaults(handler=command_verify_source)

    for command, handler in (
        ("stage-scientific", command_stage_scientific),
        ("stage-public-read", command_stage_public_read),
        ("stage-scenes", command_stage_scenes),
    ):
        stage = sub.add_parser(command)
        stage.add_argument("--manifest", required=True, type=Path)
        stage.add_argument("--state-dir", required=True, type=Path)
        stage.add_argument("--incoming-dir", required=True, type=Path)
        stage.set_defaults(handler=handler)

    verify = sub.add_parser("verify-installed")
    verify.add_argument("--manifest", required=True, type=Path)
    verify.add_argument("--state-dir", required=True, type=Path)
    verify.set_defaults(handler=command_verify_installed)

    activate = sub.add_parser("activate")
    activate.add_argument("--manifest", required=True, type=Path)
    activate.add_argument("--state-dir", required=True, type=Path)
    activate.set_defaults(handler=command_activate)

    rollback = sub.add_parser("rollback")
    rollback.add_argument("--build-id", required=True)
    rollback.add_argument("--state-dir", required=True, type=Path)
    rollback.set_defaults(handler=command_rollback)

    verify_env = sub.add_parser("verify-runtime-env")
    verify_env.add_argument("--manifest", required=True, type=Path)
    verify_env.add_argument("--env-file", required=True, type=Path, action="append")
    verify_env.set_defaults(handler=command_verify_runtime_env)

    configure_env = sub.add_parser("configure-runtime-env")
    configure_env.add_argument("--manifest", required=True, type=Path)
    configure_env.add_argument("--env-file", required=True, type=Path)
    configure_env.set_defaults(handler=command_configure_runtime_env)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = args.handler(args)
    except (
        FileNotFoundError,
        ValueError,
        OSError,
        sqlite3.DatabaseError,
        subprocess.CalledProcessError,
        tarfile.TarError,
        json.JSONDecodeError,
    ) as exc:
        raise SystemExit(f"public edge release failed: {exc}") from exc
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
