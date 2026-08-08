#!/usr/bin/env python3
"""Snapshot, verify, restore, and retire inactive public-edge rollback builds."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


SCHEMA_VERSION = "spacegate.public_edge_cold_snapshot.v1"
MARKER_NAME = ".antiproton-data-volume-id"
BUILD_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,159}")
VOLUME_ID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
MINIMUM_FREE_BYTES = 15 * 1024**3


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def validate_build_id(value: str) -> str:
    if BUILD_ID_RE.fullmatch(value) is None:
        raise ValueError(f"unsafe build id: {value!r}")
    return value


def validate_volume_id(value: str) -> str:
    normalized = value.strip().lower()
    if VOLUME_ID_RE.fullmatch(normalized) is None:
        raise ValueError("invalid cold volume id")
    return normalized


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def bounded_path(root: Path, candidate: Path, *, must_exist: bool = False) -> Path:
    resolved_root = root.resolve(strict=True)
    resolved = candidate.resolve(strict=must_exist)
    if not resolved.is_relative_to(resolved_root):
        raise ValueError(f"path escapes bounded root {resolved_root}: {candidate}")
    return resolved


def verify_cold_root(
    cold_root: Path,
    expected_volume_id: str,
    *,
    hot_root: Path | None = None,
    require_mount: bool = True,
    require_distinct_filesystem: bool = True,
) -> dict[str, Any]:
    expected = validate_volume_id(expected_volume_id)
    cold = cold_root.resolve(strict=True)
    volume = cold.parent
    if require_mount and not volume.is_mount():
        raise ValueError(f"cold volume root is not a mount point: {volume}")
    marker = volume / MARKER_NAME
    if not marker.is_file() or marker.is_symlink():
        raise ValueError(f"cold volume marker is missing: {marker}")
    actual = marker.read_text(encoding="utf-8").strip().lower()
    if actual != expected:
        raise ValueError(
            f"cold volume marker mismatch: expected={expected} actual={actual}"
        )
    if marker.stat().st_dev != cold.stat().st_dev:
        raise ValueError("cold root and volume marker are on different filesystems")
    if hot_root is not None and require_distinct_filesystem:
        hot = hot_root.resolve(strict=True)
        if hot.stat().st_dev == cold.stat().st_dev:
            raise ValueError("hot and cold roots must use different filesystems")
    return {
        "status": "pass",
        "cold_root": str(cold),
        "volume_root": str(volume),
        "volume_id": actual,
        "free_bytes": shutil.disk_usage(cold).free,
    }


def iter_tree(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    directories: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"rollback tree contains a symbolic link: {path}")
        relative = path.relative_to(root).as_posix()
        pure = PurePosixPath(relative)
        if pure.is_absolute() or ".." in pure.parts or not pure.parts:
            raise ValueError(f"unsafe rollback path: {relative!r}")
        if path.is_dir():
            directories.append(
                {"path": relative, "mode": path.stat().st_mode & 0o7777}
            )
        elif path.is_file():
            files.append(
                {
                    "path": relative,
                    "bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "mode": path.stat().st_mode & 0o7777,
                }
            )
        else:
            raise ValueError(f"unsupported rollback file type: {path}")
    return directories, files


def copy_tree(source: Path, target: Path) -> None:
    if target.exists():
        raise ValueError(f"copy target already exists: {target}")
    shutil.copytree(source, target, symlinks=False, copy_function=shutil.copy2)


def snapshot_path(cold_root: Path, build_id: str) -> Path:
    return cold_root / "rollbacks" / build_id / "snapshot.json"


def snapshot_root_from_manifest(snapshot: Path, manifest: dict[str, Any]) -> Path:
    expected = validate_build_id(str(manifest.get("build_id") or ""))
    root = snapshot.resolve(strict=True).parent
    if root.name != expected or root.parent.name != "rollbacks":
        raise ValueError("cold snapshot path does not match its build id")
    return root


def validate_snapshot(manifest: dict[str, Any]) -> dict[str, Any]:
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("incompatible cold snapshot schema")
    validate_build_id(str(manifest.get("build_id") or ""))
    validate_volume_id(str(manifest.get("volume_id") or ""))
    directories = manifest.get("directories")
    if not isinstance(directories, list):
        raise ValueError("cold snapshot directory inventory is missing")
    seen_directories: set[str] = set()
    for row in directories:
        if not isinstance(row, dict):
            raise ValueError("invalid cold snapshot directory row")
        relative = PurePosixPath(str(row.get("path") or ""))
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or not relative.parts
            or relative.as_posix() in seen_directories
        ):
            raise ValueError("unsafe or duplicate cold snapshot directory path")
        seen_directories.add(relative.as_posix())
        if not isinstance(row.get("mode"), int):
            raise ValueError("invalid cold snapshot directory mode")
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("cold snapshot has no file inventory")
    seen: set[str] = set()
    for row in files:
        if not isinstance(row, dict):
            raise ValueError("invalid cold snapshot file row")
        relative = PurePosixPath(str(row.get("path") or ""))
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or not relative.parts
            or relative.as_posix() in seen
        ):
            raise ValueError("unsafe or duplicate cold snapshot path")
        seen.add(relative.as_posix())
        if not isinstance(row.get("bytes"), int) or row["bytes"] < 0:
            raise ValueError("invalid cold snapshot byte count")
        if not re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256") or "")):
            raise ValueError("invalid cold snapshot checksum")
        if relative.parent.as_posix() != "." and relative.parent.as_posix() not in seen_directories:
            raise ValueError("cold snapshot file parent is not inventoried")
    archive = manifest.get("archive")
    if not isinstance(archive, dict):
        raise ValueError("cold snapshot archive contract is missing")
    relative = PurePosixPath(str(archive.get("path") or ""))
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError("unsafe cold snapshot archive path")
    if not isinstance(archive.get("bytes"), int) or archive["bytes"] < 1:
        raise ValueError("invalid cold snapshot archive size")
    if not re.fullmatch(r"[0-9a-f]{64}", str(archive.get("sha256") or "")):
        raise ValueError("invalid cold snapshot archive checksum")
    logical = str(manifest.get("logical_sha256") or "")
    expected_logical = hashlib.sha256(
        canonical_json(
            {
                "build_id": manifest["build_id"],
                "directories": manifest.get("directories") or [],
                "files": files,
                "archive": archive,
            }
        )
    ).hexdigest()
    if logical != expected_logical:
        raise ValueError("cold snapshot logical checksum mismatch")
    return manifest


def verify_snapshot_tree(root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    build = root / "out" / manifest["build_id"]
    actual_directories, actual_files = iter_tree(build)
    if actual_directories != manifest["directories"]:
        raise ValueError("cold rollback directory inventory mismatch")
    if actual_files != manifest["files"]:
        raise ValueError("cold rollback file inventory mismatch")
    archive = bounded_path(
        root, root / manifest["archive"]["path"], must_exist=True
    )
    if archive.stat().st_size != manifest["archive"]["bytes"]:
        raise ValueError("cold rollback archive byte mismatch")
    if sha256_file(archive) != manifest["archive"]["sha256"]:
        raise ValueError("cold rollback archive checksum mismatch")
    return {
        "status": "pass",
        "build_id": manifest["build_id"],
        "file_count": len(manifest["files"]),
        "build_bytes": sum(row["bytes"] for row in manifest["files"]),
        "archive_bytes": manifest["archive"]["bytes"],
        "logical_sha256": manifest["logical_sha256"],
    }


def verify_snapshot_files(snapshot: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    root = snapshot_root_from_manifest(snapshot, manifest)
    return verify_snapshot_tree(root, manifest)


def command_verify_volume(args: argparse.Namespace) -> dict[str, Any]:
    return verify_cold_root(
        args.cold_root,
        args.volume_id,
        hot_root=args.hot_state_dir,
    )


def command_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    build_id = validate_build_id(args.build_id)
    cold = args.cold_root.resolve(strict=True)
    state = args.hot_state_dir.resolve(strict=True)
    volume = verify_cold_root(cold, args.volume_id, hot_root=state)
    source = bounded_path(
        state / "out", state / "out" / build_id, must_exist=True
    )
    served = state / "served" / "current"
    if served.exists() and served.resolve(strict=True) == source:
        raise ValueError("refusing to snapshot the currently served build as inactive")
    archive = args.archive.resolve(strict=True)
    if not archive.is_file() or not archive.name.startswith(f"{build_id}."):
        raise ValueError("rollback archive does not match build id")
    final = snapshot_path(cold, build_id).parent
    if final.exists():
        existing_path = final / "snapshot.json"
        existing = validate_snapshot(load_json(existing_path))
        verified = verify_snapshot_files(existing_path, existing)
        return {**verified, "reused": True, "cold_free_bytes": volume["free_bytes"]}
    final.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=final.parent))
    try:
        copied_build = staging / "out" / build_id
        copied_build.parent.mkdir(parents=True)
        copy_tree(source, copied_build)
        copied_archive = staging / "archive" / archive.name
        copied_archive.parent.mkdir(parents=True)
        shutil.copy2(archive, copied_archive)
        directories, files = iter_tree(copied_build)
        archive_row = {
            "path": f"archive/{archive.name}",
            "bytes": copied_archive.stat().st_size,
            "sha256": sha256_file(copied_archive),
            "mode": copied_archive.stat().st_mode & 0o7777,
        }
        logical_payload = {
            "build_id": build_id,
            "directories": directories,
            "files": files,
            "archive": archive_row,
        }
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": utc_now(),
            "build_id": build_id,
            "volume_id": validate_volume_id(args.volume_id),
            "source_build": str(source),
            "source_archive": str(archive),
            **logical_payload,
            "logical_sha256": hashlib.sha256(
                canonical_json(logical_payload)
            ).hexdigest(),
        }
        atomic_json(staging / "snapshot.json", manifest)
        validate_snapshot(manifest)
        verify_snapshot_tree(staging, manifest)
        os.replace(staging, final)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
    verified = verify_snapshot_files(final / "snapshot.json", manifest)
    return {**verified, "reused": False, "cold_free_bytes": shutil.disk_usage(cold).free}


def command_verify_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    cold = args.cold_root.resolve(strict=True)
    verify_cold_root(cold, args.volume_id, hot_root=args.hot_state_dir)
    snapshot = snapshot_path(cold, validate_build_id(args.build_id))
    manifest = validate_snapshot(load_json(snapshot))
    if manifest["volume_id"] != validate_volume_id(args.volume_id):
        raise ValueError("cold snapshot volume id mismatch")
    return verify_snapshot_files(snapshot, manifest)


def command_restore(args: argparse.Namespace) -> dict[str, Any]:
    cold = args.cold_root.resolve(strict=True)
    state = args.hot_state_dir.resolve(strict=True)
    verify_cold_root(cold, args.volume_id, hot_root=state)
    build_id = validate_build_id(args.build_id)
    snapshot = snapshot_path(cold, build_id)
    manifest = validate_snapshot(load_json(snapshot))
    verified = verify_snapshot_files(snapshot, manifest)
    target = state / "out" / build_id
    if target.exists():
        _, files = iter_tree(target)
        if files != manifest["files"]:
            raise ValueError("existing hot rollback target differs from snapshot")
        return {**verified, "restored": False, "hot_free_bytes": shutil.disk_usage(state).free}
    required = verified["build_bytes"] + args.minimum_free_bytes
    available = shutil.disk_usage(state).free
    if available < required:
        raise ValueError(
            f"hot restore reserve violated: available={available} required={required}"
        )
    (state / "out").mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.restore.", dir=state / "out"))
    try:
        copied = staging / build_id
        copy_tree(snapshot.parent / "out" / build_id, copied)
        _, files = iter_tree(copied)
        if files != manifest["files"]:
            raise ValueError("restored rollback differs from cold snapshot")
        os.replace(copied, target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return {**verified, "restored": True, "hot_free_bytes": shutil.disk_usage(state).free}


def command_retire_hot(args: argparse.Namespace) -> dict[str, Any]:
    cold = args.cold_root.resolve(strict=True)
    state = args.hot_state_dir.resolve(strict=True)
    verify_cold_root(cold, args.volume_id, hot_root=state)
    build_id = validate_build_id(args.build_id)
    snapshot = snapshot_path(cold, build_id)
    manifest = validate_snapshot(load_json(snapshot))
    verified = verify_snapshot_files(snapshot, manifest)
    source = bounded_path(
        state / "out", state / "out" / build_id, must_exist=True
    )
    served = state / "served" / "current"
    if served.exists() and served.resolve(strict=True) == source:
        raise ValueError("refusing to retire the currently served build")
    _, source_files = iter_tree(source)
    if source_files != manifest["files"]:
        raise ValueError("hot rollback source differs from verified cold snapshot")
    archive = args.archive.resolve(strict=True)
    if (
        archive.stat().st_size != manifest["archive"]["bytes"]
        or sha256_file(archive) != manifest["archive"]["sha256"]
    ):
        raise ValueError("hot rollback archive differs from verified cold snapshot")
    shutil.rmtree(source)
    archive.unlink()
    return {
        **verified,
        "retired_hot_build": str(source),
        "retired_hot_archive": str(archive),
        "hot_free_bytes": shutil.disk_usage(state).free,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def common(command: argparse.ArgumentParser) -> None:
        command.add_argument("--cold-root", required=True, type=Path)
        command.add_argument("--hot-state-dir", required=True, type=Path)
        command.add_argument("--volume-id", required=True)

    verify_volume = sub.add_parser("verify-volume")
    common(verify_volume)
    verify_volume.set_defaults(handler=command_verify_volume)

    snapshot = sub.add_parser("snapshot")
    common(snapshot)
    snapshot.add_argument("--build-id", required=True)
    snapshot.add_argument("--archive", required=True, type=Path)
    snapshot.set_defaults(handler=command_snapshot)

    verify = sub.add_parser("verify-snapshot")
    common(verify)
    verify.add_argument("--build-id", required=True)
    verify.set_defaults(handler=command_verify_snapshot)

    restore = sub.add_parser("restore")
    common(restore)
    restore.add_argument("--build-id", required=True)
    restore.add_argument(
        "--minimum-free-bytes", type=int, default=MINIMUM_FREE_BYTES
    )
    restore.set_defaults(handler=command_restore)

    retire = sub.add_parser("retire-hot")
    common(retire)
    retire.add_argument("--build-id", required=True)
    retire.add_argument("--archive", required=True, type=Path)
    retire.set_defaults(handler=command_retire_hot)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = args.handler(args)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
