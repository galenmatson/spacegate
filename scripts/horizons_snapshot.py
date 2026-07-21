#!/usr/bin/env python3
"""Immutable raw-response snapshots for bounded JPL Horizons collectors."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SNAPSHOT_CONTRACT = "spacegate.horizons_raw_snapshot.v1"


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return sha256_bytes(encoded)


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9._-]+", "_", value.lower()).strip("._-")


def tree_files(path: Path) -> list[Path]:
    return sorted(child for child in path.rglob("*") if child.is_file())


def tree_sha256(path: Path) -> str:
    return stable_hash(
        [
            {
                "path": child.relative_to(path).as_posix(),
                "bytes": child.stat().st_size,
                "sha256": sha256_file(child),
            }
            for child in tree_files(path)
        ]
    )


def tree_bytes(path: Path) -> int:
    return sum(child.stat().st_size for child in tree_files(path))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_raw = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_raw)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def copy_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_raw = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_raw)
    try:
        shutil.copy2(source, temporary)
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


@dataclass(frozen=True)
class ResponseCapture:
    source_pk: str
    object_name: str
    horizons_command: str
    center_code: str
    query_url: str
    query_parameters: dict[str, str]
    payload: bytes

    @property
    def stem(self) -> str:
        return f"{slug(self.source_pk)}_{slug(self.object_name)}"

    @property
    def response_path(self) -> str:
        return f"responses/{self.stem}.txt"

    @property
    def query_path(self) -> str:
        return f"queries/{self.stem}.json"

    @property
    def response_sha256(self) -> str:
        return sha256_bytes(self.payload)


def seed_payload(seed_version: str, targets: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "spacegate.horizons_operator_seed.v1",
        "seed_version": seed_version,
        "targets": targets,
    }


def seed_sha256(seed_version: str, targets: list[dict[str, Any]]) -> str:
    return stable_hash(seed_payload(seed_version, targets))


def response_index_rows(
    captures: list[ResponseCapture], retrieved_at: str
) -> list[dict[str, Any]]:
    return [
        {
            "source_pk": capture.source_pk,
            "object_name": capture.object_name,
            "horizons_command": capture.horizons_command,
            "center_code": capture.center_code,
            "query_url": capture.query_url,
            "query_parameters_json": json.dumps(
                capture.query_parameters, sort_keys=True, separators=(",", ":")
            ),
            "response_path": capture.response_path,
            "response_sha256": capture.response_sha256,
            "response_bytes": len(capture.payload),
            "retrieved_at": retrieved_at,
        }
        for capture in captures
    ]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_horizons_snapshot(
    *,
    state_dir: Path,
    family: str,
    table_source_name: str,
    response_source_name: str,
    parsed_filename: str,
    legacy_relative_path: str,
    manifest_filename: str,
    source_version: str,
    source_url: str,
    retrieved_at: str,
    rows: list[dict[str, Any]],
    fieldnames: list[str],
    captures: list[ResponseCapture],
    seed_version: str,
    targets: list[dict[str, Any]],
    collector_path: Path,
    query_signature: dict[str, Any],
) -> tuple[Path, list[dict[str, Any]]]:
    if not rows or len(rows) != len(captures):
        raise ValueError("Horizons rows and response captures must be nonempty and equal")
    row_keys = [str(row["source_pk"]) for row in rows]
    capture_keys = [capture.source_pk for capture in captures]
    if len(set(row_keys)) != len(row_keys) or row_keys != capture_keys:
        raise ValueError("Horizons rows and response captures have different source keys")
    if set(rows[0]) != set(fieldnames) or any(set(row) != set(fieldnames) for row in rows):
        raise ValueError("Horizons parsed rows do not exactly match the declared schema")

    snapshots_root = state_dir / "raw" / family / "snapshots"
    snapshots_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".horizons-", dir=snapshots_root))
    extracted_root = temporary / "extracted"
    source_root = temporary / "source"
    try:
        parsed_path = extracted_root / parsed_filename
        write_csv(parsed_path, rows, fieldnames)
        for capture in captures:
            response_path = source_root / capture.response_path
            response_path.parent.mkdir(parents=True, exist_ok=True)
            response_path.write_bytes(capture.payload)
            write_json(
                source_root / capture.query_path,
                {
                    "schema_version": "spacegate.horizons_query_capture.v1",
                    "source_pk": capture.source_pk,
                    "object_name": capture.object_name,
                    "horizons_command": capture.horizons_command,
                    "center_code": capture.center_code,
                    "query_url": capture.query_url,
                    "query_parameters": capture.query_parameters,
                    "response_path": capture.response_path,
                    "response_sha256": capture.response_sha256,
                    "response_bytes": len(capture.payload),
                    "retrieved_at": retrieved_at,
                },
            )
        write_json(source_root / "targets.json", seed_payload(seed_version, targets))
        index_fields = [
            "source_pk",
            "object_name",
            "horizons_command",
            "center_code",
            "query_url",
            "query_parameters_json",
            "response_path",
            "response_sha256",
            "response_bytes",
            "retrieved_at",
        ]
        write_csv(
            source_root / "response_index.csv",
            response_index_rows(captures, retrieved_at),
            index_fields,
        )
        write_json(
            source_root / "snapshot_metadata.json",
            {
                "schema_version": SNAPSHOT_CONTRACT,
                "source_version": source_version,
                "source_url": source_url,
                "retrieved_at": retrieved_at,
                "collector_path": collector_path.as_posix(),
                "collector_sha256": sha256_file(collector_path),
                "operator_seed_version": seed_version,
                "operator_seed_sha256": seed_sha256(seed_version, targets),
                "query_signature": query_signature,
                "response_count": len(captures),
            },
        )
        extracted_sha256 = sha256_file(parsed_path)
        source_sha256 = tree_sha256(source_root)
        snapshot_id = stable_hash(
            {
                "schema_version": SNAPSHOT_CONTRACT,
                "family": family,
                "source_version": source_version,
                "extracted_sha256": extracted_sha256,
                "source_sha256": source_sha256,
            }
        )[:24]
        destination = snapshots_root / snapshot_id
        if destination.exists():
            if tree_sha256(destination / "source") != source_sha256:
                raise ValueError(f"immutable Horizons snapshot changed: {destination}")
            shutil.rmtree(temporary)
        else:
            os.replace(temporary, destination)

        immutable_parsed = destination / "extracted" / parsed_filename
        immutable_source = destination / "source"
        copy_atomic(immutable_parsed, state_dir / legacy_relative_path)
        manifest = [
            {
                "source_name": table_source_name,
                "source_version": source_version,
                "url": source_url,
                "dest_path": immutable_parsed.relative_to(state_dir).as_posix(),
                "retrieved_at": retrieved_at,
                "checked_at": retrieved_at,
                "bytes_written": immutable_parsed.stat().st_size,
                "row_count": len(rows),
                "sha256": extracted_sha256,
                "query_signature": query_signature,
            },
            {
                "source_name": response_source_name,
                "source_version": source_version,
                "url": source_url,
                "dest_path": immutable_source.relative_to(state_dir).as_posix(),
                "retrieved_at": retrieved_at,
                "checked_at": retrieved_at,
                "bytes_written": tree_bytes(immutable_source),
                "row_count": len(captures),
                "sha256": source_sha256,
                "query_signature": query_signature,
            },
        ]
        write_json_atomic(
            state_dir / "reports" / "manifests" / manifest_filename, manifest
        )
        return destination, manifest
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise
