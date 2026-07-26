from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


EXPECTED_MANIFEST_SCHEMA = "spacegate.smart_tags_manifest.v1"
EXPECTED_TAG_SCHEMA = "spacegate.smart_tags.v1"
EXPECTED_ASSIGNMENT_SCHEMA = "spacegate.smart_tag_assignments.v1"
EXPECTED_SOURCE_SUMMARY_SCHEMA = "spacegate.smart_tag_source_summary.v1"


class SmartTagsUnavailable(RuntimeError):
    pass


class SmartTagsIncompatible(SmartTagsUnavailable):
    pass


@dataclass(frozen=True)
class SmartTagPaths:
    database: Path
    manifest: Path


def _state_dir() -> Path:
    root = Path(__file__).resolve().parents[3]
    return Path(
        os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or root / "data"
    )


def required() -> bool:
    return str(os.getenv("SPACEGATE_SMART_TAGS_REQUIRED", "")).strip().lower() in {
        "1",
        "true",
        "yes",
    }


def allow_sample() -> bool:
    return str(os.getenv("SPACEGATE_SMART_TAGS_ALLOW_SAMPLE", "")).strip().lower() in {
        "1",
        "true",
        "yes",
    }


def paths_for_build(build_id: str) -> SmartTagPaths:
    explicit = str(os.getenv("SPACEGATE_SMART_TAGS_PATH") or "").strip()
    if explicit:
        database = Path(explicit)
        return SmartTagPaths(database=database, manifest=database.parent / "manifest.json")
    root = _state_dir() / "derived" / "smart_tags" / build_id / "current"
    return SmartTagPaths(database=root / "smart_tags.sqlite", manifest=root / "manifest.json")


def _manifest(paths: SmartTagPaths, build_id: str) -> dict[str, Any]:
    try:
        manifest = json.loads(paths.manifest.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        raise SmartTagsUnavailable("smart-tag artifact unavailable") from exc
    if not isinstance(manifest, dict):
        raise SmartTagsIncompatible("smart-tag manifest is not an object")
    expected = {
        "schema_version": EXPECTED_MANIFEST_SCHEMA,
        "status": "pass",
        "build_id": build_id,
        "tag_schema_version": EXPECTED_TAG_SCHEMA,
        "assignment_schema_version": EXPECTED_ASSIGNMENT_SCHEMA,
        "source_summary_schema_version": EXPECTED_SOURCE_SUMMARY_SCHEMA,
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise SmartTagsIncompatible(f"smart-tag manifest mismatch: {key}")
    if manifest.get("sample_limit") is not None and not allow_sample():
        raise SmartTagsIncompatible("sample smart-tag artifact cannot serve public traffic")
    if not paths.database.is_file():
        raise SmartTagsUnavailable("smart-tag database unavailable")
    spec = (manifest.get("artifacts") or {}).get("database") or {}
    if paths.database.stat().st_size != spec.get("bytes"):
        raise SmartTagsIncompatible("smart-tag database byte size mismatch")
    return manifest


def connect(build_id: str) -> sqlite3.Connection:
    paths = paths_for_build(build_id)
    manifest = _manifest(paths, build_id)
    uri = f"file:{paths.database.resolve()}?mode=ro&immutable=1"
    con = sqlite3.connect(uri, uri=True, timeout=2.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    con.execute("PRAGMA cache_size=-8192")
    metadata = dict(con.execute("SELECT key,value FROM metadata"))
    if (
        metadata.get("build_id") != build_id
        or metadata.get("registry_hash") != manifest.get("registry_hash")
        or metadata.get("schema_version") != EXPECTED_TAG_SCHEMA
    ):
        con.close()
        raise SmartTagsIncompatible("smart-tag database identity mismatch")
    return con


def attach_to_public_read(con: sqlite3.Connection, build_id: str) -> bool:
    attached = {
        str(row[1]) for row in con.execute("PRAGMA database_list").fetchall()
    }
    if "smart_tags" in attached:
        return True
    paths = paths_for_build(build_id)
    try:
        manifest = _manifest(paths, build_id)
    except SmartTagsIncompatible:
        raise
    except SmartTagsUnavailable:
        if required():
            raise
        return False
    uri = f"file:{paths.database.resolve()}?mode=ro&immutable=1"
    con.execute("ATTACH DATABASE ? AS smart_tags", (uri,))
    metadata = dict(con.execute("SELECT key,value FROM smart_tags.metadata"))
    if (
        metadata.get("build_id") != build_id
        or metadata.get("registry_hash") != manifest.get("registry_hash")
        or metadata.get("schema_version") != EXPECTED_TAG_SCHEMA
    ):
        con.execute("DETACH DATABASE smart_tags")
        raise SmartTagsIncompatible("attached smart-tag database identity mismatch")
    return True


def is_attached(con: sqlite3.Connection) -> bool:
    return any(
        str(row[1]) == "smart_tags"
        for row in con.execute("PRAGMA database_list").fetchall()
    )


def _definition_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "key": row["tag_key"],
        "label": row["label"],
        "name": row["name"],
        "category": row["category"],
        "kind": row["kind"],
        "layer": row["layer"],
        "target_types": json.loads(row["target_types_json"]),
        "visual_token": row["visual_token"],
        "priority": {
            "compact": row["compact_priority"],
            "normal": row["normal_priority"],
            "expanded": row["expanded_priority"],
        },
        "concept_slug": row["concept_slug"],
        "tooltip": row["tooltip"],
        "short_tooltip": row["short_tooltip"],
        "source_policy": row["source_policy"],
        "evaluator": {
            "id": row["evaluator_id"],
            "version": row["evaluator_version"],
            "params": json.loads(row["evaluator_params_json"]),
        },
        "filterable": bool(row["filterable"]),
        "rollup": row["rollup"],
    }


def registry_payload(con: sqlite3.Connection) -> dict[str, Any]:
    metadata = dict(con.execute("SELECT key,value FROM metadata"))
    definitions = [
        _definition_payload(row)
        for row in con.execute(
            "SELECT * FROM tag_definitions ORDER BY category,name,tag_key"
        )
    ]
    return {
        "schema_version": "spacegate.smart_tag_registry_api.v1",
        "build_id": metadata["build_id"],
        "registry_id": metadata["registry_id"],
        "registry_version": metadata["registry_version"],
        "registry_hash": metadata["registry_hash"],
        "definitions": definitions,
    }


def definition_payload(con: sqlite3.Connection, tag_key: str) -> dict[str, Any] | None:
    row = con.execute(
        "SELECT * FROM tag_definitions WHERE tag_key=?", (tag_key,)
    ).fetchone()
    return _definition_payload(row) if row else None


def validate_filter_keys(
    con: sqlite3.Connection, keys: Iterable[str]
) -> list[str]:
    normalized = sorted({str(key).strip().lower() for key in keys if str(key).strip()})
    if not normalized:
        return []
    placeholders = ",".join("?" for _ in normalized)
    accepted = {
        str(row[0])
        for row in con.execute(
            f"""
            SELECT tag_key FROM smart_tags.tag_definitions
            WHERE filterable=1 AND tag_key IN ({placeholders})
            """,
            normalized,
        )
    }
    missing = [key for key in normalized if key not in accepted]
    if missing:
        raise ValueError(f"unknown or non-filterable smart tag: {', '.join(missing)}")
    return normalized


def system_tags_attached(
    con: sqlite3.Connection, system_ids: Iterable[int]
) -> dict[int, list[dict[str, Any]]]:
    ids = sorted({int(value) for value in system_ids})
    result = {system_id: [] for system_id in ids}
    if not ids or not is_attached(con):
        return result
    placeholders = ",".join("?" for _ in ids)
    rows = con.execute(
        f"""
        SELECT m.system_id,m.member_count,m.primary_target_type,
               m.primary_target_key,m.basis_kind,d.*
        FROM smart_tags.system_tag_membership m
        JOIN smart_tags.tag_definitions d USING(tag_key)
        WHERE m.system_id IN ({placeholders})
        ORDER BY m.system_id,d.normal_priority DESC,d.category,d.name,d.tag_key
        """,
        ids,
    )
    for row in rows:
        value = _definition_payload(row)
        value["assignment"] = {
            "scope": "system" if row["primary_target_type"] == "system" else "member_rollup",
            "member_count": row["member_count"],
            "primary_target_type": row["primary_target_type"],
            "primary_target_key": row["primary_target_key"],
            "basis_kind": row["basis_kind"],
        }
        result[int(row["system_id"])].append(value)
    return result


def source_summary_attached(
    con: sqlite3.Connection, system_ids: Iterable[int]
) -> dict[int, list[dict[str, Any]]]:
    ids = sorted({int(value) for value in system_ids})
    result = {system_id: [] for system_id in ids}
    if not ids or not is_attached(con):
        return result
    placeholders = ",".join("?" for _ in ids)
    rows = con.execute(
        f"""
        SELECT s.system_id,s.contribution_kind,s.member_count,d.*
        FROM smart_tags.system_sources s
        JOIN smart_tags.source_definitions d USING(source_key)
        WHERE s.system_id IN ({placeholders})
        ORDER BY s.system_id,s.contribution_kind,d.publisher,d.source_id
        """,
        ids,
    )
    for row in rows:
        result[int(row["system_id"])].append(
            {
                "key": row["source_key"],
                "source_id": row["source_id"],
                "release_id": row["release_id"],
                "publisher": row["publisher"],
                "citation_url": row["citation_url"],
                "license": {
                    "name": row["license_name"],
                    "url": row["license_url"],
                },
                "authority_roles": json.loads(row["authority_roles_json"]),
                "contribution_kind": row["contribution_kind"],
                "member_count": row["member_count"],
            }
        )
    return result


def system_payload(con: sqlite3.Connection, system_id: int) -> dict[str, Any]:
    tags = system_tags_attached(con, [system_id]).get(system_id, [])
    sources = source_summary_attached(con, [system_id]).get(system_id, [])
    return {
        "schema_version": "spacegate.smart_tags_system.v1",
        "system_id": system_id,
        "smart_tags": tags,
        "source_summary": sources,
    }
