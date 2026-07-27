from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


EXPECTED_MANIFEST_SCHEMA = "spacegate.smart_tags_manifest.v2"
EXPECTED_TAG_SCHEMA = "spacegate.smart_tags.v2"
EXPECTED_ASSIGNMENT_SCHEMA = "spacegate.smart_tag_assignments.v2"
EXPECTED_SOURCE_SUMMARY_SCHEMA = "spacegate.smart_tag_source_summary.v2"
EXPECTED_SOURCE_CONTRIBUTION_SCHEMA = "spacegate.smart_tag_source_contributions.v1"
EXPECTED_COMPILER_VERSION = "spacegate.smart_tags_compiler.v2.2"
EVIDENCE_STATUS_BITS = (
    ("source", 1),
    ("derived", 2),
    ("assumed", 4),
    ("screen", 8),
    ("candidate", 16),
    ("ambiguous", 32),
    ("quarantined", 64),
    ("missing", 128),
    ("source_model", 256),
)
_RUNTIME_LOCK = threading.Lock()
_RUNTIME_COUNTERS: Counter[str] = Counter()
_RUNTIME_TIMING_MS: Counter[str] = Counter()
_RUNTIME_TIMING_MAX_MS: dict[str, float] = {}
_RUNTIME_IDENTITY: dict[str, str] = {}


class SmartTagsUnavailable(RuntimeError):
    pass


class SmartTagsIncompatible(SmartTagsUnavailable):
    pass


@dataclass(frozen=True)
class SmartTagPaths:
    database: Path
    assignments: Path
    source_contributions: Path
    manifest: Path


def _increment(key: str, amount: int = 1) -> None:
    with _RUNTIME_LOCK:
        _RUNTIME_COUNTERS[key] += amount


def _record_query(key: str, started: float, result_count: int = 0) -> None:
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    with _RUNTIME_LOCK:
        _RUNTIME_COUNTERS[f"{key}_queries"] += 1
        _RUNTIME_COUNTERS[f"{key}_results"] += max(0, int(result_count))
        _RUNTIME_TIMING_MS[key] += elapsed_ms
        _RUNTIME_TIMING_MAX_MS[key] = max(
            elapsed_ms, _RUNTIME_TIMING_MAX_MS.get(key, 0.0)
        )


def _record_identity(build_id: str, registry_hash: Any) -> None:
    with _RUNTIME_LOCK:
        _RUNTIME_IDENTITY["build_id"] = build_id
        _RUNTIME_IDENTITY["registry_hash"] = str(registry_hash or "")


def runtime_stats() -> dict[str, Any]:
    with _RUNTIME_LOCK:
        counters = dict(_RUNTIME_COUNTERS)
        timing = {
            key: {
                "total_ms": round(float(total), 3),
                "max_ms": round(
                    float(_RUNTIME_TIMING_MAX_MS.get(key, 0.0)), 3
                ),
                "average_ms": round(
                    float(total)
                    / max(1, int(_RUNTIME_COUNTERS.get(f"{key}_queries", 0))),
                    3,
                ),
            }
            for key, total in _RUNTIME_TIMING_MS.items()
        }
        identity = dict(_RUNTIME_IDENTITY)
    return {
        "required": required(),
        "artifact_identity": identity or None,
        "counters": counters,
        "query_timing": timing,
    }


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
        return SmartTagPaths(
            database=database,
            assignments=database.parent / "assignments.parquet",
            source_contributions=database.parent / "source_contributions.parquet",
            manifest=database.parent / "manifest.json",
        )
    root = _state_dir() / "derived" / "smart_tags" / build_id / "current"
    return SmartTagPaths(
        database=root / "smart_tags.sqlite",
        assignments=root / "assignments.parquet",
        source_contributions=root / "source_contributions.parquet",
        manifest=root / "manifest.json",
    )


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
        "source_contribution_schema_version": EXPECTED_SOURCE_CONTRIBUTION_SCHEMA,
        "compiler_version": EXPECTED_COMPILER_VERSION,
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
    assignment_spec = (manifest.get("artifacts") or {}).get("assignments") or {}
    if (
        not paths.assignments.is_file()
        or paths.assignments.stat().st_size != assignment_spec.get("bytes")
    ):
        raise SmartTagsIncompatible("smart-tag assignment artifact mismatch")
    contribution_spec = (
        (manifest.get("artifacts") or {}).get("source_contributions") or {}
    )
    if (
        not paths.source_contributions.is_file()
        or paths.source_contributions.stat().st_size
        != contribution_spec.get("bytes")
    ):
        raise SmartTagsIncompatible(
            "smart-tag source-contribution artifact mismatch"
        )
    return manifest


def connect(build_id: str) -> sqlite3.Connection:
    started = time.perf_counter()
    paths = paths_for_build(build_id)
    try:
        manifest = _manifest(paths, build_id)
    except SmartTagsIncompatible:
        _increment("connect_incompatible")
        raise
    except SmartTagsUnavailable:
        _increment("connect_unavailable")
        raise
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
        or metadata.get("compiler_version") != EXPECTED_COMPILER_VERSION
    ):
        con.close()
        _increment("connect_incompatible")
        raise SmartTagsIncompatible("smart-tag database identity mismatch")
    _record_identity(build_id, manifest.get("registry_hash"))
    _record_query("connect", started, 1)
    return con


def attach_to_public_read(con: sqlite3.Connection, build_id: str) -> bool:
    started = time.perf_counter()
    attached = {
        str(row[1]) for row in con.execute("PRAGMA database_list").fetchall()
    }
    if "smart_tags" in attached:
        _increment("attachment_reused")
        return True
    paths = paths_for_build(build_id)
    try:
        manifest = _manifest(paths, build_id)
    except SmartTagsIncompatible:
        _increment("attachment_incompatible")
        raise
    except SmartTagsUnavailable:
        _increment("attachment_unavailable")
        if required():
            raise
        _increment("compatibility_untagged_reads")
        return False
    uri = f"file:{paths.database.resolve()}?mode=ro&immutable=1"
    con.execute("ATTACH DATABASE ? AS smart_tags", (uri,))
    metadata = dict(con.execute("SELECT key,value FROM smart_tags.metadata"))
    if (
        metadata.get("build_id") != build_id
        or metadata.get("registry_hash") != manifest.get("registry_hash")
        or metadata.get("schema_version") != EXPECTED_TAG_SCHEMA
        or metadata.get("compiler_version") != EXPECTED_COMPILER_VERSION
    ):
        con.execute("DETACH DATABASE smart_tags")
        _increment("attachment_incompatible")
        raise SmartTagsIncompatible("attached smart-tag database identity mismatch")
    _record_identity(build_id, manifest.get("registry_hash"))
    _record_query("attachment", started, 1)
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
    started = time.perf_counter()
    metadata = dict(con.execute("SELECT key,value FROM metadata"))
    definitions = [
        _definition_payload(row)
        for row in con.execute(
            "SELECT * FROM tag_definitions ORDER BY category,name,tag_key"
        )
    ]
    payload = {
        "schema_version": "spacegate.smart_tag_registry_api.v1",
        "build_id": metadata["build_id"],
        "registry_id": metadata["registry_id"],
        "registry_version": metadata["registry_version"],
        "registry_hash": metadata["registry_hash"],
        "definitions": definitions,
    }
    _record_query("registry", started, len(definitions))
    return payload


def definition_payload(con: sqlite3.Connection, tag_key: str) -> dict[str, Any] | None:
    started = time.perf_counter()
    row = con.execute(
        "SELECT * FROM tag_definitions WHERE tag_key=?", (tag_key,)
    ).fetchone()
    payload = _definition_payload(row) if row else None
    _record_query("definition", started, int(payload is not None))
    return payload


def _source_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "key": row["source_key"],
        "source_id": row["source_id"],
        "release_id": row["release_id"],
        "public_name": row["public_name"],
        "publisher": row["publisher"],
        "description": row["description"],
        "mission_instrument": row["mission_instrument"],
        "citation_url": row["citation_url"],
        "license": {
            "name": row["license_name"],
            "url": row["license_url"],
        },
        "authority_roles": json.loads(row["authority_roles_json"]),
    }


def source_payload(con: sqlite3.Connection, source_key: str) -> dict[str, Any] | None:
    started = time.perf_counter()
    row = con.execute(
        "SELECT * FROM source_definitions WHERE source_key=?", (source_key,)
    ).fetchone()
    payload = _source_payload(row) if row else None
    _record_query("source_definition", started, int(payload is not None))
    return payload


def validate_filter_keys(
    con: sqlite3.Connection, keys: Iterable[str]
) -> list[str]:
    started = time.perf_counter()
    normalized = sorted({str(key).strip().lower() for key in keys if str(key).strip()})
    if not normalized:
        _record_query("filter_validation", started, 0)
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
        _increment("filter_validation_rejections")
        raise ValueError(f"unknown or non-filterable smart tag: {', '.join(missing)}")
    _record_query("filter_validation", started, len(normalized))
    return normalized


def system_tags_attached(
    con: sqlite3.Connection, system_ids: Iterable[int]
) -> dict[int, list[dict[str, Any]]]:
    started = time.perf_counter()
    ids = sorted({int(value) for value in system_ids})
    result = {system_id: [] for system_id in ids}
    if not ids or not is_attached(con):
        _record_query("system_tags", started, 0)
        return result
    placeholders = ",".join("?" for _ in ids)
    rows = con.execute(
        f"""
        SELECT m.system_id,m.member_count,m.scope_code,m.basis_code,
               m.evidence_status_mask,m.min_confidence,m.max_confidence,d.*
        FROM smart_tags.system_tag_membership m
        JOIN smart_tags.tag_definitions d USING(tag_id)
        WHERE m.system_id IN ({placeholders})
        ORDER BY m.system_id,d.normal_priority DESC,d.category,d.name,d.tag_key
        """,
        ids,
    )
    for row in rows:
        value = _definition_payload(row)
        value["assignment"] = {
            "scope": "system" if row["scope_code"] == 0 else "member_rollup",
            "member_count": row["member_count"],
            "basis_kind": "direct" if row["basis_code"] == 0 else "member_rollup",
            "evidence_statuses": [
                status
                for status, bit in EVIDENCE_STATUS_BITS
                if int(row["evidence_status_mask"] or 0) & bit
            ],
            "min_confidence": row["min_confidence"],
            "max_confidence": row["max_confidence"],
        }
        result[int(row["system_id"])].append(value)
    _record_query(
        "system_tags", started, sum(len(values) for values in result.values())
    )
    return result


def source_summary_attached(
    con: sqlite3.Connection, system_ids: Iterable[int]
) -> dict[int, list[dict[str, Any]]]:
    started = time.perf_counter()
    ids = sorted({int(value) for value in system_ids})
    result = {system_id: [] for system_id in ids}
    if not ids or not is_attached(con):
        _record_query("source_summary", started, 0)
        return result
    placeholders = ",".join("?" for _ in ids)
    rows = con.execute(
        f"""
        SELECT s.system_id,s.contribution_kind,s.member_count,d.*
        FROM smart_tags.system_sources s
        JOIN smart_tags.source_definitions d USING(source_num)
        WHERE s.system_id IN ({placeholders})
        ORDER BY s.system_id,s.contribution_kind,d.publisher,d.source_id
        """,
        ids,
    )
    for row in rows:
        value = _source_payload(row)
        value["contribution_kind"] = row["contribution_kind"]
        value["member_count"] = row["member_count"]
        result[int(row["system_id"])].append(value)
    _record_query(
        "source_summary",
        started,
        sum(len(values) for values in result.values()),
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


def assignments_payload(
    build_id: str,
    system_id: int,
    *,
    offset: int = 0,
    limit: int = 100,
) -> dict[str, Any]:
    started = time.perf_counter()
    import duckdb

    paths = paths_for_build(build_id)
    manifest = _manifest(paths, build_id)
    def matching_rows(
        path: Path, *, row_offset: int, row_limit: int
    ) -> tuple[int, list[dict[str, Any]]]:
        try:
            with duckdb.connect(":memory:") as con:
                total = int(
                    con.execute(
                        "SELECT count(*) FROM read_parquet(?) WHERE system_id=?",
                        [str(path), system_id],
                    ).fetchone()[0]
                )
                cursor = con.execute(
                    "SELECT * FROM read_parquet(?) WHERE system_id=? "
                    "ORDER BY ALL LIMIT ? OFFSET ?",
                    [str(path), system_id, row_limit, row_offset],
                )
                columns = [description[0] for description in cursor.description]
                rows = [
                    dict(zip(columns, row, strict=True))
                    for row in cursor.fetchall()
                ]
                return total, rows
        except duckdb.Error as exc:
            _increment("assignment_artifact_failures")
            raise SmartTagsUnavailable(
                "Smart Tag assignment evidence is unavailable"
            ) from exc

    total, rows = matching_rows(
        paths.assignments, row_offset=offset, row_limit=limit
    )
    source_total, source_rows = matching_rows(
        paths.source_contributions, row_offset=0, row_limit=limit
    )
    next_offset = offset + len(rows)
    payload = {
        "schema_version": "spacegate.smart_tag_assignment_api.v2",
        "build_id": build_id,
        "registry_hash": manifest["registry_hash"],
        "system_id": system_id,
        "offset": offset,
        "limit": limit,
        "total": total,
        "next_offset": next_offset if next_offset < total else None,
        "assignments": rows,
        "source_contribution_total": source_total,
        "source_contributions": source_rows[:limit],
    }
    _record_query(
        "assignment_evidence",
        started,
        len(rows) + min(limit, len(source_rows)),
    )
    return payload
