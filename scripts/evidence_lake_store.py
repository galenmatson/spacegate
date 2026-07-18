#!/usr/bin/env python3
"""Build immutable raw snapshots and source-native typed Evidence Lake tables."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from evidence_lake_registry import (
    DEFAULT_REGISTRY,
    delimited_fields,
    json_record_fields,
    load_json,
    stable_hash,
    validate_registry,
)


RAW_CONTRACT = "spacegate.evidence_raw_snapshot.v1"
TYPED_CONTRACT = "spacegate.evidence_typed_snapshot.v1"
BASE_PARSER_CONTRACT_VERSION = "evidence_typed_cook_v4"
SOURCE_PARSER_CONTRACT_VERSIONS = {
    "tess.identity_and_candidate_evidence": "evidence_typed_cook_v5",
}
TABULAR_SUFFIXES = (".csv", ".csv.gz")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9._-]+", "_", value.lower()).strip("._-")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(child for child in path.rglob("*") if child.is_file())


def tree_sha256(path: Path) -> str:
    rows = []
    for child in tree_files(path):
        relative = child.name if path.is_file() else child.relative_to(path).as_posix()
        rows.append({"path": relative, "bytes": child.stat().st_size, "sha256": file_sha256(child)})
    return stable_hash(rows)


def json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def manifest_entries(manifest_dir: Path) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted(manifest_dir.glob("*_manifest.json")):
        payload = load_json(path)
        if not isinstance(payload, list):
            raise ValueError(f"manifest root must be a list: {path}")
        for entry in payload:
            if not isinstance(entry, dict) or not entry.get("source_name"):
                continue
            rows[(path.name, str(entry["source_name"]))] = entry
    return rows


def resolve_source_path(state_dir: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else state_dir / path


def hardlink_or_copy(source: Path, destination: Path, *, allow_hardlink: bool) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if allow_hardlink:
        try:
            os.link(source, destination)
            return "hardlink"
        except OSError:
            pass
    shutil.copy2(source, destination)
    return "copy"


def copy_tree_immutable(
    source: Path,
    destination: Path,
    *,
    allow_hardlink: bool,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    files: list[dict[str, Any]] = []
    methods = {"hardlink": 0, "copy": 0}
    base = source.parent if source.is_file() else source
    for child in tree_files(source):
        relative = child.name if source.is_file() else child.relative_to(base).as_posix()
        target = destination / relative
        method = hardlink_or_copy(child, target, allow_hardlink=allow_hardlink)
        methods[method] += 1
        files.append(
            {
                "path": relative,
                "bytes": target.stat().st_size,
                "sha256": file_sha256(target),
                "materialization": method,
            }
        )
    return files, methods


def source_snapshot_id(source: dict[str, Any], entries: list[dict[str, Any]]) -> str:
    identity = {
        "source_id": source["source_id"],
        "release_id": source["release_id"],
        "entries": [
            {
                "manifest": entry["_manifest"],
                "source_name": entry.get("source_name"),
                "sha256": entry.get("sha256"),
                "query_signature": entry.get("query_signature"),
                "url": entry.get("url"),
            }
            for entry in entries
        ],
    }
    return stable_hash(identity)[:24]


def selected_sources(registry: dict[str, Any], requested: set[str]) -> Iterable[dict[str, Any]]:
    for source in registry["sources"]:
        if source["state"] in {"planned", "disabled"}:
            continue
        if requested and source["source_id"] not in requested:
            continue
        yield source


def parser_contract_version(source: dict[str, Any]) -> str:
    return SOURCE_PARSER_CONTRACT_VERSIONS.get(
        str(source["source_id"]),
        BASE_PARSER_CONTRACT_VERSION,
    )


def build_raw_snapshot(
    source: dict[str, Any],
    current_entries: dict[tuple[str, str], dict[str, Any]],
    state_dir: Path,
    raw_root: Path,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for binding in source["manifest_entries"]:
        key = (binding["manifest"], binding["source_name"])
        if key not in current_entries:
            raise ValueError(f"missing manifest entry {key} for {source['source_id']}")
        entry = dict(current_entries[key])
        entry["_manifest"] = binding["manifest"]
        entries.append(entry)
    snapshot_id = source_snapshot_id(source, entries)
    source_root = raw_root / slug(source["source_id"]) / slug(source["release_id"])
    destination = source_root / snapshot_id
    manifest_path = destination / "snapshot_manifest.json"
    if destination.exists():
        if not manifest_path.exists():
            raise ValueError(f"immutable snapshot lacks manifest: {destination}")
        existing = load_json(manifest_path)
        if existing.get("snapshot_id") != snapshot_id or existing.get("source_id") != source["source_id"]:
            raise ValueError(f"immutable snapshot identity mismatch: {destination}")
        return existing

    source_root.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{snapshot_id}.", dir=source_root))
    artifact_reports: list[dict[str, Any]] = []
    try:
        for entry in entries:
            source_path = resolve_source_path(state_dir, str(entry.get("dest_path") or ""))
            if not source_path.exists():
                raise FileNotFoundError(source_path)
            artifact_root = temp_dir / "artifacts" / slug(str(entry["source_name"]))
            # A link to a mutable legacy path is not an immutable snapshot: an
            # in-place writer would mutate both names. Reuse blocks only when
            # the source itself is already in a content-addressed snapshot.
            allow_hardlink = "snapshots" in source_path.parts
            files, methods = copy_tree_immutable(
                source_path,
                artifact_root,
                allow_hardlink=allow_hardlink,
            )
            actual_sha = tree_sha256(artifact_root)
            expected_sha = str(entry.get("sha256") or "")
            if source_path.is_file() and len(files) == 1 and expected_sha:
                if files[0]["sha256"] != expected_sha:
                    raise ValueError(
                        f"source checksum mismatch for {entry['source_name']}: "
                        f"{files[0]['sha256']} != {expected_sha}"
                    )
            artifact_reports.append(
                {
                    "manifest": entry["_manifest"],
                    "source_name": entry["source_name"],
                    "source_version": entry.get("source_version"),
                    "url": entry.get("url"),
                    "query": entry.get("query"),
                    "query_signature": entry.get("query_signature"),
                    "retrieved_at": entry.get("retrieved_at"),
                    "expected_sha256": expected_sha or None,
                    "tree_sha256": actual_sha,
                    "expected_row_count": entry.get("row_count"),
                    "artifact_kind": "directory" if source_path.is_dir() else "file",
                    "artifact_path": f"artifacts/{slug(str(entry['source_name']))}",
                    "files": files,
                    "materialization_counts": methods,
                }
            )
        payload = {
            "schema_version": RAW_CONTRACT,
            "snapshot_id": snapshot_id,
            "source_id": source["source_id"],
            "release_id": source["release_id"],
            "registry_version": source.get("registry_version"),
            "created_at": utc_now(),
            "authority_roles": source["authority_roles"],
            "license": source["license"],
            "citation_url": source["citation_url"],
            "identity_namespaces": source["identity_namespaces"],
            "frame_epoch": source.get("frame_epoch"),
            "artifacts": artifact_reports,
        }
        payload["content_sha256"] = stable_hash(
            {
                "source_id": payload["source_id"],
                "release_id": payload["release_id"],
                "artifacts": artifact_reports,
            }
        )
        json_write(temp_dir / "snapshot_manifest.json", payload)
        os.replace(temp_dir, destination)
        return payload
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parquet_metadata(path: Path, con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    description = con.execute(f"describe select * from read_parquet({sql_string(str(path))})").fetchall()
    row_count = int(con.execute(f"select count(*) from read_parquet({sql_string(str(path))})").fetchone()[0])
    return {
        "row_count": row_count,
        "columns": [{"name": row[0], "type": row[1]} for row in description],
        "bytes": path.stat().st_size,
        "sha256": file_sha256(path),
    }


def delimiter_for(path: Path) -> str:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8-sig", errors="replace", newline="") as handle:
        header = handle.readline()
    try:
        return csv.Sniffer().sniff(header, delimiters=",;\t|").delimiter
    except csv.Error:
        return ","


def write_delimited_parquet(paths: list[Path], output: Path, con: duckdb.DuckDBPyConnection) -> list[str]:
    delimiters = {delimiter_for(path) for path in paths}
    if len(delimiters) != 1:
        raise ValueError(f"mixed delimiters in one source artifact: {sorted(delimiters)}")
    delimiter = next(iter(delimiters))
    source_fields = [delimited_fields(path) for path in paths]
    if any(fields != source_fields[0] for fields in source_fields[1:]):
        raise ValueError("field order differs across files in one source artifact")
    fields = source_fields[0]
    if len(fields) != len(set(fields)):
        raise ValueError("duplicate or empty source field names are not supported")
    values = ",".join(sql_string(str(path)) for path in paths)
    source_expr = f"[{values}]" if len(paths) > 1 else sql_string(str(paths[0]))
    con.execute(
        f"""
        copy (
          select *
          from read_csv(
            {source_expr},
            delim={sql_string(delimiter)},
            quote='"',
            escape='"',
            header=true,
            all_varchar=true,
            max_line_size=100000000,
            strict_mode=false,
            null_padding=false,
            ignore_errors=false,
            union_by_name=true,
            normalize_names=false
          )
        ) to {sql_string(str(output))}
        (format parquet, compression zstd, row_group_size 122880)
        """
    )
    return sorted(fields)


def write_delimited_continuation_parquet(
    artifacts: list[tuple[dict[str, Any], list[Path]]],
    output: Path,
    con: duckdb.DuckDBPyConnection,
    header_source: str,
) -> tuple[list[str], list[dict[str, Any]]]:
    by_name = {str(artifact["source_name"]): paths for artifact, paths in artifacts}
    header_paths = by_name.get(header_source) or []
    if len(header_paths) != 1:
        raise ValueError(f"continuation header artifact must contain one file: {header_source}")
    fields = delimited_fields(header_paths[0])
    if not fields or len(fields) != len(set(fields)):
        raise ValueError(f"invalid continuation header fields: {header_source}")
    delimiter = delimiter_for(header_paths[0])
    columns = "{" + ",".join(f"{sql_string(field)}:'VARCHAR'" for field in fields) + "}"
    scans: list[str] = []
    row_reports: list[dict[str, Any]] = []
    for artifact, paths in artifacts:
        if len(paths) != 1:
            raise ValueError(
                f"continuation artifact must contain one file: {artifact['source_name']}"
            )
        path = paths[0]
        if delimiter_for(path) != delimiter:
            raise ValueError(f"continuation delimiter drift: {artifact['source_name']}")
        is_header = artifact["source_name"] == header_source
        scan = (
            "select * from read_csv("
            f"{sql_string(str(path))}, delim={sql_string(delimiter)}, quote='\"', escape='\"', "
            f"header={'true' if is_header else 'false'}, auto_detect=false, columns={columns}, "
            "strict_mode=false, null_padding=false, ignore_errors=false, "
            "max_line_size=100000000, normalize_names=false)"
        )
        count = int(con.execute(f"select count(*) from ({scan})").fetchone()[0])
        row_reports.append(
            {
                "source_name": artifact["source_name"],
                "row_count": count,
                "header": is_header,
            }
        )
        scans.append(scan)
    con.execute(
        f"copy ({' union all '.join(scans)}) to {sql_string(str(output))} "
        "(format parquet, compression zstd, row_group_size 122880)"
    )
    return sorted(fields), row_reports


def cook_delimited_continuation(
    source: dict[str, Any],
    raw_manifest: dict[str, Any],
    raw_snapshot_dir: Path,
    output: Path,
    con: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    layout = source["schema_policy"]["artifact_layout"]
    source_names = [layout["header_artifact"], *(layout.get("continuation_artifacts") or [])]
    artifacts_by_name = {
        str(artifact["source_name"]): artifact for artifact in raw_manifest["artifacts"]
    }
    if set(source_names) != set(artifacts_by_name):
        raise ValueError(
            "continuation layout must account for every source artifact: "
            f"layout={sorted(source_names)} raw={sorted(artifacts_by_name)}"
        )
    artifacts = [
        (artifacts_by_name[name], artifact_input_files(raw_snapshot_dir, artifacts_by_name[name]))
        for name in source_names
    ]
    expected_fields, source_rows = write_delimited_continuation_parquet(
        artifacts,
        output,
        con,
        str(layout["header_artifact"]),
    )
    metadata = parquet_metadata(output, con)
    actual_fields = sorted(column["name"] for column in metadata["columns"])
    if actual_fields != expected_fields:
        raise ValueError("continuation field accounting mismatch")
    expected_rows = sum(int(report["row_count"]) for report in source_rows)
    if metadata["row_count"] != expected_rows:
        raise ValueError(
            f"continuation row accounting mismatch: {metadata['row_count']} != {expected_rows}"
        )
    return {
        "source_name": str(layout["table_name"]),
        "status": "typed",
        "parser": "duckdb_delimited_continuation_explicit_lexical_v1",
        "typing_status": "lexical_preserved_source_types_pending",
        "raw_tree_sha256": stable_hash(
            {artifact["source_name"]: artifact["tree_sha256"] for artifact, _ in artifacts}
        ),
        "source_row_accounting": source_rows,
        "parquet_path": f"tables/{output.name}",
        **metadata,
    }


def mast_arrow_type(source_type: str) -> pa.DataType:
    normalized = source_type.strip().lower()
    if normalized in {"int", "integer", "long"}:
        return pa.int64()
    if normalized in {"float", "double", "real"}:
        return pa.float64()
    if normalized in {"bool", "boolean"}:
        return pa.bool_()
    if normalized in {"string", "char", "varchar"}:
        return pa.string()
    raise ValueError(f"unsupported MAST field type: {source_type!r}")


def mast_source_schema(paths: list[Path]) -> tuple[pa.Schema, list[dict[str, str]]]:
    ordered_names: list[str] = []
    declared: dict[str, str] = {}
    for path in paths:
        payload = load_json(path)
        fields = payload.get("fields") if isinstance(payload, dict) else None
        if not isinstance(fields, list):
            raise ValueError(f"MAST JSON artifact lacks field declarations: {path}")
        for field in fields:
            if not isinstance(field, dict) or not field.get("name") or not field.get("type"):
                raise ValueError(f"invalid MAST field declaration in {path}: {field!r}")
            name = str(field["name"])
            source_type = str(field["type"]).lower()
            if name in declared and declared[name] != source_type:
                raise ValueError(
                    f"MAST field type drift for {name}: {declared[name]} != {source_type}"
                )
            if name not in declared:
                ordered_names.append(name)
                declared[name] = source_type
    schema = pa.schema([pa.field(name, mast_arrow_type(declared[name])) for name in ordered_names])
    return schema, [{"name": name, "source_type": declared[name]} for name in ordered_names]


def mast_typed_rows(rows: list[dict[str, Any]], schema: pa.Schema, path: Path) -> pa.Table:
    expected = set(schema.names)
    extras = sorted({name for row in rows for name in row} - expected)
    if extras:
        raise ValueError(f"MAST rows contain undeclared fields in {path}: {extras[:20]}")
    columns: list[pa.Array] = []
    for field in schema:
        values = [row.get(field.name) for row in rows]
        if pa.types.is_string(field.type):
            values = [None if value is None else str(value) for value in values]
        columns.append(pa.array(values, type=field.type, safe=True))
    return pa.Table.from_arrays(columns, schema=schema)


def write_mast_json_parquet(paths: list[Path], output: Path) -> list[dict[str, str]]:
    schema, source_schema = mast_source_schema(paths)
    writer: pq.ParquetWriter | None = None
    try:
        for path in paths:
            payload = load_json(path)
            rows = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(rows, list) or not rows:
                continue
            if not all(isinstance(row, dict) for row in rows):
                raise ValueError(f"MAST data rows must be objects: {path}")
            table = mast_typed_rows(rows, schema, path)
            if writer is None:
                writer = pq.ParquetWriter(output, schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError("MAST JSON artifact contained no data rows")
    return source_schema


def artifact_input_files(snapshot_dir: Path, artifact: dict[str, Any]) -> list[Path]:
    root = snapshot_dir / artifact["artifact_path"]
    return [root / item["path"] for item in artifact["files"]]


def cook_artifact(
    source: dict[str, Any],
    snapshot_dir: Path,
    artifact: dict[str, Any],
    output: Path,
    con: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    files = artifact_input_files(snapshot_dir, artifact)
    tabular = [path for path in files if path.name.endswith(TABULAR_SUFFIXES)]
    json_files = [path for path in files if path.suffix == ".json"]
    parser = ""
    expected_fields: list[str] = []
    if tabular:
        expected_fields = write_delimited_parquet(tabular, output, con)
        parser = "duckdb_read_csv_explicit_lexical_shape_checked_v4"
    elif json_files and source["schema_policy"]["kind"] == "mixed_tabular_snapshot":
        source_schema = write_mast_json_parquet(json_files, output)
        parser = "mast_json_declared_schema_arrow_v2"
        expected_fields = sorted(field["name"] for field in source_schema)
    else:
        return {
            "source_name": artifact["source_name"],
            "status": "parser_pending",
            "reason": f"no E1 parser for {source['schema_policy']['kind']}",
            "raw_tree_sha256": artifact["tree_sha256"],
        }
    metadata = parquet_metadata(output, con)
    actual_fields = sorted(column["name"] for column in metadata["columns"])
    if actual_fields != expected_fields:
        missing = sorted(set(expected_fields) - set(actual_fields))
        extra = sorted(set(actual_fields) - set(expected_fields))
        raise ValueError(
            f"field accounting mismatch for {artifact['source_name']}: "
            f"expected={len(expected_fields)} actual={len(actual_fields)} "
            f"missing={missing[:20]} extra={extra[:20]}"
        )
    expected_rows = artifact.get("expected_row_count")
    if expected_rows is not None and int(expected_rows) != metadata["row_count"]:
        raise ValueError(
            f"row count mismatch for {artifact['source_name']}: "
            f"{metadata['row_count']} != {expected_rows}"
        )
    return {
        "source_name": artifact["source_name"],
        "status": "typed",
        "parser": parser,
        "typing_status": (
            "source_schema_typed"
            if parser == "mast_json_declared_schema_arrow_v2"
            else "lexical_preserved_source_types_pending"
        ),
        **({"source_schema": source_schema} if parser == "mast_json_declared_schema_arrow_v2" else {}),
        "raw_tree_sha256": artifact["tree_sha256"],
        "parquet_path": f"tables/{output.name}",
        **metadata,
    }


def latest_snapshot_dir(raw_root: Path, source: dict[str, Any]) -> Path:
    root = raw_root / slug(source["source_id"]) / slug(source["release_id"])
    candidates = [
        path for path in root.iterdir()
        if path.is_dir() and (path / "snapshot_manifest.json").exists()
    ] if root.exists() else []
    if not candidates:
        raise FileNotFoundError(f"no raw snapshot for {source['source_id']}")
    return max(
        candidates,
        key=lambda path: (
            str(load_json(path / "snapshot_manifest.json").get("created_at") or ""),
            path.name,
        ),
    )


def latest_typed_snapshot_dir(typed_root: Path, source: dict[str, Any], raw_snapshot_id: str) -> Path:
    root = typed_root / slug(source["source_id"]) / slug(source["release_id"]) / raw_snapshot_id
    candidates = [
        path for path in root.iterdir() if path.is_dir() and (path / "typed_manifest.json").exists()
    ] if root.exists() else []
    if not candidates:
        raise FileNotFoundError(f"no typed snapshot for {source['source_id']}:{raw_snapshot_id}")
    return max(
        candidates,
        key=lambda path: (
            str(load_json(path / "typed_manifest.json").get("created_at") or ""),
            path.name,
        ),
    )


def build_typed_snapshot(source: dict[str, Any], raw_snapshot_dir: Path, typed_root: Path) -> dict[str, Any]:
    raw_manifest = load_json(raw_snapshot_dir / "snapshot_manifest.json")
    snapshot_id = raw_manifest["snapshot_id"]
    root = typed_root / slug(source["source_id"]) / slug(source["release_id"])
    parser_version = parser_contract_version(source)
    typed_snapshot_id = stable_hash(
        {
            "raw_snapshot_id": snapshot_id,
            "raw_content_sha256": raw_manifest["content_sha256"],
            "parser_contract_version": parser_version,
        }
    )[:24]
    destination = root / snapshot_id / typed_snapshot_id
    manifest_path = destination / "typed_manifest.json"
    if destination.exists():
        if not manifest_path.exists():
            raise ValueError(f"immutable typed snapshot lacks manifest: {destination}")
        return load_json(manifest_path)

    root.mkdir(parents=True, exist_ok=True)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{snapshot_id}.", dir=root))
    (temp_dir / "tables").mkdir()
    con = duckdb.connect()
    con.execute("set preserve_insertion_order=false")
    reports: list[dict[str, Any]] = []
    try:
        layout = source["schema_policy"].get("artifact_layout") or {}
        if layout.get("kind") == "delimited_continuation":
            output = temp_dir / "tables" / f"{slug(str(layout['table_name']))}.parquet"
            try:
                reports.append(
                    cook_delimited_continuation(
                        source, raw_manifest, raw_snapshot_dir, output, con
                    )
                )
            except Exception as exc:
                if output.exists():
                    output.unlink()
                reports.append(
                    {
                        "source_name": str(layout["table_name"]),
                        "status": "parser_error",
                        "reason": f"{type(exc).__name__}: {exc}",
                    }
                )
        else:
            for artifact in raw_manifest["artifacts"]:
                output = temp_dir / "tables" / f"{slug(str(artifact['source_name']))}.parquet"
                try:
                    report = cook_artifact(source, raw_snapshot_dir, artifact, output, con)
                except Exception as exc:
                    report = {
                        "source_name": artifact["source_name"],
                        "status": "parser_error",
                        "reason": f"{type(exc).__name__}: {exc}",
                        "raw_tree_sha256": artifact["tree_sha256"],
                    }
                if report["status"] != "typed" and output.exists():
                    output.unlink()
                reports.append(report)
        payload = {
            "schema_version": TYPED_CONTRACT,
            "snapshot_id": snapshot_id,
            "typed_snapshot_id": typed_snapshot_id,
            "parser_contract_version": parser_version,
            "source_id": source["source_id"],
            "release_id": source["release_id"],
            "raw_content_sha256": raw_manifest["content_sha256"],
            "created_at": utc_now(),
            "tables": reports,
        }
        payload["content_sha256"] = stable_hash(
            {
                "source_id": payload["source_id"],
                "release_id": payload["release_id"],
                "raw_content_sha256": payload["raw_content_sha256"],
                "tables": reports,
            }
        )
        json_write(temp_dir / "typed_manifest.json", payload)
        os.replace(temp_dir, destination)
        return payload
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        con.close()


def verify_snapshot(raw_dir: Path, typed_dir: Path | None = None) -> dict[str, Any]:
    raw_manifest = load_json(raw_dir / "snapshot_manifest.json")
    errors: list[str] = []
    for artifact in raw_manifest["artifacts"]:
        root = raw_dir / artifact["artifact_path"]
        actual = tree_sha256(root)
        if actual != artifact["tree_sha256"]:
            errors.append(f"raw checksum mismatch: {artifact['source_name']}")
    typed_manifest = None
    if typed_dir:
        typed_manifest = load_json(typed_dir / "typed_manifest.json")
        if typed_manifest.get("raw_content_sha256") != raw_manifest.get("content_sha256"):
            errors.append("typed snapshot points to different raw content")
        con = duckdb.connect()
        try:
            for table in typed_manifest["tables"]:
                if table["status"] != "typed":
                    errors.append(
                        f"typed table incomplete: {table['source_name']} ({table['status']})"
                    )
                    continue
                path = typed_dir / table["parquet_path"]
                if not path.exists():
                    errors.append(f"typed table missing: {table['source_name']}")
                    continue
                current = parquet_metadata(path, con)
                for key in ("row_count", "sha256", "bytes"):
                    if current[key] != table[key]:
                        errors.append(f"typed {key} mismatch: {table['source_name']}")
        finally:
            con.close()
    return {
        "schema_version": "spacegate.evidence_snapshot_verification.v1",
        "source_id": raw_manifest["source_id"],
        "release_id": raw_manifest["release_id"],
        "snapshot_id": raw_manifest["snapshot_id"],
        "status": "pass" if not errors else "fail",
        "errors": errors,
        "typed": typed_manifest is not None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--source", action="append", default=[])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("snapshot")
    cook = sub.add_parser("cook")
    cook.add_argument("--report", type=Path)
    cook.add_argument("--allow-incomplete", action="store_true")
    verify = sub.add_parser("verify")
    verify.add_argument("--report", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_json(args.registry)
    errors = validate_registry(registry)
    if errors:
        raise SystemExit("\n".join(errors))
    for source in registry["sources"]:
        source["registry_version"] = registry["registry_version"]
    requested = set(args.source)
    sources = list(selected_sources(registry, requested))
    if requested - {source["source_id"] for source in sources}:
        unknown = sorted(requested - {source["source_id"] for source in sources})
        raise SystemExit(f"unknown or non-active source IDs: {unknown}")

    raw_root = args.state_dir / "raw" / "evidence_lake_v2"
    typed_root = args.state_dir / "typed" / "evidence_lake_v2"
    current_entries = manifest_entries(args.state_dir / "reports" / "manifests")
    reports: list[dict[str, Any]] = []
    command_failed = False

    for source in sources:
        if args.command == "snapshot":
            report = build_raw_snapshot(source, current_entries, args.state_dir, raw_root)
            print(f"raw {source['source_id']} {report['snapshot_id']}")
            reports.append(report)
        elif args.command == "cook":
            raw_dir = latest_snapshot_dir(raw_root, source)
            try:
                report = build_typed_snapshot(source, raw_dir, typed_root)
                typed = sum(1 for table in report["tables"] if table["status"] == "typed")
                pending = sum(1 for table in report["tables"] if table["status"] != "typed")
                command_failed = command_failed or pending > 0
                print(
                    f"typed {source['source_id']} {report['snapshot_id']} "
                    f"tables={typed} pending={pending}"
                )
                reports.append(report)
            except Exception as exc:
                command_failed = True
                report = {
                    "source_id": source["source_id"],
                    "release_id": source["release_id"],
                    "status": "source_error",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
                print(f"typed {source['source_id']} ERROR {report['reason']}")
                reports.append(report)
        elif args.command == "verify":
            raw_dir = latest_snapshot_dir(raw_root, source)
            try:
                typed_dir = latest_typed_snapshot_dir(typed_root, source, raw_dir.name)
            except FileNotFoundError:
                typed_dir = None
            report = verify_snapshot(raw_dir, typed_dir)
            print(f"verify {source['source_id']} {report['status']}")
            reports.append(report)

    if args.command == "verify":
        payload = {
            "schema_version": "spacegate.evidence_lake_verification.v1",
            "generated_at": utc_now(),
            "status": "pass" if all(report["status"] == "pass" for report in reports) else "fail",
            "sources": reports,
        }
        json_write(args.report, payload)
        return 0 if payload["status"] == "pass" else 1
    if args.command == "cook":
        table_reports = [
            table
            for report in reports
            for table in report.get("tables", [])
        ]
        payload = {
            "schema_version": "spacegate.evidence_lake_typed_cook_report.v1",
            "generated_at": utc_now(),
            "status": "pass" if not command_failed else "incomplete",
            "source_count": len(reports),
            "typed_table_count": sum(
                table.get("status") == "typed" for table in table_reports
            ),
            "incomplete_table_count": sum(
                table.get("status") != "typed" for table in table_reports
            ),
            "sources": reports,
        }
        report_path = args.report or (
            args.state_dir / "reports" / "evidence_lake_v2" / "e1_typed_cook_report.json"
        )
        json_write(report_path, payload)
        return 0 if not command_failed or args.allow_incomplete else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
