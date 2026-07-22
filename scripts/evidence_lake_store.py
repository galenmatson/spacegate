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
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pypdf import PdfReader

from evidence_lake_registry import (
    DEFAULT_REGISTRY,
    delimited_fields,
    json_record_fields,
    load_json,
    stable_hash,
    validate_registry,
)
from evidence_lake_native import (
    bytes_sha256,
    parse_cds_readme,
    parse_cds_readme_text,
    parse_wds_format,
    safe_tar_members,
    tar_member_bytes,
    write_archive_member_index_parquet,
    write_atnf_catalog_parquet,
    write_atnf_glitches_parquet,
    write_atnf_references_parquet,
    write_document_lines_parquet,
    write_fits_table_parquet,
    write_fixed_width_parquet,
    write_fixed_width_text_parquet,
    write_green_snr_parquet,
    write_html_table_parquet,
    write_mcgill_magnetar_html_parquet,
    write_oec_archive_parquet,
    write_text_lines_parquet,
    write_tokenized_parquet,
    write_votable_files_parquet,
)


RAW_CONTRACT = "spacegate.evidence_raw_snapshot.v1"
TYPED_CONTRACT = "spacegate.evidence_typed_snapshot.v1"
BASE_PARSER_CONTRACT_VERSION = "evidence_typed_cook_v5"
SOURCE_PARSER_CONTRACT_VERSIONS = {
    "gaia.dr3.gaia_source": "evidence_typed_cook_votable_v1",
    "gaia.dr3.astrophysical_parameters": "evidence_typed_cook_votable_v1",
    "gaia.dr3.astrophysical_parameters_supp": "evidence_typed_cook_votable_v1",
    "gaia.dr3.non_single_star": "evidence_typed_cook_votable_v1",
    "gaia.dr3.variability": "evidence_typed_cook_votable_v1",
    "gaia.dr3.external_crossmatches": "evidence_typed_cook_votable_v1",
    "distance.gaia_edr3_bailer_jones": "evidence_typed_cook_votable_case_collision_v2",
    "clusters.hunt_reffert_2024": "evidence_typed_cook_votable_v1",
    "identity.simbad": "evidence_typed_cook_tap_csv_v1",
    "spectroscopy.apogee_dr17": "evidence_typed_cook_fits_multi_hdu_v2",
    "spectroscopy.galah_dr4": "evidence_typed_cook_fits_v1",
    "spectroscopy.lamost_dr11": "evidence_typed_cook_fits_v1",
    "multiplicity.el_badry_2021_wide_binary": "evidence_typed_cook_fits_v1",
    "nasa_exoplanet_archive.planetary_systems": "evidence_typed_cook_tap_csv_v1",
    "naming.iau_wgsn": "evidence_typed_cook_html_table_v2",
    "standards.iau_2015_resolution_b3": "evidence_typed_cook_iau_b3_pdf_v1",
    "classification.gcvs": "evidence_typed_cook_gcvs_cds_layout_delimiter_v2",
    "tess.identity_and_candidate_evidence": "evidence_typed_cook_member_lineage_v7",
    "multiplicity.wds": "evidence_typed_cook_wds_v1",
    "multiplicity.sb9": "evidence_typed_cook_cds_v2",
    "clusters.cantat_gaudin_2020": "evidence_typed_cook_cds_v2",
    "classification.vsx": "evidence_typed_cook_cds_v2",
    "ultracool.gaia_dr3_sample": "evidence_typed_cook_cds_v2",
    "extended.openngc_and_nebulae": "evidence_typed_cook_cds_v2",
    "multiplicity.msc": "evidence_typed_cook_msc_v2",
    "multiplicity.orb6": "evidence_typed_cook_orb6_v1",
    "multiplicity.debcat": "evidence_typed_cook_debcat_v1",
    "compact.atnf": "evidence_typed_cook_atnf_v2",
    "compact.gaia_edr3_white_dwarf": "evidence_typed_cook_fits_v1",
    "extended.green_snr": "evidence_typed_cook_green_snr_v2",
    "exoplanet_lifecycle.open_exoplanet_catalogue": "evidence_typed_cook_oec_xml_v1",
    "compact.mcgill_magnetar": "evidence_typed_cook_mcgill_bundle_v4",
    "infrared.catwise2020_targeted": "evidence_typed_cook_votable_member_lineage_v1",
    "infrared.allwise_targeted": "evidence_typed_cook_votable_member_lineage_v1",
}
TABULAR_SUFFIXES = (".csv", ".csv.gz")
VOTABLE_SUFFIXES = (".vot", ".vot.gz", ".votable", ".votable.gz")
FITS_SUFFIXES = (".fits", ".fits.gz", ".fit", ".fit.gz")
ACQUISITION_METADATA_FILES = {"product_manifest.json"}


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


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


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


def write_delimited_parquet(
    paths: list[Path],
    output: Path,
    con: duckdb.DuckDBPyConnection,
    *,
    member_lineage_field: str | None = None,
) -> list[str]:
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
    if member_lineage_field and member_lineage_field in fields:
        raise ValueError(
            f"member lineage field collides with a source field: {member_lineage_field}"
        )
    values = ",".join(sql_string(str(path)) for path in paths)
    source_expr = f"[{values}]" if len(paths) > 1 else sql_string(str(paths[0]))
    projection = "*"
    filename_option = ""
    if member_lineage_field:
        member_cases = " ".join(
            f"when filename={sql_string(str(path))} then {sql_string(path.name)}"
            for path in paths
        )
        projection = (
            "* exclude (filename), "
            f"case {member_cases} else error('unaccounted source member') end "
            f"as {sql_identifier(member_lineage_field)}"
        )
        filename_option = ", filename=true"
    con.execute(
        f"""
        copy (
          select {projection}
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
            {filename_option}
          )
        ) to {sql_string(str(output))}
        (format parquet, compression zstd, row_group_size 122880)
        """
    )
    return sorted([*fields, *([member_lineage_field] if member_lineage_field else [])])


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


def single_artifact_file(snapshot_dir: Path, artifact: dict[str, Any]) -> Path:
    files = [
        path
        for path in artifact_input_files(snapshot_dir, artifact)
        if path.name not in ACQUISITION_METADATA_FILES
    ]
    if len(files) != 1:
        raise ValueError(
            f"source-native artifact must contain exactly one file: {artifact['source_name']}"
        )
    return files[0]


def resolve_cds_table(
    data_path: Path,
    tables: dict[str, list[dict[str, Any]]],
) -> tuple[str, list[dict[str, Any]]]:
    data_name = data_path.name.removesuffix(".gz")
    if data_name in tables:
        return data_name, tables[data_name]
    data_stem = Path(data_name).stem.lower()
    matches = [
        name
        for name in tables
        if Path(name).stem.lower() in data_stem or data_stem in Path(name).stem.lower()
    ]
    if len(matches) == 1:
        return matches[0], tables[matches[0]]
    if len(tables) == 1:
        name = next(iter(tables))
        return name, tables[name]
    raise ValueError(
        f"cannot resolve CDS schema section for {data_path.name}: {sorted(tables)}"
    )


def documented_artifact_names(source: dict[str, Any]) -> set[str]:
    policy = source["schema_policy"]
    names = set(str(value) for value in (policy.get("readme_bindings") or {}).values())
    names.update(str(value) for value in policy.get("document_artifacts") or [])
    if policy.get("format_artifact"):
        names.add(str(policy["format_artifact"]))
    return names


def cook_documented_artifact(
    source: dict[str, Any],
    snapshot_dir: Path,
    artifact: dict[str, Any],
    artifacts_by_name: dict[str, dict[str, Any]],
    output: Path,
    con: duckdb.DuckDBPyConnection,
) -> dict[str, Any] | None:
    source_name = str(artifact["source_name"])
    policy = source["schema_policy"]
    if source_name in documented_artifact_names(source):
        path = single_artifact_file(snapshot_dir, artifact)
        write_document_lines_parquet(path, output)
        metadata = parquet_metadata(output, con)
        return {
            "source_name": source_name,
            "status": "typed",
            "parser": "source_document_lines_v1",
            "typing_status": "source_schema_document",
            "raw_tree_sha256": artifact["tree_sha256"],
            "parquet_path": f"tables/{output.name}",
            **metadata,
        }

    schema_fields: list[dict[str, Any]] | None = None
    schema_document: dict[str, Any] | None = None
    schema_section: str | None = None
    record_pattern: re.Pattern[str] | None = None
    readme_name = (policy.get("readme_bindings") or {}).get(source_name)
    if readme_name:
        schema_document = artifacts_by_name.get(str(readme_name))
        if not schema_document:
            raise ValueError(f"missing registered schema document: {readme_name}")
        readme_path = single_artifact_file(snapshot_dir, schema_document)
        data_path = single_artifact_file(snapshot_dir, artifact)
        schema_section, schema_fields = resolve_cds_table(
            data_path,
            parse_cds_readme(readme_path),
        )
    elif source_name != policy.get("format_artifact") and policy.get("format_artifact"):
        format_name = str(policy["format_artifact"])
        schema_document = artifacts_by_name.get(format_name)
        if not schema_document:
            raise ValueError(f"missing registered format document: {format_name}")
        schema_fields = parse_wds_format(single_artifact_file(snapshot_dir, schema_document))
        schema_section = "WDS BIBLE COLUMN Format"
        record_pattern = re.compile(r"^[0-9]{5}[+-][0-9]{4}")
    else:
        return None

    data_path = single_artifact_file(snapshot_dir, artifact)
    write_report = write_fixed_width_parquet(
        data_path,
        schema_fields,
        output,
        record_pattern=record_pattern,
        trailing_layout_delimiters=tuple(policy.get("trailing_layout_delimiters") or ()),
    )
    metadata = parquet_metadata(output, con)
    if metadata["row_count"] != write_report["row_count"]:
        raise ValueError("fixed-width Parquet row count differs from source row accounting")
    return {
        "source_name": source_name,
        "status": "typed",
        "parser": (
            "documented_fixed_width_lexical_layout_delimiter_v2"
            if policy.get("trailing_layout_delimiters")
            else "documented_fixed_width_lexical_v1"
        ),
        "typing_status": "source_schema_lexical",
        "raw_tree_sha256": artifact["tree_sha256"],
        "schema_document_source_name": schema_document["source_name"],
        "schema_document_sha256": schema_document["tree_sha256"],
        "schema_section": schema_section,
        "source_schema": schema_fields,
        "source_row_accounting": write_report["source_row_accounting"],
        "parquet_path": f"tables/{output.name}",
        **metadata,
    }


ORB6_FIELDS = [
    "ra_j2000_raw",
    "dec_j2000_raw",
    "wds_id",
    "discoverer_designation",
    "ads_id",
    "hd_id",
    "hip_id",
    "primary_magnitude_raw",
    "primary_magnitude_flag",
    "secondary_magnitude_raw",
    "secondary_magnitude_flag",
    "period_raw",
    "period_unit",
    "period_uncertainty_raw",
    "semimajor_axis_raw",
    "semimajor_axis_unit_flag",
    "semimajor_axis_uncertainty_raw",
    "inclination_deg_raw",
    "inclination_uncertainty_raw",
    "ascending_node_deg_raw",
    "ascending_node_flag",
    "ascending_node_uncertainty_raw",
    "periastron_epoch_raw",
    "periastron_epoch_unit",
    "periastron_epoch_uncertainty_raw",
    "eccentricity_raw",
    "eccentricity_uncertainty_raw",
    "longitude_periastron_deg_raw",
    "longitude_periastron_uncertainty_raw",
    "equinox_raw",
    "last_observed_year_raw",
    "orbit_grade_raw",
    "notes_flag",
    "reference_code",
    "plot_filename",
]

DEBCAT_FIELDS = [
    "system_name",
    "spectral_type_1",
    "spectral_type_2",
    "period_days_raw",
    "visual_magnitude_raw",
    "b_minus_v_raw",
    "log_mass_1_raw",
    "log_mass_1_uncertainty_raw",
    "log_mass_2_raw",
    "log_mass_2_uncertainty_raw",
    "log_radius_1_raw",
    "log_radius_1_uncertainty_raw",
    "log_radius_2_raw",
    "log_radius_2_uncertainty_raw",
    "log_surface_gravity_1_raw",
    "log_surface_gravity_1_uncertainty_raw",
    "log_surface_gravity_2_raw",
    "log_surface_gravity_2_uncertainty_raw",
    "log_temperature_1_raw",
    "log_temperature_1_uncertainty_raw",
    "log_temperature_2_raw",
    "log_temperature_2_uncertainty_raw",
    "log_luminosity_1_raw",
    "log_luminosity_1_uncertainty_raw",
    "log_luminosity_2_raw",
    "log_luminosity_2_uncertainty_raw",
    "metallicity_raw",
    "metallicity_uncertainty_raw",
]


def source_native_report(
    *,
    source_name: str,
    parser: str,
    typing_status: str,
    raw_tree_sha256: str,
    output: Path,
    con: duckdb.DuckDBPyConnection,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = parquet_metadata(output, con)
    return {
        "source_name": source_name,
        "status": "typed",
        "parser": parser,
        "typing_status": typing_status,
        "raw_tree_sha256": raw_tree_sha256,
        "parquet_path": f"tables/{output.name}",
        **(details or {}),
        **metadata,
    }


def cook_iau_2015_resolution_b3(
    artifact: dict[str, Any],
    path: Path,
    table_dir: Path,
    con: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    reader = PdfReader(path)
    if len(reader.pages) != 6:
        raise ValueError(f"IAU 2015 Resolution B3 page-count drift: {len(reader.pages)}")
    pages = [page.extract_text() for page in reader.pages]
    if any(not text for text in pages):
        raise ValueError("IAU 2015 Resolution B3 contains an unextractable page")

    # The immutable PDF hash is checked before this parser runs. These source
    # fragments make the reviewed extraction fail closed if PDF text semantics
    # or the PDF extraction dependency changes.
    constants = [
        ("Sun", "nominal_solar_radius", "695700000", "m", 2, "⊙ = 6.957× 108 m"),
        ("Sun", "nominal_total_solar_irradiance", "1361", "W m-2", 2, "⊙ = 1361 W m−2"),
        ("Sun", "nominal_solar_luminosity", "3.828e26", "W", 2, "⊙ = 3.828× 1026 W"),
        ("Sun", "nominal_solar_effective_temperature", "5772", "K", 2, "eﬀ⊙ = 5772 K"),
        ("Sun", "nominal_solar_mass_parameter", "1.3271244e20", "m3 s-2", 2, "⊙ = 1.327 124 4× 1020 m3s−2"),
        ("Earth", "nominal_terrestrial_equatorial_radius", "6378100", "m", 3, "eE = 6.3781× 106 m"),
        ("Earth", "nominal_terrestrial_polar_radius", "6356800", "m", 3, "pE = 6.3568× 106 m"),
        ("Jupiter", "nominal_jovian_equatorial_radius", "71492000", "m", 3, "eJ = 7.1492× 107 m"),
        ("Jupiter", "nominal_jovian_polar_radius", "66854000", "m", 3, "pJ = 6.6854× 107 m"),
        ("Earth", "nominal_terrestrial_mass_parameter", "3.986004e14", "m3 s-2", 3, "E = 3.986 004× 1014 m3 s−2"),
        ("Jupiter", "nominal_jovian_mass_parameter", "1.2668653e17", "m3 s-2", 3, "J = 1.266 865 3× 1017 m3 s−2"),
    ]
    rows: list[dict[str, Any]] = []
    for subject, parameter, value, unit, page_number, fragment in constants:
        if fragment not in pages[page_number - 1]:
            raise ValueError(f"IAU 2015 Resolution B3 extraction drift: {parameter}")
        rows.append(
            {
                "subject_name": subject,
                "object_kind": "reference_standard",
                "parameter_name": parameter,
                "parameter_occurrence": 1,
                "value_raw": value,
                "attributes_json": json.dumps(
                    {
                        "exact_by_definition": True,
                        "source_semantics": "nominal_conversion_constant",
                        "unit": unit,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "fact_semantics": "nominal_conversion_constant_exact_by_definition",
                "page_number": page_number,
                "source_fragment": fragment,
            }
        )

    estimate_fragment = "Teﬀ,⊙ = 5772.0 (± 0.8) K."
    if estimate_fragment not in pages[5]:
        raise ValueError("IAU 2015 Resolution B3 solar-temperature estimate drift")
    rows.append(
        {
            "subject_name": "Sun",
            "object_kind": "star",
            "parameter_name": "effective_temperature",
            "parameter_occurrence": 1,
            "value_raw": "5772.0",
            "attributes_json": json.dumps(
                {
                    "errorminus": "0.8",
                    "errorplus": "0.8",
                    "exact_by_definition": False,
                    "source_semantics": "current_2015_best_estimate",
                    "unit": "K",
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            "fact_semantics": "published_current_best_estimate_not_nominal_constant",
            "page_number": 6,
            "source_fragment": estimate_fragment,
        }
    )
    schema = pa.schema(
        [
            ("subject_name", pa.string()),
            ("object_kind", pa.string()),
            ("parameter_name", pa.string()),
            ("parameter_occurrence", pa.int32()),
            ("value_raw", pa.string()),
            ("attributes_json", pa.string()),
            ("fact_semantics", pa.string()),
            ("page_number", pa.int32()),
            ("source_fragment", pa.string()),
        ]
    )
    output = table_dir / "iau_2015_resolution_b3_constants.parquet"
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        output,
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
    )
    return [
        source_native_report(
            source_name="iau_2015_resolution_b3_constants",
            parser="pypdf_6_14_2_reviewed_resolution_b3_v1",
            typing_status="reviewed_source_document_constants_and_estimate",
            raw_tree_sha256=artifact["tree_sha256"],
            output=output,
            con=con,
            details={
                "source_row_accounting": {
                    "nominal_conversion_constants": len(constants),
                    "published_current_best_estimates": 1,
                    "rows_written": len(rows),
                }
            },
        )
    ]


def cook_msc_archive(
    artifact: dict[str, Any],
    path: Path,
    table_dir: Path,
    con: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    expected_members = {"Readme", "comp.tsv", "sys.tsv", "orb.tsv", "notes.tsv"}
    with tarfile.open(path, "r:gz") as archive:
        members = {member.name for member in safe_tar_members(archive)}
        if members != expected_members:
            raise ValueError(
                f"MSC archive membership drift: missing={sorted(expected_members - members)} "
                f"extra={sorted(members - expected_members)}"
            )
        dispositions = {
            "Readme": "source_schema_document",
            "comp.tsv": "typed_data",
            "sys.tsv": "typed_data",
            "orb.tsv": "typed_data",
            "notes.tsv": "typed_data",
        }
        index_output = table_dir / "msc_archive_members.parquet"
        index_result = write_archive_member_index_parquet(
            archive, index_output, dispositions=dispositions
        )
        reports.append(
            source_native_report(
                source_name="msc_archive_members",
                parser="validated_tar_member_index_v1",
                typing_status="archive_member_index",
                raw_tree_sha256=artifact["tree_sha256"],
                output=index_output,
                con=con,
                details={"source_row_accounting": index_result},
            )
        )

        readme_bytes = tar_member_bytes(archive, "Readme")
        readme_text = readme_bytes.decode("utf-8", errors="replace")
        readme_output = table_dir / "msc_readme.parquet"
        readme_result = write_text_lines_parquet(readme_text, readme_output)
        reports.append(
            source_native_report(
                source_name="msc_readme",
                parser="source_document_lines_v1",
                typing_status="source_schema_document",
                raw_tree_sha256=artifact["tree_sha256"],
                output=readme_output,
                con=con,
                details={
                    "archive_member": "Readme",
                    "archive_member_sha256": bytes_sha256(readme_bytes),
                    "source_row_accounting": readme_result,
                },
            )
        )
        schemas = parse_cds_readme_text(readme_text)
        for member_name in ("comp.tsv", "sys.tsv", "orb.tsv", "notes.tsv"):
            fields = schemas.get(member_name)
            if not fields:
                raise ValueError(f"MSC Readme lacks schema for {member_name}")
            member_bytes = tar_member_bytes(archive, member_name)
            member_text = member_bytes.decode("utf-8", errors="replace")
            output = table_dir / f"msc_{Path(member_name).stem}.parquet"
            result = write_fixed_width_text_parquet(member_text, fields, output)
            reports.append(
                source_native_report(
                    source_name=f"msc_{Path(member_name).stem}",
                    parser="documented_archive_fixed_width_lexical_v1",
                    typing_status="source_schema_lexical",
                    raw_tree_sha256=artifact["tree_sha256"],
                    output=output,
                    con=con,
                    details={
                        "archive_member": member_name,
                        "archive_member_sha256": bytes_sha256(member_bytes),
                        "schema_document_member": "Readme",
                        "schema_document_sha256": bytes_sha256(readme_bytes),
                        "source_schema": fields,
                        "source_row_accounting": result["source_row_accounting"],
                    },
                )
            )
    return reports


def cook_atnf_archive(
    artifact: dict[str, Any],
    path: Path,
    table_dir: Path,
    con: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    data_members = {
        "psrcat_tar/README": "source_document",
        "psrcat_tar/psrcat.db": "typed_data",
        "psrcat_tar/glitch.db": "typed_data",
        "psrcat_tar/psrcat_ref": "typed_data",
    }
    with tarfile.open(path, "r:gz") as archive:
        members = {member.name for member in safe_tar_members(archive)}
        missing = set(data_members) - members
        if missing:
            raise ValueError(f"ATNF archive lacks required members: {sorted(missing)}")
        index_output = table_dir / "atnf_archive_members.parquet"
        index_result = write_archive_member_index_parquet(
            archive, index_output, dispositions=data_members
        )
        reports.append(
            source_native_report(
                source_name="atnf_archive_members",
                parser="validated_tar_member_index_v1",
                typing_status="archive_member_index",
                raw_tree_sha256=artifact["tree_sha256"],
                output=index_output,
                con=con,
                details={"source_row_accounting": index_result},
            )
        )

        readme_bytes = tar_member_bytes(archive, "psrcat_tar/README")
        readme_output = table_dir / "atnf_readme.parquet"
        readme_result = write_text_lines_parquet(
            readme_bytes.decode("utf-8", errors="replace"), readme_output
        )
        reports.append(
            source_native_report(
                source_name="atnf_readme",
                parser="source_document_lines_v1",
                typing_status="source_document",
                raw_tree_sha256=artifact["tree_sha256"],
                output=readme_output,
                con=con,
                details={
                    "archive_member": "psrcat_tar/README",
                    "archive_member_sha256": bytes_sha256(readme_bytes),
                    "source_row_accounting": readme_result,
                },
            )
        )

        catalogue_bytes = tar_member_bytes(archive, "psrcat_tar/psrcat.db")
        parameters_output = table_dir / "atnf_parameters.parquet"
        comments_output = table_dir / "atnf_catalogue_comments.parquet"
        catalogue_result = write_atnf_catalog_parquet(
            catalogue_bytes.decode("utf-8", errors="replace"),
            parameters_output,
            comments_output,
        )
        shared = {
            "archive_member": "psrcat_tar/psrcat.db",
            "archive_member_sha256": bytes_sha256(catalogue_bytes),
            "source_row_accounting": catalogue_result,
        }
        reports.append(
            source_native_report(
                source_name="atnf_parameters",
                parser="atnf_parameter_blocks_lexical_v1",
                typing_status="source_schema_lexical",
                raw_tree_sha256=artifact["tree_sha256"],
                output=parameters_output,
                con=con,
                details=shared,
            )
        )
        reports.append(
            source_native_report(
                source_name="atnf_catalogue_comments",
                parser="atnf_parameter_blocks_lexical_v1",
                typing_status="source_comment_history",
                raw_tree_sha256=artifact["tree_sha256"],
                output=comments_output,
                con=con,
                details=shared,
            )
        )

        glitch_bytes = tar_member_bytes(archive, "psrcat_tar/glitch.db")
        glitch_output = table_dir / "atnf_glitches.parquet"
        glitch_result = write_atnf_glitches_parquet(
            glitch_bytes.decode("utf-8", errors="replace"), glitch_output
        )
        reports.append(
            source_native_report(
                source_name="atnf_glitches",
                parser="atnf_glitch_table_lexical_v1",
                typing_status="source_schema_lexical",
                raw_tree_sha256=artifact["tree_sha256"],
                output=glitch_output,
                con=con,
                details={
                    "archive_member": "psrcat_tar/glitch.db",
                    "archive_member_sha256": bytes_sha256(glitch_bytes),
                    "source_row_accounting": glitch_result["source_row_accounting"],
                },
            )
        )

        reference_bytes = tar_member_bytes(archive, "psrcat_tar/psrcat_ref")
        reference_output = table_dir / "atnf_references.parquet"
        reference_result = write_atnf_references_parquet(
            reference_bytes.decode("utf-8", errors="replace"), reference_output
        )
        reports.append(
            source_native_report(
                source_name="atnf_references",
                parser="atnf_reference_blocks_v1",
                typing_status="source_citation_text",
                raw_tree_sha256=artifact["tree_sha256"],
                output=reference_output,
                con=con,
                details={
                    "archive_member": "psrcat_tar/psrcat_ref",
                    "archive_member_sha256": bytes_sha256(reference_bytes),
                    "source_row_accounting": reference_result,
                },
            )
        )
    return reports


def cook_multi_hdu_fits(
    source: dict[str, Any],
    artifact: dict[str, Any],
    path: Path,
    table_dir: Path,
    con: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    contracts = source["schema_policy"].get("table_hdus") or []
    if not contracts:
        raise ValueError("multi-HDU FITS source lacks table_hdus contracts")
    reports: list[dict[str, Any]] = []
    for contract in contracts:
        hdu_index = int(contract["hdu_index"])
        table_name = str(contract["table_name"])
        output = table_dir / f"{slug(table_name)}.parquet"
        result = write_fits_table_parquet(path, output, hdu_index=hdu_index)
        source_hdu = result["source_hdu"]
        expected_rows = int(contract["expected_row_count"])
        expected_fields = int(contract["expected_field_count"])
        if source_hdu["row_count"] != expected_rows:
            raise ValueError(
                f"FITS HDU {hdu_index} row-count drift: "
                f"{source_hdu['row_count']} != {expected_rows}"
            )
        if source_hdu["field_count"] != expected_fields:
            raise ValueError(
                f"FITS HDU {hdu_index} field-count drift: "
                f"{source_hdu['field_count']} != {expected_fields}"
            )
        reports.append(
            source_native_report(
                source_name=table_name,
                parser="fits_binary_table_hdu_source_native_v2",
                typing_status="source_schema_typed_nulls_normalized",
                raw_tree_sha256=artifact["tree_sha256"],
                output=output,
                con=con,
                details=result,
            )
        )
    return reports


def cook_mcgill_magnetar_bundle(
    raw_manifest: dict[str, Any],
    raw_snapshot_dir: Path,
    table_dir: Path,
    con: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    artifacts = {
        str(artifact["source_name"]): artifact
        for artifact in raw_manifest["artifacts"]
    }
    expected = {
        "mcgill_magnetar_catalog_20260721",
        "mcgill_magnetar_main_html_20260721",
        "mcgill_magnetar_cds_readme",
        "mcgill_magnetar_cds_references",
    }
    if set(artifacts) != expected:
        raise ValueError(
            "McGill bundle artifact drift: "
            f"missing={sorted(expected - set(artifacts))} "
            f"extra={sorted(set(artifacts) - expected)}"
        )
    paths = {
        name: single_artifact_file(raw_snapshot_dir, artifact)
        for name, artifact in artifacts.items()
    }
    reports: list[dict[str, Any]] = []

    catalog_output = table_dir / "mcgill_magnetar_catalog.parquet"
    catalog_fields = write_delimited_parquet(
        [paths["mcgill_magnetar_catalog_20260721"]], catalog_output, con
    )
    reports.append(
        source_native_report(
            source_name="mcgill_magnetar_catalog",
            parser="duckdb_read_csv_explicit_lexical_shape_checked_v4",
            typing_status="source_schema_lexical",
            raw_tree_sha256=artifacts["mcgill_magnetar_catalog_20260721"][
                "tree_sha256"
            ],
            output=catalog_output,
            con=con,
            details={"source_schema": [{"name": field} for field in catalog_fields]},
        )
    )

    html_path = paths["mcgill_magnetar_main_html_20260721"]
    html_rows_output = table_dir / "mcgill_html_rows.parquet"
    html_links_output = table_dir / "mcgill_html_reference_links.parquet"
    html_references_output = table_dir / "mcgill_html_reference_index.parquet"
    html_report = write_mcgill_magnetar_html_parquet(
        html_path,
        rows_output=html_rows_output,
        links_output=html_links_output,
        references_output=html_references_output,
        base_url="https://www.physics.mcgill.ca/~pulsar/magnetar/main.html",
    )
    for source_name, output in (
        ("mcgill_html_rows", html_rows_output),
        ("mcgill_html_reference_links", html_links_output),
        ("mcgill_html_reference_index", html_references_output),
    ):
        reports.append(
            source_native_report(
                source_name=source_name,
                parser="mcgill_html_table_resources_source_native_v1",
                typing_status="source_schema_lexical_resources_preserved",
                raw_tree_sha256=artifacts[
                    "mcgill_magnetar_main_html_20260721"
                ]["tree_sha256"],
                output=output,
                con=con,
                details={"source_row_accounting": html_report},
            )
        )
    html_document_output = table_dir / "mcgill_main_html_document.parquet"
    html_document_report = write_document_lines_parquet(
        html_path, html_document_output
    )
    reports.append(
        source_native_report(
            source_name="mcgill_main_html_document",
            parser="source_document_lines_v1",
            typing_status="source_schema_document",
            raw_tree_sha256=artifacts["mcgill_magnetar_main_html_20260721"][
                "tree_sha256"
            ],
            output=html_document_output,
            con=con,
            details={"source_row_accounting": html_document_report},
        )
    )

    readme_path = paths["mcgill_magnetar_cds_readme"]
    readme_output = table_dir / "mcgill_cds_readme.parquet"
    readme_report = write_document_lines_parquet(readme_path, readme_output)
    reports.append(
        source_native_report(
            source_name="mcgill_cds_readme",
            parser="source_document_lines_v1",
            typing_status="source_schema_document",
            raw_tree_sha256=artifacts["mcgill_magnetar_cds_readme"]["tree_sha256"],
            output=readme_output,
            con=con,
            details={"source_row_accounting": readme_report},
        )
    )

    references_path = paths["mcgill_magnetar_cds_references"]
    schema_section, reference_fields = resolve_cds_table(
        references_path, parse_cds_readme(readme_path)
    )
    references_output = table_dir / "mcgill_cds_references.parquet"
    references_report = write_fixed_width_parquet(
        references_path, reference_fields, references_output
    )
    reports.append(
        source_native_report(
            source_name="mcgill_cds_references",
            parser="documented_fixed_width_lexical_v1",
            typing_status="source_schema_lexical",
            raw_tree_sha256=artifacts["mcgill_magnetar_cds_references"][
                "tree_sha256"
            ],
            output=references_output,
            con=con,
            details={
                "schema_document_source_name": "mcgill_magnetar_cds_readme",
                "schema_document_sha256": artifacts[
                    "mcgill_magnetar_cds_readme"
                ]["tree_sha256"],
                "schema_section": schema_section,
                "source_schema": reference_fields,
                "source_row_accounting": references_report[
                    "source_row_accounting"
                ],
            },
        )
    )
    return reports


def cook_special_source(
    source: dict[str, Any],
    raw_manifest: dict[str, Any],
    raw_snapshot_dir: Path,
    table_dir: Path,
    con: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]] | None:
    source_id = str(source["source_id"])
    if source_id not in {
        "multiplicity.msc",
        "multiplicity.orb6",
        "multiplicity.debcat",
        "compact.atnf",
        "compact.gaia_edr3_white_dwarf",
        "extended.green_snr",
        "spectroscopy.apogee_dr17",
        "exoplanet_lifecycle.open_exoplanet_catalogue",
        "compact.mcgill_magnetar",
        "standards.iau_2015_resolution_b3",
    }:
        return None
    if source_id == "compact.mcgill_magnetar":
        return cook_mcgill_magnetar_bundle(
            raw_manifest, raw_snapshot_dir, table_dir, con
        )
    if source_id == "standards.iau_2015_resolution_b3":
        if len(raw_manifest["artifacts"]) != 1:
            raise ValueError("IAU 2015 Resolution B3 expects one raw PDF")
        artifact = raw_manifest["artifacts"][0]
        return cook_iau_2015_resolution_b3(
            artifact,
            single_artifact_file(raw_snapshot_dir, artifact),
            table_dir,
            con,
        )
    if len(raw_manifest["artifacts"]) != 1:
        raise ValueError(f"special source expects one raw artifact: {source_id}")
    artifact = raw_manifest["artifacts"][0]
    path = single_artifact_file(raw_snapshot_dir, artifact)

    if source_id == "multiplicity.msc":
        return cook_msc_archive(artifact, path, table_dir, con)
    if source_id == "compact.atnf":
        return cook_atnf_archive(artifact, path, table_dir, con)
    if source_id == "spectroscopy.apogee_dr17":
        return cook_multi_hdu_fits(source, artifact, path, table_dir, con)
    if source_id == "exoplanet_lifecycle.open_exoplanet_catalogue":
        outputs = {
            "oec_objects": table_dir / "oec_objects.parquet",
            "oec_names": table_dir / "oec_names.parquet",
            "oec_parameters": table_dir / "oec_parameters.parquet",
            "oec_relations": table_dir / "oec_relations.parquet",
        }
        with tarfile.open(path, "r:gz") as archive:
            dispositions = {
                member.name: (
                    "typed_xml_data"
                    if member.name.endswith(".xml")
                    else "source_document_or_code"
                )
                for member in safe_tar_members(archive)
            }
            index_output = table_dir / "oec_archive_members.parquet"
            index_result = write_archive_member_index_parquet(
                archive, index_output, dispositions=dispositions
            )
            parse_result = write_oec_archive_parquet(
                archive,
                objects_output=outputs["oec_objects"],
                names_output=outputs["oec_names"],
                parameters_output=outputs["oec_parameters"],
                relations_output=outputs["oec_relations"],
            )
        reports = [
            source_native_report(
                source_name="oec_archive_members",
                parser="validated_tar_member_index_v1",
                typing_status="archive_member_index",
                raw_tree_sha256=artifact["tree_sha256"],
                output=index_output,
                con=con,
                details={"source_row_accounting": index_result},
            )
        ]
        for source_name, output in outputs.items():
            reports.append(
                source_native_report(
                    source_name=source_name,
                    parser="oec_xml_graph_source_native_v1",
                    typing_status="source_schema_lexical_attributes_preserved",
                    raw_tree_sha256=artifact["tree_sha256"],
                    output=output,
                    con=con,
                    details={"source_row_accounting": parse_result},
                )
            )
        return reports
    if source_id == "multiplicity.orb6":
        output = table_dir / "orb6_orbits.parquet"
        result = write_tokenized_parquet(
            path, output, ORB6_FIELDS, skip_lines=2, delimiter="|"
        )
        return [
            source_native_report(
                source_name="orb6_orbits",
                parser="orb6_pipe_rows_lexical_v1",
                typing_status="source_schema_lexical",
                raw_tree_sha256=artifact["tree_sha256"],
                output=output,
                con=con,
                details={
                    "source_schema": [
                        {"name": name, "source_header": header}
                        for name, header in zip(
                            ORB6_FIELDS,
                            path.read_text(encoding="utf-8", errors="replace")
                            .splitlines()[1]
                            .split("|"),
                            strict=True,
                        )
                    ],
                    "source_row_accounting": result["source_row_accounting"],
                },
            )
        ]
    if source_id == "multiplicity.debcat":
        output = table_dir / "debcat_components.parquet"
        result = write_tokenized_parquet(path, output, DEBCAT_FIELDS, skip_lines=1)
        return [
            source_native_report(
                source_name="debcat_components",
                parser="debcat_whitespace_rows_lexical_v1",
                typing_status="source_schema_lexical_sentinels_preserved",
                raw_tree_sha256=artifact["tree_sha256"],
                output=output,
                con=con,
                details={
                    "source_schema": [{"name": name} for name in DEBCAT_FIELDS],
                    "null_limit_semantics": {
                        "numeric_missing_sentinel": "-9.9900 or source-column equivalent",
                        "string_missing_sentinel": "none",
                        "normalization_status": "preserved_for_E4",
                    },
                    "source_row_accounting": result["source_row_accounting"],
                },
            )
        ]
    if source_id == "compact.gaia_edr3_white_dwarf":
        output = table_dir / "gaia_edr3_white_dwarf_main.parquet"
        result = write_fits_table_parquet(path, output)
        return [
            source_native_report(
                source_name="gaia_edr3_white_dwarf_main",
                parser="fits_binary_table_source_native_v1",
                typing_status="source_schema_typed_nulls_normalized",
                raw_tree_sha256=artifact["tree_sha256"],
                output=output,
                con=con,
                details=result,
            )
        ]
    if source_id == "extended.green_snr":
        output = table_dir / "green_snr_catalogue.parquet"
        result = write_green_snr_parquet(path, output)
        return [
            source_native_report(
                source_name="green_snr_catalogue",
                parser="green_snr_html_pre_lexical_v1",
                typing_status="source_schema_lexical_limits_preserved",
                raw_tree_sha256=artifact["tree_sha256"],
                output=output,
                con=con,
                details={
                    "source_schema": [
                        {"name": name}
                        for name in (
                            "galactic_longitude",
                            "galactic_latitude",
                            "ra_hour",
                            "ra_minute",
                            "ra_second",
                            "dec_degree",
                            "dec_arcminute",
                            "angular_size",
                            "snr_type",
                            "flux_1ghz",
                            "spectral_index",
                            "other_names",
                        )
                    ],
                    "source_row_accounting": result["source_row_accounting"],
                },
            )
        ]
    raise AssertionError(source_id)


def cook_artifact(
    source: dict[str, Any],
    snapshot_dir: Path,
    artifact: dict[str, Any],
    output: Path,
    con: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    files = artifact_input_files(snapshot_dir, artifact)
    tabular = [path for path in files if path.name.endswith(TABULAR_SUFFIXES)]
    votables = [path for path in files if path.name.endswith(VOTABLE_SUFFIXES)]
    fits_files = [path for path in files if path.name.endswith(FITS_SUFFIXES)]
    html_files = [path for path in files if path.suffix.lower() in {".html", ".htm"}]
    json_files = [path for path in files if path.suffix == ".json"]
    parser = ""
    expected_fields: list[str] = []
    member_lineage_field = str(
        (source["schema_policy"].get("member_lineage_fields") or {}).get(
            str(artifact["source_name"])
        )
        or ""
    ).strip() or None
    if tabular:
        expected_fields = write_delimited_parquet(
            tabular,
            output,
            con,
            member_lineage_field=member_lineage_field,
        )
        parser = "duckdb_read_csv_explicit_lexical_shape_checked_v4"
    elif votables:
        source_schema = write_votable_files_parquet(
            votables,
            output,
            member_lineage_field=member_lineage_field,
        )
        parser = (
            "astropy_votable_binary_arrow_member_lineage_v2"
            if member_lineage_field
            else "astropy_votable_binary_arrow_v1"
        )
        expected_fields = sorted(field["name"] for field in source_schema["source_schema"])
    elif fits_files:
        if len(fits_files) != 1:
            raise ValueError(
                f"FITS artifact must contain exactly one table payload: {artifact['source_name']}"
            )
        source_schema = write_fits_table_parquet(fits_files[0], output)
        parser = "fits_binary_table_source_native_v1"
        expected_fields = sorted(
            field["name"] for field in source_schema["source_schema"]
        )
    elif html_files and source["schema_policy"]["kind"] == "html_snapshot":
        if len(html_files) != 1:
            raise ValueError(
                f"HTML artifact must contain exactly one payload: {artifact['source_name']}"
            )
        table_contract = source["schema_policy"].get("html_table")
        if table_contract:
            source_schema = write_html_table_parquet(
                html_files[0],
                output,
                table_id=str(table_contract["table_id"]),
                fields=list(table_contract["fields"]),
            )
            parser = "validated_html_table_lexical_v1"
            expected_fields = sorted(
                [field["name"] for field in source_schema["source_schema"]]
                + [
                    "source_table_id",
                    "source_row_number",
                    "source_row_id",
                    "source_row_index",
                    "source_row_attributes_json",
                    "source_cell_resources_json",
                ]
            )
        else:
            write_document_lines_parquet(html_files[0], output)
            parser = "source_document_lines_v1"
            expected_fields = ["source_line_number", "text"]
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
            if parser
            in {
                "mast_json_declared_schema_arrow_v2",
                "astropy_votable_binary_arrow_v1",
                "fits_binary_table_source_native_v1",
            }
            else "source_schema_lexical"
            if parser == "validated_html_table_lexical_v1"
            else "lexical_preserved_source_types_pending"
        ),
        **(
            {"source_schema": source_schema}
            if parser
            in {
                "mast_json_declared_schema_arrow_v2",
                "astropy_votable_binary_arrow_v1",
                "fits_binary_table_source_native_v1",
            }
            else {
                "source_schema": source_schema["source_schema"],
                "source_table_id": source_schema["source_table_id"],
                "source_table_count": source_schema["source_table_count"],
                "excluded_page_table_count": source_schema[
                    "excluded_page_table_count"
                ],
                "excluded_footer_row_count": source_schema[
                    "excluded_footer_row_count"
                ],
            }
            if parser == "validated_html_table_lexical_v1"
            else {}
        ),
        "raw_tree_sha256": artifact["tree_sha256"],
        **(
            {"member_lineage_field": member_lineage_field}
            if member_lineage_field
            else {}
        ),
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
    # Parallel unordered Parquet writes can preserve rows and schemas while
    # producing different row-group ordering and hashes on a clean rebuild.
    con.execute("set threads=1")
    con.execute("set preserve_insertion_order=true")
    reports: list[dict[str, Any]] = []
    try:
        layout = source["schema_policy"].get("artifact_layout") or {}
        try:
            special_reports = cook_special_source(
                source,
                raw_manifest,
                raw_snapshot_dir,
                temp_dir / "tables",
                con,
            )
        except Exception as exc:
            for partial in (temp_dir / "tables").glob("*"):
                partial.unlink()
            special_reports = [
                {
                    "source_name": str(source["source_id"]),
                    "status": "parser_error",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            ]
        if special_reports is not None:
            reports.extend(special_reports)
        elif layout.get("kind") == "delimited_continuation":
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
            artifacts_by_name = {
                str(item["source_name"]): item for item in raw_manifest["artifacts"]
            }
            for artifact in raw_manifest["artifacts"]:
                output = temp_dir / "tables" / f"{slug(str(artifact['source_name']))}.parquet"
                try:
                    report = cook_documented_artifact(
                        source,
                        raw_snapshot_dir,
                        artifact,
                        artifacts_by_name,
                        output,
                        con,
                    ) or cook_artifact(source, raw_snapshot_dir, artifact, output, con)
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


def verify_snapshot(
    raw_dir: Path,
    typed_dir: Path | None = None,
    *,
    expected_parser_contract_version: str | None = None,
) -> dict[str, Any]:
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
        if (
            expected_parser_contract_version
            and typed_manifest.get("parser_contract_version")
            != expected_parser_contract_version
        ):
            errors.append(
                "typed parser contract is stale: "
                f"{typed_manifest.get('parser_contract_version')} != "
                f"{expected_parser_contract_version}"
            )
        accounted_artifacts: set[str] = set()
        for table in typed_manifest["tables"]:
            for artifact in raw_manifest["artifacts"]:
                if table.get("raw_tree_sha256") == artifact["tree_sha256"]:
                    accounted_artifacts.add(str(artifact["source_name"]))
            source_rows = table.get("source_row_accounting")
            if isinstance(source_rows, list):
                accounted_artifacts.update(
                    str(row["source_name"])
                    for row in source_rows
                    if isinstance(row, dict) and row.get("source_name")
                )
        missing_artifacts = sorted(
            str(artifact["source_name"])
            for artifact in raw_manifest["artifacts"]
            if str(artifact["source_name"]) not in accounted_artifacts
        )
        if missing_artifacts:
            errors.append(f"raw artifacts lack typed accounting: {missing_artifacts}")
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
        "raw_artifact_count": len(raw_manifest["artifacts"]),
        "accounted_raw_artifact_count": (
            len(accounted_artifacts) if typed_manifest is not None else 0
        ),
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
            report = verify_snapshot(
                raw_dir,
                typed_dir,
                expected_parser_contract_version=parser_contract_version(source),
            )
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
            "typed_row_count": sum(
                int(table.get("row_count") or 0)
                for table in table_reports
                if table.get("status") == "typed"
            ),
            "typed_column_count": sum(
                len(table.get("columns") or [])
                for table in table_reports
                if table.get("status") == "typed"
            ),
            "typed_bytes": sum(
                int(table.get("bytes") or 0)
                for table in table_reports
                if table.get("status") == "typed"
            ),
            "parser_counts": {
                parser: sum(table.get("parser") == parser for table in table_reports)
                for parser in sorted(
                    {
                        str(table.get("parser"))
                        for table in table_reports
                        if table.get("parser")
                    }
                )
            },
            "typing_status_counts": {
                status: sum(table.get("typing_status") == status for table in table_reports)
                for status in sorted(
                    {
                        str(table.get("typing_status"))
                        for table in table_reports
                        if table.get("typing_status")
                    }
                )
            },
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
