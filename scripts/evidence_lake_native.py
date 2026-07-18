#!/usr/bin/env python3
"""Source-native parsers used by the Evidence Lake v2 typed compiler."""

from __future__ import annotations

import gzip
import hashlib
import html
import re
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Any, BinaryIO, Iterable, TextIO

import pyarrow as pa
import pyarrow.parquet as pq


CDS_SECTION_RE = re.compile(r"Byte-by-byte Description of file:\s*(\S+)")
CDS_FIELD_RE = re.compile(
    r"^\s*(\d+)(?:\s*-\s*(\d+))?\s+(\S+)\s+(\S+)\s+(\S+)\s*(.*)$"
)
WDS_FIELD_RE = re.compile(
    r"^\s*(\d+)\s*-\s*(\d+)\s+(\S+)\s{2,}(.+?)\s*$"
)


def open_text(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="")
    return path.open("r", encoding="utf-8", errors="replace", newline="")


def unique_field_name(label: str, start: int, end: int, seen: set[str]) -> str:
    base = label.strip() if label.strip("-") else f"unnamed_{start}_{end}"
    name = base
    if name in {"source_line_number", "raw_row"} or name in seen:
        name = f"{base}_{start}_{end}"
    suffix = 2
    candidate = name
    while candidate in seen:
        candidate = f"{name}_{suffix}"
        suffix += 1
    seen.add(candidate)
    return candidate


def parse_cds_readme_text(text: str) -> dict[str, list[dict[str, Any]]]:
    tables: dict[str, list[dict[str, Any]]] = {}
    current: str | None = None
    fields_started = False
    fields_closed = False
    seen_by_table: dict[str, set[str]] = {}
    for line in text.splitlines():
        section = CDS_SECTION_RE.search(line)
        if section:
            current = Path(section.group(1)).name
            tables[current] = []
            seen_by_table[current] = set()
            fields_started = False
            fields_closed = False
            continue
        if not current or fields_closed:
            continue
        match = CDS_FIELD_RE.match(line)
        if not match:
            if fields_started and re.match(r"^\s*-{5,}\s*$", line):
                fields_closed = True
            continue
        start = int(match.group(1))
        end = int(match.group(2) or match.group(1))
        source_format = match.group(3)
        unit = match.group(4)
        source_label = match.group(5)
        description = match.group(6).strip()
        name = unique_field_name(source_label, start, end, seen_by_table[current])
        fields_started = True
        tables[current].append(
            {
                "name": name,
                "source_label": source_label,
                "start": start,
                "end": end,
                "source_format": source_format,
                "unit": unit,
                "description": description,
            }
        )
    return {name: fields for name, fields in tables.items() if fields}


def parse_cds_readme(path: Path) -> dict[str, list[dict[str, Any]]]:
    return parse_cds_readme_text(path.read_text(encoding="utf-8", errors="replace"))


def normalized_description(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9]+", "_", value.strip().lower()).strip("_")
    return name or "field"


def parse_wds_format(path: Path) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = WDS_FIELD_RE.match(line.rstrip("\r\n"))
            if not match:
                continue
            start = int(match.group(1))
            end = int(match.group(2))
            source_format = match.group(3)
            description = match.group(4).strip()
            label = normalized_description(description)
            name = unique_field_name(label, start, end, seen)
            fields.append(
                {
                    "name": name,
                    "source_label": description,
                    "start": start,
                    "end": end,
                    "source_format": source_format,
                    "unit": "source_documented",
                    "description": description,
                }
            )
    if not fields:
        raise ValueError(f"WDS format document contains no field definitions: {path}")
    return fields


def fixed_width_rows(
    path: Path,
    fields: list[dict[str, Any]],
    *,
    record_pattern: re.Pattern[str] | None = None,
) -> tuple[Iterable[dict[str, Any]], dict[str, int]]:
    counters = {
        "source_line_count": 0,
        "blank_line_count": 0,
        "excluded_line_count": 0,
        "short_row_count": 0,
        "max_row_chars": 0,
    }
    expected_width = max(int(field["end"]) for field in fields)

    def rows() -> Iterable[dict[str, Any]]:
        with open_text(path) as handle:
            for line_number, line in enumerate(handle, start=1):
                raw = line.rstrip("\r\n")
                counters["source_line_count"] += 1
                counters["max_row_chars"] = max(counters["max_row_chars"], len(raw))
                if not raw.strip():
                    counters["blank_line_count"] += 1
                    continue
                if record_pattern and not record_pattern.match(raw):
                    counters["excluded_line_count"] += 1
                    continue
                if len(raw) < expected_width:
                    counters["short_row_count"] += 1
                row: dict[str, Any] = {"source_line_number": line_number}
                for field in fields:
                    value = raw[int(field["start"]) - 1 : int(field["end"])]
                    row[str(field["name"])] = value.strip() or None
                row["raw_row"] = raw
                yield row

    return rows(), counters


def write_record_batches(
    rows: Iterable[dict[str, Any]],
    schema: pa.Schema,
    output: Path,
    *,
    batch_size: int = 65_536,
) -> int:
    writer = pq.ParquetWriter(output, schema, compression="zstd")
    row_count = 0
    batch: list[dict[str, Any]] = []
    try:
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                row_count += len(batch)
                batch.clear()
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            row_count += len(batch)
    finally:
        writer.close()
    return row_count


def write_fixed_width_parquet(
    path: Path,
    fields: list[dict[str, Any]],
    output: Path,
    *,
    record_pattern: re.Pattern[str] | None = None,
) -> dict[str, Any]:
    if not fields:
        raise ValueError(f"fixed-width schema has no fields: {path}")
    schema = pa.schema(
        [pa.field("source_line_number", pa.int64())]
        + [pa.field(str(field["name"]), pa.string()) for field in fields]
        + [pa.field("raw_row", pa.string())]
    )
    rows, counters = fixed_width_rows(path, fields, record_pattern=record_pattern)
    row_count = write_record_batches(rows, schema, output)
    expected_rows = (
        counters["source_line_count"]
        - counters["blank_line_count"]
        - counters["excluded_line_count"]
    )
    if row_count != expected_rows:
        raise ValueError(f"fixed-width row accounting mismatch: {row_count} != {expected_rows}")
    return {"row_count": row_count, "source_row_accounting": counters}


def write_document_lines_parquet(path: Path, output: Path) -> dict[str, int]:
    schema = pa.schema(
        [
            pa.field("source_line_number", pa.int64()),
            pa.field("text", pa.string()),
        ]
    )

    def rows() -> Iterable[dict[str, Any]]:
        with open_text(path) as handle:
            for line_number, line in enumerate(handle, start=1):
                yield {"source_line_number": line_number, "text": line.rstrip("\r\n")}

    return {"row_count": write_record_batches(rows(), schema, output)}


def bytes_sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def safe_tar_members(archive: tarfile.TarFile) -> list[tarfile.TarInfo]:
    members: list[tarfile.TarInfo] = []
    for member in archive.getmembers():
        path = Path(member.name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"unsafe archive member: {member.name}")
        if member.isdir():
            continue
        if not member.isfile():
            raise ValueError(f"unsupported archive member type: {member.name}")
        members.append(member)
    return members


def tar_member_bytes(archive: tarfile.TarFile, member_name: str) -> bytes:
    member = archive.getmember(member_name)
    if not member.isfile():
        raise ValueError(f"archive member is not a regular file: {member_name}")
    handle: BinaryIO | None = archive.extractfile(member)
    if handle is None:
        raise ValueError(f"cannot read archive member: {member_name}")
    value = handle.read()
    if len(value) != member.size:
        raise ValueError(f"archive member size mismatch: {member_name}")
    return value


def write_text_lines_parquet(text: str, output: Path) -> dict[str, int]:
    schema = pa.schema(
        [
            pa.field("source_line_number", pa.int64()),
            pa.field("text", pa.string()),
        ]
    )
    lines = text.splitlines()
    rows = (
        {"source_line_number": line_number, "text": line}
        for line_number, line in enumerate(lines, start=1)
    )
    return {"row_count": write_record_batches(rows, schema, output)}


def write_archive_member_index_parquet(
    archive: tarfile.TarFile,
    output: Path,
    *,
    dispositions: dict[str, str],
) -> dict[str, Any]:
    members = safe_tar_members(archive)
    schema = pa.schema(
        [
            pa.field("member_name", pa.string()),
            pa.field("member_bytes", pa.int64()),
            pa.field("member_sha256", pa.string()),
            pa.field("disposition", pa.string()),
        ]
    )

    def rows() -> Iterable[dict[str, Any]]:
        for member in members:
            value = tar_member_bytes(archive, member.name)
            yield {
                "member_name": member.name,
                "member_bytes": member.size,
                "member_sha256": bytes_sha256(value),
                "disposition": dispositions.get(member.name, "index_only"),
            }

    row_count = write_record_batches(rows(), schema, output)
    return {"row_count": row_count, "member_count": len(members)}


def fixed_width_text_rows(
    text: str,
    fields: list[dict[str, Any]],
) -> tuple[Iterable[dict[str, Any]], dict[str, int]]:
    counters = {
        "source_line_count": 0,
        "blank_line_count": 0,
        "excluded_line_count": 0,
        "short_row_count": 0,
        "max_row_chars": 0,
    }
    expected_width = max(int(field["end"]) for field in fields)

    def rows() -> Iterable[dict[str, Any]]:
        for line_number, raw in enumerate(text.splitlines(), start=1):
            counters["source_line_count"] += 1
            counters["max_row_chars"] = max(counters["max_row_chars"], len(raw))
            if not raw.strip():
                counters["blank_line_count"] += 1
                continue
            if len(raw) < expected_width:
                counters["short_row_count"] += 1
            row: dict[str, Any] = {"source_line_number": line_number}
            for field in fields:
                value = raw[int(field["start"]) - 1 : int(field["end"])]
                row[str(field["name"])] = value.strip() or None
            row["raw_row"] = raw
            yield row

    return rows(), counters


def write_fixed_width_text_parquet(
    text: str,
    fields: list[dict[str, Any]],
    output: Path,
) -> dict[str, Any]:
    schema = pa.schema(
        [pa.field("source_line_number", pa.int64())]
        + [pa.field(str(field["name"]), pa.string()) for field in fields]
        + [pa.field("raw_row", pa.string())]
    )
    rows, counters = fixed_width_text_rows(text, fields)
    row_count = write_record_batches(rows, schema, output)
    expected_rows = counters["source_line_count"] - counters["blank_line_count"]
    if row_count != expected_rows:
        raise ValueError(f"fixed-width row accounting mismatch: {row_count} != {expected_rows}")
    return {"row_count": row_count, "source_row_accounting": counters}


def write_tokenized_parquet(
    path: Path,
    output: Path,
    field_names: list[str],
    *,
    skip_lines: int = 0,
    delimiter: str | None = None,
) -> dict[str, Any]:
    schema = pa.schema(
        [pa.field("source_line_number", pa.int64())]
        + [pa.field(name, pa.string()) for name in field_names]
        + [pa.field("raw_row", pa.string())]
    )
    counters = {
        "source_line_count": 0,
        "header_line_count": 0,
        "blank_line_count": 0,
        "malformed_line_count": 0,
    }

    def rows() -> Iterable[dict[str, Any]]:
        with open_text(path) as handle:
            for line_number, line in enumerate(handle, start=1):
                raw = line.rstrip("\r\n")
                counters["source_line_count"] += 1
                if line_number <= skip_lines:
                    counters["header_line_count"] += 1
                    continue
                if not raw.strip():
                    counters["blank_line_count"] += 1
                    continue
                values = raw.split(delimiter) if delimiter else raw.split()
                if len(values) != len(field_names):
                    counters["malformed_line_count"] += 1
                    raise ValueError(
                        f"line {line_number} has {len(values)} fields, expected {len(field_names)}"
                    )
                row = {"source_line_number": line_number, "raw_row": raw}
                row.update(
                    {
                        name: value.strip() or None
                        for name, value in zip(field_names, values, strict=True)
                    }
                )
                yield row

    row_count = write_record_batches(rows(), schema, output)
    return {"row_count": row_count, "source_row_accounting": counters}


def write_green_snr_parquet(path: Path, output: Path) -> dict[str, Any]:
    field_names = [
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
    ]
    schema = pa.schema(
        [pa.field("source_line_number", pa.int64())]
        + [pa.field(name, pa.string()) for name in field_names]
        + [pa.field("detail_href", pa.string()), pa.field("raw_row", pa.string())]
    )
    record = re.compile(
        r'<A\s+HREF="(snrs\.G[^"]+)">(.+?)</A>(.*)', re.IGNORECASE
    )
    tag = re.compile(r"<[^>]+>")
    counters = {
        "source_line_count": 0,
        "record_line_count": 0,
        "non_record_line_count": 0,
        "malformed_record_count": 0,
    }

    def rows() -> Iterable[dict[str, Any]]:
        with open_text(path) as handle:
            for line_number, line in enumerate(handle, start=1):
                raw = line.rstrip("\r\n")
                counters["source_line_count"] += 1
                match = record.search(raw)
                if not match:
                    counters["non_record_line_count"] += 1
                    continue
                plain = html.unescape(tag.sub("", match.group(2) + match.group(3))).strip()
                values = plain.split(maxsplit=11)
                if len(values) == 11:
                    values.append("")
                if len(values) != len(field_names):
                    counters["malformed_record_count"] += 1
                    raise ValueError(f"malformed Green SNR record at line {line_number}: {plain}")
                counters["record_line_count"] += 1
                row = {"source_line_number": line_number, "raw_row": raw}
                row.update(
                    {
                        name: value.strip() or None
                        for name, value in zip(field_names, values, strict=True)
                    }
                )
                row["detail_href"] = match.group(1)
                yield row

    row_count = write_record_batches(rows(), schema, output)
    if row_count != counters["record_line_count"]:
        raise ValueError("Green SNR row accounting mismatch")
    return {"row_count": row_count, "source_row_accounting": counters}


def write_atnf_catalog_parquet(
    text: str,
    parameters_output: Path,
    comments_output: Path,
) -> dict[str, Any]:
    parameter_schema = pa.schema(
        [
            pa.field("catalogue_block", pa.int64()),
            pa.field("pulsar_name", pa.string()),
            pa.field("parameter_occurrence", pa.int32()),
            pa.field("source_line_number", pa.int64()),
            pa.field("parameter_name", pa.string()),
            pa.field("value_raw", pa.string()),
            pa.field("uncertainty_raw", pa.string()),
            pa.field("reference_raw", pa.string()),
            pa.field("raw_payload", pa.string()),
            pa.field("raw_row", pa.string()),
        ]
    )
    comment_schema = pa.schema(
        [
            pa.field("catalogue_block", pa.int64()),
            pa.field("pulsar_name", pa.string()),
            pa.field("comment_scope", pa.string()),
            pa.field("source_line_number", pa.int64()),
            pa.field("comment_text", pa.string()),
            pa.field("raw_row", pa.string()),
        ]
    )
    blocks: list[list[tuple[int, str]]] = [[]]
    for line_number, raw in enumerate(text.splitlines(), start=1):
        if raw.startswith("@-"):
            if blocks[-1]:
                blocks.append([])
            continue
        blocks[-1].append((line_number, raw))
    if blocks and not blocks[-1]:
        blocks.pop()

    parameter_rows: list[dict[str, Any]] = []
    comment_rows: list[dict[str, Any]] = []
    pulsar_blocks = 0
    for block_number, lines in enumerate(blocks):
        pulsar_name: str | None = None
        pulsar_line_number: int | None = None
        for line_number, raw in lines:
            if raw.startswith("PSRJ"):
                pulsar_name = raw[9:33].strip() or None
                pulsar_line_number = line_number
                break
        if pulsar_name:
            pulsar_blocks += 1
        occurrences: dict[str, int] = {}
        for line_number, raw in lines:
            if not raw.strip():
                continue
            if raw.startswith("#"):
                is_catalogue_header = (
                    pulsar_line_number is None or line_number < pulsar_line_number
                )
                comment_rows.append(
                    {
                        "catalogue_block": block_number,
                        "pulsar_name": None if is_catalogue_header else pulsar_name,
                        "comment_scope": (
                            "catalogue_header" if is_catalogue_header else "pulsar_record"
                        ),
                        "source_line_number": line_number,
                        "comment_text": raw[1:].strip(),
                        "raw_row": raw,
                    }
                )
                continue
            parts = raw.split(None, 1)
            parameter_name = parts[0]
            occurrences[parameter_name] = occurrences.get(parameter_name, 0) + 1
            if len(raw) > 8 and raw[8].isspace():
                value_raw = raw[9:33].strip() or None
                uncertainty_raw = raw[34:38].strip() or None
                reference_raw = raw[39:].strip() or None
                raw_payload = raw[9:].rstrip()
            else:
                raw_payload = parts[1].strip() if len(parts) > 1 else ""
                value_raw = raw_payload or None
                uncertainty_raw = None
                reference_raw = None
            parameter_rows.append(
                {
                    "catalogue_block": block_number,
                    "pulsar_name": pulsar_name,
                    "parameter_occurrence": occurrences[parameter_name],
                    "source_line_number": line_number,
                    "parameter_name": parameter_name,
                    "value_raw": value_raw,
                    "uncertainty_raw": uncertainty_raw,
                    "reference_raw": reference_raw,
                    "raw_payload": raw_payload,
                    "raw_row": raw,
                }
            )
    parameter_count = write_record_batches(parameter_rows, parameter_schema, parameters_output)
    comment_count = write_record_batches(comment_rows, comment_schema, comments_output)
    return {
        "catalogue_block_count": len(blocks),
        "pulsar_block_count": pulsar_blocks,
        "parameter_row_count": parameter_count,
        "comment_row_count": comment_count,
    }


def write_atnf_glitches_parquet(text: str, output: Path) -> dict[str, Any]:
    field_names = [
        "name_b1950_or_j2000",
        "name_j2000",
        "glitch_epoch_mjd_raw",
        "fractional_frequency_increase_raw",
        "fractional_frequency_derivative_increase_raw",
        "recovery_fraction_q_raw",
        "decay_timescale_days_raw",
        "reference_raw",
    ]
    schema = pa.schema(
        [pa.field("source_line_number", pa.int64())]
        + [pa.field(name, pa.string()) for name in field_names]
        + [pa.field("raw_row", pa.string())]
    )
    counters = {
        "source_line_count": 0,
        "header_line_count": 0,
        "blank_line_count": 0,
        "malformed_line_count": 0,
    }

    def rows() -> Iterable[dict[str, Any]]:
        for line_number, raw in enumerate(text.splitlines(), start=1):
            counters["source_line_count"] += 1
            if line_number <= 3:
                counters["header_line_count"] += 1
                continue
            if not raw.strip():
                counters["blank_line_count"] += 1
                continue
            values = re.split(r"\s{2,}", raw.strip())
            if len(values) != len(field_names):
                counters["malformed_line_count"] += 1
                raise ValueError(f"malformed ATNF glitch line {line_number}")
            row = {"source_line_number": line_number, "raw_row": raw}
            row.update(dict(zip(field_names, values, strict=True)))
            yield row

    row_count = write_record_batches(rows(), schema, output)
    return {"row_count": row_count, "source_row_accounting": counters}


def write_atnf_references_parquet(text: str, output: Path) -> dict[str, Any]:
    schema = pa.schema(
        [
            pa.field("reference_code", pa.string()),
            pa.field("source_start_line", pa.int64()),
            pa.field("source_end_line", pa.int64()),
            pa.field("citation_text", pa.string()),
            pa.field("raw_block", pa.string()),
        ]
    )
    start = re.compile(r"^\*\*\*(\S+)\s+\S+:\s*(.*)$")
    rows: list[dict[str, Any]] = []
    current_code: str | None = None
    current_start = 0
    current_lines: list[str] = []

    def finish(end_line: int) -> None:
        if current_code is None:
            return
        raw_block = "\n".join(current_lines)
        citation = " ".join(part.strip() for part in current_lines if part.strip())
        first = start.match(current_lines[0])
        if first:
            citation = " ".join(
                [first.group(2).strip()]
                + [part.strip() for part in current_lines[1:] if part.strip()]
            ).strip()
        rows.append(
            {
                "reference_code": current_code,
                "source_start_line": current_start,
                "source_end_line": end_line,
                "citation_text": citation,
                "raw_block": raw_block,
            }
        )

    lines = text.splitlines()
    for line_number, raw in enumerate(lines, start=1):
        match = start.match(raw)
        if match:
            finish(line_number - 1)
            current_code = match.group(1)
            current_start = line_number
            current_lines = [raw]
        elif current_code is not None:
            current_lines.append(raw)
    finish(len(lines))
    row_count = write_record_batches(rows, schema, output)
    return {"row_count": row_count, "source_line_count": len(lines)}


def fits_arrow_type(source_format: str) -> pa.DataType:
    match = re.fullmatch(r"(?:\d+)?([AIJKEDL])", source_format.strip().upper())
    if not match:
        raise ValueError(f"unsupported scalar FITS column format: {source_format}")
    code = match.group(1)
    return {
        "A": pa.string(),
        "I": pa.int16(),
        "J": pa.int32(),
        "K": pa.int64(),
        "E": pa.float32(),
        "D": pa.float64(),
        "L": pa.bool_(),
    }[code]


def write_fits_table_parquet(
    path: Path,
    output: Path,
    *,
    batch_size: int = 32_768,
) -> dict[str, Any]:
    import numpy as np
    from astropy.io import fits

    expanded_path = path
    temporary_path: Path | None = None
    if path.suffix == ".gz":
        handle = tempfile.NamedTemporaryFile(
            prefix=f".{path.stem}.", suffix=".fits", dir=output.parent, delete=False
        )
        temporary_path = Path(handle.name)
        try:
            with gzip.open(path, "rb") as source, handle:
                shutil.copyfileobj(source, handle, length=8 * 1024 * 1024)
            expanded_path = temporary_path
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise

    try:
        with fits.open(expanded_path, memmap=True, lazy_load_hdus=False) as hdus:
            table_hdus = [hdu for hdu in hdus if getattr(hdu, "data", None) is not None and hasattr(hdu, "columns")]
            if len(table_hdus) != 1:
                raise ValueError(f"expected one FITS table HDU, found {len(table_hdus)}")
            hdu = table_hdus[0]
            source_schema = []
            arrow_fields = []
            for column in hdu.columns:
                arrow_type = fits_arrow_type(column.format)
                arrow_fields.append(pa.field(column.name, arrow_type))
                source_schema.append(
                    {
                        "name": column.name,
                        "source_format": column.format,
                        "unit": column.unit,
                        "null": column.null,
                        "bscale": column.bscale,
                        "bzero": column.bzero,
                        "display_format": column.disp,
                        "dimension": column.dim,
                        "arrow_type": str(arrow_type),
                    }
                )
            schema = pa.schema(arrow_fields)
            writer = pq.ParquetWriter(output, schema, compression="zstd")
            row_count = int(len(hdu.data))
            try:
                for offset in range(0, row_count, batch_size):
                    end = min(offset + batch_size, row_count)
                    arrays: list[pa.Array] = []
                    for column, field in zip(hdu.columns, schema, strict=True):
                        values = np.asarray(hdu.data[column.name][offset:end])
                        if pa.types.is_string(field.type):
                            decoded = [
                                value.decode("utf-8", errors="replace").rstrip()
                                if isinstance(value, bytes)
                                else str(value).rstrip()
                                for value in values
                            ]
                            arrays.append(pa.array(decoded, type=field.type))
                            continue
                        values = values.astype(values.dtype.newbyteorder("="), copy=True)
                        mask = None
                        if pa.types.is_floating(field.type):
                            mask = np.isnan(values)
                        elif column.null is not None and pa.types.is_integer(field.type):
                            mask = values == column.null
                        arrays.append(pa.array(values, mask=mask, type=field.type, safe=True))
                    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
            finally:
                writer.close()
            return {
                "row_count": row_count,
                "source_schema": source_schema,
                "source_hdu": {
                    "name": hdu.name,
                    "index": hdus.index_of(hdu),
                    "row_count": row_count,
                    "field_count": len(source_schema),
                    "extension_type": type(hdu).__name__,
                },
            }
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
