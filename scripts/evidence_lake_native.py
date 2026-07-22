#!/usr/bin/env python3
"""Source-native parsers used by the Evidence Lake v2 typed compiler."""

from __future__ import annotations

import gzip
import hashlib
import html
import json
import re
import shutil
import tarfile
import tempfile
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, BinaryIO, Iterable, TextIO
from urllib.parse import unquote, urljoin

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
    trailing_layout_delimiters: tuple[str, ...] = (),
) -> tuple[Iterable[dict[str, Any]], dict[str, Any]]:
    if (
        any(
            len(value) != 1 or value.isspace()
            for value in trailing_layout_delimiters
        )
        or len(trailing_layout_delimiters) != len(set(trailing_layout_delimiters))
    ):
        raise ValueError(
            "trailing layout delimiters must be unique single non-whitespace characters"
        )
    counters = {
        "source_line_count": 0,
        "blank_line_count": 0,
        "excluded_line_count": 0,
        "short_row_count": 0,
        "max_row_chars": 0,
    }
    if trailing_layout_delimiters:
        counters["trailing_layout_delimiter_stripped_count"] = 0
        counters["trailing_layout_delimiter_stripped_by_field"] = {}
    delimiter_set = set(trailing_layout_delimiters)
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
                    normalized = value.strip()
                    if normalized and normalized[-1] in delimiter_set:
                        normalized = normalized[:-1].rstrip()
                        field_name = str(field["name"])
                        counters["trailing_layout_delimiter_stripped_count"] += 1
                        by_field = counters["trailing_layout_delimiter_stripped_by_field"]
                        by_field[field_name] = int(by_field.get(field_name, 0)) + 1
                    row[str(field["name"])] = normalized or None
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
    trailing_layout_delimiters: tuple[str, ...] = (),
) -> dict[str, Any]:
    if not fields:
        raise ValueError(f"fixed-width schema has no fields: {path}")
    schema = pa.schema(
        [pa.field("source_line_number", pa.int64())]
        + [pa.field(str(field["name"]), pa.string()) for field in fields]
        + [pa.field("raw_row", pa.string())]
    )
    rows, counters = fixed_width_rows(
        path,
        fields,
        record_pattern=record_pattern,
        trailing_layout_delimiters=trailing_layout_delimiters,
    )
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


class SourceHTMLTableParser(HTMLParser):
    """Collect source table cells without attempting to repair page semantics."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[dict[str, Any]] = []
        self._table: dict[str, Any] | None = None
        self._row: dict[str, Any] | None = None
        self._cell: dict[str, Any] | None = None
        self._section: str | None = None

    @staticmethod
    def attributes(values: list[tuple[str, str | None]]) -> dict[str, str | None]:
        return {name: value for name, value in values}

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attributes = self.attributes(attrs)
        if tag == "table" and self._table is None:
            self._table = {
                "table_id": attributes.get("id"),
                "attributes": attributes,
                "headers": [],
                "rows": [],
                "excluded_footer_row_count": 0,
            }
            return
        if self._table is None:
            return
        if tag in {"thead", "tbody", "tfoot"}:
            self._section = tag
            return
        if tag == "tr" and self._row is None:
            self._row = {
                "attributes": attributes,
                "cells": [],
                "section": self._section,
            }
            return
        if tag in {"th", "td"} and self._row is not None and self._cell is None:
            self._cell = {
                "kind": tag,
                "attributes": attributes,
                "text": [],
                "resources": [],
            }
            return
        if self._cell is not None and tag in {"a", "img", "source"}:
            resource = {
                "tag": tag,
                "attributes": attributes,
            }
            if attributes.get("href") or attributes.get("src") or attributes.get("srcset"):
                self._cell["resources"].append(resource)

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        self.handle_starttag(tag, attrs)

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell["text"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self._cell is not None:
            self._cell["text"] = " ".join("".join(self._cell["text"]).split()) or None
            assert self._row is not None
            self._row["cells"].append(self._cell)
            self._cell = None
            return
        if tag == "tr" and self._row is not None:
            cells = self._row["cells"]
            if cells and all(cell["kind"] == "th" for cell in cells):
                assert self._table is not None
                self._table["headers"] = [cell["text"] for cell in cells]
            elif cells and all(cell["kind"] == "td" for cell in cells):
                assert self._table is not None
                if self._row["section"] == "tfoot":
                    self._table["excluded_footer_row_count"] += 1
                else:
                    self._table["rows"].append(self._row)
            self._row = None
            return
        if tag in {"thead", "tbody", "tfoot"}:
            self._section = None
            return
        if tag == "table" and self._table is not None:
            self.tables.append(self._table)
            self._table = None


def parse_html_tables(path: Path) -> list[dict[str, Any]]:
    parser = SourceHTMLTableParser()
    with open_text(path) as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), ""):
            parser.feed(chunk)
    parser.close()
    if parser._table is not None or parser._row is not None or parser._cell is not None:
        raise ValueError(f"unclosed HTML table structure: {path}")
    return parser.tables


class McGillMagnetarTableParser(HTMLParser):
    """Preserve current McGill table cells and their scoped link resources."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.table_count = 0
        self.rows: list[dict[str, Any]] = []
        self._in_table = False
        self._cells: list[dict[str, Any]] | None = None
        self._cell: dict[str, Any] | None = None
        self._anchor: dict[str, Any] | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attributes = {key: value for key, value in attrs if value is not None}
        if tag == "table":
            self.table_count += 1
            self._in_table = True
        elif tag == "tr" and self._in_table and self._cells is None:
            self._cells = []
        elif tag in {"th", "td"} and self._cells is not None and self._cell is None:
            self._cell = {"kind": tag, "text": [], "resources": []}
        elif tag == "a" and self._cell is not None and self._anchor is None:
            self._anchor = {"attributes": attributes, "text": []}

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell["text"].append(data)
        if self._anchor is not None:
            self._anchor["text"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._anchor is not None:
            self._anchor["text"] = " ".join(
                "".join(self._anchor["text"]).split()
            ) or None
            assert self._cell is not None
            self._cell["resources"].append(self._anchor)
            self._anchor = None
        elif tag in {"th", "td"} and self._cell is not None:
            self._cell["text"] = " ".join(
                "".join(self._cell["text"]).split()
            ) or None
            assert self._cells is not None
            self._cells.append(self._cell)
            self._cell = None
        elif tag == "tr" and self._cells is not None:
            if self._cells and all(cell["kind"] == "td" for cell in self._cells):
                self.rows.append({"cells": self._cells})
            self._cells = None
        elif tag == "table":
            self._in_table = False


def mcgill_resource_kind(href: str) -> str:
    lowered = href.lower()
    if "/abs/" in lowered:
        return "ads_bibliography"
    if "gcn" in lowered:
        return "gcn_circular"
    if "astronomerstelegram" in lowered:
        return "astronomers_telegram"
    if href.startswith("#"):
        return "internal_navigation"
    if not re.match(r"^[a-z]+://", href, flags=re.IGNORECASE):
        return "companion_page"
    return "external_resource"


def mcgill_bibcode(href: str) -> str | None:
    match = re.search(r"/abs/([^?#/]+)", unquote(href), flags=re.IGNORECASE)
    return match.group(1) if match else None


def write_mcgill_magnetar_html_parquet(
    path: Path,
    *,
    rows_output: Path,
    links_output: Path,
    references_output: Path,
    base_url: str,
) -> dict[str, int]:
    parser = McGillMagnetarTableParser()
    with open_text(path) as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), ""):
            parser.feed(chunk)
    parser.close()
    if parser.table_count != 1:
        raise ValueError(f"expected one McGill magnetar table, found {parser.table_count}")
    expected_cells = 17
    for row_number, row in enumerate(parser.rows, start=1):
        if len(row["cells"]) not in {1, expected_cells}:
            raise ValueError(
                f"McGill row {row_number} has {len(row['cells'])} cells; "
                f"expected one section cell or {expected_cells} data cells"
            )

    row_schema = pa.schema(
        [
            pa.field("source_row_number", pa.int64()),
            pa.field("row_kind", pa.string()),
            pa.field("magnetar_name_raw", pa.string()),
            pa.field("cells_json", pa.string()),
            pa.field("resources_json", pa.string()),
        ]
    )
    link_schema = pa.schema(
        [
            pa.field("source_row_number", pa.int64()),
            pa.field("magnetar_name_raw", pa.string()),
            pa.field("source_cell_index", pa.int32()),
            pa.field("cell_text_raw", pa.string()),
            pa.field("resource_index", pa.int32()),
            pa.field("reference_code_raw", pa.string()),
            pa.field("href_raw", pa.string()),
            pa.field("href_absolute", pa.string()),
            pa.field("resource_kind", pa.string()),
            pa.field("bibcode_raw", pa.string()),
            pa.field("attributes_json", pa.string()),
        ]
    )
    reference_schema = pa.schema(
        [
            pa.field("reference_code_raw", pa.string()),
            pa.field("href_absolute", pa.string()),
            pa.field("resource_kind", pa.string()),
            pa.field("bibcode_raw", pa.string()),
            pa.field("occurrence_count", pa.int64()),
        ]
    )

    def row_records() -> Iterable[dict[str, Any]]:
        for row_number, row in enumerate(parser.rows, start=1):
            cells = row["cells"]
            resources = [
                {"cell_index": index, "resources": cell["resources"]}
                for index, cell in enumerate(cells)
                if cell["resources"]
            ]
            yield {
                "source_row_number": row_number,
                "row_kind": "data" if len(cells) == expected_cells else "section",
                "magnetar_name_raw": (
                    cells[0]["text"] if len(cells) == expected_cells else None
                ),
                "cells_json": json.dumps(
                    [cell["text"] for cell in cells],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "resources_json": json.dumps(
                    resources, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ),
            }

    def link_records() -> Iterable[dict[str, Any]]:
        for row_number, row in enumerate(parser.rows, start=1):
            cells = row["cells"]
            if len(cells) != expected_cells:
                continue
            for cell_index, cell in enumerate(cells):
                for resource_index, resource in enumerate(cell["resources"]):
                    href = str(resource["attributes"].get("href") or "").strip()
                    if not href:
                        continue
                    reference_code = str(resource.get("text") or "").strip()
                    if reference_code.startswith("[") and reference_code.endswith("]"):
                        reference_code = reference_code[1:-1].strip()
                    yield {
                        "source_row_number": row_number,
                        "magnetar_name_raw": cells[0]["text"],
                        "source_cell_index": cell_index,
                        "cell_text_raw": cell["text"],
                        "resource_index": resource_index,
                        "reference_code_raw": reference_code or None,
                        "href_raw": href,
                        "href_absolute": urljoin(base_url, href),
                        "resource_kind": mcgill_resource_kind(href),
                        "bibcode_raw": mcgill_bibcode(href),
                        "attributes_json": json.dumps(
                            resource["attributes"],
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    }

    links = list(link_records())
    external_kinds = {"ads_bibliography", "gcn_circular", "astronomers_telegram"}
    reference_counts: dict[tuple[str, str, str, str | None], int] = {}
    hrefs_by_code: dict[str, set[str]] = {}
    for link in links:
        if link["resource_kind"] not in external_kinds:
            continue
        code = str(link["reference_code_raw"] or "").strip()
        if not code:
            raise ValueError("external McGill bibliography link lacks a reference code")
        href = str(link["href_absolute"])
        hrefs_by_code.setdefault(code, set()).add(href)
        key = (code, href, str(link["resource_kind"]), link["bibcode_raw"])
        reference_counts[key] = reference_counts.get(key, 0) + 1
    ambiguous_codes = sorted(
        code for code, hrefs in hrefs_by_code.items() if len(hrefs) != 1
    )
    if ambiguous_codes:
        raise ValueError(f"ambiguous McGill reference-code links: {ambiguous_codes}")

    row_count = write_record_batches(row_records(), row_schema, rows_output)
    link_count = write_record_batches(links, link_schema, links_output)
    reference_count = write_record_batches(
        (
            {
                "reference_code_raw": code,
                "href_absolute": href,
                "resource_kind": resource_kind,
                "bibcode_raw": bibcode,
                "occurrence_count": count,
            }
            for (code, href, resource_kind, bibcode), count in sorted(
                reference_counts.items()
            )
        ),
        reference_schema,
        references_output,
    )
    return {
        "source_table_count": parser.table_count,
        "source_data_row_count": sum(
            len(row["cells"]) == expected_cells for row in parser.rows
        ),
        "source_section_row_count": sum(
            len(row["cells"]) == 1 for row in parser.rows
        ),
        "typed_row_count": row_count,
        "typed_link_count": link_count,
        "typed_external_reference_count": reference_count,
    }


def write_html_table_parquet(
    path: Path,
    output: Path,
    *,
    table_id: str,
    fields: list[dict[str, str]],
) -> dict[str, Any]:
    page_tables = parse_html_tables(path)
    tables = [table for table in page_tables if table["table_id"] == table_id]
    if len(tables) != 1:
        raise ValueError(f"expected one HTML table {table_id!r}, found {len(tables)}")
    table = tables[0]
    source_headers = table["headers"]
    expected_headers = [field["source_header"] for field in fields]
    if source_headers != expected_headers:
        raise ValueError(
            f"HTML table header drift for {table_id}: {source_headers!r} != {expected_headers!r}"
        )
    field_names = [field["name"] for field in fields]
    if len(field_names) != len(set(field_names)):
        raise ValueError(f"HTML table contract has duplicate field names: {field_names}")

    schema = pa.schema(
        [
            pa.field("source_table_id", pa.string()),
            pa.field("source_row_number", pa.int64()),
            pa.field("source_row_id", pa.string()),
            pa.field("source_row_index", pa.int64()),
            pa.field("source_row_attributes_json", pa.string()),
            pa.field("source_cell_resources_json", pa.string()),
        ]
        + [pa.field(name, pa.string()) for name in field_names]
    )

    def rows() -> Iterable[dict[str, Any]]:
        for row_number, source_row in enumerate(table["rows"], start=1):
            cells = source_row["cells"]
            if len(cells) != len(fields):
                raise ValueError(
                    f"HTML row {row_number} in {table_id} has {len(cells)} cells; "
                    f"expected {len(fields)}"
                )
            attributes = source_row["attributes"]
            row_index_raw = attributes.get("data-row-index")
            try:
                row_index = int(row_index_raw) if row_index_raw is not None else None
            except ValueError as exc:
                raise ValueError(
                    f"HTML row {row_number} has non-integer data-row-index: {row_index_raw!r}"
                ) from exc
            resources = []
            for index, cell in enumerate(cells):
                if cell["resources"]:
                    resources.append(
                        {
                            "cell_index": index,
                            "field_name": field_names[index],
                            "resources": cell["resources"],
                        }
                    )
            result = {
                "source_table_id": table_id,
                "source_row_number": row_number,
                "source_row_id": attributes.get("id"),
                "source_row_index": row_index,
                "source_row_attributes_json": json.dumps(
                    attributes, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ),
                "source_cell_resources_json": json.dumps(
                    resources, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ),
            }
            result.update(
                {
                    field_name: cell["text"]
                    for field_name, cell in zip(field_names, cells, strict=True)
                }
            )
            yield result

    row_count = write_record_batches(rows(), schema, output)
    return {
        "row_count": row_count,
        "source_table_id": table_id,
        "source_schema": fields,
        "source_table_count": len(page_tables),
        "excluded_page_table_count": len(page_tables) - 1,
        "excluded_footer_row_count": table["excluded_footer_row_count"],
    }


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


OEC_OBJECT_TAGS = {"system", "star", "binary", "planet", "satellite", "asteroid"}


def write_oec_archive_parquet(
    archive: tarfile.TarFile,
    *,
    objects_output: Path,
    names_output: Path,
    parameters_output: Path,
    relations_output: Path,
) -> dict[str, int]:
    """Preserve the OEC XML graph without promoting names into identity."""

    object_schema = pa.schema(
        [
            pa.field("source_member", pa.string()),
            pa.field("source_node_path", pa.string()),
            pa.field("parent_node_path", pa.string()),
            pa.field("object_kind", pa.string()),
            pa.field("primary_name_raw", pa.string()),
            pa.field("list_disposition_raw", pa.string()),
            pa.field("attributes_json", pa.string()),
        ]
    )
    name_schema = pa.schema(
        [
            pa.field("source_member", pa.string()),
            pa.field("source_node_path", pa.string()),
            pa.field("object_kind", pa.string()),
            pa.field("name_occurrence", pa.int32()),
            pa.field("name_raw", pa.string()),
        ]
    )
    parameter_schema = pa.schema(
        [
            pa.field("source_member", pa.string()),
            pa.field("source_node_path", pa.string()),
            pa.field("object_kind", pa.string()),
            pa.field("parameter_name", pa.string()),
            pa.field("parameter_occurrence", pa.int32()),
            pa.field("value_raw", pa.string()),
            pa.field("attributes_json", pa.string()),
        ]
    )
    relation_schema = pa.schema(
        [
            pa.field("source_member", pa.string()),
            pa.field("parent_node_path", pa.string()),
            pa.field("parent_object_kind", pa.string()),
            pa.field("parent_primary_name_raw", pa.string()),
            pa.field("child_node_path", pa.string()),
            pa.field("child_object_kind", pa.string()),
            pa.field("child_primary_name_raw", pa.string()),
            pa.field("child_occurrence", pa.int32()),
            pa.field("relation_kind", pa.string()),
        ]
    )
    object_rows: list[dict[str, Any]] = []
    name_rows: list[dict[str, Any]] = []
    parameter_rows: list[dict[str, Any]] = []
    relation_rows: list[dict[str, Any]] = []
    xml_members = 0

    def direct_text(element: ET.Element, tag: str) -> str | None:
        child = next((row for row in element if row.tag == tag), None)
        if child is None:
            return None
        return (child.text or "").strip() or None

    def visit(
        element: ET.Element,
        *,
        member_name: str,
        node_path: str,
        parent_path: str | None,
        parent_kind: str | None,
        parent_name: str | None,
        child_occurrence: int,
    ) -> None:
        object_kind = str(element.tag)
        names = [
            (child.text or "").strip()
            for child in element
            if child.tag == "name" and (child.text or "").strip()
        ]
        primary_name = names[0] if names else None
        object_rows.append(
            {
                "source_member": member_name,
                "source_node_path": node_path,
                "parent_node_path": parent_path,
                "object_kind": object_kind,
                "primary_name_raw": primary_name,
                "list_disposition_raw": direct_text(element, "list"),
                "attributes_json": json.dumps(
                    element.attrib,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }
        )
        for occurrence, name in enumerate(names, start=1):
            name_rows.append(
                {
                    "source_member": member_name,
                    "source_node_path": node_path,
                    "object_kind": object_kind,
                    "name_occurrence": occurrence,
                    "name_raw": name,
                }
            )
        if parent_path is not None:
            relation_rows.append(
                {
                    "source_member": member_name,
                    "parent_node_path": parent_path,
                    "parent_object_kind": parent_kind,
                    "parent_primary_name_raw": parent_name,
                    "child_node_path": node_path,
                    "child_object_kind": object_kind,
                    "child_primary_name_raw": primary_name,
                    "child_occurrence": child_occurrence,
                    "relation_kind": f"contains_{object_kind}",
                }
            )
        parameter_occurrences: dict[str, int] = {}
        object_occurrences: dict[str, int] = {}
        for child in element:
            if child.tag == "name":
                continue
            if child.tag in OEC_OBJECT_TAGS:
                object_occurrences[child.tag] = object_occurrences.get(child.tag, 0) + 1
                occurrence = object_occurrences[child.tag]
                visit(
                    child,
                    member_name=member_name,
                    node_path=f"{node_path}/{child.tag}[{occurrence}]",
                    parent_path=node_path,
                    parent_kind=object_kind,
                    parent_name=primary_name,
                    child_occurrence=occurrence,
                )
                continue
            if list(child):
                raise ValueError(
                    f"unsupported non-object OEC container {child.tag!r} in {member_name}"
                )
            parameter_occurrences[child.tag] = parameter_occurrences.get(child.tag, 0) + 1
            parameter_rows.append(
                {
                    "source_member": member_name,
                    "source_node_path": node_path,
                    "object_kind": object_kind,
                    "parameter_name": str(child.tag),
                    "parameter_occurrence": parameter_occurrences[child.tag],
                    "value_raw": (child.text or "").strip() or None,
                    "attributes_json": json.dumps(
                        child.attrib,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
            )

    for member in safe_tar_members(archive):
        if not member.name.endswith(".xml") or not (
            "/systems/" in member.name or "/systems_kepler/" in member.name
        ):
            continue
        xml_members += 1
        root = ET.fromstring(tar_member_bytes(archive, member.name))
        if root.tag != "system":
            raise ValueError(f"unexpected OEC root {root.tag!r}: {member.name}")
        visit(
            root,
            member_name=member.name,
            node_path="/system[1]",
            parent_path=None,
            parent_kind=None,
            parent_name=None,
            child_occurrence=1,
        )

    counts = {
        "xml_member_count": xml_members,
        "object_row_count": write_record_batches(object_rows, object_schema, objects_output),
        "name_row_count": write_record_batches(name_rows, name_schema, names_output),
        "parameter_row_count": write_record_batches(
            parameter_rows, parameter_schema, parameters_output
        ),
        "relation_row_count": write_record_batches(
            relation_rows, relation_schema, relations_output
        ),
    }
    if counts["xml_member_count"] != sum(
        row["object_kind"] == "system" for row in object_rows
    ):
        raise ValueError("OEC system/member accounting mismatch")
    return counts


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


def fits_arrow_type(
    source_format: str,
    dimension: str | None = None,
) -> pa.DataType:
    match = re.fullmatch(r"(?:(\d+))?([AIJKEDL])", source_format.strip().upper())
    if not match:
        raise ValueError(f"unsupported FITS column format: {source_format}")
    repeat = int(match.group(1) or 1)
    code = match.group(2)
    arrow_type: pa.DataType = {
        "A": pa.string(),
        "I": pa.int16(),
        "J": pa.int32(),
        "K": pa.int64(),
        "E": pa.float32(),
        "D": pa.float64(),
        "L": pa.bool_(),
    }[code]
    dimensions = (
        [int(value) for value in re.findall(r"\d+", dimension)]
        if dimension
        else []
    )
    if code == "A":
        # FITS character width is the first TDIM axis; Astropy exposes each
        # remaining axis as an array of decoded strings.
        dimensions = dimensions[1:]
    elif not dimensions and repeat > 1:
        dimensions = [repeat]
    for size in dimensions:
        arrow_type = pa.list_(arrow_type, size)
    return arrow_type


def fits_array(
    values: Any,
    field: pa.Field,
    *,
    null: int | None,
) -> pa.Array:
    import numpy as np

    data = np.asarray(values)
    list_dimensions = list(data.shape[1:])
    primitive_type = field.type
    while pa.types.is_fixed_size_list(primitive_type):
        primitive_type = primitive_type.value_type

    flat = data.reshape(-1)
    if pa.types.is_string(primitive_type):
        converted = [
            value.decode("utf-8", errors="replace").rstrip()
            if isinstance(value, bytes)
            else str(value).rstrip()
            for value in flat
        ]
        array: pa.Array = pa.array(converted, type=primitive_type)
    else:
        flat = flat.astype(flat.dtype.newbyteorder("="), copy=True)
        mask = None
        if pa.types.is_floating(primitive_type):
            mask = np.isnan(flat)
        elif null is not None and pa.types.is_integer(primitive_type):
            mask = flat == null
        array = pa.array(flat, mask=mask, type=primitive_type, safe=True)

    for size in reversed(list_dimensions):
        array = pa.FixedSizeListArray.from_arrays(array, size)
    if array.type != field.type:
        array = array.cast(field.type, safe=True)
    return array


def write_fits_table_parquet(
    path: Path,
    output: Path,
    *,
    batch_size: int = 32_768,
    hdu_index: int | None = None,
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
            if hdu_index is None and len(table_hdus) != 1:
                raise ValueError(f"expected one FITS table HDU, found {len(table_hdus)}")
            if hdu_index is None:
                hdu = table_hdus[0]
            else:
                if hdu_index < 0 or hdu_index >= len(hdus):
                    raise ValueError(f"FITS HDU index out of range: {hdu_index}")
                hdu = hdus[hdu_index]
                if hdu not in table_hdus:
                    raise ValueError(f"FITS HDU {hdu_index} is not a table")
            source_schema = []
            arrow_fields = []
            for column in hdu.columns:
                arrow_type = fits_arrow_type(column.format, column.dim)
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
                        arrays.append(
                            fits_array(values, field, null=column.null)
                        )
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


def write_votable_files_parquet(
    paths: list[Path], output: Path, *, member_lineage_field: str | None = None
) -> dict[str, Any]:
    """Preserve typed VOTable BINARY/BINARY2 response sets as one Parquet table."""
    import io
    import numpy as np
    from astropy.io.votable import parse_single_table

    writer: pq.ParquetWriter | None = None
    arrow_schema: pa.Schema | None = None
    source_schema: list[dict[str, Any]] | None = None
    total_rows = 0
    file_rows: list[dict[str, Any]] = []
    try:
        for path in sorted(paths):
            payload = path.read_bytes()
            if path.name.endswith(".gz"):
                payload = gzip.decompress(payload)
            try:
                table = parse_single_table(io.BytesIO(payload))
            except ValueError as exc:
                if member_lineage_field and "No table found" in str(exc):
                    file_rows.append(
                        {"path": path.name, "row_count": 0, "source_status": "no_table"}
                    )
                    continue
                raise
            fields = list(table.fields)
            source_field_names = [
                str(field.name or field.ID or field._unique_name) for field in fields
            ]
            if len(source_field_names) != len(set(source_field_names)):
                raise ValueError(f"VOTable contains duplicate field names: {path}")
            field_names: list[str] = []
            casefold_names: set[str] = set()
            for source_name in source_field_names:
                name = source_name
                occurrence = 2
                while name.casefold() in casefold_names:
                    name = f"{source_name}__source_case_{occurrence}"
                    occurrence += 1
                casefold_names.add(name.casefold())
                field_names.append(name)
            if member_lineage_field:
                if member_lineage_field.casefold() in casefold_names:
                    raise ValueError(
                        f"VOTable member lineage field collides with source field: "
                        f"{member_lineage_field}"
                    )
                field_names.append(member_lineage_field)
            current_source_schema = [
                {
                    "name": name,
                    **(
                        {
                            "source_name": source_name,
                            "name_normalization": "case_insensitive_collision_alias_v1",
                        }
                        if name != source_name
                        else {}
                    ),
                    "id": field.ID,
                    "datatype": field.datatype,
                    "arraysize": field.arraysize,
                    "unit": str(field.unit) if field.unit is not None else None,
                    "ucd": field.ucd,
                    "description": field.description,
                }
                for name, source_name, field in zip(
                    field_names[: len(source_field_names)], source_field_names, fields, strict=True
                )
            ]
            if member_lineage_field:
                current_source_schema.append(
                    {
                        "name": member_lineage_field,
                        "id": None,
                        "datatype": "char",
                        "arraysize": "*",
                        "unit": None,
                        "ucd": None,
                        "description": "Spacegate raw response member filename",
                        "lineage_kind": "raw_artifact_member",
                    }
                )
            if source_schema is None:
                source_schema = current_source_schema
            elif current_source_schema != source_schema:
                raise ValueError(f"VOTable source schema drift: {path}")

            arrays: list[pa.Array] = []
            for field, name in zip(
                fields, field_names[: len(source_field_names)], strict=True
            ):
                values = table.array[field._unique_name]
                mask = np.ma.getmaskarray(values)
                if values.ndim == 1:
                    data = np.asarray(values.data)
                    if data.dtype.kind in {"U", "S", "O"}:
                        converted = [
                            None if bool(mask[index]) else str(data[index]).rstrip()
                            for index in range(len(data))
                        ]
                        array = pa.array(converted)
                    else:
                        data = data.astype(data.dtype.newbyteorder("="), copy=False)
                        array = pa.array(data, mask=mask, safe=True)
                else:
                    converted = []
                    for index in range(len(values)):
                        row_mask = np.asarray(mask[index])
                        row_data = np.asarray(values.data[index])
                        if row_mask.shape == () and bool(row_mask):
                            converted.append(None)
                        else:
                            row = row_data.tolist()
                            if row_mask.shape != () and bool(row_mask.any()):
                                row = [
                                    None if bool(item_mask) else item
                                    for item, item_mask in zip(row, row_mask.tolist(), strict=True)
                                ]
                            converted.append(row)
                    array = pa.array(converted)
                arrays.append(array)
            if member_lineage_field:
                arrays.append(pa.array([path.name] * len(table.array), type=pa.string()))

            if arrow_schema is None:
                arrow_schema = pa.schema(
                    [pa.field(name, array.type) for name, array in zip(field_names, arrays, strict=True)]
                )
                writer = pq.ParquetWriter(output, arrow_schema, compression="zstd")
            assert arrow_schema is not None and writer is not None
            cast_arrays = [
                array if array.type == field.type else array.cast(field.type, safe=True)
                for array, field in zip(arrays, arrow_schema, strict=True)
            ]
            arrow_table = pa.Table.from_arrays(cast_arrays, schema=arrow_schema)
            writer.write_table(arrow_table, row_group_size=122880)
            rows = len(table.array)
            total_rows += rows
            file_rows.append({"path": path.name, "row_count": rows, "source_status": "table"})
    finally:
        if writer is not None:
            writer.close()
    if writer is None or source_schema is None or arrow_schema is None:
        raise ValueError("VOTable response set contained no tables")
    return {
        "row_count": total_rows,
        "source_schema": source_schema,
        "arrow_schema": [{"name": field.name, "type": str(field.type)} for field in arrow_schema],
        "file_row_accounting": file_rows,
        "member_lineage_field": member_lineage_field,
    }
