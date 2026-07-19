#!/usr/bin/env python3
"""Acquire release-pinned TAP products as resumable immutable response sets.

The acquisition program is data, not code.  Each product records every exact
ADQL request and byte response, then publishes one legacy-compatible manifest
entry so the Evidence Lake raw/typed compiler can snapshot it.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
import io
import json
import os
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROGRAM = ROOT / "config" / "evidence_lake" / "e3_acquisition_program.json"
DEFAULT_STATE = Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state"))
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
CONTRACT = "spacegate.tap_acquisition_snapshot.v1"
ENGINE_VERSION = "evidence_tap_acquire_v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_report(path: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": child.relative_to(path).as_posix(),
            "bytes": child.stat().st_size,
            "sha256": file_hash(child),
        }
        for child in sorted(path.rglob("*"))
        if child.is_file()
    ]


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def merge_manifest_rows(path: Path, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge source rows under an inter-process lock and promote atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        existing = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
        replacements = {row["source_name"]: row for row in rows}
        merged = [
            row for row in existing if row.get("source_name") not in replacements
        ]
        merged.extend(rows)
        merged.sort(key=lambda row: row["source_name"])
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as handle:
            json.dump(merged, handle, indent=2, sort_keys=True)
            handle.write("\n")
            temporary = Path(handle.name)
        os.replace(temporary, path)
        return merged


def tap_request(
    endpoint: str,
    adql: str,
    *,
    timeout_s: int,
    retries: int,
    max_rec: int,
) -> bytes:
    body = urllib.parse.urlencode(
        {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "FORMAT": "csv",
            "MAXREC": str(max_rec),
            "QUERY": adql,
        }
    ).encode("ascii")
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(
                endpoint,
                data=body,
                headers={
                    "User-Agent": USER_AGENT,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                payload = response.read()
            head = payload[:1000].decode("utf-8", errors="replace").lower()
            if "usagefault" in head or head.lstrip().startswith("error"):
                raise RuntimeError(head[:500])
            return payload
        except Exception as exc:  # network behavior is exercised operationally
            last_error = exc
            if attempt == retries:
                break
            delay = min(2**attempt, 30)
            print(
                f"{utc_now()} retry {attempt}/{retries - 1}: "
                f"{type(exc).__name__}: {exc}; sleeping {delay}s",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"TAP request failed after {retries} attempts: {last_error}")


def read_url(url: str, *, timeout_s: int) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return response.read()


def tap_async_request(
    endpoint: str,
    adql: str,
    *,
    timeout_s: int,
    retries: int,
    max_rec: int,
    status_path: Path | None = None,
    output_format: str = "csv",
) -> tuple[bytes, dict[str, Any]]:
    """Run one IVOA UWS TAP job and return its exact result body and lineage."""
    async_endpoint = endpoint[:-5] + "/async" if endpoint.endswith("/sync") else endpoint
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        submitted_at = utc_now()
        try:
            body = urllib.parse.urlencode(
                {
                    "REQUEST": "doQuery",
                    "LANG": "ADQL",
                    "FORMAT": output_format,
                    "MAXREC": str(max_rec),
                    "PHASE": "RUN",
                    "QUERY": adql,
                }
            ).encode("ascii")
            request = urllib.request.Request(
                async_endpoint,
                data=body,
                headers={
                    "User-Agent": USER_AGENT,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            with urllib.request.urlopen(request, timeout=min(timeout_s, 120)) as response:
                job_url = response.geturl().rstrip("/")
                response.read()
            status = {
                "mode": "ivoa_uws_async",
                "job_url": job_url,
                "submitted_at": submitted_at,
                "phases": [],
                "attempt": attempt,
            }
            if status_path is not None:
                write_json(status_path, status)
            print(f"{utc_now()} async submitted {job_url}", flush=True)
            deadline = time.monotonic() + timeout_s
            phases: list[dict[str, str]] = []
            previous = None
            while True:
                phase = read_url(job_url + "/phase", timeout_s=min(timeout_s, 120)).decode(
                    "utf-8", errors="replace"
                ).strip()
                if phase != previous:
                    phases.append({"phase": phase, "observed_at": utc_now()})
                    previous = phase
                    status["phases"] = phases
                    if status_path is not None:
                        write_json(status_path, status)
                    print(f"{utc_now()} async {job_url} phase={phase}", flush=True)
                if phase == "COMPLETED":
                    break
                if phase in {"ERROR", "ABORTED", "HELD", "SUSPENDED", "ARCHIVED"}:
                    detail = ""
                    try:
                        detail = read_url(
                            job_url + "/error", timeout_s=min(timeout_s, 120)
                        ).decode("utf-8", errors="replace")[:2000]
                    except Exception:
                        pass
                    raise RuntimeError(f"Gaia TAP async job {phase}: {detail}")
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Gaia TAP async job exceeded {timeout_s}s: {job_url}")
                time.sleep(5)
            payload = read_url(job_url + "/results/result", timeout_s=timeout_s)
            status["completed_at"] = utc_now()
            return payload, status
        except Exception as exc:  # network behavior is exercised operationally
            last_error = exc
            if attempt == retries:
                break
            delay = min(2**attempt, 30)
            print(
                f"{utc_now()} async retry {attempt}/{retries - 1}: "
                f"{type(exc).__name__}: {exc}; sleeping {delay}s",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"TAP async request failed after {retries} attempts: {last_error}")


def csv_metadata(payload: bytes) -> tuple[list[str], int]:
    text = payload.decode("utf-8-sig", errors="strict")
    reader = csv.reader(io.StringIO(text))
    fields = next(reader, [])
    if not fields:
        raise ValueError("TAP response has no CSV header")
    return fields, sum(1 for _ in reader)


def response_metadata(payload: bytes, output_format: str) -> tuple[list[str], int]:
    if output_format == "csv":
        return csv_metadata(payload)
    if output_format == "votable_gzip":
        from astropy.io.votable import parse_single_table

        table = parse_single_table(io.BytesIO(gzip.decompress(payload)))
        return [str(field.name or field.ID or field._unique_name) for field in table.fields], len(
            table.array
        )
    raise ValueError(f"unsupported TAP output format: {output_format}")


def response_suffix(output_format: str) -> str:
    if output_format == "csv":
        return ".csv.gz"
    if output_format == "votable_gzip":
        return ".vot.gz"
    raise ValueError(f"unsupported TAP output format: {output_format}")


def read_response(path: Path, output_format: str) -> bytes:
    payload = path.read_bytes()
    if output_format == "csv":
        return gzip.decompress(payload) if path.name.endswith(".gz") else payload
    return payload


def query_schema(
    endpoint: str,
    table_name: str,
    *,
    timeout_s: int,
    retries: int,
) -> list[dict[str, str]]:
    vizier = "tapvizier" in endpoint.lower()
    normalized_table_name = table_name.strip("'\"") if vizier else table_name
    safe_name = normalized_table_name.replace("'", "''")
    predicate = (
        f"table_name like '%{safe_name}%'" if vizier else f"table_name='{safe_name}'"
    )
    order_clause = "" if vizier else " order by column_index"
    adql = (
        "select column_name,datatype,unit,ucd,description "
        "from tap_schema.columns "
        f"where {predicate}{order_clause}"
    )
    payload = tap_request(endpoint, adql, timeout_s=timeout_s, retries=retries, max_rec=10000)
    rows = [dict(row) for row in csv.DictReader(io.StringIO(payload.decode("utf-8-sig")))]
    normalized: list[dict[str, str]] = []
    by_name: dict[str, dict[str, str]] = {}
    for row in rows:
        row["column_name"] = str(row.get("column_name") or "").strip("'")
        name = row["column_name"]
        if name in by_name:
            comparable = {key: value for key, value in row.items() if key != "description"}
            previous = {key: value for key, value in by_name[name].items() if key != "description"}
            if comparable != previous:
                raise ValueError(f"conflicting duplicate TAP schema field {table_name}.{name}")
            continue
        by_name[name] = row
        normalized.append(row)
    return normalized


def resolve_product_fields(
    product: dict[str, Any], schema: list[dict[str, str]]
) -> dict[str, Any]:
    """Expand declarative field groups against the release's official schema."""
    names = [row["column_name"] for row in schema]
    if product.get("preserve_all_fields"):
        selected = names
    elif product.get("select"):
        return dict(product)
    else:
        exact = set(product.get("include_fields") or [])
        prefixes = tuple(product.get("include_prefixes") or [])
        suffixes = tuple(product.get("include_suffixes") or [])
        fragments = tuple(product.get("include_contains") or [])
        ranged: set[str] = set()
        for start, end in product.get("include_field_ranges") or []:
            if start not in names or end not in names or names.index(start) > names.index(end):
                raise ValueError(f"invalid field range for {product['table']}: {start}..{end}")
            ranged.update(names[names.index(start) : names.index(end) + 1])
        selected = [
            name
            for name in names
            if name in exact
            or name in ranged
            or (prefixes and name.startswith(prefixes))
            or (suffixes and name.endswith(suffixes))
            or (fragments and any(fragment in name for fragment in fragments))
        ]
        missing = sorted(exact - set(names))
        if missing:
            raise ValueError(f"configured fields absent from {product['table']}: {missing}")
    if not selected:
        raise ValueError(f"no fields selected for {product['product_name']}")
    resolved = dict(product)
    alias = str(product.get("table_alias") or "").strip()
    resolved["select"] = [f"{alias}.{name}" if alias else name for name in selected]
    return resolved


def product_identity(program: dict[str, Any], product: dict[str, Any]) -> str:
    return stable_hash(
        {
            "program_contract": program["schema_version"],
            "engine_version": ENGINE_VERSION,
            "source_id": product["source_id"],
            "release_id": product["release_id"],
            "endpoint": product["endpoint"],
            "table": product["table"],
            "select": product["select"],
            "from": product.get("from") or product["table"],
            "where": product["where"],
            "partition_expression": product["partition_expression"],
            "order_by": product.get("order_by") or product["partition_expression"],
            "ordered": product.get("ordered", True),
            "buckets": product["buckets"],
            "max_rec": product["max_rec"],
            "tap_mode": product.get("tap_mode", "sync"),
            "output_format": product.get("output_format", "csv"),
        }
    )[:24]


def render_query(product: dict[str, Any], bucket: int) -> str:
    selected = ",\n  ".join(product["select"])
    from_clause = product.get("from") or product["table"]
    order_by = product.get("order_by") or product["partition_expression"]
    query = (
        "select\n  "
        + selected
        + "\nfrom "
        + from_clause
        + "\nwhere ("
        + product["where"]
        + ")\n  and mod("
        + product["partition_expression"]
        + f", {int(product['buckets'])}) = {bucket}"
    )
    if product.get("ordered", True):
        query += "\norder by " + order_by
    return query


def acquire_product(
    program: dict[str, Any],
    product: dict[str, Any],
    *,
    state_dir: Path,
    workers: int,
    timeout_s: int,
    retries: int,
    refresh: bool,
) -> dict[str, Any]:
    schema = query_schema(
        product["endpoint"],
        product.get("schema_table_name") or product["table"],
        timeout_s=timeout_s,
        retries=retries,
    )
    product = resolve_product_fields(product, schema)
    snapshot_id = product_identity(program, product)
    root = (
        state_dir
        / "raw"
        / "evidence_lake_v2_acquisition"
        / product["source_id"].replace(".", "_")
        / product["release_id"].replace("/", "_")
        / "snapshots"
        / snapshot_id
        / product["product_name"]
    )
    query_dir = root / "queries"
    response_dir = root / "responses"
    query_dir.mkdir(parents=True, exist_ok=True)
    response_dir.mkdir(parents=True, exist_ok=True)
    completed_manifest_path = root / "product_manifest.json"
    if refresh and completed_manifest_path.exists():
        raise ValueError(
            "refusing to refresh a completed content-addressed snapshot; "
            "bump the acquisition program or release version"
        )
    buckets = int(product["buckets"])
    max_rec = int(product["max_rec"])
    output_format = str(product.get("output_format") or "csv")
    suffix = response_suffix(output_format)
    expected_fields = [
        value.rsplit(" as ", 1)[-1].strip() if " as " in value.lower() else value.split(".")[-1].strip()
        for value in product["select"]
    ]

    pending: list[int] = []
    reused = 0
    for bucket in range(buckets):
        query = render_query(product, bucket)
        query_path = query_dir / f"bucket_{bucket:05d}.adql"
        response_path = response_dir / f"bucket_{bucket:05d}{suffix}"
        if not query_path.exists() or query_path.read_text(encoding="utf-8") != query + "\n":
            query_path.write_text(query + "\n", encoding="utf-8")
        valid = False
        if response_path.exists() and not refresh:
            fields, rows = response_metadata(
                read_response(response_path, output_format), output_format
            )
            valid = fields == expected_fields and rows < max_rec
        if valid:
            reused += 1
        else:
            response_path.unlink(missing_ok=True)
            pending.append(bucket)

    def fetch(bucket: int) -> tuple[int, int, int, str]:
        query = render_query(product, bucket)
        uws: dict[str, Any] | None = None
        if product.get("tap_mode") == "async":
            payload, uws = tap_async_request(
                product["endpoint"],
                query,
                timeout_s=timeout_s,
                retries=retries,
                max_rec=max_rec,
                status_path=query_dir / f"bucket_{bucket:05d}.uws.json",
                output_format=output_format,
            )
        else:
            payload = tap_request(
                product["endpoint"],
                query,
                timeout_s=timeout_s,
                retries=retries,
                max_rec=max_rec,
            )
        fields, rows = response_metadata(payload, output_format)
        if fields != expected_fields:
            raise ValueError(
                f"{product['product_name']} bucket {bucket} schema drift: "
                f"{fields} != {expected_fields}"
            )
        if rows >= max_rec:
            raise ValueError(
                f"{product['product_name']} bucket {bucket} reached MAXREC {max_rec}; "
                "increase buckets before retrying"
            )
        destination = response_dir / f"bucket_{bucket:05d}{suffix}"
        stored_payload = (
            gzip.compress(payload, compresslevel=6, mtime=0)
            if output_format == "csv"
            else payload
        )
        with tempfile.NamedTemporaryFile(dir=response_dir, delete=False) as handle:
            handle.write(stored_payload)
            temporary = Path(handle.name)
        os.replace(temporary, destination)
        if uws is not None:
            write_json(query_dir / f"bucket_{bucket:05d}.uws.json", uws)
        return bucket, rows, len(stored_payload), hashlib.sha256(payload).hexdigest()

    fetched: dict[int, dict[str, Any]] = {}
    if pending:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {executor.submit(fetch, bucket): bucket for bucket in pending}
            for future in as_completed(futures):
                bucket, rows, size, digest = future.result()
                fetched[bucket] = {"rows": rows, "bytes": size, "sha256": digest}
                print(
                    f"{utc_now()} {product['product_name']} "
                    f"bucket={bucket + 1}/{buckets} rows={rows:,}",
                    flush=True,
                )

    response_rows = 0
    response_bytes = 0
    response_hashes: list[dict[str, Any]] = []
    for bucket in range(buckets):
        path = response_dir / f"bucket_{bucket:05d}{suffix}"
        payload = read_response(path, output_format)
        fields, rows = response_metadata(payload, output_format)
        if fields != expected_fields or rows >= max_rec:
            raise ValueError(f"invalid completed response {path}")
        response_rows += rows
        response_bytes += path.stat().st_size
        response_hashes.append(
            {
                "bucket": bucket,
                "rows": rows,
                "response_sha256": hashlib.sha256(payload).hexdigest(),
                "stored_sha256": file_hash(path),
            }
        )

    response_set_sha256 = stable_hash(response_hashes)
    if not pending and completed_manifest_path.exists():
        completed = json.loads(completed_manifest_path.read_text(encoding="utf-8"))
        if (
            completed.get("schema_version") == CONTRACT
            and completed.get("snapshot_id") == snapshot_id
            and completed.get("response_set_sha256") == response_set_sha256
            and int(completed.get("rows", -1)) == response_rows
        ):
            files = tree_report(root)
            return {
                "source_name": product["product_name"],
                "source_version": product["release_id"],
                "url": product["endpoint"],
                "dest_path": str(root),
                "retrieved_at": completed["checked_at"],
                "checked_at": completed["checked_at"],
                "sha256": stable_hash(files),
                "bytes_written": sum(row["bytes"] for row in files),
                "row_count": response_rows,
                "query_signature": stable_hash(
                    [render_query(product, bucket) for bucket in range(buckets)]
                ),
                "snapshot_id": snapshot_id,
                "field_disposition_report": str(completed_manifest_path),
            }

    selected_names = set(expected_fields)
    explicit_omissions = product.get("omissions") or {}
    field_dispositions = []
    for field in schema:
        name = field["column_name"]
        if name in selected_names:
            disposition = "preserve"
            reason = "selected_source_native_evidence"
        else:
            disposition = "omit"
            reason = explicit_omissions.get(name) or product["unselected_field_reason"]
        field_dispositions.append({**field, "disposition": disposition, "reason": reason})
    unknown_selected = sorted(selected_names - {field["column_name"] for field in schema})
    if unknown_selected:
        raise ValueError(f"selected fields absent from upstream schema: {unknown_selected}")

    product_report = {
        "schema_version": CONTRACT,
        "program_version": program["program_version"],
        "snapshot_id": snapshot_id,
        "source_id": product["source_id"],
        "release_id": product["release_id"],
        "product_name": product["product_name"],
        "endpoint": product["endpoint"],
        "table": product["table"],
        "where": product["where"],
        "buckets": buckets,
        "max_rec": max_rec,
        "output_format": output_format,
        "rows": response_rows,
        "response_bytes": response_bytes,
        "responses_reused": reused,
        "responses_fetched": len(fetched),
        "response_set_sha256": response_set_sha256,
        "selected_field_count": len(selected_names),
        "upstream_field_count": len(schema),
        "omitted_field_count": len(schema) - len(selected_names),
        "field_dispositions": field_dispositions,
        "checked_at": utc_now(),
    }
    files = [row for row in tree_report(root) if row["path"] != "product_manifest.json"]
    product_report["payload_tree_sha256"] = stable_hash(files)
    product_report["payload_files"] = len(files)
    product_report["payload_bytes"] = sum(row["bytes"] for row in files)
    write_json(root / "product_manifest.json", product_report)
    # The legacy manifest covers the finalized, self-describing artifact tree.
    files = tree_report(root)
    tree_digest = stable_hash(files)
    return {
        "source_name": product["product_name"],
        "source_version": product["release_id"],
        "url": product["endpoint"],
        "dest_path": str(root),
        "retrieved_at": product_report["checked_at"],
        "checked_at": product_report["checked_at"],
        "sha256": tree_digest,
        "bytes_written": sum(row["bytes"] for row in files),
        "row_count": response_rows,
        "query_signature": stable_hash(
            [render_query(product, bucket) for bucket in range(buckets)]
        ),
        "snapshot_id": snapshot_id,
        "field_disposition_report": str(root / "product_manifest.json"),
    }


def build_coverage_report(
    program: dict[str, Any], manifest_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    by_name = {str(row["source_name"]): row for row in manifest_rows}
    expected = {str(product["product_name"]): product for product in program["products"]}
    completed = sorted(set(expected) & set(by_name))
    pending = sorted(set(expected) - set(by_name))
    table_reports: list[dict[str, Any]] = []
    for table_name in sorted({str(product["table"]) for product in program["products"]}):
        products = [product for product in program["products"] if product["table"] == table_name]
        acquired_reports = []
        for product in products:
            row = by_name.get(str(product["product_name"]))
            if row and Path(str(row["field_disposition_report"])).exists():
                acquired_reports.append(
                    json.loads(Path(str(row["field_disposition_report"])).read_text(encoding="utf-8"))
                )
        if not acquired_reports:
            table_reports.append(
                {
                    "table": table_name,
                    "status": "pending",
                    "products": [product["product_name"] for product in products],
                }
            )
            continue
        schema_rows = acquired_reports[0]["field_dispositions"]
        schema_names = [str(row["column_name"]) for row in schema_rows]
        if any(
            [str(row["column_name"]) for row in report["field_dispositions"]] != schema_names
            for report in acquired_reports[1:]
        ):
            raise ValueError(f"upstream schema disagreement across products for {table_name}")
        selected: set[str] = set()
        for product in products:
            selected.update(
                value.split(".")[-1]
                for value in resolve_product_fields(product, schema_rows)["select"]
            )
        omitted = sorted(set(schema_names) - selected)
        omission_reason = sorted(
            {
                str(product.get("unselected_field_reason") or "")
                for product in products
                if product.get("unselected_field_reason")
            }
        )
        table_reports.append(
            {
                "table": table_name,
                "status": "pass" if not omitted else "deliberate_omission",
                "upstream_field_count": len(schema_names),
                "selected_field_count": len(selected),
                "omitted_fields": omitted,
                "omission_reasons": omission_reason if omitted else [],
                "products": [product["product_name"] for product in products],
                "completed_products": [
                    product["product_name"]
                    for product in products
                    if product["product_name"] in by_name
                ],
            }
        )
    return {
        "schema_version": "spacegate.e3_source_coverage.v1",
        "program_version": program["program_version"],
        "status": "pass" if not pending else "in_progress",
        "summary": {
            "expected_products": len(expected),
            "completed_products": len(completed),
            "pending_products": len(pending),
            "completed_rows": sum(int(by_name[name]["row_count"]) for name in completed),
            "completed_bytes": sum(int(by_name[name]["bytes_written"]) for name in completed),
        },
        "completed": completed,
        "pending": pending,
        "table_field_coverage": table_reports,
        "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--program", type=Path, default=DEFAULT_PROGRAM)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--source-id", action="append", default=[])
    parser.add_argument("--product", action="append", default=[])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--retries", type=int, default=6)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    program = json.loads(args.program.read_text(encoding="utf-8"))
    if program.get("schema_version") != "spacegate.e3_acquisition_program.v1":
        raise ValueError("unsupported E3 acquisition program")
    selected_sources = set(args.source_id)
    selected_products = set(args.product)
    products = [
        item
        for item in program["products"]
        if (not selected_sources or item["source_id"] in selected_sources)
        and (not selected_products or item["product_name"] in selected_products)
    ]
    if not products:
        raise ValueError("no acquisition products selected")

    manifest_rows = []
    for product in products:
        manifest_rows.append(
            acquire_product(
                program,
                product,
                state_dir=args.state_dir,
                workers=args.workers,
                timeout_s=args.timeout,
                retries=args.retries,
                refresh=args.refresh,
            )
        )
    manifest_path = args.state_dir / "reports" / "manifests" / program["manifest_name"]
    merged = merge_manifest_rows(manifest_path, manifest_rows)
    coverage = build_coverage_report(program, merged)
    report = {
        "schema_version": CONTRACT,
        "program_version": program["program_version"],
        "status": coverage["status"],
        "products": merged,
        "summary": {
            "product_count": len(merged),
            "row_count": sum(int(row["row_count"]) for row in merged),
            "bytes": sum(int(row["bytes_written"]) for row in merged),
            "pending_product_count": coverage["summary"]["pending_products"],
        },
        "checked_at": utc_now(),
    }
    report_path = (
        args.state_dir / "reports" / "evidence_lake_v2" / "e3_acquisition_report.json"
    )
    write_json(report_path, report)
    write_json(
        args.state_dir
        / "reports"
        / "evidence_lake_v2"
        / "e3_source_coverage_report.json",
        coverage,
    )
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
