#!/usr/bin/env python3
"""Acquire pinned HTTP release artifacts with resumable exact-byte storage."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROGRAM = ROOT / "config" / "evidence_lake" / "e3_http_sources.json"
DEFAULT_STATE = Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state"))
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
CONTRACT = "spacegate.http_acquisition_snapshot.v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def hash_file(path: Path, algorithm: str = "sha256") -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def merge_manifest_rows(path: Path, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def product_id(product: dict[str, Any]) -> str:
    return stable_hash(
        {
            "contract": CONTRACT,
            "source_id": product["source_id"],
            "release_id": product["release_id"],
            "source_name": product["source_name"],
            "url": product["url"],
            "filename": product["filename"],
            # Validation expectations may be added after the first exact-byte
            # capture without changing the release artifact's identity.
            "expected_bytes": None,
            "expected_checksum": None,
        }
    )[:24]


def verify_expected(path: Path, product: dict[str, Any]) -> dict[str, Any]:
    size = path.stat().st_size
    expected_size = product.get("expected_bytes")
    if expected_size is not None and size != int(expected_size):
        raise ValueError(f"size mismatch for {path.name}: {size} != {expected_size}")
    sha256 = hash_file(path)
    expected_checksum = str(product.get("expected_checksum") or "")
    checksum_status = "not_declared"
    if expected_checksum:
        algorithm, expected = expected_checksum.split(":", 1)
        actual = hash_file(path, algorithm)
        if actual.lower() != expected.lower():
            raise ValueError(
                f"{algorithm} mismatch for {path.name}: {actual} != {expected}"
            )
        checksum_status = "match"
    return {"bytes": size, "sha256": sha256, "expected_checksum_status": checksum_status}


def download_resumable(
    product: dict[str, Any],
    partial: Path,
    *,
    timeout_s: int,
    read_stall_timeout_s: int,
    retries: int,
) -> dict[str, Any]:
    partial.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            offset = partial.stat().st_size if partial.exists() else 0
            headers = {"User-Agent": USER_AGENT}
            if offset:
                headers["Range"] = f"bytes={offset}-"
            request = urllib.request.Request(product["url"], headers=headers)
            with urllib.request.urlopen(
                request, timeout=min(timeout_s, read_stall_timeout_s)
            ) as response:
                status = int(response.status)
                if offset and status != 206:
                    offset = 0
                    partial.unlink(missing_ok=True)
                mode = "ab" if offset and status == 206 else "wb"
                with partial.open(mode) as handle:
                    while True:
                        chunk = response.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
                        print(
                            f"{utc_now()} {product['source_name']} "
                            f"bytes={handle.tell():,}",
                            flush=True,
                        )
                response_headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower()
                    in {"content-length", "content-range", "content-type", "etag", "last-modified"}
                }
            metadata = verify_expected(partial, product)
            return {
                **metadata,
                "response_headers": response_headers,
                "resumed_from_bytes": offset,
                "attempt": attempt,
            }
        except Exception as exc:  # network behavior is exercised operationally
            last_error = exc
            if attempt == retries:
                break
            delay = min(2**attempt, 30)
            print(
                f"{utc_now()} {product['source_name']} retry {attempt}/{retries - 1}: "
                f"{type(exc).__name__}: {exc}; sleeping {delay}s",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"HTTP acquisition failed after {retries} attempts: {last_error}")


def acquire_product(
    product: dict[str, Any],
    *,
    state_dir: Path,
    timeout_s: int,
    read_stall_timeout_s: int = 180,
    retries: int,
) -> dict[str, Any]:
    snapshot_id = product_id(product)
    root = (
        state_dir
        / "raw"
        / "evidence_lake_v2_http"
        / product["source_id"].replace(".", "_")
        / product["release_id"].replace("/", "_")
        / "snapshots"
        / snapshot_id
        / product["source_name"]
    )
    manifest_path = root / "product_manifest.json"
    payload_path = root / product["filename"]
    if manifest_path.exists() and payload_path.exists():
        report = json.loads(manifest_path.read_text(encoding="utf-8"))
        actual = verify_expected(payload_path, product)
        if report.get("sha256") != actual["sha256"]:
            raise ValueError(f"immutable HTTP snapshot changed: {root}")
        return report["legacy_manifest_entry"]

    partial = (
        state_dir / "tmp" / "evidence_lake_v2_http" / f"{snapshot_id}.{product['filename']}.partial"
    )
    downloaded = download_resumable(
        product,
        partial,
        timeout_s=timeout_s,
        read_stall_timeout_s=read_stall_timeout_s,
        retries=retries,
    )
    root.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{snapshot_id}.", dir=root.parent))
    try:
        target = temporary / product["filename"]
        os.replace(partial, target)
        retrieved_at = utc_now()
        legacy = {
            "source_name": product["source_name"],
            "source_version": product["release_id"],
            "url": product["url"],
            "dest_path": str(root),
            "retrieved_at": retrieved_at,
            "checked_at": retrieved_at,
            "sha256": downloaded["sha256"],
            "bytes_written": downloaded["bytes"],
            "row_count": product.get("expected_rows"),
            "snapshot_id": snapshot_id,
        }
        report = {
            "schema_version": CONTRACT,
            "snapshot_id": snapshot_id,
            "source_id": product["source_id"],
            "release_id": product["release_id"],
            "source_name": product["source_name"],
            "url": product["url"],
            "filename": product["filename"],
            "read_stall_timeout_s": read_stall_timeout_s,
            "retrieved_at": retrieved_at,
            **downloaded,
            "legacy_manifest_entry": legacy,
        }
        write_json(temporary / "product_manifest.json", report)
        os.replace(temporary, root)
        return legacy
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--program", type=Path, default=DEFAULT_PROGRAM)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--source-id", action="append", default=[])
    parser.add_argument("--source-name", action="append", default=[])
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--read-stall-timeout", type=int, default=180)
    parser.add_argument("--retries", type=int, default=5)
    args = parser.parse_args()
    program = json.loads(args.program.read_text(encoding="utf-8"))
    if program.get("schema_version") != "spacegate.e3_http_sources.v1":
        raise ValueError("unsupported E3 HTTP source program")
    source_ids = set(args.source_id)
    source_names = set(args.source_name)
    products = [
        product
        for product in program["products"]
        if (not source_ids or product["source_id"] in source_ids)
        and (not source_names or product["source_name"] in source_names)
    ]
    if not products:
        raise ValueError("no HTTP acquisition products selected")
    rows = [
        acquire_product(
            product,
            state_dir=args.state_dir,
            timeout_s=args.timeout,
            read_stall_timeout_s=args.read_stall_timeout,
            retries=args.retries,
        )
        for product in products
    ]
    manifest_path = (
        args.state_dir / "reports" / "manifests" / program["manifest_name"]
    )
    merged = merge_manifest_rows(manifest_path, rows)
    expected = {product["source_name"] for product in program["products"]}
    complete = {row["source_name"] for row in merged} & expected
    report = {
        "schema_version": CONTRACT,
        "program_version": program["program_version"],
        "status": "pass" if complete == expected else "in_progress",
        "summary": {
            "expected_products": len(expected),
            "completed_products": len(complete),
            "pending_products": len(expected - complete),
            "bytes": sum(int(row["bytes_written"]) for row in merged if row["source_name"] in expected),
        },
        "pending": sorted(expected - complete),
        "products": merged,
        "checked_at": utc_now(),
    }
    write_json(
        args.state_dir / "reports" / "evidence_lake_v2" / "e3_http_acquisition_report.json",
        report,
    )
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
