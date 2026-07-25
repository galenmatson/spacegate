#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import hashlib
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
BUNDLE_MATERIALIZER_VERSION = "public_read_hierarchy_materializer_v1"
_PAYLOAD_BUILDER = None


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def gzip_bytes(value: bytes) -> bytes:
    return gzip.compress(value, compresslevel=6, mtime=0)


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def load_manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("public-read manifest is not an object")
    return value


def load_payload_builder(build_dir: Path, *, workers: int):
    api_root = ROOT / "srv" / "api"
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))
    os.environ["SPACEGATE_DB_PATH"] = str(build_dir / "core.duckdb")
    os.environ.setdefault("SPACEGATE_STATE_DIR", str(build_dir.parent.parent))
    os.environ["SPACEGATE_STRICT_SIDE_DB_ATTACH"] = "1"
    os.environ["SPACEGATE_API_DB_POOL_SIZE"] = str(workers)
    os.environ["SPACEGATE_API_DUCKDB_THREADS"] = "1"
    os.environ["SPACEGATE_API_DB_ACQUIRE_TIMEOUT_SECONDS"] = "60"
    from app.main import _object_public_system_payload  # noqa: PLC0415

    return _object_public_system_payload


def initialize_bundle_worker(build_dir_text: str) -> None:
    global _PAYLOAD_BUILDER
    _PAYLOAD_BUILDER = load_payload_builder(Path(build_dir_text), workers=1)


def build_bundle_worker(row: dict[str, Any]) -> dict[str, Any]:
    if _PAYLOAD_BUILDER is None:
        raise RuntimeError("bundle worker was not initialized")
    system_id = int(row["system_id"])
    try:
        payload = _PAYLOAD_BUILDER(system_id, name_style="public_full")
        payload["read_backend"] = "public_read_v2_bundle"
        encoded = canonical_json(payload)
        return {
            "ok": True,
            "system_id": system_id,
            "stable_object_key": row["stable_object_key"],
            "payload_gzip": gzip_bytes(encoded),
            "payload_sha256": hashlib.sha256(encoded).hexdigest(),
            "uncompressed_bytes": len(encoded),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "system_id": system_id, "error": str(exc)}


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    build_dir = Path(args.build_dir).resolve(strict=True)
    build_id = build_dir.name
    public_read_dir = (
        Path(args.public_read_dir).resolve()
        if args.public_read_dir
        else Path(args.state_dir).resolve() / "derived" / "public_read" / build_id
    )
    database = public_read_dir / "public_read.sqlite"
    manifest_path = public_read_dir / "manifest.json"
    if not database.is_file() or not manifest_path.is_file():
        raise SystemExit(f"Missing public-read artifact: {public_read_dir}")
    manifest = load_manifest(manifest_path)
    if manifest.get("build_id") != build_id:
        raise SystemExit("Public-read and scientific build identities differ")
    if manifest.get("sample_limit") is not None:
        raise SystemExit("Cannot materialize deployment bundles into a sample artifact")

    manifest["status"] = "warming"
    manifest["warming"] = {
        "materializer_version": BUNDLE_MATERIALIZER_VERSION,
        "started_at_utc": utc_now(),
    }
    atomic_json(manifest_path, manifest)

    con = sqlite3.connect(str(database), timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-131072")
    required = int(
        con.execute(
            "SELECT COUNT(*) FROM systems WHERE hierarchy_representation='bundle_required'"
        ).fetchone()[0]
    )
    existing = int(con.execute("SELECT COUNT(*) FROM hierarchy_bundles").fetchone()[0])
    sql = """
      SELECT s.system_id,s.stable_object_key,s.system_name
      FROM systems s
      LEFT JOIN hierarchy_bundles b USING(system_id)
      WHERE s.hierarchy_representation='bundle_required'
        AND b.system_id IS NULL
      ORDER BY s.system_id
    """
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    selected = [dict(row) for row in con.execute(sql)]
    workers = max(1, int(args.workers))

    if workers == 1:
        initialize_bundle_worker(str(build_dir))
        results: Iterable[dict[str, Any]] = map(build_bundle_worker, selected)
        executor = None
    else:
        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=workers,
            initializer=initialize_bundle_worker,
            initargs=(str(build_dir),),
        )
        results = executor.map(build_bundle_worker, selected)

    generated = 0
    failed: list[dict[str, Any]] = []
    total_compressed_bytes = 0
    total_uncompressed_bytes = 0
    interval = max(10, min(500, max(1, len(selected) // 50)))
    try:
        for index, result in enumerate(results, start=1):
            if not result["ok"]:
                if len(failed) < 50:
                    failed.append(result)
                continue
            con.execute(
                """
                INSERT OR REPLACE INTO hierarchy_bundles VALUES (?,?,?,?,?,?,?,?)
                """,
                [
                    result["system_id"],
                    result["stable_object_key"],
                    "public_system_detail",
                    manifest["projection_schema_version"],
                    result["payload_gzip"],
                    result["payload_sha256"],
                    result["uncompressed_bytes"],
                    BUNDLE_MATERIALIZER_VERSION,
                ],
            )
            generated += 1
            total_compressed_bytes += len(result["payload_gzip"])
            total_uncompressed_bytes += int(result["uncompressed_bytes"])
            if index % 100 == 0:
                con.commit()
            if index == len(selected) or index % interval == 0:
                print(
                    json.dumps(
                        {
                            "stage": "hierarchy_bundles",
                            "processed": index,
                            "selected": len(selected),
                            "generated": generated,
                            "failed": len(failed),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)
    con.commit()
    final_count = int(con.execute("SELECT COUNT(*) FROM hierarchy_bundles").fetchone()[0])
    complete = final_count == required and not failed and not args.limit
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    if complete:
        integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
    else:
        integrity = "deferred_until_complete"
    if complete and integrity == "ok":
        con.execute("VACUUM")
    logical_digest = hashlib.sha256()
    for row in con.execute(
        """
        SELECT system_id,payload_sha256,uncompressed_bytes
        FROM hierarchy_bundles
        ORDER BY system_id
        """
    ):
        logical_digest.update(canonical_json(tuple(row)))
        logical_digest.update(b"\n")
    con.close()

    report = {
        "schema_version": "spacegate.public_read_hierarchy_materialization.v1",
        "status": "pass" if complete and integrity == "ok" else "incomplete",
        "build_id": build_id,
        "materializer_version": BUNDLE_MATERIALIZER_VERSION,
        "required": required,
        "existing_before": existing,
        "selected": len(selected),
        "generated": generated,
        "final_count": final_count,
        "failed_count": len(failed),
        "failed_examples": failed,
        "compressed_bytes_generated": total_compressed_bytes,
        "uncompressed_bytes_generated": total_uncompressed_bytes,
        "compression_ratio": (
            round(total_uncompressed_bytes / total_compressed_bytes, 4)
            if total_compressed_bytes
            else None
        ),
        "workers": workers,
        "sqlite_integrity": integrity,
        "logical_sha256": logical_digest.hexdigest(),
        "wall_seconds": round(time.perf_counter() - started, 6),
        "generated_at_utc": utc_now(),
    }
    report_dir = (
        Path(args.report_dir).resolve()
        if args.report_dir
        else Path(args.state_dir).resolve() / "reports" / "public_read" / build_id
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    atomic_json(report_dir / "hierarchy_bundle_materialization.json", report)

    manifest = load_manifest(manifest_path)
    manifest["status"] = "pass" if report["status"] == "pass" else "warming"
    manifest["hierarchy_bundles"] = {
        "materializer_version": BUNDLE_MATERIALIZER_VERSION,
        "required": required,
        "materialized": final_count,
        "report": str(
            (report_dir / "hierarchy_bundle_materialization.json").relative_to(
                Path(args.state_dir).resolve()
            )
        ),
    }
    manifest["counts"]["hierarchy_bundles"] = final_count
    manifest["artifact"]["bytes"] = database.stat().st_size
    if report["status"] == "pass":
        manifest["artifact"]["sha256"] = sha256_file(database)
        manifest["artifact"]["hash_status"] = "verified"
    else:
        manifest["artifact"]["sha256"] = None
        manifest["artifact"]["hash_status"] = "pending_finalization"
    manifest.pop("warming", None)
    atomic_json(manifest_path, manifest)
    print(json.dumps(report, indent=2, sort_keys=True))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resumably materialize nontrivial public system/hierarchy bundles."
    )
    parser.add_argument("--build-dir", required=True)
    parser.add_argument(
        "--state-dir",
        default=os.getenv("SPACEGATE_STATE_DIR", "/data/spacegate/state"),
    )
    parser.add_argument("--public-read-dir")
    parser.add_argument("--report-dir")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.workers < 1 or args.workers > 16:
        raise SystemExit("--workers must be between 1 and 16")
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be positive")
    report = run(args)
    return 0 if report["status"] == "pass" or args.limit else 1


if __name__ == "__main__":
    raise SystemExit(main())
