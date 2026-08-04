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
REUSE_LOGICAL_HASH_KEYS = ("systems", "stars", "stellar_badge_overlays")
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


def validate_reuse_manifests(
    *,
    current: dict[str, Any],
    source: dict[str, Any],
    expected_source_build_id: str,
    allow_arm_metadata_rewrite: bool = False,
) -> None:
    if source.get("build_id") != expected_source_build_id:
        raise RuntimeError("reusable bundle artifact does not match side-build lineage")
    if source.get("status") != "pass":
        raise RuntimeError("reusable bundle artifact is not complete")
    if (source.get("artifact") or {}).get("hash_status") != "verified":
        raise RuntimeError("reusable bundle artifact hash is not verified")
    if source.get("projection_schema_version") != current.get("projection_schema_version"):
        raise RuntimeError("reusable bundle projection schema differs")
    for key in REUSE_LOGICAL_HASH_KEYS:
        if (source.get("logical_hashes") or {}).get(key) != (
            current.get("logical_hashes") or {}
        ).get(key):
            raise RuntimeError(f"reusable bundle logical hash differs: {key}")
    if not allow_arm_metadata_rewrite and (
        ((source.get("source_artifacts") or {}).get("arm") or {}).get("sha256")
        != (
        (current.get("source_artifacts") or {}).get("arm") or {}
        ).get("sha256")
    ):
        raise RuntimeError("reusable bundle ARM artifact differs")


def reuse_verified_bundles(
    con: sqlite3.Connection,
    *,
    current_manifest: dict[str, Any],
    source_dir: Path,
    expected_source_build_id: str,
    allow_arm_metadata_rewrite: bool = False,
) -> int:
    source_manifest = load_manifest(source_dir / "manifest.json")
    validate_reuse_manifests(
        current=current_manifest,
        source=source_manifest,
        expected_source_build_id=expected_source_build_id,
        allow_arm_metadata_rewrite=allow_arm_metadata_rewrite,
    )
    source_db = source_dir / "public_read.sqlite"
    if not source_db.is_file():
        raise RuntimeError(f"reusable public-read database is missing: {source_db}")
    before = int(con.execute("SELECT COUNT(*) FROM hierarchy_bundles").fetchone()[0])
    con.execute("ATTACH DATABASE ? AS reusable", [str(source_db)])
    try:
        mismatches = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM systems s
                LEFT JOIN reusable.hierarchy_bundles b USING(system_id)
                WHERE s.hierarchy_representation='bundle_required'
                  AND (
                    b.system_id IS NULL
                    OR b.stable_object_key <> s.stable_object_key
                    OR b.bundle_version <> ?
                    OR b.payload_gzip IS NULL
                    OR b.payload_sha256 IS NULL
                  )
                """,
                [current_manifest["projection_schema_version"]],
            ).fetchone()[0]
        )
        if mismatches:
            raise RuntimeError(
                f"reusable hierarchy bundle coverage differs for {mismatches} systems"
            )
        con.execute(
            """
            INSERT OR IGNORE INTO hierarchy_bundles
            SELECT b.*
            FROM reusable.hierarchy_bundles b
            JOIN systems s USING(system_id)
            WHERE s.hierarchy_representation='bundle_required'
              AND b.stable_object_key=s.stable_object_key
              AND b.bundle_version=?
            """,
            [current_manifest["projection_schema_version"]],
        )
        con.commit()
    finally:
        con.execute("DETACH DATABASE reusable")
    after = int(con.execute("SELECT COUNT(*) FROM hierarchy_bundles").fetchone()[0])
    return after - before


def compact_singleton_seed_storage(
    con: sqlite3.Connection,
    *,
    policy: dict[str, Any],
) -> dict[str, Any]:
    object_type = con.execute(
        "SELECT type FROM sqlite_schema WHERE name='singleton_scene_seeds'"
    ).fetchone()
    if object_type and object_type[0] == "table":
        before = int(
            con.execute("SELECT COUNT(*) FROM singleton_scene_seeds").fetchone()[0]
        )
        con.execute("DROP TABLE singleton_scene_seeds")
        converted = True
    else:
        before = None
        converted = False
        if object_type and object_type[0] == "view":
            con.execute("DROP VIEW singleton_scene_seeds")
    con.executemany(
        "INSERT OR REPLACE INTO metadata(key,value) VALUES (?,?)",
        [
            ("singleton_scene_seed_version", policy["singleton_scene_seed_version"]),
            ("render_policy_version", policy["render_policy_version"]),
            (
                "habitable_zone_policy_version",
                policy["habitable_zone_policy_version"],
            ),
        ],
    )
    con.execute(
        """
        CREATE VIEW singleton_scene_seeds AS
        SELECT
          s.system_id, s.stable_object_key, s.system_name,
          st.star_id, st.stable_object_key AS star_stable_object_key,
          st.star_name, st.selected_classification,
          st.classification_status, st.classification_fact_id,
          st.teff_k, st.teff_k_fact_id, st.radius_rsun,
          st.radius_rsun_fact_id, st.mass_msun, st.mass_msun_fact_id,
          st.luminosity_lsun, st.luminosity_lsun_fact_id,
          st.luminosity_status, st.luminosity_basis,
          (SELECT value FROM metadata WHERE key='singleton_scene_seed_version')
            AS seed_version,
          (SELECT value FROM metadata WHERE key='render_policy_version')
            AS render_policy_version,
          (SELECT value FROM metadata WHERE key='habitable_zone_policy_version')
            AS habitable_zone_policy_version
        FROM systems s
        JOIN stars st USING (system_id)
        WHERE s.star_count = 1
          AND s.planet_count = 0
          AND s.scene_representation IN ('singleton_seed', 'compact_seed')
        """
    )
    con.execute("DROP INDEX IF EXISTS systems_coolness_idx")
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS systems_coolness_sort_idx
        ON systems(
          coalesce(coolness_rank,9223372036854775807),
          system_name_norm,
          system_id
        )
        """
    )
    after = int(con.execute("SELECT COUNT(*) FROM singleton_scene_seeds").fetchone()[0])
    if before is not None and before != after:
        raise RuntimeError(
            f"singleton seed compaction changed coverage: {before} != {after}"
        )
    return {
        "converted_from_table": converted,
        "rows": after,
        "storage": "indexed_system_star_view",
        "coolness_sort_index": "systems_coolness_sort_idx",
    }


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
    existing_before = int(
        con.execute("SELECT COUNT(*) FROM hierarchy_bundles").fetchone()[0]
    )
    reused = 0
    reuse_source_build_id = None
    if args.reuse_bundles_from:
        side_report_path = (
            Path(args.state_dir).resolve()
            / "reports"
            / build_id
            / "side_artifact_rebuild_report.json"
        )
        if not side_report_path.is_file():
            raise RuntimeError("bundle reuse requires side-build lineage report")
        side_report = load_manifest(side_report_path)
        reuse_source_build_id = str(side_report.get("source_build_id") or "")
        if not reuse_source_build_id:
            raise RuntimeError("side-build lineage report has no source build")
        copied = side_report.get("copied_artifacts") or {}
        source_build_dir = (
            Path(args.state_dir).resolve() / "out" / reuse_source_build_id
        )
        copied_arm_source = Path(
            str((copied.get("arm") or {}).get("source") or "")
        ).resolve()
        copied_hierarchy_source = Path(
            str((copied.get("canonical_hierarchy") or {}).get("source") or "")
        ).resolve()
        if copied_arm_source != (source_build_dir / "arm.duckdb").resolve():
            raise RuntimeError("side-build report does not prove preserved ARM lineage")
        if copied_hierarchy_source != (
            source_build_dir / "canonical_hierarchy.duckdb"
        ).resolve():
            raise RuntimeError(
                "side-build report does not prove preserved hierarchy lineage"
            )
        reused = reuse_verified_bundles(
            con,
            current_manifest=manifest,
            source_dir=Path(args.reuse_bundles_from).resolve(strict=True),
            expected_source_build_id=reuse_source_build_id,
            allow_arm_metadata_rewrite=True,
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
    singleton_storage = None
    if complete:
        policy_ref = str((manifest.get("policy") or {}).get("path") or "")
        policy_path = (ROOT / policy_ref).resolve()
        if not policy_ref or ROOT not in policy_path.parents:
            raise RuntimeError("public-read manifest policy path is invalid")
        policy = load_manifest(policy_path)
        singleton_storage = compact_singleton_seed_storage(con, policy=policy)
        con.commit()
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
        "existing_before": existing_before,
        "reused": reused,
        "reuse_source_build_id": reuse_source_build_id,
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
        "singleton_seed_storage": singleton_storage,
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
    parser.add_argument(
        "--reuse-bundles-from",
        help=(
            "Reuse verified hierarchy bundles from the exact side-build source "
            "when ARM and public logical hashes are unchanged."
        ),
    )
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
