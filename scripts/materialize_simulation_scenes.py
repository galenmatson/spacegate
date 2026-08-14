#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import hashlib
import json
import os
import resource
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app.simulation_scene_contract import (  # noqa: E402
    SIMULATION_SCENE_ARTIFACT_VERSION as MATERIALIZER_VERSION,
)

_SCENE_BUILDER = None
_SCENE_WORKER_CONTEXT: dict[str, Any] = {}


def _performance_token() -> tuple[
    float,
    float,
    resource.struct_rusage,
    resource.struct_rusage,
]:
    return (
        time.perf_counter(),
        time.process_time(),
        resource.getrusage(resource.RUSAGE_SELF),
        resource.getrusage(resource.RUSAGE_CHILDREN),
    )


def _performance_delta(
    name: str,
    token: tuple[
        float,
        float,
        resource.struct_rusage,
        resource.struct_rusage,
    ],
    *,
    output_bytes: int | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wall_started, cpu_started, usage_started, children_started = token
    usage = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    result: dict[str, Any] = {
        "name": name,
        "wall_seconds": round(time.perf_counter() - wall_started, 6),
        "cpu_seconds": round(time.process_time() - cpu_started, 6),
        "peak_rss_kib": int(usage.ru_maxrss),
        "child_cpu_seconds": round(
            (children.ru_utime - children_started.ru_utime)
            + (children.ru_stime - children_started.ru_stime),
            6,
        ),
        "max_child_peak_rss_kib": int(children.ru_maxrss),
        "input_blocks": int(usage.ru_inblock - usage_started.ru_inblock),
        "output_blocks": int(usage.ru_oublock - usage_started.ru_oublock),
    }
    if output_bytes is not None:
        result["output_bytes"] = int(output_bytes)
    if details:
        result["details"] = details
    return result


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _state_dir(root: Path) -> Path:
    return Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or root / "data")


def _state_dir_for_explicit_build(root: Path, build_dir: Path) -> Path:
    configured = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if configured:
        return Path(configured)
    if build_dir.parent.name == "out":
        return build_dir.parent.parent
    return _state_dir(root)


def _resolve_symlink(path: Path) -> Path:
    try:
        return path.resolve(strict=True)
    except FileNotFoundError:
        return path


def _resolve_build_dir(state_dir: Path, build_id: str | None) -> tuple[str, Path]:
    out_dir = state_dir / "out"
    if build_id:
        build_dir = out_dir / build_id
        if not build_dir.is_dir():
            raise SystemExit(f"Build directory not found: {build_dir}")
        return build_id, build_dir

    served_link = state_dir / "served" / "current"
    if served_link.exists():
        build_dir = _resolve_symlink(served_link)
        return build_dir.name, build_dir

    if not out_dir.is_dir():
        raise SystemExit(f"Missing build output directory: {out_dir}")
    candidates = [path for path in out_dir.iterdir() if path.is_dir() and not path.name.endswith(".tmp")]
    if not candidates:
        raise SystemExit(f"No build directories found in: {out_dir}")
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0].name, candidates[0]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _system_ids_from_manifest(path: Path, *, build_id: str) -> tuple[list[int], dict[str, Any]]:
    if not path.is_file():
        raise SystemExit(f"Missing scene selection manifest: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"Invalid scene selection manifest: {path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "spacegate.simulation_scene_selection.v1":
        raise SystemExit("Unsupported scene selection manifest schema")
    if str(payload.get("target_build_id") or "") != build_id:
        raise SystemExit("Scene selection manifest and target scientific build differ")
    raw_ids = payload.get("system_ids")
    if not isinstance(raw_ids, list):
        raise SystemExit("Scene selection manifest has no system_ids array")
    try:
        system_ids = [int(value) for value in raw_ids]
    except (TypeError, ValueError) as exc:
        raise SystemExit("Scene selection manifest contains a non-integer system ID") from exc
    if any(value <= 0 for value in system_ids) or len(system_ids) != len(set(system_ids)):
        raise SystemExit("Scene selection manifest system IDs must be positive and unique")
    if int(payload.get("system_count") or -1) != len(system_ids):
        raise SystemExit("Scene selection manifest system_count is inconsistent")
    return sorted(system_ids), {
        "path": str(path),
        "sha256": _file_sha256(path),
        "schema_version": payload["schema_version"],
        "selection_policy_version": payload.get("selection_policy_version"),
        "source_build_id": payload.get("source_build_id"),
        "system_count": len(system_ids),
    }


def _rows_to_dicts(cur: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _duckdb_has_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _select_system_rows(
    build_dir: Path,
    *,
    system_ids: Sequence[int],
    limit: int,
    sort: str,
    priority_profile: str,
    top_coolness_limit: int,
    min_dist_ly: float | None,
    max_dist_ly: float | None,
    min_star_count: int | None,
    min_planet_count: int | None,
) -> list[dict[str, Any]]:
    core_path = build_dir / "core.duckdb"
    disc_path = build_dir / "disc.duckdb"
    if not core_path.exists():
        raise SystemExit(f"Missing core.duckdb in build: {core_path}")

    con = duckdb.connect(str(core_path), read_only=True)
    attached_disc = False
    try:
        if system_ids:
            placeholders = ",".join(["?"] * len(system_ids))
            return _rows_to_dicts(
                con.execute(
                    f"""
                    SELECT system_id, stable_object_key, system_name, dist_ly
                    FROM systems
                    WHERE system_id IN ({placeholders})
                    ORDER BY system_id ASC
                    """,
                    list(system_ids),
                )
            )

        has_coolness = False
        if disc_path.exists():
            escaped = str(disc_path).replace("'", "''")
            con.execute(f"ATTACH '{escaped}' AS disc_db (READ_ONLY)")
            attached_disc = True
            has_coolness = _duckdb_has_table(con, "coolness_scores")

        conditions: list[str] = []
        params: list[Any] = []
        if min_dist_ly is not None:
            conditions.append("s.dist_ly >= ?")
            params.append(float(min_dist_ly))
        if max_dist_ly is not None:
            conditions.append("s.dist_ly <= ?")
            params.append(float(max_dist_ly))
        if min_star_count is not None:
            conditions.append("COALESCE(s.star_count, 0) >= ?")
            params.append(int(min_star_count))
        if min_planet_count is not None:
            conditions.append("COALESCE(s.planet_count, 0) >= ?")
            params.append(int(min_planet_count))
        if priority_profile == "search-preview":
            priority_clauses = [
                "COALESCE(s.planet_count, 0) > 0",
                "COALESCE(s.star_count, 0) > 1",
                """
                EXISTS (
                  SELECT 1
                  FROM stars st
                  WHERE st.system_id = s.system_id
                    AND (
                      UPPER(COALESCE(st.spectral_type_raw, '')) LIKE 'D%'
                      OR UPPER(COALESCE(st.spectral_class, '')) IN ('D', 'WR')
                    )
                )
                """,
            ]
            if has_coolness and top_coolness_limit > 0:
                priority_clauses.append("COALESCE(c.rank, 9223372036854775807) <= ?")
                params.append(int(top_coolness_limit))
            conditions.append("(" + " OR ".join(priority_clauses) + ")")
        where_sql = "WHERE " + " AND ".join(conditions) if conditions else ""

        coolness_select = "NULL::DOUBLE AS coolness_score, NULL::BIGINT AS coolness_rank"
        coolness_join = ""
        if has_coolness:
            coolness_select = "c.score_total AS coolness_score, c.rank AS coolness_rank"
            coolness_join = "LEFT JOIN disc_db.coolness_scores c USING (system_id)"

        if sort == "coolness":
            if not has_coolness:
                raise SystemExit("Coolness sort requested, but disc.coolness_scores is unavailable.")
            order_sql = "COALESCE(c.rank, 9223372036854775807) ASC, COALESCE(s.dist_ly, 1e12) ASC, s.system_id ASC"
        elif sort == "name":
            order_sql = "lower(COALESCE(s.system_name, s.stable_object_key, '')) ASC, s.system_id ASC"
        else:
            order_sql = "COALESCE(s.dist_ly, 1e12) ASC, s.system_id ASC"

        sql = f"""
            SELECT
              s.system_id,
              s.stable_object_key,
              s.system_name,
              s.dist_ly,
              COALESCE(s.star_count, 0) AS star_count,
              COALESCE(s.planet_count, 0) AS planet_count,
              {coolness_select}
            FROM systems s
            {coolness_join}
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ?
        """
        params.append(max(1, int(limit)))
        return _rows_to_dicts(con.execute(sql, params))
    finally:
        if attached_disc:
            try:
                con.execute("DETACH disc_db")
            except Exception:
                pass
        con.close()


def _public_read_full_scene_ids(
    state_dir: Path,
    build_id: str,
    public_read_dir: Path | None = None,
) -> list[int]:
    database = (
        public_read_dir / "public_read.sqlite"
        if public_read_dir is not None
        else state_dir
        / "derived"
        / "public_read"
        / build_id
        / "public_read.sqlite"
    )
    if not database.is_file():
        raise SystemExit(f"Missing public-read policy artifact: {database}")
    uri = f"file:{database.resolve()}?mode=ro&immutable=1"
    con = sqlite3.connect(uri, uri=True)
    try:
        return [
            int(row[0])
            for row in con.execute(
                """
                SELECT system_id
                FROM systems
                WHERE scene_representation='full_scene'
                ORDER BY system_id
                """
            )
        ]
    finally:
        con.close()


def _emit_progress(payload: dict[str, Any]) -> None:
    print("[simulation-scene-progress] " + json.dumps(payload, sort_keys=True), flush=True)


def _progress_interval(total: int) -> int:
    return max(10, min(1000, max(1, total // 50)))


def _load_scene_builder(root: Path, build_dir: Path, *, workers: int):
    api_root = root / "srv" / "api"
    sys.path.insert(0, str(api_root))
    os.environ["SPACEGATE_DB_PATH"] = str(build_dir / "core.duckdb")
    os.environ.setdefault("SPACEGATE_STATE_DIR", str(_state_dir(root)))
    os.environ["SPACEGATE_STRICT_SIDE_DB_ATTACH"] = "1"
    os.environ["SPACEGATE_API_DB_POOL_SIZE"] = str(workers)
    os.environ["SPACEGATE_API_DUCKDB_THREADS"] = "1"
    os.environ["SPACEGATE_API_DB_ACQUIRE_TIMEOUT_SECONDS"] = "60"
    from app.main import _system_simulation_scene_payload  # noqa: PLC0415

    return _system_simulation_scene_payload


def _write_scene(path: Path, payload: dict[str, Any]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        with tmp_path.open("wb") as raw:
            with gzip.GzipFile(
                filename="",
                fileobj=raw,
                mode="wb",
                compresslevel=6,
                mtime=0,
            ) as f:
                f.write(encoded)
        tmp_path.chmod(0o664)
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return path.stat().st_size


def _prune_runtime_cache(state_dir: Path) -> dict[str, int]:
    root = state_dir / "cache" / "simulation_scenes"
    try:
        limit_bytes = max(
            64 * 1024 * 1024,
            int(os.getenv("SPACEGATE_SIMULATION_SCENE_CACHE_LIMIT_BYTES", str(2 * 1024 * 1024 * 1024))),
        )
    except ValueError:
        limit_bytes = 2 * 1024 * 1024 * 1024
    files: list[tuple[int, str, int, Path]] = []
    for path in root.glob("*/system_*.json.gz"):
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        files.append((stat.st_mtime_ns, str(path), stat.st_size, path))
    total_bytes = sum(item[2] for item in files)
    removed_files = 0
    removed_bytes = 0
    if total_bytes > limit_bytes:
        files.sort()
        for _mtime_ns, _path_text, size, path in files:
            if total_bytes <= limit_bytes:
                break
            path.unlink(missing_ok=True)
            total_bytes -= size
            removed_files += 1
            removed_bytes += size
    return {
        "limit_bytes": limit_bytes,
        "retained_bytes": total_bytes,
        "removed_files": removed_files,
        "removed_bytes": removed_bytes,
    }


def _scene_artifact_reusable(path: Path, *, build_id: str) -> bool:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return False
    materialization = payload.get("materialization") if isinstance(payload, dict) else None
    return (
        isinstance(materialization, dict)
        and materialization.get("materializer_version") == MATERIALIZER_VERSION
        and materialization.get("build_id") == build_id
        and materialization.get("deterministic") is True
    )


def _initialize_scene_worker(
    root_text: str,
    build_dir_text: str,
    output_dir_text: str,
    state_dir_text: str,
    build_id: str,
    force: bool,
    output_mode: str,
) -> None:
    global _SCENE_BUILDER, _SCENE_WORKER_CONTEXT
    root = Path(root_text)
    build_dir = Path(build_dir_text)
    _SCENE_BUILDER = _load_scene_builder(root, build_dir, workers=1)
    _SCENE_WORKER_CONTEXT = {
        "build_dir": build_dir,
        "output_dir": Path(output_dir_text),
        "state_dir": Path(state_dir_text),
        "build_id": build_id,
        "force": bool(force),
        "output_mode": output_mode,
    }


def _materialize_scene_worker(row: dict[str, Any]) -> dict[str, Any]:
    if _SCENE_BUILDER is None or not _SCENE_WORKER_CONTEXT:
        raise RuntimeError("scene worker was not initialized")
    context = _SCENE_WORKER_CONTEXT
    system_id = int(row["system_id"])
    output_dir = context["output_dir"]
    build_id = str(context["build_id"])
    out_path = output_dir / f"system_{system_id}.json.gz"
    reusable = (
        out_path.exists()
        and not context["force"]
        and _scene_artifact_reusable(out_path, build_id=build_id)
    )
    if reusable:
        return {
            "status": "reused",
            "size_bytes": out_path.stat().st_size,
            "system_id": system_id,
            "system_name": row.get("system_name"),
            "out_path": str(out_path),
        }
    incompatible = bool(out_path.exists() and not context["force"])
    try:
        payload = _SCENE_BUILDER(system_id, build_id=build_id)
        payload["generated_at_utc"] = None
        payload["materialization"] = {
            "materialized": True,
            "materializer_version": MATERIALIZER_VERSION,
            "build_id": build_id,
            "deterministic": True,
        }
        size = _write_scene(out_path, payload)
        return {
            "status": "generated",
            "size_bytes": size,
            "system_id": system_id,
            "system_name": row.get("system_name"),
            "out_path": str(out_path),
            "incompatible": incompatible,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "size_bytes": 0,
            "system_id": system_id,
            "system_name": row.get("system_name"),
            "out_path": str(out_path),
            "incompatible": incompatible,
            "error": str(exc),
        }


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    process_started = _performance_token()
    phases: list[dict[str, Any]] = []
    setup_started = _performance_token()
    root = _root_dir()
    if args.build_dir:
        build_dir = Path(args.build_dir).resolve()
        state_dir = _state_dir_for_explicit_build(root, build_dir)
        build_id = args.build_id or build_dir.name.removesuffix(".tmp")
        if not (build_dir / "core.duckdb").exists():
            raise SystemExit(f"Build directory does not contain core.duckdb: {build_dir}")
    else:
        state_dir = _state_dir(root)
        build_id, build_dir = _resolve_build_dir(state_dir, args.build_id)
    build_dir = build_dir.resolve()
    workers = max(1, int(getattr(args, "workers", 1) or 1))
    phases.append(_performance_delta("setup", setup_started))

    selection_started = _performance_token()
    selected_system_ids = list(args.system_id)
    selection_manifest = None
    selection_manifest_path = str(
        getattr(args, "system_id_manifest", None) or ""
    ).strip()
    public_read_full_scene_policy = bool(
        getattr(args, "public_read_full_scene_policy", False)
    )
    if selection_manifest_path:
        if selected_system_ids or public_read_full_scene_policy:
            raise SystemExit(
                "--system-id-manifest cannot be combined with --system-id or "
                "--public-read-full-scene-policy"
            )
        selected_system_ids, selection_manifest = _system_ids_from_manifest(
            Path(selection_manifest_path).resolve(),
            build_id=build_id,
        )
    if public_read_full_scene_policy:
        if selected_system_ids:
            raise SystemExit(
                "--public-read-full-scene-policy cannot be combined with --system-id"
            )
        selected_system_ids = _public_read_full_scene_ids(
            state_dir,
            build_id,
            (
                Path(args.public_read_dir).resolve()
                if getattr(args, "public_read_dir", None)
                else None
            ),
        )
    system_rows = _select_system_rows(
        build_dir,
        system_ids=selected_system_ids,
        limit=args.limit,
        sort=args.sort,
        priority_profile=args.priority_profile,
        top_coolness_limit=args.top_coolness_limit,
        min_dist_ly=args.min_dist_ly,
        max_dist_ly=args.max_dist_ly,
        min_star_count=args.min_star_count,
        min_planet_count=args.min_planet_count,
    )
    if selected_system_ids:
        requested_ids = {int(system_id) for system_id in selected_system_ids}
        found_ids = {int(row["system_id"]) for row in system_rows}
        missing_ids = sorted(requested_ids - found_ids)
        if missing_ids:
            raise SystemExit(f"Requested system IDs are absent from build {build_id}: {missing_ids}")
    if args.output_mode == "runtime-cache":
        output_dir = state_dir / "cache" / "simulation_scenes" / build_id
        report_path = output_dir / "materialization_report.json"
    else:
        output_dir = build_dir / "disc" / "simulation_scenes"
        report_path = state_dir / "reports" / build_id / "simulation_scene_cache_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    phases.append(
        _performance_delta(
            "system_selection",
            selection_started,
            details={"selected_systems": len(system_rows)},
        )
    )

    requested = len(system_rows)
    generated = 0
    reused = 0
    incompatible_existing = 0
    failed = 0
    total_bytes = 0
    examples: list[dict[str, Any]] = []
    interval = _progress_interval(requested)
    _emit_progress(
        {
            "stage": "start",
            "build_id": build_id,
            "requested": requested,
            "output_dir": str(output_dir),
            "sort": args.sort,
            "force": bool(args.force),
            "output_mode": args.output_mode,
            "workers": workers,
        }
    )

    materialization_started = _performance_token()
    initializer_args = (
        str(root),
        str(build_dir),
        str(output_dir),
        str(state_dir),
        build_id,
        bool(args.force),
        args.output_mode,
    )
    executor: concurrent.futures.Executor
    if workers == 1:
        _initialize_scene_worker(*initializer_args)
        result_rows: Iterable[dict[str, Any]] = map(
            _materialize_scene_worker, system_rows
        )
        executor = None  # type: ignore[assignment]
    else:
        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=workers,
            initializer=_initialize_scene_worker,
            initargs=initializer_args,
        )
        result_rows = executor.map(_materialize_scene_worker, system_rows)
    try:
        for idx, result_row in enumerate(result_rows, start=1):
            status = result_row["status"]
            total_bytes += int(result_row.get("size_bytes") or 0)
            if result_row.get("incompatible"):
                incompatible_existing += 1
            if status == "generated":
                generated += 1
            elif status == "reused":
                reused += 1
            else:
                failed += 1
                if len(examples) < 12:
                    examples.append(
                        {
                            "system_id": result_row["system_id"],
                            "error": result_row.get("error"),
                        }
                    )
            out_path = Path(result_row["out_path"])
            if len(examples) < 8 and out_path.exists():
                examples.append(
                    {
                        "system_id": result_row["system_id"],
                        "system_name": result_row.get("system_name"),
                        "artifact_path": (
                            str(out_path.relative_to(build_dir))
                            if args.output_mode == "build-artifact"
                            else str(out_path.relative_to(state_dir))
                        ),
                        "size_bytes": out_path.stat().st_size,
                    }
                )
            if idx == requested or idx % interval == 0:
                _emit_progress(
                    {
                        "stage": "materializing",
                        "processed": idx,
                        "requested": requested,
                        "generated": generated,
                        "reused": reused,
                        "incompatible_existing": incompatible_existing,
                        "failed": failed,
                    }
                )
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)
    phases.append(
        _performance_delta(
            "scene_materialization",
            materialization_started,
            output_bytes=total_bytes,
            details={
                "requested": requested,
                "generated": generated,
                "reused": reused,
                "incompatible_existing": incompatible_existing,
                "failed": failed,
            },
        )
    )

    cache_prune = None
    if args.output_mode == "runtime-cache":
        prune_started = _performance_token()
        cache_prune = _prune_runtime_cache(state_dir)
        phases.append(
            _performance_delta(
                "runtime_cache_prune",
                prune_started,
                details=cache_prune,
            )
        )
    elapsed_s = time.perf_counter() - started
    process_metrics = _performance_delta("total", process_started)
    report = {
        "ok": failed == 0,
        "build_id": build_id,
        "build_dir": str(build_dir),
        "materializer_version": MATERIALIZER_VERSION,
        "generated_at_utc": _utc_now(),
        "params": {
            "sort": args.sort,
            "priority_profile": args.priority_profile,
            "top_coolness_limit": args.top_coolness_limit,
            "limit": args.limit,
            "system_id": args.system_id,
            "public_read_full_scene_policy": public_read_full_scene_policy,
            "system_id_manifest": selection_manifest,
            "min_dist_ly": args.min_dist_ly,
            "max_dist_ly": args.max_dist_ly,
            "min_star_count": args.min_star_count,
            "min_planet_count": args.min_planet_count,
            "force": bool(args.force),
            "output_mode": args.output_mode,
            "workers": workers,
        },
        "requested": requested,
        "generated": generated,
        "reused": reused,
        "incompatible_existing": incompatible_existing,
        "failed": failed,
        "selected_artifact_size_bytes": total_bytes,
        "output_dir": str(output_dir),
        "report_path": str(report_path),
        "elapsed_s": round(elapsed_s, 3),
        "performance": {
            key: value for key, value in process_metrics.items() if key != "name"
        },
        "phases": phases,
        "runtime_cache_prune": cache_prune,
        "examples": examples,
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    _emit_progress({**{k: report[k] for k in ("build_id", "requested", "generated", "reused", "failed")}, "stage": "complete", "report_path": str(report_path)})
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prebuild compressed System Simulation scene JSON artifacts for a served build.")
    parser.add_argument("--build-id", default=None, help="Build ID to target (defaults to served/current).")
    parser.add_argument("--build-dir", default=None, help="Explicit build directory, including an unpromoted .tmp build.")
    parser.add_argument("--system-id", action="append", type=int, default=[], help="Specific system_id to materialize; can be repeated.")
    parser.add_argument(
        "--system-id-manifest",
        help=(
            "Versioned scene-selection JSON containing the exact build-keyed "
            "system ID set to materialize."
        ),
    )
    parser.add_argument(
        "--public-read-full-scene-policy",
        action="store_true",
        help="Select the exact build-keyed full-scene set from the public-read artifact.",
    )
    parser.add_argument(
        "--public-read-dir",
        help="Explicit staged public-read directory for full-scene policy selection.",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Maximum systems to select when --system-id is not provided.")
    parser.add_argument("--sort", choices=["distance", "coolness", "name"], default="distance")
    parser.add_argument("--priority-profile", choices=["none", "search-preview"], default="none", help="Optional property-based priority selector for prebuilding high-value preview scenes.")
    parser.add_argument(
        "--output-mode",
        choices=["build-artifact", "runtime-cache"],
        default="build-artifact",
        help="Write immutable build artifacts during a build, or regenerable build-keyed runtime cache files after promotion.",
    )
    parser.add_argument("--top-coolness-limit", type=int, default=500, help="When using --priority-profile search-preview, include this many top-ranked coolness systems if available.")
    parser.add_argument("--min-dist-ly", type=float, default=None)
    parser.add_argument("--max-dist-ly", type=float, default=100.0)
    parser.add_argument("--min-star-count", type=int, default=None)
    parser.add_argument("--min-planet-count", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Regenerate existing scene artifacts.")
    parser.add_argument("--workers", type=int, default=1, help="Concurrent scene workers; output remains deterministic and build-keyed.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit <= 0 and not args.system_id and not args.system_id_manifest:
        raise SystemExit(
            "--limit must be > 0 unless --system-id or --system-id-manifest is provided"
        )
    if args.top_coolness_limit < 0:
        raise SystemExit("--top-coolness-limit must be >= 0")
    if args.workers < 1 or args.workers > 32:
        raise SystemExit("--workers must be between 1 and 32")
    if args.min_dist_ly is not None and args.min_dist_ly < 0:
        raise SystemExit("--min-dist-ly must be >= 0")
    if args.max_dist_ly is not None and args.max_dist_ly < 0:
        raise SystemExit("--max-dist-ly must be >= 0")
    if args.min_dist_ly is not None and args.max_dist_ly is not None and args.min_dist_ly > args.max_dist_ly:
        raise SystemExit("--min-dist-ly cannot be greater than --max-dist-ly")
    result = run(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
