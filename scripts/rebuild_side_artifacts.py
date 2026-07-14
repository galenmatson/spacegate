#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_token_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def state_dir(root: Path) -> Path:
    configured = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if configured:
        return Path(configured)
    shared_state = Path("/data/spacegate/state")
    if shared_state.exists():
        return shared_state
    return root / "data"


def git_sha(root: Path) -> str:
    try:
        return subprocess.check_output(["git", "-C", str(root), "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return "nogit"


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def resolve_source_build(state: Path, build_id: str | None) -> tuple[str, Path]:
    out_dir = state / "out"
    if build_id:
        build_dir = out_dir / build_id
        if not build_dir.is_dir():
            raise SystemExit(f"Source build not found: {build_dir}")
        return build_id, build_dir
    served = state / "served" / "current"
    if not served.exists():
        raise SystemExit("No served/current build found; pass --source-build-id explicitly.")
    build_dir = served.resolve(strict=True)
    return build_dir.name, build_dir


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            select 1
            from information_schema.tables
            where table_schema = 'main' and table_name = ?
            limit 1
            """,
            [table_name],
        ).fetchone()
    )


def update_build_metadata(db_path: Path, *, build_id: str, source_build_id: str, artifact_kind: str) -> None:
    if not db_path.exists():
        return
    con = duckdb.connect(str(db_path))
    try:
        if not table_exists(con, "build_metadata"):
            con.execute("create table build_metadata(key varchar, value varchar)")
        columns = {
            row[1]
            for row in con.execute("pragma table_info('build_metadata')").fetchall()
        }
        if {"key", "value"}.issubset(columns):
            con.execute(
                """
                delete from build_metadata
                where key in (
                  'build_id',
                  'side_artifact_rebuild_source_build_id',
                  'side_artifact_rebuild_kind',
                  'side_artifact_rebuild_generated_at'
                )
                """
            )
            con.executemany(
                "insert into build_metadata values (?, ?)",
                [
                    ("build_id", build_id),
                    ("side_artifact_rebuild_source_build_id", source_build_id),
                    ("side_artifact_rebuild_kind", artifact_kind),
                    ("side_artifact_rebuild_generated_at", utc_now()),
                ],
            )
        elif {"build_id", "generated_at", "source_kind", "source_path"}.issubset(columns):
            generated_at = utc_now()
            con.execute("update build_metadata set build_id = ?", [build_id])
            con.execute(
                "delete from build_metadata where source_kind = ?",
                [f"side_artifact_rebuild:{artifact_kind}"],
            )
            con.execute(
                """
                insert into build_metadata(build_id, generated_at, source_kind, source_path)
                values (?, ?, ?, ?)
                """,
                [
                    build_id,
                    generated_at,
                    f"side_artifact_rebuild:{artifact_kind}",
                    source_build_id,
                ],
            )
        else:
            return
        con.execute("checkpoint")
    finally:
        con.close()


def copy_file(src: Path, dst: Path, *, required: bool = True) -> dict[str, object] | None:
    if not src.exists():
        if required:
            raise SystemExit(f"Required artifact missing: {src}")
        return None
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return {"source": str(src), "target": str(dst), "bytes": dst.stat().st_size}


def copy_dir(src: Path, dst: Path, *, required: bool = True) -> dict[str, object] | None:
    if not src.exists():
        if required:
            raise SystemExit(f"Required artifact directory missing: {src}")
        return None
    shutil.copytree(src, dst)
    total = 0
    files = 0
    for path in dst.rglob("*"):
        if path.is_file():
            files += 1
            total += path.stat().st_size
    return {"source": str(src), "target": str(dst), "files": files, "bytes": total}


def main() -> int:
    root = repo_root()
    state = state_dir(root)
    parser = argparse.ArgumentParser(
        description=(
            "Clone an existing served build and rebuild side artifacts that can be "
            "regenerated from the current code without recrawling source catalogs."
        )
    )
    parser.add_argument("--source-build-id", default="", help="Source build id. Defaults to served/current.")
    parser.add_argument("--build-id", default="", help="Output build id.")
    parser.add_argument("--promote", action="store_true", help="Promote the rebuilt build after generation.")
    parser.add_argument(
        "--build-map-tiles",
        action="store_true",
        help="Build deterministic 100/250/500/1000-ly map tiles into the cloned artifact.",
    )
    parser.add_argument(
        "--skip-disc-copy",
        action="store_true",
        help="Do not copy disc.duckdb/disc side artifacts. Intended only for ARM-only smoke builds.",
    )
    args = parser.parse_args()

    source_build_id, source_build_dir = resolve_source_build(state, args.source_build_id or None)
    build_id = args.build_id or f"{build_token_now()}_{git_sha(root)}_side_rebuild"
    out_dir = state / "out"
    final_dir = out_dir / build_id
    tmp_dir = out_dir / f"{build_id}.tmp"
    reports_dir = state / "reports" / build_id
    if final_dir.exists() or tmp_dir.exists():
        raise SystemExit(f"Output build already exists: {final_dir} or {tmp_dir}")

    generated_at = utc_now()
    copied: dict[str, object] = {}
    tmp_dir.mkdir(parents=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    try:
        copied["core"] = copy_file(source_build_dir / "core.duckdb", tmp_dir / "core.duckdb")
        update_build_metadata(tmp_dir / "core.duckdb", build_id=build_id, source_build_id=source_build_id, artifact_kind="core")
        copied["parquet"] = copy_dir(source_build_dir / "parquet", tmp_dir / "parquet")
        copied["snapshots"] = copy_dir(source_build_dir / "snapshots", tmp_dir / "snapshots", required=False)
        copied["canonical_hierarchy"] = copy_file(
            source_build_dir / "canonical_hierarchy.duckdb",
            tmp_dir / "canonical_hierarchy.duckdb",
            required=False,
        )
        if copied["canonical_hierarchy"]:
            update_build_metadata(
                tmp_dir / "canonical_hierarchy.duckdb",
                build_id=build_id,
                source_build_id=source_build_id,
                artifact_kind="canonical_hierarchy",
            )
        if not args.skip_disc_copy:
            copied["disc_db"] = copy_file(source_build_dir / "disc.duckdb", tmp_dir / "disc.duckdb", required=False)
            if copied["disc_db"]:
                update_build_metadata(tmp_dir / "disc.duckdb", build_id=build_id, source_build_id=source_build_id, artifact_kind="disc")
            copied["disc_dir"] = copy_dir(source_build_dir / "disc", tmp_dir / "disc", required=False)
        if args.build_map_tiles:
            if not (tmp_dir / "disc.duckdb").exists():
                raise SystemExit("--build-map-tiles requires disc.duckdb; omit --skip-disc-copy")
            subprocess.check_call(
                [
                    sys.executable,
                    str(root / "scripts" / "build_map_tiles.py"),
                    "--state-dir",
                    str(state),
                    "--build-dir",
                    str(tmp_dir),
                    "--output-dir",
                    str(tmp_dir / "map_tiles"),
                    "--radii",
                    "100,250,500,1000",
                ],
                cwd=str(root),
            )

        arm_report = reports_dir / "arm_report.json"
        transform_version = f"side_artifact_rebuild:{git_sha(root)}"
        subprocess.check_call(
            [
                sys.executable,
                str(root / "scripts" / "build_arm.py"),
                "--core-db",
                str(tmp_dir / "core.duckdb"),
                "--arm-db",
                str(tmp_dir / "arm.duckdb"),
                "--state-dir",
                str(state),
                "--build-id",
                build_id,
                "--ingested-at",
                generated_at,
                "--transform-version",
                transform_version,
                "--report-path",
                str(arm_report),
            ],
            cwd=str(root),
        )
        if not (tmp_dir / "arm.duckdb").exists():
            raise SystemExit(f"ARM rebuild did not create {tmp_dir / 'arm.duckdb'}")

        report = {
            "generated_at": generated_at,
            "build_id": build_id,
            "source_build_id": source_build_id,
            "source_build_dir": str(source_build_dir),
            "output_build_dir": str(final_dir),
            "transform_version": transform_version,
            "copied_artifacts": copied,
            "rebuilt_artifacts": {
                "arm": {
                    "path": str(final_dir / "arm.duckdb"),
                    "report_path": str(arm_report),
                }
            },
            "notes": [
                "core.duckdb is cloned from the source build with build metadata updated.",
                "arm.duckdb is regenerated from the cloned core using current build_arm.py.",
                "disc and snapshot artifacts are copied unless --skip-disc-copy is used.",
                "Versioned map tiles are generated when --build-map-tiles is requested.",
                "This script does not download or cook source catalogs.",
            ],
        }
        (reports_dir / "side_artifact_rebuild_report.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        tmp_dir.rename(final_dir)
    except Exception:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        raise

    print(json.dumps({"status": "ok", "build_id": build_id, "build_dir": str(final_dir)}, indent=2))

    if args.promote:
        subprocess.check_call([str(root / "scripts" / "promote_build.sh"), build_id], cwd=str(root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
