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


def read_key_value_metadata(db_path: Path) -> dict[str, str]:
    if not db_path.exists():
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "build_metadata"):
            return {}
        columns = {
            str(row[1]) for row in con.execute("pragma table_info('build_metadata')").fetchall()
        }
        if not {"key", "value"}.issubset(columns):
            return {}
        return {
            str(key): "" if value is None else str(value)
            for key, value in con.execute("select key, value from build_metadata").fetchall()
        }
    finally:
        con.close()


def resolve_canonical_evidence_arm(
    *,
    state: Path,
    core_db: Path,
    explicit_path: str = "",
) -> Path | None:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.is_file():
            raise SystemExit(f"Explicit canonical evidence ARM not found: {path}")
        return path

    metadata = read_key_value_metadata(core_db)
    if not metadata.get("slice_profile_id", "").strip():
        return None

    canonical_build_id = metadata.get("bootstrap_source_build_id", "").strip()
    if not canonical_build_id:
        raise SystemExit(
            "Sliced CORE is missing bootstrap_source_build_id; refusing to re-adjudicate "
            "TESS identity against a pruned catalog."
        )
    canonical_arm = state / "out" / canonical_build_id / "arm.duckdb"
    if not canonical_arm.is_file():
        raise SystemExit(
            "Full-canonical TESS evidence ARM is unavailable for sliced side rebuild: "
            f"{canonical_arm}"
        )
    return canonical_arm.resolve()


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
        "--build-simulation-scenes",
        action="store_true",
        help="Prebuild high-value simulation scenes into the immutable side artifact.",
    )
    parser.add_argument(
        "--simulation-scene-limit",
        type=int,
        default=1000,
        help="Maximum priority simulation scenes to prebuild (default: 1000).",
    )
    parser.add_argument(
        "--preserve-arm",
        action="store_true",
        help="Copy the source ARM artifact instead of regenerating it; use for presentation-only side builds.",
    )
    parser.add_argument(
        "--skip-disc-copy",
        action="store_true",
        help="Do not copy disc.duckdb/disc side artifacts. Intended only for ARM-only smoke builds.",
    )
    parser.add_argument(
        "--canonical-evidence-arm",
        default="",
        help=(
            "Override the full-canonical ARM used to project adjudicated TESS evidence. "
            "Sliced CORE builds otherwise resolve it from bootstrap_source_build_id."
        ),
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
        transform_version = f"side_artifact_rebuild:{git_sha(root)}"
        arm_report = reports_dir / "arm_report.json"
        if args.preserve_arm:
            copied["arm"] = copy_file(source_build_dir / "arm.duckdb", tmp_dir / "arm.duckdb")
            update_build_metadata(
                tmp_dir / "arm.duckdb",
                build_id=build_id,
                source_build_id=source_build_id,
                artifact_kind="arm",
            )
        else:
            canonical_evidence_arm = resolve_canonical_evidence_arm(
                state=state,
                core_db=tmp_dir / "core.duckdb",
                explicit_path=args.canonical_evidence_arm,
            )
            build_arm_command = [
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
            ]
            if canonical_evidence_arm:
                build_arm_command.extend(
                    ["--canonical-evidence-arm", str(canonical_evidence_arm)]
                )
            subprocess.check_call(build_arm_command, cwd=str(root))
        if not (tmp_dir / "arm.duckdb").exists():
            raise SystemExit(f"ARM rebuild did not create {tmp_dir / 'arm.duckdb'}")

        if (tmp_dir / "canonical_hierarchy.duckdb").exists():
            subprocess.check_call(
                [
                    sys.executable,
                    str(root / "scripts" / "materialize_stellar_leaf_classifications.py"),
                    "--core-db", str(tmp_dir / "core.duckdb"),
                    "--arm-db", str(tmp_dir / "arm.duckdb"),
                    "--hierarchy-db", str(tmp_dir / "canonical_hierarchy.duckdb"),
                    "--build-id", build_id,
                    "--report-path", str(reports_dir / "stellar_leaf_classification_report.json"),
                ],
                cwd=str(root),
            )

        if args.build_map_tiles:
            if not (tmp_dir / "disc.duckdb").exists():
                raise SystemExit("--build-map-tiles requires disc.duckdb; omit --skip-disc-copy")
            subprocess.check_call(
                [sys.executable, str(root / "scripts" / "build_map_tiles.py"),
                 "--state-dir", str(state), "--build-dir", str(tmp_dir),
                 "--output-dir", str(tmp_dir / "map_tiles"),
                 "--radii", "100,250,500,1000"],
                cwd=str(root),
            )
            subprocess.check_call(
                [sys.executable, str(root / "scripts" / "verify_map_tiles.py"),
                 "--state-dir", str(state), "--build-dir", str(tmp_dir),
                 "--radii", "100,250,500,1000",
                 "--report-path", str(reports_dir / "map_tile_verification_report.json")],
                cwd=str(root),
            )
        if args.build_simulation_scenes:
            subprocess.check_call(
                [sys.executable, str(root / "scripts" / "materialize_simulation_scenes.py"),
                 "--build-id", build_id, "--build-dir", str(tmp_dir),
                 "--priority-profile", "search-preview", "--sort", "coolness",
                 "--limit", str(max(1, args.simulation_scene_limit)),
                 "--max-dist-ly", "1000"],
                cwd=str(root),
            )

        subprocess.check_call(
            [
                sys.executable,
                str(root / "scripts" / "derived_build_verification.py"),
                "emit",
                "--build-dir",
                str(tmp_dir),
                "--build-id",
                build_id,
                "--source-build-id",
                source_build_id,
                "--upstream-reports-dir",
                str(state / "reports" / source_build_id),
                "--report",
                str(reports_dir / "derived_build_verification_report.json"),
            ],
            cwd=str(root),
        )

        report = {
            "generated_at": generated_at,
            "build_id": build_id,
            "source_build_id": source_build_id,
            "source_build_dir": str(source_build_dir),
            "output_build_dir": str(final_dir),
            "transform_version": transform_version,
            "canonical_evidence_arm": (
                str(canonical_evidence_arm)
                if not args.preserve_arm and canonical_evidence_arm
                else None
            ),
            "copied_artifacts": copied,
            "rebuilt_artifacts": ({
                "arm": {
                    "path": str(final_dir / "arm.duckdb"),
                    "report_path": str(arm_report),
                }
            } if not args.preserve_arm else {}),
            "notes": [
                "core.duckdb is cloned from the source build with build metadata updated.",
                (
                    "arm.duckdb is copied without science regeneration for this presentation-only build."
                    if args.preserve_arm
                    else "arm.duckdb is regenerated from the cloned core using current build_arm.py."
                ),
                "disc and snapshot artifacts are copied unless --skip-disc-copy is used.",
                "Versioned map tiles are generated when --build-map-tiles is requested.",
                "Priority simulation scenes are materialized when --build-simulation-scenes is requested.",
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
