#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List

try:
    import duckdb
except ModuleNotFoundError:
    duckdb = None


def now_utc() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def detect_state_dir(root: Path) -> Path:
    raw = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if raw:
        return Path(raw).expanduser().resolve()
    return (root / "data").resolve()


def detect_dl_root() -> Path:
    raw = (os.getenv("SPACEGATE_DL_ROOT") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    if Path("/data/spacegate").exists():
        return Path("/data/spacegate/dl")
    return Path("/srv/spacegate/dl")


def detect_build_id(state_dir: Path) -> str:
    core_db = state_dir / "served" / "current" / "core.duckdb"
    if duckdb is not None and core_db.exists():
        try:
            con = duckdb.connect(str(core_db), read_only=True)
            row = con.execute(
                "select value from build_metadata where key='build_id' limit 1"
            ).fetchone()
            con.close()
            if row and str(row[0] or "").strip():
                return str(row[0]).strip()
        except Exception:
            pass
    served_link = state_dir / "served" / "current"
    try:
        if served_link.exists():
            return served_link.resolve().name
    except Exception:
        return ""
    return ""


def json_load(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def safe_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def iter_manifest_entries(manifest_dir: Path) -> Iterable[Dict[str, Any]]:
    for path in sorted(manifest_dir.glob("*_manifest.json")):
        payload = json_load(path)
        if not isinstance(payload, list):
            continue
        for row in payload:
            if not isinstance(row, dict):
                continue
            dest_path = str(row.get("dest_path") or "")
            dest = Path(dest_path)
            if len(dest.parts) < 3 or dest.parts[0] != "raw":
                continue
            out = dict(row)
            out["_manifest_file"] = path.name
            out["_catalog"] = str(dest.parts[1])
            yield out


def collect_cooked_paths(state_dir: Path, catalogs: List[str]) -> List[Path]:
    cooked: List[Path] = []
    cooked_root = state_dir / "cooked"
    for catalog in sorted(set(catalogs)):
        candidate = cooked_root / catalog
        if candidate.exists() and candidate.is_dir():
            cooked.append(candidate)
    if any(c in {"exoplanet_eu", "open_exoplanet_catalogue", "hwc"} for c in catalogs):
        lifecycle = cooked_root / "exoplanet_lifecycle"
        if lifecycle.exists() and lifecycle.is_dir():
            cooked.append(lifecycle)
    return sorted(set(cooked))


def update_current_pointer(catalog_root: Path, snapshot_id: str) -> None:
    current_link = catalog_root / "current"
    target = Path("snapshots") / snapshot_id
    if current_link.is_symlink() or current_link.exists():
        current_link.unlink()
    current_link.symlink_to(target)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Mirror catalog raw+cooked artifacts for public bootstrap distribution."
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--dl-root", default=str(detect_dl_root()))
    parser.add_argument("--snapshot-id", default="")
    parser.add_argument(
        "--catalog",
        action="append",
        default=[],
        help="Catalog folder under raw/ (repeatable). Default: all catalogs present in manifests.",
    )
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="Mirror raw artifacts only (skip cooked copy).",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    state_dir = Path(args.state_dir).resolve() if args.state_dir else detect_state_dir(root)
    dl_root = Path(args.dl_root).resolve()

    manifest_dir = state_dir / "reports" / "manifests"
    if not manifest_dir.exists():
        raise SystemExit(f"Manifest directory not found: {manifest_dir}")

    entries = list(iter_manifest_entries(manifest_dir))
    if not entries:
        raise SystemExit(f"No manifest entries with raw dest_path found in {manifest_dir}")

    requested_catalogs = sorted({str(c).strip() for c in args.catalog if str(c).strip()})
    available_catalogs = sorted({str(e.get("_catalog") or "") for e in entries})
    selected_catalogs = requested_catalogs or available_catalogs
    selected_catalog_set = set(selected_catalogs)
    unknown = sorted(set(selected_catalogs) - set(available_catalogs))
    if unknown:
        raise SystemExit(
            "Unknown catalog(s): "
            + ", ".join(unknown)
            + f" (available: {', '.join(available_catalogs)})"
        )

    build_id = detect_build_id(state_dir)
    snapshot_id = (
        args.snapshot_id.strip()
        or build_id
        or dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    )

    catalog_root = dl_root / "catalogs"
    snapshot_root = catalog_root / "snapshots" / snapshot_id
    mirror_raw_root = snapshot_root / "raw"
    mirror_cooked_root = snapshot_root / "cooked"
    snapshot_root.mkdir(parents=True, exist_ok=True)

    mirrored_rows: List[Dict[str, Any]] = []
    missing_rows: List[Dict[str, Any]] = []

    for row in entries:
        catalog = str(row.get("_catalog") or "")
        if catalog not in selected_catalog_set:
            continue
        dest_path = str(row.get("dest_path") or "")
        src = state_dir / dest_path
        relative_raw = Path(*Path(dest_path).parts[1:])
        dst = mirror_raw_root / relative_raw
        payload: Dict[str, Any] = {
            "catalog": catalog,
            "manifest_file": row.get("_manifest_file"),
            "source_name": row.get("source_name"),
            "source_version": row.get("source_version"),
            "source_url": row.get("url"),
            "dest_path": dest_path,
            "mirror_path": str(Path("raw") / relative_raw),
            "retrieved_at": row.get("retrieved_at"),
            "checked_at": row.get("checked_at"),
            "bytes_written": row.get("bytes_written"),
            "row_count": row.get("row_count"),
            "sha256_manifest": row.get("sha256"),
        }
        if not src.exists():
            payload["status"] = "missing_source_file"
            missing_rows.append(payload)
            continue
        safe_copy(src, dst)
        payload["status"] = "mirrored"
        payload["bytes_mirrored"] = int(dst.stat().st_size)
        payload["sha256_mirrored"] = sha256_file(dst)
        payload["sha256_match"] = (
            str(payload.get("sha256_manifest") or "").lower()
            == str(payload.get("sha256_mirrored") or "").lower()
        )
        mirrored_rows.append(payload)

    cooked_rows: List[Dict[str, Any]] = []
    if not args.raw_only:
        cooked_paths = collect_cooked_paths(state_dir, selected_catalogs)
        for cooked_dir in cooked_paths:
            for src in sorted(cooked_dir.rglob("*")):
                if not src.is_file():
                    continue
                rel = src.relative_to(state_dir / "cooked")
                dst = mirror_cooked_root / rel
                safe_copy(src, dst)
                cooked_rows.append(
                    {
                        "catalog": str(rel.parts[0]) if rel.parts else "",
                        "source_path": str(src.relative_to(state_dir)),
                        "mirror_path": str(Path("cooked") / rel),
                        "bytes_mirrored": int(dst.stat().st_size),
                        "sha256_mirrored": sha256_file(dst),
                    }
                )

    index_payload = {
        "snapshot_id": snapshot_id,
        "build_id": build_id or None,
        "generated_at": now_utc(),
        "state_dir": str(state_dir),
        "source_manifest_dir": str(manifest_dir),
        "selected_catalogs": selected_catalogs,
        "raw_mirrored_count": len(mirrored_rows),
        "raw_missing_count": len(missing_rows),
        "cooked_mirrored_count": len(cooked_rows),
        "raw_entries": mirrored_rows,
        "raw_missing": missing_rows,
        "cooked_artifacts": cooked_rows,
        "notes": [
            "Raw artifacts preserve upstream format for provenance and reproducibility.",
            "Cooked artifacts preserve Spacegate-normalized outputs for downstream bootstrap convenience.",
        ],
    }
    index_path = snapshot_root / "index.json"
    index_path.write_text(json.dumps(index_payload, indent=2) + "\n", encoding="utf-8")

    update_current_pointer(catalog_root, snapshot_id)
    current_meta = {
        "snapshot_id": snapshot_id,
        "build_id": build_id or None,
        "generated_at": now_utc(),
        "index": f"snapshots/{snapshot_id}/index.json",
        "selected_catalogs": selected_catalogs,
        "raw_mirrored_count": len(mirrored_rows),
        "raw_missing_count": len(missing_rows),
        "cooked_mirrored_count": len(cooked_rows),
    }
    (catalog_root / "current.json").write_text(
        json.dumps(current_meta, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"Catalog mirror snapshot: {snapshot_id}")
    print(f"Mirror root: {snapshot_root}")
    print(f"Raw mirrored: {len(mirrored_rows)}")
    print(f"Raw missing: {len(missing_rows)}")
    print(f"Cooked mirrored: {len(cooked_rows)}")
    print(f"Index: {index_path}")
    print(f"Current pointer: {catalog_root / 'current'} -> snapshots/{snapshot_id}")
    print(f"Current metadata: {catalog_root / 'current.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
