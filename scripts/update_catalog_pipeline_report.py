#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
from pathlib import Path


def now_utc() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def count_rows_fast(path: Path, max_bytes: int = 200_000_000) -> int | None:
    if not path.exists():
        return None
    try:
        size_bytes = int(path.stat().st_size)
    except OSError:
        return None
    if size_bytes <= 0 or size_bytes > max_bytes:
        return None
    suffixes = "".join(path.suffixes).lower()
    try:
        if suffixes.endswith(".csv"):
            with path.open("rt", encoding="utf-8", errors="replace") as handle:
                line_count = sum(1 for _ in handle)
            return max(line_count - 1, 0)
        if suffixes.endswith(".csv.gz"):
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
                line_count = sum(1 for _ in handle)
            return max(line_count - 1, 0)
    except Exception:
        return None
    return None


def detect_state_dir(root: Path) -> Path:
    raw = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if raw:
        return Path(raw).expanduser()
    return root / "data"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if value and value[0] not in "\"'":
            value = value.split("#", 1)[0].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ[key] = value


def init_env(root: Path) -> None:
    for env_path in (
        Path("/etc/spacegate/spacegate.env"),
        root / ".spacegate.env",
        root / ".spacegate.local.env",
    ):
        load_env_file(env_path)


def flatten_manifest_entries(manifest_dir: Path) -> list[dict]:
    rows: list[dict] = []
    if not manifest_dir.exists():
        return rows
    for manifest_path in sorted(manifest_dir.glob("*_manifest.json")):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            row = dict(item)
            row["_manifest_file"] = manifest_path.name
            rows.append(row)
    return rows


def summarize_download_stage(state_dir: Path) -> dict:
    manifest_dir = state_dir / "reports" / "manifests"
    entries = flatten_manifest_entries(manifest_dir)
    by_source: dict[str, dict] = {}
    for entry in entries:
        key = str(entry.get("source_name") or entry.get("source_catalog") or "").strip()
        if not key:
            key = str(entry.get("_manifest_file") or "unknown")
        current = by_source.get(key)
        if current is None:
            by_source[key] = entry
            continue
        current_checked = str(current.get("checked_at") or "")
        next_checked = str(entry.get("checked_at") or "")
        if next_checked >= current_checked:
            by_source[key] = entry

    sources = []
    for source_name in sorted(by_source.keys()):
        entry = by_source[source_name]
        row_count = entry.get("row_count")
        bytes_written = entry.get("bytes_written")
        try:
            row_count = int(row_count) if row_count is not None else None
        except Exception:
            row_count = None
        try:
            bytes_written = int(bytes_written) if bytes_written is not None else None
        except Exception:
            bytes_written = None
        sources.append(
            {
                "source_name": source_name,
                "source_version": entry.get("source_version"),
                "manifest_file": entry.get("_manifest_file"),
                "dest_path": entry.get("dest_path"),
                "url": entry.get("url"),
                "retrieved_at": entry.get("retrieved_at"),
                "checked_at": entry.get("checked_at"),
                "row_count": row_count,
                "bytes_written": bytes_written,
            }
        )
    return {
        "updated_at": now_utc(),
        "manifest_dir": str(manifest_dir),
        "manifest_files_count": len(list(manifest_dir.glob("*_manifest.json"))) if manifest_dir.exists() else 0,
        "source_count": len(sources),
        "sources": sources,
    }


def summarize_cook_stage(state_dir: Path) -> dict:
    cooked_specs = [
        ("athyg", "cooked/athyg/athyg.csv.gz"),
        ("gaia_backbone", "cooked/gaia_backbone/gaia_dr3_backbone.csv"),
        ("nasa_exoplanet_archive", "cooked/nasa_exoplanet_archive/pscomppars_clean.csv"),
        ("wds", "cooked/wds/wds_summary.csv"),
        ("msc", "cooked/msc/msc_components.csv"),
        ("orb6", "cooked/orb6/orb6_orbits.csv"),
        ("gaia_nss_non_single_star", "cooked/gaia_nss/gaia_dr3_non_single_star.csv"),
        ("gaia_nss_two_body_orbit", "cooked/gaia_nss/gaia_dr3_nss_two_body_orbit.csv"),
        ("sbx", "cooked/sbx/sbx_catalog.csv"),
        ("wds_gaia_xmatch", "cooked/wds_gaia_xmatch/wds_gaia_matches.csv"),
        ("gaia_classprob", "cooked/gaia_classprob/gaia_dr3_astrophysical_classprob.csv"),
        ("atnf", "cooked/atnf/pulsars.csv"),
        ("magnetar", "cooked/magnetar/magnetars.csv"),
        ("clusters", "cooked/clusters/open_clusters.csv"),
        ("cluster_members", "cooked/clusters/open_cluster_members.csv"),
        ("snr", "cooked/snr/green_snr.csv"),
        ("debcat", "cooked/debcat/debcat_binaries.csv"),
        ("kepler_eb", "cooked/kepler_eb/kepler_eb_catalog.csv"),
        ("exoplanet_lifecycle_status", "cooked/exoplanet_lifecycle/status_rows.csv"),
        ("exoplanet_lifecycle_aliases", "cooked/exoplanet_lifecycle/alias_rows.csv"),
        ("exoplanet_lifecycle_features", "cooked/exoplanet_lifecycle/features_rows.csv"),
    ]

    cooked_rows: list[dict] = []
    total_size_bytes = 0
    for catalog, rel_path in cooked_specs:
        path = state_dir / rel_path
        exists = path.exists()
        size_bytes = 0
        mtime = None
        row_count = None
        if exists:
            try:
                size_bytes = int(path.stat().st_size)
                mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
            except OSError:
                size_bytes = 0
                mtime = None
            row_count = count_rows_fast(path)
        total_size_bytes += size_bytes
        cooked_rows.append(
            {
                "catalog": catalog,
                "path": str(path),
                "exists": exists,
                "size_bytes": size_bytes,
                "mtime_utc": mtime,
                "row_count_fast": row_count,
            }
        )

    return {
        "updated_at": now_utc(),
        "catalog_count": len(cooked_rows),
        "existing_catalog_count": sum(1 for row in cooked_rows if row.get("exists")),
        "total_size_bytes": total_size_bytes,
        "catalogs": cooked_rows,
    }


def summarize_ingest_stage(build_id: str, catalog_contribution_report: Path | None) -> dict:
    out: dict = {
        "updated_at": now_utc(),
        "build_id": build_id,
    }
    if catalog_contribution_report and catalog_contribution_report.exists():
        report = read_json(catalog_contribution_report)
        out["catalog_contribution_report_path"] = str(catalog_contribution_report)
        out["catalog_contribution_generated_at"] = report.get("generated_at")
        out["catalog_contribution_entries"] = len(report.get("catalog_contributions") or [])
        out["catalog_contribution_totals"] = report.get("totals") or {}
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update shared pipeline-stage catalog report (download/cook/ingest)."
    )
    parser.add_argument(
        "--stage",
        required=True,
        choices=["download", "cook", "ingest"],
        help="Pipeline stage snapshot to update.",
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--build-id", default="", help="Required for ingest stage snapshots.")
    parser.add_argument(
        "--catalog-contribution-report",
        default="",
        help="Optional catalog contribution report path for ingest stage.",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    init_env(root)
    state_dir = detect_state_dir(root).resolve()
    report_path = state_dir / "reports" / "catalog_pipeline_report.json"

    report = read_json(report_path)
    if not isinstance(report.get("stages"), dict):
        report["stages"] = {}

    if args.stage == "download":
        report["stages"]["download"] = summarize_download_stage(state_dir)
    elif args.stage == "cook":
        report["stages"]["cook"] = summarize_cook_stage(state_dir)
    else:
        contribution_path = Path(args.catalog_contribution_report).resolve() if args.catalog_contribution_report else None
        report["stages"]["ingest"] = summarize_ingest_stage(args.build_id.strip(), contribution_path)

    report["updated_at"] = now_utc()
    report["last_stage"] = args.stage
    write_json(report_path, report)
    print(str(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
