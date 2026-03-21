#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path


def now_utc() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


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


def detect_state_dir(root: Path) -> Path:
    raw = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if raw:
        return Path(raw).expanduser()
    return root / "data"


def read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return payload


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def flatten_manifest_entries(manifest_dir: Path) -> list[dict]:
    rows: list[dict] = []
    if not manifest_dir.exists():
        return rows
    for manifest_path in sorted(manifest_dir.glob("*_manifest.json")):
        payload = read_json(manifest_path, default=[])
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            row = dict(item)
            row["_manifest_file"] = manifest_path.name
            rows.append(row)
    return rows


def parse_int_or_none(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def choose_latest(entries: list[dict]) -> dict:
    def stamp(entry: dict) -> tuple[str, str]:
        checked = str(entry.get("checked_at") or "")
        retrieved = str(entry.get("retrieved_at") or "")
        return checked, retrieved

    return max(entries, key=stamp)


def source_signature(entry: dict) -> str:
    payload = {
        "source_name": entry.get("source_name"),
        "source_version": entry.get("source_version"),
        "dest_path": entry.get("dest_path"),
        "url": entry.get("url"),
        "sha256": entry.get("sha256"),
        "bytes_written": parse_int_or_none(entry.get("bytes_written")),
        "row_count": parse_int_or_none(entry.get("row_count")),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def source_record(entry: dict) -> dict:
    record = {
        "source_name": entry.get("source_name"),
        "source_version": entry.get("source_version"),
        "manifest_file": entry.get("_manifest_file"),
        "dest_path": entry.get("dest_path"),
        "url": entry.get("url"),
        "retrieved_at": entry.get("retrieved_at"),
        "checked_at": entry.get("checked_at"),
        "sha256": entry.get("sha256"),
        "bytes_written": parse_int_or_none(entry.get("bytes_written")),
        "row_count": parse_int_or_none(entry.get("row_count")),
        "delta_update": entry.get("delta_update") if isinstance(entry.get("delta_update"), dict) else {},
    }
    record["signature"] = source_signature(record)
    return record


def collect_current_snapshot(manifest_dir: Path) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = {}
    for entry in flatten_manifest_entries(manifest_dir):
        source_name = str(entry.get("source_name") or "").strip()
        if not source_name:
            continue
        grouped.setdefault(source_name, []).append(entry)
    out: dict[str, dict] = {}
    for source_name, entries in grouped.items():
        out[source_name] = source_record(choose_latest(entries))
    return out


def changed_fields(previous: dict, current: dict) -> list[str]:
    fields = [
        "source_version",
        "dest_path",
        "url",
        "sha256",
        "bytes_written",
        "row_count",
    ]
    changed: list[str] = []
    for field in fields:
        if previous.get(field) != current.get(field):
            changed.append(field)
    return changed


def summarize(current_sources: dict[str, dict], previous_sources: dict[str, dict], baseline: bool) -> dict:
    current_keys = set(current_sources.keys())
    previous_keys = set(previous_sources.keys())
    new_keys = sorted(current_keys - previous_keys)
    missing_keys = sorted(previous_keys - current_keys)
    common_keys = sorted(current_keys & previous_keys)

    changed: list[dict] = []
    unchanged: list[dict] = []
    row_delta_known_sum = 0
    row_delta_known_count = 0

    for key in common_keys:
        curr = current_sources[key]
        prev = previous_sources[key]
        if curr.get("signature") == prev.get("signature"):
            unchanged.append(
                {
                    "source_name": key,
                    "row_count": curr.get("row_count"),
                    "bytes_written": curr.get("bytes_written"),
                }
            )
            continue
        fields = changed_fields(prev, curr)
        prev_rows = prev.get("row_count")
        curr_rows = curr.get("row_count")
        row_delta = None
        if isinstance(prev_rows, int) and isinstance(curr_rows, int):
            row_delta = curr_rows - prev_rows
            row_delta_known_sum += row_delta
            row_delta_known_count += 1
        changed.append(
            {
                "source_name": key,
                "changed_fields": fields,
                "row_delta": row_delta,
                "previous": prev,
                "current": curr,
            }
        )

    payload = {
        "baseline_created": baseline,
        "summary": {
            "current_source_count": len(current_keys),
            "previous_source_count": len(previous_keys),
            "new_source_count": len(new_keys),
            "changed_source_count": len(changed),
            "missing_source_count": len(missing_keys),
            "unchanged_source_count": len(unchanged),
            "row_delta_known_source_count": row_delta_known_count,
            "row_delta_known_sum": row_delta_known_sum,
        },
        "new_sources": [current_sources[key] for key in new_keys],
        "changed_sources": changed,
        "missing_sources": [previous_sources[key] for key in missing_keys],
        "unchanged_sources": unchanged,
    }
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create per-source delta report by comparing current manifests against prior snapshot."
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument(
        "--snapshot-path",
        default="",
        help="Snapshot file path (default: $SPACEGATE_STATE_DIR/reports/source_delta_snapshot.json).",
    )
    parser.add_argument(
        "--report-path",
        default="",
        help="Report file path (default: $SPACEGATE_STATE_DIR/reports/source_delta_report.json).",
    )
    parser.add_argument(
        "--history-dir",
        default="",
        help="History directory (default: $SPACEGATE_STATE_DIR/reports/source_delta_history).",
    )
    parser.add_argument(
        "--no-update-snapshot",
        action="store_true",
        help="Generate report without updating the snapshot baseline file.",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    init_env(root)
    state_dir = detect_state_dir(root).resolve()
    manifest_dir = state_dir / "reports" / "manifests"
    if not manifest_dir.exists():
        raise SystemExit(f"Manifest directory not found: {manifest_dir}")

    snapshot_path = (
        Path(args.snapshot_path).expanduser().resolve()
        if args.snapshot_path
        else (state_dir / "reports" / "source_delta_snapshot.json")
    )
    report_path = (
        Path(args.report_path).expanduser().resolve()
        if args.report_path
        else (state_dir / "reports" / "source_delta_report.json")
    )
    history_dir = (
        Path(args.history_dir).expanduser().resolve()
        if args.history_dir
        else (state_dir / "reports" / "source_delta_history")
    )

    previous_snapshot = read_json(snapshot_path, default={})
    previous_sources = previous_snapshot.get("sources")
    if not isinstance(previous_sources, dict):
        previous_sources = {}

    current_sources = collect_current_snapshot(manifest_dir)

    baseline = len(previous_sources) == 0
    delta_payload = summarize(current_sources=current_sources, previous_sources=previous_sources, baseline=baseline)
    generated_at = now_utc()
    report_payload = {
        "generated_at": generated_at,
        "manifest_dir": str(manifest_dir),
        "snapshot_path": str(snapshot_path),
        "previous_snapshot_updated_at": previous_snapshot.get("updated_at"),
        "current_snapshot_updated_at": generated_at,
        **delta_payload,
    }
    write_json(report_path, report_payload)

    history_payload = dict(report_payload)
    history_payload["report_path"] = str(report_path)
    history_path = history_dir / f"{generated_at.replace(':', '').replace('-', '')}.json"
    write_json(history_path, history_payload)

    if not args.no_update_snapshot:
        snapshot_payload = {
            "updated_at": generated_at,
            "manifest_dir": str(manifest_dir),
            "source_count": len(current_sources),
            "sources": current_sources,
        }
        write_json(snapshot_path, snapshot_payload)

    print(str(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
