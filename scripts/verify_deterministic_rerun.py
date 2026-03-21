#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def compare_table_fingerprints(
    current: dict,
    baseline: dict,
    table: str,
) -> list[str]:
    mismatches: list[str] = []
    current_tables = current.get("table_fingerprints") or {}
    baseline_tables = baseline.get("table_fingerprints") or {}
    c = current_tables.get(table) or {}
    b = baseline_tables.get(table) or {}
    for key in ("row_count", "xor_hash_hex", "min_hash_uint64", "max_hash_uint64"):
        if c.get(key) != b.get(key):
            mismatches.append(
                f"{table}.{key}: current={c.get(key)!r} baseline={b.get(key)!r}"
            )
    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify deterministic rerun fingerprints against prior comparable builds."
    )
    parser.add_argument("--state-dir", required=True, help="SPACEGATE_STATE_DIR path")
    parser.add_argument("--build-id", required=True, help="Target build id")
    parser.add_argument(
        "--strict-no-baseline",
        action="store_true",
        help="Fail if no comparable baseline build exists.",
    )
    args = parser.parse_args()

    state_dir = Path(args.state_dir).resolve()
    reports_dir = state_dir / "reports"
    current_report_path = reports_dir / args.build_id / "determinism_report.json"
    if not current_report_path.exists():
        raise SystemExit(f"Missing determinism report: {current_report_path}")

    current = load_json(current_report_path)
    current_fp = str(current.get("source_inputs_fingerprint") or "")
    current_transform = str(current.get("transform_version") or "")
    current_layer = str(current.get("build_layer") or "")
    current_slice_id = str(current.get("slice_profile_id") or "")
    current_slice_version = str(current.get("slice_profile_version") or "")

    candidates: list[tuple[str, dict]] = []
    for child in reports_dir.iterdir():
        if not child.is_dir():
            continue
        build_id = child.name
        if build_id == args.build_id:
            continue
        report_path = child / "determinism_report.json"
        if not report_path.exists():
            continue
        try:
            payload = load_json(report_path)
        except Exception:
            continue
        if str(payload.get("source_inputs_fingerprint") or "") != current_fp:
            continue
        if str(payload.get("transform_version") or "") != current_transform:
            continue
        if str(payload.get("build_layer") or "") != current_layer:
            continue
        if str(payload.get("slice_profile_id") or "") != current_slice_id:
            continue
        if str(payload.get("slice_profile_version") or "") != current_slice_version:
            continue
        candidates.append((build_id, payload))

    if not candidates:
        message = (
            "No comparable baseline determinism report found for "
            f"build={args.build_id} fp={current_fp[:12]}."
        )
        if args.strict_no_baseline:
            raise SystemExit(message)
        print(f"WARN: {message}")
        return 0

    candidates.sort(
        key=lambda row: (
            str((row[1] or {}).get("generated_at") or ""),
            row[0],
        )
    )
    baseline_build_id, baseline = candidates[-1]
    mismatches: list[str] = []
    for table in ("stars", "systems", "planets"):
        mismatches.extend(compare_table_fingerprints(current, baseline, table))

    if mismatches:
        print(
            "Determinism fingerprint mismatch "
            f"(current={args.build_id}, baseline={baseline_build_id}):"
        )
        for item in mismatches:
            print(f"- {item}")
        raise SystemExit(1)

    print(
        "OK: deterministic rerun fingerprint matches "
        f"baseline build {baseline_build_id}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

