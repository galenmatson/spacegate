#!/usr/bin/env python3
"""Compare and optionally remove an isolated clean runtime DISC reproduction."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from verify_e7_clean_runtime_disc import load_object, verify


DEFAULT_PRIMARY_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-disc")
DEFAULT_REPRO_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-disc-reproduction")


def compare(primary_dir: Path, reproduction_dir: Path) -> dict[str, Any]:
    primary = load_object(primary_dir / "manifest.json")
    reproduction = load_object(reproduction_dir / "manifest.json")
    reproduction_verification = verify(reproduction_dir)
    fields = [
        "build_id", "policy_version", "compiler_version", "policy_sha256",
        "compiler_sha256", "scorer_sha256", "inputs", "coolness_profile",
        "stability_databases_opened", "verification",
    ]
    field_differences = {
        field: {"primary": primary.get(field), "reproduction": reproduction.get(field)}
        for field in fields
        if primary.get(field) != reproduction.get(field)
    }
    primary_parquet = (primary.get("products") or {}).get("coolness_scores.parquet") or {}
    reproduction_parquet = (
        reproduction.get("products") or {}
    ).get("coolness_scores.parquet") or {}
    parquet_match = {
        key: primary_parquet.get(key) == reproduction_parquet.get(key)
        for key in ("bytes", "sha256", "determinism")
    }
    status = (
        "pass"
        if not field_differences
        and all(parquet_match.values())
        and reproduction_verification["status"] == "pass"
        else "fail"
    )
    return {
        "schema_version": "spacegate.e7_clean_runtime_disc_reproduction.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": primary.get("build_id"),
        "status": status,
        "field_differences": field_differences,
        "canonical_parquet_match": parquet_match,
        "primary_performance": primary.get("performance"),
        "reproduction_performance": reproduction.get("performance"),
        "reproduction_verification": reproduction_verification,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--primary-root", type=Path, default=DEFAULT_PRIMARY_ROOT)
    parser.add_argument("--reproduction-root", type=Path, default=DEFAULT_REPRO_ROOT)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    primary_dir = args.primary_root.resolve() / args.build_id
    reproduction_dir = args.reproduction_root.resolve() / args.build_id
    report = compare(primary_dir, reproduction_dir)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.cleanup and report["status"] == "pass":
        shutil.rmtree(args.reproduction_root.resolve())
        report["scratch_removed"] = not args.reproduction_root.exists()
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
