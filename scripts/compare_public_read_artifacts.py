#!/usr/bin/env python3
"""Compare two complete public-read artifacts for deterministic reproduction."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_artifact(root: Path) -> tuple[dict[str, Any], Path]:
    manifest_path = root / "manifest.json"
    database = root / "public_read.sqlite"
    if not manifest_path.is_file() or not database.is_file():
        raise SystemExit(f"incomplete public-read artifact: {root}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "pass":
        raise SystemExit(f"public-read artifact has not passed: {root}")
    expected = (manifest.get("artifact") or {}).get("sha256")
    actual = sha256(database)
    if expected != actual:
        raise SystemExit(f"manifest hash mismatch: {root}")
    return manifest, database


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-dir", required=True, type=Path)
    parser.add_argument("--reproduced-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    reference_root = args.reference_dir.resolve(strict=True)
    reproduced_root = args.reproduced_dir.resolve(strict=True)
    reference, reference_db = load_artifact(reference_root)
    reproduced, reproduced_db = load_artifact(reproduced_root)
    comparisons = {
        "build_id": reference.get("build_id") == reproduced.get("build_id"),
        "projection_schema_version": reference.get("projection_schema_version")
        == reproduced.get("projection_schema_version"),
        "search_schema_version": reference.get("search_schema_version")
        == reproduced.get("search_schema_version"),
        "stellar_badge_overlay_schema_version": reference.get(
            "stellar_badge_overlay_schema_version"
        )
        == reproduced.get("stellar_badge_overlay_schema_version"),
        "policy_sha256": (reference.get("policy") or {}).get("sha256")
        == (reproduced.get("policy") or {}).get("sha256"),
        "source_artifacts": reference.get("source_artifacts")
        == reproduced.get("source_artifacts"),
        "counts": reference.get("counts") == reproduced.get("counts"),
        "representation_counts": reference.get("representation_counts")
        == reproduced.get("representation_counts"),
        "logical_hashes": reference.get("logical_hashes")
        == reproduced.get("logical_hashes"),
        "artifact_bytes": reference_db.stat().st_size
        == reproduced_db.stat().st_size,
        "artifact_sha256": sha256(reference_db) == sha256(reproduced_db),
    }
    report = {
        "schema_version": "spacegate.public_read_determinism.v1",
        "status": "pass" if all(comparisons.values()) else "fail",
        "build_id": reference.get("build_id"),
        "comparisons": comparisons,
        "reference": {
            "path": str(reference_root),
            "bytes": reference_db.stat().st_size,
            "sha256": sha256(reference_db),
        },
        "reproduced": {
            "path": str(reproduced_root),
            "bytes": reproduced_db.stat().st_size,
            "sha256": sha256(reproduced_db),
        },
        "generated_at_utc": utc_now(),
    }
    atomic_json(args.output.resolve(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
