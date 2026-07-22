#!/usr/bin/env python3
"""Rebuild clean cluster evidence in scratch and compare canonical Parquet hashes."""

from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

from compile_e7_clean_clusters import (
    DEFAULT_POLICY,
    DEFAULT_STATE,
    compile_clusters,
    read_json,
    write_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--scratch-parent", type=Path, default=Path("/mnt/space/spacegate/e7-clean-cluster-reproduction"))
    parser.add_argument("--report", type=Path, default=DEFAULT_STATE / "reports/evidence_lake_v2/e7_clean_clusters_reproduction.json")
    args = parser.parse_args()
    reference = read_json(args.reference / "manifest.json")
    args.scratch_parent.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="clean-clusters.", dir=args.scratch_parent))
    try:
        compile_report = scratch / "compile.json"
        reproduced = compile_clusters(
            policy_path=args.policy,
            state=args.state,
            output_root=scratch / "artifacts",
            report_path=compile_report,
        )
        differing = []
        for name in sorted(set(reference["deterministic_files"]) | set(reproduced["deterministic_files"])):
            if reference["deterministic_files"].get(name) != reproduced["deterministic_files"].get(name):
                differing.append(name)
        report = {
            "schema_version": "spacegate.e7_clean_clusters_reproduction.v1",
            "build_id": reference["build_id"],
            "status": "pass" if reproduced["build_id"] == reference["build_id"] and not differing else "fail",
            "reproduced_build_id": reproduced["build_id"],
            "differing_files": differing,
            "compile_wall_seconds": reproduced["wall_seconds"],
            "compile_cpu_seconds": reproduced["cpu_seconds"],
            "scratch_removed": True,
        }
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    write_json(args.report, report)
    print(f"Clean cluster reproduction {report['status']}: {report['build_id']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
