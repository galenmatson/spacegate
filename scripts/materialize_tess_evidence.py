#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb

from tess_evidence_materialization import materialize_core


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize targeted TESS identity into a Spacegate core artifact.")
    parser.add_argument("--core-db", required=True)
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--append-search-terms", action="store_true")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(args.state_dir or os.getenv("SPACEGATE_STATE_DIR") or root / "data")
    core_db = Path(args.core_db)
    report_path = Path(args.report_path) if args.report_path else state_dir / "reports" / "tess_identity_coverage_report.json"
    con = duckdb.connect(str(core_db))
    try:
        report = materialize_core(
            con,
            cooked_dir=state_dir / "cooked" / "tess_evidence",
            manifest_path=state_dir / "reports" / "manifests" / "tess_evidence_manifest.json",
            report_path=report_path,
            append_search_terms=args.append_search_terms,
        )
        con.execute("checkpoint")
    finally:
        con.close()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
