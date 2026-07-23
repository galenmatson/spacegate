#!/usr/bin/env python3
"""Verify logical and byte-exact reproduction of an E5 planet shard."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def table_delta(
    left: Path, right: Path, table: str
) -> dict[str, int]:
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{str(left).replace(chr(39), chr(39) * 2)}' AS left_db (READ_ONLY)")
        con.execute(f"ATTACH '{str(right).replace(chr(39), chr(39) * 2)}' AS right_db (READ_ONLY)")
        left_only = int(con.execute(
            f"SELECT count(*) FROM (SELECT * FROM left_db.{table} EXCEPT SELECT * FROM right_db.{table})"
        ).fetchone()[0])
        right_only = int(con.execute(
            f"SELECT count(*) FROM (SELECT * FROM right_db.{table} EXCEPT SELECT * FROM left_db.{table})"
        ).fetchone()[0])
        return {"left_only": left_only, "right_only": right_only}
    finally:
        con.close()


def verify(left_dir: Path, right_dir: Path) -> dict[str, Any]:
    left = load_json(left_dir / "manifest.json")
    right = load_json(right_dir / "manifest.json")
    deterministic_products = sorted(
        name for name, item in (left.get("products") or {}).items()
        if item.get("determinism") == "byte_exact"
    )
    product_checks = {
        name: {
            "sha256_match": left["products"][name]["sha256"] == right["products"][name]["sha256"],
            "bytes_match": left["products"][name]["bytes"] == right["products"][name]["bytes"],
        }
        for name in deterministic_products
    }
    logical = {
        table: table_delta(
            left_dir / "selected_planet_derivations.duckdb",
            right_dir / "selected_planet_derivations.duckdb",
            table,
        )
        for table in ("selected_facts", "selected_fact_derivations")
    }
    checks = {
        "build_id_match": left.get("build_id") == right.get("build_id"),
        "counts_match": left.get("counts") == right.get("counts"),
        "verification_match": left.get("verification") == right.get("verification"),
        "byte_exact_products_match": all(
            all(values.values()) for values in product_checks.values()
        ),
        "logical_tables_match": all(
            not values["left_only"] and not values["right_only"]
            for values in logical.values()
        ),
    }
    return {
        "schema_version": "spacegate.e5_planet_derivation_reproduction.v1",
        "left_build_id": left.get("build_id"),
        "right_build_id": right.get("build_id"),
        "checks": checks,
        "byte_exact_products": product_checks,
        "logical_table_deltas": logical,
        "status": "pass" if all(checks.values()) else "fail",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--left", type=Path, required=True)
    parser.add_argument("--right", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = verify(args.left.resolve(), args.right.resolve())
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
