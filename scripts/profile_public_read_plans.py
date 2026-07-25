#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def query_plan(
    con: sqlite3.Connection,
    sql: str,
    params: list[Any],
) -> list[str]:
    return [
        str(row[3])
        for row in con.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    ]


def includes(plan: list[str], required: str) -> bool:
    return any(required in row for row in plan)


def run(database: Path) -> dict[str, Any]:
    con = sqlite3.connect(f"file:{database.resolve()}?mode=ro&immutable=1", uri=True)
    try:
        con.row_factory = sqlite3.Row
        metadata = dict(con.execute("SELECT key,value FROM metadata").fetchall())
        system_id = int(
            con.execute(
                "SELECT system_id FROM systems ORDER BY system_id LIMIT 1"
            ).fetchone()[0]
        )
        bundle_row = con.execute(
            "SELECT system_id FROM hierarchy_bundles ORDER BY system_id LIMIT 1"
        ).fetchone()
        if bundle_row is None:
            bundle_row = con.execute(
                """
                SELECT system_id FROM systems
                WHERE hierarchy_representation='bundle_required'
                ORDER BY system_id LIMIT 1
                """
            ).fetchone()
        bundle_id = int(bundle_row[0] if bundle_row is not None else system_id)
        seed_id = int(
            con.execute(
                "SELECT system_id FROM singleton_scene_seeds ORDER BY system_id LIMIT 1"
            ).fetchone()[0]
        )
        exact_term = str(
            con.execute(
                "SELECT term_norm FROM search_terms ORDER BY search_term_id LIMIT 1"
            ).fetchone()[0]
        )
        identifier = con.execute(
            """
            SELECT namespace,id_value_norm
            FROM exact_identifiers
            ORDER BY identifier_id
            LIMIT 1
            """
        ).fetchone()
        plans = {
            "system_summary": query_plan(
                con,
                "SELECT * FROM systems WHERE system_id=?",
                [system_id],
            ),
            "system_stars": query_plan(
                con,
                "SELECT * FROM stars WHERE system_id=? ORDER BY star_id",
                [system_id],
            ),
            "hierarchy_bundle": query_plan(
                con,
                "SELECT * FROM hierarchy_bundles WHERE system_id=?",
                [bundle_id],
            ),
            "singleton_scene_seed": query_plan(
                con,
                "SELECT * FROM singleton_scene_seeds WHERE system_id=?",
                [seed_id],
            ),
            "exact_search_term": query_plan(
                con,
                """
                SELECT * FROM search_terms
                WHERE term_norm=?
                ORDER BY term_priority,is_primary DESC,search_term_id
                LIMIT 5000
                """,
                [exact_term],
            ),
            "exact_identifier": query_plan(
                con,
                """
                SELECT * FROM exact_identifiers
                WHERE namespace=? AND id_value_norm=?
                ORDER BY system_id,identifier_id
                """,
                [identifier["namespace"], identifier["id_value_norm"]],
            ),
            "trigram_candidates": query_plan(
                con,
                """
                SELECT st.search_term_id
                FROM search_terms_fts f
                JOIN search_terms st ON st.search_term_id=f.rowid
                WHERE search_terms_fts MATCH ?
                LIMIT 5000
                """,
                ['"alp"'],
            ),
            "filtered_coolness": query_plan(
                con,
                """
                SELECT system_id
                FROM systems
                WHERE planet_count >= 1
                ORDER BY coalesce(coolness_rank,9223372036854775807),
                         system_name_norm,system_id
                LIMIT 50
                """,
                [],
            ),
            "unfiltered_coolness": query_plan(
                con,
                """
                SELECT system_id
                FROM systems
                ORDER BY coalesce(coolness_rank,9223372036854775807),
                         system_name_norm,system_id
                LIMIT 50
                """,
                [],
            ),
        }
        checks = {
            "system_summary_primary_key": includes(
                plans["system_summary"], "INTEGER PRIMARY KEY"
            ),
            "system_stars_index": includes(
                plans["system_stars"], "stars_system_idx"
            ),
            "hierarchy_bundle_primary_key": includes(
                plans["hierarchy_bundle"], "INTEGER PRIMARY KEY"
            ),
            "singleton_system_primary_key": includes(
                plans["singleton_scene_seed"], "INTEGER PRIMARY KEY"
            ),
            "singleton_stars_index": includes(
                plans["singleton_scene_seed"], "stars_system_idx"
            ),
            "exact_term_index": includes(
                plans["exact_search_term"], "search_terms_exact_idx"
            ),
            "exact_identifier_index": includes(
                plans["exact_identifier"], "exact_identifiers_lookup_idx"
            ),
            "trigram_virtual_index": includes(
                plans["trigram_candidates"], "VIRTUAL TABLE INDEX"
            ),
            "filtered_system_index": any(
                "INDEX systems_" in row for row in plans["filtered_coolness"]
            ),
            "unfiltered_coolness_index": includes(
                plans["unfiltered_coolness"], "systems_coolness_sort_idx"
            ),
        }
        return {
            "schema_version": "spacegate.public_read_query_plan.v1",
            "status": "pass" if all(checks.values()) else "fail",
            "generated_at_utc": utc_now(),
            "database": database.name,
            "build_id": metadata.get("build_id"),
            "projection_schema_version": metadata.get("projection_schema_version"),
            "checks": checks,
            "plans": plans,
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify Public Read v2 lookup plans use bounded indexes."
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = run(args.database)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f".{args.output.name}.tmp")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(args.output)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
