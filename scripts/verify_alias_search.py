#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from typing import Optional

import duckdb


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^0-9A-Za-z]+", " ", value).strip().lower()).strip()


@dataclass(frozen=True)
class AliasCase:
    query: str
    expected_wds_id: Optional[str] = None
    max_dist_ly: Optional[float] = None
    min_planet_count: Optional[int] = None
    expected_display_term: Optional[str] = None


CASES = [
    AliasCase("Castor", expected_wds_id="07346+3153", expected_display_term="Castor"),
    AliasCase("Alpha Geminorum", expected_wds_id="07346+3153"),
    AliasCase("Toliman", expected_wds_id="14396-6050", expected_display_term="Toliman"),
    AliasCase("Alpha Centauri", expected_wds_id="14396-6050"),
    AliasCase("Sirius", max_dist_ly=10.0, expected_display_term="Sirius"),
    AliasCase("Proxima Centauri", max_dist_ly=10.0, expected_display_term="Proxima Centauri"),
    AliasCase("Jabbah", expected_wds_id="16120-1928", expected_display_term="Jabbah"),
    AliasCase("Copernicus", min_planet_count=5, expected_display_term="Copernicus"),
]


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            select 1
            from information_schema.tables
            where table_schema = 'main' and table_name = ?
            limit 1
            """,
            [table_name],
        ).fetchone()
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify common-name alias search materialization.")
    parser.add_argument("--core-db", required=True, help="Path to core.duckdb")
    args = parser.parse_args()

    con = duckdb.connect(args.core_db, read_only=True)
    for table_name in ("aliases", "system_search_terms", "systems"):
        if not table_exists(con, table_name):
            raise SystemExit(f"Missing required table: {table_name}")

    alias_count = int(con.execute("select count(*)::bigint from aliases").fetchone()[0] or 0)
    proper_count = int(
        con.execute(
            """
            select count(*)::bigint
            from aliases
            where alias_kind in ('proper_name', 'member_proper_name')
            """
        ).fetchone()[0]
        or 0
    )
    expanded_bayer_count = int(
        con.execute(
            """
            select count(*)::bigint
            from aliases
            where alias_kind in ('bayer_expanded_name', 'member_bayer_expanded_name')
            """
        ).fetchone()[0]
        or 0
    )
    if alias_count < 100_000:
        raise SystemExit(f"Alias gate failed: expected broad alias corpus, got {alias_count:,} rows")
    if proper_count < 500:
        raise SystemExit(f"Alias gate failed: expected proper-name aliases, got {proper_count:,}")
    if expanded_bayer_count < 500:
        raise SystemExit(
            f"Alias gate failed: expected expanded Bayer aliases, got {expanded_bayer_count:,}"
        )

    failures: list[str] = []
    for case in CASES:
        query_norm = normalize_name(case.query)
        rows = con.execute(
            """
            select
              s.system_id,
              s.system_name,
              s.wds_id,
              s.dist_ly,
              coalesce(s.planet_count, 0) as planet_count,
              t.term_raw,
              t.term_kind,
              t.term_priority
            from system_search_terms t
            join systems s on s.system_id = t.system_id
            where t.term_norm = ?
            order by
              t.term_priority asc,
              coalesce(s.dist_ly, 1e12) asc,
              s.system_id asc
            limit 12
            """,
            [query_norm],
        ).fetchall()
        if not rows:
            failures.append(f"{case.query}: no system_search_terms exact hit")
            continue

        def matches(row: tuple) -> bool:
            _, _, wds_id, dist_ly, planet_count, term_raw, _, _ = row
            if case.expected_wds_id and str(wds_id or "") != case.expected_wds_id:
                return False
            if case.max_dist_ly is not None and (dist_ly is None or float(dist_ly) > case.max_dist_ly):
                return False
            if case.min_planet_count is not None and int(planet_count or 0) < case.min_planet_count:
                return False
            if case.expected_display_term and normalize_name(str(term_raw or "")) != normalize_name(
                case.expected_display_term
            ):
                return False
            return True

        if not any(matches(row) for row in rows):
            failures.append(f"{case.query}: hits did not satisfy expected target; got {rows[:3]!r}")

    if failures:
        raise SystemExit("Alias search gate failed:\n- " + "\n- ".join(failures))

    print(
        "OK: alias search gate "
        f"(aliases={alias_count:,}, proper_names={proper_count:,}, expanded_bayer={expanded_bayer_count:,})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
