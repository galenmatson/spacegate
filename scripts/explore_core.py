#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path

import duckdb


def default_db_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    state_dir = Path(os.getenv("SPACEGATE_STATE_DIR") or root / "data")
    return state_dir / "served" / "current" / "core.duckdb"


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=True)


def cmd_stats(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute(
        """
        with system_counts as (
          select system_id, count(*) as cnt
          from stars
          group by system_id
        )
        select
          (select count(*) from stars) as stars,
          (select count(*) from systems) as systems,
          (select count(*) from planets) as planets,
          (select count(*) from system_counts where cnt = 2) as binary_systems,
          (select count(*) from system_counts where cnt > 1) as multi_star_systems
        """
    ).fetchone()
    print(f"stars:   {row[0]}")
    print(f"systems: {row[1]}")
    print(f"planets: {row[2]}")
    print(f"binaries: {row[3]}")
    print(f"multi_star_systems: {row[4]}")
    return 0


def cmd_search(con: duckdb.DuckDBPyConnection, name: str, limit: int) -> int:
    pattern = f"%{name}%"
    rows = con.execute(
        """
        select kind, id, name, system_id
        from (
          select 'star' as kind, star_id as id, star_name as name, system_id
          from stars
          where star_name is not null and star_name ilike ?
          union all
          select 'system' as kind, system_id as id, system_name as name, system_id
          from systems
          where system_name is not null and system_name ilike ?
          union all
          select 'planet' as kind, planet_id as id, planet_name as name, system_id
          from planets
          where planet_name is not null and planet_name ilike ?
        ) q
        order by name
        limit ?
        """,
        [pattern, pattern, pattern, limit],
    ).fetchall()

    if not rows:
        print("No matches.")
        return 0

    for kind, obj_id, obj_name, system_id in rows:
        sys_part = f" system_id={system_id}" if system_id is not None else ""
        print(f"[{kind}] id={obj_id} name={obj_name}{sys_part}")
    return 0


def cmd_system(con: duckdb.DuckDBPyConnection, system_id: int) -> int:
    system = con.execute(
        """
        select system_id, system_name, dist_ly, x_helio_ly, y_helio_ly, z_helio_ly
        from systems
        where system_id = ?
        """,
        [system_id],
    ).fetchone()
    if system is None:
        print(f"System {system_id} not found.")
        return 1

    print(
        "system_id={0} name={1} dist_ly={2} x={3} y={4} z={5}".format(
            system[0], system[1], system[2], system[3], system[4], system[5]
        )
    )
    print("members:")
    members = con.execute(
        """
        select star_id, star_name, vmag
        from stars
        where system_id = ?
        order by vmag asc nulls last, stable_object_key asc
        """,
        [system_id],
    ).fetchall()
    for star_id, star_name, vmag in members:
        vmag_str = "" if vmag is None else f" vmag={vmag}"
        print(f"  star_id={star_id} name={star_name}{vmag_str}")
    return 0


def cmd_neighbors(con: duckdb.DuckDBPyConnection, star_id: int, k: int) -> int:
    target = con.execute(
        """
        select x_helio_ly, y_helio_ly, z_helio_ly
        from stars
        where star_id = ?
        """,
        [star_id],
    ).fetchone()
    if target is None:
        print(f"Star {star_id} not found.")
        return 1
    if any(v is None for v in target):
        print(f"Star {star_id} is missing coordinates.")
        return 1

    rows = con.execute(
        """
        with target as (
          select ?::double as x, ?::double as y, ?::double as z
        )
        select s.star_id, s.star_name, s.system_id,
               sqrt((s.x_helio_ly - t.x)*(s.x_helio_ly - t.x)
                  + (s.y_helio_ly - t.y)*(s.y_helio_ly - t.y)
                  + (s.z_helio_ly - t.z)*(s.z_helio_ly - t.z)) as dist_ly
        from stars s, target t
        where s.star_id != ?
          and s.x_helio_ly is not null
          and s.y_helio_ly is not null
          and s.z_helio_ly is not null
        order by dist_ly asc
        limit ?
        """,
        [target[0], target[1], target[2], star_id, k],
    ).fetchall()

    for nbr_id, nbr_name, system_id, dist in rows:
        sys_part = f" system_id={system_id}" if system_id is not None else ""
        print(f"star_id={nbr_id} name={nbr_name}{sys_part} dist_ly={dist}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Explore Spacegate core DuckDB")
    parser.add_argument("--db", default=str(default_db_path()))

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("stats")

    search_p = sub.add_parser("search")
    search_p.add_argument("--name", required=True)
    search_p.add_argument("--limit", type=int, default=25)

    system_p = sub.add_parser("system")
    system_p.add_argument("--id", type=int, required=True)

    neighbors_p = sub.add_parser("neighbors")
    neighbors_p.add_argument("--star-id", type=int, required=True)
    neighbors_p.add_argument("--k", type=int, default=10)

    args = parser.parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 1

    con = connect(db_path)
    try:
        if args.command == "stats":
            return cmd_stats(con)
        if args.command == "search":
            return cmd_search(con, args.name, args.limit)
        if args.command == "system":
            return cmd_system(con, args.id)
        if args.command == "neighbors":
            return cmd_neighbors(con, args.star_id, args.k)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
