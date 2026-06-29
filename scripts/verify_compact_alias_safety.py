#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

import duckdb


COMPACT_FAMILIES = {"white_dwarf", "neutron_star", "black_hole"}
PRIMARY_ALIAS_KINDS = {
    "proper_name",
    "bayer_name",
    "bayer_root_name",
    "bayer_expanded_name",
    "flamsteed_name",
}
MEMBER_PRIMARY_ALIAS_KINDS = {f"member_{kind}" for kind in PRIMARY_ALIAS_KINDS}


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


def compact_predicate(alias: str = "st") -> str:
    families = ", ".join(f"'{value}'" for value in sorted(COMPACT_FAMILIES))
    return f"""
    (
      coalesce({alias}.object_family, '') in ({families})
      or coalesce({alias}.object_type, '') in ({families})
      or upper(coalesce({alias}.spectral_type_raw, '')) like 'D%'
      or coalesce({alias}.spectral_class, '') = 'D'
    )
    """


def fetch_hazards(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    primary_aliases = ", ".join(f"'{value}'" for value in sorted(PRIMARY_ALIAS_KINDS))
    member_aliases = ", ".join(f"'{value}'" for value in sorted(MEMBER_PRIMARY_ALIAS_KINDS))
    compact = compact_predicate("st")
    sibling_compact = compact_predicate("sib")
    rows = con.execute(
        f"""
        with compact_stars as (
          select
            st.star_id,
            st.system_id,
            st.star_name,
            st.stable_object_key,
            st.gaia_id,
            st.hip_id,
            st.hd_id,
            st.wds_id,
            st.object_family,
            st.object_type,
            st.spectral_type_raw,
            st.spectral_class,
            st.vmag,
            s.system_name,
            s.stable_object_key as system_stable_object_key,
            s.wds_id as system_wds_id,
            s.star_count,
            exists (
              select 1
              from stars sib
              where sib.system_id = st.system_id
                and sib.star_id <> st.star_id
                and not {sibling_compact}
            ) as has_noncompact_sibling
          from stars st
          left join systems s on s.system_id = st.system_id
          where {compact}
        ), star_alias_summary as (
          select
            a.star_id,
            list(distinct a.alias_raw order by a.alias_raw) filter (
              where a.alias_kind in ({primary_aliases})
                and coalesce(a.source_catalog, '') = 'athyg_crosswalk'
            ) as primary_aliases,
            count(*) filter (
              where a.alias_kind in ({primary_aliases})
                and coalesce(a.source_catalog, '') = 'athyg_crosswalk'
            ) as primary_alias_count,
            count(*) filter (
              where a.alias_kind in ({primary_aliases})
                and a.alias_kind <> 'proper_name'
                and coalesce(a.source_catalog, '') = 'athyg_crosswalk'
            ) as nonproper_primary_alias_count
          from aliases a
          where a.star_id is not null
          group by a.star_id
        ), system_alias_summary as (
          select
            a.system_id,
            list(distinct a.alias_raw order by a.alias_raw) filter (
              where a.alias_kind in ({member_aliases})
                and coalesce(a.source_catalog, '') = 'athyg_crosswalk'
            ) as member_primary_aliases,
            count(*) filter (
              where a.alias_kind in ({member_aliases})
                and coalesce(a.source_catalog, '') = 'athyg_crosswalk'
            ) as member_primary_alias_count,
            count(*) filter (
              where a.alias_kind in ({member_aliases})
                and a.alias_kind <> 'member_proper_name'
                and coalesce(a.source_catalog, '') = 'athyg_crosswalk'
            ) as nonproper_member_primary_alias_count
          from aliases a
          where a.system_id is not null
          group by a.system_id
        )
        select
          c.star_id,
          c.system_id,
          c.star_name,
          c.stable_object_key,
          c.system_name,
          c.system_stable_object_key,
          c.gaia_id,
          c.hip_id,
          c.hd_id,
          c.wds_id,
          c.system_wds_id,
          c.object_family,
          c.object_type,
          c.spectral_type_raw,
          c.spectral_class,
          c.vmag,
          c.star_count,
          c.has_noncompact_sibling,
          coalesce(sa.primary_alias_count, 0) as primary_alias_count,
          coalesce(sysa.member_primary_alias_count, 0) as member_primary_alias_count,
          coalesce(sa.nonproper_primary_alias_count, 0) as nonproper_primary_alias_count,
          coalesce(sysa.nonproper_member_primary_alias_count, 0) as nonproper_member_primary_alias_count,
          coalesce(sa.primary_aliases, []) as primary_aliases,
          coalesce(sysa.member_primary_aliases, []) as member_primary_aliases
        from compact_stars c
        left join star_alias_summary sa on sa.star_id = c.star_id
        left join system_alias_summary sysa on sysa.system_id = c.system_id
        where not c.has_noncompact_sibling
          and (
            coalesce(sa.primary_alias_count, 0) > 0
            or coalesce(sysa.member_primary_alias_count, 0) > 0
          )
          and (
            c.hd_id is not null
            or c.wds_id is not null
            or c.system_wds_id is not null
            or coalesce(sa.nonproper_primary_alias_count, 0) > 0
            or coalesce(sysa.nonproper_member_primary_alias_count, 0) > 0
          )
        order by coalesce(c.vmag, 999), c.star_id
        limit 100
        """
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect unsafe bright-primary alias/identifier attachment to compact-object rows."
    )
    parser.add_argument("--core-db", required=True, help="Path to core.duckdb")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Print hazards but exit 0. Useful while validating known-bad served builds.",
    )
    args = parser.parse_args()

    con = duckdb.connect(args.core_db, read_only=True)
    for table_name in ("stars", "systems", "aliases"):
        if not table_exists(con, table_name):
            raise SystemExit(f"Missing required table: {table_name}")

    hazards = fetch_hazards(con)
    payload = {
        "core_db": args.core_db,
        "hazard_count": len(hazards),
        "hazards": hazards,
        "policy": (
            "Compact-object rows without a non-compact sibling must not carry "
            "AT-HYG bright-primary aliases plus HD/WDS or non-proper primary aliases."
        ),
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    elif hazards:
        print(f"Compact alias safety hazards: {len(hazards)}")
        for hazard in hazards[:20]:
            aliases = list(hazard.get("primary_aliases") or []) + list(hazard.get("member_primary_aliases") or [])
            print(
                "- "
                f"star_id={hazard.get('star_id')} system_id={hazard.get('system_id')} "
                f"name={hazard.get('star_name')!r} spectral={hazard.get('spectral_type_raw')!r} "
                f"gaia={hazard.get('gaia_id')} hip={hazard.get('hip_id')} hd={hazard.get('hd_id')} "
                f"aliases={aliases[:6]!r}"
            )
    else:
        print("OK: compact alias safety gate")

    if hazards and not args.warn_only:
        raise SystemExit(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
