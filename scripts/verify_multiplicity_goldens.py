#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

try:
    import duckdb
except ModuleNotFoundError as exc:
    raise SystemExit(
        "python module 'duckdb' not found. Run from the project venv "
        "(for example: /srv/spacegate/app/.venv/bin/python scripts/verify_multiplicity_goldens.py)"
    ) from exc

TIER_RANK = {
    "illustrative": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


def normalize_label(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().lower().split())


def resolve_default_paths(root: Path) -> tuple[Path, Path]:
    env_state = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if env_state:
        state_dir = Path(env_state)
    else:
        repo_state = root / "data"
        shared_state = Path("/data/spacegate/data")
        if shared_state.exists():
            state_dir = shared_state
        else:
            state_dir = repo_state
    served = state_dir / "served" / "current"
    return served / "core.duckdb", served / "arm.duckdb"


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        select 1
        from information_schema.tables
        where table_schema = 'main' and table_name = ?
        limit 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def fail(results: dict[str, Any], system_id: str, message: str) -> None:
    results["systems"].append(
        {
            "id": system_id,
            "status": "fail",
            "message": message,
        }
    )


def skip_result(results: dict[str, Any], system_id: str, message: str) -> None:
    results["systems"].append(
        {
            "id": system_id,
            "status": "skipped",
            "message": message,
        }
    )


def pass_result(results: dict[str, Any], system_id: str, details: dict[str, Any]) -> None:
    payload = {"id": system_id, "status": "pass"}
    payload.update(details)
    results["systems"].append(payload)


def find_component_rows(
    arm: duckdb.DuckDBPyConnection,
    label_token: str,
    *,
    component_type: str | None = None,
    stable_key_scope: str | None = None,
) -> list[tuple[str, str, str]]:
    token = normalize_label(label_token)
    type_filter = "and component_type = ?" if component_type else ""
    scope_filter = "and stable_component_key like ?" if stable_key_scope else ""
    params = [token, f"% {token}%"]
    if component_type:
        params.append(component_type)
    if stable_key_scope:
        params.append(stable_key_scope)
    rows = arm.execute(
        f"""
        select
          stable_component_key,
          coalesce(display_name, '') as display_name,
          lower(coalesce(catalog_component_label, '')) as component_label
        from component_entities
        where (
             lower(coalesce(catalog_component_label, '')) = ?
          or lower(coalesce(display_name, '')) like ?
        )
        {type_filter}
        {scope_filter}
        """,
        params,
    ).fetchall()
    return [(str(r[0]), str(r[1]), str(r[2])) for r in rows]


def pick_component_row(
    rows: list[tuple[str, str, str]],
    name_aliases: list[str],
) -> tuple[str, str, str] | None:
    if not rows:
        return None
    normalized_aliases = [normalize_label(alias) for alias in name_aliases if normalize_label(alias)]
    if not normalized_aliases:
        return rows[0]
    for row in rows:
        display_norm = normalize_label(row[1])
        for alias in normalized_aliases:
            if alias and alias in display_norm:
                return row
    return rows[0]


def check_containment_edges(
    arm: duckdb.DuckDBPyConnection,
    system_cfg: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    wds_id = str(system_cfg.get("wds_id") or "").strip()
    key_scope = f"%{wds_id}%" if wds_id else "%"
    for edge in system_cfg.get("expected_containment_edges") or []:
        if not isinstance(edge, list) or len(edge) != 2:
            failures.append(f"invalid containment edge: {edge!r}")
            continue
        parent_label = normalize_label(edge[0])
        child_label = normalize_label(edge[1])
        if parent_label == "root":
            count = arm.execute(
                """
                select count(*)
                from system_hierarchy_edges h
                join component_entities parent_ce on parent_ce.stable_component_key = h.parent_component_key
                join component_entities child_ce on child_ce.stable_component_key = h.child_component_key
                where parent_ce.stable_component_key like 'comp:msc_system:wds:%'
                  and parent_ce.stable_component_key like ?
                  and lower(coalesce(child_ce.catalog_component_label, '')) = ?
                  and h.edge_kind = 'contains'
                """,
                [key_scope, child_label],
            ).fetchone()[0]
        else:
            count = arm.execute(
                """
                select count(*)
                from system_hierarchy_edges h
                join component_entities parent_ce on parent_ce.stable_component_key = h.parent_component_key
                join component_entities child_ce on child_ce.stable_component_key = h.child_component_key
                where lower(coalesce(parent_ce.catalog_component_label, '')) = ?
                  and lower(coalesce(child_ce.catalog_component_label, '')) = ?
                  and parent_ce.stable_component_key like ?
                  and h.edge_kind = 'contains'
                """,
                [parent_label, child_label, key_scope],
            ).fetchone()[0]
        if int(count or 0) < 1:
            failures.append(f"{parent_label}->{child_label}: containment edge missing")
    return failures


def check_orbital_solution_periods(
    arm: duckdb.DuckDBPyConnection,
    system_cfg: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    wds_id = str(system_cfg.get("wds_id") or "").strip()
    key_scope = f"%{wds_id}%" if wds_id else "%"
    for item in system_cfg.get("expected_msc_orbital_periods_days") or []:
        pair = item.get("pair") if isinstance(item, dict) else None
        if not isinstance(pair, list) or len(pair) != 2:
            failures.append(f"invalid period check: {item!r}")
            continue
        a = normalize_label(pair[0])
        b = normalize_label(pair[1])
        expected = float(item.get("period_days"))
        tolerance = float(item.get("tolerance_days", 0.01))
        rows = arm.execute(
            """
            select os.period_days
            from orbit_edges e
            join component_entities p on p.stable_component_key = e.primary_component_key
            join component_entities s on s.stable_component_key = e.secondary_component_key
            join orbital_solutions os on os.orbit_edge_id = e.orbit_edge_id
            where os.solution_source_catalog = 'msc'
              and e.host_component_key like ?
              and (
                (
                  lower(coalesce(p.catalog_component_label, '')) = ?
                  and lower(coalesce(s.catalog_component_label, '')) = ?
                )
                or
                (
                  lower(coalesce(p.catalog_component_label, '')) = ?
                  and lower(coalesce(s.catalog_component_label, '')) = ?
                )
              )
            """,
            [key_scope, a, b, b, a],
        ).fetchall()
        if not rows:
            failures.append(f"{a}-{b}: MSC orbital solution missing")
            continue
        if not any(r[0] is not None and abs(float(r[0]) - expected) <= tolerance for r in rows):
            values = ", ".join(str(r[0]) for r in rows)
            failures.append(f"{a}-{b}: expected {expected} +/- {tolerance} days, got {values}")
    return failures


def find_system_ids_from_aliases(
    core: duckdb.DuckDBPyConnection,
    alias_queries: list[str],
) -> list[int]:
    system_ids: set[int] = set()
    for alias in alias_queries:
        token = normalize_label(alias)
        if not token:
            continue
        rows = core.execute(
            """
            with alias_hits as (
              select target_type, target_id
              from aliases
              where alias_norm = ?
            )
            select distinct
              case
                when h.target_type = 'system' then h.target_id
                when h.target_type = 'star' then s.system_id
                else null
              end as system_id
            from alias_hits h
            left join stars s on h.target_type = 'star' and s.star_id = h.target_id
            where (
              (h.target_type = 'system' and h.target_id is not null)
              or
              (h.target_type = 'star' and s.system_id is not null)
            )
            """,
            [token],
        ).fetchall()
        for row in rows:
            if row and row[0] is not None:
                system_ids.add(int(row[0]))
    return sorted(system_ids)


def check_presence_mode(
    core: duckdb.DuckDBPyConnection,
    system_cfg: dict[str, Any],
    results: dict[str, Any],
) -> None:
    system_id = str(system_cfg.get("id") or "unknown")
    required = bool(system_cfg.get("required", True))
    scope = str(system_cfg.get("scope") or "core")
    alias_queries = [str(v) for v in (system_cfg.get("alias_queries") or system_cfg.get("name_aliases") or [])]
    if not alias_queries:
        fail(results, system_id, "presence mode requires alias_queries or name_aliases")
        return
    matched_system_ids = find_system_ids_from_aliases(core, alias_queries)
    if not matched_system_ids:
        if required:
            fail(results, system_id, "No system match from aliases")
        else:
            skip_result(results, system_id, "No system match from aliases (optional golden)")
        return

    star_count = int(
        core.execute(
            "select count(*) from stars where system_id in (select unnest(?))",
            [matched_system_ids],
        ).fetchone()[0]
        or 0
    )
    planet_count = int(
        core.execute(
            "select count(*) from planets where system_id in (select unnest(?))",
            [matched_system_ids],
        ).fetchone()[0]
        or 0
    )
    min_system_matches = int(system_cfg.get("min_system_matches", 1))
    min_star_rows = int(system_cfg.get("min_star_rows", 1))
    min_planet_rows = int(system_cfg.get("min_planet_rows", 0))

    if len(matched_system_ids) < min_system_matches:
        fail(
            results,
            system_id,
            f"Matched systems {len(matched_system_ids)} < required {min_system_matches}",
        )
        return
    if star_count < min_star_rows:
        fail(results, system_id, f"Matched stars {star_count} < required {min_star_rows}")
        return
    if planet_count < min_planet_rows:
        fail(results, system_id, f"Matched planets {planet_count} < required {min_planet_rows}")
        return

    pass_result(
        results,
        system_id,
        {
            "mode": "presence",
            "scope": scope,
            "required": required,
            "matched_systems": len(matched_system_ids),
            "matched_stars": star_count,
            "matched_planets": planet_count,
            "aliases_checked": alias_queries,
        },
    )


def check_query_mode(
    core: duckdb.DuckDBPyConnection,
    arm: duckdb.DuckDBPyConnection | None,
    system_cfg: dict[str, Any],
    results: dict[str, Any],
) -> None:
    system_id = str(system_cfg.get("id") or "unknown")
    required = bool(system_cfg.get("required", True))
    scope = str(system_cfg.get("scope") or "core")
    sql_count = str(system_cfg.get("sql_count") or "").strip()
    if not sql_count:
        fail(results, system_id, "query mode requires sql_count")
        return
    query_con = arm if scope == "arm" else core
    if query_con is None:
        if required:
            fail(results, system_id, "query mode requested arm scope but arm DB/tables are unavailable")
        else:
            skip_result(results, system_id, "optional arm query skipped (arm DB/tables unavailable)")
        return
    try:
        row = query_con.execute(sql_count).fetchone()
        value = int((row[0] if row else 0) or 0)
    except Exception as exc:
        if required:
            fail(results, system_id, f"query execution failed: {exc}")
        else:
            skip_result(results, system_id, f"optional query failed: {exc}")
        return

    min_value = system_cfg.get("min_value")
    max_value = system_cfg.get("max_value")
    if min_value is not None and value < int(min_value):
        fail(results, system_id, f"query value {value} < required min {int(min_value)}")
        return
    if max_value is not None and value > int(max_value):
        fail(results, system_id, f"query value {value} > required max {int(max_value)}")
        return

    pass_result(
        results,
        system_id,
        {
            "mode": "query",
            "scope": scope,
            "required": required,
            "value": value,
            "min_value": int(min_value) if min_value is not None else None,
            "max_value": int(max_value) if max_value is not None else None,
            "label": str(system_cfg.get("label") or ""),
        },
    )


def check_system(
    core: duckdb.DuckDBPyConnection,
    arm: duckdb.DuckDBPyConnection,
    system_cfg: dict[str, Any],
    results: dict[str, Any],
) -> None:
    system_id = str(system_cfg.get("id") or "unknown")
    if not bool(system_cfg.get("enabled", True)):
        skip_result(results, system_id, "disabled fixture entry")
        return
    mode = normalize_label(system_cfg.get("mode") or "hierarchy")
    if mode == "presence":
        check_presence_mode(core, system_cfg, results)
        return
    if mode == "query":
        check_query_mode(core, arm, system_cfg, results)
        return

    expected_components = [normalize_label(v) for v in system_cfg.get("expected_stellar_components", [])]
    expected_pairs = system_cfg.get("expected_inner_binary_pairs", [])
    name_aliases = [str(v) for v in (system_cfg.get("name_aliases") or [])]
    expected_count = int(system_cfg.get("expected_stellar_component_count", len(expected_components)))
    wds_id = str(system_cfg.get("wds_id") or "").strip()
    stable_key_scope = f"%wds:{wds_id}:%" if wds_id else None
    min_tier = normalize_label(system_cfg.get("minimum_confidence_tier", "medium"))
    min_rank = TIER_RANK.get(min_tier, 2)

    label_matches: dict[str, list[tuple[str, str, str]]] = {}
    component_keys: dict[str, str] = {}
    missing_labels: list[str] = []
    for label in expected_components:
        rows = find_component_rows(
            arm,
            label,
            component_type="star",
            stable_key_scope=stable_key_scope,
        )
        label_matches[label] = rows
        if not rows:
            missing_labels.append(label)
            continue
        chosen = pick_component_row(rows, name_aliases)
        if chosen is None:
            missing_labels.append(label)
            continue
        component_keys[label] = chosen[0]

    if missing_labels:
        fail(results, system_id, f"Missing expected components: {', '.join(sorted(missing_labels))}")
        return

    if len(component_keys) != expected_count:
        fail(
            results,
            system_id,
            f"Expected {expected_count} stellar components, matched {len(component_keys)}",
        )
        return

    pair_failures: list[str] = []
    for pair in expected_pairs:
        if not isinstance(pair, list) or len(pair) != 2:
            pair_failures.append(f"invalid fixture pair: {pair!r}")
            continue
        a = normalize_label(pair[0])
        b = normalize_label(pair[1])
        if a not in component_keys or b not in component_keys:
            pair_failures.append(f"{a}-{b}: missing component key")
            continue
        count = arm.execute(
            """
            select count(*)
            from orbit_edges
            where
              (primary_component_key = ? and secondary_component_key = ?)
              or
              (primary_component_key = ? and secondary_component_key = ?)
            """,
            [component_keys[a], component_keys[b], component_keys[b], component_keys[a]],
        ).fetchone()[0]
        if int(count or 0) < 1:
            pair_failures.append(f"{a}-{b}: orbit edge missing")

    if pair_failures:
        fail(results, system_id, "Pair checks failed: " + "; ".join(pair_failures))
        return

    containment_failures = check_containment_edges(arm, system_cfg)
    if containment_failures:
        fail(results, system_id, "Containment checks failed: " + "; ".join(containment_failures))
        return

    period_failures = check_orbital_solution_periods(arm, system_cfg)
    if period_failures:
        fail(results, system_id, "Orbital solution checks failed: " + "; ".join(period_failures))
        return

    key_values = list(component_keys.values())
    edge_tiers = arm.execute(
        """
        select lower(coalesce(confidence_tier, 'low')) as tier
        from system_hierarchy_edges
        where parent_component_key in (select unnest(?))
           or child_component_key in (select unnest(?))
        union all
        select lower(coalesce(confidence_tier, 'low')) as tier
        from orbit_edges
        where primary_component_key in (select unnest(?))
           or secondary_component_key in (select unnest(?))
        """,
        [key_values, key_values, key_values, key_values],
    ).fetchall()
    ranks = [TIER_RANK.get(normalize_label(r[0]), 1) for r in edge_tiers]
    if not ranks:
        fail(results, system_id, "No confidence tiers found on hierarchy/orbit edges")
        return
    if min(ranks) < min_rank:
        fail(
            results,
            system_id,
            f"Confidence floor violation: minimum tier rank {min(ranks)} < required {min_rank}",
        )
        return

    missing_provenance = arm.execute(
        """
        select count(*) from (
          select source_catalog, source_pk from system_hierarchy_edges
          where parent_component_key in (select unnest(?))
             or child_component_key in (select unnest(?))
          union all
          select source_catalog, source_pk from orbit_edges
          where primary_component_key in (select unnest(?))
             or secondary_component_key in (select unnest(?))
        ) t
        where source_catalog is null or trim(source_catalog) = ''
           or source_pk is null or trim(source_pk) = ''
        """,
        [key_values, key_values, key_values, key_values],
    ).fetchone()[0]
    if int(missing_provenance or 0) > 0:
        fail(results, system_id, f"Missing provenance on {missing_provenance} edge rows")
        return

    pass_result(
        results,
        system_id,
        {
            "mode": "hierarchy",
            "scope": str(system_cfg.get("scope") or "core"),
            "component_count": len(component_keys),
            "pairs_checked": len(expected_pairs),
            "containment_edges_checked": len(system_cfg.get("expected_containment_edges") or []),
            "orbital_periods_checked": len(system_cfg.get("expected_msc_orbital_periods_days") or []),
            "minimum_tier_rank": min(ranks),
        },
    )


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    default_core, default_arm = resolve_default_paths(root)

    parser = argparse.ArgumentParser(description="Validate multiplicity golden-system expectations.")
    parser.add_argument(
        "--fixture",
        default=str(root / "scripts" / "fixtures" / "multiplicity_goldens.json"),
        help="Path to golden-system fixture JSON.",
    )
    parser.add_argument("--core-db", default=str(default_core), help="Path to core.duckdb (metadata context).")
    parser.add_argument("--arm-db", default=str(default_arm), help="Path to arm.duckdb.")
    parser.add_argument(
        "--require-arm",
        action="store_true",
        help="Fail if arm DB/tables are missing.",
    )
    args = parser.parse_args()

    fixture_path = Path(args.fixture)
    if not fixture_path.exists():
        raise SystemExit(f"Fixture not found: {fixture_path}")
    fixture = json.loads(fixture_path.read_text())
    systems = fixture.get("systems") or []
    if not systems:
        raise SystemExit("Fixture has no systems.")

    core_db = Path(args.core_db)
    if not core_db.exists():
        raise SystemExit(f"Core DB not found: {core_db}")

    arm_db = Path(args.arm_db)
    results: dict[str, Any] = {
        "status": "pass",
        "core_db": str(core_db),
        "arm_db": str(arm_db),
        "fixture": str(fixture_path),
        "systems": [],
    }

    core = duckdb.connect(str(core_db), read_only=True)
    hierarchy_required = any(
        bool(cfg.get("enabled", True))
        and normalize_label(cfg.get("mode") or "hierarchy") == "hierarchy"
        for cfg in systems
    )

    if not arm_db.exists():
        if args.require_arm and hierarchy_required:
            raise SystemExit(f"Arm DB not found: {arm_db}")
        arm = None
    else:
        arm = duckdb.connect(str(arm_db), read_only=True)

    if arm is not None:
        required_tables = ["component_entities", "system_hierarchy_edges", "orbit_edges"]
        missing_tables = [t for t in required_tables if not table_exists(arm, t)]
        if missing_tables:
            if args.require_arm and hierarchy_required:
                raise SystemExit(f"Arm DB missing required tables: {', '.join(missing_tables)}")
            arm.close()
            arm = None

    for system_cfg in systems:
        mode = normalize_label(system_cfg.get("mode") or "hierarchy")
        if mode == "hierarchy" and arm is None:
            system_id = str(system_cfg.get("id") or "unknown")
            if bool(system_cfg.get("required", True)):
                fail(results, system_id, "Hierarchy check requested but arm DB/tables unavailable")
            else:
                skip_result(results, system_id, "Hierarchy check skipped (arm DB/tables unavailable)")
            continue
        check_system(core, arm, system_cfg, results)

    failures = [s for s in results["systems"] if s.get("status") == "fail"]
    if failures:
        results["status"] = "fail"
    else:
        skips = [s for s in results["systems"] if s.get("status") == "skipped"]
        if skips:
            results["status"] = "pass_with_skips"

    print(json.dumps(results, indent=2, sort_keys=True))
    core.close()
    if arm is not None:
        arm.close()
    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
