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


def pass_result(results: dict[str, Any], system_id: str, details: dict[str, Any]) -> None:
    payload = {"id": system_id, "status": "pass"}
    payload.update(details)
    results["systems"].append(payload)


def find_component_rows(
    arm: duckdb.DuckDBPyConnection,
    label_token: str,
) -> list[tuple[str, str, str]]:
    token = normalize_label(label_token)
    rows = arm.execute(
        """
        select
          stable_component_key,
          coalesce(display_name, '') as display_name,
          lower(coalesce(catalog_component_label, '')) as component_label
        from component_entities
        where lower(coalesce(catalog_component_label, '')) = ?
           or lower(coalesce(display_name, '')) like ?
        """,
        [token, f"% {token}%"],
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


def check_system(
    arm: duckdb.DuckDBPyConnection,
    system_cfg: dict[str, Any],
    results: dict[str, Any],
) -> None:
    system_id = str(system_cfg.get("id") or "unknown")
    expected_components = [normalize_label(v) for v in system_cfg.get("expected_stellar_components", [])]
    expected_pairs = system_cfg.get("expected_inner_binary_pairs", [])
    name_aliases = [str(v) for v in (system_cfg.get("name_aliases") or [])]
    expected_count = int(system_cfg.get("expected_stellar_component_count", len(expected_components)))
    min_tier = normalize_label(system_cfg.get("minimum_confidence_tier", "medium"))
    min_rank = TIER_RANK.get(min_tier, 2)

    label_matches: dict[str, list[tuple[str, str, str]]] = {}
    component_keys: dict[str, str] = {}
    missing_labels: list[str] = []
    for label in expected_components:
        rows = find_component_rows(arm, label)
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
            "component_count": len(component_keys),
            "pairs_checked": len(expected_pairs),
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

    if not arm_db.exists():
        if args.require_arm:
            raise SystemExit(f"Arm DB not found: {arm_db}")
        results["status"] = "skipped"
        results["reason"] = "arm DB missing"
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    arm = duckdb.connect(str(arm_db), read_only=True)
    required_tables = ["component_entities", "system_hierarchy_edges", "orbit_edges"]
    missing_tables = [t for t in required_tables if not table_exists(arm, t)]
    if missing_tables:
        if args.require_arm:
            raise SystemExit(f"Arm DB missing required tables: {', '.join(missing_tables)}")
        results["status"] = "skipped"
        results["reason"] = f"missing arm tables: {', '.join(missing_tables)}"
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    for system_cfg in systems:
        check_system(arm, system_cfg, results)

    failures = [s for s in results["systems"] if s.get("status") != "pass"]
    if failures:
        results["status"] = "fail"

    print(json.dumps(results, indent=2, sort_keys=True))
    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
