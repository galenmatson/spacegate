#!/usr/bin/env python3
"""Audit the focused E5 planet-derivation delta through clean public runtime."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DERIVED_QUANTITIES = (
    "semi_major_axis_au",
    "insol_earth",
    "eq_temp_k",
)
INVENTORY_TABLES = (
    "systems",
    "stars",
    "planets",
    "aliases",
    "system_search_terms",
)
CATEGORY_COLUMNS = (
    "planet_size_mass_class",
    "planet_insolation_class",
    "planet_orbit_class",
    "planet_composition_proxy_class",
    "spacegate_hab_score",
)


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()]


def row_delta(
    con: duckdb.DuckDBPyConnection,
    left: str,
    right: str,
    selected_columns: list[str],
) -> dict[str, int]:
    projection = ",".join(f'"{name}"' for name in selected_columns)
    return {
        "left_only": scalar(
            con,
            f"SELECT count(*) FROM (SELECT {projection} FROM {left} "
            f"EXCEPT SELECT {projection} FROM {right})",
        ),
        "right_only": scalar(
            con,
            f"SELECT count(*) FROM (SELECT {projection} FROM {right} "
            f"EXCEPT SELECT {projection} FROM {left})",
        ),
    }


def audit(
    *,
    reference_public: Path,
    candidate_public: Path,
    reference_science: Path,
    candidate_science: Path,
    candidate_arm: Path,
    derivation_shard: Path,
) -> dict[str, Any]:
    con = duckdb.connect(config={"threads": "8", "memory_limit": "24GB"})
    try:
        con.execute(f"ATTACH {sql_literal(reference_public / 'core.duckdb')} AS ref_core (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(candidate_public / 'core.duckdb')} AS cand_core (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(reference_science)} AS ref_science (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(candidate_science)} AS cand_science (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(candidate_arm)} AS cand_arm (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(derivation_shard)} AS shard (READ_ONLY)")

        inventory = {
            table: {
                "reference": scalar(con, f"SELECT count(*) FROM ref_core.{table}"),
                "candidate": scalar(con, f"SELECT count(*) FROM cand_core.{table}"),
            }
            for table in INVENTORY_TABLES
        }
        quantity_deltas: dict[str, Any] = {}
        for quantity in DERIVED_QUANTITIES:
            quantity_deltas[quantity] = {
                "shard_facts": scalar(
                    con,
                    "SELECT count(*) FROM shard.selected_facts "
                    f"WHERE quantity_key='{quantity}'",
                ),
                "candidate_only_values": scalar(
                    con,
                    f"""
                    SELECT count(*) FROM ref_science.selected_planet_parameters r
                    JOIN cand_science.selected_planet_parameters c USING(planet_id)
                    WHERE r.{quantity} IS NULL AND c.{quantity} IS NOT NULL
                    """,
                ),
                "existing_value_changes": scalar(
                    con,
                    f"""
                    SELECT count(*) FROM ref_science.selected_planet_parameters r
                    JOIN cand_science.selected_planet_parameters c USING(planet_id)
                    WHERE r.{quantity} IS NOT NULL
                      AND c.{quantity} IS DISTINCT FROM r.{quantity}
                    """,
                ),
                "lost_values": scalar(
                    con,
                    f"""
                    SELECT count(*) FROM ref_science.selected_planet_parameters r
                    JOIN cand_science.selected_planet_parameters c USING(planet_id)
                    WHERE r.{quantity} IS NOT NULL AND c.{quantity} IS NULL
                    """,
                ),
                "candidate_only_without_shard_lineage": scalar(
                    con,
                    f"""
                    SELECT count(*) FROM ref_science.selected_planet_parameters r
                    JOIN cand_science.selected_planet_parameters c USING(planet_id)
                    LEFT JOIN shard.selected_facts f
                      ON f.selected_fact_id=c.{quantity}_fact_id
                    WHERE r.{quantity} IS NULL AND c.{quantity} IS NOT NULL
                      AND f.selected_fact_id IS NULL
                    """,
                ),
            }
        planet_columns = columns(con, "cand_science.selected_planet_parameters")
        changed_columns = {
            name
            for quantity in DERIVED_QUANTITIES
            for name in (
                quantity,
                f"{quantity}_lower",
                f"{quantity}_upper",
                f"{quantity}_fact_id",
            )
        }
        unchanged_planet_delta = row_delta(
            con,
            "ref_science.selected_planet_parameters",
            "cand_science.selected_planet_parameters",
            [name for name in planet_columns if name not in changed_columns],
        )
        arm_projection_delta = row_delta(
            con,
            "cand_science.selected_planet_parameters",
            "cand_arm.e6_selected_planet_parameters",
            planet_columns,
        )
        public_value_mismatches = {
            quantity: scalar(
                con,
                f"""
                SELECT count(*) FROM cand_core.planets p
                JOIN cand_arm.e6_selected_planet_parameters s USING(planet_id)
                WHERE p.{quantity} IS DISTINCT FROM s.{quantity}
                """,
            )
            for quantity in DERIVED_QUANTITIES
        }
        category_deltas = {
            column: {
                "existing_changes": scalar(
                    con,
                    f"""
                    SELECT count(*) FROM ref_core.planets r
                    JOIN cand_core.planets c USING(planet_id)
                    WHERE r.{column} IS NOT NULL AND c.{column} IS DISTINCT FROM r.{column}
                    """,
                ),
                "new_values": scalar(
                    con,
                    f"""
                    SELECT count(*) FROM ref_core.planets r
                    JOIN cand_core.planets c USING(planet_id)
                    WHERE r.{column} IS NULL AND c.{column} IS NOT NULL
                    """,
                ),
                "lost_values": scalar(
                    con,
                    f"""
                    SELECT count(*) FROM ref_core.planets r
                    JOIN cand_core.planets c USING(planet_id)
                    WHERE r.{column} IS NOT NULL AND c.{column} IS NULL
                    """,
                ),
            }
            for column in CATEGORY_COLUMNS
        }
        luminosity_lineage = {
            str(status): int(count)
            for status, count in con.execute(
                """
                SELECT luminosity_lsun_status,count(*)
                FROM cand_science.selected_stellar_parameters
                WHERE luminosity_lsun IS NOT NULL
                GROUP BY 1 ORDER BY 1
                """
            ).fetchall()
        }
        checks = {
            "inventory_deltas": sum(
                abs(row["candidate"] - row["reference"]) for row in inventory.values()
            ),
            "planet_subject_delta": abs(
                scalar(con, "SELECT count(*) FROM ref_science.selected_planet_parameters")
                - scalar(con, "SELECT count(*) FROM cand_science.selected_planet_parameters")
            ),
            "unchanged_planet_left_only": unchanged_planet_delta["left_only"],
            "unchanged_planet_right_only": unchanged_planet_delta["right_only"],
            "existing_derived_quantity_changes": sum(
                row["existing_value_changes"] for row in quantity_deltas.values()
            ),
            "lost_derived_quantity_values": sum(
                row["lost_values"] for row in quantity_deltas.values()
            ),
            "derived_count_mismatches": sum(
                row["candidate_only_values"] != row["shard_facts"]
                for row in quantity_deltas.values()
            ),
            "derived_without_shard_lineage": sum(
                row["candidate_only_without_shard_lineage"]
                for row in quantity_deltas.values()
            ),
            "arm_projection_left_only": arm_projection_delta["left_only"],
            "arm_projection_right_only": arm_projection_delta["right_only"],
            "public_selected_value_mismatches": sum(public_value_mismatches.values()),
            "existing_category_changes": sum(
                row["existing_changes"] for row in category_deltas.values()
            ),
            "lost_category_values": sum(
                row["lost_values"] for row in category_deltas.values()
            ),
            "missing_luminosity_status": scalar(
                con,
                """
                SELECT count(*) FROM cand_science.selected_stellar_parameters
                WHERE luminosity_lsun IS NOT NULL AND luminosity_lsun_status IS NULL
                """,
            ),
            "luminosity_derivation_count_mismatch": abs(
                luminosity_lineage.get("derived", 0)
                - scalar(
                    con,
                    "SELECT count(*) FROM cand_science.evidence_fact_selected_fact_derivations "
                    "WHERE quantity_key='luminosity_lsun'",
                )
            ),
        }
        failing = {key: value for key, value in checks.items() if value}
        candidate_build_id = dict(
            con.execute("SELECT key,value FROM cand_core.build_metadata").fetchall()
        ).get("build_id")
        return {
            "schema_version": "spacegate.e7_planet_derivation_cutover_audit.v1",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "reference_build_id": reference_public.name,
            "candidate_build_id": candidate_build_id,
            "inventory": inventory,
            "quantity_deltas": quantity_deltas,
            "unchanged_planet_projection_delta": unchanged_planet_delta,
            "arm_projection_delta": arm_projection_delta,
            "public_value_mismatches": public_value_mismatches,
            "category_deltas": category_deltas,
            "luminosity_lineage": luminosity_lineage,
            "checks": checks,
            "failing_checks": failing,
            "status": "pass" if not failing else "fail",
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference-public", type=Path, required=True)
    parser.add_argument("--candidate-public", type=Path, required=True)
    parser.add_argument("--reference-science", type=Path, required=True)
    parser.add_argument("--candidate-science", type=Path, required=True)
    parser.add_argument("--candidate-arm", type=Path, required=True)
    parser.add_argument("--derivation-shard", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = audit(
        reference_public=args.reference_public.resolve(),
        candidate_public=args.candidate_public.resolve(),
        reference_science=args.reference_science.resolve(),
        candidate_science=args.candidate_science.resolve(),
        candidate_arm=args.candidate_arm.resolve(),
        derivation_shard=args.derivation_shard.resolve(),
    )
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.report.with_name(f".{args.report.name}.{os.getpid()}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, args.report)
    print(rendered, end="")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
