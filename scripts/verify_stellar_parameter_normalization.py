#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import duckdb

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from srv.api.app.main import _stellar_main_sequence_proxy


DEFAULT_BUILD_DIR = Path("/data/spacegate/state/served/current")


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _unit_checks() -> None:
    k0 = _stellar_main_sequence_proxy({"spectral_type_raw": "K0V", "spectral_class": "K"})
    k4 = _stellar_main_sequence_proxy({"spectral_type_raw": "K4V", "spectral_class": "K"})
    _assert(k0.get("basis") == "spectral_subclass_main_sequence_mass_prior_v1", "K0V should use subclass mass prior")
    _assert(k4.get("basis") == "spectral_subclass_main_sequence_mass_prior_v1", "K4V should use subclass mass prior")
    _assert(float(k0["mass_msun"]) > float(k4["mass_msun"]), f"K0V mass should exceed K4V mass: {k0} vs {k4}")
    _assert(float(k0["teff_k"]) > float(k4["teff_k"]), f"K0V Teff should exceed K4V Teff: {k0} vs {k4}")

    g2 = _stellar_main_sequence_proxy({"spectral_type_raw": "G2V", "spectral_class": "G"})
    _assert(abs(float(g2["mass_msun"]) - 1.0) < 0.03, f"G2V solar mass prior should be near 1 Msun: {g2}")

    giant = _stellar_main_sequence_proxy({"spectral_type_raw": "K0III", "spectral_class": "K"})
    _assert(not giant.get("mass_msun"), f"giants must not receive main-sequence mass priors: {giant}")

    wd = _stellar_main_sequence_proxy({"spectral_type_raw": "DA2", "spectral_class": "D", "object_type": "white_dwarf"})
    _assert(not wd.get("mass_msun"), f"white dwarfs must not receive main-sequence mass priors: {wd}")


def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
    return con.execute(sql, params or []).fetchall()


def _db_checks(build_dir: Path) -> None:
    arm_path = build_dir / "arm.duckdb"
    if not arm_path.exists():
        raise FileNotFoundError(f"arm.duckdb not found: {arm_path}")
    con = duckdb.connect(str(arm_path), read_only=True)
    try:
        msc_rows = _rows(
            con,
            """
            select
              o.source_pk,
              o.period_days,
              o.semi_major_axis_arcsec,
              o.eccentricity,
              o.inclination_deg,
              e.orbit_edge_id,
              count(s.orbital_solution_id) as solution_count
            from msc_orbit_details o
            left join orbit_edges e
              on (
                e.primary_component_key = o.primary_component_key
                and e.secondary_component_key = o.secondary_component_key
              ) or (
                e.primary_component_key = o.secondary_component_key
                and e.secondary_component_key = o.primary_component_key
              )
            left join orbital_solutions s
              on s.orbit_edge_id = e.orbit_edge_id
             and s.source_catalog = 'msc'
            where o.wds_id = '18055+0230'
            group by all
            order by o.source_pk
            """,
        )
        _assert(msc_rows, "70 Oph MSC orbit rows should be preserved")
        _assert(any(row[5] is not None for row in msc_rows), f"70 Oph MSC orbit rows should resolve to ARM orbit_edges: {msc_rows}")
        _assert(any(int(row[6] or 0) > 0 for row in msc_rows), f"70 Oph MSC orbit rows should resolve to ARM orbital_solutions: {msc_rows}")

        masses = _rows(
            con,
            """
            select mass_primary_msun, mass_secondary_msun, spectral_type_primary, spectral_type_secondary
            from msc_system_details
            where wds_id = '18055+0230'
              and primary_label = 'a'
              and secondary_label = 'b'
            """,
        )
        _assert(masses, "70 Oph MSC system details should include endpoint evidence")
        _assert(any(row[0] is not None and row[1] is not None for row in masses), f"70 Oph MSC endpoint masses should be preserved: {masses}")
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify stellar parameter normalization and source-backed MSC orbit use.")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIR)
    parser.add_argument("--skip-db", action="store_true", help="Only run Python prior policy checks.")
    args = parser.parse_args()

    _unit_checks()
    if not args.skip_db:
        _db_checks(args.build_dir)
    print("stellar parameter normalization checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
