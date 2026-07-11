#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_BUILD_DIR = Path("/data/spacegate/state/served/current")


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _rows(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
) -> list[tuple[Any, ...]]:
    return con.execute(sql, params or []).fetchall()


def _find_system(con: duckdb.DuckDBPyConnection, query: str) -> tuple[Any, ...] | None:
    rows = _rows(
        con,
        """
        select
          s.system_id,
          s.system_name,
          st.star_id,
          st.star_name,
          st.source_catalog,
          st.source_version,
          st.gaia_id,
          st.dist_ly,
          st.spectral_class,
          st.spectral_type_raw,
          st.catalog_ids_json
        from system_search_terms t
        join systems s on s.system_id = t.system_id
        join stars st on st.system_id = s.system_id
        where t.term_norm = lower(regexp_replace(regexp_replace(?, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))
        order by t.term_priority asc, st.source_catalog asc, st.star_id asc
        limit 1
        """,
        [query],
    )
    return rows[0] if rows else None


def _check_named_system(
    con: duckdb.DuckDBPyConnection,
    *,
    query: str,
    expected_name_fragment: str,
    expected_source_catalog: str,
    max_dist_ly: float,
    allowed_classes: set[str],
) -> None:
    row = _find_system(con, query)
    _assert(row is not None, f"{query!r} should resolve through system_search_terms")
    (
        _system_id,
        system_name,
        _star_id,
        star_name,
        source_catalog,
        _source_version,
        _gaia_id,
        dist_ly,
        spectral_class,
        spectral_type_raw,
        catalog_ids_json,
    ) = row
    name_text = f"{system_name or ''} {star_name or ''}".lower()
    _assert(
        expected_name_fragment.lower() in name_text,
        f"{query!r} resolved to unexpected name system={system_name!r} star={star_name!r}",
    )
    _assert(
        source_catalog == expected_source_catalog,
        f"{query!r} source_catalog should be {expected_source_catalog!r}, got {source_catalog!r}",
    )
    _assert(dist_ly is not None and float(dist_ly) <= max_dist_ly, f"{query!r} distance is not nearby: {dist_ly!r}")
    _assert(
        spectral_class in allowed_classes,
        f"{query!r} should carry ultracool spectral class {allowed_classes}, got class={spectral_class!r} raw={spectral_type_raw!r}",
    )
    _assert(
        catalog_ids_json and "ultracoolsheet" in str(catalog_ids_json).lower(),
        f"{query!r} catalog_ids_json should preserve UltracoolSheet metadata: {catalog_ids_json!r}",
    )


def _check_report(build_dir: Path) -> None:
    report_path = build_dir.parent.parent / "reports" / build_dir.name / "nearby_ultracool_inventory_report.json"
    if not report_path.exists():
        raise FileNotFoundError(f"nearby ultracool report not found: {report_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify nearby UltracoolSheet inventory promotion.")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIR)
    parser.add_argument("--skip-report", action="store_true")
    args = parser.parse_args()

    core_path = args.build_dir / "core.duckdb"
    if not core_path.exists():
        raise FileNotFoundError(f"core.duckdb not found: {core_path}")
    con = duckdb.connect(str(core_path), read_only=True)
    try:
        _check_named_system(
            con,
            query="Luhman 16",
            expected_name_fragment="Luhman 16",
            expected_source_catalog="ultracoolsheet",
            max_dist_ly=7.0,
            allowed_classes={"L", "T"},
        )
        _check_named_system(
            con,
            query="WISE 0855",
            expected_name_fragment="WISE J0855",
            expected_source_catalog="ultracoolsheet",
            max_dist_ly=8.0,
            allowed_classes={"Y"},
        )
        _check_named_system(
            con,
            query="WISEA J085510.74-071442.5",
            expected_name_fragment="WISE J0855",
            expected_source_catalog="ultracoolsheet",
            max_dist_ly=8.0,
            allowed_classes={"Y"},
        )
        if not args.skip_report:
            _check_report(args.build_dir.resolve())
    finally:
        con.close()

    print("nearby ultracool inventory checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
