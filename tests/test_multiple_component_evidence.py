from __future__ import annotations

import csv
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from cook_multiplicity import cook_sb9  # noqa: E402
from verify_multiple_component_evidence import science_table_fingerprint  # noqa: E402


def _put(line: list[str], start: int, end: int, value: str) -> None:
    width = end - start + 1
    text = value[:width].ljust(width)
    line[start - 1 : end] = list(text)


def test_cook_sb9_preserves_component_spectra_and_orbit(tmp_path: Path) -> None:
    main_line = [" "] * 132
    for start, end, value in (
        (1, 4, "462"), (17, 18, "07"), (19, 20, "34"), (21, 25, "36.00"),
        (26, 26, "+"), (27, 28, "31"), (29, 30, "53"), (31, 34, "18.0"),
        (36, 42, "A"), (44, 49, "1.980"), (50, 50, "V"),
        (52, 57, "9.000"), (58, 58, "V"), (60, 91, "A1V"),
        (93, 102, "dM1e"), (104, 132, "HIP 36850"),
    ):
        _put(main_line, start, end, value)

    orbit_line = [" "] * 295
    for start, end, value in (
        (1, 4, "462"), (6, 6, "1"), (8, 23, "9.212749000"),
        (79, 89, "0.499000000"), (124, 133, "13.00000"),
        (148, 157, "100.00000"), (226, 228, "4.0"),
        (230, 280, "2004A&A...424..727P"), (293, 295, "PUB"),
    ):
        _put(orbit_line, start, end, value)

    main = tmp_path / "main.dat"
    aliases = tmp_path / "alias.dat"
    orbits = tmp_path / "orbits.dat"
    main.write_text("".join(main_line) + "\n", encoding="ascii")
    aliases.write_text(" 462 HIP 36850                         \n", encoding="ascii")
    orbits.write_text("".join(orbit_line) + "\n", encoding="ascii")

    out = tmp_path / "cooked"
    counts = cook_sb9(
        main, aliases, orbits,
        out / "sb9_systems.csv", out / "sb9_aliases.csv", out / "sb9_orbits.csv",
    )
    assert counts == (1, 1, 1)

    with (out / "sb9_systems.csv").open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    assert row["sb9_sequence"] == "462"
    assert row["component_label"] == "A"
    assert row["spectral_type_primary"] == "A1V"
    assert row["spectral_type_secondary"] == "dM1e"

    with (out / "sb9_orbits.csv").open(newline="", encoding="utf-8") as handle:
        orbit = next(csv.DictReader(handle))
    assert orbit["period_days"] == "9.212749"
    assert orbit["reference_bibcode"] == "2004A&A...424..727P"


def test_science_fingerprint_excludes_build_metadata_but_not_evidence() -> None:
    first = duckdb.connect(":memory:")
    second = duckdb.connect(":memory:")
    for con, build_id, ingested_at, value in (
        (first, "build-a", "2026-07-15T00:00:00Z", "A1V"),
        (second, "build-b", "2026-07-16T00:00:00Z", "A1V"),
    ):
        con.execute(
            "create table evidence(id bigint, build_id varchar, ingested_at varchar, value varchar)"
        )
        con.execute("insert into evidence values (1, ?, ?, ?)", [build_id, ingested_at, value])

    first_fp = science_table_fingerprint(first, "evidence", "id")
    second_fp = science_table_fingerprint(second, "evidence", "id")
    assert first_fp == second_fp

    second.execute("update evidence set value='M1V'")
    assert science_table_fingerprint(first, "evidence", "id") != science_table_fingerprint(
        second, "evidence", "id"
    )
