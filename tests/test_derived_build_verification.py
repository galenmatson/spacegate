from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "derived_build_verification.py"


def make_build(tmp_path: Path) -> Path:
    build_dir = tmp_path / "derived"
    parquet_dir = build_dir / "parquet"
    parquet_dir.mkdir(parents=True)
    con = duckdb.connect(str(build_dir / "core.duckdb"))
    con.execute("create table systems(system_id bigint, stable_object_key varchar)")
    con.execute("create table stars(star_id bigint, system_id bigint, stable_object_key varchar)")
    con.execute("create table planets(planet_id bigint, star_id bigint, system_id bigint, stable_object_key varchar)")
    con.execute("create table aliases(alias varchar)")
    con.execute("create table system_search_terms(search_term varchar)")
    con.execute("create table build_metadata(key varchar, value varchar)")
    con.execute("insert into systems values (1, 'system:test')")
    con.execute("insert into stars values (2, 1, 'star:test')")
    con.execute("insert into planets values (3, 2, 1, 'planet:test')")
    con.execute("insert into aliases values ('test')")
    con.execute("insert into system_search_terms values ('test')")
    con.execute("insert into build_metadata values ('build_id', 'derived-test')")
    for table in ("systems", "stars", "planets", "aliases", "system_search_terms"):
        con.execute(f"copy {table} to '{parquet_dir / (table + '.parquet')}' (format parquet)")
    con.close()
    return build_dir


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_emit_and_verify_derived_build_report(tmp_path: Path) -> None:
    build_dir = make_build(tmp_path)
    report = tmp_path / "report.json"
    emitted = run_script(
        "emit",
        "--build-dir",
        str(build_dir),
        "--build-id",
        "derived-test",
        "--source-build-id",
        "canonical-test",
        "--report",
        str(report),
    )
    assert emitted.returncode == 0, emitted.stderr
    payload = json.loads(report.read_text())
    assert payload["status"] == "pass"
    assert payload["counts"]["systems"] == 1
    assert all(value == 0 for value in payload["checks"].values())

    verified = run_script(
        "verify",
        "--build-dir",
        str(build_dir),
        "--build-id",
        "derived-test",
        "--report",
        str(report),
    )
    assert verified.returncode == 0, verified.stderr


def test_verify_detects_derived_build_drift(tmp_path: Path) -> None:
    build_dir = make_build(tmp_path)
    report = tmp_path / "report.json"
    assert run_script(
        "emit",
        "--build-dir",
        str(build_dir),
        "--build-id",
        "derived-test",
        "--report",
        str(report),
    ).returncode == 0
    con = duckdb.connect(str(build_dir / "core.duckdb"))
    con.execute("insert into aliases values ('changed')")
    con.close()

    verified = run_script(
        "verify",
        "--build-dir",
        str(build_dir),
        "--build-id",
        "derived-test",
        "--report",
        str(report),
    )
    assert verified.returncode != 0
    assert "mismatch for counts" in verified.stderr
