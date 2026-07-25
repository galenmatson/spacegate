from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "srv" / "api"))

import build_public_read_models as builder  # noqa: E402
from app import public_read  # noqa: E402
from app import main as api_main  # noqa: E402


def make_projection(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    builder.create_schema(con)
    con.executemany(
        "INSERT INTO metadata(key,value) VALUES (?,?)",
        [
            ("build_id", "test-build"),
            ("projection_schema_version", public_read.EXPECTED_PROJECTION_SCHEMA),
            ("search_schema_version", public_read.EXPECTED_SEARCH_SCHEMA),
        ],
    )
    system_row = [
        1,
        "canon:system:test",
        "Alpha Test",
        "alpha test",
        None,
        "singleton",
        1.0,
        "singleton",
        "[]",
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        1,
        5700.0,
        5700.0,
        '["G"]',
        16,
        0,
        0,
        10.0,
        20.0,
        12.0,
        1.0,
        2.0,
        3.0,
        "123",
        "4",
        "5",
        10,
        20.0,
        0,
        0,
        "G",
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        "test",
        "v1",
        "1",
        "hash",
        "compiler",
        "singleton_seed",
        "singleton_seed",
    ]
    con.execute(
        "INSERT INTO systems VALUES (" + ",".join("?" for _ in system_row) + ")",
        system_row,
    )
    star_row = [
        11,
        1,
        "canon:star:test",
        "Alpha Test A",
        "alpha test a",
        "A",
        10.0,
        20.0,
        12.0,
        "G2 V",
        "G",
        "2",
        "V",
        None,
        5.0,
        "123",
        "4",
        "5",
        None,
        "stellar",
        "star",
        "G",
        "source",
        "selected_spectral_type",
        "fact-class",
        0.99,
        5772.0,
        5700.0,
        5800.0,
        "fact-teff",
        1.0,
        "fact-radius",
        1.0,
        "fact-mass",
        1.0,
        "fact-lum",
        "source_selected",
        "selected_fact",
        "evidence-lake",
        "test",
        "v1",
        "star-hash",
        "compiler",
    ]
    con.execute(
        "INSERT INTO stars VALUES (" + ",".join("?" for _ in star_row) + ")",
        star_row,
    )
    con.executemany(
        "INSERT INTO search_terms VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, 1, "system", 1, None, None, "Alpha Test", "alpha test", "name", 1, 1, "test", "v1"),
            (2, 1, "star", 11, 11, None, "HIP 4", "hip 4", "identifier", 2, 0, "test", "v1"),
        ],
    )
    con.execute(
        "INSERT INTO identifier_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("accepted-tic-123", "tic", "tic 123", "accepted", "accepted_binding", 1, 11, "canon:star:test", "tic", "v1", "{}"),
    )
    con.execute(
        "INSERT INTO identifier_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("deferred-tic-999", "tic", "tic 999", "deferred", "ambiguous_component_scope", None, None, None, "tic", "v1", "{}"),
    )
    con.execute(
        """
        INSERT INTO singleton_scene_seeds
        SELECT s.system_id,s.stable_object_key,s.system_name,st.star_id,
               st.stable_object_key,st.star_name,st.selected_classification,
               st.classification_status,st.classification_fact_id,st.teff_k,
               st.teff_k_fact_id,st.radius_rsun,st.radius_rsun_fact_id,
               st.mass_msun,st.mass_msun_fact_id,st.luminosity_lsun,
               st.luminosity_lsun_fact_id,st.luminosity_status,
               st.luminosity_basis,'seed-v1','render-v1','hz-v1'
        FROM systems s JOIN stars st USING(system_id)
        """
    )
    builder.create_indexes(con)
    return con


def search(con: sqlite3.Connection, query: str, **overrides):
    values = {
        "q_norm": query,
        "system_id_exact": None,
        "identifier_namespace": None,
        "identifier_norm": None,
        "max_dist_ly": None,
        "min_dist_ly": None,
        "origin": None,
        "min_star_count": None,
        "max_star_count": None,
        "min_planet_count": None,
        "max_planet_count": None,
        "min_temp_k": None,
        "max_temp_k": None,
        "spectral_mask": 0,
        "has_planets": None,
        "has_habitable": None,
        "planet_category_mask": 0,
        "min_coolness_score": None,
        "max_coolness_score": None,
        "sort": "match",
        "limit": 10,
        "include_total": True,
        "name_style": "public_full",
    }
    values.update(overrides)
    return public_read.search_systems(con, **values)


def test_search_v2_exact_prefix_substring_and_bounded_typo(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    assert search(con, "alpha test")[0][0]["match_resolution"] == "exact"
    assert search(con, "alpha")[0][0]["match_resolution"] == "prefix"
    assert search(con, "pha tes")[0][0]["match_resolution"] == "substring"
    typo = search(con, "alpha tesr")[0]
    assert typo and typo[0]["match_resolution"] == "fuzzy"
    con.close()


def test_search_v2_direct_system_identity(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    rows, total, resolution = search(
        con,
        "system 1",
        system_id_exact=1,
    )
    assert [row["system_id"] for row in rows] == [1]
    assert rows[0]["match_rank"] == 0
    assert total == 1
    assert resolution is None
    con.close()


def test_identifier_outcomes_never_fall_through_to_fuzzy_results(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    accepted, _, resolution = search(
        con,
        "tic 123",
        identifier_namespace="tic",
        identifier_norm="tic 123",
    )
    assert [row["system_id"] for row in accepted] == [1]
    assert resolution["match_status"] == "exact_match"

    deferred, count, resolution = search(
        con,
        "tic 999",
        identifier_namespace="tic",
        identifier_norm="tic 999",
    )
    assert deferred == []
    assert count == 0
    assert resolution["match_status"] == "exact_no_match"
    assert resolution["reason"] == "ambiguous_component_scope"
    con.close()


def test_summary_and_singleton_seed_retain_selected_fact_lineage(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    summary = public_read.system_summary(con, 1)
    seed = public_read.singleton_scene_seed(con, 1)
    stars, planets = public_read.system_objects(con, 1)
    assert summary["stellar_class_badges"] == ["G"]
    assert seed["teff_k_fact_id"] == "fact-teff"
    assert seed["luminosity_lsun_fact_id"] == "fact-lum"
    assert stars[0]["arm_evidence"]["selected_parameters"]["mass_msun_fact_id"] == "fact-mass"
    assert planets == []
    con.close()


def test_runtime_rejects_sample_or_build_mismatched_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "public_read.sqlite"
    con = make_projection(artifact)
    con.close()
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "build_id": "test-build",
                "projection_schema_version": public_read.EXPECTED_PROJECTION_SCHEMA,
                "search_schema_version": public_read.EXPECTED_SEARCH_SCHEMA,
                "sample_limit": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SPACEGATE_PUBLIC_READ_PATH", str(artifact))
    monkeypatch.setattr(public_read.db, "build_id", lambda: "test-build")
    with pytest.raises(public_read.PublicReadIncompatible, match="sample"):
        public_read.connect()


def test_singleton_scene_uses_projected_selected_values_without_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "public_read.sqlite"
    con = make_projection(artifact)
    con.close()
    monkeypatch.setattr(
        api_main.public_read,
        "connect",
        lambda _build_id=None: public_read._connect_path(artifact),
    )
    scene = api_main._projected_singleton_simulation_scene(
        1,
        build_id="test-build",
        name_style="public_full",
    )
    assert scene["scene_tier"] == "singleton_seed"
    assert scene["scene_seed"]["luminosity_lsun_fact_id"] == "fact-lum"
    assert scene["render_scene"]["bodies"]["stars"]
