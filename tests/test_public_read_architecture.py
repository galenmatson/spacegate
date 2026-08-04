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
import materialize_public_read_bundles as bundle_materializer  # noqa: E402
from app import public_read  # noqa: E402
from app import main as api_main  # noqa: E402


def test_hierarchy_bundle_reuse_requires_matching_verified_lineage() -> None:
    logical = {
        "systems": "systems-hash",
        "stars": "stars-hash",
        "stellar_badge_overlays": "overlays-hash",
    }
    current = {
        "projection_schema_version": "spacegate.public_read.v2",
        "logical_hashes": logical,
        "source_artifacts": {"arm": {"sha256": "arm-hash"}},
    }
    source = {
        **current,
        "build_id": "source-build",
        "status": "pass",
        "artifact": {"hash_status": "verified"},
    }
    bundle_materializer.validate_reuse_manifests(
        current=current,
        source=source,
        expected_source_build_id="source-build",
    )
    mismatched = {**source, "logical_hashes": {**logical, "stars": "different"}}
    with pytest.raises(RuntimeError, match="stars"):
        bundle_materializer.validate_reuse_manifests(
            current=current,
            source=mismatched,
            expected_source_build_id="source-build",
        )
    arm_rewritten = {
        **current,
        "source_artifacts": {"arm": {"sha256": "metadata-rewritten-arm"}},
    }
    bundle_materializer.validate_reuse_manifests(
        current=arm_rewritten,
        source=source,
        expected_source_build_id="source-build",
        allow_arm_metadata_rewrite=True,
    )


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
            (
                "stellar_badge_overlay_schema_version",
                public_read.EXPECTED_STELLAR_BADGE_OVERLAY_SCHEMA,
            ),
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
    builder.insert_singleton_seeds(
        con,
        {
            "singleton_scene_seed_version": "seed-v1",
            "render_policy_version": "render-v1",
            "habitable_zone_policy_version": "hz-v1",
        },
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


def attach_tag_fixture(con: sqlite3.Connection) -> None:
    first = dict(con.execute("SELECT * FROM systems WHERE system_id=1").fetchone())
    first.update(
        {
            "system_id": 2,
            "stable_object_key": "canon:system:beta",
            "system_name": "Beta Test",
            "system_name_norm": "beta test",
            "dist_ly": 30.0,
            "coolness_rank": 20,
        }
    )
    columns = list(first)
    con.execute(
        f"INSERT INTO systems({','.join(columns)}) VALUES ("
        + ",".join("?" for _ in columns)
        + ")",
        [first[column] for column in columns],
    )
    con.execute("ATTACH DATABASE ':memory:' AS smart_tags")
    con.executescript(
        """
        CREATE TABLE smart_tags.tag_definitions(
          tag_id INTEGER PRIMARY KEY,
          tag_key TEXT UNIQUE,
          label TEXT,name TEXT,category TEXT,kind TEXT,layer TEXT,
          target_types_json TEXT,visual_token TEXT,
          compact_priority INTEGER,normal_priority INTEGER,
          expanded_priority INTEGER,concept_slug TEXT,tooltip TEXT,
          short_tooltip TEXT,source_policy TEXT,evaluator_id TEXT,
          evaluator_version INTEGER,evaluator_params_json TEXT,
          filterable INTEGER,rollup TEXT
        );
        CREATE TABLE smart_tags.system_tag_membership(
          system_id INTEGER,
          tag_id INTEGER,
          member_count INTEGER,
          scope_code INTEGER,
          basis_code INTEGER,
          evidence_status_mask INTEGER,
          min_confidence REAL,
          max_confidence REAL,
          PRIMARY KEY(system_id,tag_id)
        );
        CREATE TABLE smart_tags.source_definitions(
          source_num INTEGER PRIMARY KEY,source_key TEXT,source_id TEXT,
          release_id TEXT,public_name TEXT,short_name TEXT,publisher TEXT,description TEXT,
          mission_instrument TEXT,citation_url TEXT,license_name TEXT,
          license_url TEXT,authority_roles_json TEXT
        );
        CREATE TABLE smart_tags.system_sources(
          system_id INTEGER,source_num INTEGER,contribution_kind TEXT,
          member_count INTEGER
        );
        INSERT INTO smart_tags.tag_definitions VALUES
          (1,'science:test.alpha','Alpha','Alpha','test','concept','disc',
           '["system"]','test',1,1,1,NULL,'Alpha','Alpha','test',
           'system_count_v1',1,'{}',1,'direct'),
          (2,'science:test.beta','Beta','Beta','test','concept','disc',
           '["system"]','test',1,1,1,NULL,'Beta','Beta','test',
           'system_count_v1',1,'{}',1,'direct'),
          (3,'evidence:test.nonfilterable','Evidence','Evidence','evidence',
           'status','arm','["system"]','evidence',1,1,1,NULL,'Evidence',
           'Evidence','status','system_count_v1',1,'{}',0,'none');
        INSERT INTO smart_tags.system_tag_membership VALUES
          (1,1,1,0,0,1,1.0,1.0),
          (1,2,1,0,0,2,0.9,0.9),
          (2,1,1,0,0,1,1.0,1.0);
        """
    )


def test_search_v2_exact_prefix_substring_and_bounded_typo(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    assert search(con, "alpha test")[0][0]["match_resolution"] == "exact"
    assert search(con, "alpha")[0][0]["match_resolution"] == "prefix"
    assert search(con, "pha tes")[0][0]["match_resolution"] == "substring"
    typo = search(con, "alpha tesr")[0]
    assert typo and typo[0]["match_resolution"] == "fuzzy"
    con.close()


def test_search_v2_catalog_like_names_do_not_fall_through_to_fuzzy(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    con.execute(
        "INSERT INTO search_terms VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (3, 1, "system", 1, None, None, "V1581 Cyg", "v1581 cyg", "name", 1, 1, "test", "v1"),
    )
    rows, count, _ = search(con, "v1513 cyg")
    assert rows == []
    assert count == 0
    con.close()


def test_search_v2_tag_any_all_exclude_and_filter_combinations(
    tmp_path: Path,
) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    attach_tag_fixture(con)
    rows, _, _ = search(
        con,
        "",
        tags_any=["science:test.beta"],
        max_dist_ly=20,
        sort="name",
    )
    assert [row["system_id"] for row in rows] == [1]
    rows, _, _ = search(
        con,
        "",
        tags_all=["science:test.alpha", "science:test.beta"],
        sort="name",
    )
    assert [row["system_id"] for row in rows] == [1]
    rows, _, _ = search(
        con,
        "",
        tags_exclude=["science:test.beta"],
        sort="name",
    )
    assert [row["system_id"] for row in rows] == [2]
    first_page, _, _ = search(
        con,
        "",
        tags_any=["science:test.alpha"],
        sort="name",
        limit=1,
    )
    assert [row["system_id"] for row in first_page] == [1]
    second_page, _, _ = search(
        con,
        "",
        tags_any=["science:test.alpha"],
        sort="name",
        limit=1,
        cursor_values={"name": "alpha test", "id": 1},
    )
    assert [row["system_id"] for row in second_page] == [2]
    with pytest.raises(ValueError, match="unknown or non-filterable"):
        search(
            con,
            "",
            tags_any=["evidence:test.nonfilterable"],
            sort="name",
        )
    con.close()


def test_tag_filter_parser_is_bounded_normalized_and_fail_closed() -> None:
    assert api_main._parse_tag_filter(
        "Science:System.Binary, science:system.binary"
    ) == ["science:system.binary"]
    with pytest.raises(api_main.HTTPException) as malformed:
        api_main._parse_tag_filter("science:system.binary;drop")
    assert malformed.value.status_code == 400
    with pytest.raises(api_main.HTTPException) as excessive:
        api_main._parse_tag_filter(
            ",".join(f"science:test.{index}" for index in range(33))
        )
    assert excessive.value.status_code == 400


def test_root_naming_uses_system_alias_scope_not_member_proper_name(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    con.executemany(
        "INSERT INTO aliases VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, 1, 11, "star", 11, "canon:star:test", "Ran", "ran", "proper_name", 1, 0, "test", "v1"),
            (2, 1, None, "system", 1, "canon:system:test", "Epsilon Eridani", "epsilon eridani", "member_bayer_expanded_name", 22, 0, "test", "v1"),
        ],
    )
    naming = public_read.system_naming_aliases(con, [1])[1]
    display = public_read.choose_display_name_info(
        "eps Eri",
        naming,
        preferred_query_norm="epsilon eridani",
        root_system=True,
    )
    assert display["display_name"] == "Epsilon Eridani"
    con.close()


def test_stellar_badge_overlay_supersedes_core_badges(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    con.executemany(
        "INSERT INTO stellar_badge_overlays VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, 0, "leaf:a", "comp:a", "comp:a", None, "leaf:a", "Alpha Test A", "a", "G", "source", "direct", "fact-g", "test", "v1"),
            (1, 1, "leaf:b", "comp:b", "comp:b", None, "leaf:b", "Alpha Test B", "b", "M", "assumed", "mass prior", "fact-m", "test", "v1"),
        ],
    )
    badges = public_read.stellar_badges_for_systems(con, [1])[1]
    assert [row["classification_value"] for row in badges] == ["G", "M"]
    assert [row["catalog_component_label"] for row in badges] == ["a", "b"]
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
    assert resolution["resolution_status"] == "accepted"
    assert resolution["deferred"] is False
    assert resolution["evidence_record_count"] == 1

    deferred, count, resolution = search(
        con,
        "tic 999",
        identifier_namespace="tic",
        identifier_norm="tic 999",
    )
    assert deferred == []
    assert count == 0
    assert resolution["match_status"] == "exact_no_match"
    assert resolution["resolution_status"] == "deferred"
    assert resolution["deferred"] is True
    assert resolution["reason"] == "ambiguous_component_scope"

    unknown, count, resolution = search(
        con,
        "tic 1000",
        identifier_namespace="tic",
        identifier_norm="tic 1000",
    )
    assert unknown == []
    assert count == 0
    assert resolution["match_status"] == "exact_no_match"
    assert resolution["resolution_status"] == "not_found"
    assert resolution["deferred"] is False
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
    assert public_read.preview_policy(summary) == {
        "preview_tier": "lightweight_singleton",
        "preview_basis": ["public_read:singleton_seed"],
        "is_lightweight_preview_safe": True,
        "has_prebuilt_simulation_scene": False,
    }
    con.close()


def test_planet_projection_exposes_quantity_fact_lineage(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    con.execute(
        """
        INSERT INTO planets(
          planet_id,system_id,stable_object_key,planet_name,
          orbital_period_days,selected_fact_lineage_json
        ) VALUES (?,?,?,?,?,?)
        """,
        [
            21,
            1,
            "canon:planet:test-b",
            "Alpha Test b",
            12.5,
            json.dumps(
                {
                    "lineage_version": "spacegate.planet_selected_fact_lineage.v1",
                    "orbital_period_days": {
                        "lower": 12.4,
                        "upper": 12.6,
                        "fact_id": "fact-period",
                    },
                }
            ),
        ],
    )
    _, planets = public_read.system_objects(con, 1)
    assert planets[0]["orbital_period_days"] == 12.5
    assert planets[0]["selected_fact_lineage"]["orbital_period_days"] == {
        "lower": 12.4,
        "upper": 12.6,
        "fact_id": "fact-period",
    }
    con.close()


def test_projection_preview_policy_rejects_unknown_representation() -> None:
    with pytest.raises(public_read.PublicReadIncompatible):
        public_read.preview_policy({"scene_representation": "legacy_guess"})


def test_singleton_seed_view_is_coverage_preserving_and_indexed(tmp_path: Path) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    seed = con.execute(
        "SELECT system_id,star_id,seed_version FROM singleton_scene_seeds"
    ).fetchone()
    assert tuple(seed) == (1, 11, "seed-v1")
    plan = " ".join(
        str(row[3])
        for row in con.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM singleton_scene_seeds WHERE system_id=?",
            [1],
        )
    )
    assert "INTEGER PRIMARY KEY" in plan
    storage = bundle_materializer.compact_singleton_seed_storage(
        con,
        policy={
            "singleton_scene_seed_version": "seed-v2",
            "render_policy_version": "render-v2",
            "habitable_zone_policy_version": "hz-v2",
        },
    )
    assert storage == {
        "coolness_sort_index": "systems_coolness_sort_idx",
        "converted_from_table": False,
        "rows": 1,
        "storage": "indexed_system_star_view",
    }
    assert con.execute(
        "SELECT seed_version FROM singleton_scene_seeds WHERE system_id=1"
    ).fetchone()[0] == "seed-v2"
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
                "stellar_badge_overlay_schema_version": (
                    public_read.EXPECTED_STELLAR_BADGE_OVERLAY_SCHEMA
                ),
                "sample_limit": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SPACEGATE_PUBLIC_READ_PATH", str(artifact))
    monkeypatch.setattr(public_read.db, "build_id", lambda: "test-build")
    with pytest.raises(public_read.PublicReadIncompatible, match="sample"):
        public_read.connect()


def test_legacy_compatibility_fallback_requires_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(
        "SPACEGATE_PUBLIC_READ_COMPATIBILITY_FALLBACK",
        raising=False,
    )
    assert public_read.compatibility_fallback_enabled() is False
    monkeypatch.setenv("SPACEGATE_PUBLIC_READ_COMPATIBILITY_FALLBACK", "true")
    assert public_read.compatibility_fallback_enabled() is True


def test_missing_projection_fails_detail_visibly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args, **_kwargs):
        raise public_read.PublicReadUnavailable("projection missing")

    monkeypatch.delenv(
        "SPACEGATE_PUBLIC_READ_COMPATIBILITY_FALLBACK",
        raising=False,
    )
    monkeypatch.setattr(public_read, "connect", unavailable)
    with pytest.raises(api_main.HTTPException) as error:
        api_main.system_detail(1)
    assert error.value.status_code == 503
    assert error.value.detail["code"] == "public_read_unavailable"


def test_missing_required_hierarchy_bundle_fails_visible(
    tmp_path: Path,
) -> None:
    con = make_projection(tmp_path / "read.sqlite")
    con.execute(
        "UPDATE systems SET hierarchy_representation='bundle_required' WHERE system_id=1"
    )
    con.commit()
    with pytest.raises(
        public_read.PublicReadIncompatible,
        match="required hierarchy bundle",
    ):
        public_read.projected_system_detail(con, 1)
    con.close()


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


def test_projected_singleton_detail_does_not_probe_disc_narratives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = tmp_path / "public_read.sqlite"
    con = make_projection(artifact)
    con.close()
    monkeypatch.setattr(
        api_main.public_read,
        "connect",
        lambda _build_id=None: public_read._connect_path(artifact),
    )
    monkeypatch.setattr(api_main.db, "build_id", lambda: "test-build")
    monkeypatch.setattr(
        api_main,
        "system_narrative_blocks",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("projected singleton probed DISC narration")
        ),
    )
    detail = api_main.system_detail(1, name_style="public_full")
    assert detail["read_backend"] == "public_read_v2_singleton"
    assert detail["narrative_blocks"]
