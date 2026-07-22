from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_e6_coolness_ab as auditor  # noqa: E402


def make_disc(path: Path, build_id: str, rows: list[tuple[int, int, float, str, int]]) -> None:
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE coolness_scores(
          system_id BIGINT,rank BIGINT,score_total DOUBLE,build_id VARCHAR,
          profile_id VARCHAR,profile_version VARCHAR,dominant_spectral_class VARCHAR,
          nice_planet_count BIGINT,system_name VARCHAR,star_count BIGINT,
          planet_count BIGINT,exotic_star_feature DOUBLE,score_exotic_star DOUBLE
        )
        """
    )
    con.executemany(
        "INSERT INTO coolness_scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                system_id, rank, score, build_id, "profile", "1", stellar_class,
                planets, f"System {system_id}", 1, planets, 0.0, 0.0,
            )
            for system_id, rank, score, stellar_class, planets in rows
        ],
    )
    con.close()


def test_coolness_ab_accepts_reviewable_rank_changes(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate.duckdb"
    reference = tmp_path / "reference.duckdb"
    make_disc(reference, "old", [(1, 1, 9.0, "M", 0), (2, 2, 8.0, "G", 0)])
    make_disc(candidate, "new", [(1, 2, 8.5, "WD", 0), (2, 1, 9.5, "G", 1)])

    report = auditor.audit(
        candidate_disc=candidate,
        reference_disc=reference,
        build_id="new",
        profile_id="profile",
        profile_version="1",
    )

    assert report["status"] == "pass"
    assert report["deltas"]["rank_changed_systems"] == 2
    assert report["deltas"]["dominant_class_changed_systems"] == 1
    assert report["deltas"]["nice_planet_count_changed_systems"] == 1
    assert report["top_candidate"][0]["system_id"] == 2
    assert report["top_candidate"][0]["system_name"] == "System 2"
    assert report["top_reference"][0]["system_id"] == 1
