#!/usr/bin/env python3
"""Compare E6 coolness output with the stability reference."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import resource
import time
from typing import Any

import duckdb


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def audit(
    *,
    candidate_disc: Path,
    reference_disc: Path,
    build_id: str,
    profile_id: str,
    profile_version: str,
) -> dict[str, Any]:
    started = time.monotonic()
    con = duckdb.connect(str(candidate_disc), read_only=True)
    try:
        con.execute(
            f"ATTACH {sql_literal(reference_disc.resolve())} AS reference (READ_ONLY)"
        )
        inventory = con.execute(
            """
            SELECT
              (SELECT count(*) FROM coolness_scores),
              (SELECT count(*) FROM reference.coolness_scores),
              (SELECT count(*)-count(DISTINCT system_id) FROM coolness_scores),
              (SELECT count(*)-count(DISTINCT system_id) FROM reference.coolness_scores),
              (SELECT count(*) FROM coolness_scores c
               WHERE NOT EXISTS (
                 SELECT 1 FROM reference.coolness_scores r WHERE r.system_id=c.system_id
               )),
              (SELECT count(*) FROM reference.coolness_scores r
               WHERE NOT EXISTS (
                 SELECT 1 FROM coolness_scores c WHERE c.system_id=r.system_id
               ))
            """
        ).fetchone()
        lineage = con.execute(
            """
            SELECT
              count(*) FILTER (WHERE build_id<>?),
              count(*) FILTER (WHERE profile_id<>? OR profile_version<>?),
              count(*) FILTER (WHERE score_total IS NULL OR NOT isfinite(score_total)),
              count(*) FILTER (WHERE rank IS NULL OR rank<1)
            FROM coolness_scores
            """,
            [build_id, profile_id, profile_version],
        ).fetchone()
        deltas = con.execute(
            """
            SELECT count(*) FILTER (WHERE c.rank<>r.rank),
                   count(*) FILTER (WHERE c.dominant_spectral_class<>r.dominant_spectral_class),
                   count(*) FILTER (WHERE c.nice_planet_count<>r.nice_planet_count),
                   median(abs(c.rank-r.rank)),
                   quantile_cont(abs(c.rank-r.rank),0.95),
                   max(abs(c.rank-r.rank)),
                   median(abs(c.score_total-r.score_total)),
                   quantile_cont(abs(c.score_total-r.score_total),0.95),
                   max(abs(c.score_total-r.score_total))
            FROM coolness_scores c JOIN reference.coolness_scores r USING (system_id)
            """
        ).fetchone()
        top_overlap: dict[str, dict[str, int]] = {}
        for limit in (100, 1000, 10000):
            overlap = con.execute(
                """
                SELECT count(*) FROM coolness_scores c
                JOIN reference.coolness_scores r USING (system_id)
                WHERE c.rank<=? AND r.rank<=?
                """,
                [limit, limit],
            ).fetchone()[0]
            expected = min(limit, int(inventory[0]), int(inventory[1]))
            top_overlap[str(limit)] = {
                "limit": limit,
                "overlap": int(overlap),
                "changed_members": expected - int(overlap),
            }
        class_transitions = [
            {
                "reference_class": str(old),
                "candidate_class": str(new),
                "systems": int(count),
            }
            for old, new, count in con.execute(
                """
                SELECT r.dominant_spectral_class,c.dominant_spectral_class,count(*) n
                FROM coolness_scores c JOIN reference.coolness_scores r USING (system_id)
                WHERE c.dominant_spectral_class<>r.dominant_spectral_class
                GROUP BY 1,2 ORDER BY n DESC,1,2 LIMIT 100
                """
            ).fetchall()
        ]
        top_candidate = [
            {
                "system_id": int(row[0]),
                "system_name": str(row[1]),
                "candidate_rank": int(row[2]),
                "reference_rank": int(row[3]),
                "candidate_score": float(row[4]),
                "reference_score": float(row[5]),
                "candidate_class": str(row[6]),
                "reference_class": str(row[7]),
                "star_count": int(row[8]),
                "planet_count": int(row[9]),
                "candidate_exotic_star_feature": float(row[10]),
                "reference_exotic_star_feature": float(row[11]),
                "candidate_exotic_star_score": float(row[12]),
                "reference_exotic_star_score": float(row[13]),
            }
            for row in con.execute(
                """
                SELECT c.system_id,c.system_name,c.rank,r.rank,
                       c.score_total,r.score_total,
                       c.dominant_spectral_class,r.dominant_spectral_class,
                       c.star_count,c.planet_count,
                       c.exotic_star_feature,r.exotic_star_feature,
                       c.score_exotic_star,r.score_exotic_star
                FROM coolness_scores c JOIN reference.coolness_scores r USING (system_id)
                ORDER BY c.rank LIMIT 50
                """
            ).fetchall()
        ]
        top_reference = [
            {
                "system_id": int(row[0]),
                "system_name": str(row[1]),
                "candidate_rank": int(row[2]),
                "reference_rank": int(row[3]),
                "candidate_score": float(row[4]),
                "reference_score": float(row[5]),
                "candidate_class": str(row[6]),
                "reference_class": str(row[7]),
                "star_count": int(row[8]),
                "planet_count": int(row[9]),
                "candidate_exotic_star_feature": float(row[10]),
                "reference_exotic_star_feature": float(row[11]),
                "candidate_exotic_star_score": float(row[12]),
                "reference_exotic_star_score": float(row[13]),
            }
            for row in con.execute(
                """
                SELECT c.system_id,c.system_name,c.rank,r.rank,
                       c.score_total,r.score_total,
                       c.dominant_spectral_class,r.dominant_spectral_class,
                       c.star_count,c.planet_count,
                       c.exotic_star_feature,r.exotic_star_feature,
                       c.score_exotic_star,r.score_exotic_star
                FROM coolness_scores c JOIN reference.coolness_scores r USING (system_id)
                ORDER BY r.rank LIMIT 50
                """
            ).fetchall()
        ]
        largest_top_rank_moves = [
            {
                "system_id": int(row[0]),
                "system_name": str(row[1]),
                "candidate_rank": int(row[2]),
                "reference_rank": int(row[3]),
                "rank_delta": int(row[2] - row[3]),
                "candidate_score": float(row[4]),
                "reference_score": float(row[5]),
                "candidate_class": str(row[6]),
                "reference_class": str(row[7]),
                "star_count": int(row[8]),
                "planet_count": int(row[9]),
                "candidate_exotic_star_feature": float(row[10]),
                "reference_exotic_star_feature": float(row[11]),
                "candidate_exotic_star_score": float(row[12]),
                "reference_exotic_star_score": float(row[13]),
            }
            for row in con.execute(
                """
                SELECT c.system_id,c.system_name,c.rank,r.rank,
                       c.score_total,r.score_total,
                       c.dominant_spectral_class,r.dominant_spectral_class,
                       c.star_count,c.planet_count,
                       c.exotic_star_feature,r.exotic_star_feature,
                       c.score_exotic_star,r.score_exotic_star
                FROM coolness_scores c JOIN reference.coolness_scores r USING (system_id)
                WHERE c.rank<=10000 OR r.rank<=10000
                ORDER BY abs(c.rank-r.rank) DESC,c.system_id LIMIT 100
                """
            ).fetchall()
        ]
    finally:
        con.close()

    checks = {
        "inventory_delta": abs(int(inventory[0]) - int(inventory[1])),
        "candidate_duplicate_systems": int(inventory[2]),
        "reference_duplicate_systems": int(inventory[3]),
        "candidate_only_systems": int(inventory[4]),
        "reference_only_systems": int(inventory[5]),
        "wrong_build_rows": int(lineage[0]),
        "wrong_profile_rows": int(lineage[1]),
        "invalid_score_rows": int(lineage[2]),
        "invalid_rank_rows": int(lineage[3]),
    }
    failing = {key: value for key, value in checks.items() if value != 0}
    return {
        "schema_version": "spacegate.e6_coolness_ab.v1",
        "status": "pass" if not failing else "fail",
        "build_id": build_id,
        "candidate_disc": str(candidate_disc.resolve()),
        "reference_disc": str(reference_disc.resolve()),
        "profile_id": profile_id,
        "profile_version": profile_version,
        "inventory": {"candidate": int(inventory[0]), "reference": int(inventory[1])},
        "deltas": {
            "rank_changed_systems": int(deltas[0]),
            "dominant_class_changed_systems": int(deltas[1]),
            "nice_planet_count_changed_systems": int(deltas[2]),
            "absolute_rank_delta_median": float(deltas[3] or 0),
            "absolute_rank_delta_p95": float(deltas[4] or 0),
            "absolute_rank_delta_max": int(deltas[5] or 0),
            "absolute_score_delta_median": float(deltas[6] or 0),
            "absolute_score_delta_p95": float(deltas[7] or 0),
            "absolute_score_delta_max": float(deltas[8] or 0),
        },
        "top_rank_overlap": top_overlap,
        "top_candidate": top_candidate,
        "top_reference": top_reference,
        "largest_top_rank_moves": largest_top_rank_moves,
        "dominant_class_transitions": class_transitions,
        "checks": checks,
        "failing_checks": failing,
        "wall_seconds": round(time.monotonic() - started, 6),
        "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-disc", type=Path, required=True)
    parser.add_argument("--reference-disc", type=Path, required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--profile-version", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit(
        candidate_disc=args.candidate_disc,
        reference_disc=args.reference_disc,
        build_id=args.build_id,
        profile_id=args.profile_id,
        profile_version=args.profile_version,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
