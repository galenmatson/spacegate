#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb


DEFAULT_PROFILE_ID = "default"
DEFAULT_PROFILE_VERSION = "1"
DEFAULT_WEIGHTS = {
    "luminosity": 0.22,
    "proper_motion": 0.10,
    "multiplicity": 0.14,
    "nice_planets": 0.12,
    "weird_planets": 0.14,
    "proximity": 0.08,
    "system_complexity": 0.12,
    "exotic_star": 0.08,
}


def _state_dir(root: Path) -> Path:
    return Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or root / "data")


def _resolve_symlink(path: Path) -> Path:
    try:
        return path.resolve(strict=True)
    except FileNotFoundError:
        return path


def _select_latest_build(out_dir: Path) -> Path:
    candidates = [p for p in out_dir.iterdir() if p.is_dir() and not p.name.endswith(".tmp")]
    if not candidates:
        raise SystemExit(f"No build directories found in: {out_dir}")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def resolve_build_dir(state_dir: Path, build_id: str | None, prefer_latest_out: bool) -> tuple[str, Path]:
    out_dir = state_dir / "out"
    served_link = state_dir / "served" / "current"
    if build_id:
        build_dir = out_dir / build_id
        if not build_dir.is_dir():
            raise SystemExit(f"Build directory not found: {build_dir}")
        return build_id, build_dir

    if prefer_latest_out:
        build_dir = _select_latest_build(out_dir)
        return build_dir.name, build_dir

    if served_link.exists():
        build_dir = _resolve_symlink(served_link)
        return build_dir.name, build_dir

    build_dir = _select_latest_build(out_dir)
    return build_dir.name, build_dir


def sql_lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def validate_weights(weights: dict[str, float]) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for key in DEFAULT_WEIGHTS:
        raw = weights.get(key, DEFAULT_WEIGHTS[key])
        try:
            value = float(raw)
        except (TypeError, ValueError) as exc:
            raise SystemExit(f"Invalid weight for {key}: {raw!r}") from exc
        if value < 0:
            raise SystemExit(f"Weight must be non-negative: {key}={value}")
        normalized[key] = value
    return normalized


def build_scores(
    *,
    core_db_path: Path,
    rich_db_path: Path,
    weights: dict[str, float],
    build_id: str,
    profile_id: str,
    profile_version: str,
) -> None:
    rich_db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(rich_db_path))
    try:
        core_path_sql = str(core_db_path).replace("'", "''")
        con.execute(f"ATTACH '{core_path_sql}' AS core_db (READ_ONLY)")
        con.execute(
            """
CREATE OR REPLACE TABLE coolness_scores AS
WITH star_scored AS (
  SELECT
    system_id,
    COALESCE(spectral_class, '?') AS spectral_class,
    COALESCE(luminosity_class, '') AS luminosity_class,
    LOWER(COALESCE(spectral_type_raw, '')) AS spectral_type_lc,
    CASE COALESCE(spectral_class, '')
      WHEN 'O' THEN 1.00
      WHEN 'B' THEN 0.90
      WHEN 'A' THEN 0.80
      WHEN 'F' THEN 0.55
      WHEN 'G' THEN 0.45
      WHEN 'K' THEN 0.35
      WHEN 'M' THEN 0.25
      WHEN 'L' THEN 0.20
      WHEN 'T' THEN 0.15
      WHEN 'Y' THEN 0.10
      ELSE 0.10
    END AS spectral_score,
    SQRT(COALESCE(pm_ra_mas_yr, 0.0) * COALESCE(pm_ra_mas_yr, 0.0) +
         COALESCE(pm_dec_mas_yr, 0.0) * COALESCE(pm_dec_mas_yr, 0.0)) AS pm_mas_yr,
    CASE
      WHEN regexp_matches(LOWER(COALESCE(spectral_type_raw, '')), 'pulsar|magnetar|neutron|white\\s*dwarf|\\bwd\\b|wolf\\s*rayet|\\bwr\\b') THEN 1.00
      WHEN regexp_matches(LOWER(COALESCE(spectral_type_raw, '')), 'pec|var|flare') THEN 0.80
      WHEN COALESCE(luminosity_class, '') IN ('I', 'II', 'III', 'VII') THEN 0.70
      WHEN COALESCE(spectral_class, '') IN ('O', 'B', 'L', 'T', 'Y') THEN 0.60
      ELSE 0.00
    END AS star_exotic_raw
  FROM core_db.stars
  WHERE system_id IS NOT NULL
),
star_features AS (
  SELECT
    system_id,
    COUNT(*)::BIGINT AS star_count,
    MAX(spectral_score) AS luminosity_feature,
    AVG(pm_mas_yr) AS avg_pm_mas_yr,
    MAX(star_exotic_raw) AS exotic_star_feature
  FROM star_scored
  GROUP BY system_id
),
dominant_spectral AS (
  SELECT system_id, spectral_class AS dominant_spectral_class
  FROM (
    SELECT
      system_id,
      spectral_class,
      spectral_score,
      ROW_NUMBER() OVER (
        PARTITION BY system_id
        ORDER BY spectral_score DESC, spectral_class ASC
      ) AS rn
    FROM star_scored
  ) ranked
  WHERE rn = 1
),
planet_features AS (
  SELECT
    system_id,
    COUNT(*)::BIGINT AS planet_count,
    SUM(
      CASE
        WHEN COALESCE(match_confidence, 0.0) >= 0.80
         AND COALESCE(eq_temp_k, -1.0) BETWEEN 180.0 AND 350.0
         AND COALESCE(mass_earth, mass_jup * 317.8, -1.0) BETWEEN 0.3 AND 8.0
         AND COALESCE(eccentricity, 0.0) <= 0.35
        THEN 1 ELSE 0
      END
    )::BIGINT AS nice_planet_count,
    SUM(
      CASE
        WHEN COALESCE(match_confidence, 0.0) >= 0.80
         AND (
           COALESCE(eq_temp_k, 0.0) >= 1000.0
           OR COALESCE(eccentricity, 0.0) >= 0.60
           OR COALESCE(orbital_period_days, 99999.0) <= 2.0
         )
        THEN 1 ELSE 0
      END
    )::BIGINT AS weird_planet_count,
    SUM(
      CASE
        WHEN COALESCE(match_confidence, 0.0) >= 0.80
         AND COALESCE(orbital_period_days, 99999.0) <= 2.0
        THEN 1 ELSE 0
      END
    )::BIGINT AS ultra_short_period_count,
    SUM(
      CASE
        WHEN COALESCE(match_confidence, 0.0) >= 0.80
         AND COALESCE(eccentricity, 0.0) >= 0.60
        THEN 1 ELSE 0
      END
    )::BIGINT AS high_eccentricity_count
  FROM core_db.planets
  WHERE system_id IS NOT NULL
  GROUP BY system_id
),
base AS (
  SELECT
    s.system_id,
    s.stable_object_key,
    s.system_name,
    s.dist_ly,
    COALESCE(sf.star_count, 0) AS star_count,
    COALESCE(sf.luminosity_feature, 0.0) AS luminosity_feature,
    COALESCE(sf.avg_pm_mas_yr, 0.0) AS avg_pm_mas_yr,
    COALESCE(sf.exotic_star_feature, 0.0) AS exotic_star_feature,
    COALESCE(pf.planet_count, 0) AS planet_count,
    COALESCE(pf.nice_planet_count, 0) AS nice_planet_count,
    COALESCE(pf.weird_planet_count, 0) AS weird_planet_count,
    COALESCE(pf.ultra_short_period_count, 0) AS ultra_short_period_count,
    COALESCE(pf.high_eccentricity_count, 0) AS high_eccentricity_count,
    COALESCE(ds.dominant_spectral_class, '?') AS dominant_spectral_class
  FROM core_db.systems s
  LEFT JOIN star_features sf USING (system_id)
  LEFT JOIN planet_features pf USING (system_id)
  LEFT JOIN dominant_spectral ds USING (system_id)
),
features AS (
  SELECT
    *,
    LEAST(GREATEST(star_count - 1, 0), 4) / 4.0 AS multiplicity_feature,
    LEAST(nice_planet_count, 3) / 3.0 AS nice_planets_feature,
    LEAST(weird_planet_count, 3) / 3.0 AS weird_planets_feature,
    1.0 / (1.0 + POW(COALESCE(dist_ly, 10000.0) / 20.0, 2.0)) AS proximity_feature,
    LEAST(
      (
        (LN(1.0 + CAST(star_count AS DOUBLE)) / LN(5.0)) +
        (LN(1.0 + CAST(planet_count AS DOUBLE)) / LN(11.0))
      ) / 2.0
      + CASE WHEN star_count >= 2 AND planet_count >= 1 THEN 0.10 ELSE 0.0 END
      + CASE WHEN nice_planet_count > 0 AND weird_planet_count > 0 THEN 0.10 ELSE 0.0 END,
      1.0
    ) AS system_complexity_feature,
    MAX(avg_pm_mas_yr) OVER () AS max_avg_pm_mas_yr
  FROM base
),
normalized AS (
  SELECT
    *,
    CASE
      WHEN COALESCE(max_avg_pm_mas_yr, 0.0) > 0 THEN avg_pm_mas_yr / max_avg_pm_mas_yr
      ELSE 0.0
    END AS proper_motion_feature,
    LEAST(GREATEST(exotic_star_feature, 0.0), 1.0) AS exotic_star_feature_norm
  FROM features
),
scored AS (
  SELECT
    system_id,
    stable_object_key,
    system_name,
    dist_ly,
    dominant_spectral_class,
    star_count,
    planet_count,
    nice_planet_count,
    weird_planet_count,
    ultra_short_period_count,
    high_eccentricity_count,
    luminosity_feature,
    proper_motion_feature,
    multiplicity_feature,
    nice_planets_feature,
    weird_planets_feature,
    proximity_feature,
    system_complexity_feature,
    exotic_star_feature_norm AS exotic_star_feature,
    (luminosity_feature * ?) AS score_luminosity,
    (proper_motion_feature * ?) AS score_proper_motion,
    (multiplicity_feature * ?) AS score_multiplicity,
    (nice_planets_feature * ?) AS score_nice_planets,
    (weird_planets_feature * ?) AS score_weird_planets,
    (proximity_feature * ?) AS score_proximity,
    (system_complexity_feature * ?) AS score_system_complexity,
    (exotic_star_feature_norm * ?) AS score_exotic_star
  FROM normalized
)
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      (score_luminosity + score_proper_motion + score_multiplicity +
       score_nice_planets + score_weird_planets + score_proximity +
       score_system_complexity + score_exotic_star) DESC,
      system_id ASC
  )::BIGINT AS rank,
  system_id,
  stable_object_key,
  system_name,
  ?::VARCHAR AS build_id,
  ?::VARCHAR AS profile_id,
  ?::VARCHAR AS profile_version,
  dist_ly,
  dominant_spectral_class,
  star_count,
  planet_count,
  nice_planet_count,
  weird_planet_count,
  ultra_short_period_count,
  high_eccentricity_count,
  luminosity_feature,
  proper_motion_feature,
  multiplicity_feature,
  nice_planets_feature,
  weird_planets_feature,
  proximity_feature,
  system_complexity_feature,
  exotic_star_feature,
  score_luminosity,
  score_proper_motion,
  score_multiplicity,
  score_nice_planets,
  score_weird_planets,
  score_proximity,
  score_system_complexity,
  score_exotic_star,
  ROUND(100.0 * (
    score_luminosity + score_proper_motion + score_multiplicity +
    score_nice_planets + score_weird_planets + score_proximity +
    score_system_complexity + score_exotic_star
  ), 6) AS score_total
FROM scored
            """,
            [
                weights["luminosity"],
                weights["proper_motion"],
                weights["multiplicity"],
                weights["nice_planets"],
                weights["weird_planets"],
                weights["proximity"],
                weights["system_complexity"],
                weights["exotic_star"],
                build_id,
                profile_id,
                profile_version,
            ],
        )
    finally:
        con.close()


def write_outputs(
    *,
    rich_db_path: Path,
    rich_parquet_path: Path,
    report_path: Path,
    build_id: str,
    profile_id: str,
    profile_version: str,
    weights: dict[str, float],
) -> None:
    rich_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(rich_db_path), read_only=True)
    try:
        parquet_path_sql = str(rich_parquet_path).replace("'", "''")
        con.execute(
            f"""
COPY (
  SELECT * FROM coolness_scores ORDER BY rank ASC
) TO '{parquet_path_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        summary = con.execute(
            """
SELECT
  COUNT(*)::BIGINT AS system_count,
  SUM(CASE WHEN planet_count > 0 THEN 1 ELSE 0 END)::BIGINT AS systems_with_planets,
  SUM(CASE WHEN star_count > 1 THEN 1 ELSE 0 END)::BIGINT AS multi_star_systems,
  MIN(score_total) AS score_min,
  MAX(score_total) AS score_max,
  AVG(score_total) AS score_avg
FROM coolness_scores
            """
        ).fetchone()

        top_rows = con.execute(
            """
SELECT
  rank,
  system_id,
  stable_object_key,
  system_name,
  dist_ly,
  dominant_spectral_class,
  star_count,
  planet_count,
  nice_planet_count,
  weird_planet_count,
  ultra_short_period_count,
  high_eccentricity_count,
  score_total,
  system_complexity_feature,
  exotic_star_feature,
  score_luminosity,
  score_proper_motion,
  score_multiplicity,
  score_nice_planets,
  score_weird_planets,
  score_proximity,
  score_system_complexity,
  score_exotic_star
FROM coolness_scores
ORDER BY rank ASC
LIMIT 25
            """
        ).fetchall()

        top_distribution_rows = con.execute(
            """
SELECT
  dominant_spectral_class,
  COUNT(*)::BIGINT AS systems
FROM coolness_scores
WHERE rank <= 200
GROUP BY dominant_spectral_class
ORDER BY systems DESC, dominant_spectral_class ASC
            """
        ).fetchall()
    finally:
        con.close()

    report = {
        "build_id": build_id,
        "profile": {
            "profile_id": profile_id,
            "profile_version": profile_version,
            "weights": weights,
        },
        "summary": {
            "system_count": int(summary[0]),
            "systems_with_planets": int(summary[1]),
            "multi_star_systems": int(summary[2]),
            "score_min": float(summary[3]) if summary[3] is not None else None,
            "score_max": float(summary[4]) if summary[4] is not None else None,
            "score_avg": float(summary[5]) if summary[5] is not None else None,
        },
        "top_25": [
            {
                "rank": int(r[0]),
                "system_id": int(r[1]),
                "stable_object_key": r[2],
                "system_name": r[3],
                "dist_ly": float(r[4]) if r[4] is not None else None,
                "dominant_spectral_class": r[5],
                "star_count": int(r[6]),
                "planet_count": int(r[7]),
                "nice_planet_count": int(r[8]),
                "weird_planet_count": int(r[9]),
                "ultra_short_period_count": int(r[10]),
                "high_eccentricity_count": int(r[11]),
                "score_total": float(r[12]),
                "feature_values": {
                    "system_complexity": float(r[13]),
                    "exotic_star": float(r[14]),
                },
                "score_breakdown": {
                    "luminosity": float(r[15]),
                    "proper_motion": float(r[16]),
                    "multiplicity": float(r[17]),
                    "nice_planets": float(r[18]),
                    "weird_planets": float(r[19]),
                    "proximity": float(r[20]),
                    "system_complexity": float(r[21]),
                    "exotic_star": float(r[22]),
                },
            }
            for r in top_rows
        ],
        "top_200_spectral_distribution": [
            {
                "spectral_class": row[0],
                "systems": int(row[1]),
            }
            for row in top_distribution_rows
        ],
        "artifacts": {
            "rich_db": str(rich_db_path),
            "parquet": str(rich_parquet_path),
            "report": str(report_path),
        },
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute deterministic coolness rankings into rich artifacts."
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--build-id", default=None)
    parser.add_argument(
        "--latest-out",
        action="store_true",
        help="Target newest completed build in $SPACEGATE_STATE_DIR/out.",
    )
    parser.add_argument("--profile-id", default=DEFAULT_PROFILE_ID)
    parser.add_argument("--profile-version", default=DEFAULT_PROFILE_VERSION)
    parser.add_argument(
        "--weights-json",
        default="",
        help="Optional JSON object with weight overrides.",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    state_dir = _state_dir(root)
    build_id, build_dir = resolve_build_dir(state_dir, args.build_id, args.latest_out)
    core_db_path = build_dir / "core.duckdb"
    if not core_db_path.exists():
        raise SystemExit(f"Missing core DB: {core_db_path}")

    overrides: dict[str, float] = {}
    if args.weights_json.strip():
        try:
            parsed = json.loads(args.weights_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid --weights-json: {exc}") from exc
        if not isinstance(parsed, dict):
            raise SystemExit("--weights-json must be a JSON object")
        overrides = parsed  # type: ignore[assignment]
    weights = validate_weights({**DEFAULT_WEIGHTS, **overrides})

    rich_db_path = build_dir / "rich.duckdb"
    rich_parquet_path = build_dir / "rich" / "coolness_scores.parquet"
    report_path = state_dir / "reports" / build_id / "coolness_report.json"

    build_scores(
        core_db_path=core_db_path,
        rich_db_path=rich_db_path,
        weights=weights,
        build_id=build_id,
        profile_id=str(args.profile_id).strip() or DEFAULT_PROFILE_ID,
        profile_version=str(args.profile_version).strip() or DEFAULT_PROFILE_VERSION,
    )
    write_outputs(
        rich_db_path=rich_db_path,
        rich_parquet_path=rich_parquet_path,
        report_path=report_path,
        build_id=build_id,
        profile_id=str(args.profile_id).strip() or DEFAULT_PROFILE_ID,
        profile_version=str(args.profile_version).strip() or DEFAULT_PROFILE_VERSION,
        weights=weights,
    )

    print(f"Scored build: {build_id}")
    print(f"Rich DB: {rich_db_path}")
    print(f"Parquet: {rich_parquet_path}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
