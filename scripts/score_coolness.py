#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_PROFILE_ID = "default"
DEFAULT_PROFILE_VERSION = "1"
PROFILE_SCHEMA_VERSION = 1
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
PROFILE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _actor() -> str:
    return (
        os.getenv("SPACEGATE_ACTOR")
        or os.getenv("USER")
        or os.getenv("LOGNAME")
        or "unknown"
    )


def _state_dir(root: Path) -> Path:
    return Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or root / "data")


def _profile_store_dir(state_dir: Path) -> Path:
    return Path(
        os.getenv("SPACEGATE_COOLNESS_PROFILE_DIR")
        or (state_dir / "config" / "coolness_profiles")
    )


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


def _json_canonical(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def _hash_weights(weights: dict[str, float]) -> str:
    return hashlib.sha256(_json_canonical(weights).encode("utf-8")).hexdigest()


def _validate_token(value: str, field: str) -> str:
    token = str(value or "").strip()
    if not token:
        raise SystemExit(f"Missing required {field}")
    if not PROFILE_TOKEN_RE.match(token):
        raise SystemExit(
            f"Invalid {field}: {token!r}. Allowed: letters, numbers, '.', '_' and '-'."
        )
    return token


def _profiles_root(store_dir: Path) -> Path:
    return store_dir / "profiles"


def _profile_path(store_dir: Path, profile_id: str, profile_version: str) -> Path:
    pid = _validate_token(profile_id, "profile_id")
    pver = _validate_token(profile_version, "profile_version")
    return _profiles_root(store_dir) / pid / f"{pver}.json"


def _active_path(store_dir: Path) -> Path:
    return store_dir / "active.json"


def _activations_log_path(store_dir: Path) -> Path:
    return store_dir / "activations.jsonl"


def _audit_log_path(store_dir: Path) -> Path:
    return store_dir / "audit.jsonl"


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            out.append(item)
    return out


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


def _parse_weights_json(raw: str) -> dict[str, float]:
    if not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid weights JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise SystemExit("Weights JSON must be an object")

    unknown = sorted(set(parsed.keys()) - set(DEFAULT_WEIGHTS.keys()))
    if unknown:
        raise SystemExit(
            f"Unknown weight keys: {', '.join(unknown)}. Allowed keys: {', '.join(DEFAULT_WEIGHTS.keys())}"
        )
    return parsed  # type: ignore[return-value]


def _profile_payload(
    profile_id: str,
    profile_version: str,
    weights: dict[str, float],
    *,
    notes: str,
    created_by: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    ts = created_at or _utc_now()
    payload = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "profile_id": profile_id,
        "profile_version": profile_version,
        "weights": validate_weights(weights),
        "created_at": ts,
        "created_by": created_by,
        "notes": notes,
    }
    payload["profile_hash"] = hashlib.sha256(
        _json_canonical(
            {
                "schema_version": payload["schema_version"],
                "profile_id": payload["profile_id"],
                "profile_version": payload["profile_version"],
                "weights": payload["weights"],
            }
        ).encode("utf-8")
    ).hexdigest()
    return payload


def _load_profile(store_dir: Path, profile_id: str, profile_version: str) -> dict[str, Any] | None:
    path = _profile_path(store_dir, profile_id, profile_version)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"Corrupt profile file: {path}")
    if "weights" not in data or not isinstance(data["weights"], dict):
        raise SystemExit(f"Profile missing weights: {path}")
    data["weights"] = validate_weights(data["weights"])
    data.setdefault("profile_id", profile_id)
    data.setdefault("profile_version", profile_version)
    data.setdefault("profile_hash", _hash_weights(data["weights"]))
    data["path"] = str(path)
    return data


def _save_profile_immutable(store_dir: Path, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    profile_id = _validate_token(str(payload.get("profile_id", "")), "profile_id")
    profile_version = _validate_token(str(payload.get("profile_version", "")), "profile_version")
    path = _profile_path(store_dir, profile_id, profile_version)
    path.parent.mkdir(parents=True, exist_ok=True)

    existing = _load_profile(store_dir, profile_id, profile_version)
    if existing is not None:
        existing_weights = validate_weights(existing["weights"])
        new_weights = validate_weights(payload["weights"])
        if existing_weights != new_weights:
            raise SystemExit(
                f"Profile {profile_id}@{profile_version} already exists with different weights. "
                "Profiles are immutable; use a new profile_version."
            )
        return existing, False

    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    created = _load_profile(store_dir, profile_id, profile_version)
    if created is None:
        raise SystemExit(f"Failed to write profile: {path}")
    return created, True


def _load_active(store_dir: Path) -> dict[str, Any] | None:
    path = _active_path(store_dir)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return None
    return data


def _record_audit(store_dir: Path, event_type: str, details: dict[str, Any]) -> None:
    event = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "created_at": _utc_now(),
        "actor": _actor(),
        "details": details,
    }
    _append_jsonl(_audit_log_path(store_dir), event)


def _set_active(
    store_dir: Path,
    *,
    profile_id: str,
    profile_version: str,
    reason: str,
    event_type: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = _load_profile(store_dir, profile_id, profile_version)
    if profile is None:
        raise SystemExit(f"Cannot activate missing profile: {profile_id}@{profile_version}")

    prev = _load_active(store_dir)
    record: dict[str, Any] = {
        "activation_id": f"act_{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "activated_at": _utc_now(),
        "activated_by": _actor(),
        "reason": reason,
        "profile_id": profile_id,
        "profile_version": profile_version,
        "profile_hash": profile.get("profile_hash"),
        "from_profile_id": (prev or {}).get("profile_id"),
        "from_profile_version": (prev or {}).get("profile_version"),
    }
    if metadata:
        record.update(metadata)

    _active_path(store_dir).write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _append_jsonl(_activations_log_path(store_dir), record)
    _record_audit(store_dir, event_type, record)
    return record


def _list_profiles(store_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    root = _profiles_root(store_dir)
    if not root.exists():
        return rows
    for id_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for file in sorted(id_dir.glob("*.json")):
            try:
                data = json.loads(file.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if not isinstance(data, dict):
                continue
            weights = data.get("weights")
            if not isinstance(weights, dict):
                continue
            rows.append(
                {
                    "profile_id": str(data.get("profile_id", id_dir.name)),
                    "profile_version": str(data.get("profile_version", file.stem)),
                    "profile_hash": data.get("profile_hash") or _hash_weights(validate_weights(weights)),
                    "created_at": data.get("created_at"),
                    "created_by": data.get("created_by"),
                    "notes": data.get("notes", ""),
                    "weights": validate_weights(weights),
                    "path": str(file),
                }
            )
    rows.sort(key=lambda r: (r["profile_id"], r["profile_version"]))
    return rows


def _ensure_profile_store(state_dir: Path) -> Path:
    store_dir = _profile_store_dir(state_dir)
    _profiles_root(store_dir).mkdir(parents=True, exist_ok=True)

    default_payload = _profile_payload(
        DEFAULT_PROFILE_ID,
        DEFAULT_PROFILE_VERSION,
        DEFAULT_WEIGHTS,
        notes="Bootstrap default profile",
        created_by="system",
    )
    _save_profile_immutable(store_dir, default_payload)

    if _load_active(store_dir) is None:
        _set_active(
            store_dir,
            profile_id=DEFAULT_PROFILE_ID,
            profile_version=DEFAULT_PROFILE_VERSION,
            reason="bootstrap active profile",
            event_type="profile.activate.bootstrap",
        )
    return store_dir


def _resolve_score_profile(
    *,
    store_dir: Path,
    profile_id: str | None,
    profile_version: str | None,
    weights_json: str,
    notes: str,
) -> tuple[dict[str, Any], bool, dict[str, Any] | None]:
    active = _load_active(store_dir)
    overrides = _parse_weights_json(weights_json)

    explicit_profile = bool(profile_id or profile_version)
    if explicit_profile and (not profile_id or not profile_version):
        raise SystemExit("Both --profile-id and --profile-version are required when specifying a profile.")

    if explicit_profile:
        pid = _validate_token(profile_id or "", "profile_id")
        pver = _validate_token(profile_version or "", "profile_version")
        profile = _load_profile(store_dir, pid, pver)
        profile_created = False

        if profile is None:
            if not overrides:
                raise SystemExit(
                    f"Profile {pid}@{pver} does not exist. Provide --weights-json to create it first."
                )
            weights = validate_weights({**DEFAULT_WEIGHTS, **overrides})
            payload = _profile_payload(
                pid,
                pver,
                weights,
                notes=notes.strip() or "created during score run",
                created_by=_actor(),
            )
            profile, profile_created = _save_profile_immutable(store_dir, payload)
            if profile_created:
                _record_audit(
                    store_dir,
                    "profile.create",
                    {
                        "profile_id": pid,
                        "profile_version": pver,
                        "profile_hash": profile.get("profile_hash"),
                        "created_during": "score",
                    },
                )
        else:
            if overrides:
                candidate = validate_weights({**profile["weights"], **overrides})
                if candidate != profile["weights"]:
                    raise SystemExit(
                        f"Profile {pid}@{pver} is immutable. --weights-json changes would mutate it. "
                        "Use a new --profile-version."
                    )

        if profile is None:
            raise SystemExit("Failed to resolve profile")
        return profile, profile_created, active

    if overrides:
        raise SystemExit(
            "--weights-json without --profile-id/--profile-version is not allowed. "
            "Use an explicit immutable profile version."
        )

    if not active:
        raise SystemExit("No active coolness profile configured.")

    profile = _load_profile(
        store_dir,
        str(active.get("profile_id", "")),
        str(active.get("profile_version", "")),
    )
    if profile is None:
        raise SystemExit(
            "Active coolness profile pointer is invalid; profile file is missing. "
            "Use 'apply' to repair it."
        )
    return profile, False, active


def _resolve_candidate_profile_for_preview(
    *,
    store_dir: Path,
    profile_id: str | None,
    profile_version: str | None,
    weights_json: str,
) -> tuple[dict[str, Any], str]:
    active = _load_active(store_dir)
    overrides = _parse_weights_json(weights_json)

    if profile_id or profile_version:
        if not profile_id or not profile_version:
            raise SystemExit("Both --profile-id and --profile-version are required when specifying a profile.")
        pid = _validate_token(profile_id, "profile_id")
        pver = _validate_token(profile_version, "profile_version")
        profile = _load_profile(store_dir, pid, pver)
        if profile:
            if overrides:
                candidate_weights = validate_weights({**profile["weights"], **overrides})
                return (
                    {
                        "profile_id": pid,
                        "profile_version": pver,
                        "weights": candidate_weights,
                        "profile_hash": _hash_weights(candidate_weights),
                    },
                    "ephemeral_override_from_profile",
                )
            return profile, "stored"
        if not overrides:
            raise SystemExit(
                f"Profile {pid}@{pver} does not exist. Provide --weights-json for an ephemeral preview."
            )
        candidate_weights = validate_weights({**DEFAULT_WEIGHTS, **overrides})
        return (
            {
                "profile_id": pid,
                "profile_version": pver,
                "weights": candidate_weights,
                "profile_hash": _hash_weights(candidate_weights),
            },
            "ephemeral_new_profile",
        )

    if not active:
        raise SystemExit("No active profile found for preview baseline.")
    active_profile = _load_profile(
        store_dir,
        str(active.get("profile_id", "")),
        str(active.get("profile_version", "")),
    )
    if active_profile is None:
        raise SystemExit("Active profile pointer is invalid.")
    if overrides:
        candidate_weights = validate_weights({**active_profile["weights"], **overrides})
        return (
            {
                "profile_id": str(active_profile.get("profile_id")),
                "profile_version": str(active_profile.get("profile_version")),
                "weights": candidate_weights,
                "profile_hash": _hash_weights(candidate_weights),
            },
            "ephemeral_override_from_active",
        )
    return active_profile, "active"


def _weight_diff(left: dict[str, float], right: dict[str, float]) -> dict[str, Any]:
    rows = []
    changed = []
    for key in DEFAULT_WEIGHTS:
        lv = float(left.get(key, DEFAULT_WEIGHTS[key]))
        rv = float(right.get(key, DEFAULT_WEIGHTS[key]))
        delta = rv - lv
        row = {
            "key": key,
            "left": lv,
            "right": rv,
            "delta": delta,
            "delta_pct_of_left": (delta / lv * 100.0) if lv != 0 else None,
        }
        rows.append(row)
        if abs(delta) > 1e-12:
            changed.append(row)
    changed.sort(key=lambda r: abs(float(r["delta"])), reverse=True)
    return {
        "changed_count": len(changed),
        "changed": changed,
        "all": rows,
    }


def build_scores(
    *,
    core_db_path: Path,
    rich_db_path: Path,
    weights: dict[str, float],
    build_id: str,
    profile_id: str,
    profile_version: str,
) -> None:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "python module 'duckdb' not found. Install requirements before running score command."
        ) from exc
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
    profile: dict[str, Any],
    weights: dict[str, float],
    store_dir: Path,
    active_at_run: dict[str, Any] | None,
) -> None:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "python module 'duckdb' not found. Install requirements before running score command."
        ) from exc
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
            "profile_id": str(profile.get("profile_id")),
            "profile_version": str(profile.get("profile_version")),
            "profile_hash": profile.get("profile_hash") or _hash_weights(weights),
            "weights_hash": _hash_weights(weights),
            "weights": weights,
            "profile_store": str(store_dir),
        },
        "provenance": {
            "scored_at": _utc_now(),
            "actor": _actor(),
            "active_profile_at_run": active_at_run,
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


def _cmd_score(args: argparse.Namespace, root: Path) -> int:
    state_dir = _state_dir(root)
    store_dir = _ensure_profile_store(state_dir)

    profile, profile_created, active_before = _resolve_score_profile(
        store_dir=store_dir,
        profile_id=args.profile_id,
        profile_version=args.profile_version,
        weights_json=args.weights_json,
        notes=args.notes,
    )
    weights = validate_weights(profile["weights"])

    build_id, build_dir = resolve_build_dir(state_dir, args.build_id, args.latest_out)
    core_db_path = build_dir / "core.duckdb"
    if not core_db_path.exists():
        raise SystemExit(f"Missing core DB: {core_db_path}")

    rich_db_path = build_dir / "rich.duckdb"
    rich_parquet_path = build_dir / "rich" / "coolness_scores.parquet"
    report_path = state_dir / "reports" / build_id / "coolness_report.json"

    build_scores(
        core_db_path=core_db_path,
        rich_db_path=rich_db_path,
        weights=weights,
        build_id=build_id,
        profile_id=str(profile["profile_id"]),
        profile_version=str(profile["profile_version"]),
    )
    write_outputs(
        rich_db_path=rich_db_path,
        rich_parquet_path=rich_parquet_path,
        report_path=report_path,
        build_id=build_id,
        profile=profile,
        weights=weights,
        store_dir=store_dir,
        active_at_run=active_before,
    )

    _record_audit(
        store_dir,
        "score.run",
        {
            "build_id": build_id,
            "profile_id": profile["profile_id"],
            "profile_version": profile["profile_version"],
            "profile_hash": profile.get("profile_hash"),
            "profile_created_during_run": profile_created,
            "report_path": str(report_path),
            "parquet_path": str(rich_parquet_path),
        },
    )

    print(f"Scored build: {build_id}")
    print(f"Profile: {profile['profile_id']}@{profile['profile_version']} ({profile.get('profile_hash')})")
    print(f"Rich DB: {rich_db_path}")
    print(f"Parquet: {rich_parquet_path}")
    print(f"Report: {report_path}")
    print(f"Profile store: {store_dir}")
    return 0


def _cmd_list(args: argparse.Namespace, root: Path) -> int:
    state_dir = _state_dir(root)
    store_dir = _ensure_profile_store(state_dir)
    active = _load_active(store_dir)
    profiles = _list_profiles(store_dir)
    out = {
        "profile_store": str(store_dir),
        "active": active,
        "count": len(profiles),
        "profiles": profiles,
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


def _cmd_preview(args: argparse.Namespace, root: Path) -> int:
    state_dir = _state_dir(root)
    store_dir = _ensure_profile_store(state_dir)
    active = _load_active(store_dir)
    active_profile = None
    if active:
        active_profile = _load_profile(
            store_dir,
            str(active.get("profile_id", "")),
            str(active.get("profile_version", "")),
        )

    candidate, source = _resolve_candidate_profile_for_preview(
        store_dir=store_dir,
        profile_id=args.profile_id,
        profile_version=args.profile_version,
        weights_json=args.weights_json,
    )

    active_weights = validate_weights((active_profile or {}).get("weights", DEFAULT_WEIGHTS))
    candidate_weights = validate_weights(candidate["weights"])
    diff = _weight_diff(active_weights, candidate_weights)

    out = {
        "source": source,
        "candidate": {
            "profile_id": candidate.get("profile_id"),
            "profile_version": candidate.get("profile_version"),
            "profile_hash": candidate.get("profile_hash") or _hash_weights(candidate_weights),
            "weights": candidate_weights,
        },
        "active": active_profile,
        "diff_vs_active": diff,
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


def _resolve_profile_or_active(
    *,
    store_dir: Path,
    profile_id: str | None,
    profile_version: str | None,
    role: str,
) -> dict[str, Any]:
    if profile_id or profile_version:
        if not profile_id or not profile_version:
            raise SystemExit(
                f"Both --{role}-profile-id and --{role}-profile-version are required when specifying {role}."
            )
        profile = _load_profile(store_dir, profile_id, profile_version)
        if profile is None:
            raise SystemExit(f"Unknown {role} profile: {profile_id}@{profile_version}")
        return profile

    active = _load_active(store_dir)
    if not active:
        raise SystemExit(f"No active profile found for {role} side.")
    profile = _load_profile(
        store_dir,
        str(active.get("profile_id", "")),
        str(active.get("profile_version", "")),
    )
    if profile is None:
        raise SystemExit(f"Active profile pointer invalid for {role} side.")
    return profile


def _cmd_diff(args: argparse.Namespace, root: Path) -> int:
    state_dir = _state_dir(root)
    store_dir = _ensure_profile_store(state_dir)

    left = _resolve_profile_or_active(
        store_dir=store_dir,
        profile_id=args.left_profile_id,
        profile_version=args.left_profile_version,
        role="left",
    )

    right_weights_json = _parse_weights_json(args.right_weights_json)
    right_profile: dict[str, Any]
    right_source = "stored_or_active"
    if args.right_profile_id or args.right_profile_version:
        right_profile = _resolve_profile_or_active(
            store_dir=store_dir,
            profile_id=args.right_profile_id,
            profile_version=args.right_profile_version,
            role="right",
        )
        if right_weights_json:
            merged = validate_weights({**right_profile["weights"], **right_weights_json})
            right_profile = {
                "profile_id": right_profile.get("profile_id"),
                "profile_version": right_profile.get("profile_version"),
                "weights": merged,
                "profile_hash": _hash_weights(merged),
            }
            right_source = "ephemeral_override_from_right_profile"
    elif right_weights_json:
        merged = validate_weights({**left["weights"], **right_weights_json})
        right_profile = {
            "profile_id": left.get("profile_id"),
            "profile_version": left.get("profile_version"),
            "weights": merged,
            "profile_hash": _hash_weights(merged),
        }
        right_source = "ephemeral_override_from_left"
    else:
        raise SystemExit(
            "Diff needs either --right-profile-id/--right-profile-version or --right-weights-json."
        )

    diff = _weight_diff(validate_weights(left["weights"]), validate_weights(right_profile["weights"]))
    out = {
        "left": {
            "profile_id": left.get("profile_id"),
            "profile_version": left.get("profile_version"),
            "profile_hash": left.get("profile_hash"),
            "weights": left.get("weights"),
        },
        "right": {
            "source": right_source,
            "profile_id": right_profile.get("profile_id"),
            "profile_version": right_profile.get("profile_version"),
            "profile_hash": right_profile.get("profile_hash"),
            "weights": right_profile.get("weights"),
        },
        "diff": diff,
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


def _cmd_apply(args: argparse.Namespace, root: Path) -> int:
    state_dir = _state_dir(root)
    store_dir = _ensure_profile_store(state_dir)

    if not args.profile_id or not args.profile_version:
        raise SystemExit("apply requires --profile-id and --profile-version")

    pid = _validate_token(args.profile_id, "profile_id")
    pver = _validate_token(args.profile_version, "profile_version")
    overrides = _parse_weights_json(args.weights_json)

    profile = _load_profile(store_dir, pid, pver)
    created = False
    if profile is None:
        if not overrides:
            raise SystemExit(
                f"Profile {pid}@{pver} does not exist. Provide --weights-json to create it during apply."
            )
        weights = validate_weights({**DEFAULT_WEIGHTS, **overrides})
        payload = _profile_payload(
            pid,
            pver,
            weights,
            notes=args.notes.strip() or "created during apply",
            created_by=_actor(),
        )
        profile, created = _save_profile_immutable(store_dir, payload)
        if created:
            _record_audit(
                store_dir,
                "profile.create",
                {
                    "profile_id": pid,
                    "profile_version": pver,
                    "profile_hash": profile.get("profile_hash"),
                    "created_during": "apply",
                },
            )
    else:
        if overrides:
            candidate = validate_weights({**profile["weights"], **overrides})
            if candidate != profile["weights"]:
                raise SystemExit(
                    f"Profile {pid}@{pver} exists and is immutable. "
                    "Weights differ from stored profile; use a new version."
                )

    active = _set_active(
        store_dir,
        profile_id=pid,
        profile_version=pver,
        reason=args.reason.strip() or "manual apply",
        event_type="profile.activate",
        metadata={"profile_created": created},
    )
    out = {
        "profile_store": str(store_dir),
        "created": created,
        "active": active,
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


def _cmd_rollback(args: argparse.Namespace, root: Path) -> int:
    steps = int(args.steps)
    if steps < 1:
        raise SystemExit("--steps must be >= 1")

    state_dir = _state_dir(root)
    store_dir = _ensure_profile_store(state_dir)
    active = _load_active(store_dir)
    if not active:
        raise SystemExit("No active profile configured.")

    history = _read_jsonl(_activations_log_path(store_dir))
    if not history:
        raise SystemExit("No activation history found.")

    current_idx = len(history) - 1
    active_activation_id = str(active.get("activation_id", "")).strip()
    if active_activation_id:
        for idx in range(len(history) - 1, -1, -1):
            if str(history[idx].get("activation_id", "")) == active_activation_id:
                current_idx = idx
                break

    target_idx = current_idx - steps
    if target_idx < 0:
        raise SystemExit(
            f"Cannot rollback {steps} step(s). Available history depth: {current_idx}."
        )

    target = history[target_idx]
    target_pid = str(target.get("profile_id", "")).strip()
    target_pver = str(target.get("profile_version", "")).strip()
    if not target_pid or not target_pver:
        raise SystemExit("Activation history is corrupt; missing target profile.")

    record = _set_active(
        store_dir,
        profile_id=target_pid,
        profile_version=target_pver,
        reason=args.reason.strip() or f"rollback {steps} step(s)",
        event_type="profile.rollback",
        metadata={
            "rollback_steps": steps,
            "from_activation_id": active.get("activation_id"),
            "to_history_index": target_idx,
        },
    )
    print(json.dumps({"profile_store": str(store_dir), "active": record}, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Coolness profile management and deterministic scoring. "
            "Commands: score, list, preview, diff, apply, rollback."
        )
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="score",
        choices=["score", "list", "preview", "diff", "apply", "rollback"],
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))

    # Scoring / profile selection
    parser.add_argument("--build-id", default=None)
    parser.add_argument(
        "--latest-out",
        action="store_true",
        help="Target newest completed build in $SPACEGATE_STATE_DIR/out.",
    )
    parser.add_argument("--profile-id", default=None)
    parser.add_argument("--profile-version", default=None)
    parser.add_argument(
        "--weights-json",
        default="",
        help="JSON object with weight overrides.",
    )
    parser.add_argument(
        "--notes",
        default="",
        help="Optional notes when creating a new immutable profile version.",
    )
    parser.add_argument(
        "--reason",
        default="",
        help="Optional activation/rollback reason.",
    )

    # Diff-specific selectors
    parser.add_argument("--left-profile-id", default=None)
    parser.add_argument("--left-profile-version", default=None)
    parser.add_argument("--right-profile-id", default=None)
    parser.add_argument("--right-profile-version", default=None)
    parser.add_argument(
        "--right-weights-json",
        default="",
        help="JSON weight overrides for right side of diff.",
    )

    parser.add_argument("--steps", type=int, default=1, help="Rollback steps (default: 1)")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    root = Path(args.root).resolve()

    if args.command == "score":
        return _cmd_score(args, root)
    if args.command == "list":
        return _cmd_list(args, root)
    if args.command == "preview":
        return _cmd_preview(args, root)
    if args.command == "diff":
        return _cmd_diff(args, root)
    if args.command == "apply":
        return _cmd_apply(args, root)
    if args.command == "rollback":
        return _cmd_rollback(args, root)

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
