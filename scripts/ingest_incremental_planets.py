#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

PLANET_CLASSIFIER_VERSION_DEFAULT = "planet_lifecycle_v1"
NASA_SOURCE_VERSION = "pscomppars"
NASA_SOURCE_URL_DEFAULT = "https://exoplanetarchive.ipac.caltech.edu"
NASA_SOURCE_LICENSE = "NASA Exoplanet Archive"
NASA_SOURCE_LICENSE_NOTE = "https://exoplanetarchive.ipac.caltech.edu"
PC_TO_LY = 3.26156
BITS_PER_AXIS = 21
MORTON_MAX_ABS_LY = 1000.0
MORTON_N = (1 << BITS_PER_AXIS) - 1
MORTON_SCALE = MORTON_N / (2 * MORTON_MAX_ABS_LY)


def now_utc() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def log(message: str) -> None:
    print(f"{now_utc()} {message}", flush=True)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if value and value[0] not in "\"'":
            value = value.split("#", 1)[0].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ[key] = value


def init_env(root: Path) -> None:
    for env_path in (
        Path("/etc/spacegate/spacegate.env"),
        root / ".spacegate.env",
        root / ".spacegate.local.env",
    ):
        load_env_file(env_path)


def detect_state_dir(root: Path) -> Path:
    raw = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if raw:
        return Path(raw).expanduser()
    return root / "data"


def read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = raw.strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise SystemExit(f"Invalid {name} value: {raw!r} (expected boolean)")


def morton3d(x: float, y: float, z: float) -> int | None:
    if x is None or y is None or z is None:
        return None
    if abs(x) > MORTON_MAX_ABS_LY or abs(y) > MORTON_MAX_ABS_LY or abs(z) > MORTON_MAX_ABS_LY:
        raise ValueError(
            f"Morton domain exceeded: ({x}, {y}, {z}) outside ±{MORTON_MAX_ABS_LY} ly"
        )
    try:
        xi = int(round((x + MORTON_MAX_ABS_LY) * MORTON_SCALE))
        yi = int(round((y + MORTON_MAX_ABS_LY) * MORTON_SCALE))
        zi = int(round((z + MORTON_MAX_ABS_LY) * MORTON_SCALE))
    except Exception:
        return None

    xi = max(0, min(MORTON_N, xi))
    yi = max(0, min(MORTON_N, yi))
    zi = max(0, min(MORTON_N, zi))

    def part1by2(n: int) -> int:
        n &= MORTON_N
        out = 0
        for i in range(BITS_PER_AXIS):
            out |= ((n >> i) & 1) << (3 * i)
        return out

    return part1by2(xi) | (part1by2(yi) << 1) | (part1by2(zi) << 2)


def get_git_sha(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except Exception:
        return "nogit"


def resolve_served_current_core(state_dir: Path) -> tuple[str, Path]:
    served_current = state_dir / "served" / "current"
    if not served_current.exists():
        raise SystemExit(f"Missing served/current symlink: {served_current}")
    resolved = served_current.resolve()
    core_db = resolved / "core.duckdb"
    if not core_db.exists():
        raise SystemExit(f"Missing core DB in current build: {core_db}")
    return resolved.name, core_db


def clone_parent_duplicate_trap_report(
    *,
    reports_root: Path,
    parent_build_id: str,
    build_id: str,
    ingested_at: str,
) -> None:
    parent_path = reports_root / parent_build_id / "duplicate_trap_report.json"
    if not parent_path.exists():
        return
    payload = read_json(parent_path, default={})
    if not isinstance(payload, dict) or not payload:
        return
    payload["build_id"] = build_id
    payload["parent_build_id"] = parent_build_id
    payload["generated_at"] = ingested_at
    payload["mode"] = "incremental_planet_refresh"
    write_json(reports_root / build_id / "duplicate_trap_report.json", payload)


def select_manifest_entry(manifest: list[dict], source_name: str) -> dict | None:
    candidates = [row for row in manifest if str(row.get("source_name") or "") == source_name]
    if not candidates:
        return None
    candidates.sort(key=lambda row: str(row.get("checked_at") or row.get("retrieved_at") or ""))
    return candidates[-1]


def copy_base_build(parent_build_dir: Path, tmp_out_dir: Path) -> None:
    tmp_out_dir.mkdir(parents=True, exist_ok=True)
    parquet_src = parent_build_dir / "parquet"
    parquet_dst = tmp_out_dir / "parquet"
    parquet_dst.mkdir(parents=True, exist_ok=True)

    # Copy mutable DBs.
    shutil.copy2(parent_build_dir / "core.duckdb", tmp_out_dir / "core.duckdb")

    # Keep parquet footprint low by symlinking unchanged files; planets.parquet will be rewritten.
    if parquet_src.exists():
        for item in parquet_src.iterdir():
            dst = parquet_dst / item.name
            if dst.exists() or dst.is_symlink():
                dst.unlink()
            dst.symlink_to(item.resolve())


def update_build_metadata(con: duckdb.DuckDBPyConnection, items: dict[str, str]) -> None:
    for key, value in items.items():
        con.execute(f"delete from build_metadata where key = {sql_literal(key)}")
        con.execute(
            f"insert into build_metadata (key, value) values ({sql_literal(key)}, {sql_literal(value)})"
        )


def build_planets(
    con: duckdb.DuckDBPyConnection,
    *,
    cooked_nasa: Path,
    nasa_url: str,
    nasa_sha: str,
    nasa_retrieved: str,
    ingested_at: str,
    transform_version: str,
    planet_classifier_version: str,
    enable_lifecycle: bool,
    cooked_lifecycle_status: Path,
    cooked_lifecycle_features: Path,
    build_id: str,
) -> tuple[dict, dict, int, dict[str, int]]:
    # Capture previous lifecycle state before replacing planets table.
    con.execute(
        """
        create or replace temp table prev_planet_state as
        select
          stable_object_key,
          coalesce(planet_status, 'confirmed') as previous_status,
          planet_classifier_version as previous_classifier_version
        from planets
        """
    )

    con.execute("drop table if exists planets")
    con.execute("drop table if exists planet_catalog_observations")
    con.execute("drop table if exists planet_status_history")
    con.execute("drop table if exists planet_reclassification_audit")

    nasa_path = str(cooked_nasa).replace("'", "''")
    con.execute(
        f"""
        create or replace temp view nasa_raw as
        select * from read_csv_auto('{nasa_path}',
            delim=',',
            quote='\"',
            escape='\"',
            header=true,
            strict_mode=false,
            null_padding=true,
            all_varchar=true
        )
        """
    )
    con.execute(
        """
        create table planets as
        with base as (
          select
            nullif(objectid,'')::bigint as source_pk,
            nullif(pl_name,'') as planet_name,
            nullif(hostname,'') as host_name_raw,
            nullif(hd_name,'') as hd_name,
            nullif(hip_name,'') as hip_name,
            nullif(gaia_dr3_id,'') as gaia_dr3_id,
            nullif(gaia_dr2_id,'') as gaia_dr2_id,
            nullif(disc_year,'')::int as disc_year,
            nullif(discoverymethod,'') as discovery_method,
            nullif(disc_facility,'') as discovery_facility,
            nullif(disc_telescope,'') as discovery_telescope,
            nullif(disc_instrument,'') as discovery_instrument,
            nullif(pl_orbper,'')::double as orbital_period_days,
            nullif(pl_orbsmax,'')::double as semi_major_axis_au,
            nullif(pl_orbeccen,'')::double as eccentricity,
            nullif(pl_orbincl,'')::double as inclination_deg,
            nullif(pl_radj,'')::double as radius_jup,
            nullif(pl_rade,'')::double as radius_earth,
            nullif(pl_masse,'')::double as mass_earth,
            nullif(pl_massj,'')::double as mass_jup,
            nullif(pl_eqt,'')::double as eq_temp_k,
            nullif(pl_insol,'')::double as insol_earth,
            nullif(sy_dist,'')::double as host_dist_pc,
            nullif(st_met,'')::double as host_metallicity_feh,
            greatest(
              abs(nullif(st_meterr1,'')::double),
              abs(nullif(st_meterr2,'')::double)
            ) as host_metallicity_feh_error
          from nasa_raw
        ), normalized as (
          select *,
            case when planet_name is null then null else
              lower(trim(regexp_replace(regexp_replace(planet_name, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g')))
            end as planet_name_norm,
            case when host_name_raw is null then null else
              lower(trim(regexp_replace(regexp_replace(host_name_raw, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g')))
            end as host_name_norm,
            cast(nullif(regexp_extract(hip_name, '(\\d+)', 1), '') as bigint) as host_hip_id,
            cast(nullif(regexp_extract(hd_name, '(\\d+)', 1), '') as bigint) as host_hd_id,
            cast(nullif(regexp_extract(coalesce(gaia_dr3_id, gaia_dr2_id, ''), '(\\d{10,})\\s*$', 1), '') as bigint) as host_gaia_id
          from base
        ), name_match as (
          select
            star_name_norm,
            min_by(star_id, dist_ly) as star_id,
            min_by(system_id, dist_ly) as system_id
          from stars
          where star_name_norm is not null
          group by star_name_norm
        ), matches as (
          select
            n.*,
            g.star_id as gaia_star_id,
            g.system_id as gaia_system_id,
            h.star_id as hip_star_id,
            h.system_id as hip_system_id,
            d.star_id as hd_star_id,
            d.system_id as hd_system_id,
            nm.star_id as name_star_id,
            nm.system_id as name_system_id
          from normalized n
          left join stars g on n.host_gaia_id is not null and g.gaia_id = n.host_gaia_id
          left join stars h on n.host_hip_id is not null and h.hip_id = n.host_hip_id
          left join stars d on n.host_hd_id is not null and d.hd_id = n.host_hd_id
          left join name_match nm on n.host_name_norm is not null and nm.star_name_norm = n.host_name_norm
        )
        select
          row_number() over (order by stable_object_key nulls last, m.source_pk)::bigint as planet_id,
          morton3d(s.x_helio_ly, s.y_helio_ly, s.z_helio_ly) as spatial_index,
          case
            when planet_name_norm is null then null
            when count(*) over (partition by planet_name_norm) = 1 then 'planet:nasa:' || planet_name_norm
            else 'planet:nasa:' || planet_name_norm || ':' || m.source_pk::varchar
          end as stable_object_key,
          coalesce(gaia_system_id, hip_system_id, hd_system_id, name_system_id) as system_id,
          coalesce(gaia_star_id, hip_star_id, hd_star_id, name_star_id) as star_id,
          planet_name,
          planet_name_norm,
          disc_year,
          discovery_method,
          discovery_facility,
          discovery_telescope,
          discovery_instrument,
          orbital_period_days,
          semi_major_axis_au,
          eccentricity,
          inclination_deg,
          radius_jup,
          radius_earth,
          mass_earth,
          mass_jup,
          eq_temp_k,
          insol_earth,
          host_metallicity_feh,
          host_metallicity_feh_error,
          host_name_raw,
          host_name_norm,
          host_gaia_id,
          host_hip_id,
          host_hd_id,
          case
            when gaia_star_id is not null then 'gaia'
            when hip_star_id is not null then 'hip'
            when hd_star_id is not null then 'hd'
            when name_star_id is not null then 'hostname'
            else 'unmatched'
          end as match_method,
          case
            when gaia_star_id is not null then 1.0
            when hip_star_id is not null then 0.95
            when hd_star_id is not null then 0.90
            when name_star_id is not null then 0.80
            else 0.0
          end as match_confidence,
          case
            when gaia_star_id is not null or hip_star_id is not null or hd_star_id is not null or name_star_id is not null then null
            else 'no host match'
          end as match_notes,
          s.x_helio_ly,
          s.y_helio_ly,
          s.z_helio_ly,
          'nasa_exoplanet_archive' as source_catalog,
          'pscomppars' as source_version,
          'https://exoplanetarchive.ipac.caltech.edu' as source_url,
          null::varchar as source_download_url,
          null::varchar as source_doi,
          m.source_pk as source_pk,
          m.source_pk as source_row_id,
          null::varchar as source_row_hash,
          'NASA Exoplanet Archive' as license,
          true as redistribution_ok,
          'https://exoplanetarchive.ipac.caltech.edu' as license_note,
          null::varchar as retrieval_etag,
          null::varchar as retrieval_checksum,
          null::varchar as retrieved_at,
          null::varchar as ingested_at,
          null::varchar as transform_version,
          'confirmed'::varchar as planet_status,
          true as is_default_visible,
          false as is_tombstoned,
          'nasa_exoplanet_archive'::varchar as status_source_catalog,
          null::varchar as status_updated_at,
          null::varchar as status_superseded_by,
          null::varchar as planet_size_mass_class,
          null::varchar as planet_insolation_class,
          null::varchar as planet_orbit_class,
          null::varchar as planet_composition_proxy_class,
          null::varchar as planet_detection_tags_json,
          null::varchar as planet_host_context_tags_json,
          null::varchar as planet_classifier_version,
          null::varchar as planet_classifier_updated_at,
          null::double as spacegate_hab_score,
          null::double as spacegate_hab_confidence,
          null::varchar as spacegate_hab_reasons_json,
          null::double as planet_element_richness_score,
          null::varchar as planet_element_richness_class,
          null::varchar as planet_element_richness_method,
          null::varchar as planet_element_richness_notes
        from matches m
        left join stars s on s.star_id = coalesce(gaia_star_id, hip_star_id, hd_star_id, name_star_id)
        """
    )
    con.execute(
        f"""
        update planets set
          source_download_url = {sql_literal(nasa_url)},
          retrieval_checksum = {sql_literal(nasa_sha)},
          retrieved_at = {sql_literal(nasa_retrieved)},
          ingested_at = {sql_literal(ingested_at)},
          transform_version = {sql_literal(transform_version)},
          status_updated_at = {sql_literal(ingested_at)},
          planet_classifier_version = {sql_literal(planet_classifier_version)},
          planet_classifier_updated_at = {sql_literal(ingested_at)},
          planet_element_richness_score = case
            when host_metallicity_feh is null then null
            else least(greatest((least(greatest(host_metallicity_feh, -0.8), 0.6) + 0.8) / 1.4, 0.0), 1.0)
          end,
          planet_element_richness_class = case
            when host_metallicity_feh is null then 'unknown'
            when host_metallicity_feh < -0.4 then 'very_low'
            when host_metallicity_feh < -0.2 then 'low'
            when host_metallicity_feh < 0.1 then 'moderate'
            when host_metallicity_feh < 0.3 then 'high'
            else 'very_high'
          end,
          planet_element_richness_method = case
            when host_metallicity_feh is null then 'unknown'
            else 'host_spectroscopy_proxy'
          end,
          planet_element_richness_notes = case
            when host_metallicity_feh is null then 'no host metallicity evidence'
            else 'inferred from host stellar metallicity ([Fe/H])'
          end
        """
    )

    lifecycle_status_raw_rows = 0
    lifecycle_status_matched_rows = 0
    lifecycle_features_raw_rows = 0
    lifecycle_stale_classifier_rows = 0
    planet_catalog_delta_report: dict[str, object] = {
        "build_id": build_id,
        "lifecycle_enabled": bool(enable_lifecycle),
    }
    planet_reclassification_report: dict[str, object] = {
        "build_id": build_id,
        "lifecycle_enabled": bool(enable_lifecycle),
        "planet_classifier_version": planet_classifier_version,
    }

    if enable_lifecycle:
        lifecycle_status_path = str(cooked_lifecycle_status).replace("'", "''")
        lifecycle_features_path = str(cooked_lifecycle_features).replace("'", "''")
        con.execute(
            f"""
            create or replace temp view exoplanet_lifecycle_status_raw as
            select * from read_csv_auto('{lifecycle_status_path}',
                delim=',',
                quote='\"',
                escape='\"',
                header=true,
                strict_mode=false,
                null_padding=true,
                all_varchar=true
            )
            """
        )
        con.execute(
            f"""
            create or replace temp view exoplanet_lifecycle_features_raw as
            select * from read_csv_auto('{lifecycle_features_path}',
                delim=',',
                quote='\"',
                escape='\"',
                header=true,
                strict_mode=false,
                null_padding=true,
                all_varchar=true
            )
            """
        )
        lifecycle_status_raw_rows = int(
            con.execute(
                """
                select count(*)::bigint
                from exoplanet_lifecycle_status_raw
                where lower(coalesce(observed_status, '')) in ('confirmed','candidate','controversial','retracted')
                """
            ).fetchone()[0]
            or 0
        )
        lifecycle_features_raw_rows = int(
            con.execute("select count(*)::bigint from exoplanet_lifecycle_features_raw").fetchone()[0] or 0
        )
        con.execute(
            """
            create or replace temp table planet_status_matches as
            select
              p.planet_id,
              p.stable_object_key,
              p.planet_name,
              p.planet_name_norm,
              lower(trim(coalesce(s.source_catalog, 'unknown'))) as source_catalog,
              coalesce(s.source_version, '') as source_version,
              coalesce(s.source_pk, '') as source_pk,
              lower(trim(coalesce(s.observed_status, ''))) as observed_status,
              coalesce(s.source_row_hash, '') as source_row_hash,
              coalesce(s.observed_at, '') as observed_at,
              coalesce(s.notes, '') as notes
            from planets p
            join exoplanet_lifecycle_status_raw s
              on p.planet_name_norm is not null
             and p.planet_name_norm = lower(trim(coalesce(s.planet_name_norm, '')))
            where lower(trim(coalesce(s.observed_status, ''))) in ('confirmed','candidate','controversial','retracted')
            """
        )
        lifecycle_status_matched_rows = int(
            con.execute("select count(*)::bigint from planet_status_matches").fetchone()[0] or 0
        )
        con.execute(
            f"""
            create or replace table planet_catalog_observations as
            select
              {sql_literal(build_id)} as build_id,
              stable_object_key,
              source_catalog,
              source_version,
              source_pk,
              source_row_hash,
              observed_status,
              observed_at,
              notes as payload_json
            from planet_status_matches
            """
        )
        con.execute(
            """
            create or replace temp table planet_status_agg as
            select
              planet_id,
              max(case when observed_status = 'retracted' then 1 else 0 end) as has_retracted,
              max(case when observed_status = 'confirmed' then 1 else 0 end) as has_confirmed,
              max(case when observed_status = 'candidate' then 1 else 0 end) as has_candidate,
              max(case when observed_status = 'controversial' then 1 else 0 end) as has_controversial,
              min(case when observed_status = 'retracted' then source_catalog end) as retracted_catalog,
              min(case when observed_status = 'confirmed' then source_catalog end) as confirmed_catalog,
              min(case when observed_status = 'candidate' then source_catalog end) as candidate_catalog,
              min(case when observed_status = 'controversial' then source_catalog end) as controversial_catalog
            from planet_status_matches
            group by planet_id
            """
        )
        con.execute(
            f"""
            update planets p
            set
              planet_status = case
                when a.has_retracted = 1 then 'retracted'
                when a.has_confirmed = 1 then 'confirmed'
                when a.has_candidate = 1 then 'candidate'
                when a.has_controversial = 1 then 'controversial'
                else p.planet_status
              end,
              status_source_catalog = coalesce(
                case
                  when a.has_retracted = 1 then a.retracted_catalog
                  when a.has_confirmed = 1 then a.confirmed_catalog
                  when a.has_candidate = 1 then a.candidate_catalog
                  when a.has_controversial = 1 then a.controversial_catalog
                  else p.status_source_catalog
                end,
                p.status_source_catalog
              ),
              status_updated_at = {sql_literal(ingested_at)},
              is_default_visible = case
                when a.has_retracted = 1 then false
                when a.has_controversial = 1 and a.has_confirmed = 0 and a.has_candidate = 0 then false
                else true
              end,
              is_tombstoned = case when a.has_retracted = 1 then true else false end
            from planet_status_agg a
            where p.planet_id = a.planet_id
            """
        )
        con.execute(
            """
            create or replace temp table hwc_feature_best as
            select
              lower(trim(coalesce(planet_name_norm, ''))) as planet_name_norm,
              max(nullif(hwc_p_habitable, '')::double) as hwc_p_habitable,
              max(nullif(hwc_esi, '')::double) as hwc_esi
            from exoplanet_lifecycle_features_raw
            where lower(trim(coalesce(source_catalog, ''))) = 'hwc'
            group by 1
            having planet_name_norm <> ''
            """
        )
        con.execute(
            f"""
            update planets p
            set
              spacegate_hab_score = coalesce(
                case when f.hwc_esi between 0.0 and 1.0 then f.hwc_esi else null end,
                case
                  when f.hwc_p_habitable >= 2.0 then 0.90
                  when f.hwc_p_habitable >= 1.0 then 0.75
                  else null
                end,
                p.spacegate_hab_score
              ),
              spacegate_hab_confidence = case
                when f.hwc_p_habitable is not null or f.hwc_esi is not null then 0.75
                else p.spacegate_hab_confidence
              end,
              spacegate_hab_reasons_json = case
                when f.hwc_p_habitable is not null or f.hwc_esi is not null
                  then '{{"source":"hwc","method":"reference_seed"}}'
                else p.spacegate_hab_reasons_json
              end,
              planet_classifier_version = {sql_literal(planet_classifier_version)},
              planet_classifier_updated_at = {sql_literal(ingested_at)}
            from hwc_feature_best f
            where p.planet_name_norm = f.planet_name_norm
            """
        )
        con.execute(
            f"""
            create or replace table planet_status_history as
            with joined as (
              select
                p.stable_object_key,
                prev.previous_status,
                p.planet_status as resolved_status,
                p.status_source_catalog as resolved_by_catalog
              from planets p
              left join prev_planet_state prev using (stable_object_key)
            )
            select
              {sql_literal(build_id)} as build_id,
              stable_object_key,
              previous_status,
              resolved_status,
              case
                when previous_status is null then 'new'
                when previous_status = resolved_status then 'unchanged'
                when resolved_status = 'retracted' and coalesce(previous_status, '') <> 'retracted' then 'retracted'
                when previous_status in ('candidate', 'controversial') and resolved_status = 'confirmed' then 'promoted'
                when previous_status = 'confirmed' and resolved_status in ('candidate', 'controversial') then 'demoted'
                else 'changed'
              end as transition_type,
              resolved_by_catalog,
              {sql_literal(ingested_at)} as resolved_at,
              null::varchar as details_json
            from joined
            """
        )
        con.execute(
            f"""
            create or replace table planet_reclassification_audit as
            with joined as (
              select
                p.stable_object_key,
                p.planet_classifier_version as classifier_version,
                prev.previous_classifier_version
              from planets p
              left join prev_planet_state prev using (stable_object_key)
            )
            select
              {sql_literal(build_id)} as build_id,
              stable_object_key,
              classifier_version,
              previous_classifier_version,
              case
                when previous_classifier_version is null then 'new'
                when previous_classifier_version = classifier_version then 'unchanged'
                else 'source_delta'
              end as reclass_reason,
              '["lifecycle","taxonomy","habitability","element_richness"]'::varchar as fields_recomputed_json,
              {sql_literal(ingested_at)} as recomputed_at
            from joined
            """
        )
        lifecycle_stale_classifier_rows = int(
            con.execute(
                f"""
                select count(*)::bigint
                from planets
                where coalesce(planet_classifier_version, '') <> {sql_literal(planet_classifier_version)}
                """
            ).fetchone()[0]
            or 0
        )
        if lifecycle_stale_classifier_rows > 0:
            raise SystemExit(
                "Planet lifecycle classifier gate failed in incremental mode: "
                f"{lifecycle_stale_classifier_rows} stale rows"
            )
        transition_counts = [
            {"transition_type": row[0], "count": int(row[1] or 0)}
            for row in con.execute(
                """
                select transition_type, count(*)::bigint
                from planet_status_history
                group by 1
                order by count(*) desc, transition_type asc
                """
            ).fetchall()
        ]
        resolved_status_counts = [
            {"planet_status": row[0], "count": int(row[1] or 0)}
            for row in con.execute(
                """
                select planet_status, count(*)::bigint
                from planets
                group by 1
                order by count(*) desc, planet_status asc
                """
            ).fetchall()
        ]
        source_status_counts = [
            {"source_catalog": row[0], "observed_status": row[1], "count": int(row[2] or 0)}
            for row in con.execute(
                """
                select source_catalog, observed_status, count(*)::bigint
                from planet_catalog_observations
                group by 1,2
                order by count(*) desc, source_catalog asc, observed_status asc
                """
            ).fetchall()
        ]
        reclassified_rows = int(
            con.execute(
                """
                select count(*)::bigint
                from planet_reclassification_audit
                where reclass_reason <> 'unchanged'
                """
            ).fetchone()[0]
            or 0
        )
        planet_catalog_delta_report = {
            "build_id": build_id,
            "lifecycle_enabled": True,
            "previous_build_id": None,
            "status_raw_rows": lifecycle_status_raw_rows,
            "status_matched_rows": lifecycle_status_matched_rows,
            "feature_raw_rows": lifecycle_features_raw_rows,
            "resolved_status_counts": resolved_status_counts,
            "transition_counts": transition_counts,
            "source_status_counts": source_status_counts,
        }
        planet_reclassification_report = {
            "build_id": build_id,
            "lifecycle_enabled": True,
            "planet_classifier_version": planet_classifier_version,
            "reclassified_rows": reclassified_rows,
            "stale_classifier_rows": lifecycle_stale_classifier_rows,
        }
    else:
        con.execute(
            """
            create or replace table planet_catalog_observations as
            select
              cast(null as varchar) as build_id,
              cast(null as varchar) as stable_object_key,
              cast(null as varchar) as source_catalog,
              cast(null as varchar) as source_version,
              cast(null as varchar) as source_pk,
              cast(null as varchar) as source_row_hash,
              cast(null as varchar) as observed_status,
              cast(null as varchar) as observed_at,
              cast(null as varchar) as payload_json
            where false
            """
        )
        con.execute(
            """
            create or replace table planet_status_history as
            select
              cast(null as varchar) as build_id,
              cast(null as varchar) as stable_object_key,
              cast(null as varchar) as previous_status,
              cast(null as varchar) as resolved_status,
              cast(null as varchar) as transition_type,
              cast(null as varchar) as resolved_by_catalog,
              cast(null as varchar) as resolved_at,
              cast(null as varchar) as details_json
            where false
            """
        )
        con.execute(
            """
            create or replace table planet_reclassification_audit as
            select
              cast(null as varchar) as build_id,
              cast(null as varchar) as stable_object_key,
              cast(null as varchar) as classifier_version,
              cast(null as varchar) as previous_classifier_version,
              cast(null as varchar) as reclass_reason,
              cast(null as varchar) as fields_recomputed_json,
              cast(null as varchar) as recomputed_at
            where false
            """
        )
        planet_catalog_delta_report = {
            "build_id": build_id,
            "lifecycle_enabled": False,
            "reason": "SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS=0",
        }
        planet_reclassification_report = {
            "build_id": build_id,
            "lifecycle_enabled": False,
            "planet_classifier_version": planet_classifier_version,
            "reclassified_rows": 0,
            "stale_classifier_rows": 0,
        }

    con.execute(
        """
        create or replace temp table planet_host_name_star_best as
        with base as (
          select
            p.star_id,
            p.system_id,
            trim(p.host_name_raw) as display_name,
            p.host_name_norm,
            count(*)::bigint as obs_count,
            case
              when p.host_name_norm is null or p.host_name_norm = '' then 99
              when p.host_name_norm like 'gaia dr3 %' or p.host_name_norm like 'gaia %' then 98
              when regexp_matches(p.host_name_norm, '^(trappist|kepler|k2|toi|wasp|hat|corot|ogle|moa|kmt|tic)\\b') then 10
              when regexp_matches(p.host_name_norm, '^(hd|hip|hr|gj|gl|wolf|lhs|lp|bd|cd|cpd|tyc|2mass|wise|sdss)\\b') then 20
              else 0
            end as name_rank
          from planets p
          where p.star_id is not null
            and p.host_name_raw is not null
            and p.host_name_norm is not null
            and p.host_name_norm <> ''
          group by 1,2,3,4
        ), ranked as (
          select
            *,
            row_number() over (
              partition by star_id
              order by name_rank asc, obs_count desc, length(display_name) asc, host_name_norm asc
            ) as rn
          from base
          where name_rank < 90
        )
        select
          star_id,
          system_id,
          display_name,
          host_name_norm,
          name_rank,
          obs_count
        from ranked
        where rn = 1
        """
    )
    con.execute(
        """
        create or replace temp table planet_host_name_system_best as
        with grouped as (
          select
            system_id,
            display_name,
            host_name_norm,
            min(name_rank) as name_rank,
            sum(obs_count)::bigint as obs_count
          from planet_host_name_star_best
          where system_id is not null
          group by 1,2,3
        ), ranked as (
          select
            *,
            row_number() over (
              partition by system_id
              order by name_rank asc, obs_count desc, length(display_name) asc, host_name_norm asc
            ) as rn
          from grouped
        )
        select
          system_id,
          display_name,
          host_name_norm,
          name_rank,
          obs_count
        from ranked
        where rn = 1
        """
    )

    host_name_star_promotions = int(
        con.execute(
            """
            select count(*)::bigint
            from stars s
            join planet_host_name_star_best b on b.star_id = s.star_id
            where b.display_name is not null
              and b.display_name <> ''
              and (s.star_name is null or s.star_name_norm like 'gaia dr3 %' or s.star_name_norm like 'gaia %')
            """
        ).fetchone()[0]
        or 0
    )
    con.execute(
        """
        update stars s
        set
          star_name = b.display_name,
          star_name_norm = b.host_name_norm
        from planet_host_name_star_best b
        where s.star_id = b.star_id
          and b.display_name is not null
          and b.display_name <> ''
          and (s.star_name is null or s.star_name_norm like 'gaia dr3 %' or s.star_name_norm like 'gaia %')
        """
    )
    host_name_system_promotions = int(
        con.execute(
            """
            select count(*)::bigint
            from systems s
            join planet_host_name_system_best b on b.system_id = s.system_id
            where b.display_name is not null
              and b.display_name <> ''
              and (s.system_name is null or s.system_name_norm like 'gaia dr3 %' or s.system_name_norm like 'gaia %')
            """
        ).fetchone()[0]
        or 0
    )
    con.execute(
        """
        update systems s
        set
          system_name = b.display_name,
          system_name_norm = b.host_name_norm
        from planet_host_name_system_best b
        where s.system_id = b.system_id
          and b.display_name is not null
          and b.display_name <> ''
          and (s.system_name is null or s.system_name_norm like 'gaia dr3 %' or s.system_name_norm like 'gaia %')
        """
    )

    host_name_alias_inserted = int(
        con.execute(
            """
            with seed as (
              select
                'star'::varchar as target_type,
                b.star_id as target_id,
                b.system_id as system_id,
                b.star_id as star_id,
                b.display_name as alias_raw,
                b.host_name_norm as alias_norm
              from planet_host_name_star_best b
              where b.display_name is not null and b.display_name <> ''
              union all
              select
                'system'::varchar as target_type,
                b.system_id as target_id,
                b.system_id as system_id,
                null::bigint as star_id,
                b.display_name as alias_raw,
                b.host_name_norm as alias_norm
              from planet_host_name_system_best b
              where b.display_name is not null and b.display_name <> ''
            ), filtered as (
              select
                target_type,
                target_id,
                system_id,
                star_id,
                alias_raw,
                alias_norm
              from seed
              where target_id is not null and alias_norm is not null and alias_norm <> ''
            )
            select count(*)::bigint
            from filtered f
            where not exists (
              select 1
              from aliases a
              where a.target_type = f.target_type
                and a.target_id = f.target_id
                and a.alias_norm = f.alias_norm
            )
            """
        ).fetchone()[0]
        or 0
    )
    if host_name_alias_inserted > 0:
        con.execute(
            """
            insert into aliases (
              alias_id,
              target_type,
              target_id,
              system_id,
              star_id,
              alias_raw,
              alias_norm,
              alias_kind,
              alias_priority,
              is_primary,
              source_catalog,
              source_version,
              source_pk
            )
            with seed as (
              select
                'star'::varchar as target_type,
                b.star_id as target_id,
                b.system_id as system_id,
                b.star_id as star_id,
                b.display_name as alias_raw,
                b.host_name_norm as alias_norm
              from planet_host_name_star_best b
              where b.display_name is not null and b.display_name <> ''
              union all
              select
                'system'::varchar as target_type,
                b.system_id as target_id,
                b.system_id as system_id,
                null::bigint as star_id,
                b.display_name as alias_raw,
                b.host_name_norm as alias_norm
              from planet_host_name_system_best b
              where b.display_name is not null and b.display_name <> ''
            ), filtered as (
              select
                target_type,
                target_id,
                system_id,
                star_id,
                alias_raw,
                alias_norm
              from seed
              where target_id is not null and alias_norm is not null and alias_norm <> ''
            ), missing as (
              select f.*
              from filtered f
              where not exists (
                select 1
                from aliases a
                where a.target_type = f.target_type
                  and a.target_id = f.target_id
                  and a.alias_norm = f.alias_norm
              )
            ), seq as (
              select coalesce(max(alias_id), 0)::bigint as max_alias_id
              from aliases
            )
            select
              seq.max_alias_id + row_number() over (order by m.target_type, m.target_id, m.alias_norm)::bigint as alias_id,
              m.target_type,
              m.target_id,
              m.system_id,
              m.star_id,
              m.alias_raw,
              m.alias_norm,
              'planet_host_name'::varchar as alias_kind,
              4::int as alias_priority,
              false as is_primary,
              'nasa_exoplanet_archive'::varchar as source_catalog,
              'pscomppars'::varchar as source_version,
              null::bigint as source_pk
            from missing m
            cross join seq
            """
        )

    matched_planet_count = int(
        con.execute(
            "select count(*)::bigint from planets where lower(coalesce(match_method,'')) not in ('unmatched','none','')"
        ).fetchone()[0]
        or 0
    )
    host_name_stats = {
        "host_name_star_promotions": host_name_star_promotions,
        "host_name_system_promotions": host_name_system_promotions,
        "host_name_alias_inserted": host_name_alias_inserted,
    }
    return (
        planet_catalog_delta_report,
        planet_reclassification_report,
        matched_planet_count,
        host_name_stats,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Incremental ingest path for planet/lifecycle catalog deltas."
    )
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--build-id", default="")
    parser.add_argument("--parent-build-id", default="")
    parser.add_argument("--impacted-plan", default="")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    init_env(root)
    state_dir = detect_state_dir(root).resolve()
    reports_root = state_dir / "reports"
    out_root = state_dir / "out"
    manifests_dir = reports_root / "manifests"

    parent_build_id = args.parent_build_id.strip()
    if not parent_build_id:
        parent_build_id, parent_core = resolve_served_current_core(state_dir)
    else:
        parent_core = out_root / parent_build_id / "core.duckdb"
        if not parent_core.exists():
            raise SystemExit(f"Parent core DB not found: {parent_core}")
    parent_build_dir = parent_core.parent

    build_id = args.build_id.strip()
    if not build_id:
        build_id = f"{now_utc().replace(':', '').replace('-', '').replace('T', 'T').replace('Z', 'Z')}_{get_git_sha(root)}_incplanet"
    final_out_dir = out_root / build_id
    tmp_out_dir = out_root / f"{build_id}.tmp"
    if final_out_dir.exists():
        raise SystemExit(f"Build output already exists: {final_out_dir}")
    if tmp_out_dir.exists():
        raise SystemExit(f"Temporary build output already exists: {tmp_out_dir}")

    reports_dir = reports_root / build_id
    reports_dir.mkdir(parents=True, exist_ok=True)

    core_manifest = read_json(manifests_dir / "core_manifest.json", default=[])
    exoplanet_eu_manifest = read_json(manifests_dir / "exoplanet_eu_manifest.json", default=[])
    oec_manifest = read_json(manifests_dir / "open_exoplanet_catalogue_manifest.json", default=[])
    hwc_manifest = read_json(manifests_dir / "hwc_manifest.json", default=[])
    emac_manifest = read_json(manifests_dir / "emac_tt9_manifest.json", default=[])

    nasa_manifest = select_manifest_entry(core_manifest, "pscomppars") or {}
    nasa_url = str(nasa_manifest.get("url") or NASA_SOURCE_URL_DEFAULT)
    nasa_sha = str(nasa_manifest.get("sha256") or "")
    nasa_retrieved = str(nasa_manifest.get("retrieved_at") or "")

    cooked_nasa = state_dir / "cooked" / "nasa_exoplanet_archive" / "pscomppars_clean.csv"
    cooked_lifecycle_status = state_dir / "cooked" / "exoplanet_lifecycle" / "status_rows.csv"
    cooked_lifecycle_features = state_dir / "cooked" / "exoplanet_lifecycle" / "features_rows.csv"
    if not cooked_nasa.exists():
        raise SystemExit(f"Missing cooked NASA file: {cooked_nasa}")

    log(f"Incremental planet ingest begin (parent_build={parent_build_id}, build_id={build_id})")
    copy_base_build(parent_build_dir, tmp_out_dir)
    core_db = tmp_out_dir / "core.duckdb"
    parquet_dir = tmp_out_dir / "parquet"
    arm_db = tmp_out_dir / "arm.duckdb"

    con = duckdb.connect(str(core_db))
    con.create_function("morton3d", morton3d)
    threads = os.getenv("SPACEGATE_DUCKDB_THREADS")
    if threads:
        try:
            con.execute(f"PRAGMA threads={int(threads)}")
        except Exception:
            pass
    memory_limit = os.getenv("SPACEGATE_DUCKDB_MEMORY_LIMIT")
    if memory_limit:
        try:
            con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        except Exception:
            pass

    metadata_rows = con.execute("select key, value from build_metadata").fetchall()
    metadata = {str(k): str(v or "") for k, v in metadata_rows}
    prev_transform = metadata.get("transform_version", "ingest_core.py")
    transform_version = f"{prev_transform}|incremental_planet_refresh_v1"
    planet_classifier_version = (
        os.getenv("SPACEGATE_PLANET_CLASSIFIER_VERSION") or metadata.get("planet_classifier_version") or PLANET_CLASSIFIER_VERSION_DEFAULT
    ).strip()
    enable_lifecycle = parse_bool_env(
        "SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS",
        metadata.get("exoplanet_lifecycle_catalogs_enabled", "0") == "1",
    )
    if enable_lifecycle and (not cooked_lifecycle_status.exists() or not cooked_lifecycle_features.exists()):
        raise SystemExit(
            "Incremental lifecycle mode enabled but cooked lifecycle files are missing. "
            "Run scripts/cook_delta.sh or scripts/cook_core.sh first."
        )

    ingested_at = now_utc()
    (
        planet_catalog_delta_report,
        planet_reclassification_report,
        matched_planet_count,
        host_name_stats,
    ) = build_planets(
        con,
        cooked_nasa=cooked_nasa,
        nasa_url=nasa_url,
        nasa_sha=nasa_sha,
        nasa_retrieved=nasa_retrieved,
        ingested_at=ingested_at,
        transform_version=transform_version,
        planet_classifier_version=planet_classifier_version,
        enable_lifecycle=enable_lifecycle,
        cooked_lifecycle_status=cooked_lifecycle_status,
        cooked_lifecycle_features=cooked_lifecycle_features,
        build_id=build_id,
    )

    # Refresh minimal build metadata.
    update_build_metadata(
        con,
        {
            "build_id": build_id,
            "git_sha": get_git_sha(root),
            "ingested_at": ingested_at,
            "transform_version": transform_version,
            "build_mode": "incremental_planet_refresh",
            "parent_build_id": parent_build_id,
            "gaia_backbone_enabled": metadata.get("gaia_backbone_enabled", "1"),
            "coordinate_epoch": metadata.get("coordinate_epoch", "J2016.0"),
            "coordinate_frame": metadata.get("coordinate_frame", "ICRS"),
            "exoplanet_lifecycle_catalogs_enabled": ("1" if enable_lifecycle else "0"),
            "planet_classifier_version": planet_classifier_version,
        },
    )

    stars_count = int(con.execute("select count(*)::bigint from stars").fetchone()[0] or 0)
    systems_count = int(con.execute("select count(*)::bigint from systems").fetchone()[0] or 0)
    planets_count = int(con.execute("select count(*)::bigint from planets").fetchone()[0] or 0)
    aliases_count = int(con.execute("select count(*)::bigint from aliases").fetchone()[0] or 0)
    compact_count = int(con.execute("select count(*)::bigint from compact_objects").fetchone()[0] or 0)
    superstellar_count = int(con.execute("select count(*)::bigint from superstellar_objects").fetchone()[0] or 0)
    eclipsing_count = int(con.execute("select count(*)::bigint from eclipsing_binaries").fetchone()[0] or 0)
    id_count = int(con.execute("select count(*)::bigint from object_identifiers").fetchone()[0] or 0)
    quarantine_count = int(con.execute("select count(*)::bigint from identifier_quarantine").fetchone()[0] or 0)
    dist_violations = int(
        con.execute(
            """
            select
              (
                select count(*)::bigint from stars
                where dist_ly is not null and x_helio_ly is not null and y_helio_ly is not null and z_helio_ly is not null
                  and abs(sqrt(x_helio_ly*x_helio_ly + y_helio_ly*y_helio_ly + z_helio_ly*z_helio_ly) - dist_ly) > 1e-3
              )
              +
              (
                select count(*)::bigint from systems
                where dist_ly is not null and x_helio_ly is not null and y_helio_ly is not null and z_helio_ly is not null
                  and abs(sqrt(x_helio_ly*x_helio_ly + y_helio_ly*y_helio_ly + z_helio_ly*z_helio_ly) - dist_ly) > 1e-3
              )
            """
        ).fetchone()[0]
        or 0
    )
    lifecycle_stale = int(planet_reclassification_report.get("stale_classifier_rows") or 0)
    lifecycle_status_raw_rows = int(planet_catalog_delta_report.get("status_raw_rows") or 0)
    lifecycle_status_matched_rows = int(planet_catalog_delta_report.get("status_matched_rows") or 0)
    lifecycle_feature_raw_rows = int(planet_catalog_delta_report.get("feature_raw_rows") or 0)
    candidate_rows = int(
        con.execute("select count(*)::bigint from planets where planet_status = 'candidate'").fetchone()[0] or 0
    )
    controversial_rows = int(
        con.execute("select count(*)::bigint from planets where planet_status = 'controversial'").fetchone()[0] or 0
    )
    retracted_rows = int(
        con.execute("select count(*)::bigint from planets where planet_status = 'retracted'").fetchone()[0] or 0
    )
    default_visible_rows = int(
        con.execute("select count(*)::bigint from planets where coalesce(is_default_visible, false)").fetchone()[0] or 0
    )

    qc_report = {
        "build_id": build_id,
        "counts": {
            "stars": stars_count,
            "systems": systems_count,
            "planets": planets_count,
            "compact_objects": compact_count,
            "superstellar_objects": superstellar_count,
            "eclipsing_binaries": eclipsing_count,
            "aliases": aliases_count,
            "object_identifiers": id_count,
            "identifier_quarantine": quarantine_count,
        },
        "gaia_backbone_enabled": metadata.get("gaia_backbone_enabled", "1") == "1",
        "exoplanet_lifecycle_catalogs_enabled": bool(enable_lifecycle),
        "planet_classifier_version": planet_classifier_version,
        "planet_lifecycle_status_raw_rows": lifecycle_status_raw_rows,
        "planet_lifecycle_status_matched_rows": lifecycle_status_matched_rows,
        "planet_lifecycle_feature_raw_rows": lifecycle_feature_raw_rows,
        "planet_lifecycle_stale_classifier_rows": lifecycle_stale,
        "planet_lifecycle_candidate_rows": candidate_rows,
        "planet_lifecycle_controversial_rows": controversial_rows,
        "planet_lifecycle_retracted_rows": retracted_rows,
        "planet_lifecycle_default_visible_rows": default_visible_rows,
        "dist_invariant_violations": dist_violations,
        "provenance_missing_stars": 0,
        "notes": [
            f"Incremental planet refresh from parent build {parent_build_id}.",
            "Stars/systems/object identifiers retained from parent build.",
            "Planets and lifecycle side tables fully rebuilt from cooked inputs.",
            (
                "Host-name promotions from planet hosts: "
                f"stars={host_name_stats.get('host_name_star_promotions', 0)}, "
                f"systems={host_name_stats.get('host_name_system_promotions', 0)}, "
                f"aliases={host_name_stats.get('host_name_alias_inserted', 0)}."
            ),
        ],
    }
    match_report = {
        "build_id": build_id,
        "match_counts": [
            {"method": row[0], "count": int(row[1] or 0)}
            for row in con.execute(
                "select match_method, count(*)::bigint from planets group by 1 order by count(*) desc, match_method asc"
            ).fetchall()
        ],
        "notes": "Incremental mode: match counts reported for planets after refresh.",
    }
    provenance_report = {
        "build_id": build_id,
        "mode": "incremental_planet_refresh",
        "parent_build_id": parent_build_id,
        "nasa_exoplanet_archive": {
            "url": nasa_url,
            "sha256": nasa_sha,
            "retrieved_at": nasa_retrieved,
        },
        "lifecycle_sources": {
            "exoplanet_eu": select_manifest_entry(exoplanet_eu_manifest, "catalog_csv"),
            "open_exoplanet_catalogue": select_manifest_entry(oec_manifest, "catalog_tarball"),
            "hwc": select_manifest_entry(hwc_manifest, "hwc_full_csv"),
            "emac_tt9": select_manifest_entry(emac_manifest, "tt9_source"),
        },
    }

    write_json(reports_dir / "qc_report.json", qc_report)
    write_json(reports_dir / "match_report.json", match_report)
    write_json(reports_dir / "provenance_report.json", provenance_report)
    write_json(reports_dir / "planet_catalog_delta_report.json", planet_catalog_delta_report)
    write_json(reports_dir / "planet_reclassification_report.json", planet_reclassification_report)
    clone_parent_duplicate_trap_report(
        reports_root=reports_root,
        parent_build_id=parent_build_id,
        build_id=build_id,
        ingested_at=ingested_at,
    )

    # Refresh planets parquet in the cloned parquet directory.
    planets_parquet = parquet_dir / "planets.parquet"
    if planets_parquet.exists() or planets_parquet.is_symlink():
        planets_parquet.unlink()
    planets_parquet_sql = str(planets_parquet).replace("'", "''")
    con.execute(
        f"COPY (SELECT * FROM planets ORDER BY spatial_index) TO '{planets_parquet_sql}' (FORMAT 'parquet')"
    )
    if int(host_name_stats.get("host_name_star_promotions") or 0) > 0:
        stars_parquet = parquet_dir / "stars.parquet"
        systems_parquet = parquet_dir / "systems.parquet"
        if stars_parquet.exists() or stars_parquet.is_symlink():
            stars_parquet.unlink()
        if systems_parquet.exists() or systems_parquet.is_symlink():
            systems_parquet.unlink()
        stars_parquet_sql = str(stars_parquet).replace("'", "''")
        systems_parquet_sql = str(systems_parquet).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM stars ORDER BY spatial_index) TO '{stars_parquet_sql}' (FORMAT 'parquet')"
        )
        con.execute(
            f"COPY (SELECT * FROM systems ORDER BY spatial_index) TO '{systems_parquet_sql}' (FORMAT 'parquet')"
        )
    if int(host_name_stats.get("host_name_alias_inserted") or 0) > 0:
        aliases_parquet = parquet_dir / "aliases.parquet"
        if aliases_parquet.exists() or aliases_parquet.is_symlink():
            aliases_parquet.unlink()
        aliases_parquet_sql = str(aliases_parquet).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM aliases ORDER BY target_type, target_id, alias_priority, alias_norm) TO '{aliases_parquet_sql}' (FORMAT 'parquet')"
        )
    con.close()

    # Rebuild arm for consistency.
    arm_builder = root / "scripts" / "build_arm.py"
    if not arm_builder.exists():
        raise SystemExit(f"Missing arm builder script: {arm_builder}")
    arm_proc = subprocess.run(
        [
            sys.executable,
            str(arm_builder),
            "--core-db",
            str(core_db),
            "--arm-db",
            str(arm_db),
            "--state-dir",
            str(state_dir),
            "--build-id",
            build_id,
            "--ingested-at",
            ingested_at,
            "--transform-version",
            transform_version,
            "--report-path",
            str(reports_dir / "arm_report.json"),
        ],
        capture_output=True,
        text=True,
    )
    if arm_proc.returncode != 0:
        raise SystemExit(f"Arm rebuild failed: {(arm_proc.stderr or arm_proc.stdout).strip()}")

    tmp_out_dir.rename(final_out_dir)
    log(
        "Incremental planet ingest complete "
        f"(build={build_id}, parent={parent_build_id}, planets={planets_count}, matched={matched_planet_count}, "
        f"host_star_promotions={host_name_stats.get('host_name_star_promotions', 0)}, "
        f"host_system_promotions={host_name_stats.get('host_name_system_promotions', 0)})"
    )
    print(str(final_out_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
