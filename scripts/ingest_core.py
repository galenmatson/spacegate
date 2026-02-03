#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import subprocess
from pathlib import Path

import duckdb

PC_TO_LY = 3.26156
MORTON_OFFSET = 5000.0
MORTON_SCALE = 1000.0
MORTON_BITS = 23  # Spec says 21, but 23 is needed to cover +/-1000 ly with offset+scale.


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


def load_manifest(manifest_path: Path) -> dict:
    if not manifest_path.exists():
        return {}
    data = json.loads(manifest_path.read_text())
    return {entry.get("source_name"): entry for entry in data}


def morton3d(x: float, y: float, z: float) -> int | None:
    if x is None or y is None or z is None:
        return None
    try:
        xi = int(round((x + MORTON_OFFSET) * MORTON_SCALE))
        yi = int(round((y + MORTON_OFFSET) * MORTON_SCALE))
        zi = int(round((z + MORTON_OFFSET) * MORTON_SCALE))
    except Exception:
        return None
    if xi < 0 or yi < 0 or zi < 0:
        return None
    max_val = (1 << MORTON_BITS) - 1
    if xi > max_val or yi > max_val or zi > max_val:
        return None

    def part1by2(n: int) -> int:
        n &= max_val
        out = 0
        for i in range(MORTON_BITS):
            out |= ((n >> i) & 1) << (3 * i)
        return out

    return part1by2(xi) | (part1by2(yi) << 1) | (part1by2(zi) << 2)


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--build-id", default=None)
    args = parser.parse_args()

    root = Path(args.root)
    cooked_athyg = root / "cooked" / "athyg" / "athyg.csv.gz"
    cooked_nasa = root / "cooked" / "nasa_exoplanet_archive" / "pscomppars_clean.csv"
    manifest_path = root / "raw" / "manifests" / "core_manifest.json"

    if not cooked_athyg.exists():
        raise SystemExit(f"Missing cooked AT-HYG: {cooked_athyg}")
    if not cooked_nasa.exists():
        raise SystemExit(f"Missing cooked NASA: {cooked_nasa}")

    manifest = load_manifest(manifest_path)

    today = dt.date.today().strftime("%Y-%m-%d")
    build_id = args.build_id or f"{today}_{get_git_sha(root)}"

    out_dir = root / "out" / build_id
    parquet_dir = out_dir / "parquet"
    reports_dir = root / "reports" / build_id

    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    db_path = out_dir / "core.duckdb"
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    con.create_function("morton3d", morton3d)

    ingested_at = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    transform_version = get_git_sha(root)

    athyg_p1 = manifest.get("athyg_v33-1", {})
    athyg_p2 = manifest.get("athyg_v33-2", {})
    nasa_manifest = manifest.get("pscomppars", {})

    athyg_p1_url = athyg_p1.get("url", "https://codeberg.org/astronexus/athyg")
    athyg_p2_url = athyg_p2.get("url", "https://codeberg.org/astronexus/athyg")

    athyg_p1_sha = athyg_p1.get("sha256")
    athyg_p2_sha = athyg_p2.get("sha256")
    athyg_p1_retrieved = athyg_p1.get("retrieved_at")
    athyg_p2_retrieved = athyg_p2.get("retrieved_at")

    athyg_checksum = ",".join([s for s in [athyg_p1_sha, athyg_p2_sha] if s]) or None
    athyg_retrieved = max([t for t in [athyg_p1_retrieved, athyg_p2_retrieved] if t], default=None)
    athyg_download_url = ";".join([u for u in [athyg_p1_url, athyg_p2_url] if u])
    if not athyg_download_url:
        athyg_download_url = None

    nasa_url = nasa_manifest.get(
        "url",
        "https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+pscomppars&format=csv",
    )
    nasa_sha = nasa_manifest.get("sha256")
    nasa_retrieved = nasa_manifest.get("retrieved_at")

    con.execute(
        """
        create or replace temp view athyg_raw as
        select * from read_csv_auto(?,
            compression='gzip',
            delim=',',
            quote='"',
            escape='"',
            header=true,
            strict_mode=false,
            null_padding=true,
            all_varchar=true
        )
        """,
        [str(cooked_athyg)],
    )

    # Build stars table
    con.execute(
        """
        create or replace temp view stars_stage as
        with base as (
          select
            nullif(id,'')::bigint as source_pk,
            nullif(gaia,'')::bigint as gaia_id,
            nullif(hip,'')::bigint as hip_id,
            nullif(hd,'')::bigint as hd_id,
            nullif(hr,'')::bigint as hr_id,
            nullif(gl,'') as gl_id,
            nullif(tyc,'') as tyc_id,
            nullif(hyg,'')::bigint as hyg_id,
            nullif(bayer,'') as bayer,
            nullif(flam,'') as flam,
            nullif(con,'') as con,
            nullif(proper,'') as proper,
            nullif(ra,'')::double as ra_deg,
            nullif(dec,'')::double as dec_deg,
            nullif(dist,'')::double as dist_pc,
            nullif(x0,'')::double as x_pc,
            nullif(y0,'')::double as y_pc,
            nullif(z0,'')::double as z_pc,
            nullif(mag,'')::double as vmag,
            nullif(absmag,'')::double as absmag,
            nullif(ci,'')::double as color_index,
            nullif(rv,'')::double as radial_velocity_kms,
            nullif(pm_ra,'')::double as pm_ra_mas_yr,
            nullif(pm_dec,'')::double as pm_dec_mas_yr,
            nullif(vx,'')::double as vx_kms,
            nullif(vy,'')::double as vy_kms,
            nullif(vz,'')::double as vz_kms,
            nullif(spect,'') as spectral_type_raw,
            nullif(pos_src,'') as pos_src,
            nullif(dist_src,'') as dist_src,
            nullif(mag_src,'') as mag_src,
            nullif(rv_src,'') as rv_src,
            nullif(pm_src,'') as pm_src,
            nullif(spect_src,'') as spect_src
          from athyg_raw
        ), coords as (
          select *,
            coalesce(dist_pc, sqrt(x_pc*x_pc + y_pc*y_pc + z_pc*z_pc)) as dist_pc_final
          from base
        ), converted as (
          select *,
            dist_pc_final * ? as dist_ly,
            x_pc * ? as x_helio_ly,
            y_pc * ? as y_helio_ly,
            z_pc * ? as z_helio_ly
          from coords
        ), named as (
          select *,
            case
              when proper is not null then proper
              when bayer is not null and con is not null then bayer || ' ' || con
              when flam is not null and con is not null then flam || ' ' || con
              when hip_id is not null then 'HIP ' || hip_id::varchar
              when hd_id is not null then 'HD ' || hd_id::varchar
              when gaia_id is not null then 'Gaia DR3 ' || gaia_id::varchar
              else null
            end as star_name,
            regexp_extract(proper, ' ([A-Za-z]{1,2})$', 1) as component,
            case
              when proper is not null then regexp_replace(proper, '\\s+[A-Za-z]{1,2}$', '')
              else null
            end as system_name_root
          from converted
        ), normalized as (
          select *,
            case when star_name is null then null else
              lower(trim(regexp_replace(regexp_replace(star_name, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g')))
            end as star_name_norm,
            case when system_name_root is null then null else
              lower(trim(regexp_replace(regexp_replace(system_name_root, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g')))
            end as system_name_root_norm,
            regexp_extract(spectral_type_raw, '([OBAFGKMLTY])', 1) as spectral_class,
            regexp_extract(spectral_type_raw, '[OBAFGKMLTY]([0-9](?:\\.[0-9])?)', 1) as spectral_subtype,
            regexp_extract(spectral_type_raw, '(I{1,3}|IV|V|VI|VII)', 1) as luminosity_class
          from named
        ), filtered as (
          select * from normalized where dist_ly is not null and dist_ly <= 1000
        )
        select
          *,
          case
            when gaia_id is not null then 'star:gaia:' || gaia_id::varchar
            when hip_id is not null then 'star:hip:' || hip_id::varchar
            when hd_id is not null then 'star:hd:' || hd_id::varchar
            else 'star:hash:' || substr(sha256(
              coalesce(star_name_norm,'') || '|' ||
              coalesce(round(ra_deg,5)::varchar,'') || '|' ||
              coalesce(round(dec_deg,5)::varchar,'') || '|' ||
              coalesce(round(dist_ly,3)::varchar,'')
            ), 1, 16)
          end as stable_object_key,
          morton3d(x_helio_ly, y_helio_ly, z_helio_ly) as spatial_index
        from filtered
        """,
        [PC_TO_LY, PC_TO_LY, PC_TO_LY, PC_TO_LY],
    )

    con.execute(
        """
        create table stars as
        select
          row_number() over ()::bigint as star_id,
          spatial_index,
          null::bigint as system_id,
          stable_object_key,
          star_name,
          star_name_norm,
          component,
          system_name_root,
          system_name_root_norm,
          ra_deg,
          dec_deg,
          dist_ly,
          x_helio_ly,
          y_helio_ly,
          z_helio_ly,
          null::double as x_gal_ly,
          null::double as y_gal_ly,
          null::double as z_gal_ly,
          pm_ra_mas_yr,
          pm_dec_mas_yr,
          radial_velocity_kms,
          spectral_type_raw,
          spectral_class,
          spectral_subtype,
          luminosity_class,
          null::varchar as spectral_peculiar,
          vmag,
          absmag,
          color_index,
          gaia_id,
          hip_id,
          hd_id,
          json_object('gaia', gaia_id, 'hip', hip_id, 'hd', hd_id, 'hr', hr_id, 'gl', gl_id, 'tyc', tyc_id, 'hyg', hyg_id) as catalog_ids_json,
          'athyg' as source_catalog,
          'v3.3' as source_version,
          'https://codeberg.org/astronexus/athyg' as source_url,
          ? as source_download_url,
          null::varchar as source_doi,
          source_pk as source_pk,
          source_pk as source_row_id,
          null::varchar as source_row_hash,
          'CC BY-SA 4.0' as license,
          true as redistribution_ok,
          'https://codeberg.org/astronexus/athyg' as license_note,
          null::varchar as retrieval_etag,
          ? as retrieval_checksum,
          ? as retrieved_at,
          ? as ingested_at,
          ? as transform_version,
          system_name_root_norm
        from stars_stage
        """,
        [athyg_download_url, athyg_checksum, athyg_retrieved, ingested_at, transform_version],
    )

    # Systems (name-based only)
    con.execute(
        """
        create table systems as
        with groups as (
          select
            case
              when system_name_root_norm is not null then 'name:' || system_name_root_norm
              else 'star:' || stable_object_key
            end as system_group_key,
            *
          from stars
        ), primary_star as (
          select *,
            row_number() over (partition by system_group_key order by vmag asc nulls last) as rn
          from groups
        ), system_rows as (
          select * from primary_star where rn = 1
        )
        select
          row_number() over (order by system_group_key)::bigint as system_id,
          spatial_index,
          case
            when stable_object_key like 'star:gaia:%' then replace(stable_object_key, 'star:gaia:', 'system:gaia:')
            when stable_object_key like 'star:hip:%' then replace(stable_object_key, 'star:hip:', 'system:hip:')
            when stable_object_key like 'star:hd:%' then replace(stable_object_key, 'star:hd:', 'system:hd:')
            else replace(stable_object_key, 'star:', 'system:')
          end as stable_object_key,
          coalesce(system_name_root, star_name) as system_name,
          coalesce(system_name_root_norm, star_name_norm) as system_name_norm,
          ra_deg,
          dec_deg,
          dist_ly,
          x_helio_ly,
          y_helio_ly,
          z_helio_ly,
          null::double as x_gal_ly,
          null::double as y_gal_ly,
          null::double as z_gal_ly,
          gaia_id,
          hip_id,
          hd_id,
          'athyg' as source_catalog,
          'v3.3' as source_version,
          'https://codeberg.org/astronexus/athyg' as source_url,
          ? as source_download_url,
          null::varchar as source_doi,
          source_pk as source_pk,
          source_pk as source_row_id,
          null::varchar as source_row_hash,
          'CC BY-SA 4.0' as license,
          true as redistribution_ok,
          'https://codeberg.org/astronexus/athyg' as license_note,
          null::varchar as retrieval_etag,
          ? as retrieval_checksum,
          ? as retrieved_at,
          ? as ingested_at,
          ? as transform_version,
          system_group_key
        from system_rows
        """,
        [athyg_download_url, athyg_checksum, athyg_retrieved, ingested_at, transform_version],
    )

    # Assign system_id to stars
    con.execute(
        """
        update stars
        set system_id = systems.system_id
        from systems
        where (
          case
            when stars.system_name_root_norm is not null then 'name:' || stars.system_name_root_norm
            else 'star:' || stars.stable_object_key
          end
        ) = systems.system_group_key
        """
    )

    con.execute("alter table systems drop column system_group_key")
    con.execute("alter table stars drop column system_name_root")
    con.execute("alter table stars drop column system_name_root_norm")

    # Planets
    con.execute(
        """
        create or replace temp view nasa_raw as
        select * from read_csv_auto(?, header=true, all_varchar=true)
        """,
        [str(cooked_nasa)],
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
            nullif(sy_dist,'')::double as host_dist_pc
          from nasa_raw
        ), normalized as (
          select *,
            case when planet_name is null then null else
              lower(trim(regexp_replace(regexp_replace(planet_name, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g')))
            end as planet_name_norm,
            case when host_name_raw is null then null else
              lower(trim(regexp_replace(regexp_replace(host_name_raw, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g')))
            end as host_name_norm,
            cast(regexp_extract(hip_name, '(\\d+)', 1) as bigint) as host_hip_id,
            cast(regexp_extract(hd_name, '(\\d+)', 1) as bigint) as host_hd_id,
            cast(regexp_extract(coalesce(gaia_dr3_id, gaia_dr2_id, ''), '(\\d+)', 1) as bigint) as host_gaia_id
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
          row_number() over ()::bigint as planet_id,
          morton3d(s.x_helio_ly, s.y_helio_ly, s.z_helio_ly) as spatial_index,
          case
            when planet_name_norm is null then null
            when count(*) over (partition by planet_name_norm) = 1 then 'planet:nasa:' || planet_name_norm
            else 'planet:nasa:' || planet_name_norm || ':' || source_pk::varchar
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
          ? as source_download_url,
          null::varchar as source_doi,
          source_pk as source_pk,
          source_pk as source_row_id,
          null::varchar as source_row_hash,
          'NASA Exoplanet Archive' as license,
          true as redistribution_ok,
          'https://exoplanetarchive.ipac.caltech.edu' as license_note,
          null::varchar as retrieval_etag,
          ? as retrieval_checksum,
          ? as retrieved_at,
          ? as ingested_at,
          ? as transform_version
        from matches
        left join stars s on s.star_id = coalesce(gaia_star_id, hip_star_id, hd_star_id, name_star_id)
        """,
        [nasa_url, nasa_sha, nasa_retrieved, ingested_at, transform_version],
    )

    # Provenance already applied with combined checksums.
    # Reports
    counts = con.execute(
        """
        select
          (select count(*) from stars) as stars,
          (select count(*) from systems) as systems,
          (select count(*) from planets) as planets
        """
    ).fetchone()

    match_counts = con.execute(
        """
        select match_method, count(*) as count from planets group by match_method order by count desc
        """
    ).fetchall()

    dist_violations = con.execute(
        """
        select count(*) from stars
        where dist_ly is not null and x_helio_ly is not null and y_helio_ly is not null and z_helio_ly is not null
          and abs(sqrt(x_helio_ly*x_helio_ly + y_helio_ly*y_helio_ly + z_helio_ly*z_helio_ly) - dist_ly) > 1e-3
        """
    ).fetchone()[0]

    provenance_missing = con.execute(
        """
        select
          sum(case when source_catalog is null or source_version is null or source_url is null or source_pk is null
                    or license is null or retrieved_at is null or transform_version is null or ingested_at is null
               then 1 else 0 end) as missing
        from stars
        """
    ).fetchone()[0]

    qc_report = {
        "build_id": build_id,
        "counts": {"stars": counts[0], "systems": counts[1], "planets": counts[2]},
        "dist_invariant_violations": dist_violations,
        "provenance_missing_stars": provenance_missing,
        "notes": [
            "System grouping is name-based only; proximity-based grouping not yet implemented.",
            "Morton code uses 23 bits to cover +/-1000 ly with offset+scale; spec currently says 21 bits.",
        ],
    }

    match_report = {
        "build_id": build_id,
        "match_counts": [{"method": row[0], "count": row[1]} for row in match_counts],
    }

    provenance_report = {
        "build_id": build_id,
        "athyg": {
            "source_url": "https://codeberg.org/astronexus/athyg",
            "part1": athyg_p1,
            "part2": athyg_p2,
        },
        "nasa_exoplanet_archive": nasa_manifest,
    }

    write_json(reports_dir / "qc_report.json", qc_report)
    write_json(reports_dir / "match_report.json", match_report)
    write_json(reports_dir / "provenance_report.json", provenance_report)

    # Parquet exports (sorted by spatial_index)
    con.execute(
        f"COPY (SELECT * FROM stars ORDER BY spatial_index) TO '{parquet_dir / 'stars.parquet'}' (FORMAT 'parquet')"
    )
    con.execute(
        f"COPY (SELECT * FROM systems ORDER BY spatial_index) TO '{parquet_dir / 'systems.parquet'}' (FORMAT 'parquet')"
    )
    con.execute(
        f"COPY (SELECT * FROM planets ORDER BY spatial_index) TO '{parquet_dir / 'planets.parquet'}' (FORMAT 'parquet')"
    )

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
