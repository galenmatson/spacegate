#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import math
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb
from astropy.table import Table


CATWISE_COLUMNS = (
    "source_name,source_id,ra,dec,w1mpro,w2mpro,w1snr,w2snr,pmra,pmdec,"
    "sigpmra,sigpmdec,par_pm,par_pmSig,cc_flags,ab_flags,n_aw,dist_cc"
)
ALLWISE_COLUMNS = (
    "designation,ra,dec,w1mpro,w2mpro,w3mpro,w4mpro,w1snr,w2snr,w3snr,w4snr,"
    "pmra,pmdec,sigpmra,sigpmdec,cc_flags,ph_qual,ext_flg,nb,na"
)
GATOR_URL = "https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query"
CATWISE_VERSION = "CatWISE2020"
ALLWISE_VERSION = "AllWISE Source Catalog"
SOURCE_POSITION_EPOCH_YEAR = 2016.0
CATWISE_QUERY_EPOCH_YEAR = 2015.40
ALLWISE_QUERY_EPOCH_YEAR = 2010.50


PUBLIC_GOLDEN_NAMES = [
    "tau ceti",
    "trappist 1",
    "alpha centauri",
    "proxima centauri",
    "sirius",
    "55 cnc",
    "epsilon eridani",
    "barnard s star",
    "wolf 359",
    "vega",
    "fomalhaut",
    "luhman 16",
    "wise 0855",
    "ugps j0722",
]


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def clean_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if hasattr(value, "mask") and bool(value.mask):
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return number
    except Exception:
        return None


def clean_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if hasattr(value, "mask") and bool(value.mask):
            return None
        return int(value)
    except Exception:
        return None


def clean_text(value: Any) -> str:
    try:
        if value is None:
            return ""
        if hasattr(value, "mask") and bool(value.mask):
            return ""
        return str(value).strip()
    except Exception:
        return ""


def propagate_position(
    *,
    ra_deg: float,
    dec_deg: float,
    pm_ra_mas_yr: float | None,
    pm_dec_mas_yr: float | None,
    from_epoch_year: float,
    to_epoch_year: float,
) -> tuple[float, float]:
    if pm_ra_mas_yr is None and pm_dec_mas_yr is None:
        return ra_deg, dec_deg
    delta_years = to_epoch_year - from_epoch_year
    dec_radians = math.radians(dec_deg)
    cos_dec = max(0.01, abs(math.cos(dec_radians)))
    ra_shift_deg = ((pm_ra_mas_yr or 0.0) * delta_years) / (1000.0 * 3600.0 * cos_dec)
    dec_shift_deg = ((pm_dec_mas_yr or 0.0) * delta_years) / (1000.0 * 3600.0)
    return (ra_deg + ra_shift_deg) % 360.0, max(-90.0, min(90.0, dec_deg + dec_shift_deg))


def query_gator(
    *,
    catalog: str,
    ra_deg: float,
    dec_deg: float,
    radius_arcsec: float,
    selcols: str,
    outrows: int,
    timeout_s: float,
) -> Table:
    params = {
        "catalog": catalog,
        "spatial": "cone",
        "objstr": f"{ra_deg:.10f},{dec_deg:.10f}",
        "radius": f"{radius_arcsec:.3f}",
        "outfmt": "3",
        "selcols": selcols,
        "outrows": str(outrows),
    }
    url = f"{GATOR_URL}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "Spacegate WISE evidence collector"})
    data = urlopen(request, timeout=timeout_s).read()
    if b"<INFO name=\"Error\"" in data or b"struct stat=\"ERROR\"" in data:
        raise RuntimeError(data[:1000].decode("utf-8", "replace"))
    try:
        return Table.read(io.BytesIO(data), format="votable")
    except Exception as exc:
        if "No table found" in str(exc):
            return Table()
        raise


def match_confidence(distance_arcsec: float | None, score: float) -> str:
    if distance_arcsec is None:
        return "low"
    if distance_arcsec <= 2.5 and score >= 0.72:
        return "high"
    if distance_arcsec <= 8.0 and score >= 0.45:
        return "medium"
    return "low"


def score_candidate(row: dict[str, Any], radius_arcsec: float) -> float:
    distance = clean_float(row.get("dist"))
    distance_score = 0.0
    if distance is not None:
        distance_score = max(0.0, 1.0 - min(distance, radius_arcsec) / max(radius_arcsec, 0.001))
    snrs = [clean_float(row.get("w1snr")), clean_float(row.get("w2snr"))]
    snr_score = min(1.0, sum(max(0.0, value or 0.0) for value in snrs) / 60.0)
    cc_flags = clean_text(row.get("cc_flags")).lower()
    ab_flags = clean_text(row.get("ab_flags")).lower()
    clean_flag_score = 1.0 if (not cc_flags or set(cc_flags) <= {"0"}) and (not ab_flags or set(ab_flags) <= {"0"}) else 0.45
    w1 = clean_float(row.get("w1mpro"))
    w2 = clean_float(row.get("w2mpro"))
    color_score = 0.0
    if w1 is not None and w2 is not None:
        color_score = min(1.0, max(0.0, (w1 - w2) / 1.2))
    pmra = clean_float(row.get("pmra"))
    pmdec = clean_float(row.get("pmdec"))
    motion_score = 0.0
    if pmra is not None and pmdec is not None:
        motion_score = min(1.0, math.sqrt(pmra * pmra + pmdec * pmdec) / 0.6)
    return (
        distance_score * 0.46
        + snr_score * 0.20
        + clean_flag_score * 0.18
        + color_score * 0.08
        + motion_score * 0.08
    )


def candidate_review_signal(row: dict[str, Any], short_name: str) -> tuple[bool, dict[str, Any]]:
    w1 = clean_float(row.get("w1mpro"))
    w2 = clean_float(row.get("w2mpro"))
    color = (w1 - w2) if w1 is not None and w2 is not None else None
    pmra = clean_float(row.get("pmra"))
    pmdec = clean_float(row.get("pmdec"))
    pm_total = math.sqrt((pmra or 0.0) ** 2 + (pmdec or 0.0) ** 2) if pmra is not None or pmdec is not None else None
    if short_name == "allwise" and pm_total is not None:
        pm_total_arcsec_yr = pm_total / 1000.0
    else:
        pm_total_arcsec_yr = pm_total
    snr_w2 = clean_float(row.get("w2snr")) or 0.0
    artifact_flags = clean_text(row.get("cc_flags")).lower()
    clean_artifacts = not artifact_flags or set(artifact_flags) <= {"0"}
    is_candidate = bool(
        color is not None
        and color >= 0.75
        and pm_total_arcsec_yr is not None
        and pm_total_arcsec_yr >= 0.15
        and snr_w2 >= 5.0
        and clean_artifacts
    )
    return is_candidate, {
        "w1_minus_w2": color,
        "pm_total_arcsec_yr": pm_total_arcsec_yr,
        "w2_snr": snr_w2,
        "artifact_flags": artifact_flags,
    }


def row_dict(table: Table, row: Any) -> dict[str, Any]:
    return {name.lower(): row[name] for name in table.colnames}


def select_targets(con: duckdb.DuckDBPyConnection, limit: int) -> list[dict[str, Any]]:
    golden_terms = ",".join(["?"] * len(PUBLIC_GOLDEN_NAMES))
    rows = con.execute(
        f"""
        with golden_systems as (
          select distinct system_id
          from aliases
          where alias_norm in ({golden_terms})
        ), scored as (
          select
            st.star_id,
            st.system_id,
            st.stable_object_key,
            st.star_name,
            st.ra_deg,
            st.dec_deg,
            st.dist_ly,
            st.pm_ra_mas_yr,
            st.pm_dec_mas_yr,
            st.spectral_class,
            sy.system_name,
            sy.star_count,
            sy.planet_count,
            case
              when st.system_id in (select system_id from golden_systems) then 1000
              when upper(coalesce(st.spectral_class, '')) in ('L', 'T', 'Y') then 900
              when coalesce(sy.planet_count, 0) > 0 then 800
              when coalesce(sy.star_count, 0) > 1 then 700
              when coalesce(st.dist_ly, 999999) <= 25 then 600
              when coalesce(st.dist_ly, 999999) <= 100 then 500
              else 100
            end as priority
          from stars st
          join systems sy on sy.system_id = st.system_id
          where st.ra_deg is not null
            and st.dec_deg is not null
            and st.dist_ly is not null
            and st.dist_ly <= 1000
        )
        select *
        from scored
        order by priority desc, dist_ly asc nulls last, star_id asc
        limit ?
        """,
        [*PUBLIC_GOLDEN_NAMES, int(limit)],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def dedupe_rows(rows: list[dict[str, Any]], key_columns: tuple[str, ...]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(str(row.get(column) or "") for column in key_columns)
        if key not in deduped:
            deduped[key] = row
    return list(deduped.values())


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect targeted CatWISE/AllWISE evidence for Spacegate objects.")
    parser.add_argument("--core-db", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--radius-arcsec", type=float, default=30.0)
    parser.add_argument("--outrows", type=int, default=20)
    parser.add_argument("--max-candidates-per-target", type=int, default=3)
    parser.add_argument("--sleep-s", type=float, default=0.15)
    parser.add_argument("--timeout-s", type=float, default=30.0)
    parser.add_argument("--catalog", choices=["catwise", "allwise", "both"], default="both")
    parser.add_argument("--report-path", default="")
    args = parser.parse_args()

    core_db = Path(args.core_db)
    state_dir = Path(args.state_dir)
    if not core_db.exists():
        raise SystemExit(f"core DB not found: {core_db}")

    output_dir = state_dir / "cooked" / "wise"
    retrieved_at = utc_now()
    con = duckdb.connect(str(core_db), read_only=True)
    targets = select_targets(con, args.limit)
    con.close()

    source_rows: list[dict[str, Any]] = []
    match_rows: list[dict[str, Any]] = []
    photometry_rows: list[dict[str, Any]] = []
    motion_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    catalogs = []
    if args.catalog in {"catwise", "both"}:
        catalogs.append(("catwise_2020", CATWISE_VERSION, CATWISE_COLUMNS, "catwise", CATWISE_QUERY_EPOCH_YEAR))
    if args.catalog in {"allwise", "both"}:
        catalogs.append(("allwise_p3as_psd", ALLWISE_VERSION, ALLWISE_COLUMNS, "allwise", ALLWISE_QUERY_EPOCH_YEAR))

    for index, target in enumerate(targets, start=1):
        source_ra = float(target["ra_deg"])
        source_dec = float(target["dec_deg"])
        pm_ra = clean_float(target.get("pm_ra_mas_yr"))
        pm_dec = clean_float(target.get("pm_dec_mas_yr"))
        for catalog_name, catalog_version, columns, short_name, query_epoch_year in catalogs:
            ra, dec = propagate_position(
                ra_deg=source_ra,
                dec_deg=source_dec,
                pm_ra_mas_yr=pm_ra,
                pm_dec_mas_yr=pm_dec,
                from_epoch_year=SOURCE_POSITION_EPOCH_YEAR,
                to_epoch_year=query_epoch_year,
            )
            try:
                table = query_gator(
                    catalog=catalog_name,
                    ra_deg=ra,
                    dec_deg=dec,
                    radius_arcsec=args.radius_arcsec,
                    selcols=columns,
                    outrows=args.outrows,
                    timeout_s=args.timeout_s,
                )
            except Exception as exc:
                errors.append({
                    "star_id": target.get("star_id"),
                    "system_id": target.get("system_id"),
                    "catalog": short_name,
                    "error": str(exc)[:1000],
                })
                continue
            candidates = []
            for row in table:
                item = row_dict(table, row)
                score = score_candidate(item, args.radius_arcsec)
                candidates.append((score, item))
            candidates.sort(key=lambda pair: pair[0], reverse=True)
            for rank, (score, item) in enumerate(candidates[: args.max_candidates_per_target], start=1):
                if short_name == "catwise":
                    source_designation = clean_text(item.get("source_name"))
                    source_id = clean_text(item.get("source_id"))
                else:
                    source_designation = clean_text(item.get("designation"))
                    source_id = source_designation
                source_key = f"{short_name}:{source_id or source_designation}"
                angular_sep = clean_float(item.get("dist"))
                confidence = match_confidence(angular_sep, score)
                provenance = {
                    "query_ra_deg": ra,
                    "query_dec_deg": dec,
                    "source_ra_deg": source_ra,
                    "source_dec_deg": source_dec,
                    "query_radius_arcsec": args.radius_arcsec,
                    "source_position_epoch_year": SOURCE_POSITION_EPOCH_YEAR,
                    "query_epoch_year": query_epoch_year,
                    "pm_ra_mas_yr": pm_ra,
                    "pm_dec_mas_yr": pm_dec,
                    "irsa_catalog": catalog_name,
                    "retrieved_at": retrieved_at,
                    "collector": "collect_wise_evidence.py",
                    "collector_version": "wise_evidence_v1",
                }
                source_rows.append({
                    "source_catalog": short_name,
                    "source_version": catalog_version,
                    "source_key": source_key,
                    "source_designation": source_designation,
                    "source_id": source_id,
                    "ra_deg": clean_float(item.get("ra")),
                    "dec_deg": clean_float(item.get("dec")),
                    "retrieved_at": retrieved_at,
                    "provenance_json": json.dumps(provenance, sort_keys=True),
                    "source_row_hash": stable_hash(json.dumps({k: clean_text(v) for k, v in item.items()}, sort_keys=True)),
                })
                match_rows.append({
                    "target_type": "star",
                    "target_id": target.get("star_id"),
                    "system_id": target.get("system_id"),
                    "stable_object_key": target.get("stable_object_key") or "",
                    "source_catalog": short_name,
                    "source_version": catalog_version,
                    "source_key": source_key,
                    "source_designation": source_designation,
                    "angular_sep_arcsec": angular_sep,
                    "match_rank": rank,
                    "match_score": round(score, 6),
                    "confidence_tier": confidence,
                    "match_method": "irsa_cone_rank_v1",
                    "conflict_status": "candidate" if confidence == "low" or rank > 1 else "accepted_match",
                    "provenance_json": json.dumps(provenance, sort_keys=True),
                })
                photometry_rows.append({
                    "source_catalog": short_name,
                    "source_version": catalog_version,
                    "source_key": source_key,
                    "target_type": "star",
                    "target_id": target.get("star_id"),
                    "system_id": target.get("system_id"),
                    "w1_mag": clean_float(item.get("w1mpro")),
                    "w2_mag": clean_float(item.get("w2mpro")),
                    "w3_mag": clean_float(item.get("w3mpro")),
                    "w4_mag": clean_float(item.get("w4mpro")),
                    "w1_snr": clean_float(item.get("w1snr")),
                    "w2_snr": clean_float(item.get("w2snr")),
                    "w3_snr": clean_float(item.get("w3snr")),
                    "w4_snr": clean_float(item.get("w4snr")),
                    "quality_flags": clean_text(item.get("ph_qual")),
                    "artifact_flags": clean_text(item.get("cc_flags")),
                    "blend_flags": json.dumps({
                        "ab_flags": clean_text(item.get("ab_flags")),
                        "n_aw": clean_int(item.get("n_aw")),
                        "ext_flg": clean_int(item.get("ext_flg")),
                        "nb": clean_int(item.get("nb")),
                        "na": clean_int(item.get("na")),
                        "dist_cc": clean_float(item.get("dist_cc")),
                    }, sort_keys=True),
                    "provenance_json": json.dumps(provenance, sort_keys=True),
                })
                motion_rows.append({
                    "source_catalog": short_name,
                    "source_version": catalog_version,
                    "source_key": source_key,
                    "target_type": "star",
                    "target_id": target.get("star_id"),
                    "system_id": target.get("system_id"),
                    "pm_ra": clean_float(item.get("pmra")),
                    "pm_dec": clean_float(item.get("pmdec")),
                    "pm_unit": "arcsec/yr" if short_name == "catwise" else "mas/yr",
                    "pm_ra_error": clean_float(item.get("sigpmra")),
                    "pm_dec_error": clean_float(item.get("sigpmdec")),
                    "parallax_like_arcsec": clean_float(item.get("par_pm")),
                    "parallax_like_error_arcsec": clean_float(item.get("par_pmsig")),
                    "parallax_like_note": "CatWISE par_pm is candidate evidence, not Gaia-grade distance authority." if short_name == "catwise" else "",
                    "provenance_json": json.dumps(provenance, sort_keys=True),
                })
                candidate_signal, candidate_basis = candidate_review_signal(item, short_name)
                if candidate_signal:
                    candidate_rows.append({
                        "candidate_status": "needs_review",
                        "candidate_kind": "nearby_ultracool_or_brown_dwarf",
                        "nearest_target_type": "star",
                        "nearest_target_id": target.get("star_id"),
                        "nearest_system_id": target.get("system_id"),
                        "nearest_stable_object_key": target.get("stable_object_key") or "",
                        "source_catalog": short_name,
                        "source_version": catalog_version,
                        "source_key": source_key,
                        "source_designation": source_designation,
                        "ra_deg": clean_float(item.get("ra")),
                        "dec_deg": clean_float(item.get("dec")),
                        "angular_sep_arcsec": angular_sep,
                        "w1_minus_w2": candidate_basis["w1_minus_w2"],
                        "pm_total_arcsec_yr": candidate_basis["pm_total_arcsec_yr"],
                        "w2_snr": candidate_basis["w2_snr"],
                        "candidate_score": round(score, 6),
                        "review_reason": "red_w1_w2_high_motion_wise_candidate_v1",
                        "provenance_json": json.dumps(provenance, sort_keys=True),
                    })
            time.sleep(max(0.0, args.sleep_s))
        if index % 25 == 0:
            print(f"{utc_now()} WISE collection progress {index}/{len(targets)}", flush=True)

    source_columns = [
        "source_catalog", "source_version", "source_key", "source_designation", "source_id",
        "ra_deg", "dec_deg", "retrieved_at", "provenance_json", "source_row_hash",
    ]
    match_columns = [
        "target_type", "target_id", "system_id", "stable_object_key", "source_catalog",
        "source_version", "source_key", "source_designation", "angular_sep_arcsec",
        "match_rank", "match_score", "confidence_tier", "match_method",
        "conflict_status", "provenance_json",
    ]
    photometry_columns = [
        "source_catalog", "source_version", "source_key", "target_type", "target_id",
        "system_id", "w1_mag", "w2_mag", "w3_mag", "w4_mag", "w1_snr", "w2_snr",
        "w3_snr", "w4_snr", "quality_flags", "artifact_flags", "blend_flags",
        "provenance_json",
    ]
    motion_columns = [
        "source_catalog", "source_version", "source_key", "target_type", "target_id",
        "system_id", "pm_ra", "pm_dec", "pm_unit", "pm_ra_error", "pm_dec_error",
        "parallax_like_arcsec", "parallax_like_error_arcsec", "parallax_like_note",
        "provenance_json",
    ]
    candidate_columns = [
        "candidate_status", "candidate_kind", "nearest_target_type", "nearest_target_id",
        "nearest_system_id", "nearest_stable_object_key", "source_catalog", "source_version",
        "source_key", "source_designation", "ra_deg", "dec_deg", "angular_sep_arcsec",
        "w1_minus_w2", "pm_total_arcsec_yr", "w2_snr", "candidate_score",
        "review_reason", "provenance_json",
    ]
    source_rows = dedupe_rows(source_rows, ("source_catalog", "source_key"))
    candidate_rows = dedupe_rows(candidate_rows, ("source_catalog", "source_key", "nearest_system_id"))
    write_csv(output_dir / "wise_sources.csv", source_rows, source_columns)
    write_csv(output_dir / "infrared_source_matches.csv", match_rows, match_columns)
    write_csv(output_dir / "infrared_photometry.csv", photometry_rows, photometry_columns)
    write_csv(output_dir / "infrared_motion_evidence.csv", motion_rows, motion_columns)
    write_csv(output_dir / "infrared_candidate_queue.csv", candidate_rows, candidate_columns)

    report = {
        "schema_version": "wise_evidence_collection_report_v1",
        "retrieved_at": retrieved_at,
        "target_count": len(targets),
        "source_rows": len(source_rows),
        "match_rows": len(match_rows),
        "photometry_rows": len(photometry_rows),
        "motion_rows": len(motion_rows),
        "candidate_rows": len(candidate_rows),
        "error_count": len(errors),
        "errors": errors[:50],
        "output_dir": str(output_dir),
        "policy": "WISE/CatWISE/AllWISE rows are ARM evidence only; no core promotion.",
    }
    report_path = Path(args.report_path) if args.report_path else state_dir / "reports" / "wise_evidence_collection_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
