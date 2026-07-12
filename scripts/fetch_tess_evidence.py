#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import tempfile
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

NASA_TAP_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
MAST_INVOKE_URL = "https://mast.stsci.edu/api/v0/invoke"
GAIA_TAP_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
TOI_QUERY = "select * from toi order by toi"
TIC_COLUMNS = (
    "ID,version,HIP,TYC,TWOMASS,GAIA,objType,typeSrc,ra,dec,pmRA,e_pmRA,"
    "pmDEC,e_pmDEC,plx,e_plx,Tmag,e_Tmag,d,e_d,disposition,duplicate_id,objID"
)
TOI_COOKED_FIELDS = [
    "source_key", "tic_id", "toi", "toi_display", "toi_prefix", "ctoi_alias",
    "planet_number", "disposition", "ra_deg", "dec_deg", "pm_ra_mas_yr",
    "pm_dec_mas_yr", "tmag", "transit_epoch_bjd", "transit_epoch_err_plus",
    "transit_epoch_err_minus", "orbital_period_days", "orbital_period_err_plus",
    "orbital_period_err_minus", "transit_duration_hours",
    "transit_duration_err_plus", "transit_duration_err_minus", "transit_depth_ppm",
    "transit_depth_err_plus", "transit_depth_err_minus", "planet_radius_earth",
    "planet_radius_err_plus", "planet_radius_err_minus", "insolation_earth",
    "insolation_err_plus", "insolation_err_minus", "equilibrium_temp_k",
    "stellar_distance_pc", "stellar_teff_k", "stellar_logg_cgs",
    "stellar_radius_solar", "sectors", "toi_created", "row_updated_at",
    "release_date", "source_row_hash"
]
TIC_COOKED_FIELDS = [
    "tic_id", "tic_version", "hip_id", "tyc_id", "twomass_id", "gaia_dr2_id",
    "object_type", "type_source", "ra_deg", "dec_deg", "pm_ra_mas_yr",
    "pm_ra_error_mas_yr", "pm_dec_mas_yr", "pm_dec_error_mas_yr",
    "parallax_mas", "parallax_error_mas", "tmag", "tmag_error",
    "distance_pc", "distance_error_pc", "disposition", "duplicate_id",
    "mast_object_id", "source_row_hash"
]
NEIGHBOUR_FIELDS = [
    "dr2_source_id", "dr3_source_id", "angular_distance_arcsec",
    "magnitude_difference", "number_of_neighbours", "proper_motion_propagation"
]
GAIA_DR3_FIELDS = [
    "source_id", "ra_deg", "dec_deg", "parallax_mas", "parallax_error_mas",
    "pm_ra_mas_yr", "pm_dec_mas_yr", "ruwe", "phot_g_mag", "bp_rp"
]
GAIA_EXTERNAL_FIELDS = [
    "namespace", "external_id", "dr3_source_id", "angular_distance_arcsec",
    "number_of_neighbours", "xm_flag"
]
TOI_HISTORY_FIELDS = [
    "source_key", "tic_id", "toi_display", "disposition", "effective_at",
    "release_date", "source_row_hash", "first_observed_at", "last_observed_at"
]


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def log(message: str) -> None:
    print(f"{utc_now()} {message}", flush=True)


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def row_hash(value: dict[str, Any]) -> str:
    return sha256_bytes(stable_json(value).encode("utf-8"))


def atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=path.parent, prefix=path.name + ".tmp.") as handle:
        handle.write(payload)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write(path, (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def cached_payload(path: Path, request_text: str, *, resume_raw: bool) -> bytes | None:
    request_hash_path = path.with_suffix(path.suffix + ".request.sha256")
    expected_hash = sha256_bytes(request_text.encode("utf-8"))
    if not resume_raw or not path.exists() or not request_hash_path.exists():
        return None
    if request_hash_path.read_text(encoding="ascii").strip() != expected_hash:
        return None
    return path.read_bytes()


def write_cached_payload(path: Path, payload: bytes, request_text: str) -> None:
    atomic_write(path, payload)
    atomic_write(
        path.with_suffix(path.suffix + ".request.sha256"),
        (sha256_bytes(request_text.encode("utf-8")) + "\n").encode("ascii"),
    )


def fetch_bytes(
    url: str,
    *,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout_s: int,
    retries: int,
) -> bytes:
    request_headers = {"User-Agent": USER_AGENT}
    request_headers.update(headers or {})
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, data=data, headers=request_headers)
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                return response.read()
        except Exception as exc:  # pragma: no cover - network failure path
            last_error = exc
            if attempt == retries:
                break
            sleep_s = min(2**attempt, 30)
            log(f"request retry {attempt}/{retries - 1}: {type(exc).__name__}: {exc}")
            time.sleep(sleep_s)
    raise RuntimeError(f"request failed after {retries} attempts: {last_error}")


def parse_tic_id(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if text.startswith("TIC"):
        text = text[3:].strip()
    digits = "".join(character for character in text if character.isdigit())
    return str(int(digits)) if digits else None


def chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def read_csv_tic_ids(path: Path, column: str) -> set[str]:
    if not path.exists():
        return set()
    result: set[str] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            tic_id = parse_tic_id(row.get(column))
            if tic_id:
                result.add(tic_id)
    return result


def read_operator_seeds(path: Path) -> tuple[str, set[str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("tic_ids"), list):
        raise ValueError(f"Invalid TESS target seed config: {path}")
    ids = {tic_id for value in payload["tic_ids"] if (tic_id := parse_tic_id(value))}
    return str(payload.get("version") or "unversioned"), ids


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", newline="", encoding="utf-8", delete=False,
        dir=path.parent, prefix=path.name + ".tmp."
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def fetch_toi(raw_dir: Path, *, timeout_s: int, retries: int) -> tuple[Path, list[dict[str, str]]]:
    query_url = NASA_TAP_URL + "?" + urllib.parse.urlencode({"query": TOI_QUERY, "format": "csv"})
    payload = fetch_bytes(query_url, timeout_s=timeout_s, retries=retries)
    path = raw_dir / "toi.csv"
    atomic_write(path, payload)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    return path, rows


def build_target_universe(
    *,
    toi_rows: list[dict[str, str]],
    nasa_planets_path: Path,
    tess_eb_path: Path,
    seed_path: Path,
) -> tuple[str, dict[str, set[str]]]:
    source_ids: dict[str, set[str]] = defaultdict(set)
    for row in toi_rows:
        if tic_id := parse_tic_id(row.get("tid")):
            source_ids[tic_id].add("nasa_toi")
    for tic_id in read_csv_tic_ids(nasa_planets_path, "tic_id"):
        source_ids[tic_id].add("nasa_planet_host")
    for tic_id in read_csv_tic_ids(tess_eb_path, "tic_id"):
        source_ids[tic_id].add("tess_eb")
    seed_version, seed_ids = read_operator_seeds(seed_path)
    for tic_id in seed_ids:
        source_ids[tic_id].add("operator_seed")
    return seed_version, source_ids


def fetch_targeted_tic(
    raw_dir: Path,
    tic_ids: list[str],
    *,
    chunk_size: int,
    timeout_s: int,
    retries: int,
    resume_raw: bool,
) -> tuple[list[Path], list[dict[str, Any]]]:
    paths: list[Path] = []
    rows: list[dict[str, Any]] = []
    chunk_dir = raw_dir / "tic_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    for index, tic_chunk in enumerate(chunks(tic_ids, chunk_size), start=1):
        path = chunk_dir / f"tic_{index:05d}.json"
        request_payload = {
            "service": "Mast.Catalogs.Filtered.Tic",
            "params": {
                "columns": TIC_COLUMNS,
                "filters": [{"paramName": "ID", "values": tic_chunk}],
            },
            "format": "json",
            "pagesize": len(tic_chunk) + 100,
            "page": 1,
        }
        request_text = stable_json(request_payload)
        payload = cached_payload(path, request_text, resume_raw=resume_raw)
        if payload is None:
            encoded = urllib.parse.urlencode({"request": request_text}).encode("ascii")
            payload = fetch_bytes(
                MAST_INVOKE_URL, data=encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout_s=timeout_s, retries=retries,
            )
            write_cached_payload(path, payload, request_text)
        response = json.loads(payload)
        if response.get("status") != "COMPLETE":
            raise RuntimeError(f"MAST TIC chunk {index} failed: {response.get('msg')}")
        paths.append(path)
        rows.extend(response.get("data") or [])
        if index % 10 == 0:
            log(f"MAST TIC: chunks={index}, rows={len(rows):,}")
    return paths, rows


def fetch_dr2_neighbourhood(
    raw_dir: Path,
    dr2_ids: list[str],
    *,
    chunk_size: int,
    timeout_s: int,
    retries: int,
    resume_raw: bool,
) -> tuple[list[Path], list[dict[str, str]]]:
    paths: list[Path] = []
    rows: list[dict[str, str]] = []
    chunk_dir = raw_dir / "gaia_dr2_neighbourhood_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    columns = (
        "dr2_source_id,dr3_source_id,angular_distance,magnitude_difference,"
        "proper_motion_propagation"
    )
    for index, dr2_chunk in enumerate(chunks(dr2_ids, chunk_size), start=1):
        path = chunk_dir / f"dr2_neighbourhood_{index:05d}.csv"
        query = (
            f"select {columns} from gaiadr3.dr2_neighbourhood "
            f"where dr2_source_id in ({','.join(dr2_chunk)}) "
            "order by dr2_source_id,angular_distance,dr3_source_id"
        )
        payload = cached_payload(path, query, resume_raw=resume_raw)
        if payload is None:
            encoded = urllib.parse.urlencode({
                "REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "csv", "QUERY": query,
            }).encode("ascii")
            payload = fetch_bytes(
                GAIA_TAP_URL, data=encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout_s=timeout_s, retries=retries,
            )
            write_cached_payload(path, payload, query)
        paths.append(path)
        with path.open(newline="", encoding="utf-8-sig") as handle:
            rows.extend(csv.DictReader(handle))
        if index % 10 == 0:
            log(f"Gaia DR2 neighbourhood: chunks={index}, rows={len(rows):,}")
    return paths, rows


def fetch_gaia_dr3_targets(
    raw_dir: Path,
    dr3_ids: list[str],
    *,
    chunk_size: int,
    timeout_s: int,
    retries: int,
    resume_raw: bool,
) -> tuple[list[Path], list[dict[str, str]]]:
    paths: list[Path] = []
    rows: list[dict[str, str]] = []
    chunk_dir = raw_dir / "gaia_dr3_source_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    columns = "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,ruwe,phot_g_mean_mag,bp_rp"
    for index, dr3_chunk in enumerate(chunks(dr3_ids, chunk_size), start=1):
        path = chunk_dir / f"gaia_dr3_source_{index:05d}.csv"
        query = (
            f"select {columns} from gaiadr3.gaia_source "
            f"where source_id in ({','.join(dr3_chunk)}) order by source_id"
        )
        payload = cached_payload(path, query, resume_raw=resume_raw)
        if payload is None:
            encoded = urllib.parse.urlencode({
                "REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "csv", "QUERY": query,
            }).encode("ascii")
            payload = fetch_bytes(
                GAIA_TAP_URL, data=encoded,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout_s=timeout_s, retries=retries,
            )
            write_cached_payload(path, payload, query)
        paths.append(path)
        with path.open(newline="", encoding="utf-8-sig") as handle:
            rows.extend(csv.DictReader(handle))
        if index % 10 == 0:
            log(f"Gaia DR3 targets: chunks={index}, rows={len(rows):,}")
    return paths, rows


def fetch_gaia_external_crossmatches(
    raw_dir: Path,
    tic_rows: list[dict[str, Any]],
    *,
    chunk_size: int,
    timeout_s: int,
    retries: int,
    resume_raw: bool,
) -> tuple[list[Path], list[dict[str, str]]]:
    table_specs = {
        "hip": ("hipparcos2_best_neighbour", "HIP", True),
        "tyc": ("tycho2tdsc_merge_best_neighbour", "TYC", False),
        "twomass": ("tmass_psc_xsc_best_neighbour", "TWOMASS", False),
    }
    chunk_dir = raw_dir / "gaia_external_crossmatch_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    result: list[dict[str, str]] = []
    rows_without_gaia = [row for row in tic_rows if not str(row.get("GAIA") or "").isdigit()]
    for namespace, (table_name, field_name, numeric) in table_specs.items():
        values = sorted({str(row.get(field_name) or "").strip() for row in rows_without_gaia if str(row.get(field_name) or "").strip()})
        for index, value_chunk in enumerate(chunks(values, chunk_size), start=1):
            path = chunk_dir / f"{namespace}_{index:05d}.csv"
            if numeric:
                query_values = ",".join(str(int(value)) for value in value_chunk if value.isdigit())
            else:
                query_values = ",".join("'" + value.replace("'", "''") + "'" for value in value_chunk)
            if not query_values:
                continue
            query = (
                "select source_id,original_ext_source_id,angular_distance,number_of_neighbours,xm_flag "
                f"from gaiadr3.{table_name} where original_ext_source_id in ({query_values}) "
                "order by original_ext_source_id,angular_distance,source_id"
            )
            payload = cached_payload(path, query, resume_raw=resume_raw)
            if payload is None:
                encoded = urllib.parse.urlencode({
                    "REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "csv", "QUERY": query,
                }).encode("ascii")
                payload = fetch_bytes(
                    GAIA_TAP_URL, data=encoded,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout_s=timeout_s, retries=retries,
                )
                write_cached_payload(path, payload, query)
            paths.append(path)
            with path.open(newline="", encoding="utf-8-sig") as handle:
                for row in csv.DictReader(handle):
                    result.append({
                        "namespace": namespace,
                        "external_id": str(row.get("original_ext_source_id") or "").strip(),
                        "dr3_source_id": str(row.get("source_id") or "").strip(),
                        "angular_distance_arcsec": str(row.get("angular_distance") or "").strip(),
                        "number_of_neighbours": str(row.get("number_of_neighbours") or "").strip(),
                        "xm_flag": str(row.get("xm_flag") or "").strip(),
                    })
    return paths, result


def aggregate_files(paths: list[Path]) -> tuple[int, str]:
    digest = hashlib.sha256()
    total_bytes = 0
    for path in sorted(paths):
        file_hash = sha256_file(path)
        size = path.stat().st_size
        digest.update(f"{path.name}\0{size}\0{file_hash}\n".encode("utf-8"))
        total_bytes += size
    return total_bytes, digest.hexdigest()


def manifest_entry(
    source_name: str, source_version: str, url: str, dest_path: Path,
    *, retrieved_at: str, row_count: int, bytes_written: int | None = None,
    sha256: str | None = None, extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entry = {
        "source_name": source_name, "source_version": source_version, "url": url,
        "dest_path": str(dest_path), "retrieved_at": retrieved_at,
        "checked_at": retrieved_at, "bytes_written": bytes_written if bytes_written is not None else dest_path.stat().st_size,
        "sha256": sha256 or sha256_file(dest_path), "row_count": row_count,
    }
    entry.update(extra or {})
    return entry


def build_snapshot(
    toi_rows: list[dict[str, str]], tic_rows: list[dict[str, Any]],
    target_sources: dict[str, set[str]],
) -> dict[str, Any]:
    toi_signatures = {
        str(row.get("toidisplay") or row.get("toi")): {
            "tic_id": parse_tic_id(row.get("tid")),
            "disposition": str(row.get("tfopwg_disp") or "").strip(),
            "row_updated_at": str(row.get("rowupdate") or "").strip(),
            "row_hash": row_hash(row),
        }
        for row in toi_rows
    }
    tic_signatures = {str(row["ID"]): row_hash(row) for row in tic_rows if row.get("ID") is not None}
    dispositions: dict[str, int] = defaultdict(int)
    for row in toi_rows:
        dispositions[str(row.get("tfopwg_disp") or "UNKNOWN").strip() or "UNKNOWN"] += 1
    return {
        "toi": toi_signatures,
        "tic": tic_signatures,
        "target_sources": {tic_id: sorted(sources) for tic_id, sources in sorted(target_sources.items())},
        "counts": {
            "toi_rows": len(toi_rows), "target_tic_ids": len(target_sources),
            "retrieved_tic_rows": len(tic_rows), "toi_dispositions": dict(sorted(dispositions.items())),
        },
    }


def diff_snapshot(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    def keyed_delta(name: str) -> dict[str, Any]:
        before = previous.get(name) or {}
        after = current.get(name) or {}
        common = sorted(set(before) & set(after))
        changed = [key for key in common if before[key] != after[key]]
        disposition_changes = []
        if name == "toi":
            disposition_changes = [
                {"source_key": key, "before": before[key].get("disposition"), "after": after[key].get("disposition")}
                for key in common if before[key].get("disposition") != after[key].get("disposition")
            ]
        return {
            "added": sorted(set(after) - set(before)), "removed": sorted(set(before) - set(after)),
            "changed": changed, "disposition_changes": disposition_changes,
        }
    return {"toi": keyed_delta("toi"), "tic": keyed_delta("tic")}


def cook_toi(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    mapping = {
        "tid": "tic_id", "toi": "toi", "toidisplay": "toi_display", "toipfx": "toi_prefix",
        "ctoi_alias": "ctoi_alias", "pl_pnum": "planet_number", "tfopwg_disp": "disposition",
        "ra": "ra_deg", "dec": "dec_deg", "st_pmra": "pm_ra_mas_yr", "st_pmdec": "pm_dec_mas_yr",
        "st_tmag": "tmag", "pl_tranmid": "transit_epoch_bjd", "pl_tranmiderr1": "transit_epoch_err_plus",
        "pl_tranmiderr2": "transit_epoch_err_minus", "pl_orbper": "orbital_period_days",
        "pl_orbpererr1": "orbital_period_err_plus", "pl_orbpererr2": "orbital_period_err_minus",
        "pl_trandurh": "transit_duration_hours", "pl_trandurherr1": "transit_duration_err_plus",
        "pl_trandurherr2": "transit_duration_err_minus", "pl_trandep": "transit_depth_ppm",
        "pl_trandeperr1": "transit_depth_err_plus", "pl_trandeperr2": "transit_depth_err_minus",
        "pl_rade": "planet_radius_earth", "pl_radeerr1": "planet_radius_err_plus",
        "pl_radeerr2": "planet_radius_err_minus", "pl_insol": "insolation_earth",
        "pl_insolerr1": "insolation_err_plus", "pl_insolerr2": "insolation_err_minus",
        "pl_eqt": "equilibrium_temp_k", "st_dist": "stellar_distance_pc", "st_teff": "stellar_teff_k",
        "st_logg": "stellar_logg_cgs", "st_rad": "stellar_radius_solar", "sectors": "sectors",
        "toi_created": "toi_created", "rowupdate": "row_updated_at", "release_date": "release_date",
    }
    cooked = []
    for raw in rows:
        item = {field: "" for field in TOI_COOKED_FIELDS}
        for source, target in mapping.items():
            item[target] = str(raw.get(source) or "").strip()
        item["tic_id"] = parse_tic_id(raw.get("tid")) or ""
        item["source_key"] = item["toi_display"] or ("TOI-" + item["toi"])
        item["source_row_hash"] = row_hash(raw)
        cooked.append(item)
    return sorted(cooked, key=lambda row: (float(row["toi"] or 0), row["source_key"]))


def cook_tic(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapping = {
        "ID": "tic_id", "version": "tic_version", "HIP": "hip_id", "TYC": "tyc_id",
        "TWOMASS": "twomass_id", "GAIA": "gaia_dr2_id", "objType": "object_type",
        "typeSrc": "type_source", "ra": "ra_deg", "dec": "dec_deg", "pmRA": "pm_ra_mas_yr",
        "e_pmRA": "pm_ra_error_mas_yr", "pmDEC": "pm_dec_mas_yr", "e_pmDEC": "pm_dec_error_mas_yr",
        "plx": "parallax_mas", "e_plx": "parallax_error_mas", "Tmag": "tmag",
        "e_Tmag": "tmag_error", "d": "distance_pc", "e_d": "distance_error_pc",
        "disposition": "disposition", "duplicate_id": "duplicate_id", "objID": "mast_object_id",
    }
    cooked = []
    for raw in rows:
        item = {field: "" for field in TIC_COOKED_FIELDS}
        for source, target in mapping.items():
            value = raw.get(source)
            item[target] = "" if value is None else str(value).strip()
        item["tic_id"] = parse_tic_id(raw.get("ID")) or ""
        item["source_row_hash"] = row_hash(raw)
        cooked.append(item)
    return sorted(cooked, key=lambda row: int(row["tic_id"]))


def cook_neighbours(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    neighbour_counts: dict[str, int] = defaultdict(int)
    for raw in rows:
        neighbour_counts[str(raw.get("dr2_source_id") or "").strip()] += 1
    cooked = []
    for raw in rows:
        dr2_source_id = str(raw.get("dr2_source_id") or "").strip()
        cooked.append({
            "dr2_source_id": dr2_source_id,
            "dr3_source_id": str(raw.get("dr3_source_id") or "").strip(),
            "angular_distance_arcsec": str(raw.get("angular_distance") or "").strip(),
            "magnitude_difference": str(raw.get("magnitude_difference") or "").strip(),
            "number_of_neighbours": neighbour_counts[dr2_source_id],
            "proper_motion_propagation": str(raw.get("proper_motion_propagation") or "").strip(),
        })
    return sorted(cooked, key=lambda row: (int(row["dr2_source_id"]), float(row["angular_distance_arcsec"] or 1e9), int(row["dr3_source_id"])))


def cook_gaia_dr3(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    mapping = {
        "source_id": "source_id", "ra": "ra_deg", "dec": "dec_deg",
        "parallax": "parallax_mas", "parallax_error": "parallax_error_mas",
        "pmra": "pm_ra_mas_yr", "pmdec": "pm_dec_mas_yr", "ruwe": "ruwe",
        "phot_g_mean_mag": "phot_g_mag", "bp_rp": "bp_rp",
    }
    cooked = []
    for raw in rows:
        cooked.append({target: str(raw.get(source) or "").strip() for source, target in mapping.items()})
    return sorted(cooked, key=lambda row: int(row["source_id"]))


def update_disposition_history(
    path: Path, toi_rows: list[dict[str, Any]], *, observed_at: str
) -> None:
    history: dict[tuple[str, str, str], dict[str, str]] = {}
    if path.exists():
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                key = (row["source_key"], row["disposition"], row["effective_at"])
                history[key] = row
    for row in toi_rows:
        effective_at = row["row_updated_at"] or row["toi_created"] or row["release_date"]
        key = (row["source_key"], row["disposition"], effective_at)
        existing = history.get(key)
        history[key] = {
            "source_key": row["source_key"], "tic_id": row["tic_id"],
            "toi_display": row["toi_display"], "disposition": row["disposition"],
            "effective_at": effective_at, "release_date": row["release_date"],
            "source_row_hash": row["source_row_hash"],
            "first_observed_at": existing["first_observed_at"] if existing else observed_at,
            "last_observed_at": observed_at,
        }
    write_csv(
        path, TOI_HISTORY_FIELDS,
        [history[key] for key in sorted(history, key=lambda value: (value[0], value[2], value[1]))],
    )


def archive_raw_snapshot(
    raw_dir: Path,
    snapshot_id: str,
    chunk_groups: dict[str, list[Path]],
) -> Path:
    snapshot_dir = raw_dir / "snapshots" / snapshot_id
    if snapshot_dir.exists():
        return snapshot_dir
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    for name in ("toi.csv", "target_tic_ids.csv"):
        os.link(raw_dir / name, snapshot_dir / name)
    for name, paths in chunk_groups.items():
        destination = snapshot_dir / name
        destination.mkdir(parents=True, exist_ok=False)
        for path in sorted(paths):
            os.link(path, destination / path.name)
            request_hash_path = path.with_suffix(path.suffix + ".request.sha256")
            if request_hash_path.exists():
                os.link(request_hash_path, destination / request_hash_path.name)
    return snapshot_dir


def resolve_state_dir(root: Path, value: str | None) -> Path:
    configured = value or os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    return Path(configured) if configured else root / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and cook targeted TESS identity/evidence sources.")
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--seed-config", default=None)
    parser.add_argument("--timeout-s", type=int, default=240)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--tic-chunk-size", type=int, default=500)
    parser.add_argument("--gaia-chunk-size", type=int, default=500)
    parser.add_argument(
        "--resume-raw", action="store_true",
        help="Reuse completed raw MAST/Gaia chunks after an interrupted run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    state_dir = resolve_state_dir(root, args.state_dir)
    raw_dir = state_dir / "raw" / "tess_evidence"
    cooked_dir = state_dir / "cooked" / "tess_evidence"
    report_dir = state_dir / "reports"
    manifest_path = report_dir / "manifests" / "tess_evidence_manifest.json"
    seed_path = Path(args.seed_config) if args.seed_config else root / "config" / "tess_target_seeds.json"
    nasa_planets_path = state_dir / "raw" / "nasa_exoplanet_archive" / "pscomppars.csv"
    tess_eb_path = state_dir / "raw" / "tess_eb" / "tess_eb_catalog.csv"
    retrieved_at = utc_now()

    log("TESS evidence: fetching NASA TOI snapshot")
    toi_path, toi_rows = fetch_toi(raw_dir, timeout_s=args.timeout_s, retries=args.retries)
    seed_version, target_sources = build_target_universe(
        toi_rows=toi_rows, nasa_planets_path=nasa_planets_path,
        tess_eb_path=tess_eb_path, seed_path=seed_path,
    )
    target_ids = sorted(target_sources, key=int)
    target_family_counts = {
        family: sum(family in families for families in target_sources.values())
        for family in sorted({family for families in target_sources.values() for family in families})
    }
    target_path = raw_dir / "target_tic_ids.csv"
    write_csv(target_path, ["tic_id", "source_families"], [
        {"tic_id": tic_id, "source_families": "|".join(sorted(target_sources[tic_id]))}
        for tic_id in target_ids
    ])
    log(f"TESS evidence: fetching {len(target_ids):,} targeted TIC rows")
    tic_paths, tic_rows = fetch_targeted_tic(
        raw_dir, target_ids, chunk_size=args.tic_chunk_size,
        timeout_s=args.timeout_s, retries=args.retries, resume_raw=args.resume_raw,
    )
    dr2_ids = sorted({str(row["GAIA"]) for row in tic_rows if str(row.get("GAIA") or "").isdigit()}, key=int)
    log(f"TESS evidence: fetching Gaia DR2 neighbourhood for {len(dr2_ids):,} source IDs")
    neighbour_paths, neighbour_rows = fetch_dr2_neighbourhood(
        raw_dir, dr2_ids, chunk_size=args.gaia_chunk_size,
        timeout_s=args.timeout_s, retries=args.retries, resume_raw=args.resume_raw,
    )
    log("TESS evidence: fetching Gaia DR3 external-catalog best neighbours")
    external_paths, external_rows = fetch_gaia_external_crossmatches(
        raw_dir, tic_rows, chunk_size=args.gaia_chunk_size,
        timeout_s=args.timeout_s, retries=args.retries, resume_raw=args.resume_raw,
    )
    dr3_ids = sorted(
        {
            str(row.get("dr3_source_id") or "")
            for row in [*neighbour_rows, *external_rows]
            if str(row.get("dr3_source_id") or "").isdigit()
        },
        key=int,
    )
    log(f"TESS evidence: fetching Gaia DR3 source rows for {len(dr3_ids):,} candidates")
    gaia_dr3_paths, gaia_dr3_rows = fetch_gaia_dr3_targets(
        raw_dir, dr3_ids, chunk_size=args.gaia_chunk_size,
        timeout_s=args.timeout_s, retries=args.retries, resume_raw=args.resume_raw,
    )

    cooked_toi = cooked_dir / "toi.csv"
    cooked_tic = cooked_dir / "targeted_tic.csv"
    cooked_targets = cooked_dir / "target_tic_ids.csv"
    cooked_neighbours = cooked_dir / "gaia_dr2_neighbourhood.csv"
    cooked_gaia_dr3 = cooked_dir / "gaia_dr3_targets.csv"
    cooked_external = cooked_dir / "gaia_external_crossmatches.csv"
    cooked_toi_rows = cook_toi(toi_rows)
    write_csv(cooked_toi, TOI_COOKED_FIELDS, cooked_toi_rows)
    write_csv(cooked_tic, TIC_COOKED_FIELDS, cook_tic(tic_rows))
    write_csv(cooked_targets, ["tic_id", "source_families"], [
        {"tic_id": tic_id, "source_families": "|".join(sorted(target_sources[tic_id]))}
        for tic_id in target_ids
    ])
    write_csv(cooked_neighbours, NEIGHBOUR_FIELDS, cook_neighbours(neighbour_rows))
    write_csv(cooked_gaia_dr3, GAIA_DR3_FIELDS, cook_gaia_dr3(gaia_dr3_rows))
    write_csv(cooked_external, GAIA_EXTERNAL_FIELDS, sorted(
        external_rows, key=lambda row: (row["namespace"], row["external_id"], row["dr3_source_id"])
    ))
    update_disposition_history(
        cooked_dir / "toi_disposition_history.csv", cooked_toi_rows, observed_at=retrieved_at
    )

    tic_bytes, tic_hash = aggregate_files(tic_paths)
    neighbour_bytes, neighbour_hash = aggregate_files(neighbour_paths)
    gaia_dr3_bytes, gaia_dr3_hash = aggregate_files(gaia_dr3_paths)
    external_bytes, external_hash = aggregate_files(external_paths)
    snapshot_id = "_".join((
        sha256_file(toi_path)[:12], sha256_file(target_path)[:12],
        tic_hash[:12], neighbour_hash[:12], external_hash[:12], gaia_dr3_hash[:12],
    ))
    snapshot_dir = archive_raw_snapshot(raw_dir, snapshot_id, {
        "tic_chunks": tic_paths,
        "gaia_dr2_neighbourhood_chunks": neighbour_paths,
        "gaia_external_crossmatch_chunks": external_paths,
        "gaia_dr3_source_chunks": gaia_dr3_paths,
    })
    atomic_write_json(raw_dir / "current.json", {
        "snapshot_id": snapshot_id, "snapshot_path": str(snapshot_dir),
        "retrieved_at": retrieved_at,
    })
    manifest = [
        manifest_entry("nasa_toi", "nasa_toi_tap_snapshot", NASA_TAP_URL, snapshot_dir / "toi.csv",
                       retrieved_at=retrieved_at, row_count=len(toi_rows),
                       extra={"query": TOI_QUERY, "query_sha256": sha256_bytes(TOI_QUERY.encode()),
                              "snapshot_id": snapshot_id}),
        manifest_entry("tess_target_set", seed_version, str(seed_path), snapshot_dir / "target_tic_ids.csv",
                       retrieved_at=retrieved_at, row_count=len(target_ids),
                       extra={
                           "seed_config_path": str(seed_path),
                           "seed_config_sha256": sha256_file(seed_path),
                           "nasa_confirmed_planets_path": str(nasa_planets_path),
                           "nasa_confirmed_planets_sha256": sha256_file(nasa_planets_path),
                           "tess_eb_path": str(tess_eb_path),
                           "tess_eb_sha256": sha256_file(tess_eb_path),
                           "target_family_counts": target_family_counts,
                           "snapshot_id": snapshot_id,
                       }),
        manifest_entry("mast_tic_targeted", "tic_v8_targeted", MAST_INVOKE_URL, snapshot_dir / "tic_chunks",
                       retrieved_at=retrieved_at, row_count=len(tic_rows), bytes_written=tic_bytes, sha256=tic_hash,
                       extra={"target_id_count": len(target_ids), "chunk_count": len(tic_paths),
                              "query_input_sha256": sha256_file(target_path), "snapshot_id": snapshot_id}),
        manifest_entry("gaia_dr2_neighbourhood_targeted", "gaiadr3_dr2_neighbourhood_targeted", GAIA_TAP_URL,
                       snapshot_dir / "gaia_dr2_neighbourhood_chunks", retrieved_at=retrieved_at,
                       row_count=len(neighbour_rows), bytes_written=neighbour_bytes, sha256=neighbour_hash,
                       extra={"dr2_source_id_count": len(dr2_ids), "chunk_count": len(neighbour_paths),
                              "snapshot_id": snapshot_id}),
        manifest_entry("gaia_dr3_targets", "gaiadr3_gaia_source_targeted", GAIA_TAP_URL,
                       snapshot_dir / "gaia_dr3_source_chunks", retrieved_at=retrieved_at,
                       row_count=len(gaia_dr3_rows), bytes_written=gaia_dr3_bytes, sha256=gaia_dr3_hash,
                       extra={"dr3_source_id_count": len(dr3_ids), "chunk_count": len(gaia_dr3_paths),
                              "snapshot_id": snapshot_id}),
        manifest_entry("gaia_external_crossmatches", "gaiadr3_external_best_neighbour_targeted", GAIA_TAP_URL,
                       snapshot_dir / "gaia_external_crossmatch_chunks", retrieved_at=retrieved_at,
                       row_count=len(external_rows), bytes_written=external_bytes, sha256=external_hash,
                       extra={"chunk_count": len(external_paths), "snapshot_id": snapshot_id}),
    ]
    atomic_write_json(manifest_path, manifest)

    snapshot_path = report_dir / "tess_source_snapshot.json"
    previous = json.loads(snapshot_path.read_text(encoding="utf-8")) if snapshot_path.exists() else {}
    snapshot = build_snapshot(toi_rows, tic_rows, target_sources)
    delta = {
        "generated_at": retrieved_at, "previous_snapshot_present": bool(previous),
        "counts": snapshot["counts"], "delta": diff_snapshot(previous, snapshot),
        "cooked_sha256": {
            "toi": sha256_file(cooked_toi), "targeted_tic": sha256_file(cooked_tic),
            "target_set": sha256_file(cooked_targets), "gaia_dr2_neighbourhood": sha256_file(cooked_neighbours),
            "gaia_dr3_targets": sha256_file(cooked_gaia_dr3),
            "gaia_external_crossmatches": sha256_file(cooked_external),
        },
    }
    atomic_write_json(report_dir / "tess_source_delta_report.json", delta)
    history_path = report_dir / "tess_source_delta_history" / (retrieved_at.replace(":", "").replace("-", "") + ".json")
    atomic_write_json(history_path, delta)
    atomic_write_json(snapshot_path, snapshot)
    log(
        "TESS evidence complete: "
        f"toi={len(toi_rows):,}, targets={len(target_ids):,}, tic={len(tic_rows):,}, "
        f"dr2_neighbours={len(neighbour_rows):,}, gaia_dr3={len(gaia_dr3_rows):,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
