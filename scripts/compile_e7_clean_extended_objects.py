#!/usr/bin/env python3
"""Compile clean extended-object geometry and search from selected evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import astropy.units as u
import duckdb
import pyarrow as pa
from astropy.coordinates import FK4, ICRS, SkyCoord
from astropy.time import Time


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_extended_objects.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT = Path("/mnt/space/spacegate/e7-clean-extended-objects")
LY_PER_PC = 3.26156


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def number(value: Any) -> float | None:
    raw = str(value or "").strip().replace("−", "-")
    if not raw:
        return None
    try:
        parsed = float(raw.rstrip(":?vV"))
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def hms(hours: Any, minutes: Any, seconds: Any = 0) -> float | None:
    h, m, s = number(hours), number(minutes), number(seconds)
    if h is None or m is None:
        return None
    return 15.0 * (h + m / 60.0 + (s or 0.0) / 3600.0)


def dms(sign: Any, degrees: Any, minutes: Any, seconds: Any = 0) -> float | None:
    d, m, s = number(degrees), number(minutes), number(seconds)
    if d is None or m is None:
        return None
    value = abs(d) + m / 60.0 + (s or 0.0) / 3600.0
    return -value if str(sign or degrees).strip().startswith("-") or d < 0 else value


def sexagesimal(value: Any, *, ra: bool) -> float | None:
    raw = str(value or "").strip()
    parts = re.split(r"[:\s]+", raw.lstrip("+-"))
    if len(parts) < 2:
        return None
    return hms(*parts[:3]) if ra else dms(raw[:1], *parts[:3])


def to_icrs(ra_deg: float | None, dec_deg: float | None, epoch: str) -> tuple[float | None, float | None]:
    if ra_deg is None or dec_deg is None:
        return None, None
    if epoch == "J2000":
        return ra_deg % 360.0, dec_deg
    coordinate = SkyCoord(
        ra=ra_deg * u.deg, dec=dec_deg * u.deg,
        frame=FK4(equinox=Time(epoch)),
    ).transform_to(ICRS())
    return float(coordinate.ra.deg), float(coordinate.dec.deg)


def angular_size(value: Any) -> tuple[float | None, float | None]:
    raw = str(value or "").strip().lower().replace("×", "x")
    if not raw:
        return None, None
    parts = re.split(r"\s*x\s*", raw)
    major = number(parts[0])
    minor = number(parts[1]) if len(parts) > 1 else major
    return major, minor


def normalized_geometry(source_table: str, geometry: dict[str, Any]) -> dict[str, Any]:
    ra = dec = major = minor = position_angle = area = None
    frame, epoch = "ICRS", "J2000"
    if source_table.startswith("openngc_"):
        ra, dec = sexagesimal(geometry.get("RA"), ra=True), sexagesimal(geometry.get("Dec"), ra=False)
        major, minor = number(geometry.get("MajAx")), number(geometry.get("MinAx"))
        position_angle = number(geometry.get("PosAng"))
    elif source_table == "green_snr_catalogue":
        ra = hms(geometry.get("ra_hour"), geometry.get("ra_minute"), geometry.get("ra_second"))
        dec = dms(geometry.get("dec_degree"), geometry.get("dec_degree"), geometry.get("dec_arcminute"))
        major, minor = angular_size(geometry.get("angular_size"))
    elif source_table == "lbn_vii_9":
        ra, dec = hms(geometry.get("RAh"), geometry.get("RAm")), dms(geometry.get("DE-"), geometry.get("DEd"), geometry.get("DEm"))
        major, minor, area = number(geometry.get("Diam1")), number(geometry.get("Diam2")), number(geometry.get("Area"))
        frame, epoch = "FK4", "B1950"
    elif source_table == "ldn_vii_7a":
        ra, dec = hms(geometry.get("RAh"), geometry.get("RAm")), dms(geometry.get("DE-"), geometry.get("DEd"), geometry.get("DEm"))
        area = number(geometry.get("Area"))
        frame, epoch = "FK4", "B1950"
    elif source_table == "barnard_vii_220a":
        ra = hms(geometry.get("RA2000h"), geometry.get("RA2000m"), geometry.get("RA2000s"))
        dec = dms(geometry.get("DE2000-"), geometry.get("DE2000d"), geometry.get("DE2000m"))
        major = minor = number(geometry.get("Diam"))
    elif source_table == "magakian_2003":
        ra = hms(geometry.get("RAh"), geometry.get("RAm"), geometry.get("RAs"))
        dec = dms(geometry.get("DE-"), geometry.get("DEd"), geometry.get("DEm"), geometry.get("DEs"))
    elif source_table == "sharpless_vii_20":
        ra = hms(geometry.get("RA1950h"), geometry.get("RA1950m"), (number(geometry.get("RA1950ds")) or 0) / 10.0)
        dec = dms(geometry.get("DE1950-"), geometry.get("DE1950d"), geometry.get("DE1950m"), geometry.get("DE1950s"))
        major = minor = number(geometry.get("Diam"))
        frame, epoch = "FK4", "B1950"
    elif source_table == "cederblad_vii_231":
        ra, dec = hms(geometry.get("RAh"), geometry.get("RAm")), dms(geometry.get("DE-"), geometry.get("DEd"), geometry.get("DEm"))
        major, minor = number(geometry.get("Dim1")), number(geometry.get("Dim2"))
        frame, epoch = "FK4", "B1900"
    elif source_table == "vdb_vii_21":
        # The catalog marks these as old Galactic coordinates. Preserve them in
        # raw evidence until a reviewed frame conversion policy is registered.
        major, minor = number(geometry.get("BRadMax")), number(geometry.get("RRadMax"))
        frame, epoch = "galactic_legacy", "source_native"
    ra, dec = to_icrs(ra, dec, epoch) if frame == "FK4" else (ra, dec)
    shape = "missing"
    if ra is not None and dec is not None:
        shape = "ellipse" if major is not None and minor is not None else "point"
        if major is None and area is not None and area > 0:
            major = minor = 120.0 * math.sqrt(area / math.pi)
            shape = "equivalent_circle"
    return {
        "ra_deg": ra, "dec_deg": dec, "source_frame": frame,
        "source_epoch": epoch, "major_axis_arcmin": major,
        "minor_axis_arcmin": minor, "position_angle_deg": position_angle,
        "shape_kind": shape,
    }


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_clean_extended_objects_policy.v1":
        raise ValueError("unsupported clean extended-object policy")
    expected = {
        "open_stability_databases": False,
        "identity_seed_is_scientific_authority": False,
        "preserve_all_geometry_candidates": True,
        "preserve_missing_geometry_objects": True,
        "promote_unselected_cluster_distance": False,
        "promote_selected_cluster_distance": True,
        "promote_associated_star_distance_without_selected_binding": False,
        "promote_associated_star_distance_with_selected_binding": True,
    }
    if policy.get("rules") != expected:
        raise ValueError("unsafe clean extended-object rules")
    priorities = policy.get("source_priority") or {}
    if not priorities or len(set(priorities.values())) != len(priorities):
        raise ValueError("source priorities must be unique")


def resolve_inputs(policy: dict[str, Any], state: Path) -> dict[str, Path]:
    seed = state / "derived/evidence_lake_v2/extended_identity_seed" / policy["identity_seed"]["seed_id"]
    selected = state / "derived/evidence_lake_v2/selected_extended_objects" / policy["selected_extended_objects"]["build_id"]
    clusters = state / "derived/evidence_lake_v2/clean_clusters" / policy["clean_clusters"]["build_id"]
    vocabulary = state / "derived/evidence_lake_v2/permanent_identity_vocabulary" / policy["permanent_identity_vocabulary"]["build_id"]
    placements = state / "derived/evidence_lake_v2/selected_system_placements" / policy["selected_system_placements"]["build_id"]
    paths = {
        "seed_manifest": seed / "manifest.json",
        "nodes": seed / "extended_identity_nodes.parquet",
        "aliases": seed / "extended_object_aliases.parquet",
        "identifiers": seed / "extended_object_identifiers.parquet",
        "reconciliation": seed / "extended_object_reconciliation.parquet",
        "quarantine": seed / "extended_object_identity_quarantine.parquet",
        "selected_manifest": selected / "manifest.json",
        "bindings": selected / "parquet/extended_object_bindings.parquet",
        "evidence": selected / "parquet/extended_object_evidence_projection.parquet",
        "cluster_manifest": clusters / "manifest.json",
        "cluster_bindings": clusters / "parquet/cluster_identity_bindings.parquet",
        "cluster_evidence": clusters / "parquet/cluster_evidence_projection.parquet",
        "vocabulary_manifest": vocabulary / "manifest.json",
        "identity_aliases": vocabulary / "aliases.parquet",
        "placement_manifest": placements / "manifest.json",
        "system_placements": placements / "selected_system_placements.parquet",
    }
    for path in paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)
    expected = {
        "seed_manifest": policy["identity_seed"]["manifest_sha256"],
        "selected_manifest": policy["selected_extended_objects"]["manifest_sha256"],
        "bindings": policy["selected_extended_objects"]["bindings_sha256"],
        "evidence": policy["selected_extended_objects"]["evidence_sha256"],
        "cluster_manifest": policy["clean_clusters"]["manifest_sha256"],
        "cluster_bindings": policy["clean_clusters"]["bindings_sha256"],
        "cluster_evidence": policy["clean_clusters"]["evidence_sha256"],
        "vocabulary_manifest": policy["permanent_identity_vocabulary"]["manifest_sha256"],
        "identity_aliases": policy["permanent_identity_vocabulary"]["aliases_sha256"],
        "placement_manifest": policy["selected_system_placements"]["manifest_sha256"],
        "system_placements": policy["selected_system_placements"]["placements_sha256"],
    }
    for key, digest in expected.items():
        if file_hash(paths[key]) != digest:
            raise ValueError(f"pinned extended-object input mismatch: {key}")
    seed_manifest = load_object(paths["seed_manifest"])
    for key in ("nodes", "aliases", "identifiers", "reconciliation", "quarantine"):
        product_key = "extended_identity_nodes" if key == "nodes" else (
            "extended_object_reconciliation" if key == "reconciliation" else
            "extended_object_identity_quarantine" if key == "quarantine" else
            f"extended_object_{key}"
        )
        if file_hash(paths[key]) != seed_manifest["products"][product_key]["sha256"]:
            raise ValueError(f"identity seed product mismatch: {key}")
    return paths


def build_candidates(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> int:
    con.execute(
        """
        CREATE TABLE extended_object_geometry_candidates(
          geometry_candidate_id BIGINT,extended_object_id BIGINT,stable_object_key VARCHAR,
          evidence_id VARCHAR,source_record_id VARCHAR,source_id VARCHAR,release_id VARCHAR,
          source_table VARCHAR,source_record_key VARCHAR,authority_role VARCHAR,
          source_priority INTEGER,ra_deg DOUBLE,dec_deg DOUBLE,source_frame VARCHAR,
          source_epoch VARCHAR,shape_kind VARCHAR,major_axis_arcmin DOUBLE,
          minor_axis_arcmin DOUBLE,position_angle_deg DOUBLE,geometry_raw JSON,
          reference_raw VARCHAR,method VARCHAR,normalization_version VARCHAR
        )
        """
    )
    rows = con.execute(
        """
        SELECT canonical_extended_object_id,canonical_stable_object_key,evidence_id,
               source_record_id,source_id,release_id,source_table,source_record_key,
               authority_role,geometry_raw,reference_raw,method,normalization_version
        FROM selected_evidence
        WHERE projection_status='eligible_for_extended_quantity_selection'
          AND canonical_extended_object_id IS NOT NULL
        ORDER BY canonical_extended_object_id,source_table,evidence_id
        """
    ).fetchall()
    output = []
    for index, row in enumerate(rows, start=1):
        geometry = json.loads(str(row[9])) if row[9] is not None else {}
        normalized = normalized_geometry(str(row[6]), geometry)
        output.append((
            index, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
            int(policy["source_priority"][str(row[6])]), normalized["ra_deg"], normalized["dec_deg"],
            normalized["source_frame"], normalized["source_epoch"], normalized["shape_kind"],
            normalized["major_axis_arcmin"], normalized["minor_axis_arcmin"],
            normalized["position_angle_deg"], str(row[9]) if row[9] is not None else None,
            row[10], row[11], row[12],
        ))
    columns = [
        "geometry_candidate_id", "extended_object_id", "stable_object_key",
        "evidence_id", "source_record_id", "source_id", "release_id",
        "source_table", "source_record_key", "authority_role", "source_priority",
        "ra_deg", "dec_deg", "source_frame", "source_epoch", "shape_kind",
        "major_axis_arcmin", "minor_axis_arcmin", "position_angle_deg",
        "geometry_raw", "reference_raw", "method", "normalization_version",
    ]
    batch = pa.Table.from_pydict(
        {name: [row[index] for row in output] for index, name in enumerate(columns)}
    )
    con.register("geometry_candidate_batch", batch)
    try:
        con.execute("INSERT INTO extended_object_geometry_candidates SELECT * FROM geometry_candidate_batch")
    finally:
        con.unregister("geometry_candidate_batch")
    return len(output)


def normalized_cluster_context(source_id: str, parameters: dict[str, Any]) -> dict[str, Any]:
    if source_id == "clusters.hunt_reffert_2024":
        ra = number(parameters.get("RA_ICRS"))
        dec = number(parameters.get("DE_ICRS"))
    elif source_id == "clusters.cantat_gaudin_2020":
        ra = number(parameters.get("RAdeg"))
        dec = number(parameters.get("DEdeg"))
    else:
        raise ValueError(f"unsupported selected cluster source: {source_id}")
    if ra is not None:
        ra %= 360.0
    return {
        "ra_deg": ra,
        "dec_deg": dec,
        "source_frame": "ICRS",
        "source_epoch": "source_cluster_mean",
        "shape_kind": "point" if ra is not None and dec is not None else "missing",
    }


def append_cluster_geometry_candidates(
    con: duckdb.DuckDBPyConnection,
    policy: dict[str, Any],
    *,
    starting_id: int,
) -> int:
    rows = con.execute(
        """
        SELECT canonical_extended_object_id,canonical_cluster_stable_object_key,
               evidence_id,source_record_id,source_id,release_id,source_table,
               cluster_identity_raw,authority_role,parameter_set_raw,reference_raw,
               method,normalization_version
        FROM cluster_evidence
        WHERE projection_status='eligible_for_quantity_selection'
          AND canonical_extended_object_id IS NOT NULL
        ORDER BY authority_rank,canonical_extended_object_id,evidence_id
        """
    ).fetchall()
    output = []
    for offset, row in enumerate(rows, start=1):
        parameters = json.loads(str(row[9])) if row[9] is not None else {}
        normalized = normalized_cluster_context(str(row[4]), parameters)
        output.append((
            starting_id + offset,row[0],row[1],row[2],row[3],row[4],row[5],row[6],
            f"{row[4]}:{row[7]}",row[8],int(policy["source_priority"][str(row[6])]),
            normalized["ra_deg"],normalized["dec_deg"],normalized["source_frame"],
            normalized["source_epoch"],normalized["shape_kind"],None,None,None,
            str(row[9]) if row[9] is not None else None,row[10],row[11],row[12],
        ))
    columns = [
        "geometry_candidate_id", "extended_object_id", "stable_object_key",
        "evidence_id", "source_record_id", "source_id", "release_id",
        "source_table", "source_record_key", "authority_role", "source_priority",
        "ra_deg", "dec_deg", "source_frame", "source_epoch", "shape_kind",
        "major_axis_arcmin", "minor_axis_arcmin", "position_angle_deg",
        "geometry_raw", "reference_raw", "method", "normalization_version",
    ]
    batch = pa.Table.from_pydict(
        {name: [row[index] for row in output] for index, name in enumerate(columns)}
    )
    con.register("cluster_geometry_batch", batch)
    try:
        con.execute(
            "INSERT INTO extended_object_geometry_candidates "
            "SELECT * FROM cluster_geometry_batch"
        )
    finally:
        con.unregister("cluster_geometry_batch")
    return len(output)


def build_relation_claims(
    con: duckdb.DuckDBPyConnection, policy: dict[str, Any],
) -> int:
    con.execute(
        """
        CREATE TABLE extended_object_relation_claims(
          relation_claim_id BIGINT,extended_object_id BIGINT,stable_object_key VARCHAR,
          evidence_id VARCHAR,source_record_id VARCHAR,source_id VARCHAR,release_id VARCHAR,
          source_table VARCHAR,source_record_key VARCHAR,relation_kind VARCHAR,
          target_namespace VARCHAR,target_value_raw VARCHAR,target_value_norm VARCHAR,
          source_claim_status VARCHAR,claim_expansion_method VARCHAR,reference_raw VARCHAR,
          method VARCHAR,normalization_version VARCHAR
        )
        """
    )
    supported = set(policy["relation_policy"]["supported_sources"])
    rows = con.execute(
        """
        SELECT canonical_extended_object_id,canonical_stable_object_key,evidence_id,
               source_record_id,source_id,release_id,source_table,source_record_key,
               parameter_set_raw,reference_raw,method,normalization_version
        FROM selected_evidence
        WHERE projection_status='eligible_for_extended_quantity_selection'
          AND canonical_extended_object_id IS NOT NULL
          AND source_table IN ('magakian_2003','vdb_vii_21')
        ORDER BY source_table,source_record_key,evidence_id
        """
    ).fetchall()
    output: list[tuple[Any, ...]] = []
    maximum_expansion = int(policy["relation_policy"]["maximum_hd_range_expansion"])
    for row in rows:
        source_table = str(row[6])
        if source_table not in supported:
            raise ValueError(f"unsupported relation source: {source_table}")
        parameters = json.loads(str(row[8])) if row[8] is not None else {}
        if source_table == "magakian_2003":
            raw = str(parameters.get("HD") or "").strip()
        else:
            raw = (
                str(parameters.get("HD") or "").strip()
                + str(parameters.get("HD2") or "").strip()
            )
        if not raw:
            continue
        matches = list(re.finditer(r"(?<!\d)(\d{3,6})(?:-(\d{1,6}))?", raw))
        expanded: list[int] = []
        invalid_range = False
        for match in matches:
            start = int(match.group(1))
            end = start
            if match.group(2):
                suffix = match.group(2)
                end = (
                    int(str(start)[:-len(suffix)] + suffix)
                    if len(suffix) < len(str(start)) else int(suffix)
                )
            if end < start or end - start > maximum_expansion:
                invalid_range = True
                continue
            expanded.extend(range(start, end + 1))
        if not expanded:
            output.append((
                len(output) + 1,row[0],row[1],row[2],row[3],row[4],row[5],row[6],
                row[7],"illuminated_by","hd",raw,None,
                "excluded_invalid_range" if invalid_range else "excluded_source_null_marker",
                "hd_range_expansion_v1",row[9],row[10],row[11],
            ))
            continue
        status = "source_uncertain" if "?" in raw else "candidate"
        for hd_id in sorted(set(expanded)):
            output.append((
                len(output) + 1,row[0],row[1],row[2],row[3],row[4],row[5],row[6],
                row[7],"illuminated_by","hd",raw,f"hd {hd_id}",status,
                "hd_range_expansion_v1",row[9],row[10],row[11],
            ))
    columns = [
        "relation_claim_id","extended_object_id","stable_object_key","evidence_id",
        "source_record_id","source_id","release_id","source_table",
        "source_record_key","relation_kind","target_namespace","target_value_raw",
        "target_value_norm","source_claim_status","claim_expansion_method",
        "reference_raw","method","normalization_version",
    ]
    batch = pa.Table.from_pydict(
        {name: [row[index] for row in output] for index, name in enumerate(columns)}
    )
    con.register("relation_claim_batch", batch)
    try:
        con.execute("INSERT INTO extended_object_relation_claims SELECT * FROM relation_claim_batch")
    finally:
        con.unregister("relation_claim_batch")
    return len(output)


def compile_extended(
    policy_path: Path, state: Path, output_root: Path, *, link_into_state: bool,
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    phase_timings: list[dict[str, Any]] = []
    phase_started = time.monotonic()
    phase_cpu_started = time.process_time()
    policy = load_object(policy_path)
    validate_policy(policy)
    paths = resolve_inputs(policy, state)
    policy_sha = file_hash(policy_path)
    compiler_sha = file_hash(Path(__file__).resolve())
    build_id = stable_hash({
        "policy_sha256": policy_sha, "compiler_sha256": compiler_sha,
        "input_sha256": {key: file_hash(path) for key, path in paths.items()},
    })[:24]
    final = output_root / build_id
    if (final / "manifest.json").is_file():
        return load_object(final / "manifest.json")
    phase_timings.append({
        "phase": "resolve_and_hash_inputs",
        "wall_seconds": round(time.monotonic() - phase_started, 6),
        "cpu_seconds": round(time.process_time() - phase_cpu_started, 6),
    })
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    db = staging / "clean_extended_objects.duckdb"
    parquet = staging / "parquet"
    parquet.mkdir()
    con = duckdb.connect(str(db), config={"threads": "4", "memory_limit": "4GB"})
    compile_succeeded = False
    try:
        phase_started = time.monotonic()
        phase_cpu_started = time.process_time()
        for name in ("nodes", "aliases", "identifiers", "reconciliation", "quarantine"):
            con.execute(f"CREATE VIEW seed_{name} AS SELECT * FROM read_parquet({sql_literal(paths[name])})")
        con.execute(f"CREATE VIEW selected_bindings AS SELECT * FROM read_parquet({sql_literal(paths['bindings'])})")
        con.execute(f"CREATE VIEW selected_evidence AS SELECT * FROM read_parquet({sql_literal(paths['evidence'])})")
        con.execute(f"CREATE VIEW cluster_bindings AS SELECT * FROM read_parquet({sql_literal(paths['cluster_bindings'])})")
        con.execute(f"CREATE VIEW cluster_evidence AS SELECT * FROM read_parquet({sql_literal(paths['cluster_evidence'])})")
        con.execute(f"CREATE VIEW identity_aliases AS SELECT * FROM read_parquet({sql_literal(paths['identity_aliases'])})")
        con.execute(f"CREATE VIEW system_placements AS SELECT * FROM read_parquet({sql_literal(paths['system_placements'])})")
        candidate_count = build_candidates(con, policy)
        cluster_candidate_count = append_cluster_geometry_candidates(
            con, policy, starting_id=candidate_count,
        )
        candidate_count += cluster_candidate_count
        relation_claim_count = build_relation_claims(con, policy)
        phase_timings.append({
            "phase": "load_inputs_and_compile_geometry_candidates",
            "wall_seconds": round(time.monotonic() - phase_started, 6),
            "cpu_seconds": round(time.process_time() - phase_cpu_started, 6),
        })
        phase_started = time.monotonic()
        phase_cpu_started = time.process_time()
        con.execute(
            f"""
            CREATE TABLE selected_extended_object_geometry AS
            SELECT * EXCLUDE(choice) FROM (
              SELECT *,row_number() OVER(PARTITION BY extended_object_id ORDER BY
                CASE WHEN ra_deg IS NOT NULL AND dec_deg IS NOT NULL THEN 0 ELSE 1 END,
                source_priority,
                CASE WHEN major_axis_arcmin IS NOT NULL THEN 0 ELSE 1 END,
                source_record_key,evidence_id) choice
              FROM extended_object_geometry_candidates
            ) WHERE choice=1 ORDER BY extended_object_id;

            CREATE TABLE extended_object_relation_bindings AS
            WITH endpoint_summary AS (
              SELECT c.relation_claim_id,c.extended_object_id,c.stable_object_key,
                     c.evidence_id,c.source_record_id,c.source_id,c.release_id,
                     c.source_table,c.source_record_key,c.relation_kind,
                     c.target_namespace,c.target_value_raw,c.target_value_norm,
                     c.source_claim_status,c.claim_expansion_method,c.reference_raw,
                     count(DISTINCT a.stable_object_key) target_star_count,
                     count(DISTINCT a.system_stable_object_key) target_system_count,
                     CASE WHEN count(DISTINCT a.stable_object_key)=1
                       THEN min(a.stable_object_key) END target_star_stable_object_key,
                     CASE WHEN count(DISTINCT a.system_stable_object_key)=1
                       THEN min(a.system_stable_object_key) END target_system_stable_object_key,
                     coalesce(to_json(list(DISTINCT a.stable_object_key ORDER BY a.stable_object_key)
                       FILTER (WHERE a.stable_object_key IS NOT NULL)),'[]') target_star_candidates_json,
                     coalesce(to_json(list(DISTINCT a.system_stable_object_key ORDER BY a.system_stable_object_key)
                       FILTER (WHERE a.system_stable_object_key IS NOT NULL)),'[]') target_system_candidates_json
              FROM extended_object_relation_claims c
              LEFT JOIN identity_aliases a
                ON c.target_value_norm=a.alias_norm
               AND a.target_type='star' AND a.alias_kind='hd_id'
              GROUP BY c.relation_claim_id,c.extended_object_id,c.stable_object_key,
                       c.evidence_id,c.source_record_id,c.source_id,c.release_id,
                       c.source_table,c.source_record_key,c.relation_kind,
                       c.target_namespace,c.target_value_raw,c.target_value_norm,
                       c.source_claim_status,c.claim_expansion_method,c.reference_raw
            )
            SELECT sha256(concat_ws('|',relation_claim_id::VARCHAR,
                     coalesce(target_value_norm,'missing'),{sql_literal(policy['policy_version'])})) relation_binding_id,
                   *,CASE WHEN source_claim_status LIKE 'excluded_%' THEN source_claim_status
                          WHEN source_claim_status='source_uncertain' THEN 'excluded_source_uncertain'
                          WHEN target_system_count=0 THEN 'missing'
                          WHEN target_system_count>1 THEN 'ambiguous'
                          ELSE 'accepted' END binding_status,
                   CASE WHEN source_claim_status LIKE 'excluded_%' THEN 'source field is not an actionable HD relation claim'
                          WHEN source_claim_status='source_uncertain' THEN 'source marks the HD relation uncertain'
                          WHEN target_system_count=0 THEN 'HD claim has no clean permanent identity endpoint'
                          WHEN target_system_count>1 THEN 'HD claim resolves to multiple canonical systems'
                          WHEN target_star_count=1 THEN 'HD claim resolves to one canonical star and system'
                          ELSE 'HD claim resolves to one canonical system with component scope retained as ambiguous' END binding_reason,
                   {sql_literal(policy['policy_version'])} policy_version
            FROM endpoint_summary ORDER BY relation_claim_id;

            CREATE TABLE extended_object_relation_distance_candidates AS
            SELECT row_number() OVER(ORDER BY b.extended_object_id,b.target_system_stable_object_key)::BIGINT
                     relation_distance_candidate_id,
                   b.extended_object_id,b.stable_object_key,
                   b.target_system_stable_object_key,
                   p.representative_object_key,p.distance_pc,p.dist_ly,
                   p.placement_source,p.placement_method,p.policy_version placement_policy_version,
                   count(*) relation_claim_count,
                   to_json(list(b.relation_claim_id ORDER BY b.relation_claim_id)) relation_claim_ids_json,
                   to_json(list(b.relation_binding_id ORDER BY b.relation_claim_id)) relation_binding_ids_json,
                   to_json(list(b.evidence_id ORDER BY b.relation_claim_id)) relation_evidence_ids_json
            FROM extended_object_relation_bindings b
            JOIN system_placements p
              ON p.system_stable_object_key=b.target_system_stable_object_key
             AND p.placement_status='selected' AND p.distance_pc IS NOT NULL
            WHERE b.binding_status='accepted'
            GROUP BY b.extended_object_id,b.stable_object_key,b.target_system_stable_object_key,
                     p.representative_object_key,p.distance_pc,p.dist_ly,
                     p.placement_source,p.placement_method,p.policy_version
            ORDER BY b.extended_object_id,b.target_system_stable_object_key;

            CREATE TABLE selected_extended_object_relation_distance AS
            WITH grouped AS (
              SELECT extended_object_id,min(stable_object_key) stable_object_key,
                     count(*) target_system_count,median(distance_pc) dist_pc,
                     min(distance_pc) distance_low_pc,max(distance_pc) distance_high_pc,
                     (max(distance_pc)-min(distance_pc))/median(distance_pc) distance_spread_fraction,
                     sum(relation_claim_count)::BIGINT relation_claim_count,
                     to_json(list(relation_distance_candidate_id ORDER BY relation_distance_candidate_id))
                       relation_distance_candidate_ids_json,
                     to_json(list(target_system_stable_object_key ORDER BY target_system_stable_object_key))
                       target_system_keys_json
              FROM extended_object_relation_distance_candidates GROUP BY extended_object_id
            )
            SELECT *,CASE WHEN target_system_count=1 THEN 'associated_system_selected_placement_v1'
                          ELSE 'coherent_associated_system_median_v1' END AS method,
                   'accepted_relation_selected_placement' AS distance_confidence,
                   CASE WHEN target_system_count=1 OR distance_spread_fraction<={float(policy['relation_policy']['maximum_coherent_distance_spread_fraction'])}
                        THEN 'selected' ELSE 'conflicting_system_distances' END AS selection_status,
                   {sql_literal(policy['policy_version'])} AS policy_version
            FROM grouped ORDER BY extended_object_id;

            CREATE TABLE extended_object_distance_candidates AS
            SELECT row_number() OVER(ORDER BY canonical_extended_object_id,authority_rank,evidence_id)::BIGINT
                     distance_candidate_id,
                   canonical_extended_object_id extended_object_id,
                   canonical_cluster_stable_object_key stable_object_key,
                   evidence_id,source_record_id,source_id,release_id,source_table,
                   authority_role,authority_rank,
                   CASE source_id
                     WHEN 'clusters.hunt_reffert_2024'
                       THEN try_cast(json_extract_string(parameter_set_raw,'$.dist50') AS DOUBLE)
                     WHEN 'clusters.cantat_gaudin_2020'
                       THEN try_cast(json_extract_string(parameter_set_raw,'$.DistPc') AS DOUBLE)
                   END dist_pc,
                   CASE source_id
                     WHEN 'clusters.hunt_reffert_2024'
                       THEN try_cast(json_extract_string(parameter_set_raw,'$.dist16') AS DOUBLE)
                   END distance_low_pc,
                   CASE source_id
                     WHEN 'clusters.hunt_reffert_2024'
                       THEN try_cast(json_extract_string(parameter_set_raw,'$.dist84') AS DOUBLE)
                   END distance_high_pc,
                   method,model,reference_raw,quality_json,
                   CASE source_id
                     WHEN 'clusters.hunt_reffert_2024' THEN 'current_source_model'
                     WHEN 'clusters.cantat_gaudin_2020' THEN 'supplementary_source_model'
                   END distance_confidence,
                   parameter_set_raw evidence_raw,normalization_version
            FROM cluster_evidence
            WHERE projection_status='eligible_for_quantity_selection'
              AND canonical_extended_object_id IS NOT NULL
              AND CASE source_id
                    WHEN 'clusters.hunt_reffert_2024'
                      THEN try_cast(json_extract_string(parameter_set_raw,'$.dist50') AS DOUBLE)
                    WHEN 'clusters.cantat_gaudin_2020'
                      THEN try_cast(json_extract_string(parameter_set_raw,'$.DistPc') AS DOUBLE)
                  END IS NOT NULL
            ORDER BY extended_object_id,authority_rank,evidence_id;

            INSERT INTO extended_object_distance_candidates
            SELECT (SELECT coalesce(max(distance_candidate_id),0) FROM extended_object_distance_candidates)
                     + row_number() OVER(ORDER BY extended_object_id),
                   extended_object_id,stable_object_key,
                   sha256(concat_ws('|','extended_relation_distance',extended_object_id::VARCHAR,
                     relation_distance_candidate_ids_json,{sql_literal(policy['policy_version'])})),
                   sha256(concat_ws('|','extended_relation_source',extended_object_id::VARCHAR,
                     target_system_keys_json)),
                   'derived.extended_object_relation',{sql_literal(policy['policy_version'])},
                   'selected_extended_object_relation_distance',
                   'accepted_relation_selected_system_placement',150,
                   dist_pc,distance_low_pc,distance_high_pc,method,
                   'selected_system_placement_projection','source_relation_and_selected_placement',
                   json_object('target_system_count',target_system_count,
                     'distance_spread_fraction',distance_spread_fraction,
                     'relation_claim_count',relation_claim_count),
                   distance_confidence,
                   json_object('relation_distance_candidate_ids',relation_distance_candidate_ids_json,
                     'target_system_keys',target_system_keys_json,
                     'placement_build_id',{sql_literal(policy['selected_system_placements']['build_id'])}),
                   {sql_literal(policy['compiler_version'])}
            FROM selected_extended_object_relation_distance
            WHERE selection_status='selected' ORDER BY extended_object_id;

            CREATE TABLE selected_extended_object_distance AS
            SELECT * EXCLUDE(choice) FROM (
              SELECT *,row_number() OVER(PARTITION BY extended_object_id ORDER BY
                authority_rank,evidence_id) choice
              FROM extended_object_distance_candidates
            ) WHERE choice=1 ORDER BY extended_object_id;

            CREATE TABLE extended_objects AS
            SELECT n.extended_object_id,n.stable_object_key,n.canonical_name,n.display_name,
                   n.entity_kind,n.object_family,n.object_type,
                   g.ra_deg,g.dec_deg,coalesce(g.shape_kind,'missing') shape_kind,
                   g.major_axis_arcmin,g.minor_axis_arcmin,g.position_angle_deg,
                   g.source_record_key geometry_source_record_key,
                   CASE WHEN g.ra_deg IS NULL OR g.dec_deg IS NULL THEN 'missing'
                        WHEN g.shape_kind='equivalent_circle' THEN 'derived_area'
                        ELSE 'source' END geometry_status,
                   d.dist_pc,d.dist_pc*{LY_PER_PC} dist_ly,d.distance_low_pc,
                   d.distance_high_pc,coalesce(d.method,'missing') distance_method,
                   coalesce(d.distance_confidence,'missing') distance_confidence,
                   CASE WHEN d.evidence_id IS NULL THEN '{{}}'
                        ELSE json_object('evidence_id',d.evidence_id,'source_record_id',d.source_record_id,
                          'source_id',d.source_id,'release_id',d.release_id,
                          'reference',d.reference_raw,'authority_rank',d.authority_rank)::VARCHAR END distance_evidence_json,
                   CASE WHEN n.object_family='galaxy' THEN 'extragalactic_sky'
                        WHEN n.object_type='globular_cluster' THEN 'deep_galactic'
                        WHEN d.dist_pc IS NULL THEN 'sky_only'
                        WHEN g.ra_deg IS NULL OR g.dec_deg IS NULL THEN 'sky_only'
                        WHEN d.dist_pc*{LY_PER_PC}>{float(policy['local_3d_maximum_ly'])} THEN 'deep_galactic'
                        ELSE 'local_3d' END map_domain,
                   CASE WHEN d.dist_pc IS NULL OR g.ra_deg IS NULL OR g.dec_deg IS NULL
                          OR d.dist_pc*{LY_PER_PC}>{float(policy['local_3d_maximum_ly'])}
                          OR n.object_family='galaxy' OR n.object_type='globular_cluster' THEN NULL
                        WHEN d.dist_pc*{LY_PER_PC}<=100 THEN 100
                        WHEN d.dist_pc*{LY_PER_PC}<=250 THEN 250
                        WHEN d.dist_pc*{LY_PER_PC}<=500 THEN 500
                        ELSE 1000 END::DOUBLE nominal_radius_tier_ly,
                   CASE WHEN d.dist_pc IS NOT NULL AND d.dist_pc*{LY_PER_PC}<={float(policy['local_3d_maximum_ly'])}
                          AND n.object_family<>'galaxy' AND n.object_type<>'globular_cluster' AND g.ra_deg IS NOT NULL
                        THEN d.dist_pc*{LY_PER_PC}*cos(radians(g.dec_deg))*cos(radians(g.ra_deg)) END x_helio_ly,
                   CASE WHEN d.dist_pc IS NOT NULL AND d.dist_pc*{LY_PER_PC}<={float(policy['local_3d_maximum_ly'])}
                          AND n.object_family<>'galaxy' AND n.object_type<>'globular_cluster' AND g.ra_deg IS NOT NULL
                        THEN d.dist_pc*{LY_PER_PC}*cos(radians(g.dec_deg))*sin(radians(g.ra_deg)) END y_helio_ly,
                   CASE WHEN d.dist_pc IS NOT NULL AND d.dist_pc*{LY_PER_PC}<={float(policy['local_3d_maximum_ly'])}
                          AND n.object_family<>'galaxy' AND n.object_type<>'globular_cluster' AND g.ra_deg IS NOT NULL
                        THEN d.dist_pc*{LY_PER_PC}*sin(radians(g.dec_deg)) END z_helio_ly,
                   n.type_policy_version,g.source_id source_catalog,g.release_id source_version,
                   CASE g.source_id
                     WHEN 'extended.green_snr' THEN {sql_literal(policy['source_urls']['extended.green_snr'])}
                     WHEN 'extended.openngc_and_nebulae' THEN {sql_literal(policy['source_urls']['extended.openngc_and_nebulae'])}
                   END::VARCHAR source_url,
                   NULL::VARCHAR source_download_url,NULL::INTEGER source_doi,
                   g.source_record_key source_pk,NULL::INTEGER source_row_id,g.evidence_id source_row_hash,
                   'catalog_citation_required'::VARCHAR license,true redistribution_ok,
                   'Retain source-specific citation and license metadata'::VARCHAR license_note,
                   NULL::INTEGER retrieval_etag,NULL::VARCHAR retrieval_checksum,
                   NULL::VARCHAR retrieved_at,NULL::VARCHAR ingested_at,
                   {sql_literal(policy['compiler_version'])}::VARCHAR transform_version
            FROM seed_nodes n
            LEFT JOIN selected_extended_object_geometry g USING(extended_object_id)
            LEFT JOIN selected_extended_object_distance d USING(extended_object_id)
            ORDER BY n.extended_object_id;

            CREATE TABLE extended_object_aliases AS SELECT * FROM seed_aliases ORDER BY extended_object_alias_id;
            CREATE TABLE extended_object_identifiers AS SELECT * FROM seed_identifiers ORDER BY extended_object_identifier_id;
            CREATE TABLE extended_object_source_reconciliation AS
              SELECT * RENAME(extended_object_reconciliation_id AS extended_object_reconciliation_id)
              FROM seed_reconciliation ORDER BY extended_object_reconciliation_id;
            CREATE TABLE extended_object_identity_quarantine AS SELECT * FROM seed_quarantine ORDER BY extended_object_quarantine_id;
            CREATE TABLE extended_object_search_terms AS
            SELECT row_number() OVER(ORDER BY extended_object_id,alias_priority,alias_norm,alias_raw,extended_object_alias_id)::BIGINT extended_object_search_term_id,
                   extended_object_id,alias_raw term_raw,alias_norm term_norm,
                   alias_kind term_kind,alias_priority term_priority
            FROM extended_object_aliases ORDER BY extended_object_id,term_priority,term_norm,term_raw;
            CREATE TABLE build_metadata(key VARCHAR,value VARCHAR);
            INSERT INTO build_metadata VALUES
              ('build_id',{sql_literal(build_id)}),('build_kind','e7_clean_extended_objects'),
              ('policy_version',{sql_literal(policy['policy_version'])}),
              ('stability_database_opened','0'),('identity_seed_scientific_authority','0');
            CREATE UNIQUE INDEX extended_object_id_uq ON extended_objects(extended_object_id);
            CREATE UNIQUE INDEX extended_object_key_uq ON extended_objects(stable_object_key);
            CREATE INDEX extended_search_norm_idx ON extended_object_search_terms(term_norm);
            CHECKPOINT;
            """
        )
        phase_timings.append({
            "phase": "select_and_materialize",
            "wall_seconds": round(time.monotonic() - phase_started, 6),
            "cpu_seconds": round(time.process_time() - phase_cpu_started, 6),
        })
        tables = [
            "extended_objects", "extended_object_aliases", "extended_object_identifiers",
            "extended_object_source_reconciliation", "extended_object_identity_quarantine",
            "extended_object_search_terms", "extended_object_geometry_candidates",
            "selected_extended_object_geometry",
            "extended_object_distance_candidates",
            "selected_extended_object_distance",
            "extended_object_relation_claims",
            "extended_object_relation_bindings",
            "extended_object_relation_distance_candidates",
            "selected_extended_object_relation_distance",
        ]
        counts = {table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]) for table in tables}
        checks = {
            "identity_inventory_delta": counts["extended_objects"] - int(con.execute("SELECT count(*) FROM seed_nodes").fetchone()[0]),
            "geometry_candidate_delta": counts["extended_object_geometry_candidates"] - candidate_count,
            "relation_claim_delta": counts["extended_object_relation_claims"] - relation_claim_count,
            "duplicate_object_ids": int(con.execute("SELECT count(*) FROM (SELECT extended_object_id FROM extended_objects GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
            "duplicate_stable_keys": int(con.execute("SELECT count(*) FROM (SELECT stable_object_key FROM extended_objects GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
            "orphan_geometry": int(con.execute("SELECT count(*) FROM extended_object_geometry_candidates g LEFT JOIN extended_objects o USING(extended_object_id) WHERE o.extended_object_id IS NULL").fetchone()[0]),
            "selected_geometry_cardinality": int(con.execute("SELECT count(*) FROM (SELECT extended_object_id FROM selected_extended_object_geometry GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "selected_distance_cardinality": int(con.execute("SELECT count(*) FROM (SELECT extended_object_id FROM selected_extended_object_distance GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
            "distance_without_selected_candidate": int(con.execute("SELECT count(*) FROM extended_objects o WHERE o.dist_pc IS NOT NULL AND NOT EXISTS (SELECT 1 FROM selected_extended_object_distance d WHERE d.extended_object_id=o.extended_object_id AND d.dist_pc=o.dist_pc)").fetchone()[0]),
            "nonpositive_distance": int(con.execute("SELECT count(*) FROM extended_objects WHERE dist_pc<=0").fetchone()[0]),
            "local_3d_missing_cartesian": int(con.execute("SELECT count(*) FROM extended_objects WHERE map_domain='local_3d' AND (x_helio_ly IS NULL OR y_helio_ly IS NULL OR z_helio_ly IS NULL)").fetchone()[0]),
            "cartesian_norm_mismatch": int(con.execute("SELECT count(*) FROM extended_objects WHERE map_domain='local_3d' AND abs(sqrt(x_helio_ly*x_helio_ly+y_helio_ly*y_helio_ly+z_helio_ly*z_helio_ly)-dist_ly)>greatest(1e-9,dist_ly*1e-12)").fetchone()[0]),
            "galaxy_family_wrong_map_domain": int(con.execute("SELECT count(*) FROM extended_objects WHERE object_family='galaxy' AND map_domain<>'extragalactic_sky'").fetchone()[0]),
            "accepted_relation_without_system": int(con.execute("SELECT count(*) FROM extended_object_relation_bindings WHERE binding_status='accepted' AND target_system_stable_object_key IS NULL").fetchone()[0]),
            "selected_relation_without_placement": int(con.execute("SELECT count(*) FROM selected_extended_object_relation_distance WHERE selection_status='selected' AND dist_pc IS NULL").fetchone()[0]),
            "relation_distance_without_accepted_binding": int(con.execute("SELECT count(*) FROM extended_object_relation_distance_candidates c WHERE NOT EXISTS (SELECT 1 FROM extended_object_relation_bindings b WHERE b.extended_object_id=c.extended_object_id AND b.target_system_stable_object_key=c.target_system_stable_object_key AND b.binding_status='accepted')").fetchone()[0]),
            "stability_database_reads": 0,
        }
        if any(checks.values()):
            raise ValueError(f"clean extended-object verification failed: {checks}")
        phase_started = time.monotonic()
        phase_cpu_started = time.process_time()
        for table in tables:
            con.execute(
                f"COPY (SELECT * FROM {table}) TO {sql_literal(parquet / (table + '.parquet'))} "
                "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)"
            )
        phase_timings.append({
            "phase": "export_parquet",
            "wall_seconds": round(time.monotonic() - phase_started, 6),
            "cpu_seconds": round(time.process_time() - phase_cpu_started, 6),
        })
        compile_succeeded = True
    finally:
        con.close()
        if not compile_succeeded:
            shutil.rmtree(staging, ignore_errors=True)
    phase_started = time.monotonic()
    phase_cpu_started = time.process_time()
    products = {}
    for path in [db, *sorted(parquet.glob("*.parquet"))]:
        products[str(path.relative_to(staging))] = {
            "bytes": path.stat().st_size, "sha256": file_hash(path),
            "determinism": "logical_tables" if path.suffix == ".duckdb" else "byte_exact",
        }
    phase_timings.append({
        "phase": "hash_products",
        "wall_seconds": round(time.monotonic() - phase_started, 6),
        "cpu_seconds": round(time.process_time() - phase_cpu_started, 6),
    })
    manifest = {
        "schema_version": "spacegate.e7_clean_extended_objects_manifest.v1",
        "build_id": build_id, "status": "pass",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "policy_version": policy["policy_version"], "compiler_version": policy["compiler_version"],
        "policy_sha256": policy_sha, "compiler_sha256": compiler_sha,
        "stability_databases_opened": [], "identity_seed_scientific_authority": False,
        "inputs": {
            "selected_extended_objects_build_id": policy["selected_extended_objects"]["build_id"],
            "clean_clusters_build_id": policy["clean_clusters"]["build_id"],
            "permanent_identity_vocabulary_build_id": policy["permanent_identity_vocabulary"]["build_id"],
            "selected_system_placements_build_id": policy["selected_system_placements"]["build_id"],
        },
        "counts": counts, "verification": checks, "products": products,
        "timing": {
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
            "phase_timings": phase_timings,
            "artifact_bytes": sum(product["bytes"] for product in products.values()),
        },
    }
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staging, final)
    if link_into_state:
        links = state / "derived/evidence_lake_v2/clean_extended_objects"
        links.mkdir(parents=True, exist_ok=True)
        link = links / build_id
        if not link.exists() and not link.is_symlink():
            temporary = links / f".{build_id}.link"
            temporary.unlink(missing_ok=True)
            temporary.symlink_to(final)
            os.replace(temporary, link)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = compile_extended(
        args.policy.resolve(), args.state_dir.resolve(), args.output_root.resolve(),
        link_into_state=not args.no_state_link,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
