#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import heapq
import json
import math
import os
import shutil
import struct
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "srv" / "api"))
from app.queries import choose_display_name_info


SCHEMA_VERSION = "spacegate_map_tile_v2"
MANIFEST_VERSION = "spacegate_map_manifest_v1"
INDEX_VERSION = "spacegate_map_index_v1"
FRAME = "heliocentric_icrs_j2016"
ROOT_HALF_EXTENT_LY = 1024.0
BASE_EXACT_DEPTH = 4
MAX_EXACT_DEPTH = 5
MAX_EXACT_RECORDS = 32768
SAMPLE_DEPTHS = (2, 3)
SAMPLE_LIMIT = 256
RECORD_STRUCT = struct.Struct("<QfffffIIHIHIHIHIHHHBBI")
MAGIC = b"SGTILE1\0"
SPECTRAL_CODES = {value: idx for idx, value in enumerate((
    "UNKNOWN", "O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D",
    "WR", "WD", "NS", "PULSAR", "MAGNETAR", "BLACK HOLE",
))}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        json.dump(payload, stream, indent=2, sort_keys=True)
        stream.write("\n")
        temp_path = Path(stream.name)
    os.replace(temp_path, path)
    path.chmod(0o664)


def morton3(x: int, y: int, z: int, depth: int) -> int:
    value = 0
    for bit in range(depth):
        value |= ((x >> bit) & 1) << (3 * bit)
        value |= ((y >> bit) & 1) << (3 * bit + 1)
        value |= ((z >> bit) & 1) << (3 * bit + 2)
    return value


def tile_id(depth: int, x: int, y: int, z: int) -> str:
    width = max(1, math.ceil(depth * 3 / 4))
    return f"d{depth}-{morton3(x, y, z, depth):0{width}x}"


def cell_index(value: float, depth: int) -> int:
    cells = 1 << depth
    normalized = (float(value) + ROOT_HALF_EXTENT_LY) / (ROOT_HALF_EXTENT_LY * 2.0)
    return max(0, min(cells - 1, int(math.floor(normalized * cells))))


def cell_bounds(depth: int, x: int, y: int, z: int) -> tuple[list[float], list[float], list[float]]:
    width = ROOT_HALF_EXTENT_LY * 2.0 / (1 << depth)
    minimum = [-ROOT_HALF_EXTENT_LY + axis * width for axis in (x, y, z)]
    maximum = [value + width for value in minimum]
    center = [(low + high) / 2.0 for low, high in zip(minimum, maximum)]
    return minimum, maximum, center


def interest_score(row: tuple[Any, ...]) -> float:
    coolness = max(0.0, min(float(row[7] or 0.0), 42.0)) / 42.0
    stars = int(row[9] or 0)
    planets = int(row[10] or 0)
    nice_planets = int(row[12] or 0)
    spectral = str(row[8] or "UNKNOWN").upper()
    rare = 1.0 if spectral in {"O", "B", "L", "T", "Y", "D"} else 0.0
    return min(1.0, coolness * 0.55 + (0.18 if planets else 0.0) + (0.08 if stars > 1 else 0.0) + rare * 0.12 + (0.07 if nice_planets else 0.0))


def deterministic_uniform_key(system_id: int) -> int:
    value = int(system_id) & 0xFFFFFFFFFFFFFFFF
    value ^= value >> 30
    value *= 0xBF58476D1CE4E5B9
    value &= 0xFFFFFFFFFFFFFFFF
    value ^= value >> 27
    value *= 0x94D049BB133111EB
    value &= 0xFFFFFFFFFFFFFFFF
    return value ^ (value >> 31)


@dataclass
class SampleAccumulator:
    represented: int = 0
    top: list[tuple[float, int, tuple[Any, ...]]] = field(default_factory=list)
    uniform: list[tuple[int, int, tuple[Any, ...]]] = field(default_factory=list)

    def add(self, row: tuple[Any, ...]) -> None:
        self.represented += 1
        system_id = int(row[0])
        interest = interest_score(row)
        top_entry = (interest, system_id, row)
        if len(self.top) < SAMPLE_LIMIT // 2:
            heapq.heappush(self.top, top_entry)
        elif top_entry[:2] > self.top[0][:2]:
            heapq.heapreplace(self.top, top_entry)
        uniform_key = deterministic_uniform_key(system_id)
        uniform_entry = (-uniform_key, system_id, row)
        if len(self.uniform) < SAMPLE_LIMIT // 2:
            heapq.heappush(self.uniform, uniform_entry)
        elif uniform_entry[:2] > self.uniform[0][:2]:
            heapq.heapreplace(self.uniform, uniform_entry)

    def rows(self) -> list[tuple[Any, ...]]:
        selected: dict[int, tuple[Any, ...]] = {}
        for _, system_id, row in sorted(self.top, reverse=True):
            selected[system_id] = row
        for _, system_id, row in sorted(self.uniform, reverse=True):
            selected.setdefault(system_id, row)
        return sorted(selected.values(), key=lambda row: int(row[0]))[:SAMPLE_LIMIT]


def encode_tile(
    *,
    depth: int,
    x: int,
    y: int,
    z: int,
    rows: Iterable[tuple[Any, ...]],
    exact: bool,
    represented_count: int,
) -> tuple[bytes, dict[str, Any]]:
    rows = list(rows)
    minimum, maximum, center = cell_bounds(depth, x, y, z)
    strings = bytearray()
    records = bytearray()
    interests: list[float] = []
    planet_systems = 0
    multi_star_systems = 0
    rare_systems = 0
    for row in rows:
        system_id = int(row[0])
        stable_key = str(row[1] or "")
        fallback_name = str(row[2] or stable_key or f"System {system_id}")
        names = [fallback_name, *[str(value or fallback_name) for value in row[14:17]]]
        names = names[:4] + [fallback_name] * max(0, 4 - len(names))
        x_ly, y_ly, z_ly = (float(row[3]), float(row[4]), float(row[5]))
        distance = float(row[6] or math.sqrt(x_ly * x_ly + y_ly * y_ly + z_ly * z_ly))
        coolness = float(row[7] or 0.0)
        spectral = str(row[8] or "UNKNOWN").upper()
        stars = max(0, min(65535, int(row[9] or 0)))
        planets = max(0, min(65535, int(row[10] or 0)))
        coolness_rank = max(0, min(0xFFFFFFFF, int(row[11] or 0)))
        nice_planets = int(row[12] or 0)
        max_teff = max(0, min(0xFFFFFFFF, int(round(float(row[13] or 0)))))
        name_refs: list[int] = []
        for name in names:
            name_bytes = name.encode("utf-8")[:65535]
            name_refs.extend((len(strings), len(name_bytes)))
            strings.extend(name_bytes)
        key_bytes = stable_key.encode("utf-8")[:65535]
        key_offset = len(strings)
        strings.extend(key_bytes)
        flags = (1 if nice_planets > 0 else 0) | (4 if not exact else 0)
        records.extend(RECORD_STRUCT.pack(
            system_id,
            x_ly - center[0], y_ly - center[1], z_ly - center[2],
            distance, coolness, coolness_rank,
            *name_refs, key_offset, len(key_bytes),
            stars, planets, SPECTRAL_CODES.get(spectral, 0), flags, max_teff,
        ))
        score = interest_score(row)
        interests.append(score)
        planet_systems += int(planets > 0)
        multi_star_systems += int(stars > 1)
        rare_systems += int(spectral in {"O", "B", "L", "T", "Y", "D"})
    header = {
        "schema_version": SCHEMA_VERSION,
        "tile_id": tile_id(depth, x, y, z),
        "depth": depth,
        "morton": str(morton3(x, y, z, depth)),
        "cell": [x, y, z],
        "bounds_min_ly": minimum,
        "bounds_max_ly": maximum,
        "origin_ly": center,
        "exact": exact,
        "represented_count": represented_count,
        "emitted_count": len(rows),
        "record_size": RECORD_STRUCT.size,
        "string_bytes": len(strings),
    }
    header_bytes = json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    raw = MAGIC + struct.pack("<I", len(header_bytes)) + header_bytes + records + strings
    interest_sorted = sorted(interests, reverse=True)
    top_k = interest_sorted[: min(16, len(interest_sorted))]
    metadata = {
        **header,
        "interest": {
            "max": max(interests, default=0.0),
            "mean": sum(interests) / len(interests) if interests else 0.0,
            "top_k_mean": sum(top_k) / len(top_k) if top_k else 0.0,
            "planet_systems": planet_systems,
            "multi_star_systems": multi_star_systems,
            "rare_class_systems": rare_systems,
        },
        "raw_bytes": len(raw),
    }
    return raw, metadata


def write_tile(root: Path, radius: int, raw: bytes, metadata: dict[str, Any]) -> dict[str, Any]:
    compressed = gzip.compress(raw, compresslevel=6, mtime=0)
    digest = hashlib.sha256(compressed).hexdigest()
    relative = Path(f"radius-{radius}") / "tiles" / f"{digest}.sgtile.gz"
    output = root / relative
    output.parent.mkdir(parents=True, exist_ok=True)
    if not output.exists():
        output.write_bytes(compressed)
    return {
        **metadata,
        "sha256": digest,
        "compressed_bytes": len(compressed),
        "url": f"/map-tiles/{relative.as_posix()}",
    }


def tile_query(radius: int) -> str:
    width4 = ROOT_HALF_EXTENT_LY * 2.0 / (1 << BASE_EXACT_DEPTH)
    width5 = ROOT_HALF_EXTENT_LY * 2.0 / (1 << MAX_EXACT_DEPTH)
    return f"""
      WITH stellar_class_candidates AS (
        SELECT
          system_id,
          star_id,
          absmag,
          vmag,
          CASE
            WHEN regexp_matches(lower(coalesce(spectral_type_raw, '') || ' ' || coalesce(object_type, '')), 'black[ _-]?hole') THEN 'BLACK HOLE'
            WHEN regexp_matches(lower(coalesce(spectral_type_raw, '') || ' ' || coalesce(object_type, '')), 'magnetar') THEN 'MAGNETAR'
            WHEN lower(coalesce(object_type, '')) = 'pulsar' OR regexp_matches(lower(coalesce(spectral_type_raw, '')), 'pulsar|\\bpsr\\b') THEN 'PULSAR'
            WHEN regexp_matches(lower(coalesce(spectral_type_raw, '') || ' ' || coalesce(object_type, '')), 'neutron[ _-]?star') THEN 'NS'
            WHEN regexp_matches(upper(coalesce(spectral_type_raw, '')), '^W[CNOR]') OR regexp_matches(lower(coalesce(spectral_type_raw, '')), 'wolf[ _-]?rayet') THEN 'WR'
            WHEN lower(coalesce(object_type, '')) = 'white_dwarf' OR upper(coalesce(spectral_class, '')) = 'D' THEN 'WD'
            WHEN upper(coalesce(spectral_class, '')) IN ('O','B','A','F','G','K','M','L','T','Y') THEN upper(spectral_class)
            ELSE 'UNKNOWN'
          END AS stellar_class,
          CASE
            WHEN regexp_matches(lower(coalesce(spectral_type_raw, '') || ' ' || coalesce(object_type, '')), 'black[ _-]?hole') THEN 8.0
            WHEN regexp_matches(upper(coalesce(spectral_type_raw, '')), '^W[CNOR]') OR regexp_matches(lower(coalesce(spectral_type_raw, '')), 'wolf[ _-]?rayet') THEN 20.0
            WHEN regexp_matches(lower(coalesce(spectral_type_raw, '') || ' ' || coalesce(object_type, '')), 'magnetar') THEN 1.4
            WHEN lower(coalesce(object_type, '')) = 'pulsar' OR regexp_matches(lower(coalesce(spectral_type_raw, '')), 'pulsar|\\bpsr\\b') THEN 1.4
            WHEN regexp_matches(lower(coalesce(spectral_type_raw, '') || ' ' || coalesce(object_type, '')), 'neutron[ _-]?star') THEN 1.4
            WHEN lower(coalesce(object_type, '')) = 'white_dwarf' OR upper(coalesce(spectral_class, '')) = 'D' THEN 0.6
            WHEN regexp_matches(upper(coalesce(spectral_type_raw, '')), '(^|[^I])I([^I]|$)') THEN greatest(
              8.0,
              CASE upper(coalesce(spectral_class, '')) WHEN 'O' THEN 20.0 WHEN 'B' THEN 5.0 ELSE 0.0 END
            )
            WHEN regexp_matches(upper(coalesce(spectral_type_raw, '')), 'III') THEN 1.5
            WHEN upper(coalesce(spectral_class, '')) = 'O' THEN 20.0
            WHEN upper(coalesce(spectral_class, '')) = 'B' THEN 5.0
            WHEN upper(coalesce(spectral_class, '')) = 'A' THEN 2.0
            WHEN upper(coalesce(spectral_class, '')) = 'F' THEN 1.3
            WHEN upper(coalesce(spectral_class, '')) = 'G' THEN 1.0
            WHEN upper(coalesce(spectral_class, '')) = 'K' THEN 0.75
            WHEN upper(coalesce(spectral_class, '')) = 'M' THEN 0.3
            WHEN upper(coalesce(spectral_class, '')) IN ('L','T','Y') THEN 0.06
            ELSE 0.0
          END AS mass_proxy_msun
        FROM stars
        WHERE system_id IS NOT NULL
      ), representative_stellar_class AS (
        SELECT system_id, stellar_class
        FROM stellar_class_candidates
        QUALIFY row_number() OVER (
          PARTITION BY system_id
          ORDER BY mass_proxy_msun DESC, absmag ASC NULLS LAST, vmag ASC NULLS LAST, star_id ASC
        ) = 1
      ), positioned AS (
        SELECT s.*,
          greatest(0, least(15, floor((s.x_helio_ly + 1024.0) / {width4})::INTEGER)) AS x4,
          greatest(0, least(15, floor((s.y_helio_ly + 1024.0) / {width4})::INTEGER)) AS y4,
          greatest(0, least(15, floor((s.z_helio_ly + 1024.0) / {width4})::INTEGER)) AS z4,
          greatest(0, least(31, floor((s.x_helio_ly + 1024.0) / {width5})::INTEGER)) AS x5,
          greatest(0, least(31, floor((s.y_helio_ly + 1024.0) / {width5})::INTEGER)) AS y5,
          greatest(0, least(31, floor((s.z_helio_ly + 1024.0) / {width5})::INTEGER)) AS z5
        FROM systems s
        WHERE s.dist_ly <= {float(radius)}
          AND s.x_helio_ly IS NOT NULL AND s.y_helio_ly IS NOT NULL AND s.z_helio_ly IS NOT NULL
      ), counts AS (
        SELECT x4, y4, z4, count(*) AS n FROM positioned GROUP BY ALL
      ), rows_with_tile AS (
        SELECT p.*,
          CASE WHEN c.n > {MAX_EXACT_RECORDS} THEN {MAX_EXACT_DEPTH} ELSE {BASE_EXACT_DEPTH} END AS tile_depth,
          CASE WHEN c.n > {MAX_EXACT_RECORDS} THEN p.x5 ELSE p.x4 END AS tile_x,
          CASE WHEN c.n > {MAX_EXACT_RECORDS} THEN p.y5 ELSE p.y4 END AS tile_y,
          CASE WHEN c.n > {MAX_EXACT_RECORDS} THEN p.z5 ELSE p.z4 END AS tile_z
        FROM positioned p JOIN counts c USING (x4, y4, z4)
      )
      SELECT
        r.system_id, r.stable_object_key, r.system_name,
        r.x_helio_ly, r.y_helio_ly, r.z_helio_ly, r.dist_ly,
        coalesce(c.score_total, 0.0) AS coolness_score,
        coalesce(nullif(rc.stellar_class, ''), nullif(c.dominant_spectral_class, ''), 'UNKNOWN') AS representative_stellar_class,
        coalesce(r.star_count, 0), coalesce(r.planet_count, 0), coalesce(c.rank, 0),
        coalesce(c.nice_planet_count, 0), coalesce(r.max_star_teff_k, 0),
        r.tile_depth, r.tile_x, r.tile_y, r.tile_z
      FROM rows_with_tile r
      LEFT JOIN disc_db.coolness_scores c USING (system_id)
      LEFT JOIN representative_stellar_class rc USING (system_id)
      ORDER BY r.tile_depth, r.tile_x, r.tile_y, r.tile_z, r.system_id
    """


def build_radius(
    con: duckdb.DuckDBPyConnection,
    label_con: duckdb.DuckDBPyConnection,
    output_root: Path,
    radius: int,
    profile: dict[str, Any],
    public_enabled: bool,
) -> dict[str, Any]:
    expected_count = int(con.execute(
        """
        select count(*) from systems
        where dist_ly <= ? and x_helio_ly is not null
          and y_helio_ly is not null and z_helio_ly is not null
        """,
        [radius],
    ).fetchone()[0])
    cursor = con.execute(tile_query(radius))
    exact_tiles: list[dict[str, Any]] = []
    samples: dict[tuple[int, int, int, int], SampleAccumulator] = defaultdict(SampleAccumulator)
    current_key: tuple[int, int, int, int] | None = None
    current_rows: list[tuple[Any, ...]] = []
    exact_count = 0

    def public_display_rows(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        system_ids = [int(row[0]) for row in rows]
        aliases: dict[int, list[dict[str, Any]]] = defaultdict(list)
        if not system_ids:
            return rows
        for system_id, alias_raw, alias_kind, alias_priority in label_con.execute(
            """
            select system_id, alias_raw, alias_kind, alias_priority
            from aliases
            where target_type = 'system'
              and system_id in (select * from unnest(?))
            order by system_id, alias_priority, alias_kind, alias_raw
            """,
            [system_ids],
        ).fetchall():
            aliases[int(system_id)].append({
                "alias_raw": alias_raw,
                "alias_kind": alias_kind,
                "alias_priority": alias_priority,
            })
        output: list[tuple[Any, ...]] = []
        styles = ("public_full", "astronomer_abbrev", "catalog_compact", "source_technical")
        for row in rows:
            names = [
                choose_display_name_info(
                    row[2], aliases.get(int(row[0]), []), root_system=True, name_style=style,
                )["display_name"]
                for style in styles
            ]
            output.append((*row[:2], names[0], *row[3:], *names[1:]))
        return output

    def flush() -> None:
        nonlocal current_rows, exact_count
        if current_key is None or not current_rows:
            return
        depth, x, y, z = current_key
        display_rows = public_display_rows(current_rows)
        raw, metadata = encode_tile(
            depth=depth, x=x, y=y, z=z, rows=display_rows,
            exact=True, represented_count=len(display_rows),
        )
        exact_tiles.append(write_tile(output_root, radius, raw, metadata))
        exact_count += len(current_rows)
        current_rows = []

    while True:
        batch = cursor.fetchmany(50000)
        if not batch:
            break
        for full_row in batch:
            row = tuple(full_row[:14])
            key = tuple(int(value) for value in full_row[14:18])
            if current_key is not None and key != current_key:
                flush()
            current_key = key
            current_rows.append(row)
            for depth in SAMPLE_DEPTHS:
                sample_key = (
                    depth,
                    cell_index(float(row[3]), depth),
                    cell_index(float(row[4]), depth),
                    cell_index(float(row[5]), depth),
                )
                samples[sample_key].add(row)
            if key[0] > BASE_EXACT_DEPTH:
                samples[(BASE_EXACT_DEPTH, key[1] // 2, key[2] // 2, key[3] // 2)].add(row)
    flush()
    if exact_count != expected_count:
        raise RuntimeError(
            f"Radius {radius} exact membership truncated: expected {expected_count}, emitted {exact_count}"
        )

    sample_tiles: list[dict[str, Any]] = []
    for (depth, x, y, z), accumulator in sorted(samples.items()):
        rows = public_display_rows(accumulator.rows())
        raw, metadata = encode_tile(
            depth=depth, x=x, y=y, z=z, rows=rows, exact=False,
            represented_count=accumulator.represented,
        )
        sample_tiles.append(write_tile(output_root, radius, raw, metadata))

    tiles = sample_tiles + exact_tiles
    by_parent: dict[str, list[str]] = defaultdict(list)
    for tile in tiles:
        depth = int(tile["depth"])
        x, y, z = (int(value) for value in tile["cell"])
        parent = tile_id(depth - 1, x // 2, y // 2, z // 2) if depth > min(SAMPLE_DEPTHS) else None
        tile["parent_tile_id"] = parent
        if parent:
            by_parent[parent].append(tile["tile_id"])
    for tile in tiles:
        tile["children"] = sorted(by_parent.get(tile["tile_id"], []))

    compressed_total = sum(int(tile["compressed_bytes"]) for tile in tiles)
    exact_compressed = sum(int(tile["compressed_bytes"]) for tile in exact_tiles)
    planet_systems = sum(int(tile["interest"]["planet_systems"]) for tile in exact_tiles)
    multi_star_systems = sum(int(tile["interest"]["multi_star_systems"]) for tile in exact_tiles)
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "tile_schema_version": SCHEMA_VERSION,
        "build_id": profile["build_id"],
        "generated_at": utc_now(),
        "radius_ly": radius,
        "public_enabled": public_enabled,
        "scope": "systems",
        "coordinate_contract": {
            "frame": FRAME,
            "origin": "Sol barycentric approximation used by served heliocentric coordinates",
            "units": "light_year",
            "root_bounds_ly": [-ROOT_HALF_EXTENT_LY, ROOT_HALF_EXTENT_LY],
            "subdivision": "Cartesian octree; half-open cells except closed positive root boundary",
            "morton_bit_order": "x,y,z least-significant interleave",
            "base_exact_depth": BASE_EXACT_DEPTH,
            "max_exact_depth": MAX_EXACT_DEPTH,
            "split_record_threshold": MAX_EXACT_RECORDS,
            "position_encoding": "cell-relative IEEE-754 float32",
        },
        "identity_contract": {
            "system_id": "unsigned 64-bit little-endian; browser decoder emits exact safe integers or decimal strings",
            "stable_object_key": "UTF-8 string table",
        },
        "representative_class_contract": {
            "version": "mass_proxy_then_intrinsic_brightness_v2",
            "field": "representative_stellar_class",
            "policy": "object/spectral/evolutionary mass proxy; then lowest available absolute magnitude; stable star_id tie-break",
        },
        "coolness_profile": profile,
        "sampling_policy": {
            "version": "spatial_interest_mix_v1",
            "sample_depths": [*SAMPLE_DEPTHS, BASE_EXACT_DEPTH],
            "per_tile_limit": SAMPLE_LIMIT,
            "interest_fraction": 0.5,
            "uniform_fraction": 0.5,
            "exact_membership_unchanged": True,
        },
        "counts": {
            "eligible_systems": exact_count,
            "exact_emitted_systems": exact_count,
            "exact_tiles": len(exact_tiles),
            "sample_tiles": len(sample_tiles),
            "all_tile_artifacts": len(tiles),
            "planet_systems": planet_systems,
            "multi_star_systems": multi_star_systems,
        },
        "bytes": {
            "exact_compressed": exact_compressed,
            "all_compressed": compressed_total,
        },
        "tiles": tiles,
    }
    deterministic_manifest = {key: value for key, value in manifest.items() if key != "generated_at"}
    manifest_bytes = json.dumps(deterministic_manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    manifest["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
    atomic_json(output_root / f"radius-{radius}" / "manifest.json", manifest)
    return manifest


def read_profile(state_dir: Path, con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    active_path = state_dir / "config" / "coolness_profiles" / "active.json"
    active = json.loads(active_path.read_text(encoding="utf-8")) if active_path.exists() else {}
    build_id = str(con.execute("select value from build_metadata where key='build_id'").fetchone()[0])
    row = con.execute("select profile_id, profile_version from disc_db.coolness_scores limit 1").fetchone()
    return {
        "build_id": build_id,
        "profile_id": str(active.get("profile_id") or (row[0] if row else "unknown")),
        "profile_version": str(active.get("profile_version") or (row[1] if row else "unknown")),
        "profile_hash": str(active.get("profile_hash") or "unrecorded"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deterministic Spacegate octree map tiles.")
    parser.add_argument("--state-dir", type=Path, default=Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state")))
    parser.add_argument("--build-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--radii", default="100,250,500,1000")
    parser.add_argument("--public-radii", default="100,250,500,1000")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    build_dir = (args.build_dir or (args.state_dir / "served" / "current")).resolve()
    output_dir = (args.output_dir or (build_dir / "map_tiles")).resolve()
    radii = sorted({int(value) for value in args.radii.split(",") if value.strip()})
    public_radii = sorted({int(value) for value in args.public_radii.split(",") if value.strip()})
    if any(radius not in {100, 250, 500, 1000} for radius in radii):
        raise SystemExit("Supported radii are 100,250,500,1000")
    if any(radius not in radii for radius in public_radii):
        raise SystemExit("Public radii must be included in --radii")
    if output_dir.exists():
        if not args.replace:
            raise SystemExit(f"Output already exists: {output_dir}; pass --replace")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    con = duckdb.connect(str(build_dir / "core.duckdb"), read_only=True)
    label_con = duckdb.connect(str(build_dir / "core.duckdb"), read_only=True)
    con.execute(f"ATTACH '{str(build_dir / 'disc.duckdb').replace("'", "''")}' AS disc_db (READ_ONLY)")
    try:
        profile = read_profile(args.state_dir, con)
        manifests = [
            build_radius(con, label_con, output_dir, radius, profile, radius in public_radii)
            for radius in radii
        ]
    finally:
        label_con.close()
        con.close()
    index = {
        "index_version": INDEX_VERSION,
        "build_id": profile["build_id"],
        "generated_at": utc_now(),
        "public_radii_ly": public_radii,
        "verification_radii_ly": radii,
        "manifests": {
            str(manifest["radius_ly"]): f"/map-tiles/radius-{manifest['radius_ly']}/manifest.json"
            for manifest in manifests
        },
    }
    atomic_json(output_dir / "index.json", index)
    report = {
        "schema_version": "spacegate_map_tile_build_report_v1",
        "build_id": profile["build_id"],
        "output_dir": str(output_dir),
        "radii": [{"radius_ly": m["radius_ly"], "counts": m["counts"], "bytes": m["bytes"], "manifest_sha256": m["manifest_sha256"]} for m in manifests],
    }
    report_dir = args.state_dir / "reports" / profile["build_id"]
    atomic_json(report_dir / "map_tile_build_report.json", report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
