#!/usr/bin/env python3
import argparse
import atexit
import datetime as dt
import json
import os
import signal
import subprocess
import time
from pathlib import Path

import duckdb

PC_TO_LY = 3.26156
BITS_PER_AXIS = 21
MORTON_MAX_ABS_LY = 1000.0
MORTON_N = (1 << BITS_PER_AXIS) - 1
MORTON_SCALE = MORTON_N / (2 * MORTON_MAX_ABS_LY)
PROX_MAX_DIST_LY = 0.25
PROX_CELL_SIZE_LY = 0.25
PROX_PAIR_ESTIMATE_LIMIT = 50_000_000


def log(message: str) -> None:
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{timestamp} {message}", flush=True)


def read_int_file(path: Path) -> int | None:
    try:
        text = path.read_text().strip()
    except Exception:
        return None
    if not text:
        return None
    if text.lower() == "max":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def detect_mem_total_bytes() -> int | None:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return None
    try:
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    return int(parts[1]) * 1024
    except Exception:
        return None
    return None


def detect_cgroup_limit_bytes(mem_total: int | None) -> int | None:
    candidates = [
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ]
    for path in candidates:
        if not path.exists():
            continue
        limit = read_int_file(path)
        if limit is None:
            continue
        # Ignore "unlimited" sentinel values.
        if limit >= (1 << 60):
            continue
        if mem_total is not None and limit > mem_total:
            # Likely an unlimited cgroup value, fall back to mem_total.
            continue
        return limit
    return None


def detect_available_memory_bytes() -> int | None:
    mem_total = detect_mem_total_bytes()
    cgroup_limit = detect_cgroup_limit_bytes(mem_total)
    if mem_total is not None and cgroup_limit is not None:
        return min(mem_total, cgroup_limit)
    if cgroup_limit is not None:
        return cgroup_limit
    return mem_total


def choose_memory_limit_bytes(available_bytes: int) -> int:
    target = int(available_bytes * 0.70)
    min_bytes = 512 * 1024 * 1024
    if target < min_bytes:
        target = min(min_bytes, available_bytes)
    return max(target, 1)


def resolve_duckdb_memory_limit() -> tuple[str | None, str]:
    env_limit = os.getenv("SPACEGATE_DUCKDB_MEMORY_LIMIT")
    if env_limit:
        return env_limit.strip(), "env"
    available = detect_available_memory_bytes()
    if available:
        limit_bytes = choose_memory_limit_bytes(available)
        return f"{limit_bytes}B", "auto"
    return None, "default"


def resolve_duckdb_threads() -> tuple[int | None, str]:
    env_threads = os.getenv("SPACEGATE_DUCKDB_THREADS")
    if env_threads:
        try:
            threads = int(env_threads)
        except ValueError as exc:
            raise SystemExit(
                f"Invalid SPACEGATE_DUCKDB_THREADS value: {env_threads!r} (expected integer)"
            ) from exc
        if threads < 1:
            raise SystemExit(
                f"Invalid SPACEGATE_DUCKDB_THREADS value: {env_threads!r} (must be >= 1)"
            )
        return threads, "env"
    try:
        cpu = len(os.sched_getaffinity(0))
    except AttributeError:
        cpu = os.cpu_count() or 1
    return max(cpu, 1), "auto"


def acquire_lock(lock_path: Path, build_id: str) -> None:
    lock_info = (
        f"pid={os.getpid()}\n"
        f"build_id={build_id}\n"
        f"started_at={dt.datetime.now(dt.UTC).isoformat(timespec='seconds').replace('+00:00', 'Z')}\n"
    )

    def read_lock_details() -> str:
        try:
            return lock_path.read_text()
        except Exception:
            return "(unable to read lock details)"

    def parse_pid(details: str) -> int | None:
        for line in details.splitlines():
            if line.startswith("pid="):
                try:
                    return int(line.split("=", 1)[1].strip())
                except ValueError:
                    return None
        return None

    if lock_path.exists():
        details = read_lock_details()
        pid = parse_pid(details)
        stale = False
        if pid is not None:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                stale = True
            except PermissionError:
                stale = False
        if stale:
            log(f"Stale lockfile detected for pid={pid}; removing {lock_path}")
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
        else:
            raise SystemExit(
                f"Lockfile exists: {lock_path}\n"
                f"{details}\n"
                "Another ingest_core may be running. Remove the lockfile if you are sure it is stale."
            )

    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        details = read_lock_details()
        raise SystemExit(
            f"Lockfile exists: {lock_path}\n"
            f"{details}\n"
            "Another ingest_core may be running. Remove the lockfile if you are sure it is stale."
        )

    with os.fdopen(fd, "w") as handle:
        handle.write(lock_info)

    def cleanup() -> None:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass

    def handle_signal(signum: int, _frame) -> None:
        cleanup()
        raise SystemExit(f"Interrupted by signal {signum}")

    atexit.register(cleanup)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle_signal)


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


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[int, int] = {}
        self.rank: dict[int, int] = {}

    def add(self, x: int) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: int) -> int:
        self.add(x)
        root = self.parent[x]
        if root != x:
            self.parent[x] = self.find(root)
        return self.parent[x]

    def union(self, a: int, b: int) -> None:
        self.add(a)
        self.add(b)
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1

    def items(self) -> list[int]:
        return list(self.parent.keys())


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

    if xi < 0:
        xi = 0
    elif xi > MORTON_N:
        xi = MORTON_N
    if yi < 0:
        yi = 0
    elif yi > MORTON_N:
        yi = MORTON_N
    if zi < 0:
        zi = 0
    elif zi > MORTON_N:
        zi = MORTON_N

    def part1by2(n: int) -> int:
        n &= MORTON_N
        out = 0
        for i in range(BITS_PER_AXIS):
            out |= ((n >> i) & 1) << (3 * i)
        return out

    return part1by2(xi) | (part1by2(yi) << 1) | (part1by2(zi) << 2)


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True))


def has_retrieval(manifest_entry: dict) -> bool:
    if not manifest_entry:
        return False
    return bool(
        manifest_entry.get("sha256")
        or manifest_entry.get("etag")
        or manifest_entry.get("retrieval_etag")
    )


def require_manifest_entry(manifest: dict, source_name: str, label: str) -> dict:
    entry = manifest.get(source_name)
    if not entry:
        raise SystemExit(
            f"Missing manifest entry for {label} ({source_name}). "
            f"Re-run the downloader to refresh $SPACEGATE_STATE_DIR/reports/manifests."
        )
    if not entry.get("retrieved_at"):
        raise SystemExit(
            f"Manifest entry for {label} ({source_name}) missing retrieved_at. "
            f"Re-run the downloader."
        )
    if not entry.get("sha256") and not entry.get("etag") and not entry.get("retrieval_etag"):
        raise SystemExit(
            f"Manifest entry for {label} ({source_name}) missing checksum/etag. "
            f"Re-run the downloader."
        )
    return entry


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--build-id", default=None)
    args = parser.parse_args()

    root = Path(args.root)
    state_dir = Path(os.getenv("SPACEGATE_STATE_DIR") or root / "data")
    cooked_athyg = state_dir / "cooked" / "athyg" / "athyg.csv.gz"
    cooked_nasa = state_dir / "cooked" / "nasa_exoplanet_archive" / "pscomppars_clean.csv"
    manifest_path = state_dir / "reports" / "manifests" / "core_manifest.json"

    if not cooked_athyg.exists():
        raise SystemExit(f"Missing cooked AT-HYG: {cooked_athyg}")
    if not cooked_nasa.exists():
        raise SystemExit(f"Missing cooked NASA: {cooked_nasa}")

    log("Ingest core start")
    manifest = load_manifest(manifest_path)

    now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H%M%SZ")
    build_id = args.build_id or f"{now}_{get_git_sha(root)}"
    log(f"Build id: {build_id}")

    lock_path = state_dir / "out" / ".ingest_core.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    acquire_lock(lock_path, build_id)

    final_out_dir = state_dir / "out" / build_id
    tmp_out_dir = state_dir / "out" / f"{build_id}.tmp"
    parquet_dir = tmp_out_dir / "parquet"
    reports_dir = state_dir / "reports" / build_id

    if final_out_dir.exists():
        raise SystemExit(f"Build output already exists: {final_out_dir}")
    if tmp_out_dir.exists():
        raise SystemExit(f"Temporary build output already exists: {tmp_out_dir}")

    tmp_out_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    tmp_work_dir = tmp_out_dir / "tmp"
    tmp_work_dir.mkdir(parents=True, exist_ok=True)

    db_path = tmp_out_dir / "core.duckdb"

    con = duckdb.connect(str(db_path))
    con.create_function("morton3d", morton3d)
    threads, threads_source = resolve_duckdb_threads()
    if threads is not None:
        con.execute(f"SET threads TO {threads}")
        log(f"DuckDB threads set to {threads} ({threads_source})")

    memory_limit, mem_source = resolve_duckdb_memory_limit()
    if memory_limit is not None:
        con.execute(f"SET memory_limit='{memory_limit}'")
        log(f"DuckDB memory_limit set to {memory_limit} ({mem_source})")
    else:
        log("DuckDB memory_limit not set (using DuckDB default)")
    con.execute(f"SET temp_directory='{str(tmp_work_dir).replace("'", "''")}'")

    ingested_at = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    transform_version = get_git_sha(root)

    log("Writing build_metadata")
    con.execute(
        """
        create table if not exists build_metadata (
          key text,
          value text
        )
        """
    )
    con.execute(
        f"""
        insert into build_metadata values
          ('build_id', {sql_literal(build_id)}),
          ('git_sha', {sql_literal(transform_version)}),
          ('morton_bits_per_axis', {sql_literal(str(BITS_PER_AXIS))}),
          ('morton_max_abs_ly', {sql_literal(str(MORTON_MAX_ABS_LY))}),
          ('morton_scale', {sql_literal(str(MORTON_SCALE))}),
          ('morton_quantization', {sql_literal('round((coord + max_abs) * scale), clamp to [0,N]')}),
          ('morton_frame', {sql_literal('heliocentric_ly')})
        """
    )

    log("Loading manifest entries")
    athyg_p1 = require_manifest_entry(manifest, "athyg_v33-1", "AT-HYG part 1")
    athyg_p2 = require_manifest_entry(manifest, "athyg_v33-2", "AT-HYG part 2")
    nasa_manifest = require_manifest_entry(
        manifest, "pscomppars", "NASA Exoplanet Archive"
    )

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
    athyg_has_retrieval = has_retrieval(athyg_p1) or has_retrieval(athyg_p2)

    nasa_url = nasa_manifest.get(
        "url",
        "https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+pscomppars&format=csv",
    )
    nasa_sha = nasa_manifest.get("sha256")
    nasa_retrieved = nasa_manifest.get("retrieved_at")
    nasa_has_retrieval = has_retrieval(nasa_manifest)

    athyg_path = str(cooked_athyg).replace("'", "''")
    log("Loading cooked AT-HYG")
    con.execute(
        f"""
        create or replace temp view athyg_raw as
        select * from read_csv_auto('{athyg_path}',
            compression='gzip',
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

    # Build stars table
    log("Building stars table")
    con.execute(
        f"""
        create or replace temp view stars_stage_base as
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
            dist_pc_final * {PC_TO_LY} as dist_ly,
            x_pc * {PC_TO_LY} as x_helio_ly,
            y_pc * {PC_TO_LY} as y_helio_ly,
            z_pc * {PC_TO_LY} as z_helio_ly
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
          select * from normalized where dist_ly is not null and dist_ly <= {MORTON_MAX_ABS_LY}
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
        from filtered
        """
    )

    log("Validating Morton domain")
    max_abs = con.execute(
        """
        select max(greatest(abs(x_helio_ly), abs(y_helio_ly), abs(z_helio_ly)))
        from stars_stage_base
        """
    ).fetchone()[0]
    if max_abs is not None and max_abs > MORTON_MAX_ABS_LY:
        raise SystemExit(
            f"Morton domain exceeded: max |coord| = {max_abs:.6f} ly "
            f"> {MORTON_MAX_ABS_LY} ly. Increase MORTON_MAX_ABS_LY or filter input."
        )

    log("Computing spatial_index (Morton)")
    con.execute(
        """
        create or replace temp view stars_stage as
        select *,
          cast(morton3d(x_helio_ly, y_helio_ly, z_helio_ly) as bigint) as spatial_index
        from stars_stage_base
        """
    )

    con.execute(
        """
        create table stars as
        select
          row_number() over (order by stable_object_key)::bigint as star_id,
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
          null::varchar as source_download_url,
          null::varchar as source_doi,
          source_pk as source_pk,
          source_pk as source_row_id,
          null::varchar as source_row_hash,
          'CC BY-SA 4.0' as license,
          true as redistribution_ok,
          'https://codeberg.org/astronexus/athyg' as license_note,
          null::varchar as retrieval_etag,
          null::varchar as retrieval_checksum,
          null::varchar as retrieved_at,
          null::varchar as ingested_at,
          null::varchar as transform_version,
          system_name_root_norm
        from stars_stage
        """,
    )
    con.execute(
        f"""
        update stars set
          source_download_url = {sql_literal(athyg_download_url)},
          retrieval_checksum = {sql_literal(athyg_checksum)},
          retrieved_at = {sql_literal(athyg_retrieved)},
          ingested_at = {sql_literal(ingested_at)},
          transform_version = {sql_literal(transform_version)}
        """
    )

    # System grouping: name-based first, then optional proximity for ungrouped stars.
    log("System grouping: name-based pass")
    name_stage_start = time.monotonic()
    con.execute(
        """
        create temp table name_groups as
        select star_id, 'name:' || system_name_root_norm as system_group_key
        from stars
        where system_name_root_norm is not null
        """
    )
    name_group_count = con.execute(
        "select count(distinct system_group_key) from name_groups"
    ).fetchone()[0]
    total_stars = con.execute("select count(*) from stars").fetchone()[0]
    name_grouped = con.execute("select count(*) from name_groups").fetchone()[0]
    prox_eligible = total_stars - name_grouped
    log(f"System grouping: name pass complete in {time.monotonic() - name_stage_start:.1f}s")
    log(
        "System grouping: counts "
        f"(total={total_stars}, name_grouped={name_grouped}, proximity_eligible={prox_eligible})"
    )

    proximity_enabled = os.getenv("SPACEGATE_ENABLE_PROXIMITY") == "1"
    prox_group_count = 0
    pair_count = 0

    if proximity_enabled and prox_eligible > 0:
        stage_start = time.monotonic()
        log(
            "System grouping: proximity pass "
            f"(cell_size={PROX_CELL_SIZE_LY} ly, max_dist={PROX_MAX_DIST_LY} ly)"
        )
        con.execute(
            f"""
            create or replace temp view ungrouped_cells as
            select
              star_id,
              x_helio_ly as x,
              y_helio_ly as y,
              z_helio_ly as z,
              cast(floor(x_helio_ly / {PROX_CELL_SIZE_LY}) as bigint) as cell_x,
              cast(floor(y_helio_ly / {PROX_CELL_SIZE_LY}) as bigint) as cell_y,
              cast(floor(z_helio_ly / {PROX_CELL_SIZE_LY}) as bigint) as cell_z
            from stars
            where system_name_root_norm is null
            """
        )

        con.execute(
            """
            create or replace temp table neighbor_offsets(dx, dy, dz) as
            select * from (
              values
                (-1,-1,-1), (-1,-1,0), (-1,-1,1),
                (-1,0,-1),  (-1,0,0),  (-1,0,1),
                (-1,1,-1),  (-1,1,0),  (-1,1,1),
                (0,-1,-1),  (0,-1,0),  (0,-1,1),
                (0,0,-1),   (0,0,0),   (0,0,1),
                (0,1,-1),   (0,1,0),   (0,1,1),
                (1,-1,-1),  (1,-1,0),  (1,-1,1),
                (1,0,-1),   (1,0,0),   (1,0,1),
                (1,1,-1),   (1,1,0),   (1,1,1)
            )
            """
        )

        preflight_start = time.monotonic()
        log("System grouping: preflight cell counts (python)")
        cell_counts: dict[tuple[int, int, int], int] = {}
        intra_pairs = 0
        star_count = 0
        cell_max = 0
        last_log = time.monotonic()
        cur = con.execute(
            """
            select cell_x, cell_y, cell_z, count(*)::bigint as cnt
            from ungrouped_cells
            group by 1, 2, 3
            """
        )
        while True:
            rows = cur.fetchmany(10000)
            if not rows:
                break
            for cx, cy, cz, cnt in rows:
                cell_counts[(int(cx), int(cy), int(cz))] = int(cnt)
                star_count += int(cnt)
                intra_pairs += int(cnt) * (int(cnt) - 1) // 2
                if cnt > cell_max:
                    cell_max = int(cnt)
            now = time.monotonic()
            if now - last_log >= 10:
                log(
                    "System grouping: preflight cell counts progress "
                    f"(cells={len(cell_counts):,}, stars={star_count:,})"
                )
                last_log = now

        cell_count = len(cell_counts)
        densest_cells = sorted(
            [(cx, cy, cz, cnt) for (cx, cy, cz), cnt in cell_counts.items()],
            key=lambda item: item[3],
            reverse=True,
        )[:20]

        log(
            "System grouping: preflight cell counts complete "
            f"in {time.monotonic() - preflight_start:.1f}s"
        )

        neighbor_start = time.monotonic()
        log("System grouping: preflight neighbor upper bound (python)")
        neighbor_upper_raw = 0
        last_log = time.monotonic()
        offsets = [
            (-1, -1, -1),
            (-1, -1, 0),
            (-1, -1, 1),
            (-1, 0, -1),
            (-1, 0, 0),
            (-1, 0, 1),
            (-1, 1, -1),
            (-1, 1, 0),
            (-1, 1, 1),
            (0, -1, -1),
            (0, -1, 0),
            (0, -1, 1),
            (0, 0, -1),
            (0, 0, 0),
            (0, 0, 1),
            (0, 1, -1),
            (0, 1, 0),
            (0, 1, 1),
            (1, -1, -1),
            (1, -1, 0),
            (1, -1, 1),
            (1, 0, -1),
            (1, 0, 0),
            (1, 0, 1),
            (1, 1, -1),
            (1, 1, 0),
            (1, 1, 1),
        ]
        processed = 0
        for (cx, cy, cz), cnt in cell_counts.items():
            neighbor_sum = 0
            for dx, dy, dz in offsets:
                neighbor_sum += cell_counts.get((cx + dx, cy + dy, cz + dz), 0)
            neighbor_upper_raw += cnt * max(neighbor_sum - 1, 0)
            processed += 1
            now = time.monotonic()
            if now - last_log >= 10:
                log(
                    "System grouping: preflight neighbor progress "
                    f"(cells={processed:,}/{cell_count:,})"
                )
                last_log = now

        neighbor_upper = neighbor_upper_raw // 2
        log(
            "System grouping: preflight neighbor upper bound complete "
            f"in {time.monotonic() - neighbor_start:.1f}s"
        )

        estimate_pairs = max(intra_pairs, neighbor_upper)
        log(
            "System grouping: preflight "
            f"(cells={cell_count}, max_cell={cell_max}, "
            f"intra_pairs={intra_pairs:,}, neighbor_upper={neighbor_upper:,}) "
            f"in {time.monotonic() - preflight_start:.1f}s"
        )

        if estimate_pairs > PROX_PAIR_ESTIMATE_LIMIT:
            densest_lines = "\n".join(
                [f"  cell({row[0]},{row[1]},{row[2]}): {row[3]}" for row in densest_cells]
            )
            raise SystemExit(
                "Proximity preflight failed: estimated candidate pairs exceed limit.\n"
                f"N stars: {prox_eligible}\n"
                f"Non-empty cells: {cell_count}\n"
                f"Top densest cells:\n{densest_lines}\n"
                f"Estimated candidate pairs: {estimate_pairs:,}\n"
                f"Limit: {PROX_PAIR_ESTIMATE_LIMIT:,}\n"
                "Suggestion: reduce radius, increase cell size, or refactor to incremental neighbor processing."
            )

        uf = UnionFind()
        cells = sorted(cell_counts.keys())

        batch_size = 5000
        batches = 0
        last_log = time.monotonic()
        debug_sql = os.getenv("SPACEGATE_DEBUG_SQL") == "1"
        debug_done = False

        log(f"System grouping: pairing cells (batches of {batch_size})")
        for idx in range(0, len(cells), batch_size):
            batch = cells[idx : idx + batch_size]
            con.execute(
                """
                create or replace temp table batch_cells(
                  cell_x bigint,
                  cell_y bigint,
                  cell_z bigint
                )
                """
            )
            con.executemany("insert into batch_cells values (?, ?, ?)", batch)

            pair_query = f"""
                select a.star_id, b.star_id
                from ungrouped_cells a
                join batch_cells bc
                  on a.cell_x = bc.cell_x
                 and a.cell_y = bc.cell_y
                 and a.cell_z = bc.cell_z
                join neighbor_offsets o on true
                join ungrouped_cells b
                  on b.cell_x = a.cell_x + o.dx
                 and b.cell_y = a.cell_y + o.dy
                 and b.cell_z = a.cell_z + o.dz
                where a.star_id < b.star_id
                  and (
                    (a.x - b.x) * (a.x - b.x) +
                    (a.y - b.y) * (a.y - b.y) +
                    (a.z - b.z) * (a.z - b.z)
                  ) <= {PROX_MAX_DIST_LY * PROX_MAX_DIST_LY}
                """
            if debug_sql and not debug_done:
                explain = con.execute(f"explain analyze {pair_query}").fetchall()
                log("EXPLAIN ANALYZE for proximity batch:")
                for row in explain:
                    print(row[0])
                debug_done = True

            cur = con.execute(pair_query)
            while True:
                batch_pairs = cur.fetchmany(100000)
                if not batch_pairs:
                    break
                for a_id, b_id in batch_pairs:
                    uf.union(a_id, b_id)
                pair_count += len(batch_pairs)
                now = time.monotonic()
                if now - last_log >= 10:
                    log(
                        "System grouping: proximity progress "
                        f"(pairs={pair_count:,}, batches={batches}, "
                        f"cells_processed={min(idx + batch_size, len(cells))}/{len(cells)})"
                    )
                    last_log = now

            batches += 1
            now = time.monotonic()
            if now - last_log >= 10:
                log(
                    "System grouping: proximity progress "
                    f"(pairs={pair_count:,}, batches={batches}, "
                    f"cells_processed={min(idx + batch_size, len(cells))}/{len(cells)})"
                )
                last_log = now

        con.execute(
            """
            create temp table prox_roots(
              star_id bigint,
              root_id bigint
            )
            """
        )
        roots = []
        for star_id in uf.items():
            roots.append((star_id, uf.find(star_id)))
            if len(roots) >= 100000:
                con.executemany("insert into prox_roots values (?, ?)", roots)
                roots = []
        if roots:
            con.executemany("insert into prox_roots values (?, ?)", roots)

        con.execute(
            """
            create temp view prox_roots_full as
            select u.star_id,
                   coalesce(p.root_id, u.star_id) as root_id
            from (select star_id from stars where system_name_root_norm is null) u
            left join prox_roots p using (star_id)
            """
        )

        con.execute(
            """
            create temp view prox_primary as
            select root_id, stable_object_key
            from (
              select pr.root_id,
                     s.stable_object_key,
                     row_number() over (
                       partition by pr.root_id
                       order by s.vmag asc nulls last, s.stable_object_key asc, s.star_id asc
                     ) as rn
              from prox_roots_full pr
              join stars s on s.star_id = pr.star_id
            ) ranked
            where rn = 1
            """
        )

        con.execute(
            """
            create temp table prox_groups as
            select pr.star_id,
                   'prox:' || p.stable_object_key as system_group_key
            from prox_roots_full pr
            join prox_primary p on p.root_id = pr.root_id
            """
        )
        prox_group_count = con.execute(
            "select count(distinct system_group_key) from prox_groups"
        ).fetchone()[0]

        log(
            "System grouping: proximity pass complete "
            f"(pairs_processed={pair_count:,}, elapsed={time.monotonic() - stage_start:.1f}s)"
        )
    elif proximity_enabled:
        log("System grouping: proximity pass skipped (no eligible stars)")
    else:
        log("System grouping: proximity disabled (SPACEGATE_ENABLE_PROXIMITY!=1)")

    if proximity_enabled:
        con.execute(
            """
            create temp table system_groups as
            select * from name_groups
            union all
            select * from prox_groups
            """
        )
    else:
        con.execute(
            """
            create temp table system_groups as
            select * from name_groups
            union all
            select star_id, 'solo:' || stable_object_key as system_group_key
            from stars
            where system_name_root_norm is null
            """
        )

    group_counts = con.execute(
        """
        select
          (select count(*) from stars) as total,
          (select count(*) from system_groups) as grouped,
          (select count(*) from (
            select star_id, count(*) as cnt from system_groups group by star_id having cnt > 1
          )) as dupes
        """
    ).fetchone()
    if group_counts[1] != group_counts[0] or group_counts[2] != 0:
        raise SystemExit(
            "System grouping failed: "
            f"total={group_counts[0]}, grouped={group_counts[1]}, duplicates={group_counts[2]}"
        )

    log("System grouping: creating systems table")
    con.execute(
        """
        create table systems as
        with grouped as (
          select s.*, g.system_group_key
          from stars s
          join system_groups g using (star_id)
        ), primary_star as (
          select *,
            row_number() over (
              partition by system_group_key
              order by vmag asc nulls last, stable_object_key asc
            ) as rn
          from grouped
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
          null::varchar as source_download_url,
          null::varchar as source_doi,
          source_pk as source_pk,
          source_pk as source_row_id,
          null::varchar as source_row_hash,
          'CC BY-SA 4.0' as license,
          true as redistribution_ok,
          'https://codeberg.org/astronexus/athyg' as license_note,
          null::varchar as retrieval_etag,
          null::varchar as retrieval_checksum,
          null::varchar as retrieved_at,
          null::varchar as ingested_at,
          null::varchar as transform_version,
          system_group_key
        from system_rows
        """
    )
    con.execute(
        f"""
        update systems set
          source_download_url = {sql_literal(athyg_download_url)},
          retrieval_checksum = {sql_literal(athyg_checksum)},
          retrieved_at = {sql_literal(athyg_retrieved)},
          ingested_at = {sql_literal(ingested_at)},
          transform_version = {sql_literal(transform_version)}
        """
    )

    con.execute(
        """
        update stars
        set system_id = systems.system_id
        from system_groups
        join systems using (system_group_key)
        where stars.star_id = system_groups.star_id
        """
    )

    con.execute("alter table systems drop column system_group_key")

    system_counts = con.execute(
        """
        select
          count(*) as total_systems,
          sum(case when cnt > 1 then 1 else 0 end) as multi_star_systems,
          max(cnt) as max_component_size
        from (
          select system_id, count(*) as cnt from stars group by system_id
        )
        """
    ).fetchone()

    solo_group_count = con.execute(
        """
        select count(distinct system_group_key)
        from system_groups
        where system_group_key like 'solo:%'
        """
    ).fetchone()[0]

    system_grouping_report = {
        "build_id": build_id,
        "proximity_enabled": proximity_enabled,
        "name_group_count": name_group_count,
        "proximity_group_count": prox_group_count,
        "solo_group_count": solo_group_count,
        "total_systems": system_counts[0],
        "multi_star_systems": system_counts[1],
        "max_component_size": system_counts[2],
        "proximity_pairs_processed": pair_count,
    }
    write_json(reports_dir / "system_grouping_report.json", system_grouping_report)

    con.execute("drop table system_groups")
    con.execute("alter table stars drop column system_name_root")
    con.execute("alter table stars drop column system_name_root_norm")

    # Planets
    log("Building planets table")
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
            cast(nullif(regexp_extract(hip_name, '(\\d+)', 1), '') as bigint) as host_hip_id,
            cast(nullif(regexp_extract(hd_name, '(\\d+)', 1), '') as bigint) as host_hd_id,
            cast(nullif(regexp_extract(coalesce(gaia_dr3_id, gaia_dr2_id, ''), '(\\d+)', 1), '') as bigint) as host_gaia_id
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
          null::varchar as transform_version
        from matches m
        left join stars s on s.star_id = coalesce(gaia_star_id, hip_star_id, hd_star_id, name_star_id)
        """,
    )
    con.execute(
        f"""
        update planets set
          source_download_url = {sql_literal(nasa_url)},
          retrieval_checksum = {sql_literal(nasa_sha)},
          retrieved_at = {sql_literal(nasa_retrieved)},
          ingested_at = {sql_literal(ingested_at)},
          transform_version = {sql_literal(transform_version)}
        """
    )

    # Provenance QC gate
    required_text = [
        "source_catalog",
        "source_version",
        "source_url",
        "source_download_url",
        "license",
        "license_note",
        "retrieved_at",
        "ingested_at",
        "transform_version",
    ]
    required_numeric = ["source_pk"]
    required_bool = ["redistribution_ok"]
    optional_text = ["source_doi"]

    def count_null_text(table: str, col: str) -> int:
        return con.execute(
            f"""
            select count(*) from {table}
            where {col} is null or trim({col}) = ''
            """
        ).fetchone()[0]

    def count_null(table: str, col: str) -> int:
        return con.execute(
            f"select count(*) from {table} where {col} is null"
        ).fetchone()[0]

    def count_row_id_or_hash_missing(table: str) -> int:
        return con.execute(
            f"""
            select count(*) from {table}
            where source_row_id is null
              and (source_row_hash is null or trim(source_row_hash) = '')
            """
        ).fetchone()[0]

    def count_retrieval_missing(table: str) -> int:
        return con.execute(
            f"""
            select count(*) from {table}
            where (retrieval_etag is null or trim(retrieval_etag) = '')
              and (retrieval_checksum is null or trim(retrieval_checksum) = '')
            """
        ).fetchone()[0]

    def table_provenance_report(table: str, require_retrieval: bool) -> dict:
        report = {
            "null_counts": {},
            "row_id_or_hash_missing": 0,
            "retrieval_missing": 0,
            "warnings": [],
        }
        failures = 0

        for col in required_text:
            cnt = count_null_text(table, col)
            report["null_counts"][col] = cnt
            if cnt:
                failures += cnt

        for col in required_numeric:
            cnt = count_null(table, col)
            report["null_counts"][col] = cnt
            if cnt:
                failures += cnt

        for col in required_bool:
            cnt = count_null(table, col)
            report["null_counts"][col] = cnt
            if cnt:
                failures += cnt

        for col in optional_text:
            report["null_counts"][col] = count_null_text(table, col)

        row_missing = count_row_id_or_hash_missing(table)
        report["row_id_or_hash_missing"] = row_missing
        if row_missing:
            failures += row_missing

        retrieval_missing = count_retrieval_missing(table)
        report["retrieval_missing"] = retrieval_missing
        if retrieval_missing and require_retrieval:
            failures += retrieval_missing
        elif retrieval_missing and not require_retrieval:
            report["warnings"].append(
                "retrieval_etag/checksum missing but raw manifest lacks them"
            )

        report["failures"] = failures
        return report

    provenance_report = {
        "build_id": build_id,
        "athyg": {
            "source_url": "https://codeberg.org/astronexus/athyg",
            "part1": athyg_p1,
            "part2": athyg_p2,
        },
        "nasa_exoplanet_archive": nasa_manifest,
        "tables": {
            "stars": table_provenance_report("stars", athyg_has_retrieval),
            "systems": table_provenance_report("systems", athyg_has_retrieval),
            "planets": table_provenance_report("planets", nasa_has_retrieval),
        },
    }

    write_json(reports_dir / "provenance_report.json", provenance_report)

    total_failures = sum(
        provenance_report["tables"][name]["failures"]
        for name in ("stars", "systems", "planets")
    )
    if total_failures > 0:
        raise SystemExit(
            f"Provenance QC failed: {total_failures} missing required fields. "
            f"See {reports_dir / 'provenance_report.json'}"
        )

    # Reports
    log("QC checks")
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

    dist_violations_stars = con.execute(
        """
        select count(*) from stars
        where dist_ly is not null and x_helio_ly is not null and y_helio_ly is not null and z_helio_ly is not null
          and abs(sqrt(x_helio_ly*x_helio_ly + y_helio_ly*y_helio_ly + z_helio_ly*z_helio_ly) - dist_ly) > 1e-3
        """
    ).fetchone()[0]

    dist_violations_systems = con.execute(
        """
        select count(*) from systems
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
        "dist_invariant_violations": dist_violations_stars + dist_violations_systems,
        "dist_invariant_violations_stars": dist_violations_stars,
        "dist_invariant_violations_systems": dist_violations_systems,
        "provenance_missing_stars": provenance_missing,
        "morton": {
            "bits_per_axis": BITS_PER_AXIS,
            "max_abs_ly": MORTON_MAX_ABS_LY,
            "scale": MORTON_SCALE,
            "n": MORTON_N,
        },
        "notes": [
            "System grouping uses name-root, then optional proximity grouping for remaining stars (SPACEGATE_ENABLE_PROXIMITY=1).",
        ],
    }

    match_report = {
        "build_id": build_id,
        "match_counts": [{"method": row[0], "count": row[1]} for row in match_counts],
    }

    write_json(reports_dir / "qc_report.json", qc_report)
    write_json(reports_dir / "match_report.json", match_report)

    if dist_violations_stars + dist_violations_systems > 0:
        raise SystemExit(
            "QC failed: distance invariant violations detected. "
            f"See {reports_dir / 'qc_report.json'}"
        )

    # Parquet exports (sorted by spatial_index)
    log("Writing Parquet exports")
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
    tmp_out_dir.rename(final_out_dir)
    log(f"Promoted build output to {final_out_dir}")
    log("Ingest core complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
