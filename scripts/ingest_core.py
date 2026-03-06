#!/usr/bin/env python3
import argparse
import atexit
import datetime as dt
import json
import os
import shutil
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
COORDINATE_EPOCH = "J2016.0"
COORDINATE_FRAME = "ICRS"
GAIA_NSS_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
GAIA_NSS_VERSION = "dr3_tap_partitioned_parallax_gte_3.26156"
GAIA_BACKBONE_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
GAIA_BACKBONE_VERSION = "dr3_gaia_source_parallax_gte_3.26156"
WDS_GAIA_XMATCH_URL = "https://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync"
WDS_GAIA_XMATCH_VERSION = "vizier_B_wds_wds_to_I_355_gaiadr3_best"
MSC_URL = "https://www.ctio.noirlab.edu/~atokovin/stars/newmsc-20240101.tar.gz"
MSC_VERSION = "2024-01-01"
PROX_MAX_DIST_LY = 0.25
PROX_CELL_SIZE_LY = 0.25
PROX_PAIR_ESTIMATE_LIMIT = 50_000_000
WDS_GAIA_MATCH_MAX_ARCSEC_DEFAULT = 2.0
WDS_GAIA_GATE_MAX_DIST_SPREAD_LY_DEFAULT = 10.0
WDS_GAIA_GATE_MAX_PM_DELTA_MASYR_DEFAULT = 25.0


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


def parse_positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise SystemExit(f"Invalid {name} value: {raw!r} (expected positive float)") from exc
    if value <= 0:
        raise SystemExit(f"Invalid {name} value: {raw!r} (must be > 0)")
    return value


def parse_optional_nonnegative_float_env(name: str) -> float | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError as exc:
        raise SystemExit(f"Invalid {name} value: {raw!r} (expected number >= 0)") from exc
    if value < 0:
        raise SystemExit(f"Invalid {name} value: {raw!r} (must be >= 0)")
    return value


def parse_optional_positive_float_env(name: str) -> float | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError as exc:
        raise SystemExit(f"Invalid {name} value: {raw!r} (expected number > 0)") from exc
    if value <= 0:
        raise SystemExit(f"Invalid {name} value: {raw!r} (must be > 0)")
    return value


def parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = raw.strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise SystemExit(f"Invalid {name} value: {raw!r} (expected boolean)")


def parse_spectral_csv_env(name: str) -> list[str]:
    raw = os.getenv(name)
    if raw is None:
        return []
    text = raw.strip()
    if not text:
        return []
    allowed = {"O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D", "UNKNOWN"}
    tokens: list[str] = []
    for part in text.split(","):
        token = part.strip().upper()
        if not token:
            continue
        if token in {"?", "UNK", "UNSPECIFIED"}:
            token = "UNKNOWN"
        if token not in allowed:
            raise SystemExit(
                f"Invalid {name} token: {part!r} (allowed: {', '.join(sorted(allowed))})"
            )
        if token not in tokens:
            tokens.append(token)
    return tokens


def format_float_or_empty(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:g}"


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
    state_dir = Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or root / "data")
    enable_gaia_backbone = os.getenv("SPACEGATE_ENABLE_GAIA_BACKBONE", "0") == "1"
    enable_msc = os.getenv("SPACEGATE_ENABLE_MSC") == "1"
    enable_gaia_nss = os.getenv("SPACEGATE_ENABLE_GAIA_NSS", "1") != "0"
    enable_wds_gaia_xmatch = os.getenv("SPACEGATE_ENABLE_WDS_GAIA_XMATCH") == "1"
    wds_gaia_match_max_arcsec = parse_positive_float_env(
        "SPACEGATE_WDS_GAIA_MATCH_MAX_ARCSEC",
        WDS_GAIA_MATCH_MAX_ARCSEC_DEFAULT,
    )
    wds_gaia_gate_max_dist_spread_ly = parse_positive_float_env(
        "SPACEGATE_WDS_GAIA_GATE_MAX_DIST_SPREAD_LY",
        WDS_GAIA_GATE_MAX_DIST_SPREAD_LY_DEFAULT,
    )
    wds_gaia_gate_max_pm_delta_mas_yr = parse_positive_float_env(
        "SPACEGATE_WDS_GAIA_GATE_MAX_PM_DELTA_MASYR",
        WDS_GAIA_GATE_MAX_PM_DELTA_MASYR_DEFAULT,
    )
    slice_max_distance_ly = parse_optional_positive_float_env("SPACEGATE_SLICE_MAX_DISTANCE_LY")
    slice_min_parallax_over_error = parse_optional_nonnegative_float_env(
        "SPACEGATE_SLICE_MIN_PARALLAX_OVER_ERROR"
    )
    slice_max_parallax_error_mas = parse_optional_nonnegative_float_env(
        "SPACEGATE_SLICE_MAX_PARALLAX_ERROR_MAS"
    )
    slice_max_ruwe = parse_optional_nonnegative_float_env("SPACEGATE_SLICE_MAX_RUWE")
    slice_require_spectral = parse_bool_env("SPACEGATE_SLICE_REQUIRE_SPECTRAL_CLASS", False)
    slice_require_color = parse_bool_env("SPACEGATE_SLICE_REQUIRE_COLOR_INDEX", False)
    slice_allowed_spectral = parse_spectral_csv_env("SPACEGATE_SLICE_ALLOWED_SPECTRAL")
    cooked_athyg = state_dir / "cooked" / "athyg" / "athyg.csv.gz"
    cooked_gaia_backbone = state_dir / "cooked" / "gaia_backbone" / "gaia_dr3_backbone.csv"
    cooked_nasa = state_dir / "cooked" / "nasa_exoplanet_archive" / "pscomppars_clean.csv"
    cooked_wds = state_dir / "cooked" / "wds" / "wds_summary.csv"
    cooked_msc = state_dir / "cooked" / "msc" / "msc_components.csv"
    cooked_orb6 = state_dir / "cooked" / "orb6" / "orb6_orbits.csv"
    cooked_gaia_nss_non_single = state_dir / "cooked" / "gaia_nss" / "gaia_dr3_non_single_star.csv"
    cooked_gaia_nss_two_body = state_dir / "cooked" / "gaia_nss" / "gaia_dr3_nss_two_body_orbit.csv"
    cooked_wds_gaia_xmatch = state_dir / "cooked" / "wds_gaia_xmatch" / "wds_gaia_matches.csv"
    manifest_dir = state_dir / "reports" / "manifests"
    manifest_path = manifest_dir / "core_manifest.json"
    wds_manifest_path = manifest_dir / "wds_manifest.json"
    msc_manifest_path = manifest_dir / "msc_manifest.json"
    orb6_manifest_path = manifest_dir / "orb6_manifest.json"
    gaia_backbone_manifest_path = manifest_dir / "gaia_backbone_manifest.json"
    gaia_nss_manifest_path = manifest_dir / "gaia_nss_manifest.json"
    wds_gaia_xmatch_manifest_path = manifest_dir / "wds_gaia_xmatch_manifest.json"

    if not enable_gaia_backbone and not cooked_athyg.exists():
        raise SystemExit(f"Missing cooked AT-HYG: {cooked_athyg}")
    if enable_gaia_backbone and not cooked_gaia_backbone.exists():
        raise SystemExit(f"Missing cooked Gaia backbone: {cooked_gaia_backbone}")
    if not cooked_nasa.exists():
        raise SystemExit(f"Missing cooked NASA: {cooked_nasa}")
    if not cooked_wds.exists():
        raise SystemExit(f"Missing cooked WDS: {cooked_wds}")
    if enable_msc and not cooked_msc.exists():
        raise SystemExit(f"Missing cooked MSC: {cooked_msc}")
    if not cooked_orb6.exists():
        raise SystemExit(f"Missing cooked ORB6: {cooked_orb6}")
    if enable_gaia_nss and not cooked_gaia_nss_non_single.exists():
        raise SystemExit(f"Missing cooked Gaia NSS non_single_star: {cooked_gaia_nss_non_single}")
    if enable_gaia_nss and not cooked_gaia_nss_two_body.exists():
        raise SystemExit(f"Missing cooked Gaia NSS two_body: {cooked_gaia_nss_two_body}")
    if enable_wds_gaia_xmatch and not cooked_wds_gaia_xmatch.exists():
        raise SystemExit(f"Missing cooked WDS-Gaia XMatch: {cooked_wds_gaia_xmatch}")

    log("Ingest core start")
    log(
        "Slice policy: "
        f"max_distance_ly={format_float_or_empty(slice_max_distance_ly) or '(default)'} "
        f"min_parallax_over_error={format_float_or_empty(slice_min_parallax_over_error) or '(off)'} "
        f"max_parallax_error_mas={format_float_or_empty(slice_max_parallax_error_mas) or '(off)'} "
        f"max_ruwe={format_float_or_empty(slice_max_ruwe) or '(off)'} "
        f"require_spectral={'1' if slice_require_spectral else '0'} "
        f"require_color={'1' if slice_require_color else '0'} "
        f"allowed_spectral={','.join(slice_allowed_spectral) if slice_allowed_spectral else '(all)'}"
    )
    manifest: dict[str, dict] = {}
    manifest_paths = [manifest_path, wds_manifest_path, orb6_manifest_path]
    if enable_gaia_backbone:
        manifest_paths.append(gaia_backbone_manifest_path)
    if enable_msc:
        manifest_paths.append(msc_manifest_path)
    if enable_gaia_nss:
        manifest_paths.append(gaia_nss_manifest_path)
    if enable_wds_gaia_xmatch:
        manifest_paths.append(wds_gaia_xmatch_manifest_path)
    for path in manifest_paths:
        manifest.update(load_manifest(path))

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
    keep_tmp = os.getenv("SPACEGATE_KEEP_TMP") == "1"
    def cleanup_tmp() -> None:
        if keep_tmp:
            return
        if tmp_out_dir.exists():
            log(f"Cleaning up temp output: {tmp_out_dir}")
            shutil.rmtree(tmp_out_dir, ignore_errors=True)
    atexit.register(cleanup_tmp)

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
          ('gaia_backbone_enabled', {sql_literal("1" if enable_gaia_backbone else "0")}),
          ('coordinate_epoch', {sql_literal(COORDINATE_EPOCH)}),
          ('coordinate_frame', {sql_literal(COORDINATE_FRAME)}),
          ('morton_bits_per_axis', {sql_literal(str(BITS_PER_AXIS))}),
          ('morton_max_abs_ly', {sql_literal(str(MORTON_MAX_ABS_LY))}),
          ('morton_scale', {sql_literal(str(MORTON_SCALE))}),
          ('morton_quantization', {sql_literal('round((coord + max_abs) * scale), clamp to [0,N]')}),
          ('morton_frame', {sql_literal('heliocentric_ly')}),
          ('slice_max_distance_ly', {sql_literal(format_float_or_empty(slice_max_distance_ly))}),
          ('slice_min_parallax_over_error', {sql_literal(format_float_or_empty(slice_min_parallax_over_error))}),
          ('slice_max_parallax_error_mas', {sql_literal(format_float_or_empty(slice_max_parallax_error_mas))}),
          ('slice_max_ruwe', {sql_literal(format_float_or_empty(slice_max_ruwe))}),
          ('slice_require_spectral_class', {sql_literal("1" if slice_require_spectral else "0")}),
          ('slice_require_color_index', {sql_literal("1" if slice_require_color else "0")}),
          ('slice_allowed_spectral', {sql_literal(",".join(slice_allowed_spectral))}),
          ('wds_gaia_match_max_arcsec', {sql_literal(str(wds_gaia_match_max_arcsec))}),
          ('wds_gaia_gate_max_dist_spread_ly', {sql_literal(str(wds_gaia_gate_max_dist_spread_ly))}),
          ('wds_gaia_gate_max_pm_delta_mas_yr', {sql_literal(str(wds_gaia_gate_max_pm_delta_mas_yr))})
        """
    )

    log("Loading manifest entries")
    athyg_p1 = (
        require_manifest_entry(manifest, "athyg_v33-1", "AT-HYG part 1")
        if not enable_gaia_backbone
        else None
    )
    athyg_p2 = (
        require_manifest_entry(manifest, "athyg_v33-2", "AT-HYG part 2")
        if not enable_gaia_backbone
        else None
    )
    gaia_backbone_manifest = (
        require_manifest_entry(manifest, "gaia_dr3_backbone", "Gaia DR3 backbone")
        if enable_gaia_backbone
        else None
    )
    nasa_manifest = require_manifest_entry(
        manifest, "pscomppars", "NASA Exoplanet Archive"
    )
    wds_manifest = require_manifest_entry(manifest, "wdsweb_summ2", "WDS")
    orb6_manifest = require_manifest_entry(manifest, "orb6orbits", "ORB6")
    msc_manifest = (
        require_manifest_entry(manifest, "newmsc_20240101", "MSC")
        if enable_msc
        else None
    )
    gaia_nss_non_single_manifest = (
        require_manifest_entry(manifest, "gaia_dr3_non_single_star", "Gaia DR3 non_single_star")
        if enable_gaia_nss
        else None
    )
    gaia_nss_two_body_manifest = (
        require_manifest_entry(manifest, "gaia_dr3_nss_two_body_orbit", "Gaia DR3 nss_two_body_orbit")
        if enable_gaia_nss
        else None
    )
    wds_gaia_xmatch_manifest = (
        require_manifest_entry(manifest, "wds_gaia_xmatch_best", "WDS Gaia XMatch best")
        if enable_wds_gaia_xmatch
        else None
    )

    athyg_checksum = None
    athyg_retrieved = None
    athyg_download_url = None
    athyg_has_retrieval = False
    if athyg_p1 and athyg_p2:
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

    gaia_backbone_checksum = (
        gaia_backbone_manifest.get("sha256") if gaia_backbone_manifest else None
    )
    gaia_backbone_retrieved = (
        gaia_backbone_manifest.get("retrieved_at") if gaia_backbone_manifest else None
    )
    gaia_backbone_download_url = (
        gaia_backbone_manifest.get("url", GAIA_BACKBONE_URL) if gaia_backbone_manifest else None
    )
    gaia_backbone_has_retrieval = has_retrieval(gaia_backbone_manifest or {})

    if enable_gaia_backbone:
        base_source_catalog = "gaia_dr3"
        base_source_version = (
            gaia_backbone_manifest.get("source_version", GAIA_BACKBONE_VERSION)
            if gaia_backbone_manifest
            else GAIA_BACKBONE_VERSION
        )
        base_source_url = gaia_backbone_download_url or GAIA_BACKBONE_URL
        base_source_download_url = gaia_backbone_download_url or GAIA_BACKBONE_URL
        base_source_license = "ESA Gaia DR3"
        base_source_license_note = "https://gea.esac.esa.int/archive/documentation/GDR3/"
        base_source_checksum = gaia_backbone_checksum
        base_source_retrieved = gaia_backbone_retrieved
        base_source_has_retrieval = gaia_backbone_has_retrieval
        base_only_match_method = "gaia_backbone"
    else:
        base_source_catalog = "athyg"
        base_source_version = "v3.3"
        base_source_url = "https://codeberg.org/astronexus/athyg"
        base_source_download_url = athyg_download_url
        base_source_license = "CC BY-SA 4.0"
        base_source_license_note = "https://codeberg.org/astronexus/athyg"
        base_source_checksum = athyg_checksum
        base_source_retrieved = athyg_retrieved
        base_source_has_retrieval = athyg_has_retrieval
        base_only_match_method = "athyg_only"

    nasa_url = nasa_manifest.get(
        "url",
        "https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+*+from+pscomppars&format=csv",
    )
    nasa_sha = nasa_manifest.get("sha256")
    nasa_retrieved = nasa_manifest.get("retrieved_at")
    nasa_has_retrieval = has_retrieval(nasa_manifest)
    msc_sha = msc_manifest.get("sha256") if msc_manifest else None
    msc_retrieved = msc_manifest.get("retrieved_at") if msc_manifest else None
    gaia_nss_non_single_sha = (
        gaia_nss_non_single_manifest.get("sha256") if gaia_nss_non_single_manifest else None
    )
    gaia_nss_non_single_retrieved = (
        gaia_nss_non_single_manifest.get("retrieved_at") if gaia_nss_non_single_manifest else None
    )
    gaia_nss_two_body_sha = (
        gaia_nss_two_body_manifest.get("sha256") if gaia_nss_two_body_manifest else None
    )
    gaia_nss_two_body_retrieved = (
        gaia_nss_two_body_manifest.get("retrieved_at") if gaia_nss_two_body_manifest else None
    )
    wds_gaia_xmatch_sha = (
        wds_gaia_xmatch_manifest.get("sha256") if wds_gaia_xmatch_manifest else None
    )
    wds_gaia_xmatch_retrieved = (
        wds_gaia_xmatch_manifest.get("retrieved_at") if wds_gaia_xmatch_manifest else None
    )

    if enable_gaia_backbone:
        gaia_backbone_path = str(cooked_gaia_backbone).replace("'", "''")
        log("Loading cooked Gaia backbone")
        con.execute(
            f"""
            create or replace temp view gaia_backbone_raw as
            select * from read_csv_auto('{gaia_backbone_path}',
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
            create or replace temp view athyg_raw as
            with base as (
              select
                nullif(source_id, '')::bigint as source_id,
                nullif(ra_deg, '')::double as ra_deg,
                nullif(dec_deg, '')::double as dec_deg,
                nullif(parallax_mas, '')::double as parallax_mas,
                nullif(parallax_error_mas, '')::double as parallax_error_mas,
                nullif(parallax_over_error, '')::double as parallax_over_error,
                nullif(pm_ra_mas_yr, '')::double as pm_ra_mas_yr,
                nullif(pm_dec_mas_yr, '')::double as pm_dec_mas_yr,
                nullif(radial_velocity_kms, '')::double as radial_velocity_kms,
                nullif(phot_g_mag, '')::double as phot_g_mag,
                nullif(bp_rp, '')::double as bp_rp,
                nullif(ruwe, '')::double as ruwe
              from gaia_backbone_raw
            ), coords as (
              select
                *,
                case
                  when parallax_mas is not null and parallax_mas > 0 then 1000.0 / parallax_mas
                  else null
                end as dist_pc
              from base
            )
            select
              source_id::varchar as id,
              null::varchar as tyc,
              source_id::varchar as gaia,
              null::varchar as hyg,
              null::varchar as hip,
              null::varchar as hd,
              null::varchar as hr,
              null::varchar as gl,
              null::varchar as bayer,
              null::varchar as flam,
              null::varchar as con,
              'Gaia DR3 ' || source_id::varchar as proper,
              case when ra_deg is not null then (ra_deg / 15.0)::varchar else null end as ra,
              case when dec_deg is not null then dec_deg::varchar else null end as dec,
              'gaia_dr3' as pos_src,
              case when dist_pc is not null then dist_pc::varchar else null end as dist,
              case when parallax_mas is not null then parallax_mas::varchar else null end as parallax_mas,
              case when parallax_error_mas is not null then parallax_error_mas::varchar else null end as parallax_error_mas,
              case when parallax_over_error is not null then parallax_over_error::varchar else null end as parallax_over_error,
              case when ruwe is not null then ruwe::varchar else null end as ruwe,
              case
                when dist_pc is not null and ra_deg is not null and dec_deg is not null
                  then (dist_pc * cos(dec_deg * pi() / 180.0) * cos(ra_deg * pi() / 180.0))::varchar
                else null
              end as x0,
              case
                when dist_pc is not null and ra_deg is not null and dec_deg is not null
                  then (dist_pc * cos(dec_deg * pi() / 180.0) * sin(ra_deg * pi() / 180.0))::varchar
                else null
              end as y0,
              case
                when dist_pc is not null and dec_deg is not null
                  then (dist_pc * sin(dec_deg * pi() / 180.0))::varchar
                else null
              end as z0,
              'gaia_parallax' as dist_src,
              case when phot_g_mag is not null then phot_g_mag::varchar else null end as mag,
              case
                when phot_g_mag is not null and dist_pc is not null and dist_pc > 0
                  then (phot_g_mag - 5.0 * (log10(dist_pc) - 1.0))::varchar
                else null
              end as absmag,
              case when bp_rp is not null then bp_rp::varchar else null end as ci,
              'gaia_g_bp_rp' as mag_src,
              case when radial_velocity_kms is not null then radial_velocity_kms::varchar else null end as rv,
              'gaia_dr3' as rv_src,
              case when pm_ra_mas_yr is not null then pm_ra_mas_yr::varchar else null end as pm_ra,
              case when pm_dec_mas_yr is not null then pm_dec_mas_yr::varchar else null end as pm_dec,
              'gaia_dr3' as pm_src,
              null::varchar as vx,
              null::varchar as vy,
              null::varchar as vz,
              null::varchar as spect,
              null::varchar as spect_src
            from coords
            where dist_pc is not null
            """
        )
    else:
        athyg_path = str(cooked_athyg).replace("'", "''")
        log("Loading cooked AT-HYG")
        con.execute(
            f"""
            create or replace temp view athyg_raw_source as
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
        con.execute(
            """
            create or replace temp view athyg_raw as
            select
              *,
              null::varchar as parallax_mas,
              null::varchar as parallax_error_mas,
              null::varchar as parallax_over_error,
              null::varchar as ruwe
            from athyg_raw_source
            """
        )

    wds_path = str(cooked_wds).replace("'", "''")
    msc_path = str(cooked_msc).replace("'", "''")
    orb6_path = str(cooked_orb6).replace("'", "''")
    gaia_nss_non_single_path = str(cooked_gaia_nss_non_single).replace("'", "''")
    gaia_nss_two_body_path = str(cooked_gaia_nss_two_body).replace("'", "''")
    wds_gaia_xmatch_path = str(cooked_wds_gaia_xmatch).replace("'", "''")

    log("Loading cooked multiplicity catalogs")
    con.execute(
        f"""
        create or replace temp view wds_raw as
        select * from read_csv_auto('{wds_path}',
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
    if enable_msc:
        con.execute(
            f"""
            create or replace temp view msc_raw as
            select * from read_csv_auto('{msc_path}',
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
    else:
        con.execute(
            """
            create or replace temp view msc_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              wds_id, ra_deg, dec_deg, parallax_mas, parallax_ref, pm_ra_mas_yr, pm_dec_mas_yr,
              radial_velocity_kms, component, sep_arcsec, spectral_type_raw, hip_id, hd_id, bmag,
              vmag, imag, jmag, hmag, kmag, ncomp, grade, other_identifiers, preferred_name,
              subsystem_count, orbit_count
            )
            where false
            """
        )
    con.execute(
        f"""
        create or replace temp view orb6_raw as
        select * from read_csv_auto('{orb6_path}',
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
    if enable_gaia_nss:
        con.execute(
            f"""
            create or replace temp view gaia_nss_non_single_raw as
            select * from read_csv_auto('{gaia_nss_non_single_path}',
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
            create or replace temp view gaia_nss_two_body_raw as
            select * from read_csv_auto('{gaia_nss_two_body_path}',
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
    else:
        con.execute(
            """
            create or replace temp view gaia_nss_non_single_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              source_id, non_single_star, ra_deg, dec_deg, parallax_mas, parallax_error_mas,
              pm_ra_mas_yr, pm_dec_mas_yr, radial_velocity_kms
            )
            where false
            """
        )
        con.execute(
            """
            create or replace temp view gaia_nss_two_body_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              source_id, nss_solution_type, ra_deg, dec_deg, parallax_mas, parallax_error_mas, pm_ra_mas_yr,
              pm_dec_mas_yr, period_days, eccentricity, center_of_mass_velocity_kms, semi_amplitude_primary_kms,
              mass_ratio, inclination_deg, flags, significance
            )
            where false
            """
        )
    if enable_wds_gaia_xmatch:
        con.execute(
            f"""
            create or replace temp view wds_gaia_xmatch_raw as
            select * from read_csv_auto('{wds_gaia_xmatch_path}',
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
    else:
        con.execute(
            """
            create or replace temp view wds_gaia_xmatch_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              wds_id, component, gaia_id, ang_dist_arcsec, obs_last_year, pa_last_deg, sep_last_arcsec,
              mag_primary, mag_secondary, wds_raj2000, wds_dej2000, gaia_dr3_name, gaia_ra_deg, gaia_dec_deg,
              gaia_plx_mas, gaia_pmra_mas_yr, gaia_pmdec_mas_yr, gaia_ruwe, gaia_gmag
            )
            where false
            """
        )
    con.execute(
        """
        create or replace temp view wds_support as
        select distinct nullif(wds_id, '') as wds_id
        from wds_raw
        where nullif(wds_id, '') is not null
        """
    )
    con.execute(
        """
        create or replace temp view orb6_support as
        select distinct nullif(wds_id, '') as wds_id
        from orb6_raw
        where nullif(wds_id, '') is not null
        """
    )

    # Build stars table
    log("Building stars table")
    con.execute(
        f"""
        create or replace temp view athyg_stage_base as
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
            (nullif(ra,'')::double * 15.0) as ra_deg,
            nullif(dec,'')::double as dec_deg,
            nullif(dist,'')::double as dist_pc,
            nullif(parallax_mas,'')::double as parallax_mas,
            nullif(parallax_error_mas,'')::double as parallax_error_mas,
            nullif(parallax_over_error,'')::double as parallax_over_error,
            nullif(ruwe,'')::double as ruwe,
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
            nullif(spect,'') as spectral_type_raw
          from athyg_raw
        ), coords as (
          select *,
            case
              when x_pc is not null and y_pc is not null and z_pc is not null then sqrt(x_pc*x_pc + y_pc*y_pc + z_pc*z_pc)
              else dist_pc
            end as dist_pc_final
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
          end as stable_object_key
        from filtered
        """
    )
    con.execute(
        """
        create temp table athyg_stars_stage as
        select
          row_number() over (order by stable_object_key, source_pk)::bigint as athyg_row_id,
          *
        from athyg_stage_base
        """
    )
    con.execute(
        f"""
        create or replace temp view msc_components as
        with base as (
          select
            row_number() over (
              order by
                coalesce(nullif(wds_id, ''), ''),
                coalesce(nullif(component, ''), ''),
                coalesce(nullif(nullif(hip_id, ''), '0')::bigint, 0),
                coalesce(nullif(nullif(hd_id, ''), '0')::bigint, 0),
                coalesce(nullif(ra_deg, '')::double, 0),
                coalesce(nullif(dec_deg, '')::double, 0)
            )::bigint as msc_row_num,
            nullif(wds_id, '') as wds_id,
            nullif(component, '') as msc_component,
            nullif(nullif(hip_id, ''), '0')::bigint as hip_id,
            nullif(nullif(hd_id, ''), '0')::bigint as hd_id,
            nullif(ra_deg, '')::double as ra_deg,
            nullif(dec_deg, '')::double as dec_deg,
            nullif(parallax_mas, '')::double as parallax_mas,
            nullif(pm_ra_mas_yr, '')::double as pm_ra_mas_yr,
            nullif(pm_dec_mas_yr, '')::double as pm_dec_mas_yr,
            nullif(radial_velocity_kms, '')::double as radial_velocity_kms,
            nullif(vmag, '')::double as vmag,
            nullif(bmag, '')::double as bmag,
            nullif(spectral_type_raw, '') as spectral_type_raw,
            nullif(preferred_name, '') as preferred_name,
            nullif(other_identifiers, '') as other_identifiers,
            nullif(subsystem_count, '')::bigint as subsystem_count,
            nullif(orbit_count, '')::bigint as orbit_count
          from msc_raw
        ), coords as (
          select *,
            case
              when parallax_mas is not null and parallax_mas > 0 then 1000.0 / parallax_mas
              else null
            end as dist_pc
          from base
        ), converted as (
          select *,
            dist_pc * {PC_TO_LY} as dist_ly,
            case
              when dist_pc is not null and ra_deg is not null and dec_deg is not null
                then dist_pc * cos(dec_deg * pi() / 180.0) * cos(ra_deg * pi() / 180.0)
              else null
            end as x_pc,
            case
              when dist_pc is not null and ra_deg is not null and dec_deg is not null
                then dist_pc * cos(dec_deg * pi() / 180.0) * sin(ra_deg * pi() / 180.0)
              else null
            end as y_pc,
            case
              when dist_pc is not null and dec_deg is not null
                then dist_pc * sin(dec_deg * pi() / 180.0)
              else null
            end as z_pc
          from coords
        ), named as (
          select *,
            case
              when preferred_name is not null then preferred_name
              when hip_id is not null then 'HIP ' || hip_id::varchar
              when hd_id is not null then 'HD ' || hd_id::varchar
              when wds_id is not null and msc_component is not null then 'WDS ' || wds_id || ' ' || msc_component
              when wds_id is not null then 'WDS ' || wds_id
              else null
            end as star_name
          from converted
        ), normalized as (
          select *,
            case when star_name is null then null else
              lower(trim(regexp_replace(regexp_replace(star_name, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g')))
            end as star_name_norm,
            regexp_extract(spectral_type_raw, '([OBAFGKMLTY])', 1) as spectral_class,
            regexp_extract(spectral_type_raw, '[OBAFGKMLTY]([0-9](?:\\.[0-9])?)', 1) as spectral_subtype,
            regexp_extract(spectral_type_raw, '(I{1,3}|IV|V|VI|VII)', 1) as luminosity_class,
            lower(regexp_replace(coalesce(msc_component, ''), '[^0-9A-Za-z]+', '', 'g')) as component_norm
          from named
        )
        select *
        from normalized
        where dist_ly is not null and dist_ly <= {MORTON_MAX_ABS_LY}
        """
    )
    con.execute(
        """
        create temp table msc_exact_matches as
        with candidates as (
          select
            m.msc_row_num,
            s.athyg_row_id,
            m.wds_id,
            m.msc_component,
            case
              when m.hip_id is not null and s.hip_id = m.hip_id and m.hd_id is not null and s.hd_id = m.hd_id then 3
              when m.hip_id is not null and s.hip_id = m.hip_id then 2
              when m.hd_id is not null and s.hd_id = m.hd_id then 1
              else 0
            end as match_score,
            case
              when m.hip_id is not null and s.hip_id = m.hip_id and m.hd_id is not null and s.hd_id = m.hd_id then 'msc_exact_hip_hd'
              when m.hip_id is not null and s.hip_id = m.hip_id then 'msc_exact_hip'
              else 'msc_exact_hd'
            end as match_method,
            case
              when m.hip_id is not null and s.hip_id = m.hip_id and m.hd_id is not null and s.hd_id = m.hd_id then 1.0
              when m.hip_id is not null and s.hip_id = m.hip_id then 0.99
              else 0.97
            end as match_confidence
          from msc_components m
          join athyg_stars_stage s
            on (m.hip_id is not null and s.hip_id = m.hip_id)
            or (m.hd_id is not null and s.hd_id = m.hd_id)
        ), ranked as (
          select
            *,
            row_number() over (partition by msc_row_num order by match_score desc, athyg_row_id asc) as rn_msc,
            row_number() over (partition by athyg_row_id order by match_score desc, msc_row_num asc) as rn_star
          from candidates
        )
        select
          msc_row_num,
          athyg_row_id,
          wds_id,
          msc_component,
          match_method,
          match_confidence
        from ranked
        where rn_msc = 1 and rn_star = 1
        """
    )
    con.execute(
        """
        create or replace temp view gaia_nss_non_single as
        select
          cast(nullif(source_id, '') as bigint) as gaia_id,
          coalesce(cast(nullif(non_single_star, '') as int), 0) as non_single_star
        from gaia_nss_non_single_raw
        where cast(nullif(source_id, '') as bigint) is not null
          and coalesce(cast(nullif(non_single_star, '') as int), 0) = 1
        """
    )
    con.execute(
        """
        create or replace temp view gaia_nss_two_body_agg as
        with base as (
          select
            cast(nullif(source_id, '') as bigint) as gaia_id,
            nullif(nss_solution_type, '') as nss_solution_type,
            cast(nullif(significance, '') as double) as significance
          from gaia_nss_two_body_raw
          where cast(nullif(source_id, '') as bigint) is not null
        )
        select
          gaia_id,
          count(*)::bigint as nss_solution_count,
          coalesce(max(significance), 0.0) as nss_significance_max,
          coalesce(
            '[' || string_agg(distinct '"' || replace(nss_solution_type, '"', '\\"') || '"', ',') || ']',
            '[]'
          ) as nss_solution_types_json
        from base
        group by gaia_id
        """
    )
    con.execute(
        f"""
        create or replace temp view wds_gaia_candidates as
        with base as (
          select
            cast(nullif(gaia_id, '') as bigint) as gaia_id,
            nullif(wds_id, '') as wds_id,
            nullif(component, '') as component,
            cast(nullif(ang_dist_arcsec, '') as double) as ang_dist_arcsec,
            cast(nullif(gaia_plx_mas, '') as double) as gaia_plx_mas,
            cast(nullif(gaia_pmra_mas_yr, '') as double) as gaia_pmra_mas_yr,
            cast(nullif(gaia_pmdec_mas_yr, '') as double) as gaia_pmdec_mas_yr
          from wds_gaia_xmatch_raw
          where cast(nullif(gaia_id, '') as bigint) is not null
            and nullif(wds_id, '') is not null
            and cast(nullif(ang_dist_arcsec, '') as double) is not null
            and cast(nullif(ang_dist_arcsec, '') as double) <= {wds_gaia_match_max_arcsec}
        )
        select * from base
        """
    )
    con.execute(
        """
        create or replace temp view wds_gaia_unique as
        with ranked as (
          select
            c.*,
            row_number() over (
              partition by c.wds_id, c.gaia_id
              order by c.ang_dist_arcsec asc, coalesce(c.component, '') asc
            ) as rn
          from wds_gaia_candidates c
        )
        select
          gaia_id,
          wds_id,
          component,
          ang_dist_arcsec,
          gaia_plx_mas,
          gaia_pmra_mas_yr,
          gaia_pmdec_mas_yr
        from ranked
        where rn = 1
        """
    )
    con.execute(
        f"""
        create or replace temp view wds_gaia_group_gate as
        with ast as (
          select
            u.wds_id,
            u.gaia_id,
            case
              when u.gaia_plx_mas is not null and u.gaia_plx_mas > 0
                then (1000.0 / u.gaia_plx_mas) * {PC_TO_LY}
              else null
            end as dist_ly,
            u.gaia_pmra_mas_yr as pmra_mas_yr,
            u.gaia_pmdec_mas_yr as pmdec_mas_yr
          from wds_gaia_unique u
        ), agg as (
          select
            wds_id,
            count(*)::bigint as matched_member_count,
            sum(case when dist_ly is not null then 1 else 0 end)::bigint as dist_member_count,
            sum(case when pmra_mas_yr is not null and pmdec_mas_yr is not null then 1 else 0 end)::bigint as pm_member_count,
            min(dist_ly) as dist_min_ly,
            max(dist_ly) as dist_max_ly,
            case
              when min(dist_ly) is not null and max(dist_ly) is not null then max(dist_ly) - min(dist_ly)
              else null
            end as dist_spread_ly,
            min(pmra_mas_yr) as pmra_min_mas_yr,
            max(pmra_mas_yr) as pmra_max_mas_yr,
            min(pmdec_mas_yr) as pmdec_min_mas_yr,
            max(pmdec_mas_yr) as pmdec_max_mas_yr,
            case
              when min(pmra_mas_yr) is not null and max(pmra_mas_yr) is not null
               and min(pmdec_mas_yr) is not null and max(pmdec_mas_yr) is not null
              then sqrt(
                (max(pmra_mas_yr) - min(pmra_mas_yr)) * (max(pmra_mas_yr) - min(pmra_mas_yr)) +
                (max(pmdec_mas_yr) - min(pmdec_mas_yr)) * (max(pmdec_mas_yr) - min(pmdec_mas_yr))
              )
              else null
            end as pm_vector_spread_mas_yr
          from ast
          group by wds_id
        )
        select
          *,
          case
            when matched_member_count < 2 then true
            when dist_member_count < 2 then false
            when pm_member_count < 2 then false
            when coalesce(dist_spread_ly, 1e18) > {wds_gaia_gate_max_dist_spread_ly} then false
            when coalesce(pm_vector_spread_mas_yr, 1e18) > {wds_gaia_gate_max_pm_delta_mas_yr} then false
            else true
          end as physical_group_pass
        from agg
        """
    )
    con.execute(
        """
        create or replace temp view wds_gaia_star_map_pregate as
        with agg as (
          select gaia_id, count(distinct wds_id) as wds_count
          from wds_gaia_unique
          group by gaia_id
        ), ranked as (
          select
            b.*,
            row_number() over (
              partition by b.gaia_id
              order by b.ang_dist_arcsec asc, b.wds_id asc, coalesce(b.component, '') asc
            ) as rn
          from wds_gaia_unique b
        )
        select
          r.gaia_id,
          r.wds_id,
          r.component as wds_component,
          r.ang_dist_arcsec
        from ranked r
        join agg a using (gaia_id)
        where r.rn = 1 and a.wds_count = 1
        """
    )
    con.execute(
        """
        create or replace temp view wds_gaia_star_map as
        select
          p.gaia_id,
          p.wds_id,
          p.wds_component,
          p.ang_dist_arcsec
        from wds_gaia_star_map_pregate p
        left join wds_gaia_group_gate g using (wds_id)
        where coalesce(g.physical_group_pass, true)
        """
    )
    con.execute(
        f"""
        create or replace temp view final_star_rows as
        with athyg_final as (
          select
            cast(morton3d(a.x_helio_ly, a.y_helio_ly, a.z_helio_ly) as bigint) as spatial_index,
            null::bigint as system_id,
            a.stable_object_key,
            a.star_name,
            a.star_name_norm,
            coalesce(a.component, m.msc_component, w.wds_component) as component,
            a.system_name_root,
            a.system_name_root_norm,
            a.ra_deg,
            a.dec_deg,
            a.dist_ly,
            a.parallax_mas,
            a.parallax_error_mas,
            a.parallax_over_error,
            a.ruwe,
            a.x_helio_ly,
            a.y_helio_ly,
            a.z_helio_ly,
            null::double as x_gal_ly,
            null::double as y_gal_ly,
            null::double as z_gal_ly,
            a.pm_ra_mas_yr,
            a.pm_dec_mas_yr,
            a.radial_velocity_kms,
            a.spectral_type_raw,
            a.spectral_class,
            a.spectral_subtype,
            a.luminosity_class,
            null::varchar as spectral_peculiar,
            a.vmag,
            a.absmag,
            a.color_index,
            a.gaia_id,
            a.hip_id,
            a.hd_id,
            coalesce(m.wds_id, w.wds_id) as wds_id,
            case
              when n.gaia_id is not null and t.gaia_id is not null and m.wds_id is not null then 'gaia_nss_two_body+' || coalesce(m.match_method, 'msc')
              when n.gaia_id is not null and t.gaia_id is not null and m.wds_id is null and w.wds_id is not null then 'gaia_nss_two_body+wds_gaia_xmatch'
              when n.gaia_id is not null and m.wds_id is not null then 'gaia_nss+' || coalesce(m.match_method, 'msc')
              when n.gaia_id is not null and m.wds_id is null and w.wds_id is not null then 'gaia_nss+wds_gaia_xmatch'
              when t.gaia_id is not null and m.wds_id is null and w.wds_id is not null then 'gaia_nss_two_body+wds_gaia_xmatch'
              when m.wds_id is null and w.wds_id is not null then 'wds_gaia_xmatch'
              when t.gaia_id is not null then 'gaia_nss_two_body'
              when n.gaia_id is not null then 'gaia_nss'
              else coalesce(m.match_method, {sql_literal(base_only_match_method)})
            end as multiplicity_match_method,
            greatest(
              coalesce(m.match_confidence, 0.0),
              case
                when w.wds_id is not null then 0.90
                else 0.0
              end,
              case
                when t.gaia_id is not null then 0.99
                when n.gaia_id is not null then 0.96
                else 0.0
              end
            ) as multiplicity_match_confidence,
            case
              when n.gaia_id is not null and t.gaia_id is not null and m.wds_id is not null then '["gaia_nss","gaia_nss_two_body","msc"]'
              when n.gaia_id is not null and t.gaia_id is not null and m.wds_id is null and w.wds_id is not null then '["gaia_nss","gaia_nss_two_body","wds_gaia_xmatch"]'
              when n.gaia_id is not null and t.gaia_id is not null then '["gaia_nss","gaia_nss_two_body"]'
              when n.gaia_id is not null and m.wds_id is not null then '["gaia_nss","msc"]'
              when n.gaia_id is not null and m.wds_id is null and w.wds_id is not null then '["gaia_nss","wds_gaia_xmatch"]'
              when t.gaia_id is not null and m.wds_id is not null then '["gaia_nss_two_body","msc"]'
              when t.gaia_id is not null and m.wds_id is null and w.wds_id is not null then '["gaia_nss_two_body","wds_gaia_xmatch"]'
              when t.gaia_id is not null then '["gaia_nss_two_body"]'
              when n.gaia_id is not null then '["gaia_nss"]'
              when m.wds_id is not null then '["msc"]'
              when w.wds_id is not null then '["wds_gaia_xmatch"]'
              else '[]'
            end as multiplicity_source_catalogs_json,
            coalesce(n.gaia_id is not null, false) as gaia_non_single_star,
            coalesce(t.nss_solution_count, 0) as gaia_nss_solution_count,
            coalesce(t.nss_solution_types_json, '[]') as gaia_nss_solution_types_json,
            t.nss_significance_max as gaia_nss_significance_max,
            json_object(
              'gaia', a.gaia_id,
              'hip', a.hip_id,
              'hd', a.hd_id,
              'hr', a.hr_id,
              'gl', a.gl_id,
              'tyc', a.tyc_id,
              'hyg', a.hyg_id,
              'wds', coalesce(m.wds_id, w.wds_id),
              'wds_component', coalesce(m.msc_component, w.wds_component)
            ) as catalog_ids_json,
            {sql_literal(base_source_catalog)} as source_catalog,
            {sql_literal(base_source_version)} as source_version,
            {sql_literal(base_source_url)} as source_url,
            {sql_literal(base_source_download_url)} as source_download_url,
            null::varchar as source_doi,
            a.source_pk as source_pk,
            a.source_pk as source_row_id,
            null::varchar as source_row_hash,
            {sql_literal(base_source_license)} as license,
            true as redistribution_ok,
            {sql_literal(base_source_license_note)} as license_note,
            null::varchar as retrieval_etag,
            {sql_literal(base_source_checksum)} as retrieval_checksum,
            {sql_literal(base_source_retrieved)} as retrieved_at,
            {sql_literal(ingested_at)} as ingested_at,
            {sql_literal(transform_version)} as transform_version
          from athyg_stars_stage a
          left join msc_exact_matches m on m.athyg_row_id = a.athyg_row_id
          left join wds_gaia_star_map w on w.gaia_id = a.gaia_id
          left join gaia_nss_non_single n on n.gaia_id = a.gaia_id
          left join gaia_nss_two_body_agg t on t.gaia_id = a.gaia_id
        ), msc_only as (
          select
            cast(morton3d(m.x_pc * {PC_TO_LY}, m.y_pc * {PC_TO_LY}, m.z_pc * {PC_TO_LY}) as bigint) as spatial_index,
            null::bigint as system_id,
            case
              when m.hip_id is not null then 'star:hip:' || m.hip_id::varchar
              when m.hd_id is not null then 'star:hd:' || m.hd_id::varchar
              when m.wds_id is not null and m.component_norm <> '' then 'star:wds:' || m.wds_id || ':' || m.component_norm
              else 'star:hash:' || substr(sha256(
                coalesce(m.star_name_norm,'') || '|' ||
                coalesce(round(m.ra_deg,5)::varchar,'') || '|' ||
                coalesce(round(m.dec_deg,5)::varchar,'') || '|' ||
                coalesce(round(m.dist_ly,3)::varchar,'')
              ), 1, 16)
            end as stable_object_key,
            m.star_name,
            m.star_name_norm,
            m.msc_component as component,
            null::varchar as system_name_root,
            null::varchar as system_name_root_norm,
            m.ra_deg,
            m.dec_deg,
            m.dist_ly,
            null::double as parallax_mas,
            null::double as parallax_error_mas,
            null::double as parallax_over_error,
            null::double as ruwe,
            m.x_pc * {PC_TO_LY} as x_helio_ly,
            m.y_pc * {PC_TO_LY} as y_helio_ly,
            m.z_pc * {PC_TO_LY} as z_helio_ly,
            null::double as x_gal_ly,
            null::double as y_gal_ly,
            null::double as z_gal_ly,
            m.pm_ra_mas_yr,
            m.pm_dec_mas_yr,
            m.radial_velocity_kms,
            m.spectral_type_raw,
            m.spectral_class,
            m.spectral_subtype,
            m.luminosity_class,
            null::varchar as spectral_peculiar,
            m.vmag,
            case
              when m.vmag is not null and m.dist_pc is not null and m.dist_pc > 0
                then m.vmag - 5.0 * (log10(m.dist_pc) - 1.0)
              else null
            end as absmag,
            case
              when m.bmag is not null and m.vmag is not null then m.bmag - m.vmag
              else null
            end as color_index,
            null::bigint as gaia_id,
            m.hip_id,
            m.hd_id,
            m.wds_id,
            'msc_insert' as multiplicity_match_method,
            1.0 as multiplicity_match_confidence,
            '["msc"]' as multiplicity_source_catalogs_json,
            false as gaia_non_single_star,
            0::bigint as gaia_nss_solution_count,
            '[]' as gaia_nss_solution_types_json,
            null::double as gaia_nss_significance_max,
            json_object(
              'gaia', null,
              'hip', m.hip_id,
              'hd', m.hd_id,
              'wds', m.wds_id,
              'wds_component', m.msc_component
            ) as catalog_ids_json,
            'msc' as source_catalog,
            {sql_literal(MSC_VERSION)} as source_version,
            {sql_literal(MSC_URL)} as source_url,
            {sql_literal(MSC_URL)} as source_download_url,
            null::varchar as source_doi,
            900000000000 + m.msc_row_num as source_pk,
            900000000000 + m.msc_row_num as source_row_id,
            sha256(
              coalesce(m.wds_id, '') || '|' ||
              coalesce(m.msc_component, '') || '|' ||
              coalesce(round(m.ra_deg, 6)::varchar, '') || '|' ||
              coalesce(round(m.dec_deg, 6)::varchar, '')
            ) as source_row_hash,
            'unspecified' as license,
            true as redistribution_ok,
            'Official public MSC bulk export; explicit license not stated on source page.' as license_note,
            null::varchar as retrieval_etag,
            {sql_literal(msc_sha)} as retrieval_checksum,
            {sql_literal(msc_retrieved)} as retrieved_at,
            {sql_literal(ingested_at)} as ingested_at,
            {sql_literal(transform_version)} as transform_version
          from msc_components m
          left join msc_exact_matches x on x.msc_row_num = m.msc_row_num
          where x.msc_row_num is null
        )
        select * from athyg_final
        union all
        select * from msc_only
        """
    )

    log("Validating Morton domain")
    max_abs = con.execute(
        """
        select max(greatest(abs(x_helio_ly), abs(y_helio_ly), abs(z_helio_ly)))
        from final_star_rows
        """
    ).fetchone()[0]
    if max_abs is not None and max_abs > MORTON_MAX_ABS_LY:
        raise SystemExit(
            f"Morton domain exceeded: max |coord| = {max_abs:.6f} ly "
            f"> {MORTON_MAX_ABS_LY} ly. Increase MORTON_MAX_ABS_LY or filter input."
        )

    slice_conditions: list[str] = []
    if slice_max_distance_ly is not None:
        max_dist = min(slice_max_distance_ly, MORTON_MAX_ABS_LY)
        slice_conditions.append(f"(dist_ly is not null and dist_ly <= {max_dist})")
    if slice_min_parallax_over_error is not None:
        slice_conditions.append(
            f"(parallax_over_error is not null and parallax_over_error >= {slice_min_parallax_over_error})"
        )
    if slice_max_parallax_error_mas is not None:
        slice_conditions.append(
            f"(parallax_error_mas is not null and parallax_error_mas <= {slice_max_parallax_error_mas})"
        )
    if slice_max_ruwe is not None:
        slice_conditions.append(f"(ruwe is not null and ruwe <= {slice_max_ruwe})")
    if slice_require_spectral:
        slice_conditions.append("(spectral_class is not null and spectral_class <> '')")
    if slice_require_color:
        slice_conditions.append("(color_index is not null)")
    if slice_allowed_spectral:
        allowed_list_sql = ", ".join(sql_literal(token) for token in slice_allowed_spectral)
        slice_conditions.append(
            f"(coalesce(upper(spectral_class), 'UNKNOWN') in ({allowed_list_sql}))"
        )
    slice_where_sql = " and ".join(slice_conditions) if slice_conditions else "true"

    con.execute(
        f"""
        create table stars as
        with sliced as (
          select *
          from final_star_rows
          where {slice_where_sql}
        )
        select
          row_number() over (
            order by stable_object_key, source_catalog, coalesce(wds_id, ''), coalesce(component, '')
          )::bigint as star_id,
          *
        from sliced
        """
    )

    slice_input_star_count = con.execute("select count(*) from final_star_rows").fetchone()[0]
    slice_output_star_count = con.execute("select count(*) from stars").fetchone()[0]
    slice_sliced_out_star_count = max(slice_input_star_count - slice_output_star_count, 0)
    slice_sliced_out_star_pct = (
        (float(slice_sliced_out_star_count) / float(slice_input_star_count) * 100.0)
        if slice_input_star_count
        else 0.0
    )
    slice_policy_report = {
        "build_id": build_id,
        "slice_policy": {
            "max_distance_ly": slice_max_distance_ly,
            "min_parallax_over_error": slice_min_parallax_over_error,
            "max_parallax_error_mas": slice_max_parallax_error_mas,
            "max_ruwe": slice_max_ruwe,
            "require_spectral_class": slice_require_spectral,
            "require_color_index": slice_require_color,
            "allowed_spectral_classes": slice_allowed_spectral,
        },
        "counts": {
            "input_star_rows": int(slice_input_star_count),
            "retained_star_rows": int(slice_output_star_count),
            "sliced_out_star_rows": int(slice_sliced_out_star_count),
            "sliced_out_star_pct": slice_sliced_out_star_pct,
        },
        "where_sql": slice_where_sql,
    }
    write_json(reports_dir / "slice_policy_report.json", slice_policy_report)

    # System grouping: WDS first, then name-root, then optional proximity for remaining stars.
    log("System grouping: WDS pass")
    con.execute(
        """
        create temp table wds_groups as
        select star_id, 'wds:' || wds_id as system_group_key
        from stars
        where wds_id is not null
        """
    )
    wds_group_count = con.execute(
        "select count(distinct system_group_key) from wds_groups"
    ).fetchone()[0]
    wds_grouped = con.execute("select count(*) from wds_groups").fetchone()[0]

    log("System grouping: name-based pass")
    name_stage_start = time.monotonic()
    con.execute(
        """
        create temp table name_groups as
        select star_id, 'name:' || system_name_root_norm as system_group_key
        from stars
        where wds_id is null and system_name_root_norm is not null
        """
    )
    name_group_count = con.execute(
        "select count(distinct system_group_key) from name_groups"
    ).fetchone()[0]
    total_stars = con.execute("select count(*) from stars").fetchone()[0]
    name_grouped = con.execute("select count(*) from name_groups").fetchone()[0]
    prox_eligible = total_stars - wds_grouped - name_grouped
    log(f"System grouping: name pass complete in {time.monotonic() - name_stage_start:.1f}s")
    log(
        "System grouping: counts "
        f"(total={total_stars}, wds_grouped={wds_grouped}, name_grouped={name_grouped}, proximity_eligible={prox_eligible})"
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
            where wds_id is null and system_name_root_norm is null
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
            from (select star_id from stars where wds_id is null and system_name_root_norm is null) u
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
            select * from wds_groups
            union all
            select * from name_groups
            union all
            select * from prox_groups
            """
        )
    else:
        con.execute(
            """
            create temp table system_groups as
            select * from wds_groups
            union all
            select * from name_groups
            union all
            select star_id, 'solo:' || stable_object_key as system_group_key
            from stars
            where wds_id is null and system_name_root_norm is null
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
        create temp view system_group_support as
        with grouped as (
          select g.system_group_key, s.wds_id, s.source_catalog, s.gaia_non_single_star
          from system_groups g
          join stars s using (star_id)
        ), aggregated as (
          select
            system_group_key,
            max(g.wds_id) as wds_id,
            max(case when source_catalog = 'msc' then 1 else 0 end) as has_msc_insert,
            max(case when gaia_non_single_star then 1 else 0 end) as has_gaia_nss,
            max(case when w.wds_id is not null then 1 else 0 end) as has_wds_evidence,
            max(case when o.wds_id is not null then 1 else 0 end) as has_orb6_evidence
          from grouped g
          left join wds_support w on w.wds_id = g.wds_id
          left join orb6_support o on o.wds_id = g.wds_id
          group by system_group_key
        )
        select
          system_group_key,
          wds_id,
          case
            when system_group_key like 'wds:%' then 'wds'
            when system_group_key like 'name:%' then 'name_root'
            when system_group_key like 'prox:%' then 'proximity'
            else 'singleton'
          end as grouping_basis,
          has_gaia_nss = 1 as has_gaia_nss_evidence,
          has_msc_insert = 1 as has_msc_evidence,
          has_wds_evidence = 1 as has_wds_evidence,
          has_orb6_evidence = 1 as has_orb6_evidence,
          case
            when system_group_key like 'wds:%' and has_msc_insert = 1 and has_wds_evidence = 1 and has_orb6_evidence = 1 then 0.99
            when system_group_key like 'wds:%' and has_msc_insert = 1 and (has_wds_evidence = 1 or has_orb6_evidence = 1) then 0.97
            when system_group_key like 'wds:%' and has_msc_insert = 1 then 0.95
            when system_group_key like 'wds:%' then 0.90
            when system_group_key like 'name:%' then 0.80
            when system_group_key like 'prox:%' then 0.65
            else 1.0
          end as grouping_confidence,
          case
            when system_group_key like 'wds:%' and has_msc_insert = 1 and has_wds_evidence = 1 and has_orb6_evidence = 1 then '["msc","wds","orb6"]'
            when system_group_key like 'wds:%' and has_msc_insert = 1 and has_wds_evidence = 1 then '["msc","wds"]'
            when system_group_key like 'wds:%' and has_msc_insert = 1 and has_orb6_evidence = 1 then '["msc","orb6"]'
            when system_group_key like 'wds:%' and has_msc_insert = 1 then '["msc"]'
            when system_group_key like 'wds:%' and has_wds_evidence = 1 and has_orb6_evidence = 1 then '["wds","orb6"]'
            when system_group_key like 'wds:%' and has_wds_evidence = 1 then '["wds"]'
            when system_group_key like 'wds:%' and has_orb6_evidence = 1 then '["orb6"]'
            else '[]'
          end as grouping_source_catalogs_json
        from aggregated
        """
    )
    con.execute(
        """
        create table systems as
        with grouped as (
          select s.*, g.system_group_key, sg.wds_id as group_wds_id, sg.grouping_basis, sg.grouping_confidence,
                 sg.has_gaia_nss_evidence, sg.has_msc_evidence, sg.has_wds_evidence, sg.has_orb6_evidence, sg.grouping_source_catalogs_json
          from stars s
          join system_groups g using (star_id)
          join system_group_support sg using (system_group_key)
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
            when grouping_basis = 'wds' and group_wds_id is not null then 'system:wds:' || group_wds_id
            when stable_object_key like 'star:gaia:%' then replace(stable_object_key, 'star:gaia:', 'system:gaia:')
            when stable_object_key like 'star:hip:%' then replace(stable_object_key, 'star:hip:', 'system:hip:')
            when stable_object_key like 'star:hd:%' then replace(stable_object_key, 'star:hd:', 'system:hd:')
            else replace(stable_object_key, 'star:', 'system:')
          end as stable_object_key,
          coalesce(system_name_root, star_name) as system_name,
          coalesce(system_name_root_norm, star_name_norm) as system_name_norm,
          group_wds_id as wds_id,
          grouping_basis,
          grouping_confidence,
          grouping_source_catalogs_json,
          has_gaia_nss_evidence,
          has_msc_evidence,
          has_wds_evidence,
          has_orb6_evidence,
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
          source_catalog,
          source_version,
          source_url,
          source_download_url,
          null::varchar as source_doi,
          source_pk as source_pk,
          source_row_id,
          source_row_hash,
          license,
          redistribution_ok,
          license_note,
          null::varchar as retrieval_etag,
          retrieval_checksum,
          retrieved_at,
          ingested_at,
          transform_version,
          system_group_key
        from system_rows
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
    gaia_nss_star_count = con.execute(
        "select count(*) from stars where gaia_non_single_star"
    ).fetchone()[0]
    gaia_nss_system_count = con.execute(
        "select count(distinct system_id) from stars where gaia_non_single_star"
    ).fetchone()[0]
    gaia_nss_two_body_star_count = con.execute(
        "select count(*) from stars where coalesce(gaia_nss_solution_count, 0) > 0"
    ).fetchone()[0]
    wds_gaia_xmatch_star_count = con.execute(
        "select count(*) from stars where multiplicity_match_method like '%wds_gaia_xmatch%'"
    ).fetchone()[0]
    wds_gaia_xmatch_pregate_mapping_count = con.execute(
        "select count(*) from wds_gaia_star_map_pregate"
    ).fetchone()[0]
    wds_gaia_xmatch_postgate_mapping_count = con.execute(
        "select count(*) from wds_gaia_star_map"
    ).fetchone()[0]
    wds_gaia_xmatch_rejected_mapping_count = (
        wds_gaia_xmatch_pregate_mapping_count - wds_gaia_xmatch_postgate_mapping_count
    )
    wds_gaia_gate_candidate_group_count = con.execute(
        "select count(*) from wds_gaia_group_gate where matched_member_count >= 2"
    ).fetchone()[0]
    wds_gaia_gate_pass_group_count = con.execute(
        "select count(*) from wds_gaia_group_gate where matched_member_count >= 2 and physical_group_pass"
    ).fetchone()[0]
    wds_gaia_gate_rejected_group_count = (
        wds_gaia_gate_candidate_group_count - wds_gaia_gate_pass_group_count
    )
    wds_gaia_gate_dist_reject_group_count = con.execute(
        f"""
        select count(*)
        from wds_gaia_group_gate
        where matched_member_count >= 2
          and (
            dist_member_count < 2
            or coalesce(dist_spread_ly, 1e18) > {wds_gaia_gate_max_dist_spread_ly}
          )
        """
    ).fetchone()[0]
    wds_gaia_gate_pm_reject_group_count = con.execute(
        f"""
        select count(*)
        from wds_gaia_group_gate
        where matched_member_count >= 2
          and (
            pm_member_count < 2
            or coalesce(pm_vector_spread_mas_yr, 1e18) > {wds_gaia_gate_max_pm_delta_mas_yr}
          )
        """
    ).fetchone()[0]

    system_grouping_report = {
        "build_id": build_id,
        "proximity_enabled": proximity_enabled,
        "msc_enabled": enable_msc,
        "gaia_nss_enabled": enable_gaia_nss,
        "wds_gaia_xmatch_enabled": enable_wds_gaia_xmatch,
        "wds_gaia_match_max_arcsec": wds_gaia_match_max_arcsec,
        "wds_gaia_gate_max_dist_spread_ly": wds_gaia_gate_max_dist_spread_ly,
        "wds_gaia_gate_max_pm_delta_mas_yr": wds_gaia_gate_max_pm_delta_mas_yr,
        "wds_group_count": wds_group_count,
        "name_group_count": name_group_count,
        "proximity_group_count": prox_group_count,
        "solo_group_count": solo_group_count,
        "total_systems": system_counts[0],
        "multi_star_systems": system_counts[1],
        "max_component_size": system_counts[2],
        "gaia_nss_star_count": gaia_nss_star_count,
        "gaia_nss_system_count": gaia_nss_system_count,
        "gaia_nss_two_body_star_count": gaia_nss_two_body_star_count,
        "wds_gaia_xmatch_star_count": wds_gaia_xmatch_star_count,
        "wds_gaia_xmatch_pregate_mapping_count": wds_gaia_xmatch_pregate_mapping_count,
        "wds_gaia_xmatch_postgate_mapping_count": wds_gaia_xmatch_postgate_mapping_count,
        "wds_gaia_xmatch_rejected_mapping_count": wds_gaia_xmatch_rejected_mapping_count,
        "wds_gaia_gate_candidate_group_count": wds_gaia_gate_candidate_group_count,
        "wds_gaia_gate_pass_group_count": wds_gaia_gate_pass_group_count,
        "wds_gaia_gate_rejected_group_count": wds_gaia_gate_rejected_group_count,
        "wds_gaia_gate_dist_reject_group_count": wds_gaia_gate_dist_reject_group_count,
        "wds_gaia_gate_pm_reject_group_count": wds_gaia_gate_pm_reject_group_count,
        "proximity_pairs_processed": pair_count,
        "notes": [
            "Grouping precedence: WDS-linked multiplicity first, then name root, then optional proximity, then singleton.",
            (
                "WDS-Gaia XMatch evidence is enabled (SPACEGATE_ENABLE_WDS_GAIA_XMATCH=1); grouping requires physical consistency gate pass for multi-member WDS groups."
                if enable_wds_gaia_xmatch
                else "WDS-Gaia XMatch evidence is disabled by default (SPACEGATE_ENABLE_WDS_GAIA_XMATCH!=1)."
            ),
            (
                "Gaia NSS star-level multiplicity evidence is active in this build."
                if enable_gaia_nss
                else "Gaia NSS star-level multiplicity evidence is disabled (SPACEGATE_ENABLE_GAIA_NSS=0)."
            ),
            (
                "MSC matching is conservative in this pass: exact HIP/HD matches only; unmatched MSC components are inserted as new stars."
                if enable_msc
                else "MSC ingest is disabled by default; WDS/ORB6 remain loaded as support catalogs only."
            ),
        ],
    }
    write_json(reports_dir / "system_grouping_report.json", system_grouping_report)

    if enable_gaia_backbone:
        gaia_backbone_raw_count = con.execute(
            "select count(*) from gaia_backbone_raw"
        ).fetchone()[0]
        gaia_backbone_stars_count = con.execute(
            "select count(*) from stars where source_catalog = 'gaia_dr3'"
        ).fetchone()[0]
        gaia_backbone_report = {
            "build_id": build_id,
            "source_name": "gaia_dr3_backbone",
            "source_version": base_source_version,
            "raw_row_count": gaia_backbone_raw_count,
            "stars_from_backbone_count": gaia_backbone_stars_count,
            "rows_dropped_before_star_emit": gaia_backbone_raw_count - gaia_backbone_stars_count,
            "manifest_row_count": (
                gaia_backbone_manifest.get("row_count")
                if gaia_backbone_manifest
                else None
            ),
            "notes": [
                "rows_dropped_before_star_emit reflects rows filtered by ingest validation and Morton-domain constraints.",
            ],
        }
        write_json(reports_dir / "gaia_backbone_report.json", gaia_backbone_report)

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

    base_source_manifest_block = (
        {
            "source_catalog": "gaia_dr3",
            "manifest": gaia_backbone_manifest,
        }
        if enable_gaia_backbone
        else {
            "source_catalog": "athyg",
            "source_url": "https://codeberg.org/astronexus/athyg",
            "part1": athyg_p1,
            "part2": athyg_p2,
        }
    )

    provenance_report = {
        "build_id": build_id,
        "base_source": base_source_manifest_block,
        "nasa_exoplanet_archive": nasa_manifest,
        "wds": wds_manifest,
        "orb6": orb6_manifest,
        "tables": {
            "stars": table_provenance_report("stars", base_source_has_retrieval),
            "systems": table_provenance_report("systems", base_source_has_retrieval),
            "planets": table_provenance_report("planets", nasa_has_retrieval),
        },
    }
    if not enable_gaia_backbone:
        provenance_report["athyg"] = base_source_manifest_block
    if enable_gaia_backbone:
        provenance_report["gaia_dr3_backbone"] = gaia_backbone_manifest
    if msc_manifest:
        provenance_report["msc"] = msc_manifest
    if gaia_nss_non_single_manifest:
        provenance_report["gaia_nss_non_single_star"] = gaia_nss_non_single_manifest
    if gaia_nss_two_body_manifest:
        provenance_report["gaia_nss_two_body_orbit"] = gaia_nss_two_body_manifest
    if wds_gaia_xmatch_manifest:
        provenance_report["wds_gaia_xmatch_best"] = wds_gaia_xmatch_manifest

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
        "gaia_backbone_enabled": enable_gaia_backbone,
        "base_source_catalog": base_source_catalog,
        "gaia_nss_enabled": enable_gaia_nss,
        "wds_gaia_xmatch_enabled": enable_wds_gaia_xmatch,
        "gaia_nss_star_count": gaia_nss_star_count,
        "gaia_nss_system_count": gaia_nss_system_count,
        "gaia_nss_two_body_star_count": gaia_nss_two_body_star_count,
        "wds_gaia_xmatch_star_count": wds_gaia_xmatch_star_count,
        "wds_gaia_xmatch_pregate_mapping_count": wds_gaia_xmatch_pregate_mapping_count,
        "wds_gaia_xmatch_postgate_mapping_count": wds_gaia_xmatch_postgate_mapping_count,
        "wds_gaia_xmatch_rejected_mapping_count": wds_gaia_xmatch_rejected_mapping_count,
        "wds_gaia_gate_candidate_group_count": wds_gaia_gate_candidate_group_count,
        "wds_gaia_gate_pass_group_count": wds_gaia_gate_pass_group_count,
        "wds_gaia_gate_rejected_group_count": wds_gaia_gate_rejected_group_count,
        "wds_gaia_gate_max_dist_spread_ly": wds_gaia_gate_max_dist_spread_ly,
        "wds_gaia_gate_max_pm_delta_mas_yr": wds_gaia_gate_max_pm_delta_mas_yr,
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
            "System grouping uses WDS-linked multiplicity first, then name-root, then optional proximity grouping for remaining stars (SPACEGATE_ENABLE_PROXIMITY=1).",
            (
                "WDS-Gaia XMatch evidence enabled (SPACEGATE_ENABLE_WDS_GAIA_XMATCH=1) with physical consistency gating on multi-member WDS groups."
                if enable_wds_gaia_xmatch
                else "WDS-Gaia XMatch evidence disabled (SPACEGATE_ENABLE_WDS_GAIA_XMATCH!=1)."
            ),
            (
                "Gaia NSS star-level multiplicity evidence enabled (SPACEGATE_ENABLE_GAIA_NSS!=0)."
                if enable_gaia_nss
                else "Gaia NSS star-level multiplicity evidence disabled (SPACEGATE_ENABLE_GAIA_NSS=0)."
            ),
            (
                "MSC enrichment is conservative in this pass: exact HIP/HD matches only; unmatched MSC components are inserted as new stars."
                if enable_msc
                else "MSC enrichment is disabled by default; current build does not insert MSC-derived component stars."
            ),
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
