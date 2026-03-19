#!/usr/bin/env python3
import argparse
import atexit
import datetime as dt
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import duckdb

PC_TO_LY = 3.26156
GAIA_BOUNDARY_MIN_PARALLAX_MAS_DEFAULT = 3.26156
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
SBX_URL = "https://astro.ulb.ac.be/sbx/tap/sync"
SBX_VERSION = "sbx_tap_parallax_gte_3.26156"
MSC_URL = "https://www.ctio.noirlab.edu/~atokovin/stars/newmsc-20240101.tar.gz"
MSC_VERSION = "2024-01-01"
GAIA_CLASSPROB_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
GAIA_CLASSPROB_VERSION = "dr3_astrophysical_parameters_parallax_gte_3.26156"
ATNF_URL = "https://www.atnf.csiro.au/research/pulsar/psrcat/"
ATNF_VERSION = "psrcat_pkg"
MAGNETAR_URL = "https://www.physics.mcgill.ca/~pulsar/magnetar/"
MAGNETAR_VERSION = "TabO1"
WHITE_DWARF_URL = "https://warwick.ac.uk/fac/sci/physics/research/astro/research/catalogues/gaiaedr3_wd_main.fits.gz"
WHITE_DWARF_VERSION = "gaiaedr3_wd_main"
GAIA_UCD_URL = "https://cdsarc.cds.unistra.fr/ftp/J/A+A/669/A139/table4.dat"
GAIA_UCD_VERSION = "2023A&A...669A.139S"
CLUSTERS_URL = "https://cdsarc.cds.unistra.fr/ftp/J/A+A/640/A1/"
CLUSTERS_VERSION = "2020A&A...640A...1C"
SNR_URL = "https://www.mrao.cam.ac.uk/surveys/snrs/"
SNR_VERSION = "2024-10"
DEBCAT_URL = "https://www.astro.keele.ac.uk/jkt/debcat/debs.dat"
DEBCAT_VERSION = "debs_dat"
KEPLER_EB_URL = "https://keplerebs.villanova.edu/"
KEPLER_EB_VERSION = "third_revision_2019-08-08"
TESS_EB_URL = "https://tessebs.villanova.edu/search_results"
TESS_EB_VERSION = "search_results_in_catalog_v1"
ATHYG_ALIAS_URL = "https://codeberg.org/astronexus/athyg"
ATHYG_ALIAS_VERSION = "v3.3"
EXOPLANET_EU_URL = "https://www.exoplanet.eu/catalog/"
EXOPLANET_EU_VERSION = "catalog_csv"
OPEN_EXOPLANET_CATALOGUE_URL = "https://github.com/OpenExoplanetCatalogue/open_exoplanet_catalogue"
OPEN_EXOPLANET_CATALOGUE_VERSION = "tarball_master"
HWC_URL = "https://phl.upr.edu/hwc/data"
HWC_VERSION = "hwc_csv"
SOL_AUTHORITY_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
SOL_AUTHORITY_VERSION = "horizons_s1_j2016"
EARTH_RADIUS_KM = 6371.0088
EARTH_MASS_KG = 5.9722e24
PROX_MAX_DIST_LY = 0.25
PROX_CELL_SIZE_LY = 0.25
PROX_PAIR_ESTIMATE_LIMIT = 50_000_000
WDS_GAIA_MATCH_MAX_ARCSEC_DEFAULT = 2.0
WDS_GAIA_GATE_MAX_DIST_SPREAD_LY_DEFAULT = 10.0
WDS_GAIA_GATE_MAX_PM_DELTA_MASYR_DEFAULT = 25.0
WDS_IDENTIFIER_BRIDGE_MAX_ANG_ARCSEC = 3.0
WDS_IDENTIFIER_BRIDGE_MAX_DIST_DELTA_LY = 1.0
WHITE_DWARF_PROB_THRESHOLD = 0.5
WHITE_DWARF_CATALOG_PWD_THRESHOLD_DEFAULT = 0.8
ALIAS_NAME_OVERRIDE_LIMIT = 200000
ALIAS_POS_MAX_DELTA_RA_DEG = 0.12
ALIAS_POS_MAX_DELTA_DEC_DEG = 0.12
ALIAS_POS_MAX_DELTA_DIST_LY = 1.0
ALIAS_POS_MAX_ANG_SEP_ARCSEC = 45.0
ATHYG_MERGE_SKY_BIN_FACTOR = 4.0  # 0.25 degree bins
ATHYG_MERGE_POSITIONAL_CONFIDENCE_NAMED = 0.90
ATHYG_MERGE_POSITIONAL_CONFIDENCE_NUMERIC = 0.88
ATHYG_MERGE_AMBIGUOUS_DEFAULT_LIMIT = 10_000
ATHYG_MERGE_GAIA_COLLISION_MAX = 0
ATHYG_MERGE_HIP_COLLISION_MAX = 3_000
ATHYG_MERGE_HD_COLLISION_MAX = 3_000
PLANET_CLASSIFIER_VERSION_DEFAULT = "planet_lifecycle_v1"
DUPLICATE_PAIR_MAX_ANG_ARCSEC_DEFAULT = 3.0
DUPLICATE_PAIR_MAX_DIST_DELTA_LY_DEFAULT = 1.0
DUPLICATE_PAIR_MAX_PM_DELTA_MASYR_DEFAULT = 20.0
DUPLICATE_PAIR_HIGH_MAX_ANG_ARCSEC_DEFAULT = 1.0
DUPLICATE_PAIR_HIGH_MAX_DIST_DELTA_LY_DEFAULT = 0.5
DUPLICATE_PAIR_HIGH_MAX_PM_DELTA_MASYR_DEFAULT = 10.0
DUPLICATE_HIGH_PAIR_MAX_DEFAULT = 0
MULTIPLICITY_GAIA_DUPLICATE_MAX_DEFAULT = 0
MULTIPLICITY_WDS_COMPONENT_DUPLICATE_MAX_DEFAULT = 0


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


def parse_nonnegative_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    text = raw.strip()
    try:
        value = int(text)
    except ValueError as exc:
        raise SystemExit(f"Invalid {name} value: {raw!r} (expected integer >= 0)") from exc
    if value < 0:
        raise SystemExit(f"Invalid {name} value: {raw!r} (must be >= 0)")
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


def format_count(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):,}"


def pct_value(part: int | float | None, whole: int | float | None) -> float:
    try:
        p = float(part or 0.0)
        w = float(whole or 0.0)
    except Exception:
        return 0.0
    if w <= 0.0:
        return 0.0
    return (p / w) * 100.0


def format_stage_totals(totals: dict[str, int | None]) -> str:
    order = [
        "stars",
        "systems",
        "planets",
        "aliases",
        "compact_objects",
        "superstellar_objects",
        "eclipsing_binaries",
    ]
    parts: list[str] = []
    for key in order:
        if key not in totals:
            continue
        parts.append(f"{key}={format_count(totals.get(key))}")
    return ", ".join(parts)


def log_stage_complete(
    stage_label: str,
    stage_started_monotonic: float,
    totals: dict[str, int | None] | None = None,
    *,
    extra: str | None = None,
) -> None:
    elapsed_s = time.monotonic() - stage_started_monotonic
    message = f"{stage_label} complete in {elapsed_s:.1f}s"
    if totals:
        message += f" | totals: {format_stage_totals(totals)}"
    if extra:
        message += f" | {extra}"
    log(message)


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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_mtime_utc(path: Path) -> str | None:
    try:
        ts = path.stat().st_mtime
    except OSError:
        return None
    return dt.datetime.fromtimestamp(ts, dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def resolve_served_current_core(state_dir: Path) -> tuple[str | None, Path | None]:
    served_current = state_dir / "served" / "current"
    if not served_current.exists():
        return (None, None)
    try:
        resolved = served_current.resolve()
    except Exception:
        return (None, None)
    core_path = resolved / "core.duckdb"
    if not core_path.exists():
        return (None, None)
    return (resolved.name, core_path)


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
    enable_msc = parse_bool_env("SPACEGATE_ENABLE_MSC", True)
    if not enable_msc:
        raise SystemExit(
            "MSC is mandatory for default science ingest (SPACEGATE_ENABLE_MSC=0 is not supported)."
        )
    enable_gaia_nss = os.getenv("SPACEGATE_ENABLE_GAIA_NSS", "1") != "0"
    enable_sbx = parse_bool_env("SPACEGATE_ENABLE_SBX", True)
    enable_wds_gaia_xmatch = parse_bool_env("SPACEGATE_ENABLE_WDS_GAIA_XMATCH", True)
    enable_gaia_classprob = parse_bool_env("SPACEGATE_ENABLE_GAIA_CLASSPROB", True)
    enable_gaia_ucd = parse_bool_env("SPACEGATE_ENABLE_GAIA_UCD", True)
    enable_compact_catalogs = parse_bool_env("SPACEGATE_ENABLE_COMPACT_OBJECT_CATALOGS", True)
    enable_superstellar_catalogs = parse_bool_env("SPACEGATE_ENABLE_SUPERSTELLAR_CATALOGS", True)
    enable_eclipsing_catalogs = parse_bool_env("SPACEGATE_ENABLE_ECLIPSING_CATALOGS", True)
    enable_kepler_eb = parse_bool_env("SPACEGATE_ENABLE_KEPLER_EB", False)
    enable_tess_eb = parse_bool_env("SPACEGATE_ENABLE_TESS_EB", True)
    enable_exoplanet_lifecycle_catalogs = parse_bool_env(
        "SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS", False
    )
    enable_sol_authority = parse_bool_env("SPACEGATE_ENABLE_SOL_AUTHORITY", True)
    enable_aliases = parse_bool_env("SPACEGATE_ENABLE_ALIASES", True)
    enable_athyg_alias_crosswalk = parse_bool_env("SPACEGATE_ENABLE_ATHYG_ALIAS_CROSSWALK", True)
    enable_athyg_supplement_merge = parse_bool_env("SPACEGATE_ENABLE_ATHYG_SUPPLEMENT_MERGE", False)
    gaia_backbone_min_parallax_mas = parse_positive_float_env(
        "SPACEGATE_GAIA_BACKBONE_MIN_PARALLAX_MAS",
        GAIA_BOUNDARY_MIN_PARALLAX_MAS_DEFAULT,
    )
    gaia_nss_min_parallax_mas = parse_positive_float_env(
        "SPACEGATE_GAIA_NSS_MIN_PARALLAX_MAS",
        GAIA_BOUNDARY_MIN_PARALLAX_MAS_DEFAULT,
    )
    gaia_classprob_min_parallax_mas = parse_positive_float_env(
        "SPACEGATE_GAIA_CLASSPROB_MIN_PARALLAX_MAS",
        GAIA_BOUNDARY_MIN_PARALLAX_MAS_DEFAULT,
    )
    athyg_merge_ambiguous_limit = parse_nonnegative_int_env(
        "SPACEGATE_ATHYG_MERGE_AMBIGUOUS_LIMIT",
        ATHYG_MERGE_AMBIGUOUS_DEFAULT_LIMIT,
    )
    athyg_merge_gaia_collision_max = parse_nonnegative_int_env(
        "SPACEGATE_ATHYG_MERGE_GAIA_COLLISION_MAX",
        ATHYG_MERGE_GAIA_COLLISION_MAX,
    )
    athyg_merge_hip_collision_max = parse_nonnegative_int_env(
        "SPACEGATE_ATHYG_MERGE_HIP_COLLISION_MAX",
        ATHYG_MERGE_HIP_COLLISION_MAX,
    )
    athyg_merge_hd_collision_max = parse_nonnegative_int_env(
        "SPACEGATE_ATHYG_MERGE_HD_COLLISION_MAX",
        ATHYG_MERGE_HD_COLLISION_MAX,
    )
    open_cluster_member_min_probability = parse_optional_nonnegative_float_env(
        "SPACEGATE_OPEN_CLUSTER_MEMBER_MIN_PROBABILITY"
    )
    if open_cluster_member_min_probability is None:
        open_cluster_member_min_probability = 0.7
    white_dwarf_catalog_pwd_threshold = parse_optional_nonnegative_float_env(
        "SPACEGATE_WHITE_DWARF_CATALOG_PWD_THRESHOLD"
    )
    if white_dwarf_catalog_pwd_threshold is None:
        white_dwarf_catalog_pwd_threshold = WHITE_DWARF_CATALOG_PWD_THRESHOLD_DEFAULT
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
    duplicate_pair_max_ang_arcsec = parse_positive_float_env(
        "SPACEGATE_DUPLICATE_PAIR_MAX_ANG_ARCSEC",
        DUPLICATE_PAIR_MAX_ANG_ARCSEC_DEFAULT,
    )
    duplicate_pair_max_dist_delta_ly = parse_positive_float_env(
        "SPACEGATE_DUPLICATE_PAIR_MAX_DIST_DELTA_LY",
        DUPLICATE_PAIR_MAX_DIST_DELTA_LY_DEFAULT,
    )
    duplicate_pair_max_pm_delta_mas_yr = parse_positive_float_env(
        "SPACEGATE_DUPLICATE_PAIR_MAX_PM_DELTA_MASYR",
        DUPLICATE_PAIR_MAX_PM_DELTA_MASYR_DEFAULT,
    )
    duplicate_pair_high_max_ang_arcsec = min(
        parse_positive_float_env(
            "SPACEGATE_DUPLICATE_PAIR_HIGH_MAX_ANG_ARCSEC",
            DUPLICATE_PAIR_HIGH_MAX_ANG_ARCSEC_DEFAULT,
        ),
        duplicate_pair_max_ang_arcsec,
    )
    duplicate_pair_high_max_dist_delta_ly = min(
        parse_positive_float_env(
            "SPACEGATE_DUPLICATE_PAIR_HIGH_MAX_DIST_DELTA_LY",
            DUPLICATE_PAIR_HIGH_MAX_DIST_DELTA_LY_DEFAULT,
        ),
        duplicate_pair_max_dist_delta_ly,
    )
    duplicate_pair_high_max_pm_delta_mas_yr = min(
        parse_positive_float_env(
            "SPACEGATE_DUPLICATE_PAIR_HIGH_MAX_PM_DELTA_MASYR",
            DUPLICATE_PAIR_HIGH_MAX_PM_DELTA_MASYR_DEFAULT,
        ),
        duplicate_pair_max_pm_delta_mas_yr,
    )
    duplicate_fail_on_high = parse_bool_env("SPACEGATE_DUPLICATE_FAIL_ON_HIGH", False)
    duplicate_high_pair_max = parse_nonnegative_int_env(
        "SPACEGATE_DUPLICATE_HIGH_PAIR_MAX",
        DUPLICATE_HIGH_PAIR_MAX_DEFAULT,
    )
    multiplicity_gaia_duplicate_max = parse_nonnegative_int_env(
        "SPACEGATE_MULTIPLICITY_GAIA_DUPLICATE_MAX",
        MULTIPLICITY_GAIA_DUPLICATE_MAX_DEFAULT,
    )
    multiplicity_wds_component_duplicate_max = parse_nonnegative_int_env(
        "SPACEGATE_MULTIPLICITY_WDS_COMPONENT_DUPLICATE_MAX",
        MULTIPLICITY_WDS_COMPONENT_DUPLICATE_MAX_DEFAULT,
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
    slice_profile_id = (os.getenv("SPACEGATE_SLICE_PROFILE_ID") or "").strip()
    slice_profile_version = (os.getenv("SPACEGATE_SLICE_PROFILE_VERSION") or "").strip()
    build_layer = (os.getenv("SPACEGATE_BUILD_LAYER") or "core").strip().lower()
    source_galaxy_build_id = (os.getenv("SPACEGATE_SOURCE_GALAXY_BUILD_ID") or "").strip()
    cooked_athyg = state_dir / "cooked" / "athyg" / "athyg.csv.gz"
    cooked_gaia_backbone = state_dir / "cooked" / "gaia_backbone" / "gaia_dr3_backbone.csv"
    cooked_nasa = state_dir / "cooked" / "nasa_exoplanet_archive" / "pscomppars_clean.csv"
    cooked_wds = state_dir / "cooked" / "wds" / "wds_summary.csv"
    cooked_msc = state_dir / "cooked" / "msc" / "msc_components.csv"
    cooked_orb6 = state_dir / "cooked" / "orb6" / "orb6_orbits.csv"
    cooked_gaia_nss_non_single = state_dir / "cooked" / "gaia_nss" / "gaia_dr3_non_single_star.csv"
    cooked_gaia_nss_two_body = state_dir / "cooked" / "gaia_nss" / "gaia_dr3_nss_two_body_orbit.csv"
    cooked_sbx = state_dir / "cooked" / "sbx" / "sbx_catalog.csv"
    cooked_wds_gaia_xmatch = state_dir / "cooked" / "wds_gaia_xmatch" / "wds_gaia_matches.csv"
    cooked_gaia_classprob = (
        state_dir / "cooked" / "gaia_classprob" / "gaia_dr3_astrophysical_classprob.csv"
    )
    cooked_gaia_ucd = state_dir / "cooked" / "gaia_ucd" / "gaia_ucd_memberships.csv"
    cooked_atnf = state_dir / "cooked" / "atnf" / "pulsars.csv"
    cooked_magnetar = state_dir / "cooked" / "magnetar" / "magnetars.csv"
    cooked_white_dwarf = state_dir / "cooked" / "white_dwarf" / "gaiaedr3_white_dwarf.csv"
    cooked_open_clusters = state_dir / "cooked" / "clusters" / "open_clusters.csv"
    cooked_open_cluster_members = state_dir / "cooked" / "clusters" / "open_cluster_members.csv"
    cooked_snr = state_dir / "cooked" / "snr" / "green_snr.csv"
    cooked_debcat = state_dir / "cooked" / "debcat" / "debcat_binaries.csv"
    cooked_kepler_eb = state_dir / "cooked" / "kepler_eb" / "kepler_eb_catalog.csv"
    cooked_tess_eb = state_dir / "cooked" / "tess_eb" / "tess_eb_catalog.csv"
    cooked_sol_authority = state_dir / "cooked" / "sol_authority" / "sol_system_objects.csv"
    cooked_exoplanet_lifecycle_status = state_dir / "cooked" / "exoplanet_lifecycle" / "status_rows.csv"
    cooked_exoplanet_lifecycle_aliases = state_dir / "cooked" / "exoplanet_lifecycle" / "alias_rows.csv"
    cooked_exoplanet_lifecycle_features = state_dir / "cooked" / "exoplanet_lifecycle" / "features_rows.csv"
    manifest_dir = state_dir / "reports" / "manifests"
    manifest_path = manifest_dir / "core_manifest.json"
    wds_manifest_path = manifest_dir / "wds_manifest.json"
    msc_manifest_path = manifest_dir / "msc_manifest.json"
    orb6_manifest_path = manifest_dir / "orb6_manifest.json"
    gaia_backbone_manifest_path = manifest_dir / "gaia_backbone_manifest.json"
    gaia_nss_manifest_path = manifest_dir / "gaia_nss_manifest.json"
    sbx_manifest_path = manifest_dir / "sbx_manifest.json"
    wds_gaia_xmatch_manifest_path = manifest_dir / "wds_gaia_xmatch_manifest.json"
    gaia_classprob_manifest_path = manifest_dir / "gaia_classprob_manifest.json"
    gaia_ucd_manifest_path = manifest_dir / "gaia_ucd_manifest.json"
    atnf_manifest_path = manifest_dir / "atnf_manifest.json"
    magnetar_manifest_path = manifest_dir / "magnetar_manifest.json"
    white_dwarf_manifest_path = manifest_dir / "white_dwarf_manifest.json"
    clusters_manifest_path = manifest_dir / "clusters_manifest.json"
    snr_manifest_path = manifest_dir / "snr_manifest.json"
    debcat_manifest_path = manifest_dir / "debcat_manifest.json"
    kepler_eb_manifest_path = manifest_dir / "kepler_eb_manifest.json"
    tess_eb_manifest_path = manifest_dir / "tess_eb_manifest.json"
    sol_authority_manifest_path = manifest_dir / "sol_authority_manifest.json"
    exoplanet_eu_manifest_path = manifest_dir / "exoplanet_eu_manifest.json"
    open_exoplanet_catalogue_manifest_path = (
        manifest_dir / "open_exoplanet_catalogue_manifest.json"
    )
    hwc_manifest_path = manifest_dir / "hwc_manifest.json"
    planet_classifier_version = (
        os.getenv("SPACEGATE_PLANET_CLASSIFIER_VERSION")
        or PLANET_CLASSIFIER_VERSION_DEFAULT
    ).strip()

    if not enable_gaia_backbone and not cooked_athyg.exists():
        raise SystemExit(f"Missing cooked AT-HYG: {cooked_athyg}")
    if enable_gaia_backbone and not cooked_gaia_backbone.exists():
        raise SystemExit(f"Missing cooked Gaia backbone: {cooked_gaia_backbone}")
    if enable_gaia_backbone and enable_athyg_supplement_merge and not cooked_athyg.exists():
        raise SystemExit(f"Missing cooked AT-HYG supplement source: {cooked_athyg}")
    if enable_aliases and enable_athyg_alias_crosswalk and not cooked_athyg.exists():
        raise SystemExit(
            f"Missing cooked AT-HYG alias crosswalk source: {cooked_athyg}"
        )
    if not cooked_nasa.exists():
        raise SystemExit(f"Missing cooked NASA: {cooked_nasa}")
    if not cooked_wds.exists():
        raise SystemExit(f"Missing cooked WDS: {cooked_wds}")
    if not cooked_msc.exists():
        raise SystemExit(f"Missing cooked MSC: {cooked_msc}")
    if not cooked_orb6.exists():
        raise SystemExit(f"Missing cooked ORB6: {cooked_orb6}")
    if enable_gaia_nss and not cooked_gaia_nss_non_single.exists():
        raise SystemExit(f"Missing cooked Gaia NSS non_single_star: {cooked_gaia_nss_non_single}")
    if enable_gaia_nss and not cooked_gaia_nss_two_body.exists():
        raise SystemExit(f"Missing cooked Gaia NSS two_body: {cooked_gaia_nss_two_body}")
    if enable_sbx and not cooked_sbx.exists():
        raise SystemExit(f"Missing cooked SBX catalog: {cooked_sbx}")
    if enable_wds_gaia_xmatch and not cooked_wds_gaia_xmatch.exists():
        raise SystemExit(f"Missing cooked WDS-Gaia XMatch: {cooked_wds_gaia_xmatch}")
    if enable_gaia_backbone and enable_gaia_classprob and not cooked_gaia_classprob.exists():
        raise SystemExit(f"Missing cooked Gaia classifier probabilities: {cooked_gaia_classprob}")
    if enable_gaia_ucd and not cooked_gaia_ucd.exists():
        raise SystemExit(f"Missing cooked Gaia UCD memberships: {cooked_gaia_ucd}")
    if enable_compact_catalogs and not cooked_atnf.exists():
        raise SystemExit(f"Missing cooked ATNF pulsars: {cooked_atnf}")
    if enable_compact_catalogs and not cooked_magnetar.exists():
        raise SystemExit(f"Missing cooked magnetars: {cooked_magnetar}")
    if enable_compact_catalogs and not cooked_white_dwarf.exists():
        raise SystemExit(f"Missing cooked white dwarf catalog: {cooked_white_dwarf}")
    if enable_superstellar_catalogs and not cooked_open_clusters.exists():
        raise SystemExit(f"Missing cooked open clusters: {cooked_open_clusters}")
    if enable_superstellar_catalogs and not cooked_open_cluster_members.exists():
        raise SystemExit(f"Missing cooked open-cluster members: {cooked_open_cluster_members}")
    if enable_superstellar_catalogs and not cooked_snr.exists():
        raise SystemExit(f"Missing cooked SNR catalog: {cooked_snr}")
    if enable_eclipsing_catalogs and not cooked_debcat.exists():
        raise SystemExit(f"Missing cooked DEBCat catalog: {cooked_debcat}")
    if enable_eclipsing_catalogs and enable_kepler_eb and not cooked_kepler_eb.exists():
        raise SystemExit(f"Missing cooked Kepler EB catalog: {cooked_kepler_eb}")
    if enable_eclipsing_catalogs and enable_tess_eb and not cooked_tess_eb.exists():
        raise SystemExit(f"Missing cooked TESS EB catalog: {cooked_tess_eb}")
    if enable_sol_authority and not cooked_sol_authority.exists():
        raise SystemExit(f"Missing cooked Sol authority catalog: {cooked_sol_authority}")
    if enable_exoplanet_lifecycle_catalogs and not cooked_exoplanet_lifecycle_status.exists():
        raise SystemExit(
            f"Missing cooked exoplanet lifecycle status rows: {cooked_exoplanet_lifecycle_status}"
        )
    if enable_exoplanet_lifecycle_catalogs and not cooked_exoplanet_lifecycle_aliases.exists():
        raise SystemExit(
            f"Missing cooked exoplanet lifecycle alias rows: {cooked_exoplanet_lifecycle_aliases}"
        )
    if enable_exoplanet_lifecycle_catalogs and not cooked_exoplanet_lifecycle_features.exists():
        raise SystemExit(
            f"Missing cooked exoplanet lifecycle feature rows: {cooked_exoplanet_lifecycle_features}"
        )

    log("Ingest core start")
    ingest_started_monotonic = time.monotonic()
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
    log(
        "Slice profile: "
        f"id={slice_profile_id or '(unset)'} "
        f"version={slice_profile_version or '(unset)'} "
        f"build_layer={build_layer} "
        f"source_galaxy_build_id={source_galaxy_build_id or '(unset)'}"
    )
    log(
        "Science catalogs: "
        f"gaia_classprob={'1' if (enable_gaia_backbone and enable_gaia_classprob) else '0'} "
        f"gaia_ucd={'1' if enable_gaia_ucd else '0'} "
        f"compact_catalogs={'1' if enable_compact_catalogs else '0'} "
        f"superstellar_catalogs={'1' if enable_superstellar_catalogs else '0'} "
        f"eclipsing_catalogs={'1' if enable_eclipsing_catalogs else '0'} "
        f"kepler_eb={'1' if (enable_eclipsing_catalogs and enable_kepler_eb) else '0'} "
        f"tess_eb={'1' if (enable_eclipsing_catalogs and enable_tess_eb) else '0'} "
        f"exoplanet_lifecycle_catalogs={'1' if enable_exoplanet_lifecycle_catalogs else '0'} "
        f"sol_authority={'1' if enable_sol_authority else '0'} "
        f"sbx={'1' if enable_sbx else '0'} "
        f"aliases={'1' if enable_aliases else '0'} "
        f"athyg_alias_crosswalk={'1' if (enable_aliases and enable_athyg_alias_crosswalk) else '0'} "
        f"athyg_supplement_merge={'1' if (enable_gaia_backbone and enable_athyg_supplement_merge) else '0'} "
        f"open_cluster_member_min_probability={open_cluster_member_min_probability} "
        f"white_dwarf_catalog_pwd_threshold={white_dwarf_catalog_pwd_threshold}"
    )
    log(
        "Identifier stewardship: "
        f"ambiguous_limit={athyg_merge_ambiguous_limit} "
        f"gaia_collision_max={athyg_merge_gaia_collision_max} "
        f"hip_collision_max={athyg_merge_hip_collision_max} "
        f"hd_collision_max={athyg_merge_hd_collision_max}"
    )
    log(
        "Duplicate trap: "
        f"pair_max_ang_arcsec={duplicate_pair_max_ang_arcsec} "
        f"pair_max_dist_delta_ly={duplicate_pair_max_dist_delta_ly} "
        f"pair_max_pm_delta_mas_yr={duplicate_pair_max_pm_delta_mas_yr} "
        f"high_max_ang_arcsec={duplicate_pair_high_max_ang_arcsec} "
        f"high_max_dist_delta_ly={duplicate_pair_high_max_dist_delta_ly} "
        f"high_max_pm_delta_mas_yr={duplicate_pair_high_max_pm_delta_mas_yr} "
        f"fail_on_high={'1' if duplicate_fail_on_high else '0'} "
        f"high_pair_max={duplicate_high_pair_max} "
        f"multiplicity_gaia_duplicate_max={multiplicity_gaia_duplicate_max} "
        f"multiplicity_wds_component_duplicate_max={multiplicity_wds_component_duplicate_max}"
    )
    manifest: dict[str, dict] = {}
    manifest_paths = [manifest_path, wds_manifest_path, orb6_manifest_path]
    if enable_gaia_backbone:
        manifest_paths.append(gaia_backbone_manifest_path)
    manifest_paths.append(msc_manifest_path)
    if enable_gaia_nss:
        manifest_paths.append(gaia_nss_manifest_path)
    if enable_sbx:
        manifest_paths.append(sbx_manifest_path)
    if enable_wds_gaia_xmatch:
        manifest_paths.append(wds_gaia_xmatch_manifest_path)
    if enable_gaia_backbone and enable_gaia_classprob:
        manifest_paths.append(gaia_classprob_manifest_path)
    if enable_gaia_ucd:
        manifest_paths.append(gaia_ucd_manifest_path)
    if enable_compact_catalogs:
        manifest_paths.extend([atnf_manifest_path, magnetar_manifest_path, white_dwarf_manifest_path])
    if enable_superstellar_catalogs:
        manifest_paths.extend([clusters_manifest_path, snr_manifest_path])
    if enable_eclipsing_catalogs:
        manifest_paths.append(debcat_manifest_path)
        if enable_kepler_eb:
            manifest_paths.append(kepler_eb_manifest_path)
        if enable_tess_eb:
            manifest_paths.append(tess_eb_manifest_path)
    if enable_sol_authority:
        manifest_paths.append(sol_authority_manifest_path)
    if enable_exoplanet_lifecycle_catalogs:
        manifest_paths.extend(
            [
                exoplanet_eu_manifest_path,
                open_exoplanet_catalogue_manifest_path,
                hwc_manifest_path,
            ]
        )
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
    arm_db_path = tmp_out_dir / "arm.duckdb"

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
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{str(tmp_work_dir).replace("'", "''")}'")

    ingested_at = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    transform_version = get_git_sha(root)
    astrometry_boundary_strategy = (
        "gaia_parallax_floor" if enable_gaia_backbone else "athyg_legacy_backbone"
    )
    astrometry_boundary_min_parallax = (
        str(gaia_backbone_min_parallax_mas) if enable_gaia_backbone else ""
    )
    astrometry_boundary_distance_ly_approx = (
        str(round((1000.0 / gaia_backbone_min_parallax_mas) * PC_TO_LY, 6))
        if enable_gaia_backbone
        else ""
    )

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
          ('astrometry_boundary_strategy', {sql_literal(astrometry_boundary_strategy)}),
          ('astrometry_boundary_min_parallax_mas', {sql_literal(astrometry_boundary_min_parallax)}),
          ('astrometry_boundary_distance_ly_approx', {sql_literal(astrometry_boundary_distance_ly_approx)}),
          ('astrometry_quality_policy_source', {sql_literal('slice_policy_and_gaia_fields')}),
          ('astrometry_quality_min_parallax_over_error', {sql_literal(format_float_or_empty(slice_min_parallax_over_error))}),
          ('astrometry_quality_max_parallax_error_mas', {sql_literal(format_float_or_empty(slice_max_parallax_error_mas))}),
          ('astrometry_quality_max_ruwe', {sql_literal(format_float_or_empty(slice_max_ruwe))}),
          ('gaia_nss_min_parallax_mas', {sql_literal(str(gaia_nss_min_parallax_mas))}),
          ('gaia_classprob_min_parallax_mas', {sql_literal(str(gaia_classprob_min_parallax_mas))}),
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
          ('slice_profile_id', {sql_literal(slice_profile_id)}),
          ('slice_profile_version', {sql_literal(slice_profile_version)}),
          ('build_layer', {sql_literal(build_layer)}),
          ('source_galaxy_build_id', {sql_literal(source_galaxy_build_id)}),
          ('wds_gaia_match_max_arcsec', {sql_literal(str(wds_gaia_match_max_arcsec))}),
          ('wds_gaia_gate_max_dist_spread_ly', {sql_literal(str(wds_gaia_gate_max_dist_spread_ly))}),
          ('wds_gaia_gate_max_pm_delta_mas_yr', {sql_literal(str(wds_gaia_gate_max_pm_delta_mas_yr))}),
          ('gaia_classprob_enabled', {sql_literal("1" if (enable_gaia_backbone and enable_gaia_classprob) else "0")}),
          ('gaia_ucd_enabled', {sql_literal("1" if enable_gaia_ucd else "0")}),
          ('sbx_enabled', {sql_literal("1" if enable_sbx else "0")}),
          ('compact_catalogs_enabled', {sql_literal("1" if enable_compact_catalogs else "0")}),
          ('superstellar_catalogs_enabled', {sql_literal("1" if enable_superstellar_catalogs else "0")}),
          ('eclipsing_catalogs_enabled', {sql_literal("1" if enable_eclipsing_catalogs else "0")}),
          ('kepler_eb_enabled', {sql_literal("1" if (enable_eclipsing_catalogs and enable_kepler_eb) else "0")}),
          ('exoplanet_lifecycle_catalogs_enabled', {sql_literal("1" if enable_exoplanet_lifecycle_catalogs else "0")}),
          ('planet_classifier_version', {sql_literal(planet_classifier_version)}),
          ('aliases_enabled', {sql_literal("1" if enable_aliases else "0")}),
          ('athyg_alias_crosswalk_enabled', {sql_literal("1" if (enable_aliases and enable_athyg_alias_crosswalk) else "0")}),
          ('athyg_supplement_merge_enabled', {sql_literal("1" if (enable_gaia_backbone and enable_athyg_supplement_merge) else "0")}),
          ('identifier_ambiguous_limit', {sql_literal(str(athyg_merge_ambiguous_limit))}),
          ('identifier_gaia_collision_max', {sql_literal(str(athyg_merge_gaia_collision_max))}),
          ('identifier_hip_collision_max', {sql_literal(str(athyg_merge_hip_collision_max))}),
          ('identifier_hd_collision_max', {sql_literal(str(athyg_merge_hd_collision_max))}),
          ('open_cluster_member_min_probability', {sql_literal(str(open_cluster_member_min_probability))}),
          ('white_dwarf_prob_threshold', {sql_literal(str(WHITE_DWARF_PROB_THRESHOLD))}),
          ('white_dwarf_catalog_pwd_threshold', {sql_literal(str(white_dwarf_catalog_pwd_threshold))})
        """
    )

    log("Loading manifest entries")
    if not enable_gaia_backbone:
        athyg_p1 = require_manifest_entry(manifest, "athyg_v33-1", "AT-HYG part 1")
        athyg_p2 = require_manifest_entry(manifest, "athyg_v33-2", "AT-HYG part 2")
    elif enable_athyg_supplement_merge:
        athyg_p1 = manifest.get("athyg_v33-1")
        athyg_p2 = manifest.get("athyg_v33-2")
        if not athyg_p1 or not athyg_p2:
            log(
                "AT-HYG supplement manifest entries missing (athyg_v33-1/2); "
                "continuing with cooked data and null retrieval metadata."
            )
    else:
        athyg_p1 = None
        athyg_p2 = None
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
    msc_manifest = require_manifest_entry(manifest, "newmsc_20240101", "MSC")
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
    sbx_systems_manifest = (
        require_manifest_entry(manifest, "sbx_systems", "SBX systems")
        if enable_sbx
        else None
    )
    sbx_alias_manifest = (
        require_manifest_entry(manifest, "sbx_alias", "SBX aliases")
        if enable_sbx
        else None
    )
    sbx_config_manifest = (
        require_manifest_entry(manifest, "sbx_configurations", "SBX configurations")
        if enable_sbx
        else None
    )
    sbx_orbits_manifest = (
        require_manifest_entry(manifest, "sbx_orbits", "SBX orbits")
        if enable_sbx
        else None
    )
    wds_gaia_xmatch_manifest = (
        require_manifest_entry(manifest, "wds_gaia_xmatch_best", "WDS Gaia XMatch best")
        if enable_wds_gaia_xmatch
        else None
    )
    gaia_classprob_manifest = (
        require_manifest_entry(
            manifest,
            "gaia_dr3_astrophysical_classprob",
            "Gaia DR3 astrophysical classifier probabilities",
        )
        if enable_gaia_backbone and enable_gaia_classprob
        else None
    )
    gaia_ucd_manifest = (
        require_manifest_entry(manifest, "table4", "Gaia UCD catalog")
        if enable_gaia_ucd
        else None
    )
    atnf_manifest = (
        require_manifest_entry(manifest, "psrcat_pkg", "ATNF pulsar catalog")
        if enable_compact_catalogs
        else None
    )
    magnetar_manifest = (
        require_manifest_entry(manifest, "TabO1", "McGill magnetar catalog")
        if enable_compact_catalogs
        else None
    )
    white_dwarf_manifest = (
        require_manifest_entry(manifest, "gaiaedr3_wd_main", "Gaia EDR3 white dwarf catalog")
        if enable_compact_catalogs
        else None
    )
    clusters_table1_manifest = (
        require_manifest_entry(manifest, "cantat_gaudin_2020_table1", "Open cluster summary catalog")
        if enable_superstellar_catalogs
        else None
    )
    clusters_members_manifest = (
        require_manifest_entry(
            manifest, "cantat_gaudin_2020_members", "Open cluster membership catalog"
        )
        if enable_superstellar_catalogs
        else None
    )
    snr_manifest = (
        require_manifest_entry(manifest, "snrs_data_html", "Galactic SNR catalog")
        if enable_superstellar_catalogs
        else None
    )
    debcat_manifest = (
        require_manifest_entry(manifest, "debs_dat", "DEBCat detached eclipsing binaries")
        if enable_eclipsing_catalogs
        else None
    )
    kepler_eb_manifest = (
        require_manifest_entry(manifest, "kepler_eb_catalog", "Kepler Eclipsing Binary Catalog")
        if enable_eclipsing_catalogs and enable_kepler_eb
        else None
    )
    tess_eb_manifest = (
        require_manifest_entry(manifest, "tess_eb_catalog", "TESS Eclipsing Binary Catalog")
        if enable_eclipsing_catalogs and enable_tess_eb
        else None
    )
    sol_authority_manifest = (
        require_manifest_entry(manifest, "sol_system_objects", "Sol authority objects")
        if enable_sol_authority
        else None
    )
    exoplanet_eu_manifest = (
        require_manifest_entry(manifest, "catalog_csv", "Exoplanet.eu catalog export")
        if enable_exoplanet_lifecycle_catalogs
        else None
    )
    open_exoplanet_catalogue_manifest = (
        require_manifest_entry(manifest, "catalog_tarball", "Open Exoplanet Catalogue tarball")
        if enable_exoplanet_lifecycle_catalogs
        else None
    )
    hwc_manifest = (
        require_manifest_entry(manifest, "hwc_full_csv", "Habitable Worlds Catalog")
        if enable_exoplanet_lifecycle_catalogs
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
    elif enable_gaia_backbone and enable_athyg_supplement_merge and cooked_athyg.exists():
        athyg_download_url = ATHYG_ALIAS_URL
        athyg_checksum = sha256_file(cooked_athyg)
        athyg_retrieved = file_mtime_utc(cooked_athyg)
        athyg_has_retrieval = True
        log("AT-HYG retrieval metadata synthesized from local cooked artifact checksum")

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
    sol_authority_url = (
        sol_authority_manifest.get("url", SOL_AUTHORITY_URL) if sol_authority_manifest else SOL_AUTHORITY_URL
    )
    sol_authority_download_url = (
        sol_authority_manifest.get("url", SOL_AUTHORITY_URL) if sol_authority_manifest else SOL_AUTHORITY_URL
    )
    sol_authority_version = (
        sol_authority_manifest.get("source_version", SOL_AUTHORITY_VERSION)
        if sol_authority_manifest
        else SOL_AUTHORITY_VERSION
    )
    sol_authority_sha = sol_authority_manifest.get("sha256") if sol_authority_manifest else None
    sol_authority_retrieved = (
        sol_authority_manifest.get("retrieved_at") if sol_authority_manifest else None
    )
    sol_authority_has_retrieval = has_retrieval(sol_authority_manifest or {})
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
    sbx_sha_parts = [
        s
        for s in [
            sbx_systems_manifest.get("sha256") if sbx_systems_manifest else None,
            sbx_alias_manifest.get("sha256") if sbx_alias_manifest else None,
            sbx_config_manifest.get("sha256") if sbx_config_manifest else None,
            sbx_orbits_manifest.get("sha256") if sbx_orbits_manifest else None,
        ]
        if s
    ]
    sbx_sha = ",".join(sbx_sha_parts) if sbx_sha_parts else None
    sbx_retrieved = (
        max(
            [
                t
                for t in [
                    sbx_systems_manifest.get("retrieved_at") if sbx_systems_manifest else None,
                    sbx_alias_manifest.get("retrieved_at") if sbx_alias_manifest else None,
                    sbx_config_manifest.get("retrieved_at") if sbx_config_manifest else None,
                    sbx_orbits_manifest.get("retrieved_at") if sbx_orbits_manifest else None,
                ]
                if t
            ],
            default=None,
        )
        if enable_sbx
        else None
    )
    wds_gaia_xmatch_sha = (
        wds_gaia_xmatch_manifest.get("sha256") if wds_gaia_xmatch_manifest else None
    )
    wds_gaia_xmatch_retrieved = (
        wds_gaia_xmatch_manifest.get("retrieved_at") if wds_gaia_xmatch_manifest else None
    )
    gaia_classprob_sha = (
        gaia_classprob_manifest.get("sha256") if gaia_classprob_manifest else None
    )
    gaia_classprob_retrieved = (
        gaia_classprob_manifest.get("retrieved_at") if gaia_classprob_manifest else None
    )
    atnf_sha = atnf_manifest.get("sha256") if atnf_manifest else None
    atnf_retrieved = atnf_manifest.get("retrieved_at") if atnf_manifest else None
    magnetar_sha = magnetar_manifest.get("sha256") if magnetar_manifest else None
    magnetar_retrieved = magnetar_manifest.get("retrieved_at") if magnetar_manifest else None
    white_dwarf_sha = white_dwarf_manifest.get("sha256") if white_dwarf_manifest else None
    white_dwarf_retrieved = (
        white_dwarf_manifest.get("retrieved_at") if white_dwarf_manifest else None
    )
    clusters_sha = (
        ",".join(
            [s for s in [clusters_table1_manifest.get("sha256"), clusters_members_manifest.get("sha256")] if s]
        )
        if clusters_table1_manifest and clusters_members_manifest
        else None
    )
    clusters_retrieved = (
        max(
            [
                t
                for t in [
                    clusters_table1_manifest.get("retrieved_at") if clusters_table1_manifest else None,
                    clusters_members_manifest.get("retrieved_at") if clusters_members_manifest else None,
                ]
                if t
            ],
            default=None,
        )
        if clusters_table1_manifest or clusters_members_manifest
        else None
    )
    snr_sha = snr_manifest.get("sha256") if snr_manifest else None
    snr_retrieved = snr_manifest.get("retrieved_at") if snr_manifest else None
    debcat_sha = debcat_manifest.get("sha256") if debcat_manifest else None
    debcat_retrieved = debcat_manifest.get("retrieved_at") if debcat_manifest else None
    kepler_eb_sha = kepler_eb_manifest.get("sha256") if kepler_eb_manifest else None
    kepler_eb_retrieved = (
        kepler_eb_manifest.get("retrieved_at") if kepler_eb_manifest else None
    )
    tess_eb_sha = tess_eb_manifest.get("sha256") if tess_eb_manifest else None
    tess_eb_retrieved = tess_eb_manifest.get("retrieved_at") if tess_eb_manifest else None

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
                nullif(ruwe, '')::double as ruwe,
                nullif(teff_gspphot, '')::double as teff_gspphot
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
              case when teff_gspphot is not null then teff_gspphot::varchar else null end as teff_k,
              'gaia_g_bp_rp' as mag_src,
              case when radial_velocity_kms is not null then radial_velocity_kms::varchar else null end as rv,
              'gaia_dr3' as rv_src,
              case when pm_ra_mas_yr is not null then pm_ra_mas_yr::varchar else null end as pm_ra,
              case when pm_dec_mas_yr is not null then pm_dec_mas_yr::varchar else null end as pm_dec,
              'gaia_dr3' as pm_src,
              null::varchar as vx,
              null::varchar as vy,
              null::varchar as vz,
              case
                when teff_gspphot is not null and teff_gspphot >= 30000 then 'O'
                when teff_gspphot is not null and teff_gspphot >= 10000 then 'B'
                when teff_gspphot is not null and teff_gspphot >= 7500 then 'A'
                when teff_gspphot is not null and teff_gspphot >= 6000 then 'F'
                when teff_gspphot is not null and teff_gspphot >= 5200 then 'G'
                when teff_gspphot is not null and teff_gspphot >= 3700 then 'K'
                when teff_gspphot is not null and teff_gspphot >= 2400 then 'M'
                when teff_gspphot is not null and teff_gspphot >= 1300 then 'L'
                when teff_gspphot is not null and teff_gspphot >= 700 then 'T'
                when teff_gspphot is not null then 'Y'
                when bp_rp is not null and bp_rp < -0.20 then 'O'
                when bp_rp is not null and bp_rp < 0.00 then 'B'
                when bp_rp is not null and bp_rp < 0.30 then 'A'
                when bp_rp is not null and bp_rp < 0.58 then 'F'
                when bp_rp is not null and bp_rp < 0.81 then 'G'
                when bp_rp is not null and bp_rp < 1.40 then 'K'
                when bp_rp is not null and bp_rp < 2.40 then 'M'
                when bp_rp is not null then 'L'
                else null
              end as spect,
              'gaia_inferred_teff_bp_rp' as spect_src
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
              null::varchar as ruwe,
              null::varchar as teff_k
            from athyg_raw_source
            """
        )

    athyg_crosswalk_source_needed = (
        (enable_aliases and enable_athyg_alias_crosswalk)
        or (enable_gaia_backbone and enable_athyg_supplement_merge)
    )
    if athyg_crosswalk_source_needed:
        athyg_alias_path = str(cooked_athyg).replace("'", "''")
        log("Loading AT-HYG alias crosswalk source")
        con.execute(
            f"""
            create or replace temp view athyg_alias_raw as
            select * from read_csv_auto('{athyg_alias_path}',
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
    else:
        con.execute(
            """
            create or replace temp view athyg_alias_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              id, gaia, hip, hd, hr, gl, tyc, hyg, proper, bayer, flam, con, spect
            )
            where false
            """
        )

    wds_path = str(cooked_wds).replace("'", "''")
    msc_path = str(cooked_msc).replace("'", "''")
    orb6_path = str(cooked_orb6).replace("'", "''")
    gaia_nss_non_single_path = str(cooked_gaia_nss_non_single).replace("'", "''")
    gaia_nss_two_body_path = str(cooked_gaia_nss_two_body).replace("'", "''")
    sbx_path = str(cooked_sbx).replace("'", "''")
    wds_gaia_xmatch_path = str(cooked_wds_gaia_xmatch).replace("'", "''")
    gaia_classprob_path = str(cooked_gaia_classprob).replace("'", "''")
    gaia_ucd_path = str(cooked_gaia_ucd).replace("'", "''")
    atnf_path = str(cooked_atnf).replace("'", "''")
    magnetar_path = str(cooked_magnetar).replace("'", "''")
    white_dwarf_path = str(cooked_white_dwarf).replace("'", "''")
    open_clusters_path = str(cooked_open_clusters).replace("'", "''")
    open_cluster_members_path = str(cooked_open_cluster_members).replace("'", "''")
    snr_path = str(cooked_snr).replace("'", "''")
    debcat_path = str(cooked_debcat).replace("'", "''")
    kepler_eb_path = str(cooked_kepler_eb).replace("'", "''")
    tess_eb_path = str(cooked_tess_eb).replace("'", "''")

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
    if enable_sbx:
        con.execute(
            f"""
            create or replace temp view sbx_raw as
            select * from read_csv_auto('{sbx_path}',
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
            create or replace temp view sbx_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              sn, gaia_id, hip_id, hd_id, wds_id, ads_id, ra_deg, dec_deg, parallax_mas, pm_ra_mas_yr,
              pm_dec_mas_yr, mag_primary, position_epoch, position_source, spectral_type_raw, family, parent,
              child1, child2, in_triple, orbit_count
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
    if enable_gaia_backbone and enable_gaia_classprob:
        con.execute(
            f"""
            create or replace temp view gaia_classprob_raw as
            select * from read_csv_auto('{gaia_classprob_path}',
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
            create or replace temp view gaia_classprob_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              source_id,
              classprob_dsc_combmod_whitedwarf,
              classprob_dsc_specmod_whitedwarf,
              classprob_dsc_combmod_star,
              classprob_dsc_specmod_star,
              classprob_dsc_combmod_binarystar,
              classprob_dsc_specmod_binarystar,
              classprob_dsc_combmod_galaxy,
              classprob_dsc_specmod_galaxy,
              classprob_dsc_combmod_quasar,
              classprob_dsc_specmod_quasar
            )
            where false
            """
        )
    if enable_gaia_ucd:
        con.execute(
            f"""
            create or replace temp view gaia_ucd_raw as
            select * from read_csv_auto('{gaia_ucd_path}',
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
            create or replace temp view gaia_ucd_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(source_id, hmac_cluster_id, banyan_cluster, banyan_probability)
            where false
            """
        )
    if enable_compact_catalogs:
        con.execute(
            f"""
            create or replace temp view atnf_raw as
            select * from read_csv_auto('{atnf_path}',
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
            create or replace temp view magnetar_raw as
            select * from read_csv_auto('{magnetar_path}',
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
            create or replace temp view white_dwarf_raw as
            select * from read_csv_auto('{white_dwarf_path}',
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
            create or replace temp view atnf_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              psrj, psrb, ra_deg, dec_deg, parallax_mas, distance_pc, type_raw, assoc_raw,
              period_s, period_derivative, spin_frequency_hz, spin_frequency_derivative_hz_s, object_type
            )
            where false
            """
        )
        con.execute(
            """
            create or replace temp view magnetar_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(
              name, ra_deg, dec_deg, distance_pc, period_s, period_dot, assoc_raw, activity_raw, bands_raw
            )
            where false
            """
        )
        con.execute(
            """
            create or replace temp view white_dwarf_raw as
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
                  cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              source_id, wdj_name, designation, ra_deg, dec_deg, parallax_mas, parallax_error_mas, parallax_over_error,
              pwd, ruwe, fit_model, teff_best_k, logg_best_cgs, mass_best_msun, teff_h_k, logg_h_cgs, mass_h_msun,
              chisq_h, teff_he_k, logg_he_cgs, mass_he_msun, chisq_he, phot_g_mag, phot_bp_mag, phot_rp_mag, bp_rp
            )
            where false
            """
        )
    if enable_superstellar_catalogs:
        con.execute(
            f"""
            create or replace temp view open_clusters_raw as
            select * from read_csv_auto('{open_clusters_path}',
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
            create or replace temp view open_cluster_members_raw as
            select * from read_csv_auto('{open_cluster_members_path}',
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
            create or replace temp view snr_raw as
            select * from read_csv_auto('{snr_path}',
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
            create or replace temp view open_clusters_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              cluster_name, ra_deg, dec_deg, glon_deg, glat_deg, radius_r50_deg, member_count_prob_gt_0_7,
              pm_ra_mas_yr, pm_ra_sigma_mas_yr, pm_dec_mas_yr, pm_dec_sigma_mas_yr, parallax_mas,
              parallax_sigma_mas, flag, age_log_yr, av_mag, distance_modulus_mag, distance_pc, x_gal_pc,
              y_gal_pc, z_gal_pc, rgc_pc
            )
            where false
            """
        )
        con.execute(
            """
            create or replace temp view open_cluster_members_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar)
                )
            ) as t(gaia_dr2_source_id, cluster_name, membership_probability, ra_deg, dec_deg)
            where false
            """
        )
        con.execute(
            """
            create or replace temp view snr_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              galactic_name, glon_deg, glat_deg, ra_deg, dec_deg, size_major_arcmin, size_minor_arcmin,
              morphology_type, flux_1ghz_jy_raw, spectral_index_raw, other_names
            )
            where false
            """
        )
    if enable_eclipsing_catalogs:
        con.execute(
            f"""
            create or replace temp view debcat_raw as
            select * from read_csv_auto('{debcat_path}',
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
        if enable_kepler_eb:
            con.execute(
                f"""
                create or replace temp view kepler_eb_raw as
                select * from read_csv_auto('{kepler_eb_path}',
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
                create or replace temp view kepler_eb_raw as
                select *
                from (
                  values
                    (
                      cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                      cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                      cast(null as varchar), cast(null as varchar), cast(null as varchar)
                    )
                ) as t(
                  kic_id, period_days, period_error_days, bjd0, bjd0_error, morphology, glon_deg, glat_deg,
                  kmag, teff_k, has_short_cadence
                )
                where false
                """
            )
        if enable_tess_eb:
            con.execute(
                f"""
                create or replace temp view tess_eb_raw as
                select * from read_csv_auto('{tess_eb_path}',
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
                create or replace temp view tess_eb_raw as
                select *
                from (
                  values
                    (
                      cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                      cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                      cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                      cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                      cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                    )
                ) as t(
                  tic_id, in_catalog, sectors, ra_deg, dec_deg, glon_deg, glat_deg, pm_ra_mas_yr, pm_dec_mas_yr,
                  tmag, teff_k, logg_cgs, metallicity_dex, bjd0, bjd0_error, period_days, period_error_days,
                  morphology, source, flags
                )
                where false
                """
            )
    else:
        con.execute(
            """
            create or replace temp view debcat_raw as
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
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              system_name, spectral_type_primary, spectral_type_secondary, period_days, vmag, b_minus_v,
              mass_primary_msun, mass_primary_err_msun, mass_secondary_msun, mass_secondary_err_msun,
              radius_primary_rsun, radius_primary_err_rsun, radius_secondary_rsun, radius_secondary_err_rsun,
              logg_primary_cgs, logg_primary_err_cgs, logg_secondary_cgs, logg_secondary_err_cgs,
              teff_primary_k, teff_primary_err_k, teff_secondary_k, teff_secondary_err_k, lum_primary_lsun,
              lum_primary_err_lsun, lum_secondary_lsun, lum_secondary_err_lsun, metallicity_dex,
              metallicity_err_dex
            )
            where false
            """
        )
        con.execute(
            """
            create or replace temp view kepler_eb_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              kic_id, period_days, period_error_days, bjd0, bjd0_error, morphology, glon_deg, glat_deg,
              kmag, teff_k, has_short_cadence
            )
            where false
            """
        )
        con.execute(
            """
            create or replace temp view tess_eb_raw as
            select *
            from (
              values
                (
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              tic_id, in_catalog, sectors, ra_deg, dec_deg, glon_deg, glat_deg, pm_ra_mas_yr, pm_dec_mas_yr,
              tmag, teff_k, logg_cgs, metallicity_dex, bjd0, bjd0_error, period_days, period_error_days,
              morphology, source, flags
            )
            where false
            """
        )
    con.execute(
        """
        create or replace temp view gaia_classprob as
        select
          nullif(source_id, '')::bigint as gaia_id,
          nullif(classprob_dsc_combmod_whitedwarf, '')::double as classprob_dsc_combmod_whitedwarf,
          nullif(classprob_dsc_specmod_whitedwarf, '')::double as classprob_dsc_specmod_whitedwarf,
          nullif(classprob_dsc_combmod_star, '')::double as classprob_dsc_combmod_star,
          nullif(classprob_dsc_specmod_star, '')::double as classprob_dsc_specmod_star,
          nullif(classprob_dsc_combmod_binarystar, '')::double as classprob_dsc_combmod_binarystar,
          nullif(classprob_dsc_specmod_binarystar, '')::double as classprob_dsc_specmod_binarystar,
          nullif(classprob_dsc_combmod_galaxy, '')::double as classprob_dsc_combmod_galaxy,
          nullif(classprob_dsc_specmod_galaxy, '')::double as classprob_dsc_specmod_galaxy,
          nullif(classprob_dsc_combmod_quasar, '')::double as classprob_dsc_combmod_quasar,
          nullif(classprob_dsc_specmod_quasar, '')::double as classprob_dsc_specmod_quasar
        from gaia_classprob_raw
        where nullif(source_id, '') is not null
        """
    )
    con.execute(
        """
        create or replace temp view gaia_ucd_catalog as
        select
          nullif(source_id, '')::bigint as gaia_id,
          nullif(hmac_cluster_id, '')::integer as hmac_cluster_id,
          nullif(banyan_cluster, '') as banyan_cluster,
          nullif(banyan_probability, '')::double as banyan_probability
        from gaia_ucd_raw
        where nullif(source_id, '') is not null
        """
    )
    con.execute(
        """
        create or replace temp view white_dwarf_catalog as
        select
          nullif(source_id, '')::bigint as gaia_id,
          nullif(wdj_name, '') as wdj_name,
          nullif(designation, '') as designation,
          nullif(ra_deg, '')::double as ra_deg,
          nullif(dec_deg, '')::double as dec_deg,
          nullif(parallax_mas, '')::double as parallax_mas,
          nullif(parallax_error_mas, '')::double as parallax_error_mas,
          nullif(parallax_over_error, '')::double as parallax_over_error,
          nullif(pwd, '')::double as pwd,
          nullif(ruwe, '')::double as ruwe,
          nullif(fit_model, '') as fit_model,
          nullif(teff_best_k, '')::double as teff_best_k,
          nullif(logg_best_cgs, '')::double as logg_best_cgs,
          nullif(mass_best_msun, '')::double as mass_best_msun,
          nullif(teff_h_k, '')::double as teff_h_k,
          nullif(logg_h_cgs, '')::double as logg_h_cgs,
          nullif(mass_h_msun, '')::double as mass_h_msun,
          nullif(chisq_h, '')::double as chisq_h,
          nullif(teff_he_k, '')::double as teff_he_k,
          nullif(logg_he_cgs, '')::double as logg_he_cgs,
          nullif(mass_he_msun, '')::double as mass_he_msun,
          nullif(chisq_he, '')::double as chisq_he,
          nullif(phot_g_mag, '')::double as phot_g_mag,
          nullif(phot_bp_mag, '')::double as phot_bp_mag,
          nullif(phot_rp_mag, '')::double as phot_rp_mag,
          nullif(bp_rp, '')::double as bp_rp
        from white_dwarf_raw
        where nullif(source_id, '') is not null
        """
    )

    stage_totals: dict[str, int | None] = {
        "stars": None,
        "systems": None,
        "planets": None,
        "aliases": None,
        "compact_objects": None,
        "superstellar_objects": None,
        "eclipsing_binaries": None,
    }

    # Build stars table
    stars_stage_started = time.monotonic()
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
            nullif(teff_k,'')::double as teff_k,
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
        create or replace temp view msc_components_pre_dedupe as
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
            nullif(parallax_ref, '') as parallax_ref,
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
        create or replace temp view msc_components_ranked as
        with scored as (
          select
            *,
            case
              when wds_id is not null and component_norm <> '' then 'wds_component:' || wds_id || ':' || component_norm
              else 'row:' || msc_row_num::varchar
            end as msc_dedupe_key,
            (
              case when coalesce(abs(pm_ra_mas_yr), 0.0) + coalesce(abs(pm_dec_mas_yr), 0.0) > 0.001 then 32 else 0 end +
              case
                when upper(coalesce(parallax_ref, '')) like 'DR3%' then 16
                when upper(coalesce(parallax_ref, '')) like 'DR2%' then 12
                when coalesce(parallax_ref, '') <> '' then 8
                else 0
              end +
              case when hip_id is not null then 4 else 0 end +
              case when hd_id is not null then 2 else 0 end +
              case when coalesce(other_identifiers, '') <> '' then 1 else 0 end
            ) as msc_quality_score
          from msc_components_pre_dedupe
        )
        select
          *,
          count(*) over (partition by msc_dedupe_key) as msc_dedupe_group_size,
          row_number() over (
            partition by msc_dedupe_key
            order by
              msc_quality_score desc,
              coalesce(abs(pm_ra_mas_yr), 0.0) + coalesce(abs(pm_dec_mas_yr), 0.0) desc,
              coalesce(parallax_mas, -1.0) desc,
              coalesce(ra_deg, 0.0) asc,
              coalesce(dec_deg, 0.0) asc,
              msc_row_num asc
          ) as msc_dedupe_rank
        from scored
        """
    )
    con.execute(
        """
        create or replace temp view msc_components as
        select *
        from msc_components_ranked
        where msc_dedupe_rank = 1
        """
    )
    msc_component_raw_count = con.execute(
        "select count(*) from msc_components_pre_dedupe"
    ).fetchone()[0]
    msc_component_retained_count = con.execute("select count(*) from msc_components").fetchone()[0]
    msc_component_dedup_dropped_count = max(
        msc_component_raw_count - msc_component_retained_count, 0
    )
    msc_component_dedup_group_count = con.execute(
        "select count(*) from (select msc_dedupe_key from msc_components_ranked group by 1 having count(*) > 1)"
    ).fetchone()[0]
    if msc_component_dedup_dropped_count > 0:
        log(
            "MSC component dedupe: "
            f"raw={format_count(msc_component_raw_count)}, "
            f"retained={format_count(msc_component_retained_count)}, "
            f"dropped={format_count(msc_component_dedup_dropped_count)}, "
            f"duplicate_groups={format_count(msc_component_dedup_group_count)}"
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
        """
        create or replace temp view sbx_catalog as
        select
          nullif(sn, '')::bigint as sbx_sn,
          nullif(gaia_id, '')::bigint as gaia_id,
          nullif(hip_id, '')::bigint as hip_id,
          nullif(hd_id, '')::bigint as hd_id,
          nullif(wds_id, '') as wds_id,
          nullif(ads_id, '') as ads_id,
          nullif(ra_deg, '')::double as ra_deg,
          nullif(dec_deg, '')::double as dec_deg,
          nullif(parallax_mas, '')::double as parallax_mas,
          nullif(pm_ra_mas_yr, '')::double as pm_ra_mas_yr,
          nullif(pm_dec_mas_yr, '')::double as pm_dec_mas_yr,
          nullif(mag_primary, '')::double as mag_primary,
          nullif(position_epoch, '')::double as position_epoch,
          nullif(position_source, '') as position_source,
          nullif(spectral_type_raw, '') as spectral_type_raw,
          nullif(family, '') as family,
          nullif(parent, '') as parent,
          nullif(child1, '') as child1,
          nullif(child2, '') as child2,
          nullif(in_triple, '') as in_triple,
          nullif(orbit_count, '')::bigint as orbit_count
        from sbx_raw
        where nullif(sn, '') is not null
        """
    )
    con.execute(
        """
        create or replace temp view sbx_athyg_matches as
        with candidates as (
          select
            a.athyg_row_id,
            s.sbx_sn,
            s.orbit_count,
            s.family,
            s.position_epoch,
            s.position_source,
            case
              when s.gaia_id is not null and a.gaia_id = s.gaia_id then 5
              when s.hip_id is not null and a.hip_id = s.hip_id and s.hd_id is not null and a.hd_id = s.hd_id then 4
              when s.hip_id is not null and a.hip_id = s.hip_id then 3
              when s.hd_id is not null and a.hd_id = s.hd_id then 2
              else 0
            end as match_score
          from athyg_stars_stage a
          join sbx_catalog s
            on (s.gaia_id is not null and a.gaia_id = s.gaia_id)
            or (s.hip_id is not null and a.hip_id = s.hip_id)
            or (s.hd_id is not null and a.hd_id = s.hd_id)
        ), ranked as (
          select
            *,
            row_number() over (
              partition by athyg_row_id
              order by match_score desc, coalesce(orbit_count, 0) desc, sbx_sn asc
            ) as rn
          from candidates
          where match_score > 0
        )
        select
          athyg_row_id,
          sbx_sn,
          orbit_count,
          family,
          position_epoch,
          position_source
        from ranked
        where rn = 1
        """
    )
    con.execute(
        """
        create or replace temp view sbx_msc_matches as
        with candidates as (
          select
            m.msc_row_num,
            s.sbx_sn,
            s.orbit_count,
            s.family,
            s.position_epoch,
            s.position_source,
            case
              when s.hip_id is not null and m.hip_id = s.hip_id and s.hd_id is not null and m.hd_id = s.hd_id then 4
              when s.hip_id is not null and m.hip_id = s.hip_id then 3
              when s.hd_id is not null and m.hd_id = s.hd_id then 2
              else 0
            end as match_score
          from msc_components m
          join sbx_catalog s
            on (s.hip_id is not null and m.hip_id = s.hip_id)
            or (s.hd_id is not null and m.hd_id = s.hd_id)
        ), ranked as (
          select
            *,
            row_number() over (
              partition by msc_row_num
              order by match_score desc, coalesce(orbit_count, 0) desc, sbx_sn asc
            ) as rn
          from candidates
          where match_score > 0
        )
        select
          msc_row_num,
          sbx_sn,
          orbit_count,
          family,
          position_epoch,
          position_source
        from ranked
        where rn = 1
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
            u.hmac_cluster_id as gaia_ucd_hmac_cluster_id,
            u.banyan_cluster as gaia_ucd_banyan_cluster,
            u.banyan_probability as gaia_ucd_banyan_probability,
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
            a.teff_k,
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
            coalesce(n.gaia_id is not null or t.gaia_id is not null, false) as has_gaia_nss_evidence,
            coalesce(m.wds_id is not null, false) as has_msc_evidence,
            coalesce(ws.wds_id is not null, false) as has_wds_evidence,
            coalesce(os.wds_id is not null, false) as has_orb6_evidence,
            sbx.sbx_sn as sbx_sn,
            sbx.orbit_count as sbx_orbit_count,
            sbx.family as sbx_family,
            sbx.position_epoch as sbx_position_epoch,
            sbx.position_source as sbx_position_source,
            json_object(
              'gaia', a.gaia_id,
              'hip', a.hip_id,
              'hd', a.hd_id,
              'hr', a.hr_id,
              'gl', a.gl_id,
              'tyc', a.tyc_id,
              'hyg', a.hyg_id,
              'wds', coalesce(m.wds_id, w.wds_id),
              'wds_component', coalesce(m.msc_component, w.wds_component),
              'sbx_sn', sbx.sbx_sn
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
          left join wds_support ws on ws.wds_id = coalesce(m.wds_id, w.wds_id)
          left join orb6_support os on os.wds_id = coalesce(m.wds_id, w.wds_id)
          left join gaia_ucd_catalog u on u.gaia_id = a.gaia_id
          left join sbx_athyg_matches sbx on sbx.athyg_row_id = a.athyg_row_id
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
            null::integer as gaia_ucd_hmac_cluster_id,
            null::varchar as gaia_ucd_banyan_cluster,
            null::double as gaia_ucd_banyan_probability,
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
            null::double as teff_k,
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
            false as has_gaia_nss_evidence,
            true as has_msc_evidence,
            coalesce(ws.wds_id is not null, false) as has_wds_evidence,
            coalesce(os.wds_id is not null, false) as has_orb6_evidence,
            sbx.sbx_sn as sbx_sn,
            sbx.orbit_count as sbx_orbit_count,
            sbx.family as sbx_family,
            sbx.position_epoch as sbx_position_epoch,
            sbx.position_source as sbx_position_source,
            json_object(
              'gaia', null,
              'hip', m.hip_id,
              'hd', m.hd_id,
              'wds', m.wds_id,
              'wds_component', m.msc_component,
              'sbx_sn', sbx.sbx_sn
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
          left join wds_support ws on ws.wds_id = m.wds_id
          left join orb6_support os on os.wds_id = m.wds_id
          left join sbx_msc_matches sbx on sbx.msc_row_num = m.msc_row_num
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
            "slice_profile_id": slice_profile_id,
            "slice_profile_version": slice_profile_version,
            "build_layer": build_layer,
            "source_galaxy_build_id": source_galaxy_build_id,
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

    log("Applying compact/remnant classification enrichments")
    con.execute("alter table stars add column object_family varchar")
    con.execute("alter table stars add column object_type varchar")
    con.execute("alter table stars add column classprob_dsc_combmod_whitedwarf double")
    con.execute("alter table stars add column classprob_dsc_specmod_whitedwarf double")
    con.execute("alter table stars add column wd_catalog_name varchar")
    con.execute("alter table stars add column wd_catalog_pwd double")
    con.execute("alter table stars add column wd_catalog_fit_model varchar")
    con.execute("alter table stars add column wd_catalog_teff_k double")
    con.execute("alter table stars add column wd_catalog_logg_cgs double")
    con.execute("alter table stars add column wd_catalog_mass_msun double")
    con.execute("alter table stars add column classification_evidence_json varchar")
    con.execute("alter table stars add column open_cluster_tags_json varchar")
    con.execute(
        """
        update stars
        set
          object_family = case
            when upper(coalesce(spectral_type_raw, '')) like 'D%' then 'white_dwarf'
            when spectral_class in ('L', 'T', 'Y') then 'brown_dwarf'
            else 'star'
          end,
          object_type = case
            when upper(coalesce(spectral_type_raw, '')) like 'D%' then 'white_dwarf'
            when spectral_class in ('L', 'T', 'Y') then 'brown_dwarf'
            else 'star'
          end,
          classification_evidence_json = json_object(
            'method', 'spectral_fallback',
            'spectral_type_raw', spectral_type_raw,
            'spectral_class', spectral_class
          )
        """
    )
    if enable_gaia_backbone and enable_gaia_classprob:
        con.execute(
            """
            update stars
            set
              classprob_dsc_combmod_whitedwarf = g.classprob_dsc_combmod_whitedwarf,
              classprob_dsc_specmod_whitedwarf = g.classprob_dsc_specmod_whitedwarf
            from gaia_classprob g
            where stars.gaia_id = g.gaia_id
            """
        )
    con.execute(
        """
        update stars
        set
          wd_catalog_name = coalesce(w.wdj_name, w.designation),
          wd_catalog_pwd = w.pwd,
          wd_catalog_fit_model = w.fit_model,
          teff_k = coalesce(w.teff_best_k, stars.teff_k),
          wd_catalog_teff_k = w.teff_best_k,
          wd_catalog_logg_cgs = w.logg_best_cgs,
          wd_catalog_mass_msun = w.mass_best_msun
        from white_dwarf_catalog w
        where stars.gaia_id = w.gaia_id
        """
    )
    con.execute(
        f"""
        update stars
        set
          object_family = 'white_dwarf',
          object_type = 'white_dwarf',
          classification_evidence_json = case
            when greatest(
              coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
              coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
            ) >= {WHITE_DWARF_PROB_THRESHOLD}
              and coalesce(wd_catalog_pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
              then json_object(
                'method', 'gaia_classprob+gaia_edr3_white_dwarf',
                'classprob_dsc_combmod_whitedwarf', classprob_dsc_combmod_whitedwarf,
                'classprob_dsc_specmod_whitedwarf', classprob_dsc_specmod_whitedwarf,
                'gaia_classprob_threshold', {WHITE_DWARF_PROB_THRESHOLD},
                'wd_catalog_pwd', wd_catalog_pwd,
                'wd_catalog_pwd_threshold', {white_dwarf_catalog_pwd_threshold},
                'wd_catalog_name', wd_catalog_name
              )
            when greatest(
              coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
              coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
            ) >= {WHITE_DWARF_PROB_THRESHOLD}
              then json_object(
                'method', 'gaia_classprob',
                'classprob_dsc_combmod_whitedwarf', classprob_dsc_combmod_whitedwarf,
                'classprob_dsc_specmod_whitedwarf', classprob_dsc_specmod_whitedwarf,
                'gaia_classprob_threshold', {WHITE_DWARF_PROB_THRESHOLD}
              )
            else json_object(
              'method', 'gaia_edr3_white_dwarf',
              'wd_catalog_pwd', wd_catalog_pwd,
              'wd_catalog_pwd_threshold', {white_dwarf_catalog_pwd_threshold},
              'wd_catalog_name', wd_catalog_name
            )
          end
        where greatest(
          coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
          coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
        ) >= {WHITE_DWARF_PROB_THRESHOLD}
          or coalesce(wd_catalog_pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
        """
    )

    alias_crosswalk_candidate_count = 0
    alias_crosswalk_matched_star_count = 0
    alias_name_override_count = 0
    athyg_merge_existing_match_count = 0
    athyg_merge_insert_count = 0
    athyg_merge_quarantine_count = 0
    athyg_merge_unresolved_count = 0
    athyg_merge_direct_gaia_count = 0
    athyg_merge_remap_count = 0
    athyg_merge_direct_legacy_id_count = 0
    athyg_merge_positional_count = 0
    athyg_merge_positional_ambiguous_count = 0
    object_identifier_count = 0
    identifier_gaia_collision_count = 0
    identifier_hip_collision_count = 0
    identifier_hd_collision_count = 0
    identifier_quarantine_count = 0
    if enable_aliases and enable_athyg_alias_crosswalk:
        log("Alias enrichment: preparing AT-HYG crosswalk candidates")
        con.execute(
            f"""
            create or replace temp view athyg_alias_candidates as
            with gaia_base as (
              select
                cast(nullif(id, '') as bigint) as source_pk,
                cast(nullif(gaia, '') as bigint) as gaia_id,
                cast(nullif(nullif(hip, ''), '0') as bigint) as hip_id,
                cast(nullif(nullif(hd, ''), '0') as bigint) as hd_id,
                cast(nullif(nullif(hr, ''), '0') as bigint) as hr_id,
                nullif(gl, '') as gl_id,
                nullif(tyc, '') as tyc_id,
                cast(nullif(nullif(hyg, ''), '0') as bigint) as hyg_id,
                nullif(proper, '') as proper_name,
                nullif(bayer, '') as bayer,
                nullif(flam, '') as flam,
                nullif(con, '') as constellation
              from athyg_alias_raw
              where cast(nullif(gaia, '') as bigint) is not null
            ), gaia_named as (
              select
                source_pk,
                gaia_id,
                hip_id,
                hd_id,
                hr_id,
                gl_id,
                tyc_id,
                hyg_id,
                proper_name,
                case
                  when bayer is not null and constellation is not null then bayer || ' ' || constellation
                  else null
                end as bayer_name,
                case
                  when flam is not null and constellation is not null then flam || ' ' || constellation
                  else null
                end as flam_name,
                0 as source_priority
              from gaia_base
            ), nogaia_base as (
              select
                cast(nullif(id, '') as bigint) as source_pk,
                cast(nullif(nullif(hip, ''), '0') as bigint) as hip_id,
                cast(nullif(nullif(hd, ''), '0') as bigint) as hd_id,
                cast(nullif(nullif(hr, ''), '0') as bigint) as hr_id,
                nullif(gl, '') as gl_id,
                nullif(tyc, '') as tyc_id,
                cast(nullif(nullif(hyg, ''), '0') as bigint) as hyg_id,
                nullif(proper, '') as proper_name,
                nullif(bayer, '') as bayer,
                nullif(flam, '') as flam,
                nullif(con, '') as constellation,
                case
                  when cast(nullif(ra, '') as double) between 0.0 and 24.0 then cast(nullif(ra, '') as double) * 15.0
                  else cast(nullif(ra, '') as double)
                end as ra_deg,
                cast(nullif(dec, '') as double) as dec_deg,
                cast(nullif(dist, '') as double) * {PC_TO_LY} as dist_ly
              from athyg_alias_raw
              where cast(nullif(gaia, '') as bigint) is null
                and (
                  nullif(proper, '') is not null
                  or (nullif(bayer, '') is not null and nullif(con, '') is not null)
                  or (nullif(flam, '') is not null and nullif(con, '') is not null)
                  or cast(nullif(nullif(hip, ''), '0') as bigint) is not null
                  or cast(nullif(nullif(hd, ''), '0') as bigint) is not null
                )
            ), nogaia_named as (
              select
                source_pk,
                hip_id,
                hd_id,
                hr_id,
                gl_id,
                tyc_id,
                hyg_id,
                proper_name,
                case
                  when bayer is not null and constellation is not null then bayer || ' ' || constellation
                  else null
                end as bayer_name,
                case
                  when flam is not null and constellation is not null then flam || ' ' || constellation
                  else null
                end as flam_name,
                ra_deg,
                dec_deg,
                dist_ly
              from nogaia_base
              where source_pk is not null
                and ra_deg is not null
                and dec_deg is not null
                and dist_ly is not null
                and (
                  proper_name is not null
                  or (bayer is not null and constellation is not null)
                  or (flam is not null and constellation is not null)
                  or hip_id is not null
                  or hd_id is not null
                )
            ), positional_candidates as (
              select
                n.source_pk,
                s.gaia_id as gaia_id,
                n.hip_id,
                n.hd_id,
                n.hr_id,
                n.gl_id,
                n.tyc_id,
                n.hyg_id,
                n.proper_name,
                n.bayer_name,
                n.flam_name,
                1 as source_priority,
                case
                  when n.hip_id is not null and s.hip_id = n.hip_id then 0
                  when n.hd_id is not null and s.hd_id = n.hd_id then 1
                  else 9
                end as identifier_match_rank,
                0.0::double as dist_delta_ly,
                0.0::double as ang_sep_arcsec
              from nogaia_named n
              join stars s on s.gaia_id is not null
              where (
                n.hip_id is not null
                and s.hip_id = n.hip_id
              ) or (
                n.hd_id is not null
                and s.hd_id = n.hd_id
              )

              union all

              select
                n.source_pk,
                s.gaia_id as gaia_id,
                n.hip_id,
                n.hd_id,
                n.hr_id,
                n.gl_id,
                n.tyc_id,
                n.hyg_id,
                n.proper_name,
                n.bayer_name,
                n.flam_name,
                2 as source_priority,
                9 as identifier_match_rank,
                abs(s.dist_ly - n.dist_ly) as dist_delta_ly,
                degrees(acos(
                  least(
                    1.0,
                    greatest(
                      -1.0,
                      sin(radians(s.dec_deg)) * sin(radians(n.dec_deg)) +
                      cos(radians(s.dec_deg)) * cos(radians(n.dec_deg)) * cos(radians(s.ra_deg - n.ra_deg))
                    )
                  )
                )) * 3600.0 as ang_sep_arcsec
              from nogaia_named n
              join stars s on s.gaia_id is not null
              where abs(s.ra_deg - n.ra_deg) <= {ALIAS_POS_MAX_DELTA_RA_DEG}
                and abs(s.dec_deg - n.dec_deg) <= {ALIAS_POS_MAX_DELTA_DEC_DEG}
                and abs(s.dist_ly - n.dist_ly) <= {ALIAS_POS_MAX_DELTA_DIST_LY}
            ), positional_best as (
              select
                source_pk,
                gaia_id,
                hip_id,
                hd_id,
                hr_id,
                gl_id,
                tyc_id,
                hyg_id,
                proper_name,
                bayer_name,
                flam_name,
                source_priority,
                row_number() over (
                  partition by source_pk
                  order by source_priority asc, identifier_match_rank asc, dist_delta_ly asc, ang_sep_arcsec asc, gaia_id asc
                ) as rn
              from positional_candidates
              where source_priority = 1
                 or ang_sep_arcsec <= {ALIAS_POS_MAX_ANG_SEP_ARCSEC}
            ), combined as (
              select
                source_pk,
                gaia_id,
                hip_id,
                hd_id,
                hr_id,
                gl_id,
                tyc_id,
                hyg_id,
                proper_name,
                bayer_name,
                flam_name,
                source_priority
              from gaia_named
              union all
              select
                source_pk,
                gaia_id,
                hip_id,
                hd_id,
                hr_id,
                gl_id,
                tyc_id,
                hyg_id,
                proper_name,
                bayer_name,
                flam_name,
                source_priority
              from positional_best
              where rn = 1
            ), ranked as (
              select
                *,
                row_number() over (
                  partition by gaia_id
                  order by
                    source_priority asc,
                    case when proper_name is not null then 0 else 1 end,
                    case when bayer_name is not null then 0 else 1 end,
                    case when flam_name is not null then 0 else 1 end,
                    case when hip_id is not null then 0 else 1 end,
                    case when hd_id is not null then 0 else 1 end,
                    source_pk asc
                ) as rn
              from combined
            )
            select
              source_pk,
              gaia_id,
              hip_id,
              hd_id,
              hr_id,
              gl_id,
              tyc_id,
              hyg_id,
              proper_name,
              bayer_name,
              flam_name,
              coalesce(proper_name, bayer_name, flam_name) as preferred_name
            from ranked
            where rn = 1
            """
        )
        alias_crosswalk_candidate_count = con.execute(
            "select count(*) from athyg_alias_candidates"
        ).fetchone()[0]
        alias_crosswalk_matched_star_count = con.execute(
            """
            select count(*)
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
            """
        ).fetchone()[0]
        alias_name_override_count = con.execute(
            """
            select count(*)
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
            where a.preferred_name is not null
              and (s.star_name is null or s.star_name_norm like 'gaia dr3 %')
            """
        ).fetchone()[0]
        con.execute(
            """
            update stars
            set
              hip_id = coalesce(stars.hip_id, a.hip_id),
              hd_id = coalesce(stars.hd_id, a.hd_id),
              star_name = case
                when (stars.star_name is null or stars.star_name_norm like 'gaia dr3 %')
                  and a.preferred_name is not null
                then a.preferred_name
                else stars.star_name
              end,
              star_name_norm = case
                when (
                  (stars.star_name is null or stars.star_name_norm like 'gaia dr3 %')
                  and a.preferred_name is not null
                ) then lower(
                  trim(
                    regexp_replace(
                      regexp_replace(a.preferred_name, '[^0-9A-Za-z]+', ' ', 'g'),
                      '\\s+',
                      ' ',
                      'g'
                    )
                  )
                )
                else stars.star_name_norm
              end,
              catalog_ids_json = json_object(
                'gaia', coalesce(stars.gaia_id, a.gaia_id),
                'hip', coalesce(stars.hip_id, a.hip_id),
                'hd', coalesce(stars.hd_id, a.hd_id),
                'hr', coalesce(a.hr_id, cast(json_extract_string(stars.catalog_ids_json, '$.hr') as bigint)),
                'gl', coalesce(a.gl_id, json_extract_string(stars.catalog_ids_json, '$.gl')),
                'tyc', coalesce(a.tyc_id, json_extract_string(stars.catalog_ids_json, '$.tyc')),
                'hyg', coalesce(a.hyg_id, cast(json_extract_string(stars.catalog_ids_json, '$.hyg') as bigint)),
                'wds', coalesce(stars.wds_id, json_extract_string(stars.catalog_ids_json, '$.wds')),
                'wds_component', coalesce(stars.component, json_extract_string(stars.catalog_ids_json, '$.wds_component')),
                'sbx_sn', stars.sbx_sn
              )
            from athyg_alias_candidates a
            where stars.gaia_id = a.gaia_id
            """
        )
        if alias_name_override_count > ALIAS_NAME_OVERRIDE_LIMIT:
            log(
                "Alias enrichment: high name override volume "
                f"({alias_name_override_count} rows; limit hint={ALIAS_NAME_OVERRIDE_LIMIT})"
            )
    else:
        con.execute(
            """
            create or replace temp view athyg_alias_candidates as
            select *
            from (
              values
                (
                  cast(null as bigint), cast(null as bigint), cast(null as bigint), cast(null as bigint),
                  cast(null as bigint), cast(null as varchar), cast(null as varchar), cast(null as bigint),
                  cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              source_pk, gaia_id, hip_id, hd_id, hr_id, gl_id, tyc_id, hyg_id,
              proper_name, bayer_name, flam_name, preferred_name
            )
            where false
            """
        )

    if enable_gaia_backbone and enable_athyg_supplement_merge:
        log("Identifier merge: building AT-HYG merge source")
        con.execute(
            f"""
            create temp table athyg_merge_source as
            with base as (
              select
                cast(nullif(src.id, '') as bigint) as source_pk,
                cast(nullif(src.gaia, '') as bigint) as gaia_id,
                cast(nullif(nullif(src.hip, ''), '0') as bigint) as hip_id,
                cast(nullif(nullif(src.hd, ''), '0') as bigint) as hd_id,
                cast(nullif(nullif(src.hr, ''), '0') as bigint) as hr_id,
                nullif(src.gl, '') as gl_id,
                nullif(src.tyc, '') as tyc_id,
                cast(nullif(nullif(src.hyg, ''), '0') as bigint) as hyg_id,
                nullif(src.proper, '') as proper_name,
                nullif(src.bayer, '') as bayer,
                nullif(src.flam, '') as flam,
                nullif(src.con, '') as constellation,
                case
                  when cast(nullif(src.ra, '') as double) between 0.0 and 24.0
                    then cast(nullif(src.ra, '') as double) * 15.0
                  else cast(nullif(src.ra, '') as double)
                end as ra_deg,
                cast(nullif(src.dec, '') as double) as dec_deg,
                cast(nullif(src.dist, '') as double) as dist_pc,
                null::double as parallax_mas,
                null::double as parallax_error_mas,
                null::double as parallax_over_error,
                null::double as ruwe,
                cast(nullif(src.pm_ra, '') as double) as pm_ra_mas_yr,
                cast(nullif(src.pm_dec, '') as double) as pm_dec_mas_yr,
                cast(nullif(src.rv, '') as double) as radial_velocity_kms,
                cast(nullif(src.ci, '') as double) as color_index,
                cast(nullif(src.mag, '') as double) as vmag,
                cast(nullif(src.absmag, '') as double) as absmag,
                cast(nullif(src.x0, '') as double) as x_pc,
                cast(nullif(src.y0, '') as double) as y_pc,
                cast(nullif(src.z0, '') as double) as z_pc,
                nullif(src.spect, '') as spectral_type_raw
              from athyg_alias_raw src
              where cast(nullif(src.id, '') as bigint) is not null
            ), coords as (
              select
                *,
                case
                  when x_pc is not null and y_pc is not null and z_pc is not null
                    then sqrt(x_pc * x_pc + y_pc * y_pc + z_pc * z_pc)
                  else dist_pc
                end as dist_pc_final
              from base
            ), normalized as (
              select
                *,
                dist_pc_final * {PC_TO_LY} as dist_ly,
                coalesce(
                  x_pc * {PC_TO_LY},
                  (dist_pc_final * cos(dec_deg * pi() / 180.0) * cos(ra_deg * pi() / 180.0)) * {PC_TO_LY}
                ) as x_helio_ly,
                coalesce(
                  y_pc * {PC_TO_LY},
                  (dist_pc_final * cos(dec_deg * pi() / 180.0) * sin(ra_deg * pi() / 180.0)) * {PC_TO_LY}
                ) as y_helio_ly,
                coalesce(
                  z_pc * {PC_TO_LY},
                  (dist_pc_final * sin(dec_deg * pi() / 180.0)) * {PC_TO_LY}
                ) as z_helio_ly,
                case
                  when proper_name is not null then proper_name
                  when bayer is not null and constellation is not null then bayer || ' ' || constellation
                  when flam is not null and constellation is not null then flam || ' ' || constellation
                  when hip_id is not null then 'HIP ' || hip_id::varchar
                  when hd_id is not null then 'HD ' || hd_id::varchar
                  when gaia_id is not null then 'Gaia ' || gaia_id::varchar
                  else null
                end as preferred_name,
                case
                  when proper_name is not null then regexp_extract(proper_name, ' ([A-Za-z]{1,2})$', 1)
                  else null
                end as component,
                case
                  when proper_name is not null then regexp_replace(proper_name, '\\s+[A-Za-z]{1,2}$', '')
                  else null
                end as system_name_root,
                regexp_extract(spectral_type_raw, '([OBAFGKMLTYD])', 1) as spectral_class,
                regexp_extract(spectral_type_raw, '[OBAFGKMLTYD]([0-9](?:\\.[0-9])?)', 1) as spectral_subtype,
                regexp_extract(spectral_type_raw, '(I{1,3}|IV|V|VI|VII)', 1) as luminosity_class,
                (
                  proper_name is not null
                  or (bayer is not null and constellation is not null)
                  or (flam is not null and constellation is not null)
                ) as has_human_name
              from coords
            )
            select
              *,
              lower(trim(regexp_replace(regexp_replace(preferred_name, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) as preferred_name_norm,
              lower(trim(regexp_replace(regexp_replace(system_name_root, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) as system_name_root_norm,
              cast(floor(ra_deg * {ATHYG_MERGE_SKY_BIN_FACTOR}) as integer) as ra_bin,
              cast(floor((dec_deg + 90.0) * {ATHYG_MERGE_SKY_BIN_FACTOR}) as integer) as dec_bin
            from normalized
            """
        )
        athyg_merge_source_count = con.execute("select count(*) from athyg_merge_source").fetchone()[0]
        log(f"Identifier merge: source rows={format_count(athyg_merge_source_count)}")

        con.execute(
            """
            create temp table star_gaia_stats as
            select gaia_id, count(*)::bigint as star_count, min(star_id)::bigint as star_id
            from stars
            where gaia_id is not null
            group by gaia_id
            """
        )
        con.execute(
            """
            create temp table star_hip_stats as
            select hip_id, count(*)::bigint as star_count, min(star_id)::bigint as star_id
            from stars
            where hip_id is not null
            group by hip_id
            """
        )
        con.execute(
            """
            create temp table star_hd_stats as
            select hd_id, count(*)::bigint as star_count, min(star_id)::bigint as star_id
            from stars
            where hd_id is not null
            group by hd_id
            """
        )
        con.execute(
            """
            create temp table athyg_merge_id_base as
            select
              a.*,
              coalesce(g.star_count, 0) as gaia_star_count,
              g.star_id as gaia_star_id,
              coalesce(h.star_count, 0) as hip_star_count,
              h.star_id as hip_star_id,
              coalesce(d.star_count, 0) as hd_star_count,
              d.star_id as hd_star_id
            from athyg_merge_source a
            left join star_gaia_stats g on a.gaia_id = g.gaia_id
            left join star_hip_stats h on a.hip_id = h.hip_id
            left join star_hd_stats d on a.hd_id = d.hd_id
            """
        )
        con.execute(
            """
            create temp table athyg_merge_id_quarantine as
            select * from (
              select
                source_pk,
                gaia_id,
                hip_id,
                hd_id,
                'gaia_id_multi_match' as reason,
                json_object('gaia_star_count', gaia_star_count) as details_json
              from athyg_merge_id_base
              where gaia_id is not null and gaia_star_count > 1
              union all
              select
                source_pk,
                gaia_id,
                hip_id,
                hd_id,
                'hip_id_multi_match' as reason,
                json_object('hip_star_count', hip_star_count) as details_json
              from athyg_merge_id_base
              where hip_id is not null and hip_star_count > 1
              union all
              select
                source_pk,
                gaia_id,
                hip_id,
                hd_id,
                'hd_id_multi_match' as reason,
                json_object('hd_star_count', hd_star_count) as details_json
              from athyg_merge_id_base
              where hd_id is not null and hd_star_count > 1
              union all
              select
                source_pk,
                gaia_id,
                hip_id,
                hd_id,
                'hip_hd_conflict' as reason,
                json_object('hip_star_id', hip_star_id, 'hd_star_id', hd_star_id) as details_json
              from athyg_merge_id_base
              where hip_star_count = 1 and hd_star_count = 1 and hip_star_id <> hd_star_id
            ) q
            """
        )
        con.execute(
            """
            create temp table athyg_merge_id_resolution as
            select
              b.source_pk,
              case
                when b.gaia_star_count = 1 then b.gaia_star_id
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hip_star_count = 1 and b.hd_star_count = 1 and b.hip_star_id = b.hd_star_id then b.hip_star_id
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hip_star_count = 1 and coalesce(b.hd_star_count, 0) = 0 then b.hip_star_id
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hd_star_count = 1 and coalesce(b.hip_star_count, 0) = 0 then b.hd_star_id
                when b.gaia_id is null and b.hip_star_count = 1 and (coalesce(b.hd_star_count, 0) = 0 or b.hd_star_id = b.hip_star_id) then b.hip_star_id
                when b.gaia_id is null and b.hd_star_count = 1 and coalesce(b.hip_star_count, 0) = 0 then b.hd_star_id
                else null
              end as matched_star_id,
              case
                when b.gaia_star_count = 1 then 'gaia_exact'
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hip_star_count = 1 and b.hd_star_count = 1 and b.hip_star_id = b.hd_star_id then 'gaia_remap_hip_hd'
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hip_star_count = 1 and coalesce(b.hd_star_count, 0) = 0 then 'gaia_remap_hip'
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hd_star_count = 1 and coalesce(b.hip_star_count, 0) = 0 then 'gaia_remap_hd'
                when b.gaia_id is null and b.hip_star_count = 1 and (coalesce(b.hd_star_count, 0) = 0 or b.hd_star_id = b.hip_star_id) then 'hip_exact'
                when b.gaia_id is null and b.hd_star_count = 1 and coalesce(b.hip_star_count, 0) = 0 then 'hd_exact'
                else null
              end as resolution_method,
              case
                when b.gaia_star_count = 1 then 1.0
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hip_star_count = 1 and b.hd_star_count = 1 and b.hip_star_id = b.hd_star_id then 0.995
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hip_star_count = 1 and coalesce(b.hd_star_count, 0) = 0 then 0.99
                when b.gaia_id is not null and b.gaia_star_count = 0 and b.hd_star_count = 1 and coalesce(b.hip_star_count, 0) = 0 then 0.99
                when b.gaia_id is null and b.hip_star_count = 1 and (coalesce(b.hd_star_count, 0) = 0 or b.hd_star_id = b.hip_star_id) then 0.985
                when b.gaia_id is null and b.hd_star_count = 1 and coalesce(b.hip_star_count, 0) = 0 then 0.98
                else null
              end as resolution_confidence
            from athyg_merge_id_base b
            where not exists (
              select 1
              from athyg_merge_id_quarantine q
              where q.source_pk = b.source_pk
            )
            """
        )
        con.execute(
            f"""
            create temp table athyg_merge_positional_unresolved as
            select *
            from athyg_merge_id_base b
            where not exists (select 1 from athyg_merge_id_resolution r where r.source_pk = b.source_pk and r.matched_star_id is not null)
              and not exists (select 1 from athyg_merge_id_quarantine q where q.source_pk = b.source_pk)
              and b.dist_ly is not null
              and b.dist_ly <= {MORTON_MAX_ABS_LY}
              and b.ra_deg is not null
              and b.dec_deg is not null
              and (b.has_human_name or b.hip_id is not null or b.hd_id is not null)
            """
        )
        con.execute(
            f"""
            create temp table stars_merge_binned as
            select
              star_id,
              ra_deg,
              dec_deg,
              dist_ly,
              cast(floor(ra_deg * {ATHYG_MERGE_SKY_BIN_FACTOR}) as integer) as ra_bin,
              cast(floor((dec_deg + 90.0) * {ATHYG_MERGE_SKY_BIN_FACTOR}) as integer) as dec_bin
            from stars
            where ra_deg is not null
              and dec_deg is not null
              and dist_ly is not null
              and dist_ly <= {MORTON_MAX_ABS_LY}
            """
        )
        con.execute(
            f"""
            create temp table athyg_merge_positional_ranked as
            with expanded as (
              select distinct
                u.source_pk,
                u.ra_deg,
                u.dec_deg,
                u.dist_ly,
                u.has_human_name,
                ((u.ra_bin + ro.delta + {int(360 * ATHYG_MERGE_SKY_BIN_FACTOR)}) % {int(360 * ATHYG_MERGE_SKY_BIN_FACTOR)}) as ra_bin_n,
                least(greatest(u.dec_bin + doff.delta, 0), {int(180 * ATHYG_MERGE_SKY_BIN_FACTOR) - 1}) as dec_bin_n
              from athyg_merge_positional_unresolved u
              cross join (values (-1), (0), (1)) as ro(delta)
              cross join (values (-1), (0), (1)) as doff(delta)
            ), candidates as (
              select
                e.source_pk,
                s.star_id,
                e.has_human_name,
                abs(s.dist_ly - e.dist_ly) as dist_delta_ly,
                degrees(acos(
                  least(
                    1.0,
                    greatest(
                      -1.0,
                      sin(radians(s.dec_deg)) * sin(radians(e.dec_deg)) +
                      cos(radians(s.dec_deg)) * cos(radians(e.dec_deg)) *
                        cos(radians(least(abs(s.ra_deg - e.ra_deg), 360.0 - abs(s.ra_deg - e.ra_deg))))
                    )
                  )
                )) * 3600.0 as ang_sep_arcsec
              from expanded e
              join stars_merge_binned s
                on s.ra_bin = e.ra_bin_n
               and s.dec_bin = e.dec_bin_n
              where least(abs(s.ra_deg - e.ra_deg), 360.0 - abs(s.ra_deg - e.ra_deg)) <= {ALIAS_POS_MAX_DELTA_RA_DEG}
                and abs(s.dec_deg - e.dec_deg) <= {ALIAS_POS_MAX_DELTA_DEC_DEG}
                and abs(s.dist_ly - e.dist_ly) <= {ALIAS_POS_MAX_DELTA_DIST_LY}
            )
            select
              *,
              row_number() over (partition by source_pk order by dist_delta_ly asc, ang_sep_arcsec asc, star_id asc) as rn,
              count(*) over (partition by source_pk) as candidate_count
            from candidates
            where ang_sep_arcsec <= {ALIAS_POS_MAX_ANG_SEP_ARCSEC}
            """
        )
        con.execute(
            """
            create temp table athyg_merge_positional_quarantine as
            select
              p.source_pk,
              b.gaia_id,
              b.hip_id,
              b.hd_id,
              'positional_ambiguous' as reason,
              json_object(
                'candidate_count', p.candidate_count,
                'best_dist_delta_ly', p.dist_delta_ly,
                'best_ang_sep_arcsec', p.ang_sep_arcsec
              ) as details_json
            from athyg_merge_positional_ranked p
            join athyg_merge_id_base b using (source_pk)
            where p.rn = 1 and p.candidate_count > 1
            """
        )
        con.execute(
            f"""
            create temp table athyg_merge_positional_resolution as
            select
              p.source_pk,
              p.star_id as matched_star_id,
              case when p.has_human_name then 'positional_named' else 'positional_numeric' end as resolution_method,
              case
                when p.has_human_name then {ATHYG_MERGE_POSITIONAL_CONFIDENCE_NAMED}
                else {ATHYG_MERGE_POSITIONAL_CONFIDENCE_NUMERIC}
              end as resolution_confidence
            from athyg_merge_positional_ranked p
            where p.rn = 1 and p.candidate_count = 1
            """
        )
        con.execute(
            """
            create temp table athyg_merge_resolution as
            select source_pk, matched_star_id, resolution_method, resolution_confidence
            from athyg_merge_id_resolution
            where matched_star_id is not null
            union all
            select source_pk, matched_star_id, resolution_method, resolution_confidence
            from athyg_merge_positional_resolution
            """
        )
        con.execute(
            """
            create temp table athyg_merge_quarantine as
            select * from athyg_merge_id_quarantine
            union all
            select * from athyg_merge_positional_quarantine
            """
        )

        con.execute(
            """
            update stars
            set
              hip_id = coalesce(stars.hip_id, a.hip_id),
              hd_id = coalesce(stars.hd_id, a.hd_id),
              star_name = case
                when (stars.star_name is null or stars.star_name_norm like 'gaia dr3 %')
                  and a.preferred_name is not null
                then a.preferred_name
                else stars.star_name
              end,
              star_name_norm = case
                when (stars.star_name is null or stars.star_name_norm like 'gaia dr3 %')
                  and a.preferred_name is not null
                then a.preferred_name_norm
                else stars.star_name_norm
              end,
              catalog_ids_json = json_object(
                'gaia', coalesce(stars.gaia_id, a.gaia_id),
                'hip', coalesce(stars.hip_id, a.hip_id),
                'hd', coalesce(stars.hd_id, a.hd_id),
                'hr', coalesce(a.hr_id, cast(json_extract_string(stars.catalog_ids_json, '$.hr') as bigint)),
                'gl', coalesce(a.gl_id, json_extract_string(stars.catalog_ids_json, '$.gl')),
                'tyc', coalesce(a.tyc_id, json_extract_string(stars.catalog_ids_json, '$.tyc')),
                'hyg', coalesce(a.hyg_id, cast(json_extract_string(stars.catalog_ids_json, '$.hyg') as bigint)),
                'wds', coalesce(stars.wds_id, json_extract_string(stars.catalog_ids_json, '$.wds')),
                'wds_component', coalesce(stars.component, json_extract_string(stars.catalog_ids_json, '$.wds_component'))
              )
            from athyg_merge_resolution r
            join athyg_merge_source a on a.source_pk = r.source_pk
            where stars.star_id = r.matched_star_id
            """
        )
        merge_slice_conditions: list[str] = []
        if slice_max_distance_ly is not None:
            max_dist = min(slice_max_distance_ly, MORTON_MAX_ABS_LY)
            merge_slice_conditions.append(f"(b.dist_ly is not null and b.dist_ly <= {max_dist})")
        if slice_min_parallax_over_error is not None:
            merge_slice_conditions.append(
                f"(b.parallax_over_error is not null and b.parallax_over_error >= {slice_min_parallax_over_error})"
            )
        if slice_max_parallax_error_mas is not None:
            merge_slice_conditions.append(
                f"(b.parallax_error_mas is not null and b.parallax_error_mas <= {slice_max_parallax_error_mas})"
            )
        if slice_max_ruwe is not None:
            merge_slice_conditions.append(f"(b.ruwe is not null and b.ruwe <= {slice_max_ruwe})")
        if slice_require_spectral:
            merge_slice_conditions.append("(b.spectral_class is not null and b.spectral_class <> '')")
        if slice_require_color:
            merge_slice_conditions.append("(b.color_index is not null)")
        if slice_allowed_spectral:
            allowed_list_sql = ", ".join(sql_literal(token) for token in slice_allowed_spectral)
            merge_slice_conditions.append(
                f"(coalesce(upper(b.spectral_class), 'UNKNOWN') in ({allowed_list_sql}))"
            )
        merge_slice_where_sql = (
            " and ".join(merge_slice_conditions) if merge_slice_conditions else "true"
        )

        con.execute(
            f"""
            create temp table athyg_merge_unmatched as
            with base as (
              select b.*
              from athyg_merge_id_base b
              where not exists (select 1 from athyg_merge_resolution r where r.source_pk = b.source_pk)
                and not exists (select 1 from athyg_merge_quarantine q where q.source_pk = b.source_pk)
                and b.dist_ly is not null
                and b.dist_ly <= {MORTON_MAX_ABS_LY}
                and b.ra_deg is not null
                and b.dec_deg is not null
                and b.x_helio_ly is not null
                and b.y_helio_ly is not null
                and b.z_helio_ly is not null
                and ({merge_slice_where_sql})
            ), deduped as (
              select
                *,
                row_number() over (
                  partition by gaia_id
                  order by
                    case when has_human_name then 0 else 1 end,
                    case when hip_id is not null then 0 else 1 end,
                    case when hd_id is not null then 0 else 1 end,
                    source_pk asc
                ) as rn_gaia,
                row_number() over (
                  partition by hip_id
                  order by
                    case when gaia_id is not null then 0 else 1 end,
                    case when has_human_name then 0 else 1 end,
                    source_pk asc
                ) as rn_hip,
                row_number() over (
                  partition by hd_id
                  order by
                    case when gaia_id is not null then 0 else 1 end,
                    case when has_human_name then 0 else 1 end,
                    source_pk asc
                ) as rn_hd
              from base
            )
            select *
            from deduped
            where (gaia_id is null or rn_gaia = 1)
              and (hip_id is null or rn_hip = 1)
              and (hd_id is null or rn_hd = 1)
            """
        )
        con.execute(
            f"""
            insert into stars (
              star_id, spatial_index, system_id, stable_object_key, star_name, star_name_norm, component,
              system_name_root, system_name_root_norm, ra_deg, dec_deg, dist_ly, parallax_mas, parallax_error_mas,
              parallax_over_error, ruwe, gaia_ucd_hmac_cluster_id, gaia_ucd_banyan_cluster, gaia_ucd_banyan_probability,
              x_helio_ly, y_helio_ly, z_helio_ly, x_gal_ly, y_gal_ly, z_gal_ly,
              pm_ra_mas_yr, pm_dec_mas_yr, radial_velocity_kms, spectral_type_raw, spectral_class, spectral_subtype,
              luminosity_class, spectral_peculiar, vmag, absmag, color_index, gaia_id, hip_id, hd_id, wds_id,
              multiplicity_match_method, multiplicity_match_confidence, multiplicity_source_catalogs_json,
              gaia_non_single_star, gaia_nss_solution_count, gaia_nss_solution_types_json, gaia_nss_significance_max,
              has_gaia_nss_evidence, has_msc_evidence, has_wds_evidence, has_orb6_evidence,
              sbx_sn, sbx_orbit_count, sbx_family, sbx_position_epoch, sbx_position_source,
              catalog_ids_json, source_catalog, source_version, source_url, source_download_url, source_doi,
              source_pk, source_row_id, source_row_hash, license, redistribution_ok, license_note, retrieval_etag,
              retrieval_checksum, retrieved_at, ingested_at, transform_version
            )
            with seq as (
              select coalesce(max(star_id), 0)::bigint as max_star_id from stars
            ), ordered as (
              select
                *,
                row_number() over (order by source_pk) as rn
              from athyg_merge_unmatched
            )
            select
              seq.max_star_id + ordered.rn::bigint as star_id,
              cast(morton3d(ordered.x_helio_ly, ordered.y_helio_ly, ordered.z_helio_ly) as bigint) as spatial_index,
              null::bigint as system_id,
              'star:athyg:' || ordered.source_pk::varchar as stable_object_key,
              ordered.preferred_name as star_name,
              ordered.preferred_name_norm as star_name_norm,
              ordered.component,
              ordered.system_name_root,
              ordered.system_name_root_norm,
              ordered.ra_deg,
              ordered.dec_deg,
              ordered.dist_ly,
              ordered.parallax_mas,
              ordered.parallax_error_mas,
              ordered.parallax_over_error,
              ordered.ruwe,
              null::integer as gaia_ucd_hmac_cluster_id,
              null::varchar as gaia_ucd_banyan_cluster,
              null::double as gaia_ucd_banyan_probability,
              ordered.x_helio_ly,
              ordered.y_helio_ly,
              ordered.z_helio_ly,
              null::double as x_gal_ly,
              null::double as y_gal_ly,
              null::double as z_gal_ly,
              ordered.pm_ra_mas_yr,
              ordered.pm_dec_mas_yr,
              ordered.radial_velocity_kms,
              ordered.spectral_type_raw,
              ordered.spectral_class,
              ordered.spectral_subtype,
              ordered.luminosity_class,
              null::varchar as spectral_peculiar,
              ordered.vmag,
              ordered.absmag,
              ordered.color_index,
              ordered.gaia_id,
              ordered.hip_id,
              ordered.hd_id,
              null::varchar as wds_id,
              'athyg_supplement_insert' as multiplicity_match_method,
              0.75 as multiplicity_match_confidence,
              '[]' as multiplicity_source_catalogs_json,
              false as gaia_non_single_star,
              0::bigint as gaia_nss_solution_count,
              '[]' as gaia_nss_solution_types_json,
              null::double as gaia_nss_significance_max,
              false as has_gaia_nss_evidence,
              false as has_msc_evidence,
              false as has_wds_evidence,
              false as has_orb6_evidence,
              null::bigint as sbx_sn,
              0::bigint as sbx_orbit_count,
              null::varchar as sbx_family,
              null::double as sbx_position_epoch,
              null::varchar as sbx_position_source,
              json_object(
                'gaia', ordered.gaia_id,
                'hip', ordered.hip_id,
                'hd', ordered.hd_id,
                'hr', ordered.hr_id,
                'gl', ordered.gl_id,
                'tyc', ordered.tyc_id,
                'hyg', ordered.hyg_id,
                'wds', null,
                'wds_component', null,
                'sbx_sn', null
              ) as catalog_ids_json,
              'athyg' as source_catalog,
              {sql_literal(ATHYG_ALIAS_VERSION)} as source_version,
              {sql_literal(ATHYG_ALIAS_URL)} as source_url,
              {sql_literal(athyg_download_url)} as source_download_url,
              null::varchar as source_doi,
              ordered.source_pk as source_pk,
              ordered.source_pk as source_row_id,
              null::varchar as source_row_hash,
              'CC BY-SA 4.0' as license,
              true as redistribution_ok,
              {sql_literal(ATHYG_ALIAS_URL)} as license_note,
              null::varchar as retrieval_etag,
              {sql_literal(athyg_checksum)} as retrieval_checksum,
              {sql_literal(athyg_retrieved)} as retrieved_at,
              {sql_literal(ingested_at)} as ingested_at,
              {sql_literal(transform_version)} as transform_version
            from ordered
            cross join seq
            """
        )

        con.execute(
            """
            update stars
            set
              object_family = case
                when upper(coalesce(spectral_type_raw, '')) like 'D%' then 'white_dwarf'
                when spectral_class in ('L', 'T', 'Y') then 'brown_dwarf'
                else coalesce(object_family, 'star')
              end,
              object_type = case
                when upper(coalesce(spectral_type_raw, '')) like 'D%' then 'white_dwarf'
                when spectral_class in ('L', 'T', 'Y') then 'brown_dwarf'
                else coalesce(object_type, 'star')
              end,
              classification_evidence_json = coalesce(
                classification_evidence_json,
                json_object(
                  'method', 'athyg_supplement_spectral_fallback',
                  'spectral_type_raw', spectral_type_raw,
                  'spectral_class', spectral_class
                )
              )
            where source_catalog = 'athyg'
              and source_version = ? and ingested_at = ? and transform_version = ?
            """,
            [ATHYG_ALIAS_VERSION, ingested_at, transform_version],
        )
        if enable_gaia_backbone and enable_gaia_classprob:
            con.execute(
                """
                update stars
                set
                  classprob_dsc_combmod_whitedwarf = g.classprob_dsc_combmod_whitedwarf,
                  classprob_dsc_specmod_whitedwarf = g.classprob_dsc_specmod_whitedwarf
                from gaia_classprob g
                where stars.source_catalog = 'athyg'
                  and stars.source_version = ? and stars.ingested_at = ? and stars.transform_version = ?
                  and stars.gaia_id = g.gaia_id
                """,
                [ATHYG_ALIAS_VERSION, ingested_at, transform_version],
            )
        con.execute(
            """
            update stars
            set
              wd_catalog_name = coalesce(w.wdj_name, w.designation),
              wd_catalog_pwd = w.pwd,
              wd_catalog_fit_model = w.fit_model,
              wd_catalog_teff_k = w.teff_best_k,
              wd_catalog_logg_cgs = w.logg_best_cgs,
              wd_catalog_mass_msun = w.mass_best_msun
            from white_dwarf_catalog w
            where stars.source_catalog = 'athyg'
              and stars.source_version = ? and stars.ingested_at = ? and stars.transform_version = ?
              and stars.gaia_id = w.gaia_id
            """,
            [ATHYG_ALIAS_VERSION, ingested_at, transform_version],
        )
        con.execute(
            f"""
            update stars
            set
              object_family = 'white_dwarf',
              object_type = 'white_dwarf',
              classification_evidence_json = case
                when greatest(
                  coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
                  coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
                ) >= {WHITE_DWARF_PROB_THRESHOLD}
                  and coalesce(wd_catalog_pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
                  then json_object(
                    'method', 'gaia_classprob+gaia_edr3_white_dwarf',
                    'classprob_dsc_combmod_whitedwarf', classprob_dsc_combmod_whitedwarf,
                    'classprob_dsc_specmod_whitedwarf', classprob_dsc_specmod_whitedwarf,
                    'gaia_classprob_threshold', {WHITE_DWARF_PROB_THRESHOLD},
                    'wd_catalog_pwd', wd_catalog_pwd,
                    'wd_catalog_pwd_threshold', {white_dwarf_catalog_pwd_threshold},
                    'wd_catalog_name', wd_catalog_name
                  )
                when greatest(
                  coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
                  coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
                ) >= {WHITE_DWARF_PROB_THRESHOLD}
                  then json_object(
                    'method', 'gaia_classprob',
                    'classprob_dsc_combmod_whitedwarf', classprob_dsc_combmod_whitedwarf,
                    'classprob_dsc_specmod_whitedwarf', classprob_dsc_specmod_whitedwarf,
                    'gaia_classprob_threshold', {WHITE_DWARF_PROB_THRESHOLD}
                  )
                else json_object(
                  'method', 'gaia_edr3_white_dwarf',
                  'wd_catalog_pwd', wd_catalog_pwd,
                  'wd_catalog_pwd_threshold', {white_dwarf_catalog_pwd_threshold},
                  'wd_catalog_name', wd_catalog_name
                )
              end
            where source_catalog = 'athyg'
              and source_version = ? and ingested_at = ? and transform_version = ?
              and (
                greatest(
                  coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
                  coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
                ) >= {WHITE_DWARF_PROB_THRESHOLD}
                or coalesce(wd_catalog_pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
              )
            """,
            [ATHYG_ALIAS_VERSION, ingested_at, transform_version],
        )

        con.execute(
            f"""
            create table identifier_quarantine as
            select
              row_number() over (order by source_pk, reason)::bigint as quarantine_id,
              'athyg' as source_catalog,
              {sql_literal(ATHYG_ALIAS_VERSION)} as source_version,
              source_pk,
              gaia_id,
              hip_id,
              hd_id,
              reason,
              details_json,
              {sql_literal(ingested_at)} as created_at
            from athyg_merge_quarantine
            """
        )
        con.execute(
            f"""
            create table object_identifiers as
            with canonical_seed as (
              select
                'star' as target_type,
                s.star_id as target_id,
                'gaia_dr3' as namespace,
                s.gaia_id::varchar as id_value_raw,
                true as is_canonical,
                'canonical_column' as resolution_method,
                1.0 as resolution_confidence,
                s.source_catalog as source_catalog,
                s.source_version as source_version,
                s.source_pk as source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk) as evidence_json
              from stars s
              where s.gaia_id is not null
              union all
              select 'star', s.star_id, 'hip', s.hip_id::varchar, true, 'canonical_column', 1.0, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where s.hip_id is not null
              union all
              select 'star', s.star_id, 'hd', s.hd_id::varchar, true, 'canonical_column', 1.0, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where s.hd_id is not null
              union all
              select 'star', s.star_id, 'hr', cast(json_extract_string(s.catalog_ids_json, '$.hr') as varchar), false, 'catalog_json', 0.95, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where json_extract_string(s.catalog_ids_json, '$.hr') is not null
              union all
              select 'star', s.star_id, 'gl', json_extract_string(s.catalog_ids_json, '$.gl'), false, 'catalog_json', 0.95, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where json_extract_string(s.catalog_ids_json, '$.gl') is not null
              union all
              select 'star', s.star_id, 'tyc', json_extract_string(s.catalog_ids_json, '$.tyc'), false, 'catalog_json', 0.95, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where json_extract_string(s.catalog_ids_json, '$.tyc') is not null
              union all
              select 'star', s.star_id, 'hyg', cast(json_extract_string(s.catalog_ids_json, '$.hyg') as varchar), false, 'catalog_json', 0.95, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where json_extract_string(s.catalog_ids_json, '$.hyg') is not null
              union all
              select 'star', s.star_id, 'wds', s.wds_id, false, 'catalog_json', 0.95, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where s.wds_id is not null
            ), remap_seed as (
              select
                'star' as target_type,
                r.matched_star_id as target_id,
                'gaia_legacy' as namespace,
                a.gaia_id::varchar as id_value_raw,
                false as is_canonical,
                r.resolution_method as resolution_method,
                r.resolution_confidence as resolution_confidence,
                'athyg' as source_catalog,
                {sql_literal(ATHYG_ALIAS_VERSION)} as source_version,
                a.source_pk as source_pk,
                json_object(
                  'athyg_source_pk', a.source_pk,
                  'resolution_method', r.resolution_method,
                  'resolution_confidence', r.resolution_confidence
                ) as evidence_json
              from athyg_merge_resolution r
              join athyg_merge_source a on a.source_pk = r.source_pk
              join stars s on s.star_id = r.matched_star_id
              where r.resolution_method like 'gaia_remap_%'
                and a.gaia_id is not null
                and (s.gaia_id is null or s.gaia_id <> a.gaia_id)
            ), combined as (
              select * from canonical_seed
              union all
              select * from remap_seed
            ), normalized as (
              select
                *,
                lower(trim(regexp_replace(regexp_replace(id_value_raw, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) as id_value_norm
              from combined
              where id_value_raw is not null and trim(id_value_raw) <> ''
            ), ranked as (
              select
                *,
                row_number() over (
                  partition by target_type, target_id, namespace, id_value_norm
                  order by is_canonical desc, resolution_confidence desc, source_pk asc
                ) as rn
              from normalized
            )
            select
              row_number() over (order by target_type, target_id, namespace, id_value_norm)::bigint as identifier_id,
              target_type,
              target_id,
              namespace,
              id_value_raw,
              id_value_norm,
              is_canonical,
              resolution_method,
              resolution_confidence,
              source_catalog,
              source_version,
              source_pk,
              evidence_json
            from ranked
            where rn = 1
            """
        )

        athyg_merge_existing_match_count = con.execute(
            "select count(*) from athyg_merge_resolution"
        ).fetchone()[0]
        athyg_merge_insert_count = con.execute(
            "select count(*) from athyg_merge_unmatched"
        ).fetchone()[0]
        athyg_merge_quarantine_count = con.execute(
            "select count(*) from athyg_merge_quarantine"
        ).fetchone()[0]
        athyg_merge_unresolved_count = athyg_merge_insert_count
        athyg_merge_direct_gaia_count = con.execute(
            "select count(*) from athyg_merge_resolution where resolution_method = 'gaia_exact'"
        ).fetchone()[0]
        athyg_merge_remap_count = con.execute(
            "select count(*) from athyg_merge_resolution where resolution_method like 'gaia_remap_%'"
        ).fetchone()[0]
        athyg_merge_direct_legacy_id_count = con.execute(
            "select count(*) from athyg_merge_resolution where resolution_method in ('hip_exact', 'hd_exact')"
        ).fetchone()[0]
        athyg_merge_positional_count = con.execute(
            "select count(*) from athyg_merge_resolution where resolution_method like 'positional_%'"
        ).fetchone()[0]
        athyg_merge_positional_ambiguous_count = con.execute(
            "select count(*) from athyg_merge_positional_quarantine"
        ).fetchone()[0]
        object_identifier_count = con.execute("select count(*) from object_identifiers").fetchone()[0]
        identifier_quarantine_count = con.execute("select count(*) from identifier_quarantine").fetchone()[0]
        identifier_gaia_collision_count = con.execute(
            """
            select count(*)
            from (
              select id_value_norm
              from object_identifiers
              where namespace = 'gaia_dr3'
              group by id_value_norm
              having count(distinct target_id) > 1
            ) t
            """
        ).fetchone()[0]
        identifier_hip_collision_count = con.execute(
            """
            select count(*)
            from (
              select id_value_norm
              from object_identifiers
              where namespace = 'hip'
              group by id_value_norm
              having count(distinct target_id) > 1
            ) t
            """
        ).fetchone()[0]
        identifier_hd_collision_count = con.execute(
            """
            select count(*)
            from (
              select id_value_norm
              from object_identifiers
              where namespace = 'hd'
              group by id_value_norm
              having count(distinct target_id) > 1
            ) t
            """
        ).fetchone()[0]
        log(
            "Identifier merge: "
            f"resolved={format_count(athyg_merge_existing_match_count)} "
            f"inserted={format_count(athyg_merge_insert_count)} "
            f"quarantined={format_count(athyg_merge_quarantine_count)} "
            f"identifiers={format_count(object_identifier_count)}"
        )
    else:
        con.execute(
            """
            create table identifier_quarantine as
            select *
            from (
              values
                (
                  cast(null as bigint), cast(null as varchar), cast(null as varchar), cast(null as bigint),
                  cast(null as bigint), cast(null as bigint), cast(null as bigint), cast(null as varchar),
                  cast(null as varchar), cast(null as varchar)
                )
            ) as t(
              quarantine_id, source_catalog, source_version, source_pk, gaia_id, hip_id, hd_id, reason, details_json, created_at
            )
            where false
            """
        )
        con.execute(
            """
            create table object_identifiers as
            with canonical_seed as (
              select
                'star' as target_type,
                s.star_id as target_id,
                'gaia_dr3' as namespace,
                s.gaia_id::varchar as id_value_raw,
                true as is_canonical,
                'canonical_column' as resolution_method,
                1.0 as resolution_confidence,
                s.source_catalog as source_catalog,
                s.source_version as source_version,
                s.source_pk as source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk) as evidence_json
              from stars s
              where s.gaia_id is not null
              union all
              select 'star', s.star_id, 'hip', s.hip_id::varchar, true, 'canonical_column', 1.0, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where s.hip_id is not null
              union all
              select 'star', s.star_id, 'hd', s.hd_id::varchar, true, 'canonical_column', 1.0, s.source_catalog, s.source_version, s.source_pk,
                json_object('source_catalog', s.source_catalog, 'source_pk', s.source_pk)
              from stars s
              where s.hd_id is not null
            ), normalized as (
              select
                *,
                lower(trim(regexp_replace(regexp_replace(id_value_raw, '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) as id_value_norm
              from canonical_seed
              where id_value_raw is not null and trim(id_value_raw) <> ''
            ), ranked as (
              select
                *,
                row_number() over (
                  partition by target_type, target_id, namespace, id_value_norm
                  order by is_canonical desc, resolution_confidence desc, source_pk asc
                ) as rn
              from normalized
            )
            select
              row_number() over (order by target_type, target_id, namespace, id_value_norm)::bigint as identifier_id,
              target_type,
              target_id,
              namespace,
              id_value_raw,
              id_value_norm,
              is_canonical,
              resolution_method,
              resolution_confidence,
              source_catalog,
              source_version,
              source_pk,
              evidence_json
            from ranked
            where rn = 1
            """
        )
        object_identifier_count = con.execute("select count(*) from object_identifiers").fetchone()[0]
        identifier_quarantine_count = con.execute("select count(*) from identifier_quarantine").fetchone()[0]
        identifier_gaia_collision_count = con.execute(
            """
            select count(*)
            from (
              select id_value_norm
              from object_identifiers
              where namespace = 'gaia_dr3'
              group by id_value_norm
              having count(distinct target_id) > 1
            ) t
            """
        ).fetchone()[0]
        identifier_hip_collision_count = con.execute(
            """
            select count(*)
            from (
              select id_value_norm
              from object_identifiers
              where namespace = 'hip'
              group by id_value_norm
              having count(distinct target_id) > 1
            ) t
            """
        ).fetchone()[0]
        identifier_hd_collision_count = con.execute(
            """
            select count(*)
            from (
              select id_value_norm
              from object_identifiers
              where namespace = 'hd'
              group by id_value_norm
              having count(distinct target_id) > 1
            ) t
            """
        ).fetchone()[0]

    slice_output_post_merge_star_count = con.execute("select count(*) from stars").fetchone()[0]
    if slice_output_post_merge_star_count != slice_output_star_count:
        slice_policy_report["counts"]["retained_star_rows_post_merge"] = int(
            slice_output_post_merge_star_count
        )
        slice_policy_report["counts"]["athyg_supplement_inserted_rows"] = int(
            max(slice_output_post_merge_star_count - slice_output_star_count, 0)
        )
    else:
        slice_policy_report["counts"]["retained_star_rows_post_merge"] = int(
            slice_output_post_merge_star_count
        )
        slice_policy_report["counts"]["athyg_supplement_inserted_rows"] = 0
    write_json(reports_dir / "slice_policy_report.json", slice_policy_report)

    alias_total_count = 0
    alias_system_count = 0
    alias_star_count = 0

    stage_totals["stars"] = con.execute("select count(*) from stars").fetchone()[0]
    log_stage_complete(
        "Stars stage",
        stars_stage_started,
        stage_totals,
        extra=(
            f"alias_crosswalk_candidates={format_count(alias_crosswalk_candidate_count)}, "
            f"alias_crosswalk_matched_stars={format_count(alias_crosswalk_matched_star_count)}, "
            f"alias_name_overrides={format_count(alias_name_override_count)}, "
            f"athyg_merge_resolved={format_count(athyg_merge_existing_match_count)}, "
            f"athyg_merge_inserted={format_count(athyg_merge_insert_count)}, "
            f"athyg_merge_quarantined={format_count(athyg_merge_quarantine_count)}"
        ),
    )

    # System grouping: WDS first, then name-root, then optional proximity for remaining stars.
    system_grouping_stage_started = time.monotonic()
    log("System grouping: WDS pass")
    con.execute(
        f"""
        create temp table wds_identifier_bridge as
        with raw_candidates as (
          select
            s.star_id,
            w.wds_id,
            case
              when s.hip_id is not null and s.hip_id = w.hip_id and s.hd_id is not null and s.hd_id = w.hd_id then 0
              when s.hip_id is not null and s.hip_id = w.hip_id then 1
              when s.hd_id is not null and s.hd_id = w.hd_id then 2
              else 9
            end as match_rank,
            abs(s.dist_ly - w.dist_ly) as dist_delta_ly,
            degrees(acos(
              least(
                1.0,
                greatest(
                  -1.0,
                  sin(radians(s.dec_deg)) * sin(radians(w.dec_deg)) +
                  cos(radians(s.dec_deg)) * cos(radians(w.dec_deg)) *
                    cos(radians(least(abs(s.ra_deg - w.ra_deg), 360.0 - abs(s.ra_deg - w.ra_deg))))
                )
              )
            )) * 3600.0 as ang_sep_arcsec
          from stars s
          join stars w
            on w.wds_id is not null
           and s.wds_id is null
           and (
             (s.hip_id is not null and w.hip_id = s.hip_id)
             or (s.hd_id is not null and w.hd_id = s.hd_id)
           )
        ), per_wds as (
          select
            star_id,
            wds_id,
            min(match_rank) as match_rank,
            min(coalesce(dist_delta_ly, 1e18)) as dist_delta_ly,
            min(coalesce(ang_sep_arcsec, 1e18)) as ang_sep_arcsec
          from raw_candidates
          where match_rank < 9
          group by star_id, wds_id
        ), filtered as (
          select *
          from per_wds
          where match_rank = 0
             or (
               match_rank in (1, 2)
               and ang_sep_arcsec <= {WDS_IDENTIFIER_BRIDGE_MAX_ANG_ARCSEC}
               and dist_delta_ly <= {WDS_IDENTIFIER_BRIDGE_MAX_DIST_DELTA_LY}
             )
        ), ranked as (
          select
            star_id,
            wds_id,
            match_rank,
            dist_delta_ly,
            ang_sep_arcsec,
            row_number() over (
              partition by star_id
              order by match_rank asc, dist_delta_ly asc, ang_sep_arcsec asc, wds_id asc
            ) as rn,
            count(*) over (
              partition by star_id, match_rank
            ) as best_rank_count
          from filtered
        )
        select
          star_id,
          'wds:' || wds_id as system_group_key
        from ranked
        where rn = 1 and best_rank_count = 1
        """
    )
    wds_identifier_bridge_count = con.execute(
        "select count(*) from wds_identifier_bridge"
    ).fetchone()[0]
    con.execute(
        """
        create temp table wds_groups as
        select star_id, 'wds:' || wds_id as system_group_key
        from stars
        where wds_id is not null

        union all

        select star_id, system_group_key
        from wds_identifier_bridge
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
        where wds_id is null
          and system_name_root_norm is not null
          and not exists (
            select 1
            from wds_identifier_bridge b
            where b.star_id = stars.star_id
          )
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
        f"(total={total_stars}, wds_grouped={wds_grouped}, wds_bridged={wds_identifier_bridge_count}, "
        f"name_grouped={name_grouped}, proximity_eligible={prox_eligible})"
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
            where wds_id is null
              and system_name_root_norm is null
              and not exists (
                select 1
                from wds_identifier_bridge b
                where b.star_id = stars.star_id
              )
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
            from (
              select star_id
              from stars
              where wds_id is null
                and system_name_root_norm is null
                and not exists (
                  select 1
                  from wds_identifier_bridge b
                  where b.star_id = stars.star_id
                )
            ) u
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
          select
            g.system_group_key,
            s.wds_id,
            coalesce(s.has_gaia_nss_evidence, false) as has_gaia_nss_evidence,
            coalesce(s.has_msc_evidence, false) as has_msc_evidence,
            coalesce(s.has_wds_evidence, false) as has_wds_evidence,
            coalesce(s.has_orb6_evidence, false) as has_orb6_evidence,
            s.sbx_sn
          from system_groups g
          join stars s using (star_id)
        ), aggregated as (
          select
            system_group_key,
            max(g.wds_id) as wds_id,
            max(case when g.has_msc_evidence then 1 else 0 end) as has_msc_insert,
            max(case when g.has_gaia_nss_evidence then 1 else 0 end) as has_gaia_nss,
            max(case when sbx_sn is not null then 1 else 0 end) as has_sbx_evidence,
            max(case when g.has_wds_evidence then 1 else 0 end) as has_wds_evidence,
            max(case when g.has_orb6_evidence then 1 else 0 end) as has_orb6_evidence
          from grouped g
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
          has_sbx_evidence = 1 as has_sbx_evidence,
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
            when (
              case
                when system_group_key like 'wds:%' and has_msc_insert = 1 and has_wds_evidence = 1 and has_orb6_evidence = 1 then 0.99
                when system_group_key like 'wds:%' and has_msc_insert = 1 and (has_wds_evidence = 1 or has_orb6_evidence = 1) then 0.97
                when system_group_key like 'wds:%' and has_msc_insert = 1 then 0.95
                when system_group_key like 'wds:%' then 0.90
                when system_group_key like 'name:%' then 0.80
                when system_group_key like 'prox:%' then 0.65
                else 1.0
              end
            ) >= 0.95 then 'high'
            when (
              case
                when system_group_key like 'wds:%' and has_msc_insert = 1 and has_wds_evidence = 1 and has_orb6_evidence = 1 then 0.99
                when system_group_key like 'wds:%' and has_msc_insert = 1 and (has_wds_evidence = 1 or has_orb6_evidence = 1) then 0.97
                when system_group_key like 'wds:%' and has_msc_insert = 1 then 0.95
                when system_group_key like 'wds:%' then 0.90
                when system_group_key like 'name:%' then 0.80
                when system_group_key like 'prox:%' then 0.65
                else 1.0
              end
            ) >= 0.80 then 'medium'
            when (
              case
                when system_group_key like 'wds:%' and has_msc_insert = 1 and has_wds_evidence = 1 and has_orb6_evidence = 1 then 0.99
                when system_group_key like 'wds:%' and has_msc_insert = 1 and (has_wds_evidence = 1 or has_orb6_evidence = 1) then 0.97
                when system_group_key like 'wds:%' and has_msc_insert = 1 then 0.95
                when system_group_key like 'wds:%' then 0.90
                when system_group_key like 'name:%' then 0.80
                when system_group_key like 'prox:%' then 0.65
                else 1.0
              end
            ) >= 0.60 then 'low'
            else 'illustrative'
          end as grouping_confidence_tier,
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
                 sg.grouping_confidence_tier,
                 sg.has_gaia_nss_evidence, sg.has_msc_evidence, sg.has_wds_evidence, sg.has_orb6_evidence, sg.grouping_source_catalogs_json
                 , sg.has_sbx_evidence
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
          grouping_confidence_tier,
          grouping_source_catalogs_json,
          has_gaia_nss_evidence,
          has_msc_evidence,
          has_sbx_evidence,
          has_wds_evidence,
          has_orb6_evidence,
          0::bigint as star_count,
          0::bigint as planet_count,
          '[]'::varchar as spectral_classes_json,
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
    con.execute(
        """
        update systems s
        set
          star_count = a.star_count,
          spectral_classes_json = a.spectral_classes_json
        from (
          select
            system_id,
            count(*)::bigint as star_count,
            coalesce(
              to_json(list_sort(list_distinct(list(spectral_class) filter (where spectral_class is not null)))),
              '[]'
            ) as spectral_classes_json
          from stars
          group by system_id
        ) a
        where s.system_id = a.system_id
        """
    )

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
    sbx_star_count = con.execute(
        "select count(*) from stars where sbx_sn is not null"
    ).fetchone()[0]
    sbx_system_count = con.execute(
        "select count(distinct system_id) from stars where sbx_sn is not null"
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
        "sbx_enabled": enable_sbx,
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
        "sbx_star_count": sbx_star_count,
        "sbx_system_count": sbx_system_count,
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
                "SBX spectroscopic-binary evidence is active in this build."
                if enable_sbx
                else "SBX spectroscopic-binary evidence is disabled (SPACEGATE_ENABLE_SBX=0)."
            ),
            "MSC matching is conservative in this pass: exact HIP/HD matches only; unmatched MSC components are inserted as new stars.",
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
    stage_totals["systems"] = int(system_counts[0] or 0)
    log_stage_complete(
        "System grouping stage",
        system_grouping_stage_started,
        stage_totals,
        extra=(
            f"multi_star_systems={format_count(system_counts[1])}, "
            f"max_component_size={format_count(system_counts[2])}"
        ),
    )

    sol_system_id: int | None = None
    sol_star_id: int | None = None
    if enable_sol_authority:
        sol_path = str(cooked_sol_authority).replace("'", "''")
        con.execute(
            f"""
            create or replace temp view sol_authority_raw as
            select * from read_csv_auto('{sol_path}',
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

        row = con.execute(
            """
            select system_id
            from systems
            where system_name_norm = 'sol'
            order by system_id
            limit 1
            """
        ).fetchone()
        if row:
            sol_system_id = int(row[0])
        else:
            sol_system_id = int(
                con.execute("select coalesce(max(system_id), 0) + 1::bigint from systems").fetchone()[0]
            )
            sol_system_hash = hashlib.sha256(
                f"system:sol|{sol_authority_version}|{transform_version}".encode("utf-8")
            ).hexdigest()
            con.execute(
                """
                insert into systems by name
                select
                  ?::bigint as system_id,
                  morton3d(0.0, 0.0, 0.0) as spatial_index,
                  'system:sol' as stable_object_key,
                  'Sol' as system_name,
                  'sol' as system_name_norm,
                  null::varchar as wds_id,
                  'authoritative' as grouping_basis,
                  1.0::double as grouping_confidence,
                  'high'::varchar as grouping_confidence_tier,
                  '["sol_authority"]'::varchar as grouping_source_catalogs_json,
                  false as has_gaia_nss_evidence,
                  false as has_msc_evidence,
                  false as has_sbx_evidence,
                  false as has_wds_evidence,
                  false as has_orb6_evidence,
                  1::bigint as star_count,
                  0::bigint as planet_count,
                  '["G"]'::varchar as spectral_classes_json,
                  null::double as ra_deg,
                  null::double as dec_deg,
                  0.0::double as dist_ly,
                  0.0::double as x_helio_ly,
                  0.0::double as y_helio_ly,
                  0.0::double as z_helio_ly,
                  null::double as x_gal_ly,
                  null::double as y_gal_ly,
                  null::double as z_gal_ly,
                  null::bigint as gaia_id,
                  null::bigint as hip_id,
                  null::bigint as hd_id,
                  'sol_authority'::varchar as source_catalog,
                  ?::varchar as source_version,
                  ?::varchar as source_url,
                  ?::varchar as source_download_url,
                  null::varchar as source_doi,
                  990000000001::bigint as source_pk,
                  990000000001::bigint as source_row_id,
                  ?::varchar as source_row_hash,
                  'NASA/JPL public domain'::varchar as license,
                  true as redistribution_ok,
                  'JPL Horizons authoritative Sol-system bootstrap'::varchar as license_note,
                  null::varchar as retrieval_etag,
                  ?::varchar as retrieval_checksum,
                  ?::varchar as retrieved_at,
                  ?::varchar as ingested_at,
                  ?::varchar as transform_version
                """,
                [
                    sol_system_id,
                    sol_authority_version,
                    sol_authority_url,
                    sol_authority_download_url,
                    sol_system_hash,
                    sol_authority_sha,
                    sol_authority_retrieved,
                    ingested_at,
                    transform_version,
                ],
            )

        row = con.execute(
            """
            select star_id
            from stars
            where star_name_norm = 'sun'
              and system_id = ?
            order by star_id
            limit 1
            """,
            [sol_system_id],
        ).fetchone()
        if row:
            sol_star_id = int(row[0])
        else:
            sol_star_id = int(
                con.execute("select coalesce(max(star_id), 0) + 1::bigint from stars").fetchone()[0]
            )
            sol_star_hash = hashlib.sha256(
                f"star:sol:sun|{sol_authority_version}|{transform_version}".encode("utf-8")
            ).hexdigest()
            con.execute(
                """
                insert into stars by name
                select
                  ?::bigint as star_id,
                  morton3d(0.0, 0.0, 0.0) as spatial_index,
                  ?::bigint as system_id,
                  'star:sol:sun' as stable_object_key,
                  'Sun' as star_name,
                  'sun' as star_name_norm,
                  'A'::varchar as component,
                  null::double as ra_deg,
                  null::double as dec_deg,
                  0.0::double as dist_ly,
                  null::double as parallax_mas,
                  null::double as parallax_error_mas,
                  null::double as parallax_over_error,
                  null::double as ruwe,
                  0.0::double as x_helio_ly,
                  0.0::double as y_helio_ly,
                  0.0::double as z_helio_ly,
                  null::double as x_gal_ly,
                  null::double as y_gal_ly,
                  null::double as z_gal_ly,
                  null::double as pm_ra_mas_yr,
                  null::double as pm_dec_mas_yr,
                  null::double as radial_velocity_kms,
                  'G2V'::varchar as spectral_type_raw,
                  'G'::varchar as spectral_class,
                  '2'::varchar as spectral_subtype,
                  'V'::varchar as luminosity_class,
                  null::varchar as spectral_peculiar,
                  null::double as vmag,
                  null::double as absmag,
                  null::double as color_index,
                  5772.0::double as teff_k,
                  null::bigint as gaia_id,
                  null::bigint as hip_id,
                  null::bigint as hd_id,
                  null::varchar as wds_id,
                  'sol_authority'::varchar as multiplicity_match_method,
                  1.0::double as multiplicity_match_confidence,
                  '["sol_authority"]'::varchar as multiplicity_source_catalogs_json,
                  false as gaia_non_single_star,
                  0::bigint as gaia_nss_solution_count,
                  '[]'::varchar as gaia_nss_solution_types_json,
                  null::double as gaia_nss_significance_max,
                  false as has_gaia_nss_evidence,
                  false as has_msc_evidence,
                  false as has_wds_evidence,
                  false as has_orb6_evidence,
                  null::bigint as sbx_sn,
                  0::bigint as sbx_orbit_count,
                  null::varchar as sbx_family,
                  null::varchar as sbx_position_epoch,
                  null::varchar as sbx_position_source,
                  '{"iau_name":"Sun","jpl_horizons_command":"10"}'::varchar as catalog_ids_json,
                  'sol_authority'::varchar as source_catalog,
                  ?::varchar as source_version,
                  ?::varchar as source_url,
                  ?::varchar as source_download_url,
                  null::varchar as source_doi,
                  990000000002::bigint as source_pk,
                  990000000002::bigint as source_row_id,
                  ?::varchar as source_row_hash,
                  'NASA/JPL public domain'::varchar as license,
                  true as redistribution_ok,
                  'JPL Horizons authoritative Sol-system bootstrap'::varchar as license_note,
                  null::varchar as retrieval_etag,
                  ?::varchar as retrieval_checksum,
                  ?::varchar as retrieved_at,
                  ?::varchar as ingested_at,
                  ?::varchar as transform_version,
                  'star'::varchar as object_family,
                  'main_sequence'::varchar as object_type,
                  '{"sol_authority":true}'::varchar as classification_evidence_json
                """,
                [
                    sol_star_id,
                    sol_system_id,
                    sol_authority_version,
                    sol_authority_url,
                    sol_authority_download_url,
                    sol_star_hash,
                    sol_authority_sha,
                    sol_authority_retrieved,
                    ingested_at,
                    transform_version,
                ],
            )
        log(
            f"Sol authority bootstrap: system_id={sol_system_id} star_id={sol_star_id}"
        )

    planet_catalog_delta_report: dict[str, object] = {
        "build_id": build_id,
        "lifecycle_enabled": bool(enable_exoplanet_lifecycle_catalogs),
    }
    planet_reclassification_report: dict[str, object] = {
        "build_id": build_id,
        "lifecycle_enabled": bool(enable_exoplanet_lifecycle_catalogs),
        "planet_classifier_version": planet_classifier_version,
    }

    # Planets
    planets_stage_started = time.monotonic()
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
        """,
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
    if enable_sol_authority and sol_system_id is not None and sol_star_id is not None:
        con.execute(
            """
            create or replace temp table sol_planet_input as
            with base as (
              select
                source_pk::bigint as source_pk,
                nullif(trim(object_name), '') as planet_name,
                lower(
                  trim(
                    regexp_replace(
                      regexp_replace(coalesce(object_name, ''), '[^0-9A-Za-z]+', ' ', 'g'),
                      '\\s+',
                      ' ',
                      'g'
                    )
                  )
                ) as planet_name_norm,
                lower(trim(coalesce(object_class, ''))) as object_class_norm,
                nullif(trim(orbital_period_days), '')::double as orbital_period_days,
                nullif(trim(semi_major_axis_au), '')::double as semi_major_axis_au,
                nullif(trim(eccentricity), '')::double as eccentricity,
                nullif(trim(inclination_deg), '')::double as inclination_deg,
                nullif(trim(radius_km), '')::double as radius_km,
                nullif(trim(mass_kg), '')::double as mass_kg,
                nullif(trim(horizons_query_url), '') as source_download_url,
                nullif(trim(source_row_hash), '') as source_row_hash
              from sol_authority_raw
            )
            select
              source_pk,
              planet_name,
              planet_name_norm,
              object_class_norm,
              orbital_period_days,
              semi_major_axis_au,
              eccentricity,
              inclination_deg,
              radius_km,
              mass_kg,
              source_download_url,
              source_row_hash
            from base
            where object_class_norm in ('planet', 'subplanet', 'dwarf_planet')
              and planet_name is not null
              and planet_name_norm is not null
              and planet_name_norm <> ''
            """
        )
        con.execute(
            """
            delete from planets p
            using sol_planet_input s
            where p.stable_object_key = 'planet:sol:' || s.planet_name_norm
            """
        )
        sol_planet_base_id = int(
            con.execute("select coalesce(max(planet_id), 0)::bigint from planets").fetchone()[0]
        )
        con.execute(
            f"""
            insert into planets by name
            select
              {sol_planet_base_id} + row_number() over (
                order by
                  case when s.object_class_norm = 'planet' then 0 else 1 end,
                  s.source_pk
              )::bigint as planet_id,
              morton3d(0.0, 0.0, 0.0) as spatial_index,
              'planet:sol:' || s.planet_name_norm as stable_object_key,
              {int(sol_system_id)}::bigint as system_id,
              {int(sol_star_id)}::bigint as star_id,
              s.planet_name as planet_name,
              s.planet_name_norm as planet_name_norm,
              null::int as disc_year,
              'historical_observation'::varchar as discovery_method,
              'Solar System'::varchar as discovery_facility,
              null::varchar as discovery_telescope,
              null::varchar as discovery_instrument,
              s.orbital_period_days,
              s.semi_major_axis_au,
              s.eccentricity,
              s.inclination_deg,
              case
                when s.radius_km is not null then s.radius_km / 71492.0
                else null
              end as radius_jup,
              case
                when s.radius_km is not null then s.radius_km / {EARTH_RADIUS_KM}
                else null
              end as radius_earth,
              case
                when s.mass_kg is not null then s.mass_kg / {EARTH_MASS_KG}
                else null
              end as mass_earth,
              case
                when s.mass_kg is not null then s.mass_kg / (317.8 * {EARTH_MASS_KG})
                else null
              end as mass_jup,
              null::double as eq_temp_k,
              null::double as insol_earth,
              0.0::double as host_metallicity_feh,
              null::double as host_metallicity_feh_error,
              'Sun'::varchar as host_name_raw,
              'sun'::varchar as host_name_norm,
              null::bigint as host_gaia_id,
              null::bigint as host_hip_id,
              null::bigint as host_hd_id,
              'sol_authority'::varchar as match_method,
              1.0::double as match_confidence,
              'authoritative Sol-system bootstrap'::varchar as match_notes,
              0.0::double as x_helio_ly,
              0.0::double as y_helio_ly,
              0.0::double as z_helio_ly,
              'sol_authority'::varchar as source_catalog,
              {sql_literal(sol_authority_version)}::varchar as source_version,
              {sql_literal(sol_authority_url)}::varchar as source_url,
              coalesce(s.source_download_url, {sql_literal(sol_authority_download_url)}::varchar) as source_download_url,
              null::varchar as source_doi,
              s.source_pk::bigint as source_pk,
              s.source_pk::bigint as source_row_id,
              s.source_row_hash::varchar as source_row_hash,
              'NASA/JPL public domain'::varchar as license,
              true as redistribution_ok,
              'JPL Horizons authoritative Sol-system bootstrap'::varchar as license_note,
              null::varchar as retrieval_etag,
              {sql_literal(sol_authority_sha)}::varchar as retrieval_checksum,
              {sql_literal(sol_authority_retrieved)}::varchar as retrieved_at,
              {sql_literal(ingested_at)}::varchar as ingested_at,
              {sql_literal(transform_version)}::varchar as transform_version,
              'confirmed'::varchar as planet_status,
              true as is_default_visible,
              false as is_tombstoned,
              'sol_authority'::varchar as status_source_catalog,
              {sql_literal(ingested_at)}::varchar as status_updated_at,
              null::varchar as status_superseded_by,
              case
                when s.object_class_norm in ('subplanet', 'dwarf_planet') then 'subplanet'
                else 'major_planet'
              end as planet_size_mass_class,
              null::varchar as planet_insolation_class,
              case
                when s.object_class_norm in ('subplanet', 'dwarf_planet') then 'solar_subplanet'
                else 'solar_planet'
              end as planet_orbit_class,
              null::varchar as planet_composition_proxy_class,
              '[]'::varchar as planet_detection_tags_json,
              case
                when s.object_class_norm in ('subplanet', 'dwarf_planet') then '["solar_system","subplanet","dwarf_planet"]'
                else '["solar_system","major_planet"]'
              end::varchar as planet_host_context_tags_json,
              {sql_literal(planet_classifier_version)}::varchar as planet_classifier_version,
              {sql_literal(ingested_at)}::varchar as planet_classifier_updated_at,
              null::double as spacegate_hab_score,
              null::double as spacegate_hab_confidence,
              null::varchar as spacegate_hab_reasons_json,
              0.5::double as planet_element_richness_score,
              'moderate'::varchar as planet_element_richness_class,
              'sol_authority_default'::varchar as planet_element_richness_method,
              'solar-system bootstrap default'::varchar as planet_element_richness_notes
            from sol_planet_input s
            """
        )
        log("Sol authority planets injected into planets table")

    lifecycle_status_raw_rows = 0
    lifecycle_status_matched_rows = 0
    lifecycle_alias_rows = 0
    lifecycle_alias_matched_rows = 0
    lifecycle_alias_matched_planets = 0
    lifecycle_features_raw_rows = 0
    lifecycle_stale_classifier_rows = 0
    if enable_exoplanet_lifecycle_catalogs:
        lifecycle_stage_started = time.monotonic()
        log("Applying exoplanet lifecycle overlays")
        lifecycle_status_path = str(cooked_exoplanet_lifecycle_status).replace("'", "''")
        lifecycle_alias_path = str(cooked_exoplanet_lifecycle_aliases).replace("'", "''")
        lifecycle_features_path = str(cooked_exoplanet_lifecycle_features).replace("'", "''")

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
            create or replace temp view exoplanet_lifecycle_alias_raw as
            select * from read_csv_auto('{lifecycle_alias_path}',
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
            con.execute("select count(*)::bigint from exoplanet_lifecycle_features_raw").fetchone()[0]
            or 0
        )
        lifecycle_alias_rows = int(
            con.execute("select count(*)::bigint from exoplanet_lifecycle_alias_raw").fetchone()[0] or 0
        )

        con.execute(
            """
            create or replace temp table lifecycle_alias_bridge as
            with alias_norm as (
              select
                lower(trim(regexp_replace(regexp_replace(coalesce(planet_name, ''), '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) as primary_norm,
                lower(trim(regexp_replace(regexp_replace(coalesce(alias_name, ''), '[^0-9A-Za-z]+', ' ', 'g'), '\\s+', ' ', 'g'))) as alias_norm
              from exoplanet_lifecycle_alias_raw
            ), pairs as (
              select alias_norm as source_name_norm, primary_norm as target_name_norm, 2 as match_rank, 'oec_alias_to_primary' as match_strategy
              from alias_norm
              where alias_norm <> '' and primary_norm <> '' and alias_norm <> primary_norm
              union all
              select primary_norm as source_name_norm, alias_norm as target_name_norm, 3 as match_rank, 'oec_primary_to_alias' as match_strategy
              from alias_norm
              where alias_norm <> '' and primary_norm <> '' and alias_norm <> primary_norm
            )
            select source_name_norm, target_name_norm, match_rank, match_strategy
            from pairs
            qualify row_number() over (
              partition by source_name_norm, target_name_norm
              order by match_rank asc, match_strategy asc
            ) = 1
            """
        )

        con.execute(
            """
            create or replace temp table planet_status_matches as
            with status_norm as (
              select
                row_number() over () as status_row_id,
                lower(trim(coalesce(source_catalog, 'unknown'))) as source_catalog,
                coalesce(source_version, '') as source_version,
                coalesce(source_pk, '') as source_pk,
                lower(trim(coalesce(observed_status, ''))) as observed_status,
                coalesce(source_row_hash, '') as source_row_hash,
                coalesce(observed_at, '') as observed_at,
                coalesce(notes, '') as notes,
                lower(trim(coalesce(planet_name_norm, ''))) as source_name_norm
              from exoplanet_lifecycle_status_raw
              where lower(trim(coalesce(observed_status, ''))) in ('confirmed','candidate','controversial','retracted')
            ), status_names as (
              select distinct source_name_norm
              from status_norm
              where source_name_norm <> ''
            ), status_name_targets as (
              select
                n.source_name_norm,
                n.source_name_norm as target_name_norm,
                1 as match_rank,
                'direct' as match_strategy
              from status_names n
              union all
              select
                n.source_name_norm,
                b.target_name_norm,
                b.match_rank,
                b.match_strategy
              from status_names n
              join lifecycle_alias_bridge b
                on b.source_name_norm = n.source_name_norm
            ), matches_raw as (
              select
                p.planet_id,
                p.stable_object_key,
                p.planet_name,
                p.planet_name_norm,
                s.status_row_id,
                s.source_catalog,
                s.source_version,
                s.source_pk,
                s.observed_status,
                s.source_row_hash,
                s.observed_at,
                case
                  when t.match_strategy = 'direct' then s.notes
                  when s.notes = '' then 'match=' || t.match_strategy
                  else s.notes || '; match=' || t.match_strategy
                end as notes,
                t.match_strategy,
                row_number() over (
                  partition by s.status_row_id, p.planet_id
                  order by t.match_rank asc
                ) as rn
              from status_norm s
              join status_name_targets t
                on t.source_name_norm = s.source_name_norm
              join planets p
                on p.planet_name_norm is not null
               and p.planet_name_norm = t.target_name_norm
            )
            select
              planet_id,
              stable_object_key,
              planet_name,
              planet_name_norm,
              source_catalog,
              source_version,
              source_pk,
              observed_status,
              source_row_hash,
              observed_at,
              notes,
              match_strategy
            from matches_raw
            where rn = 1
            """
        )

        lifecycle_status_matched_rows = int(
            con.execute("select count(*)::bigint from planet_status_matches").fetchone()[0]
            or 0
        )
        lifecycle_alias_matched_rows = int(
            con.execute(
                """
                select count(*)::bigint
                from planet_status_matches
                where match_strategy <> 'direct'
                """
            ).fetchone()[0]
            or 0
        )
        lifecycle_alias_matched_planets = int(
            con.execute(
                """
                select count(distinct stable_object_key)::bigint
                from planet_status_matches
                where match_strategy <> 'direct'
                """
            ).fetchone()[0]
            or 0
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
            with hwc_feature_norm as (
              select
                lower(trim(coalesce(planet_name_norm, ''))) as source_name_norm,
                nullif(hwc_p_habitable, '')::double as hwc_p_habitable,
                nullif(hwc_esi, '')::double as hwc_esi
              from exoplanet_lifecycle_features_raw
              where lower(trim(coalesce(source_catalog, ''))) = 'hwc'
            ), hwc_feature_targets as (
              select
                source_name_norm as planet_name_norm,
                hwc_p_habitable,
                hwc_esi
              from hwc_feature_norm
              where source_name_norm <> ''
              union all
              select
                b.target_name_norm as planet_name_norm,
                f.hwc_p_habitable,
                f.hwc_esi
              from hwc_feature_norm f
              join lifecycle_alias_bridge b
                on b.source_name_norm = f.source_name_norm
            )
            select
              planet_name_norm,
              max(hwc_p_habitable) as hwc_p_habitable,
              max(hwc_esi) as hwc_esi
            from hwc_feature_targets
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

        previous_build_id, previous_core_db = resolve_served_current_core(state_dir)
        prev_planet_status_expr = "'confirmed'"
        prev_classifier_expr = "null::varchar"
        if previous_core_db and previous_build_id != build_id:
            previous_core_db_sql = str(previous_core_db).replace("'", "''")
            con.execute(f"attach '{previous_core_db_sql}' as prev_build (read_only)")
            prev_cols = {
                str(row[1]).lower()
                for row in con.execute("select * from pragma_table_info('prev_build.planets')").fetchall()
            }
            if "planet_status" in prev_cols:
                prev_planet_status_expr = "coalesce(planet_status, 'confirmed')"
            if "planet_classifier_version" in prev_cols:
                prev_classifier_expr = "planet_classifier_version"
            con.execute(
                f"""
                create or replace temp table prev_planet_state as
                select
                  stable_object_key,
                  {prev_planet_status_expr} as previous_status,
                  {prev_classifier_expr} as previous_classifier_version
                from prev_build.planets
                """
            )
            con.execute("detach prev_build")
        else:
            con.execute(
                """
                create or replace temp table prev_planet_state as
                select
                  cast(null as varchar) as stable_object_key,
                  cast(null as varchar) as previous_status,
                  cast(null as varchar) as previous_classifier_version
                where false
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
                "Planet lifecycle classifier gate failed: "
                f"{lifecycle_stale_classifier_rows} rows are stale for classifier version {planet_classifier_version}."
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
        previous_build_id, _ = resolve_served_current_core(state_dir)
        planet_catalog_delta_report = {
            "build_id": build_id,
            "lifecycle_enabled": True,
            "previous_build_id": previous_build_id,
            "status_raw_rows": lifecycle_status_raw_rows,
            "status_matched_rows": lifecycle_status_matched_rows,
            "alias_raw_rows": lifecycle_alias_rows,
            "alias_matched_rows": lifecycle_alias_matched_rows,
            "alias_matched_planets": lifecycle_alias_matched_planets,
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

        log_stage_complete(
            "Exoplanet lifecycle stage",
            lifecycle_stage_started,
            stage_totals,
            extra=(
                f"status_rows={format_count(lifecycle_status_raw_rows)}, "
                f"matched={format_count(lifecycle_status_matched_rows)}, "
                f"alias_matched={format_count(lifecycle_alias_matched_rows)}, "
                f"reclassified={format_count(reclassified_rows)}"
            ),
        )
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

    host_name_star_promotions = 0
    host_name_system_promotions = 0
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
    con.execute(
        """
        update systems
        set planet_count = 0
        """
    )
    con.execute(
        """
        update systems s
        set planet_count = p.planet_count
        from (
          select
            system_id,
            count(*)::bigint as planet_count
          from planets
          where system_id is not null
          group by system_id
        ) p
        where s.system_id = p.system_id
        """
    )

    stage_totals["planets"] = con.execute("select count(*) from planets").fetchone()[0]
    matched_planet_count = con.execute(
        "select count(*) from planets where match_method <> 'unmatched'"
    ).fetchone()[0]
    log_stage_complete(
        "Planets stage",
        planets_stage_started,
        stage_totals,
        extra=(
            f"matched_planets={format_count(matched_planet_count)}, "
            f"host_star_promotions={format_count(host_name_star_promotions)}, "
            f"host_system_promotions={format_count(host_name_system_promotions)}"
        ),
    )

    aliases_stage_started = time.monotonic()
    log("Building alias tables")
    if enable_aliases:
        con.execute(
            """
            create or replace temp table athyg_alias_match_candidates as
            select
              a.source_pk,
              s.system_id,
              s.star_id,
              case
                when a.gaia_id is not null and s.gaia_id = a.gaia_id then 0
                when a.hip_id is not null and s.hip_id = a.hip_id and a.hd_id is not null and s.hd_id = a.hd_id then 1
                when a.hip_id is not null and s.hip_id = a.hip_id then 2
                when a.hd_id is not null and s.hd_id = a.hd_id then 3
                else 9
              end as match_rank
            from athyg_alias_candidates a
            join stars s
              on (
                (a.gaia_id is not null and s.gaia_id = a.gaia_id)
                or (a.hip_id is not null and s.hip_id = a.hip_id)
                or (a.hd_id is not null and s.hd_id = a.hd_id)
              )
            where a.source_pk is not null
            """
        )
        con.execute(
            """
            create or replace temp view athyg_alias_star_resolution as
            with ranked as (
              select
                source_pk,
                system_id,
                star_id,
                match_rank,
                row_number() over (
                  partition by source_pk
                  order by match_rank asc, system_id asc, star_id asc
                ) as rn,
                count(*) over (
                  partition by source_pk, match_rank
                ) as best_rank_count
              from athyg_alias_match_candidates
              where match_rank < 9
            )
            select
              source_pk,
              system_id,
              star_id,
              match_rank
            from ranked
            where rn = 1 and best_rank_count = 1
            """
        )
        con.execute(
            """
            create or replace temp view athyg_alias_system_resolution as
            with per_system as (
              select
                source_pk,
                system_id,
                min(match_rank) as match_rank
              from athyg_alias_match_candidates
              where match_rank < 9
              group by source_pk, system_id
            ), ranked as (
              select
                source_pk,
                system_id,
                match_rank,
                row_number() over (
                  partition by source_pk
                  order by match_rank asc, system_id asc
                ) as rn,
                count(*) over (
                  partition by source_pk, match_rank
                ) as best_rank_count
              from per_system
            )
            select
              source_pk,
              system_id,
              match_rank
            from ranked
            where rn = 1 and best_rank_count = 1
            """
        )
        con.execute(
            """
            create or replace temp view star_alias_seed as
            select
              'star' as target_type,
              s.star_id as target_id,
              s.system_id,
              s.star_id,
              b.display_name as alias_raw,
              'planet_host_name' as alias_kind,
              4 as alias_priority,
              'nasa_exoplanet_archive' as source_catalog,
              'pscomppars'::varchar as source_version,
              null::bigint as source_pk
            from stars s
            join planet_host_name_star_best b on b.star_id = s.star_id
            where b.display_name is not null
              and b.display_name <> ''

            union all

            select
              'star' as target_type,
              s.star_id as target_id,
              s.system_id,
              s.star_id,
              'HIP ' || s.hip_id::varchar as alias_raw,
              'hip_id' as alias_kind,
              5 as alias_priority,
              'athyg_crosswalk' as source_catalog,
              null::varchar as source_version,
              s.hip_id as source_pk
            from stars s
            where s.hip_id is not null

            union all

            select
              'star',
              s.star_id,
              s.system_id,
              s.star_id,
              'HD ' || s.hd_id::varchar as alias_raw,
              'hd_id',
              6,
              'athyg_crosswalk',
              null,
              s.hd_id
            from stars s
            where s.hd_id is not null

            union all

            select
              'star',
              s.star_id,
              s.system_id,
              s.star_id,
              'WDS ' || s.wds_id as alias_raw,
              'wds_id',
              7,
              'wds',
              null,
              null
            from stars s
            where s.wds_id is not null
            """
        )
        con.execute(
            """
            create or replace temp view star_alias_crosswalk_seed as
            select
              'star' as target_type,
              r.star_id as target_id,
              r.system_id,
              r.star_id,
              a.proper_name as alias_raw,
              'proper_name' as alias_kind,
              1 as alias_priority,
              'athyg_crosswalk' as source_catalog,
              'v3.3' as source_version,
              a.source_pk as source_pk
            from athyg_alias_candidates a
            join athyg_alias_star_resolution r on r.source_pk = a.source_pk
            where a.proper_name is not null

            union all

            select
              'star',
              r.star_id,
              r.system_id,
              r.star_id,
              a.bayer_name as alias_raw,
              'bayer_name',
              2,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from athyg_alias_candidates a
            join athyg_alias_star_resolution r on r.source_pk = a.source_pk
            where a.bayer_name is not null

            union all

            select
              'star',
              r.star_id,
              r.system_id,
              r.star_id,
              a.flam_name as alias_raw,
              'flamsteed_name',
              3,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from athyg_alias_candidates a
            join athyg_alias_star_resolution r on r.source_pk = a.source_pk
            where a.flam_name is not null

            union all

            select
              'star',
              r.star_id,
              r.system_id,
              r.star_id,
              'HR ' || a.hr_id::varchar as alias_raw,
              'hr_id',
              10,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from athyg_alias_candidates a
            join athyg_alias_star_resolution r on r.source_pk = a.source_pk
            where a.hr_id is not null

            union all

            select
              'star',
              r.star_id,
              r.system_id,
              r.star_id,
              a.gl_id as alias_raw,
              'gl_id',
              11,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from athyg_alias_candidates a
            join athyg_alias_star_resolution r on r.source_pk = a.source_pk
            where a.gl_id is not null

            union all

            select
              'star',
              r.star_id,
              r.system_id,
              r.star_id,
              'TYC ' || a.tyc_id as alias_raw,
              'tyc_id',
              12,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from athyg_alias_candidates a
            join athyg_alias_star_resolution r on r.source_pk = a.source_pk
            where a.tyc_id is not null

            union all

            select
              'star',
              r.star_id,
              r.system_id,
              r.star_id,
              'HYG ' || a.hyg_id::varchar as alias_raw,
              'hyg_id',
              13,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from athyg_alias_candidates a
            join athyg_alias_star_resolution r on r.source_pk = a.source_pk
            where a.hyg_id is not null
            """
        )
        con.execute(
            """
            create or replace temp view system_alias_seed as
            select
              'system' as target_type,
              s.system_id as target_id,
              s.system_id,
              null::bigint as star_id,
              b.display_name as alias_raw,
              'planet_host_name' as alias_kind,
              4 as alias_priority,
              'nasa_exoplanet_archive' as source_catalog,
              'pscomppars'::varchar as source_version,
              null::bigint as source_pk
            from systems s
            join planet_host_name_system_best b on b.system_id = s.system_id
            where b.display_name is not null
              and b.display_name <> ''

            union all

            select
              'system' as target_type,
              s.system_id as target_id,
              s.system_id,
              null::bigint as star_id,
              'WDS ' || s.wds_id as alias_raw,
              'wds_id' as alias_kind,
              2 as alias_priority,
              'wds' as source_catalog,
              null::varchar as source_version,
              null::bigint as source_pk
            from systems s
            where s.wds_id is not null

            union all

            select
              'system',
              r.system_id,
              r.system_id,
              null::bigint as star_id,
              a.proper_name as alias_raw,
              'member_proper_name' as alias_kind,
              21 as alias_priority,
              'athyg_crosswalk' as source_catalog,
              'v3.3' as source_version,
              a.source_pk as source_pk
            from athyg_alias_candidates a
            join athyg_alias_system_resolution r on r.source_pk = a.source_pk
            where a.proper_name is not null

            union all

            select
              'system',
              r.system_id,
              r.system_id,
              null::bigint as star_id,
              a.bayer_name as alias_raw,
              'member_bayer_name' as alias_kind,
              22 as alias_priority,
              'athyg_crosswalk' as source_catalog,
              'v3.3' as source_version,
              a.source_pk as source_pk
            from athyg_alias_candidates a
            join athyg_alias_system_resolution r on r.source_pk = a.source_pk
            where a.bayer_name is not null

            union all

            select
              'system',
              r.system_id,
              r.system_id,
              null::bigint as star_id,
              a.flam_name as alias_raw,
              'member_flamsteed_name' as alias_kind,
              23 as alias_priority,
              'athyg_crosswalk' as source_catalog,
              'v3.3' as source_version,
              a.source_pk as source_pk
            from athyg_alias_candidates a
            join athyg_alias_system_resolution r on r.source_pk = a.source_pk
            where a.flam_name is not null

            union all

            select
              'system',
              sa.system_id,
              sa.system_id,
              null::bigint as star_id,
              sa.alias_raw,
              'member_' || sa.alias_kind as alias_kind,
              sa.alias_priority + 20 as alias_priority,
              sa.source_catalog,
              sa.source_version,
              sa.source_pk
            from star_alias_crosswalk_seed sa
            where sa.alias_kind in ('proper_name', 'bayer_name', 'flamsteed_name')
              and sa.system_id is not null
              and sa.alias_raw is not null

            union all

            select
              'system',
              s.system_id,
              s.system_id,
              null::bigint as star_id,
              'HIP ' || s.hip_id::varchar as alias_raw,
              'hip_id',
              6,
              'athyg_crosswalk',
              null,
              s.hip_id
            from systems s
            where s.hip_id is not null

            union all

            select
              'system',
              s.system_id,
              s.system_id,
              null::bigint as star_id,
              'HD ' || s.hd_id::varchar as alias_raw,
              'hd_id',
              7,
              'athyg_crosswalk',
              null,
              s.hd_id
            from systems s
            where s.hd_id is not null
            """
        )
        con.execute(
            """
            create temp table aliases_star as
            with seed as (
              select * from star_alias_seed
              union all select * from star_alias_crosswalk_seed
            ), normalized as (
              select
                target_type,
                target_id,
                system_id,
                star_id,
                alias_raw,
                lower(
                  trim(
                    regexp_replace(
                      regexp_replace(alias_raw, '[^0-9A-Za-z]+', ' ', 'g'),
                      '\\s+',
                      ' ',
                      'g'
                    )
                  )
                ) as alias_norm,
                alias_kind,
                alias_priority,
                source_catalog,
                source_version,
                source_pk
              from seed
              where alias_raw is not null
            ), filtered as (
              select *
              from normalized
              where alias_norm is not null and alias_norm <> ''
            ), dedup as (
              select
                *,
                row_number() over (
                  partition by target_type, target_id, alias_norm
                  order by alias_priority asc, alias_kind asc, alias_raw asc
                ) as rn
              from filtered
            )
            select
              target_type,
              target_id,
              system_id,
              star_id,
              alias_raw,
              alias_norm,
              alias_kind,
              alias_priority,
              source_catalog,
              source_version,
              source_pk
            from dedup
            where rn = 1
            """
        )
        con.execute(
            """
            create temp table aliases_system as
            with seed as (
              select * from system_alias_seed
            ), normalized as (
              select
                target_type,
                target_id,
                system_id,
                star_id,
                alias_raw,
                lower(
                  trim(
                    regexp_replace(
                      regexp_replace(alias_raw, '[^0-9A-Za-z]+', ' ', 'g'),
                      '\\s+',
                      ' ',
                      'g'
                    )
                  )
                ) as alias_norm,
                alias_kind,
                alias_priority,
                source_catalog,
                source_version,
                source_pk
              from seed
              where alias_raw is not null
            ), filtered as (
              select *
              from normalized
              where alias_norm is not null and alias_norm <> ''
            ), dedup as (
              select
                *,
                row_number() over (
                  partition by target_type, target_id, alias_norm
                  order by alias_priority asc, alias_kind asc, alias_raw asc
                ) as rn
              from filtered
            )
            select
              target_type,
              target_id,
              system_id,
              star_id,
              alias_raw,
              alias_norm,
              alias_kind,
              alias_priority,
              source_catalog,
              source_version,
              source_pk
            from dedup
            where rn = 1
            """
        )
        con.execute(
            """
            create table aliases as
            with merged as (
              select * from aliases_star
              union all
              select * from aliases_system
            )
            select
              row_number() over ()::bigint as alias_id,
              target_type,
              target_id,
              system_id,
              star_id,
              alias_raw,
              alias_norm,
              alias_kind,
              alias_priority,
              alias_priority = 0 as is_primary,
              source_catalog,
              source_version,
              source_pk
            from merged
            """
        )
    else:
        con.execute(
            """
            create table aliases as
            select *
            from (
              values
                (
                  cast(null as bigint), cast(null as varchar), cast(null as bigint), cast(null as bigint),
                  cast(null as bigint), cast(null as varchar), cast(null as varchar), cast(null as varchar),
                  cast(null as integer), cast(null as boolean), cast(null as varchar), cast(null as varchar),
                  cast(null as bigint)
                )
            ) as t(
              alias_id, target_type, target_id, system_id, star_id, alias_raw, alias_norm, alias_kind,
              alias_priority, is_primary, source_catalog, source_version, source_pk
            )
            where false
            """
        )
    if enable_aliases and sol_system_id is not None:
        sol_alias_rows = [
            ("Sol", "sol_name", 0, "sol_authority"),
            ("Solar System", "solar_system", 1, "sol_authority"),
            ("Sun", "sun_name", 2, "sol_authority"),
        ]
        for alias_raw, alias_kind, alias_priority, source_catalog in sol_alias_rows:
            alias_norm = " ".join(
                "".join(ch if ch.isalnum() else " " for ch in alias_raw).lower().split()
            )
            exists = con.execute(
                """
                select 1
                from aliases
                where target_type = 'system'
                  and target_id = ?
                  and alias_norm = ?
                limit 1
                """,
                [sol_system_id, alias_norm],
            ).fetchone()
            if exists:
                continue
            next_alias_id = int(
                con.execute("select coalesce(max(alias_id), 0) + 1::bigint from aliases").fetchone()[0]
            )
            con.execute(
                """
                insert into aliases (
                  alias_id, target_type, target_id, system_id, star_id, alias_raw, alias_norm,
                  alias_kind, alias_priority, is_primary, source_catalog, source_version, source_pk
                )
                values (?, 'system', ?, ?, null, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    next_alias_id,
                    sol_system_id,
                    sol_system_id,
                    alias_raw,
                    alias_norm,
                    alias_kind,
                    alias_priority,
                    alias_priority == 0,
                    source_catalog,
                    sol_authority_version,
                    990000010000 + next_alias_id,
                ],
            )
    alias_total_count = con.execute("select count(*) from aliases").fetchone()[0]
    alias_system_count = con.execute(
        "select count(*) from aliases where target_type = 'system'"
    ).fetchone()[0]
    alias_star_count = con.execute(
        "select count(*) from aliases where target_type = 'star'"
    ).fetchone()[0]
    stage_totals["aliases"] = alias_total_count
    log_stage_complete(
        "Alias stage",
        aliases_stage_started,
        stage_totals,
        extra=(
            f"athyg_alias_candidates={format_count(alias_crosswalk_candidate_count)}, "
            f"athyg_alias_matched_stars={format_count(alias_crosswalk_matched_star_count)}"
        ),
    )

    science_side_stage_started = time.monotonic()
    log("Building compact and superstellar side tables")
    con.execute(
        f"""
        create or replace temp view compact_catalog_input as
        with atnf_norm as (
          select
            'atnf' as source_catalog,
            coalesce(nullif(psrj, ''), nullif(psrb, '')) as source_key,
            coalesce(nullif(psrj, ''), nullif(psrb, '')) as object_name,
            nullif(ra_deg, '')::double as ra_deg,
            nullif(dec_deg, '')::double as dec_deg,
            nullif(distance_pc, '')::double as distance_pc,
            nullif(parallax_mas, '')::double as parallax_mas,
            null::bigint as gaia_id,
            case
              when lower(coalesce(object_type, '')) like '%magnetar%' then 'magnetar'
              else 'pulsar'
            end as object_type,
            'neutron_star' as object_family,
            json_object(
              'psrj', nullif(psrj, ''),
              'psrb', nullif(psrb, ''),
              'type_raw', nullif(type_raw, ''),
              'assoc_raw', nullif(assoc_raw, '')
            ) as catalog_ids_json
          from atnf_raw
        ), magnetar_norm as (
          select
            'magnetar' as source_catalog,
            nullif(name, '') as source_key,
            nullif(name, '') as object_name,
            nullif(ra_deg, '')::double as ra_deg,
            nullif(dec_deg, '')::double as dec_deg,
            nullif(distance_pc, '')::double as distance_pc,
            null::double as parallax_mas,
            null::bigint as gaia_id,
            'magnetar' as object_type,
            'neutron_star' as object_family,
            json_object(
              'name', nullif(name, ''),
              'assoc_raw', nullif(assoc_raw, ''),
              'activity_raw', nullif(activity_raw, '')
            ) as catalog_ids_json
          from magnetar_raw
        ), white_dwarf_norm as (
          select
            'white_dwarf' as source_catalog,
            cast(w.gaia_id as varchar) as source_key,
            coalesce(
              nullif(w.wdj_name, ''),
              nullif(w.designation, ''),
              'Gaia DR3 ' || cast(w.gaia_id as varchar)
            ) as object_name,
            w.ra_deg as ra_deg,
            w.dec_deg as dec_deg,
            case
              when w.parallax_mas is not null and w.parallax_mas > 0.0 then 1000.0 / w.parallax_mas
              else null
            end as distance_pc,
            w.parallax_mas as parallax_mas,
            cast(w.gaia_id as bigint) as gaia_id,
            'white_dwarf' as object_type,
            'white_dwarf' as object_family,
            json_object(
              'gaia_id', cast(w.gaia_id as varchar),
              'wdj_name', nullif(w.wdj_name, ''),
              'designation', nullif(w.designation, ''),
              'pwd', w.pwd,
              'fit_model', nullif(w.fit_model, ''),
              'teff_best_k', w.teff_best_k,
              'logg_best_cgs', w.logg_best_cgs,
              'mass_best_msun', w.mass_best_msun
            ) as catalog_ids_json
          from white_dwarf_catalog w
          join stars s
            on s.gaia_id = w.gaia_id
          where w.gaia_id is not null
            and coalesce(w.pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
        )
        select
          source_catalog,
          source_key,
          object_name,
          ra_deg,
          dec_deg,
          distance_pc,
          distance_pc * {PC_TO_LY} as distance_ly,
          parallax_mas,
          gaia_id,
          object_type,
          object_family,
          catalog_ids_json
        from atnf_norm
        where source_key is not null
        union all
        select
          source_catalog,
          source_key,
          object_name,
          ra_deg,
          dec_deg,
          distance_pc,
          distance_pc * {PC_TO_LY} as distance_ly,
          parallax_mas,
          gaia_id,
          object_type,
          object_family,
          catalog_ids_json
        from magnetar_norm
        where source_key is not null
        union all
        select
          source_catalog,
          source_key,
          object_name,
          ra_deg,
          dec_deg,
          distance_pc,
          distance_pc * {PC_TO_LY} as distance_ly,
          parallax_mas,
          gaia_id,
          object_type,
          object_family,
          catalog_ids_json
        from white_dwarf_norm
        where source_key is not null
        """
    )
    con.execute(
        """
        create or replace temp view star_sky_bins as
        select
          star_id,
          system_id,
          ra_deg,
          dec_deg,
          dist_ly,
          cast(floor(ra_deg * 2.0) as bigint) as ra_bin,
          cast(floor((dec_deg + 90.0) * 2.0) as bigint) as dec_bin
        from stars
        where ra_deg is not null and dec_deg is not null
        """
    )
    con.execute(
        """
        create or replace temp view compact_catalog_bins as
        select
          *,
          cast(floor(ra_deg * 2.0) as bigint) as ra_bin,
          cast(floor((dec_deg + 90.0) * 2.0) as bigint) as dec_bin
        from compact_catalog_input
        where ra_deg is not null and dec_deg is not null
        """
    )
    con.execute(
        """
        create temp table compact_best_match as
        with id_candidates as (
          select
            c.source_catalog,
            c.source_key,
            s.star_id,
            s.system_id,
            0.0::double as ang_dist_arcsec,
            case
              when c.distance_ly is null or s.dist_ly is null then null
              else abs(c.distance_ly - s.dist_ly)
            end as dist_delta_ly,
            'gaia_id' as candidate_match_method
          from compact_catalog_input c
          join stars s
            on c.gaia_id is not null
           and s.gaia_id = c.gaia_id
        ), sky_candidates as (
          select
            c.source_catalog,
            c.source_key,
            s.star_id,
            s.system_id,
            degrees(acos(
              greatest(
                -1.0,
                least(
                  1.0,
                  sin(radians(c.dec_deg)) * sin(radians(s.dec_deg)) +
                  cos(radians(c.dec_deg)) * cos(radians(s.dec_deg)) * cos(radians(c.ra_deg - s.ra_deg))
                )
              )
            )) * 3600.0 as ang_dist_arcsec,
            case
              when c.distance_ly is null or s.dist_ly is null then null
              else abs(c.distance_ly - s.dist_ly)
            end as dist_delta_ly,
            'sky_position' as candidate_match_method
          from compact_catalog_bins c
          join star_sky_bins s
            on s.ra_bin between c.ra_bin - 1 and c.ra_bin + 1
           and s.dec_bin between c.dec_bin - 1 and c.dec_bin + 1
           and s.dec_deg between c.dec_deg - 1.0 and c.dec_deg + 1.0
           and (
             abs(s.ra_deg - c.ra_deg) <= 1.0
             or abs(abs(s.ra_deg - c.ra_deg) - 360.0) <= 1.0
           )
          where c.gaia_id is null
        ), candidates as (
          select * from id_candidates
          union all
          select * from sky_candidates
        ), ranked as (
          select
            *,
            row_number() over (
              partition by source_catalog, source_key
              order by
                case candidate_match_method when 'gaia_id' then 0 else 1 end asc,
                ang_dist_arcsec asc,
                dist_delta_ly asc nulls last,
                star_id asc
            ) as rn
          from candidates
          where
            (
              candidate_match_method = 'gaia_id'
            )
            or (
              candidate_match_method = 'sky_position'
              and ang_dist_arcsec <= 5.0
              and (dist_delta_ly is null or dist_delta_ly <= 200.0)
            )
        )
        select
          source_catalog,
          source_key,
          star_id,
          system_id,
          ang_dist_arcsec,
          dist_delta_ly,
          candidate_match_method as match_method
        from ranked
        where rn = 1
        """
    )
    con.execute(
        f"""
        create table compact_objects as
        select
          row_number() over (order by c.source_catalog, c.source_key)::bigint as compact_object_id,
          'compact:' || c.source_catalog || ':' || lower(regexp_replace(c.source_key, '[^0-9A-Za-z]+', '_', 'g')) as stable_object_key,
          m.system_id,
          m.star_id,
          c.object_name,
          c.object_family,
          c.object_type,
          c.ra_deg,
          c.dec_deg,
          c.distance_ly as dist_ly,
          c.distance_pc as dist_pc,
          c.parallax_mas,
          case when m.star_id is not null then coalesce(m.match_method, 'sky_position') else 'unmatched' end as match_method,
          case
            when m.star_id is null then 0.0
            when coalesce(m.match_method, '') = 'gaia_id' then 1.0
            when m.ang_dist_arcsec <= 0.5 then 0.99
            when m.ang_dist_arcsec <= 1.0 then 0.95
            when m.ang_dist_arcsec <= 2.0 then 0.90
            else 0.80
          end as match_confidence,
          m.ang_dist_arcsec as match_angular_distance_arcsec,
          m.dist_delta_ly as match_distance_delta_ly,
          c.catalog_ids_json,
          c.source_catalog,
          case
            when c.source_catalog = 'atnf' then {sql_literal(ATNF_VERSION)}
            when c.source_catalog = 'white_dwarf' then {sql_literal(WHITE_DWARF_VERSION)}
            else {sql_literal(MAGNETAR_VERSION)}
          end as source_version,
          case
            when c.source_catalog = 'atnf' then {sql_literal(ATNF_URL)}
            when c.source_catalog = 'white_dwarf' then {sql_literal(WHITE_DWARF_URL)}
            else {sql_literal(MAGNETAR_URL)}
          end as source_url,
          case
            when c.source_catalog = 'atnf' then {sql_literal(ATNF_URL)}
            when c.source_catalog = 'white_dwarf' then {sql_literal(WHITE_DWARF_URL)}
            else {sql_literal(MAGNETAR_URL)}
          end as source_download_url,
          null::varchar as source_doi,
          row_number() over (order by c.source_catalog, c.source_key)::bigint as source_pk,
          row_number() over (order by c.source_catalog, c.source_key)::bigint as source_row_id,
          sha256(c.source_catalog || '|' || c.source_key) as source_row_hash,
          'catalog-specific terms' as license,
          true as redistribution_ok,
          'See source catalog terms and acknowledgements.' as license_note,
          null::varchar as retrieval_etag,
          case
            when c.source_catalog = 'atnf' then {sql_literal(atnf_sha)}
            when c.source_catalog = 'white_dwarf' then {sql_literal(white_dwarf_sha)}
            else {sql_literal(magnetar_sha)}
          end as retrieval_checksum,
          case
            when c.source_catalog = 'atnf' then {sql_literal(atnf_retrieved)}
            when c.source_catalog = 'white_dwarf' then {sql_literal(white_dwarf_retrieved)}
            else {sql_literal(magnetar_retrieved)}
          end as retrieved_at,
          {sql_literal(ingested_at)} as ingested_at,
          {sql_literal(transform_version)} as transform_version
        from compact_catalog_input c
        left join compact_best_match m
          on m.source_catalog = c.source_catalog and m.source_key = c.source_key
        """
    )
    con.execute(
        """
        update stars
        set
          object_family = 'neutron_star',
          object_type = case
            when c.object_type = 'magnetar' then 'magnetar'
            else 'pulsar'
          end,
          classification_evidence_json = json_object(
            'method', 'compact_catalog_crossmatch',
            'catalog', c.source_catalog,
            'source_name', c.object_name,
            'match_angular_distance_arcsec', c.match_angular_distance_arcsec
          )
        from compact_objects c
        where stars.star_id = c.star_id
          and c.source_catalog in ('atnf', 'magnetar')
          and c.match_method = 'sky_position'
          and c.match_angular_distance_arcsec is not null
          and c.match_angular_distance_arcsec <= 1.0
        """
    )
    con.execute(
        """
        update stars
        set
          spectral_class = 'D',
          spectral_type_raw = case
            when upper(coalesce(wd_catalog_fit_model, '')) = 'H' then 'DA'
            when upper(coalesce(wd_catalog_fit_model, '')) = 'HE' then 'DB'
            when upper(coalesce(spectral_type_raw, '')) like 'D%' then spectral_type_raw
            else 'D'
          end,
          spectral_subtype = null,
          luminosity_class = null
        where object_family = 'white_dwarf'
        """
    )

    con.execute(
        f"""
        create table open_clusters as
        select
          row_number() over (order by nullif(cluster_name, ''), nullif(ra_deg, '')::double)::bigint as cluster_id,
          'cluster:' || lower(regexp_replace(coalesce(nullif(cluster_name, ''), 'unknown'), '[^0-9A-Za-z]+', '_', 'g')) as stable_object_key,
          nullif(cluster_name, '') as cluster_name,
          nullif(ra_deg, '')::double as ra_deg,
          nullif(dec_deg, '')::double as dec_deg,
          nullif(glon_deg, '')::double as glon_deg,
          nullif(glat_deg, '')::double as glat_deg,
          nullif(radius_r50_deg, '')::double as radius_r50_deg,
          nullif(member_count_prob_gt_0_7, '')::bigint as member_count_prob_gt_0_7,
          nullif(pm_ra_mas_yr, '')::double as pm_ra_mas_yr,
          nullif(pm_dec_mas_yr, '')::double as pm_dec_mas_yr,
          nullif(parallax_mas, '')::double as parallax_mas,
          nullif(distance_pc, '')::double as dist_pc,
          nullif(distance_pc, '')::double * {PC_TO_LY} as dist_ly,
          nullif(flag, '') as source_flag,
          {sql_literal("clusters")} as source_catalog,
          {sql_literal(CLUSTERS_VERSION)} as source_version,
          {sql_literal(CLUSTERS_URL)} as source_url,
          {sql_literal(CLUSTERS_URL)} as source_download_url,
          null::varchar as source_doi,
          row_number() over (order by nullif(cluster_name, ''), nullif(ra_deg, '')::double)::bigint as source_pk,
          row_number() over (order by nullif(cluster_name, ''), nullif(ra_deg, '')::double)::bigint as source_row_id,
          sha256(coalesce(cluster_name, '') || '|' || coalesce(ra_deg, '') || '|' || coalesce(dec_deg, '')) as source_row_hash,
          'CDS catalog terms' as license,
          true as redistribution_ok,
          'Cantat-Gaudin et al. 2020 catalog via CDS.' as license_note,
          null::varchar as retrieval_etag,
          {sql_literal(clusters_sha)} as retrieval_checksum,
          {sql_literal(clusters_retrieved)} as retrieved_at,
          {sql_literal(ingested_at)} as ingested_at,
          {sql_literal(transform_version)} as transform_version
        from open_clusters_raw
        where nullif(cluster_name, '') is not null
        """
    )
    con.execute(
        f"""
        create table open_cluster_memberships as
        select
          row_number() over (order by s.star_id, c.cluster_id)::bigint as cluster_membership_id,
          c.cluster_id,
          c.cluster_name,
          s.system_id,
          s.star_id,
          s.gaia_id,
          nullif(m.membership_probability, '')::double as membership_probability,
          'gaia_dr2_id_direct' as match_method,
          1.0 as match_confidence
        from open_cluster_members_raw m
        join stars s on s.gaia_id = nullif(m.gaia_dr2_source_id, '')::bigint
        join open_clusters c on c.cluster_name = nullif(m.cluster_name, '')
        where nullif(m.membership_probability, '')::double >= {open_cluster_member_min_probability}
        """
    )
    con.execute(
        """
        create temp table star_cluster_tags as
        select
          star_id,
          '[' || string_agg(distinct '"' || replace(cluster_name, '"', '\\"') || '"', ',') || ']' as tags_json
        from open_cluster_memberships
        group by star_id
        """
    )
    con.execute(
        """
        update stars
        set open_cluster_tags_json = t.tags_json
        from star_cluster_tags t
        where stars.star_id = t.star_id
        """
    )

    con.execute(
        f"""
        create table superstellar_objects as
        with clusters_as_objects as (
          select
            'open_cluster:' || cluster_id::varchar as stable_object_key,
            'open_cluster' as object_family,
            'open_cluster' as object_type,
            cluster_name as object_name,
            ra_deg,
            dec_deg,
            dist_pc,
            dist_ly,
            json_object('cluster_id', cluster_id, 'source_flag', source_flag) as object_meta_json,
            source_catalog,
            source_version,
            source_url,
            source_download_url,
            source_doi,
            source_pk,
            source_row_id,
            source_row_hash,
            license,
            redistribution_ok,
            license_note,
            retrieval_etag,
            retrieval_checksum,
            retrieved_at,
            ingested_at,
            transform_version
          from open_clusters
        ), snr_as_objects as (
          select
            'snr:' || lower(regexp_replace(coalesce(nullif(galactic_name, ''), 'unknown'), '[^0-9A-Za-z+\\-.]+', '_', 'g')) as stable_object_key,
            'superstellar' as object_family,
            'supernova_remnant' as object_type,
            nullif(galactic_name, '') as object_name,
            nullif(ra_deg, '')::double as ra_deg,
            nullif(dec_deg, '')::double as dec_deg,
            null::double as dist_pc,
            null::double as dist_ly,
            json_object(
              'glon_deg', nullif(glon_deg, '')::double,
              'glat_deg', nullif(glat_deg, '')::double,
              'size_major_arcmin', nullif(size_major_arcmin, '')::double,
              'size_minor_arcmin', nullif(size_minor_arcmin, '')::double,
              'morphology_type', nullif(morphology_type, ''),
              'spectral_index_raw', nullif(spectral_index_raw, ''),
              'other_names', nullif(other_names, '')
            ) as object_meta_json,
            {sql_literal("snr")} as source_catalog,
            {sql_literal(SNR_VERSION)} as source_version,
            {sql_literal(SNR_URL)} as source_url,
            {sql_literal(SNR_URL)} as source_download_url,
            null::varchar as source_doi,
            row_number() over (order by nullif(galactic_name, ''), nullif(ra_deg, '')::double)::bigint as source_pk,
            row_number() over (order by nullif(galactic_name, ''), nullif(ra_deg, '')::double)::bigint as source_row_id,
            sha256(coalesce(galactic_name, '') || '|' || coalesce(ra_deg, '') || '|' || coalesce(dec_deg, '')) as source_row_hash,
            'Catalog-specific terms' as license,
            true as redistribution_ok,
            'Green Galactic SNR catalog.' as license_note,
            null::varchar as retrieval_etag,
            {sql_literal(snr_sha)} as retrieval_checksum,
            {sql_literal(snr_retrieved)} as retrieved_at,
            {sql_literal(ingested_at)} as ingested_at,
            {sql_literal(transform_version)} as transform_version
          from snr_raw
          where nullif(galactic_name, '') is not null
        )
        select
          row_number() over (order by stable_object_key)::bigint as superstellar_object_id,
          *
        from (
          select * from clusters_as_objects
          union all
          select * from snr_as_objects
        ) u
        """
    )
    con.execute(
        f"""
        create table eclipsing_binaries as
        with debcat as (
          select
            'eb:debcat:' || lower(regexp_replace(coalesce(nullif(system_name, ''), 'unknown'), '[^0-9A-Za-z]+', '_', 'g')) as stable_object_key,
            nullif(system_name, '') as source_catalog_object_id,
            nullif(system_name, '') as object_name,
            null::bigint as star_id,
            null::bigint as system_id,
            'unmatched' as match_method,
            0.0 as match_confidence,
            nullif(period_days, '')::double as period_days,
            null::double as period_error_days,
            null::double as bjd0,
            null::double as bjd0_error,
            null::double as morphology,
            null::double as glon_deg,
            null::double as glat_deg,
            nullif(vmag, '')::double as kmag,
            null::double as teff_k,
            nullif(spectral_type_primary, '') as spectral_type_primary,
            nullif(spectral_type_secondary, '') as spectral_type_secondary,
            nullif(mass_primary_msun, '')::double as mass_primary_msun,
            nullif(mass_primary_err_msun, '')::double as mass_primary_err_msun,
            nullif(mass_secondary_msun, '')::double as mass_secondary_msun,
            nullif(mass_secondary_err_msun, '')::double as mass_secondary_err_msun,
            nullif(radius_primary_rsun, '')::double as radius_primary_rsun,
            nullif(radius_primary_err_rsun, '')::double as radius_primary_err_rsun,
            nullif(radius_secondary_rsun, '')::double as radius_secondary_rsun,
            nullif(radius_secondary_err_rsun, '')::double as radius_secondary_err_rsun,
            nullif(logg_primary_cgs, '')::double as logg_primary_cgs,
            nullif(logg_primary_err_cgs, '')::double as logg_primary_err_cgs,
            nullif(logg_secondary_cgs, '')::double as logg_secondary_cgs,
            nullif(logg_secondary_err_cgs, '')::double as logg_secondary_err_cgs,
            nullif(teff_primary_k, '')::double as teff_primary_k,
            nullif(teff_primary_err_k, '')::double as teff_primary_err_k,
            nullif(teff_secondary_k, '')::double as teff_secondary_k,
            nullif(teff_secondary_err_k, '')::double as teff_secondary_err_k,
            nullif(lum_primary_lsun, '')::double as lum_primary_lsun,
            nullif(lum_primary_err_lsun, '')::double as lum_primary_err_lsun,
            nullif(lum_secondary_lsun, '')::double as lum_secondary_lsun,
            nullif(lum_secondary_err_lsun, '')::double as lum_secondary_err_lsun,
            nullif(metallicity_dex, '')::double as metallicity_dex,
            nullif(metallicity_err_dex, '')::double as metallicity_err_dex,
            null::boolean as has_short_cadence,
            {sql_literal('debcat')} as source_catalog,
            {sql_literal(DEBCAT_VERSION)} as source_version,
            {sql_literal(DEBCAT_URL)} as source_url,
            {sql_literal(DEBCAT_URL)} as source_download_url,
            null::varchar as source_doi,
            'CC BY 4.0' as license,
            true as redistribution_ok,
            'DEBCat catalog terms and citation guidance apply.' as license_note,
            null::varchar as retrieval_etag,
            {sql_literal(debcat_sha)} as retrieval_checksum,
            {sql_literal(debcat_retrieved)} as retrieved_at,
            {sql_literal(ingested_at)} as ingested_at,
            {sql_literal(transform_version)} as transform_version
          from debcat_raw
          where nullif(system_name, '') is not null
        ), kepler as (
          select
            'eb:kepler_eb:kic_' || trim(kic_id) as stable_object_key,
            'KIC ' || trim(kic_id) as source_catalog_object_id,
            'KIC ' || trim(kic_id) as object_name,
            null::bigint as star_id,
            null::bigint as system_id,
            'unmatched' as match_method,
            0.0 as match_confidence,
            nullif(period_days, '')::double as period_days,
            nullif(period_error_days, '')::double as period_error_days,
            nullif(bjd0, '')::double as bjd0,
            nullif(bjd0_error, '')::double as bjd0_error,
            nullif(morphology, '')::double as morphology,
            nullif(glon_deg, '')::double as glon_deg,
            nullif(glat_deg, '')::double as glat_deg,
            nullif(kmag, '')::double as kmag,
            nullif(teff_k, '')::double as teff_k,
            null::varchar as spectral_type_primary,
            null::varchar as spectral_type_secondary,
            null::double as mass_primary_msun,
            null::double as mass_primary_err_msun,
            null::double as mass_secondary_msun,
            null::double as mass_secondary_err_msun,
            null::double as radius_primary_rsun,
            null::double as radius_primary_err_rsun,
            null::double as radius_secondary_rsun,
            null::double as radius_secondary_err_rsun,
            null::double as logg_primary_cgs,
            null::double as logg_primary_err_cgs,
            null::double as logg_secondary_cgs,
            null::double as logg_secondary_err_cgs,
            null::double as teff_primary_k,
            null::double as teff_primary_err_k,
            null::double as teff_secondary_k,
            null::double as teff_secondary_err_k,
            null::double as lum_primary_lsun,
            null::double as lum_primary_err_lsun,
            null::double as lum_secondary_lsun,
            null::double as lum_secondary_err_lsun,
            null::double as metallicity_dex,
            null::double as metallicity_err_dex,
            case
              when lower(nullif(has_short_cadence, '')) in ('true', '1', 'yes', 'y', 't') then true
              when lower(nullif(has_short_cadence, '')) in ('false', '0', 'no', 'n', 'f') then false
              else null
            end as has_short_cadence,
            {sql_literal('kepler_eb')} as source_catalog,
            {sql_literal(KEPLER_EB_VERSION)} as source_version,
            {sql_literal(KEPLER_EB_URL)} as source_url,
            {sql_literal(KEPLER_EB_URL)} as source_download_url,
            null::varchar as source_doi,
            'Catalog-specific terms' as license,
            true as redistribution_ok,
            'Kepler Eclipsing Binary Catalog terms and citation guidance apply.' as license_note,
            null::varchar as retrieval_etag,
            {sql_literal(kepler_eb_sha)} as retrieval_checksum,
            {sql_literal(kepler_eb_retrieved)} as retrieved_at,
            {sql_literal(ingested_at)} as ingested_at,
            {sql_literal(transform_version)} as transform_version
          from kepler_eb_raw
          where nullif(kic_id, '') is not null
        ), tess as (
          select
            'eb:tess_eb:tic_' || trim(tic_id) as stable_object_key,
            'TIC ' || trim(tic_id) as source_catalog_object_id,
            'TIC ' || trim(tic_id) as object_name,
            null::bigint as star_id,
            null::bigint as system_id,
            'unmatched' as match_method,
            0.0 as match_confidence,
            nullif(period_days, '')::double as period_days,
            nullif(period_error_days, '')::double as period_error_days,
            nullif(bjd0, '')::double as bjd0,
            nullif(bjd0_error, '')::double as bjd0_error,
            nullif(morphology, '')::double as morphology,
            nullif(glon_deg, '')::double as glon_deg,
            nullif(glat_deg, '')::double as glat_deg,
            nullif(tmag, '')::double as kmag,
            nullif(teff_k, '')::double as teff_k,
            null::varchar as spectral_type_primary,
            null::varchar as spectral_type_secondary,
            null::double as mass_primary_msun,
            null::double as mass_primary_err_msun,
            null::double as mass_secondary_msun,
            null::double as mass_secondary_err_msun,
            null::double as radius_primary_rsun,
            null::double as radius_primary_err_rsun,
            null::double as radius_secondary_rsun,
            null::double as radius_secondary_err_rsun,
            null::double as logg_primary_cgs,
            null::double as logg_primary_err_cgs,
            null::double as logg_secondary_cgs,
            null::double as logg_secondary_err_cgs,
            null::double as teff_primary_k,
            null::double as teff_primary_err_k,
            null::double as teff_secondary_k,
            null::double as teff_secondary_err_k,
            null::double as lum_primary_lsun,
            null::double as lum_primary_err_lsun,
            null::double as lum_secondary_lsun,
            null::double as lum_secondary_err_lsun,
            nullif(metallicity_dex, '')::double as metallicity_dex,
            null::double as metallicity_err_dex,
            null::boolean as has_short_cadence,
            {sql_literal('tess_eb')} as source_catalog,
            {sql_literal(TESS_EB_VERSION)} as source_version,
            {sql_literal(TESS_EB_URL)} as source_url,
            {sql_literal(TESS_EB_URL)} as source_download_url,
            null::varchar as source_doi,
            'Catalog-specific terms' as license,
            true as redistribution_ok,
            'TESS EB catalog terms and citation guidance apply.' as license_note,
            null::varchar as retrieval_etag,
            {sql_literal(tess_eb_sha)} as retrieval_checksum,
            {sql_literal(tess_eb_retrieved)} as retrieved_at,
            {sql_literal(ingested_at)} as ingested_at,
            {sql_literal(transform_version)} as transform_version
          from tess_eb_raw
          where nullif(tic_id, '') is not null
        ), unioned as (
          select * from debcat
          union all
          select * from kepler
          union all
          select * from tess
        )
        select
          row_number() over (order by source_catalog, source_catalog_object_id)::bigint as eclipsing_binary_id,
          stable_object_key,
          source_catalog_object_id,
          object_name,
          star_id,
          system_id,
          match_method,
          match_confidence,
          period_days,
          period_error_days,
          bjd0,
          bjd0_error,
          morphology,
          glon_deg,
          glat_deg,
          kmag,
          teff_k,
          spectral_type_primary,
          spectral_type_secondary,
          mass_primary_msun,
          mass_primary_err_msun,
          mass_secondary_msun,
          mass_secondary_err_msun,
          radius_primary_rsun,
          radius_primary_err_rsun,
          radius_secondary_rsun,
          radius_secondary_err_rsun,
          logg_primary_cgs,
          logg_primary_err_cgs,
          logg_secondary_cgs,
          logg_secondary_err_cgs,
          teff_primary_k,
          teff_primary_err_k,
          teff_secondary_k,
          teff_secondary_err_k,
          lum_primary_lsun,
          lum_primary_err_lsun,
          lum_secondary_lsun,
          lum_secondary_err_lsun,
          metallicity_dex,
          metallicity_err_dex,
          has_short_cadence,
          source_catalog,
          source_version,
          source_url,
          source_download_url,
          source_doi,
          row_number() over (order by source_catalog, source_catalog_object_id)::bigint as source_pk,
          row_number() over (order by source_catalog, source_catalog_object_id)::bigint as source_row_id,
          sha256(source_catalog || '|' || coalesce(source_catalog_object_id, '')) as source_row_hash,
          license,
          redistribution_ok,
          license_note,
          retrieval_etag,
          retrieval_checksum,
          retrieved_at,
          ingested_at,
          transform_version
        from unioned
        """
    )
    # Attach eclipsing catalogs to known stars/systems so they are inspectable in system detail.
    con.execute(
        """
        create or replace temp table eclipsing_id_match_candidates as
        with eb_norm as (
          select
            eclipsing_binary_id,
            source_catalog,
            lower(regexp_replace(coalesce(source_catalog_object_id, ''), '[^0-9A-Za-z]+', '', 'g')) as source_id_key,
            lower(regexp_replace(coalesce(object_name, ''), '[^0-9A-Za-z]+', '', 'g')) as object_name_key
          from eclipsing_binaries
        ), alias_star_unique as (
          select
            lower(regexp_replace(coalesce(alias_norm, alias_raw, ''), '[^0-9A-Za-z]+', '', 'g')) as alias_key,
            min(star_id)::bigint as star_id,
            min(system_id)::bigint as system_id
          from aliases
          where target_type = 'star'
            and star_id is not null
            and coalesce(alias_norm, alias_raw, '') <> ''
          group by 1
          having count(distinct star_id) = 1
        ), alias_system_unique as (
          select
            lower(regexp_replace(coalesce(alias_norm, alias_raw, ''), '[^0-9A-Za-z]+', '', 'g')) as alias_key,
            min(system_id)::bigint as system_id
          from aliases
          where target_type = 'system'
            and system_id is not null
            and coalesce(alias_norm, alias_raw, '') <> ''
          group by 1
          having count(distinct system_id) = 1
        ), star_name_unique as (
          select
            lower(regexp_replace(coalesce(star_name_norm, ''), '[^0-9A-Za-z]+', '', 'g')) as name_key,
            min(star_id)::bigint as star_id,
            min(system_id)::bigint as system_id
          from stars
          where coalesce(star_name_norm, '') <> ''
          group by 1
          having count(*) = 1
        ), cands as (
          select
            eb.eclipsing_binary_id,
            a.star_id,
            a.system_id,
            'catalog_id_alias'::varchar as match_method,
            0.985::double as match_confidence,
            null::double as match_sep_arcsec
          from eb_norm eb
          join alias_star_unique a on a.alias_key = eb.source_id_key
          where eb.source_id_key <> ''

          union all

          select
            eb.eclipsing_binary_id,
            null::bigint as star_id,
            a.system_id,
            'catalog_id_system_alias'::varchar as match_method,
            0.95::double as match_confidence,
            null::double as match_sep_arcsec
          from eb_norm eb
          join alias_system_unique a on a.alias_key = eb.source_id_key
          where eb.source_id_key <> ''

          union all

          select
            eb.eclipsing_binary_id,
            a.star_id,
            a.system_id,
            'debcat_name_alias'::varchar as match_method,
            0.955::double as match_confidence,
            null::double as match_sep_arcsec
          from eb_norm eb
          join alias_star_unique a on a.alias_key = eb.object_name_key
          where eb.source_catalog = 'debcat'
            and eb.object_name_key <> ''

          union all

          select
            eb.eclipsing_binary_id,
            s.star_id,
            s.system_id,
            'debcat_star_name'::varchar as match_method,
            0.94::double as match_confidence,
            null::double as match_sep_arcsec
          from eb_norm eb
          join star_name_unique s on s.name_key = eb.object_name_key
          where eb.source_catalog = 'debcat'
            and eb.object_name_key <> ''
        )
        select * from cands
        """
    )
    con.execute(
        """
        create or replace temp table eclipsing_positional_match_candidates as
        with tess_keys as (
          select
            eclipsing_binary_id,
            try_cast(regexp_extract(source_catalog_object_id, '([0-9]+)', 1) as bigint) as tic_id
          from eclipsing_binaries
          where source_catalog = 'tess_eb'
        ), tess_coords as (
          select
            tk.eclipsing_binary_id,
            nullif(t.ra_deg, '')::double as ra_deg,
            nullif(t.dec_deg, '')::double as dec_deg
          from tess_keys tk
          join tess_eb_raw t on try_cast(nullif(t.tic_id, '') as bigint) = tk.tic_id
          where nullif(t.ra_deg, '') is not null
            and nullif(t.dec_deg, '') is not null
        ), star_bins as (
          select
            star_id,
            system_id,
            ra_deg,
            dec_deg,
            floor(ra_deg * 100.0)::bigint as ra_bin,
            floor((dec_deg + 90.0) * 100.0)::bigint as dec_bin
          from stars
          where ra_deg is not null and dec_deg is not null
        ), tess_bins as (
          select
            eclipsing_binary_id,
            ra_deg,
            dec_deg,
            floor(ra_deg * 100.0)::bigint as ra_bin,
            floor((dec_deg + 90.0) * 100.0)::bigint as dec_bin
          from tess_coords
        ), candidates as (
          select
            t.eclipsing_binary_id,
            s.star_id,
            s.system_id,
            sqrt(
              power((s.ra_deg - t.ra_deg) * cos(radians((s.dec_deg + t.dec_deg) / 2.0)), 2) +
              power(s.dec_deg - t.dec_deg, 2)
            ) * 3600.0 as sep_arcsec,
            row_number() over (
              partition by t.eclipsing_binary_id
              order by
                sqrt(
                  power((s.ra_deg - t.ra_deg) * cos(radians((s.dec_deg + t.dec_deg) / 2.0)), 2) +
                  power(s.dec_deg - t.dec_deg, 2)
                ) * 3600.0 asc,
                s.star_id asc
            ) as rn
          from tess_bins t
          join star_bins s
            on s.ra_bin between t.ra_bin - 1 and t.ra_bin + 1
           and s.dec_bin between t.dec_bin - 1 and t.dec_bin + 1
           and abs(s.ra_deg - t.ra_deg) <= 0.01
           and abs(s.dec_deg - t.dec_deg) <= 0.01
        )
        select
          eclipsing_binary_id,
          star_id,
          system_id,
          case
            when sep_arcsec <= 1.0 then 'tess_radec_1arcsec'
            else 'tess_radec_2arcsec'
          end as match_method,
          case
            when sep_arcsec <= 1.0 then 0.96
            else 0.90
          end as match_confidence,
          sep_arcsec as match_sep_arcsec
        from candidates
        where rn = 1
          and sep_arcsec <= 2.0
        """
    )
    con.execute(
        """
        create or replace temp table eclipsing_match_best as
        with all_candidates as (
          select * from eclipsing_id_match_candidates
          union all
          select * from eclipsing_positional_match_candidates
        ), ranked as (
          select
            eclipsing_binary_id,
            star_id,
            system_id,
            match_method,
            match_confidence,
            match_sep_arcsec,
            row_number() over (
              partition by eclipsing_binary_id
              order by
                match_confidence desc,
                case when star_id is null then 1 else 0 end asc,
                coalesce(match_sep_arcsec, 1e12) asc,
                coalesce(star_id, 9223372036854775807) asc
            ) as rn
          from all_candidates
        )
        select
          eclipsing_binary_id,
          star_id,
          system_id,
          match_method,
          match_confidence,
          match_sep_arcsec
        from ranked
        where rn = 1
        """
    )
    con.execute(
        """
        update eclipsing_binaries eb
        set
          star_id = m.star_id,
          system_id = m.system_id,
          match_method = m.match_method,
          match_confidence = m.match_confidence
        from eclipsing_match_best m
        where eb.eclipsing_binary_id = m.eclipsing_binary_id
        """
    )
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
        with base as (
          select coalesce(max(alias_id), 0)::bigint as base_id from aliases
        ), candidates as (
          select
            eb.source_catalog,
            eb.source_version,
            eb.source_pk,
            eb.system_id,
            eb.star_id,
            eb.source_catalog_object_id as alias_raw,
            lower(regexp_replace(coalesce(eb.source_catalog_object_id, ''), '[^0-9A-Za-z]+', '', 'g')) as alias_key,
            case
              when eb.source_catalog = 'tess_eb' then 'tic_id'
              when eb.source_catalog = 'kepler_eb' then 'kic_id'
              when eb.source_catalog = 'debcat' then 'debcat_name'
              else 'catalog_object_id'
            end as alias_kind
          from eclipsing_binaries eb
          where eb.system_id is not null
            and eb.source_catalog_object_id is not null
            and trim(eb.source_catalog_object_id) <> ''
            and (
              eb.source_catalog in ('tess_eb', 'kepler_eb')
              or (eb.source_catalog = 'debcat' and eb.match_confidence >= 0.95)
            )
        ), ranked as (
          select
            c.*,
            row_number() over (
              partition by c.star_id, c.alias_key
              order by c.source_catalog asc, c.source_pk asc
            ) as rn
          from candidates c
          where c.star_id is not null
            and c.alias_key <> ''
            and not exists (
              select 1
              from aliases a
              where a.target_type = 'star'
                and a.star_id = c.star_id
                and lower(regexp_replace(coalesce(a.alias_raw, ''), '[^0-9A-Za-z]+', '', 'g')) = c.alias_key
            )
        )
        select
          base.base_id + row_number() over (order by ranked.star_id, ranked.alias_key, ranked.source_catalog, ranked.source_pk)::bigint as alias_id,
          'star' as target_type,
          ranked.star_id as target_id,
          ranked.system_id,
          ranked.star_id,
          ranked.alias_raw,
          ranked.alias_key as alias_norm,
          ranked.alias_kind,
          28 as alias_priority,
          false as is_primary,
          ranked.source_catalog,
          ranked.source_version,
          ranked.source_pk
        from ranked
        cross join base
        where ranked.rn = 1
        """
    )

    compact_count = con.execute("select count(*) from compact_objects").fetchone()[0]
    open_clusters_count = con.execute("select count(*) from open_clusters").fetchone()[0]
    open_cluster_memberships_count = con.execute(
        "select count(*) from open_cluster_memberships"
    ).fetchone()[0]
    superstellar_count = con.execute(
        "select count(*) from superstellar_objects"
    ).fetchone()[0]
    eclipsing_count = con.execute(
        "select count(*) from eclipsing_binaries"
    ).fetchone()[0]
    eclipsing_linked_count = con.execute(
        "select count(*) from eclipsing_binaries where system_id is not null"
    ).fetchone()[0]
    eclipsing_high_conf_count = con.execute(
        "select count(*) from eclipsing_binaries where system_id is not null and coalesce(match_confidence, 0.0) >= 0.95"
    ).fetchone()[0]
    stage_totals["compact_objects"] = compact_count
    stage_totals["superstellar_objects"] = superstellar_count
    stage_totals["eclipsing_binaries"] = eclipsing_count
    log_stage_complete(
        "Science side tables stage",
        science_side_stage_started,
        stage_totals,
        extra=(
            f"open_clusters={format_count(open_clusters_count)}, "
            f"open_cluster_memberships={format_count(open_cluster_memberships_count)}, "
            f"eclipsing_binaries={format_count(eclipsing_count)}, "
            f"eclipsing_linked={format_count(eclipsing_linked_count)}, "
            f"eclipsing_high_conf={format_count(eclipsing_high_conf_count)}"
        ),
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

    athyg_manifest_block = {
        "source_catalog": "athyg",
        "source_url": "https://codeberg.org/astronexus/athyg",
        "part1": athyg_p1,
        "part2": athyg_p2,
    }
    base_source_manifest_block = (
        {
            "source_catalog": "gaia_dr3",
            "manifest": gaia_backbone_manifest,
        }
        if enable_gaia_backbone
        else athyg_manifest_block
    )

    provenance_report = {
        "build_id": build_id,
        "base_source": base_source_manifest_block,
        "nasa_exoplanet_archive": nasa_manifest,
        "sol_authority": sol_authority_manifest,
        "wds": wds_manifest,
        "orb6": orb6_manifest,
        "tables": {
            "stars": table_provenance_report("stars", base_source_has_retrieval),
            "systems": table_provenance_report("systems", base_source_has_retrieval),
            "planets": table_provenance_report("planets", nasa_has_retrieval),
            "compact_objects": table_provenance_report("compact_objects", True),
            "superstellar_objects": table_provenance_report("superstellar_objects", True),
            "eclipsing_binaries": table_provenance_report("eclipsing_binaries", True),
        },
    }
    if (not enable_gaia_backbone) or (enable_gaia_backbone and enable_athyg_supplement_merge):
        provenance_report["athyg"] = athyg_manifest_block
    if enable_gaia_backbone:
        provenance_report["gaia_dr3_backbone"] = gaia_backbone_manifest
    if msc_manifest:
        provenance_report["msc"] = msc_manifest
    if gaia_nss_non_single_manifest:
        provenance_report["gaia_nss_non_single_star"] = gaia_nss_non_single_manifest
    if gaia_nss_two_body_manifest:
        provenance_report["gaia_nss_two_body_orbit"] = gaia_nss_two_body_manifest
    if sbx_systems_manifest:
        provenance_report["sbx_systems"] = sbx_systems_manifest
    if sbx_alias_manifest:
        provenance_report["sbx_alias"] = sbx_alias_manifest
    if sbx_config_manifest:
        provenance_report["sbx_configurations"] = sbx_config_manifest
    if sbx_orbits_manifest:
        provenance_report["sbx_orbits"] = sbx_orbits_manifest
    if wds_gaia_xmatch_manifest:
        provenance_report["wds_gaia_xmatch_best"] = wds_gaia_xmatch_manifest
    if gaia_classprob_manifest:
        provenance_report["gaia_dr3_astrophysical_classprob"] = gaia_classprob_manifest
    if gaia_ucd_manifest:
        provenance_report["gaia_ucd"] = gaia_ucd_manifest
    if atnf_manifest:
        provenance_report["atnf"] = atnf_manifest
    if magnetar_manifest:
        provenance_report["magnetar"] = magnetar_manifest
    if white_dwarf_manifest:
        provenance_report["white_dwarf"] = white_dwarf_manifest
    if clusters_table1_manifest:
        provenance_report["open_clusters_table1"] = clusters_table1_manifest
    if clusters_members_manifest:
        provenance_report["open_clusters_members"] = clusters_members_manifest
    if snr_manifest:
        provenance_report["snr"] = snr_manifest
    if debcat_manifest:
        provenance_report["debcat"] = debcat_manifest
    if kepler_eb_manifest:
        provenance_report["kepler_eb"] = kepler_eb_manifest
    if tess_eb_manifest:
        provenance_report["tess_eb"] = tess_eb_manifest
    if exoplanet_eu_manifest:
        provenance_report["exoplanet_eu"] = exoplanet_eu_manifest
    if open_exoplanet_catalogue_manifest:
        provenance_report["open_exoplanet_catalogue"] = open_exoplanet_catalogue_manifest
    if hwc_manifest:
        provenance_report["hwc"] = hwc_manifest

    write_json(reports_dir / "provenance_report.json", provenance_report)

    total_failures = sum(
        provenance_report["tables"][name]["failures"]
        for name in (
            "stars",
            "systems",
            "planets",
            "compact_objects",
            "superstellar_objects",
            "eclipsing_binaries",
        )
    )
    if total_failures > 0:
        raise SystemExit(
            f"Provenance QC failed: {total_failures} missing required fields. "
            f"See {reports_dir / 'provenance_report.json'}"
        )

    # Reports
    qc_stage_started = time.monotonic()
    log("QC checks")
    counts = con.execute(
        """
        select
          (select count(*) from stars) as stars,
          (select count(*) from systems) as systems,
          (select count(*) from planets) as planets,
          (select count(*) from compact_objects) as compact_objects,
          (select count(*) from open_clusters) as open_clusters,
          (select count(*) from open_cluster_memberships) as open_cluster_memberships,
          (select count(*) from superstellar_objects) as superstellar_objects,
          (select count(*) from eclipsing_binaries) as eclipsing_binaries,
          (select count(*) from object_identifiers) as object_identifiers,
          (select count(*) from identifier_quarantine) as identifier_quarantine
        """
    ).fetchone()

    object_family_counts = con.execute(
        """
        select coalesce(object_family, 'unknown') as object_family, count(*)::bigint as count
        from stars
        group by 1
        order by count desc, object_family asc
        """
    ).fetchall()

    match_counts = con.execute(
        """
        select match_method, count(*) as count from planets group by match_method order by count desc
        """
    ).fetchall()

    alias_kind_counts = con.execute(
        """
        select alias_kind, count(*)::bigint as count
        from aliases
        group by 1
        order by count desc, alias_kind asc
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

    wd_classprob_evidence_count = con.execute(
        f"""
        select count(*) from stars
        where greatest(
          coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
          coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
        ) >= {WHITE_DWARF_PROB_THRESHOLD}
        """
    ).fetchone()[0]
    wd_catalog_evidence_count = con.execute(
        f"""
        select count(*) from stars
        where coalesce(wd_catalog_pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
        """
    ).fetchone()[0]
    wd_evidence_count = con.execute(
        f"""
        select count(*) from stars
        where
          greatest(
            coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
            coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
          ) >= {WHITE_DWARF_PROB_THRESHOLD}
          or coalesce(wd_catalog_pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
        """
    ).fetchone()[0]
    wd_emitted_count = con.execute(
        "select count(*) from stars where object_family = 'white_dwarf'"
    ).fetchone()[0]
    wd_mismatch_count = con.execute(
        f"""
        select count(*) from stars
        where (
          greatest(
            coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
            coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
          ) >= {WHITE_DWARF_PROB_THRESHOLD}
          or coalesce(wd_catalog_pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
        )
          and coalesce(object_family, '') not in ('white_dwarf', 'neutron_star')
        """
    ).fetchone()[0]
    classification_safety_report = {
        "build_id": build_id,
        "white_dwarf_probability_threshold": WHITE_DWARF_PROB_THRESHOLD,
        "white_dwarf_catalog_pwd_threshold": white_dwarf_catalog_pwd_threshold,
        "white_dwarf_classprob_evidence_count": wd_classprob_evidence_count,
        "white_dwarf_catalog_evidence_count": wd_catalog_evidence_count,
        "white_dwarf_evidence_count": wd_evidence_count,
        "white_dwarf_emitted_count": wd_emitted_count,
        "white_dwarf_mismatch_count": wd_mismatch_count,
        "notes": [
            "Invariant: rows with strong white-dwarf evidence (Gaia classprob or Gaia EDR3 white-dwarf catalog) must emit object_family=white_dwarf.",
            "Compact-object catalog crossmatches may override object_family to neutron_star for high-confidence positional matches.",
        ],
    }
    write_json(reports_dir / "classification_safety_report.json", classification_safety_report)
    alias_report = {
        "build_id": build_id,
        "aliases_enabled": enable_aliases,
        "athyg_alias_crosswalk_enabled": enable_aliases and enable_athyg_alias_crosswalk,
        "alias_total_count": alias_total_count,
        "alias_system_count": alias_system_count,
        "alias_star_count": alias_star_count,
        "athyg_alias_candidate_count": alias_crosswalk_candidate_count,
        "athyg_alias_matched_star_count": alias_crosswalk_matched_star_count,
        "name_override_count": alias_name_override_count,
        "alias_kind_counts": [
            {"alias_kind": row[0], "count": row[1]} for row in alias_kind_counts
        ],
    }
    write_json(reports_dir / "alias_report.json", alias_report)
    identifier_report = {
        "build_id": build_id,
        "athyg_supplement_merge_enabled": enable_gaia_backbone and enable_athyg_supplement_merge,
        "msc_component_dedupe": {
            "raw_rows": msc_component_raw_count,
            "retained_rows": msc_component_retained_count,
            "dropped_rows": msc_component_dedup_dropped_count,
            "duplicate_groups": msc_component_dedup_group_count,
        },
        "athyg_merge": {
            "resolved_existing_rows": athyg_merge_existing_match_count,
            "inserted_new_rows": athyg_merge_insert_count,
            "quarantined_rows": athyg_merge_quarantine_count,
            "unresolved_rows": athyg_merge_unresolved_count,
            "direct_gaia_count": athyg_merge_direct_gaia_count,
            "gaia_remap_count": athyg_merge_remap_count,
            "direct_legacy_id_count": athyg_merge_direct_legacy_id_count,
            "positional_match_count": athyg_merge_positional_count,
            "positional_ambiguous_count": athyg_merge_positional_ambiguous_count,
        },
        "identifier_edges": {
            "object_identifier_count": object_identifier_count,
            "identifier_quarantine_count": identifier_quarantine_count,
            "gaia_collision_count": identifier_gaia_collision_count,
            "hip_collision_count": identifier_hip_collision_count,
            "hd_collision_count": identifier_hd_collision_count,
        },
        "gates": {
            "ambiguous_limit": athyg_merge_ambiguous_limit,
            "gaia_collision_max": athyg_merge_gaia_collision_max,
            "hip_collision_max": athyg_merge_hip_collision_max,
            "hd_collision_max": athyg_merge_hd_collision_max,
        },
    }
    write_json(reports_dir / "identifier_report.json", identifier_report)

    def duplicate_exact_counts(key_expr_sql: str, where_sql: str) -> tuple[int, int]:
        groups, extra_rows = con.execute(
            f"""
            with dup as (
              select {key_expr_sql} as duplicate_key, count(*)::bigint as row_count
              from stars
              where {where_sql}
              group by 1
              having count(*) > 1
            )
            select count(*)::bigint as duplicate_groups, coalesce(sum(row_count - 1), 0)::bigint as extra_rows
            from dup
            """
        ).fetchone()
        return int(groups or 0), int(extra_rows or 0)

    duplicate_exact_checks: list[dict] = []
    for duplicate_key_name, key_expr_sql, where_sql in [
        ("gaia_id", "gaia_id::varchar", "gaia_id is not null"),
        ("hip_id", "hip_id::varchar", "hip_id is not null"),
        ("hd_id", "hd_id::varchar", "hd_id is not null"),
        (
            "wds_component",
            "wds_id || '|' || lower(regexp_replace(coalesce(component, ''), '[^0-9A-Za-z]+', '', 'g'))",
            "wds_id is not null and coalesce(component, '') <> ''",
        ),
        (
            "source_catalog_source_pk",
            "coalesce(source_catalog, '') || '|' || source_pk::varchar",
            "source_catalog is not null and source_pk is not null",
        ),
        ("stable_object_key", "stable_object_key", "stable_object_key is not null"),
    ]:
        groups, extra_rows = duplicate_exact_counts(key_expr_sql, where_sql)
        duplicate_exact_checks.append(
            {
                "key": duplicate_key_name,
                "duplicate_groups": groups,
                "extra_rows": extra_rows,
            }
        )

    duplicate_exact_total_groups = sum(
        int(row.get("duplicate_groups") or 0) for row in duplicate_exact_checks
    )
    duplicate_exact_total_extra_rows = sum(
        int(row.get("extra_rows") or 0) for row in duplicate_exact_checks
    )
    duplicate_exact_by_key = {
        str(row.get("key") or ""): {
            "duplicate_groups": int(row.get("duplicate_groups") or 0),
            "extra_rows": int(row.get("extra_rows") or 0),
        }
        for row in duplicate_exact_checks
    }
    multiplicity_gaia_duplicate_extra_rows = int(
        duplicate_exact_by_key.get("gaia_id", {}).get("extra_rows", 0)
    )
    multiplicity_wds_component_duplicate_extra_rows = int(
        duplicate_exact_by_key.get("wds_component", {}).get("extra_rows", 0)
    )

    duplicate_exact_examples = [
        {
            "key": "wds_component",
            "rows": [
                {
                    "wds_id": row[0],
                    "component": row[1],
                    "row_count": int(row[2] or 0),
                    "source_catalogs": row[3] or "",
                }
                for row in con.execute(
                    """
                    with dup as (
                      select
                        wds_id,
                        component,
                        count(*)::bigint as row_count,
                        string_agg(distinct coalesce(source_catalog, 'unknown'), ',' order by coalesce(source_catalog, 'unknown')) as source_catalogs
                      from stars
                      where wds_id is not null and coalesce(component, '') <> ''
                      group by 1, 2
                      having count(*) > 1
                    )
                    select wds_id, component, row_count, source_catalogs
                    from dup
                    order by row_count desc, wds_id asc, component asc
                    limit 20
                    """
                ).fetchall()
            ],
        },
        {
            "key": "gaia_id",
            "rows": [
                {
                    "gaia_id": str(row[0]),
                    "row_count": int(row[1] or 0),
                }
                for row in con.execute(
                    """
                    select gaia_id, count(*)::bigint as row_count
                    from stars
                    where gaia_id is not null
                    group by 1
                    having count(*) > 1
                    order by row_count desc, gaia_id asc
                    limit 20
                    """
                ).fetchall()
            ],
        },
    ]

    con.execute(
        f"""
        create or replace temp view duplicate_pair_candidates as
        with multi_systems as (
          select system_id
          from stars
          where system_id is not null
          group by 1
          having count(*) > 1
        ), pairs as (
          select
            a.system_id,
            a.star_id as left_star_id,
            b.star_id as right_star_id,
            a.star_name as left_star_name,
            b.star_name as right_star_name,
            a.source_catalog as left_source_catalog,
            b.source_catalog as right_source_catalog,
            a.wds_id as left_wds_id,
            b.wds_id as right_wds_id,
            a.component as left_component,
            b.component as right_component,
            a.gaia_id as left_gaia_id,
            b.gaia_id as right_gaia_id,
            3600.0 * sqrt(
              power(
                (a.ra_deg - b.ra_deg) * cos(((a.dec_deg + b.dec_deg) / 2.0) * pi() / 180.0),
                2
              ) +
              power(a.dec_deg - b.dec_deg, 2)
            ) as ang_sep_arcsec,
            abs(a.dist_ly - b.dist_ly) as dist_delta_ly,
            case
              when a.pm_ra_mas_yr is not null and a.pm_dec_mas_yr is not null
               and b.pm_ra_mas_yr is not null and b.pm_dec_mas_yr is not null
              then sqrt(
                power(a.pm_ra_mas_yr - b.pm_ra_mas_yr, 2) +
                power(a.pm_dec_mas_yr - b.pm_dec_mas_yr, 2)
              )
              else null
            end as pm_delta_mas_yr,
            coalesce(a.star_name_norm, '') = coalesce(b.star_name_norm, '')
              and coalesce(a.star_name_norm, '') <> '' as same_name_norm,
            a.wds_id is not null and b.wds_id is not null
              and a.wds_id = b.wds_id
              and coalesce(a.component, '') <> ''
              and lower(regexp_replace(coalesce(a.component, ''), '[^0-9A-Za-z]+', '', 'g'))
                = lower(regexp_replace(coalesce(b.component, ''), '[^0-9A-Za-z]+', '', 'g')) as same_wds_component,
            a.gaia_id is not null and b.gaia_id is not null and a.gaia_id = b.gaia_id as same_gaia_id
          from stars a
          join stars b
            on a.system_id = b.system_id
           and a.star_id < b.star_id
          join multi_systems ms
            on ms.system_id = a.system_id
        )
        select
          *,
          (same_gaia_id or same_wds_component or same_name_norm) as likely_duplicate
        from pairs
        where ang_sep_arcsec <= {duplicate_pair_max_ang_arcsec}
          and (dist_delta_ly is null or dist_delta_ly <= {duplicate_pair_max_dist_delta_ly})
          and (pm_delta_mas_yr is null or pm_delta_mas_yr <= {duplicate_pair_max_pm_delta_mas_yr})
        """
    )

    duplicate_near_pair_count = int(
        con.execute("select count(*)::bigint from duplicate_pair_candidates").fetchone()[0] or 0
    )
    duplicate_likely_pair_count = int(
        con.execute(
            "select count(*)::bigint from duplicate_pair_candidates where likely_duplicate"
        ).fetchone()[0]
        or 0
    )
    duplicate_high_confidence_pair_count = int(
        con.execute(
            f"""
            select count(*)::bigint
            from duplicate_pair_candidates
            where likely_duplicate
              and ang_sep_arcsec <= {duplicate_pair_high_max_ang_arcsec}
              and (dist_delta_ly is null or dist_delta_ly <= {duplicate_pair_high_max_dist_delta_ly})
              and (pm_delta_mas_yr is null or pm_delta_mas_yr <= {duplicate_pair_high_max_pm_delta_mas_yr})
            """
        ).fetchone()[0]
        or 0
    )
    duplicate_likely_examples = [
        {
            "system_id": int(row[0]),
            "left_star_id": int(row[1]),
            "right_star_id": int(row[2]),
            "left_star_name": row[3] or "",
            "right_star_name": row[4] or "",
            "left_source_catalog": row[5] or "",
            "right_source_catalog": row[6] or "",
            "wds_id": row[7] or row[8] or "",
            "left_component": row[9] or "",
            "right_component": row[10] or "",
            "same_gaia_id": bool(row[11]),
            "same_wds_component": bool(row[12]),
            "same_name_norm": bool(row[13]),
            "ang_sep_arcsec": round(float(row[14] or 0.0), 4),
            "dist_delta_ly": (round(float(row[15]), 4) if row[15] is not None else None),
            "pm_delta_mas_yr": (round(float(row[16]), 4) if row[16] is not None else None),
        }
        for row in con.execute(
            f"""
            select
              system_id,
              left_star_id,
              right_star_id,
              left_star_name,
              right_star_name,
              left_source_catalog,
              right_source_catalog,
              left_wds_id,
              right_wds_id,
              left_component,
              right_component,
              same_gaia_id,
              same_wds_component,
              same_name_norm,
              ang_sep_arcsec,
              dist_delta_ly,
              pm_delta_mas_yr
            from duplicate_pair_candidates
            where likely_duplicate
            order by
              case when same_gaia_id then 0 when same_wds_component then 1 when same_name_norm then 2 else 3 end asc,
              ang_sep_arcsec asc,
              coalesce(dist_delta_ly, 0.0) asc,
              coalesce(pm_delta_mas_yr, 0.0) asc,
              system_id asc
            limit 50
            """
        ).fetchall()
    ]
    duplicate_trap_report = {
        "build_id": build_id,
        "thresholds": {
            "pair_max_ang_arcsec": duplicate_pair_max_ang_arcsec,
            "pair_max_dist_delta_ly": duplicate_pair_max_dist_delta_ly,
            "pair_max_pm_delta_mas_yr": duplicate_pair_max_pm_delta_mas_yr,
            "high_max_ang_arcsec": duplicate_pair_high_max_ang_arcsec,
            "high_max_dist_delta_ly": duplicate_pair_high_max_dist_delta_ly,
            "high_max_pm_delta_mas_yr": duplicate_pair_high_max_pm_delta_mas_yr,
            "fail_on_high": duplicate_fail_on_high,
            "high_pair_max": duplicate_high_pair_max,
            "multiplicity_gaia_duplicate_max": multiplicity_gaia_duplicate_max,
            "multiplicity_wds_component_duplicate_max": multiplicity_wds_component_duplicate_max,
        },
        "exact_key_checks": duplicate_exact_checks,
        "exact_key_totals": {
            "duplicate_groups": duplicate_exact_total_groups,
            "extra_rows": duplicate_exact_total_extra_rows,
            "multiplicity_gaia_duplicate_extra_rows": multiplicity_gaia_duplicate_extra_rows,
            "multiplicity_wds_component_duplicate_extra_rows": multiplicity_wds_component_duplicate_extra_rows,
        },
        "near_pair_totals": {
            "candidate_pairs": duplicate_near_pair_count,
            "likely_duplicate_pairs": duplicate_likely_pair_count,
            "high_confidence_pairs": duplicate_high_confidence_pair_count,
        },
        "exact_key_examples": duplicate_exact_examples,
        "likely_pair_examples": duplicate_likely_examples,
        "notes": [
            "Exact-key checks catch deterministic collisions by identifier/component namespaces.",
            "Exact-key totals are per-key sums; the same physical row can contribute to multiple key checks.",
            "Multiplicity hardening gate enforces exact-duplicate maxima on Gaia ID and WDS component key collisions.",
            "Near-pair checks run within multi-star systems to avoid quadratic whole-catalog scans.",
            "High-confidence pairs are optional QC-gated via SPACEGATE_DUPLICATE_FAIL_ON_HIGH.",
        ],
    }
    write_json(reports_dir / "duplicate_trap_report.json", duplicate_trap_report)

    planet_retracted_rows = int(
        con.execute("select count(*)::bigint from planets where planet_status = 'retracted'").fetchone()[0]
        or 0
    )
    planet_controversial_rows = int(
        con.execute("select count(*)::bigint from planets where planet_status = 'controversial'").fetchone()[0]
        or 0
    )
    planet_candidate_rows = int(
        con.execute("select count(*)::bigint from planets where planet_status = 'candidate'").fetchone()[0]
        or 0
    )
    planet_default_visible_rows = int(
        con.execute("select count(*)::bigint from planets where coalesce(is_default_visible, false)").fetchone()[0]
        or 0
    )
    sol_system_rows = int(
        con.execute(
            "select count(*)::bigint from systems where lower(coalesce(source_catalog, '')) = 'sol_authority'"
        ).fetchone()[0]
        or 0
    )
    sol_star_rows = int(
        con.execute(
            "select count(*)::bigint from stars where lower(coalesce(source_catalog, '')) = 'sol_authority'"
        ).fetchone()[0]
        or 0
    )
    sol_planet_rows = int(
        con.execute(
            "select count(*)::bigint from planets where lower(coalesce(source_catalog, '')) = 'sol_authority'"
        ).fetchone()[0]
        or 0
    )

    def manifest_row_count_match(entry: dict | None) -> bool | None:
        if not entry:
            return None
        value = entry.get("row_count_match")
        if isinstance(value, bool):
            return value
        if value is None:
            return None
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return None

    qc_report = {
        "build_id": build_id,
        "counts": {
            "stars": counts[0],
            "systems": counts[1],
            "planets": counts[2],
            "compact_objects": counts[3],
            "open_clusters": counts[4],
            "open_cluster_memberships": counts[5],
            "superstellar_objects": counts[6],
            "eclipsing_binaries": counts[7],
            "object_identifiers": counts[8],
            "identifier_quarantine": counts[9],
            "aliases": alias_total_count,
            "system_aliases": alias_system_count,
            "star_aliases": alias_star_count,
        },
        "object_family_counts": [
            {"object_family": row[0], "count": row[1]} for row in object_family_counts
        ],
        "gaia_backbone_enabled": enable_gaia_backbone,
        "base_source_catalog": base_source_catalog,
        "gaia_backbone_row_count_check_match": manifest_row_count_match(gaia_backbone_manifest),
        "gaia_classprob_enabled": enable_gaia_backbone and enable_gaia_classprob,
        "gaia_classprob_row_count_check_match": manifest_row_count_match(gaia_classprob_manifest),
        "gaia_ucd_enabled": enable_gaia_ucd,
        "compact_catalogs_enabled": enable_compact_catalogs,
        "superstellar_catalogs_enabled": enable_superstellar_catalogs,
        "eclipsing_catalogs_enabled": enable_eclipsing_catalogs,
        "kepler_eb_enabled": enable_eclipsing_catalogs and enable_kepler_eb,
        "tess_eb_enabled": enable_eclipsing_catalogs and enable_tess_eb,
        "exoplanet_lifecycle_catalogs_enabled": enable_exoplanet_lifecycle_catalogs,
        "sol_authority_enabled": enable_sol_authority,
        "sol_authority_system_rows": sol_system_rows,
        "sol_authority_star_rows": sol_star_rows,
        "sol_authority_planet_rows": sol_planet_rows,
        "planet_classifier_version": planet_classifier_version,
        "planet_lifecycle_status_raw_rows": lifecycle_status_raw_rows,
        "planet_lifecycle_status_matched_rows": lifecycle_status_matched_rows,
        "planet_lifecycle_feature_raw_rows": lifecycle_features_raw_rows,
        "planet_lifecycle_stale_classifier_rows": lifecycle_stale_classifier_rows,
        "planet_lifecycle_candidate_rows": planet_candidate_rows,
        "planet_lifecycle_controversial_rows": planet_controversial_rows,
        "planet_lifecycle_retracted_rows": planet_retracted_rows,
        "planet_lifecycle_default_visible_rows": planet_default_visible_rows,
        "msc_component_raw_rows": msc_component_raw_count,
        "msc_component_retained_rows": msc_component_retained_count,
        "msc_component_dedup_dropped_rows": msc_component_dedup_dropped_count,
        "msc_component_duplicate_groups": msc_component_dedup_group_count,
        "open_cluster_member_min_probability": open_cluster_member_min_probability,
        "gaia_nss_enabled": enable_gaia_nss,
        "gaia_nss_non_single_row_count_check_match": manifest_row_count_match(
            gaia_nss_non_single_manifest
        ),
        "gaia_nss_two_body_row_count_check_match": manifest_row_count_match(
            gaia_nss_two_body_manifest
        ),
        "sbx_enabled": enable_sbx,
        "wds_gaia_xmatch_enabled": enable_wds_gaia_xmatch,
        "gaia_nss_star_count": gaia_nss_star_count,
        "gaia_nss_system_count": gaia_nss_system_count,
        "gaia_nss_two_body_star_count": gaia_nss_two_body_star_count,
        "sbx_star_count": sbx_star_count,
        "sbx_system_count": sbx_system_count,
        "wds_gaia_xmatch_star_count": wds_gaia_xmatch_star_count,
        "wds_gaia_xmatch_pregate_mapping_count": wds_gaia_xmatch_pregate_mapping_count,
        "wds_gaia_xmatch_postgate_mapping_count": wds_gaia_xmatch_postgate_mapping_count,
        "wds_gaia_xmatch_rejected_mapping_count": wds_gaia_xmatch_rejected_mapping_count,
        "wds_gaia_gate_candidate_group_count": wds_gaia_gate_candidate_group_count,
        "wds_gaia_gate_pass_group_count": wds_gaia_gate_pass_group_count,
        "wds_gaia_gate_rejected_group_count": wds_gaia_gate_rejected_group_count,
        "wds_gaia_gate_max_dist_spread_ly": wds_gaia_gate_max_dist_spread_ly,
        "wds_gaia_gate_max_pm_delta_mas_yr": wds_gaia_gate_max_pm_delta_mas_yr,
        "white_dwarf_probability_threshold": WHITE_DWARF_PROB_THRESHOLD,
        "white_dwarf_catalog_pwd_threshold": white_dwarf_catalog_pwd_threshold,
        "white_dwarf_classprob_evidence_count": wd_classprob_evidence_count,
        "white_dwarf_catalog_evidence_count": wd_catalog_evidence_count,
        "white_dwarf_evidence_count": wd_evidence_count,
        "white_dwarf_emitted_count": wd_emitted_count,
        "white_dwarf_mismatch_count": wd_mismatch_count,
        "gaia_ucd_tagged_star_count": int(
            con.execute(
                """
                select count(*)::bigint
                from stars
                where gaia_ucd_hmac_cluster_id is not null
                   or (gaia_ucd_banyan_cluster is not null and trim(gaia_ucd_banyan_cluster) <> '')
                   or gaia_ucd_banyan_probability is not null
                """
            ).fetchone()[0]
            or 0
        ),
        "aliases_enabled": enable_aliases,
        "athyg_alias_crosswalk_enabled": enable_aliases and enable_athyg_alias_crosswalk,
        "athyg_alias_candidate_count": alias_crosswalk_candidate_count,
        "athyg_alias_matched_star_count": alias_crosswalk_matched_star_count,
        "alias_name_override_count": alias_name_override_count,
        "athyg_supplement_merge_enabled": enable_gaia_backbone and enable_athyg_supplement_merge,
        "athyg_merge_resolved_existing_rows": athyg_merge_existing_match_count,
        "athyg_merge_inserted_rows": athyg_merge_insert_count,
        "athyg_merge_quarantined_rows": athyg_merge_quarantine_count,
        "athyg_merge_unresolved_rows": athyg_merge_unresolved_count,
        "athyg_merge_gaia_exact_rows": athyg_merge_direct_gaia_count,
        "athyg_merge_gaia_remap_rows": athyg_merge_remap_count,
        "athyg_merge_direct_legacy_rows": athyg_merge_direct_legacy_id_count,
        "athyg_merge_positional_rows": athyg_merge_positional_count,
        "athyg_merge_positional_ambiguous_rows": athyg_merge_positional_ambiguous_count,
        "object_identifier_count": object_identifier_count,
        "identifier_quarantine_count": identifier_quarantine_count,
        "identifier_gaia_collision_count": identifier_gaia_collision_count,
        "identifier_hip_collision_count": identifier_hip_collision_count,
        "identifier_hd_collision_count": identifier_hd_collision_count,
        "identifier_gate_ambiguous_limit": athyg_merge_ambiguous_limit,
        "identifier_gate_gaia_collision_max": athyg_merge_gaia_collision_max,
        "identifier_gate_hip_collision_max": athyg_merge_hip_collision_max,
        "identifier_gate_hd_collision_max": athyg_merge_hd_collision_max,
        "duplicate_exact_groups": duplicate_exact_total_groups,
        "duplicate_exact_extra_rows": duplicate_exact_total_extra_rows,
        "multiplicity_gaia_duplicate_extra_rows": multiplicity_gaia_duplicate_extra_rows,
        "multiplicity_wds_component_duplicate_extra_rows": multiplicity_wds_component_duplicate_extra_rows,
        "multiplicity_gaia_duplicate_max": multiplicity_gaia_duplicate_max,
        "multiplicity_wds_component_duplicate_max": multiplicity_wds_component_duplicate_max,
        "duplicate_near_pair_count": duplicate_near_pair_count,
        "duplicate_likely_pair_count": duplicate_likely_pair_count,
        "duplicate_high_confidence_pair_count": duplicate_high_confidence_pair_count,
        "duplicate_gate_fail_on_high": duplicate_fail_on_high,
        "duplicate_gate_high_pair_max": duplicate_high_pair_max,
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
                "SBX spectroscopic-binary evidence enabled (SPACEGATE_ENABLE_SBX!=0)."
                if enable_sbx
                else "SBX spectroscopic-binary evidence disabled (SPACEGATE_ENABLE_SBX=0)."
            ),
            "MSC enrichment is conservative in this pass: exact HIP/HD matches only; unmatched MSC components are inserted as new stars.",
            (
                "AT-HYG supplement merge is enabled: deterministic ID precedence + strict positional fallback + quarantine for ambiguous mappings."
                if (enable_gaia_backbone and enable_athyg_supplement_merge)
                else "AT-HYG supplement merge is disabled for this build."
            ),
            (
                f"MSC component dedupe enabled: dropped {msc_component_dedup_dropped_count} duplicate rows "
                f"across {msc_component_dedup_group_count} duplicate WDS component groups."
                if msc_component_dedup_dropped_count > 0
                else "MSC component dedupe found no duplicate WDS component groups."
            ),
            (
                f"Duplicate trap: exact_groups={duplicate_exact_total_groups}, "
                f"near_pairs={duplicate_near_pair_count}, "
                f"likely_pairs={duplicate_likely_pair_count}, "
                f"high_confidence_pairs={duplicate_high_confidence_pair_count}, "
                f"gaia_exact_extra={multiplicity_gaia_duplicate_extra_rows}/{multiplicity_gaia_duplicate_max}, "
                f"wds_component_exact_extra={multiplicity_wds_component_duplicate_extra_rows}/{multiplicity_wds_component_duplicate_max}."
            ),
        ],
    }

    match_report = {
        "build_id": build_id,
        "match_counts": [{"method": row[0], "count": row[1]} for row in match_counts],
        "identifier_resolution_counts": [
            {"method": row[0], "count": row[1]}
            for row in con.execute(
                "select resolution_method, count(*)::bigint from object_identifiers group by resolution_method order by count(*) desc, resolution_method asc"
            ).fetchall()
        ],
        "compact_match_counts": [
            {"method": row[0], "count": row[1]}
            for row in con.execute(
                "select match_method, count(*)::bigint from compact_objects group by match_method order by count(*) desc, match_method asc"
            ).fetchall()
        ],
        "superstellar_type_counts": [
            {"object_type": row[0], "count": row[1]}
            for row in con.execute(
                "select object_type, count(*)::bigint from superstellar_objects group by object_type order by count(*) desc, object_type asc"
            ).fetchall()
        ],
        "eclipsing_source_counts": [
            {"source_catalog": row[0], "count": row[1]}
            for row in con.execute(
                "select source_catalog, count(*)::bigint from eclipsing_binaries group by source_catalog order by count(*) desc, source_catalog asc"
            ).fetchall()
        ],
        "eclipsing_match_counts": [
            {"match_method": row[0], "count": row[1]}
            for row in con.execute(
                "select match_method, count(*)::bigint from eclipsing_binaries group by match_method order by count(*) desc, match_method asc"
            ).fetchall()
        ],
    }

    def manifest_row_count(entry: dict | None) -> int | None:
        if not entry:
            return None
        value = entry.get("row_count")
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    def manifest_bytes(entry: dict | None) -> int | None:
        if not entry:
            return None
        value = entry.get("bytes_written")
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    def safe_view_count(view_name: str) -> int | None:
        try:
            return int(con.execute(f"select count(*)::bigint from {view_name}").fetchone()[0])
        except Exception:
            return None

    total_stars = int(counts[0] or 0)
    total_systems = int(counts[1] or 0)
    total_planets = int(counts[2] or 0)
    total_compact = int(counts[3] or 0)
    total_superstellar = int(counts[6] or 0)
    total_eclipsing = int(counts[7] or 0)

    star_source_counts = {
        str(row[0] or "unknown"): int(row[1] or 0)
        for row in con.execute(
            "select coalesce(source_catalog, 'unknown'), count(*)::bigint from stars group by 1"
        ).fetchall()
    }
    system_source_counts = {
        str(row[0] or "unknown"): int(row[1] or 0)
        for row in con.execute(
            "select coalesce(source_catalog, 'unknown'), count(*)::bigint from systems group by 1"
        ).fetchall()
    }
    planet_source_counts = {
        str(row[0] or "unknown"): int(row[1] or 0)
        for row in con.execute(
            "select coalesce(source_catalog, 'unknown'), count(*)::bigint from planets group by 1"
        ).fetchall()
    }
    compact_source_counts = {
        str(row[0] or "unknown"): int(row[1] or 0)
        for row in con.execute(
            "select coalesce(source_catalog, 'unknown'), count(*)::bigint from compact_objects group by 1"
        ).fetchall()
    }
    superstellar_source_counts = {
        str(row[0] or "unknown"): int(row[1] or 0)
        for row in con.execute(
            "select coalesce(source_catalog, 'unknown'), count(*)::bigint from superstellar_objects group by 1"
        ).fetchall()
    }
    eclipsing_source_counts = {
        str(row[0] or "unknown"): int(row[1] or 0)
        for row in con.execute(
            "select coalesce(source_catalog, 'unknown'), count(*)::bigint from eclipsing_binaries group by 1"
        ).fetchall()
    }

    planet_linked_rows = int(
        con.execute(
            """
            select count(*)::bigint
            from planets
            where match_method is not null
              and lower(match_method) not in ('none', 'unmatched')
            """
        ).fetchone()[0]
        or 0
    )
    planet_lifecycle_observation_counts = {
        str(row[0] or "unknown"): int(row[1] or 0)
        for row in con.execute(
            """
            select coalesce(source_catalog, 'unknown'), count(*)::bigint
            from planet_catalog_observations
            group by 1
            """
        ).fetchall()
    }
    planet_lifecycle_oec_alias_linked_rows = int(
        con.execute(
            """
            select count(*)::bigint
            from planet_catalog_observations
            where lower(coalesce(payload_json, '')) like '%match=oec_%'
            """
        ).fetchone()[0]
        or 0
    )
    planet_lifecycle_oec_alias_linked_planets = int(
        con.execute(
            """
            select count(distinct stable_object_key)::bigint
            from planet_catalog_observations
            where lower(coalesce(payload_json, '')) like '%match=oec_%'
            """
        ).fetchone()[0]
        or 0
    )
    compact_linked_rows = int(
        con.execute(
            """
            select count(*)::bigint
            from compact_objects
            where match_method is not null
              and lower(match_method) not in ('none', 'unmatched')
            """
        ).fetchone()[0]
        or 0
    )
    white_dwarf_catalog_star_rows = int(
        con.execute(
            """
            select count(*)::bigint
            from stars
            where wd_catalog_pwd is not null
            """
        ).fetchone()[0]
        or 0
    )
    white_dwarf_catalog_evidence_rows = int(
        con.execute(
            f"""
            select count(*)::bigint
            from stars
            where coalesce(wd_catalog_pwd, 0.0) >= {white_dwarf_catalog_pwd_threshold}
            """
        ).fetchone()[0]
        or 0
    )
    gaia_ucd_tagged_rows = int(
        con.execute(
            """
            select count(*)::bigint
            from stars
            where gaia_ucd_hmac_cluster_id is not null
               or (gaia_ucd_banyan_cluster is not null and trim(gaia_ucd_banyan_cluster) <> '')
               or gaia_ucd_banyan_probability is not null
            """
        ).fetchone()[0]
        or 0
    )
    eclipsing_linked_rows = int(
        con.execute(
            """
            select count(*)::bigint
            from eclipsing_binaries
            where match_method is not null
              and lower(match_method) not in ('none', 'unmatched')
            """
        ).fetchone()[0]
        or 0
    )

    star_evidence_row = con.execute(
        """
        with flags as (
          select
            case
              when coalesce(gaia_non_single_star, false)
                or coalesce(gaia_nss_solution_count, 0) > 0
                or multiplicity_source_catalogs_json like '%"gaia_nss"%'
                or multiplicity_source_catalogs_json like '%"gaia_nss_two_body"%'
              then 1 else 0
            end as has_nss,
            case
              when wds_id is not null
                or multiplicity_source_catalogs_json like '%"wds_gaia_xmatch"%'
              then 1 else 0
            end as has_wds,
            case
              when source_catalog = 'msc'
                or multiplicity_source_catalogs_json like '%"msc"%'
              then 1 else 0
            end as has_msc,
            case
              when sbx_sn is not null
              then 1 else 0
            end as has_sbx,
            case
              when classprob_dsc_combmod_whitedwarf is not null
                or classprob_dsc_specmod_whitedwarf is not null
              then 1 else 0
            end as has_classprob,
            case
              when open_cluster_tags_json is not null
                and trim(open_cluster_tags_json) <> ''
                and trim(open_cluster_tags_json) <> '[]'
              then 1 else 0
            end as has_cluster_tag
          from stars
        )
        select
          count(*)::bigint as total_rows,
          sum(has_nss)::bigint as nss_rows,
          sum(has_wds)::bigint as wds_rows,
          sum(has_msc)::bigint as msc_rows,
          sum(has_sbx)::bigint as sbx_rows,
          sum(has_classprob)::bigint as classprob_rows,
          sum(has_cluster_tag)::bigint as cluster_tag_rows,
          sum(case when has_nss = 1 and has_wds = 1 then 1 else 0 end)::bigint as nss_wds_rows,
          sum(case when has_nss = 1 and has_msc = 1 then 1 else 0 end)::bigint as nss_msc_rows,
          sum(case when has_nss = 1 and has_sbx = 1 then 1 else 0 end)::bigint as nss_sbx_rows,
          sum(case when has_wds = 1 and has_msc = 1 then 1 else 0 end)::bigint as wds_msc_rows,
          sum(case when has_wds = 1 and has_sbx = 1 then 1 else 0 end)::bigint as wds_sbx_rows,
          sum(case when has_msc = 1 and has_sbx = 1 then 1 else 0 end)::bigint as msc_sbx_rows,
          sum(case when has_nss = 1 and has_wds = 1 and has_msc = 1 then 1 else 0 end)::bigint as nss_wds_msc_rows,
          sum(case when has_nss = 1 and has_wds = 1 and has_msc = 1 and has_sbx = 1 then 1 else 0 end)::bigint as nss_wds_msc_sbx_rows
        from flags
        """
    ).fetchone()
    system_evidence_row = con.execute(
        """
        select
          count(*)::bigint as total_rows,
          sum(case when has_gaia_nss_evidence then 1 else 0 end)::bigint as nss_rows,
          sum(case when has_wds_evidence then 1 else 0 end)::bigint as wds_rows,
          sum(case when has_msc_evidence then 1 else 0 end)::bigint as msc_rows,
          sum(case when has_sbx_evidence then 1 else 0 end)::bigint as sbx_rows,
          sum(case when has_gaia_nss_evidence and has_wds_evidence then 1 else 0 end)::bigint as nss_wds_rows,
          sum(case when has_gaia_nss_evidence and has_msc_evidence then 1 else 0 end)::bigint as nss_msc_rows,
          sum(case when has_gaia_nss_evidence and has_sbx_evidence then 1 else 0 end)::bigint as nss_sbx_rows,
          sum(case when has_wds_evidence and has_msc_evidence then 1 else 0 end)::bigint as wds_msc_rows,
          sum(case when has_wds_evidence and has_sbx_evidence then 1 else 0 end)::bigint as wds_sbx_rows,
          sum(case when has_msc_evidence and has_sbx_evidence then 1 else 0 end)::bigint as msc_sbx_rows,
          sum(case when has_gaia_nss_evidence and has_wds_evidence and has_msc_evidence then 1 else 0 end)::bigint as nss_wds_msc_rows,
          sum(case when has_gaia_nss_evidence and has_wds_evidence and has_msc_evidence and has_sbx_evidence then 1 else 0 end)::bigint as nss_wds_msc_sbx_rows
        from systems
        """
    ).fetchone()

    star_nss_rows = int(star_evidence_row[1] or 0)
    star_wds_rows = int(star_evidence_row[2] or 0)
    star_msc_rows = int(star_evidence_row[3] or 0)
    star_sbx_rows = int(star_evidence_row[4] or 0)
    system_nss_rows = int(system_evidence_row[1] or 0)
    system_wds_rows = int(system_evidence_row[2] or 0)
    system_msc_rows = int(system_evidence_row[3] or 0)
    system_sbx_rows = int(system_evidence_row[4] or 0)

    def pair_overlap_rows(
        scope: str,
        left: str,
        right: str,
        left_count: int,
        right_count: int,
        intersection_count: int,
        total_count: int,
    ) -> dict:
        union_count = max(left_count + right_count - intersection_count, 0)
        return {
            "scope": scope,
            "left_catalog": left,
            "right_catalog": right,
            "left_count": int(left_count),
            "right_count": int(right_count),
            "intersection_count": int(intersection_count),
            "union_count": int(union_count),
            "jaccard_pct": round(pct_value(intersection_count, union_count), 2),
            "intersection_pct_of_scope": round(pct_value(intersection_count, total_count), 2),
            "intersection_pct_of_left": round(pct_value(intersection_count, left_count), 2),
            "intersection_pct_of_right": round(pct_value(intersection_count, right_count), 2),
        }

    star_pairwise = [
        pair_overlap_rows(
            "stars",
            "gaia_nss",
            "wds",
            star_nss_rows,
            star_wds_rows,
            int(star_evidence_row[7] or 0),
            total_stars,
        ),
        pair_overlap_rows(
            "stars",
            "gaia_nss",
            "msc",
            star_nss_rows,
            star_msc_rows,
            int(star_evidence_row[8] or 0),
            total_stars,
        ),
        pair_overlap_rows(
            "stars",
            "gaia_nss",
            "sbx",
            star_nss_rows,
            star_sbx_rows,
            int(star_evidence_row[9] or 0),
            total_stars,
        ),
        pair_overlap_rows(
            "stars",
            "wds",
            "msc",
            star_wds_rows,
            star_msc_rows,
            int(star_evidence_row[10] or 0),
            total_stars,
        ),
        pair_overlap_rows(
            "stars",
            "wds",
            "sbx",
            star_wds_rows,
            star_sbx_rows,
            int(star_evidence_row[11] or 0),
            total_stars,
        ),
        pair_overlap_rows(
            "stars",
            "msc",
            "sbx",
            star_msc_rows,
            star_sbx_rows,
            int(star_evidence_row[12] or 0),
            total_stars,
        ),
    ]
    system_pairwise = [
        pair_overlap_rows(
            "systems",
            "gaia_nss",
            "wds",
            system_nss_rows,
            system_wds_rows,
            int(system_evidence_row[5] or 0),
            total_systems,
        ),
        pair_overlap_rows(
            "systems",
            "gaia_nss",
            "msc",
            system_nss_rows,
            system_msc_rows,
            int(system_evidence_row[6] or 0),
            total_systems,
        ),
        pair_overlap_rows(
            "systems",
            "gaia_nss",
            "sbx",
            system_nss_rows,
            system_sbx_rows,
            int(system_evidence_row[7] or 0),
            total_systems,
        ),
        pair_overlap_rows(
            "systems",
            "wds",
            "msc",
            system_wds_rows,
            system_msc_rows,
            int(system_evidence_row[8] or 0),
            total_systems,
        ),
        pair_overlap_rows(
            "systems",
            "wds",
            "sbx",
            system_wds_rows,
            system_sbx_rows,
            int(system_evidence_row[9] or 0),
            total_systems,
        ),
        pair_overlap_rows(
            "systems",
            "msc",
            "sbx",
            system_msc_rows,
            system_sbx_rows,
            int(system_evidence_row[10] or 0),
            total_systems,
        ),
    ]

    def add_catalog_contribution(
        rows: list[dict],
        *,
        catalog: str,
        domain: str,
        domain_total: int,
        input_rows: int | None = None,
        input_bytes: int | None = None,
        direct_rows: int = 0,
        evidence_rows: int = 0,
        linked_rows: int = 0,
        notes: str | None = None,
    ) -> None:
        direct_pct = round(pct_value(direct_rows, domain_total), 2)
        evidence_pct = round(pct_value(evidence_rows, domain_total), 2)
        linked_pct = round(pct_value(linked_rows, domain_total), 2)
        utility_score = round((0.45 * direct_pct) + (0.35 * evidence_pct) + (0.20 * linked_pct), 2)
        if max(direct_pct, evidence_pct, linked_pct) >= 20.0:
            utility_tier = "indispensable"
        elif max(direct_pct, evidence_pct, linked_pct) >= 5.0:
            utility_tier = "strong"
        elif max(direct_pct, evidence_pct, linked_pct) > 0.0:
            utility_tier = "situational"
        else:
            utility_tier = "meh"
        rows.append(
            {
                "catalog": catalog,
                "domain": domain,
                "domain_total": int(domain_total),
                "input_rows": int(input_rows) if input_rows is not None else None,
                "input_bytes": int(input_bytes) if input_bytes is not None else None,
                "direct_rows": int(direct_rows),
                "evidence_rows": int(evidence_rows),
                "linked_rows": int(linked_rows),
                "direct_pct_of_domain": direct_pct,
                "evidence_pct_of_domain": evidence_pct,
                "linked_pct_of_domain": linked_pct,
                "utility_score": utility_score,
                "utility_tier": utility_tier,
                "notes": notes or "",
            }
        )

    sbx_input_bytes = sum(
        int(manifest_bytes(entry) or 0)
        for entry in (
            sbx_systems_manifest,
            sbx_alias_manifest,
            sbx_config_manifest,
            sbx_orbits_manifest,
        )
    )
    if sbx_input_bytes == 0:
        sbx_input_bytes = None

    catalog_contributions: list[dict] = []
    add_catalog_contribution(
        catalog_contributions,
        catalog=("gaia_dr3" if enable_gaia_backbone else "athyg"),
        domain="stars",
        domain_total=total_stars,
        input_rows=(manifest_row_count(gaia_backbone_manifest) if enable_gaia_backbone else None),
        input_bytes=(manifest_bytes(gaia_backbone_manifest) if enable_gaia_backbone else None),
        direct_rows=int(star_source_counts.get("gaia_dr3" if enable_gaia_backbone else "athyg", 0)),
        evidence_rows=0,
        linked_rows=0,
        notes=("canonical backbone inventory" if enable_gaia_backbone else "legacy canonical inventory"),
    )
    if enable_gaia_backbone and enable_athyg_supplement_merge:
        add_catalog_contribution(
            catalog_contributions,
            catalog="athyg",
            domain="stars",
            domain_total=total_stars,
            input_rows=int(athyg_merge_existing_match_count + athyg_merge_insert_count + athyg_merge_quarantine_count),
            input_bytes=manifest_bytes(athyg_p1) if athyg_p1 else None,
            direct_rows=int(star_source_counts.get("athyg", 0)),
            evidence_rows=0,
            linked_rows=0,
            notes="supplement merge source",
        )
    add_catalog_contribution(
        catalog_contributions,
        catalog="gaia_nss_non_single_star",
        domain="stars",
        domain_total=total_stars,
        input_rows=manifest_row_count(gaia_nss_non_single_manifest),
        input_bytes=manifest_bytes(gaia_nss_non_single_manifest),
        direct_rows=0,
        evidence_rows=star_nss_rows,
        linked_rows=star_nss_rows,
        notes="multiplicity evidence",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="gaia_nss_non_single_star",
        domain="systems",
        domain_total=total_systems,
        input_rows=manifest_row_count(gaia_nss_non_single_manifest),
        input_bytes=manifest_bytes(gaia_nss_non_single_manifest),
        direct_rows=0,
        evidence_rows=system_nss_rows,
        linked_rows=system_nss_rows,
        notes="system multiplicity evidence",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="wds",
        domain="stars",
        domain_total=total_stars,
        input_rows=safe_view_count("wds_raw"),
        input_bytes=manifest_bytes(wds_manifest),
        direct_rows=0,
        evidence_rows=star_wds_rows,
        linked_rows=star_wds_rows,
        notes="double-star evidence",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="wds",
        domain="systems",
        domain_total=total_systems,
        input_rows=safe_view_count("wds_raw"),
        input_bytes=manifest_bytes(wds_manifest),
        direct_rows=0,
        evidence_rows=system_wds_rows,
        linked_rows=system_wds_rows,
        notes="double-star system evidence",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="msc",
        domain="stars",
        domain_total=total_stars,
        input_rows=safe_view_count("msc_raw"),
        input_bytes=manifest_bytes(msc_manifest),
        direct_rows=int(star_source_counts.get("msc", 0)),
        evidence_rows=star_msc_rows,
        linked_rows=star_msc_rows,
        notes=(
            "hierarchical multiplicity source; "
            f"component dedupe dropped {msc_component_dedup_dropped_count} duplicate rows"
        ),
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="msc",
        domain="systems",
        domain_total=total_systems,
        input_rows=safe_view_count("msc_raw"),
        input_bytes=manifest_bytes(msc_manifest),
        direct_rows=0,
        evidence_rows=system_msc_rows,
        linked_rows=system_msc_rows,
        notes="hierarchical system evidence",
    )
    if enable_sbx:
        add_catalog_contribution(
            catalog_contributions,
            catalog="sbx",
            domain="stars",
            domain_total=total_stars,
            input_rows=safe_view_count("sbx_raw"),
            input_bytes=sbx_input_bytes,
            direct_rows=0,
            evidence_rows=star_sbx_rows,
            linked_rows=star_sbx_rows,
            notes="spectroscopic-binary evidence via exact Gaia/HIP/HD identifier joins",
        )
        add_catalog_contribution(
            catalog_contributions,
            catalog="sbx",
            domain="systems",
            domain_total=total_systems,
            input_rows=safe_view_count("sbx_raw"),
            input_bytes=sbx_input_bytes,
            direct_rows=0,
            evidence_rows=system_sbx_rows,
            linked_rows=system_sbx_rows,
            notes="spectroscopic-binary system evidence",
        )
    add_catalog_contribution(
        catalog_contributions,
        catalog="orb6",
        domain="systems",
        domain_total=total_systems,
        input_rows=safe_view_count("orb6_raw"),
        input_bytes=manifest_bytes(orb6_manifest),
        direct_rows=0,
        evidence_rows=0,
        linked_rows=0,
        notes="orbit support catalog",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="nasa_exoplanet_archive",
        domain="planets",
        domain_total=total_planets,
        input_rows=safe_view_count("nasa_raw"),
        input_bytes=manifest_bytes(nasa_manifest),
        direct_rows=int(planet_source_counts.get("nasa_exoplanet_archive", 0)),
        evidence_rows=0,
        linked_rows=planet_linked_rows,
        notes="exoplanet inventory",
    )
    if enable_sol_authority:
        add_catalog_contribution(
            catalog_contributions,
            catalog="sol_authority",
            domain="systems",
            domain_total=total_systems,
            input_rows=manifest_row_count(sol_authority_manifest),
            input_bytes=manifest_bytes(sol_authority_manifest),
            direct_rows=int(system_source_counts.get("sol_authority", 0)),
            evidence_rows=0,
            linked_rows=0,
            notes="authoritative Sol-system root record",
        )
        add_catalog_contribution(
            catalog_contributions,
            catalog="sol_authority",
            domain="stars",
            domain_total=total_stars,
            input_rows=manifest_row_count(sol_authority_manifest),
            input_bytes=manifest_bytes(sol_authority_manifest),
            direct_rows=int(star_source_counts.get("sol_authority", 0)),
            evidence_rows=0,
            linked_rows=0,
            notes="authoritative solar host record (Sun)",
        )
        add_catalog_contribution(
            catalog_contributions,
            catalog="sol_authority",
            domain="planets",
            domain_total=total_planets,
            input_rows=manifest_row_count(sol_authority_manifest),
            input_bytes=manifest_bytes(sol_authority_manifest),
            direct_rows=int(planet_source_counts.get("sol_authority", 0)),
            evidence_rows=0,
            linked_rows=int(planet_source_counts.get("sol_authority", 0)),
            notes="authoritative major and dwarf Sol-system body records",
        )
    if enable_exoplanet_lifecycle_catalogs:
        oec_direct_rows = int(planet_lifecycle_observation_counts.get("open_exoplanet_catalogue", 0))
        oec_linked_rows = int(
            con.execute(
                """
                select count(distinct stable_object_key)::bigint
                from planet_catalog_observations
                where source_catalog = 'open_exoplanet_catalogue'
                   or lower(coalesce(payload_json, '')) like '%match=oec_%'
                """
            ).fetchone()[0]
            or 0
        )
        add_catalog_contribution(
            catalog_contributions,
            catalog="exoplanet_eu",
            domain="planets",
            domain_total=total_planets,
            input_rows=manifest_row_count(exoplanet_eu_manifest),
            input_bytes=manifest_bytes(exoplanet_eu_manifest),
            direct_rows=0,
            evidence_rows=int(planet_lifecycle_observation_counts.get("exoplanet_eu", 0)),
            linked_rows=int(planet_lifecycle_observation_counts.get("exoplanet_eu", 0)),
            notes="lifecycle status evidence",
        )
        add_catalog_contribution(
            catalog_contributions,
            catalog="hwc",
            domain="planets",
            domain_total=total_planets,
            input_rows=manifest_row_count(hwc_manifest),
            input_bytes=manifest_bytes(hwc_manifest),
            direct_rows=0,
            evidence_rows=int(planet_lifecycle_observation_counts.get("hwc", 0)),
            linked_rows=int(planet_lifecycle_observation_counts.get("hwc", 0)),
            notes="habitability reference features",
        )
        add_catalog_contribution(
            catalog_contributions,
            catalog="open_exoplanet_catalogue",
            domain="planets",
            domain_total=total_planets,
            input_rows=manifest_row_count(open_exoplanet_catalogue_manifest),
            input_bytes=manifest_bytes(open_exoplanet_catalogue_manifest),
            direct_rows=0,
            evidence_rows=oec_direct_rows + planet_lifecycle_oec_alias_linked_rows,
            linked_rows=oec_linked_rows,
            notes=(
                "alias and architecture support; "
                f"alias-assisted lifecycle matches={planet_lifecycle_oec_alias_linked_rows} "
                f"(distinct planets={planet_lifecycle_oec_alias_linked_planets})"
            ),
        )
    add_catalog_contribution(
        catalog_contributions,
        catalog="gaia_classprob",
        domain="stars",
        domain_total=total_stars,
        input_rows=manifest_row_count(gaia_classprob_manifest),
        input_bytes=manifest_bytes(gaia_classprob_manifest),
        direct_rows=0,
        evidence_rows=int(star_evidence_row[5] or 0),
        linked_rows=int(star_evidence_row[5] or 0),
        notes="compact/remnant probability evidence",
    )
    if enable_gaia_ucd:
        add_catalog_contribution(
            catalog_contributions,
            catalog="gaia_ucd",
            domain="stars",
            domain_total=total_stars,
            input_rows=manifest_row_count(gaia_ucd_manifest),
            input_bytes=manifest_bytes(gaia_ucd_manifest),
            direct_rows=0,
            evidence_rows=gaia_ucd_tagged_rows,
            linked_rows=gaia_ucd_tagged_rows,
            notes="Gaia ultracool dwarf membership/classification tags (HMAC/BANYAN)",
        )
    add_catalog_contribution(
        catalog_contributions,
        catalog="white_dwarf",
        domain="stars",
        domain_total=total_stars,
        input_rows=safe_view_count("white_dwarf_raw"),
        input_bytes=manifest_bytes(white_dwarf_manifest),
        direct_rows=0,
        evidence_rows=white_dwarf_catalog_evidence_rows,
        linked_rows=white_dwarf_catalog_star_rows,
        notes="Gaia EDR3 white dwarf catalog evidence and model metadata",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="clusters",
        domain="stars",
        domain_total=total_stars,
        input_rows=safe_view_count("open_cluster_members_raw"),
        input_bytes=manifest_bytes(clusters_members_manifest),
        direct_rows=0,
        evidence_rows=int(star_evidence_row[6] or 0),
        linked_rows=int(star_evidence_row[6] or 0),
        notes="open-cluster membership tags",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="atnf",
        domain="compact_objects",
        domain_total=total_compact,
        input_rows=safe_view_count("atnf_raw"),
        input_bytes=manifest_bytes(atnf_manifest),
        direct_rows=int(compact_source_counts.get("atnf", 0)),
        evidence_rows=0,
        linked_rows=compact_linked_rows,
        notes="pulsar compact-object support",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="magnetar",
        domain="compact_objects",
        domain_total=total_compact,
        input_rows=safe_view_count("magnetar_raw"),
        input_bytes=manifest_bytes(magnetar_manifest),
        direct_rows=int(compact_source_counts.get("magnetar", 0)),
        evidence_rows=0,
        linked_rows=compact_linked_rows,
        notes="magnetar compact-object support",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="white_dwarf",
        domain="compact_objects",
        domain_total=total_compact,
        input_rows=safe_view_count("white_dwarf_raw"),
        input_bytes=manifest_bytes(white_dwarf_manifest),
        direct_rows=int(compact_source_counts.get("white_dwarf", 0)),
        evidence_rows=0,
        linked_rows=int(compact_source_counts.get("white_dwarf", 0)),
        notes="white-dwarf compact-object inventory (Gaia ID exact joins)",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="clusters",
        domain="superstellar_objects",
        domain_total=total_superstellar,
        input_rows=safe_view_count("open_clusters_raw"),
        input_bytes=manifest_bytes(clusters_table1_manifest),
        direct_rows=int(superstellar_source_counts.get("clusters", 0)),
        evidence_rows=0,
        linked_rows=0,
        notes="open-cluster object inventory",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="snr",
        domain="superstellar_objects",
        domain_total=total_superstellar,
        input_rows=safe_view_count("snr_raw"),
        input_bytes=manifest_bytes(snr_manifest),
        direct_rows=int(superstellar_source_counts.get("snr", 0)),
        evidence_rows=0,
        linked_rows=0,
        notes="supernova remnant inventory",
    )
    add_catalog_contribution(
        catalog_contributions,
        catalog="debcat",
        domain="eclipsing_binaries",
        domain_total=total_eclipsing,
        input_rows=safe_view_count("debcat_raw"),
        input_bytes=manifest_bytes(debcat_manifest),
        direct_rows=int(eclipsing_source_counts.get("debcat", 0)),
        evidence_rows=0,
        linked_rows=eclipsing_linked_rows,
        notes="detached eclipsing binaries",
    )
    if enable_eclipsing_catalogs and enable_kepler_eb:
        add_catalog_contribution(
            catalog_contributions,
            catalog="kepler_eb",
            domain="eclipsing_binaries",
            domain_total=total_eclipsing,
            input_rows=safe_view_count("kepler_eb_raw"),
            input_bytes=manifest_bytes(kepler_eb_manifest),
            direct_rows=int(eclipsing_source_counts.get("kepler_eb", 0)),
            evidence_rows=0,
            linked_rows=eclipsing_linked_rows,
            notes="Kepler eclipsing binaries",
        )
    if enable_eclipsing_catalogs and enable_tess_eb:
        add_catalog_contribution(
            catalog_contributions,
            catalog="tess_eb",
            domain="eclipsing_binaries",
            domain_total=total_eclipsing,
            input_rows=safe_view_count("tess_eb_raw"),
            input_bytes=manifest_bytes(tess_eb_manifest),
            direct_rows=int(eclipsing_source_counts.get("tess_eb", 0)),
            evidence_rows=0,
            linked_rows=eclipsing_linked_rows,
            notes="TESS eclipsing binaries",
        )

    source_inputs = []
    for catalog_name, entry in [
        ("gaia_dr3_backbone", gaia_backbone_manifest),
        ("gaia_nss_non_single_star", gaia_nss_non_single_manifest),
        ("gaia_nss_two_body_orbit", gaia_nss_two_body_manifest),
        ("sbx_systems", sbx_systems_manifest),
        ("sbx_alias", sbx_alias_manifest),
        ("sbx_configurations", sbx_config_manifest),
        ("sbx_orbits", sbx_orbits_manifest),
        ("gaia_classprob", gaia_classprob_manifest),
        ("gaia_ucd", gaia_ucd_manifest),
        ("wds", wds_manifest),
        ("wds_gaia_xmatch_best", wds_gaia_xmatch_manifest),
        ("msc", msc_manifest),
        ("orb6", orb6_manifest),
        ("nasa_exoplanet_archive", nasa_manifest),
        ("sol_authority", sol_authority_manifest),
        ("atnf", atnf_manifest),
        ("magnetar", magnetar_manifest),
        ("white_dwarf", white_dwarf_manifest),
        ("clusters_table1", clusters_table1_manifest),
        ("clusters_members", clusters_members_manifest),
        ("snr", snr_manifest),
        ("debcat", debcat_manifest),
        ("kepler_eb", kepler_eb_manifest),
        ("tess_eb", tess_eb_manifest),
        ("exoplanet_eu", exoplanet_eu_manifest),
        ("open_exoplanet_catalogue", open_exoplanet_catalogue_manifest),
        ("hwc", hwc_manifest),
        ("athyg_part1", athyg_p1),
        ("athyg_part2", athyg_p2),
    ]:
        if not entry:
            continue
        source_inputs.append(
            {
                "catalog": catalog_name,
                "source_name": entry.get("source_name"),
                "source_version": entry.get("source_version"),
                "dest_path": entry.get("dest_path"),
                "url": entry.get("url"),
                "retrieved_at": entry.get("retrieved_at"),
                "row_count": manifest_row_count(entry),
                "bytes_written": manifest_bytes(entry),
                "sha256": entry.get("sha256"),
            }
        )
    source_inputs_fingerprint = hashlib.sha256(
        json.dumps(
            sorted(
                source_inputs,
                key=lambda row: (
                    str(row.get("catalog") or ""),
                    str(row.get("source_name") or ""),
                    str(row.get("dest_path") or ""),
                ),
            ),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    qc_report["source_inputs_fingerprint"] = source_inputs_fingerprint
    qc_report["astrometry_boundary_strategy"] = astrometry_boundary_strategy
    qc_report["astrometry_boundary_min_parallax_mas"] = (
        float(gaia_backbone_min_parallax_mas) if enable_gaia_backbone else None
    )
    qc_report["astrometry_quality_policy"] = {
        "slice_min_parallax_over_error": slice_min_parallax_over_error,
        "slice_max_parallax_error_mas": slice_max_parallax_error_mas,
        "slice_max_ruwe": slice_max_ruwe,
    }

    def table_fingerprint(
        table_name: str,
        hash_expr_sql: str,
    ) -> dict[str, int | str]:
        row = con.execute(
            f"""
            select
              count(*)::bigint as row_count,
              coalesce(bit_xor({hash_expr_sql}), 0)::ubigint as xor_hash,
              coalesce(min({hash_expr_sql}), 0)::ubigint as min_hash,
              coalesce(max({hash_expr_sql}), 0)::ubigint as max_hash
            from {table_name}
            """
        ).fetchone()
        row_count = int(row[0] or 0)
        xor_hash = int(row[1] or 0)
        min_hash = int(row[2] or 0)
        max_hash = int(row[3] or 0)
        return {
            "row_count": row_count,
            "xor_hash_uint64": xor_hash,
            "xor_hash_hex": f"{xor_hash:016x}",
            "min_hash_uint64": min_hash,
            "max_hash_uint64": max_hash,
        }

    stars_fingerprint = table_fingerprint(
        "stars",
        "hash("
        "coalesce(stable_object_key,''),"
        "coalesce(source_catalog,''),"
        "coalesce(cast(gaia_id as varchar),''),"
        "coalesce(cast(hip_id as varchar),''),"
        "coalesce(cast(hd_id as varchar),''),"
        "coalesce(spectral_class,''),"
        "coalesce(spectral_subtype,''),"
        "coalesce(luminosity_class,''),"
        "coalesce(cast(round(dist_ly, 6) as varchar),''),"
        "coalesce(multiplicity_match_method,'')"
        ")",
    )
    systems_fingerprint = table_fingerprint(
        "systems",
        "hash("
        "coalesce(stable_object_key,''),"
        "coalesce(system_name_norm,''),"
        "coalesce(cast(gaia_id as varchar),''),"
        "coalesce(wds_id,''),"
        "coalesce(cast(round(dist_ly, 6) as varchar),''),"
        "coalesce(grouping_basis,''),"
        "coalesce(cast(has_wds_evidence as varchar),''),"
        "coalesce(cast(has_msc_evidence as varchar),'')"
        ")",
    )
    planets_fingerprint = table_fingerprint(
        "planets",
        "hash("
        "coalesce(stable_object_key,''),"
        "coalesce(source_catalog,''),"
        "coalesce(cast(system_id as varchar),''),"
        "coalesce(cast(star_id as varchar),''),"
        "coalesce(planet_status,''),"
        "coalesce(cast(is_default_visible as varchar),''),"
        "coalesce(cast(is_tombstoned as varchar),''),"
        "coalesce(cast(round(insol_earth, 6) as varchar),''),"
        "coalesce(cast(round(spacegate_hab_score, 6) as varchar),'')"
        ")",
    )

    catalog_contribution_report = {
        "build_id": build_id,
        "generated_at": ingested_at,
        "source_inputs_fingerprint": source_inputs_fingerprint,
        "totals": {
            "stars": total_stars,
            "systems": total_systems,
            "planets": total_planets,
            "compact_objects": total_compact,
            "superstellar_objects": total_superstellar,
            "eclipsing_binaries": total_eclipsing,
        },
        "source_inputs": source_inputs,
        "catalog_contributions": sorted(
            catalog_contributions,
            key=lambda row: (
                float(row.get("utility_score") or 0.0),
                int(row.get("direct_rows") or 0),
                int(row.get("evidence_rows") or 0),
            ),
            reverse=True,
        ),
        "overlaps": {
            "star_evidence": {
                "set_sizes": {
                    "gaia_nss": star_nss_rows,
                    "wds": star_wds_rows,
                    "msc": star_msc_rows,
                    "sbx": star_sbx_rows,
                },
                "pairwise": star_pairwise,
                "triple_overlap_count": int(star_evidence_row[13] or 0),
                "triple_overlap_pct_of_stars": round(
                    pct_value(int(star_evidence_row[13] or 0), total_stars),
                    2,
                ),
                "quadruple_overlap_count": int(star_evidence_row[14] or 0),
                "quadruple_overlap_pct_of_stars": round(
                    pct_value(int(star_evidence_row[14] or 0), total_stars),
                    2,
                ),
            },
            "system_evidence": {
                "set_sizes": {
                    "gaia_nss": system_nss_rows,
                    "wds": system_wds_rows,
                    "msc": system_msc_rows,
                    "sbx": system_sbx_rows,
                },
                "pairwise": system_pairwise,
                "triple_overlap_count": int(system_evidence_row[11] or 0),
                "triple_overlap_pct_of_systems": round(
                    pct_value(int(system_evidence_row[11] or 0), total_systems),
                    2,
                ),
                "quadruple_overlap_count": int(system_evidence_row[12] or 0),
                "quadruple_overlap_pct_of_systems": round(
                    pct_value(int(system_evidence_row[12] or 0), total_systems),
                    2,
                ),
            },
        },
        "notes": [
            "Utility rows are domain-scoped (stars/systems/planets/side tables) with percent-of-domain metrics.",
            "Direct rows: objects emitted directly from a source_catalog.",
            "Evidence rows: objects carrying catalog evidence but not necessarily emitted from that source_catalog.",
            "Linked rows: rows with non-null/non-'unmatched' match_method for matchable side tables.",
            "Pairwise overlap metrics report intersection/union and Jaccard percentages.",
        ],
    }

    athyg_retirement_report = {
        "build_id": build_id,
        "generated_at": ingested_at,
        "gaia_backbone_enabled": enable_gaia_backbone,
        "athyg_alias_crosswalk_enabled": enable_aliases and enable_athyg_alias_crosswalk,
        "athyg_supplement_merge_enabled": enable_gaia_backbone and enable_athyg_supplement_merge,
        "source_inputs_fingerprint": source_inputs_fingerprint,
        "athyg_metrics": {
            "athyg_source_star_rows": int(star_source_counts.get("athyg", 0)),
            "athyg_merge_resolved_existing_rows": int(athyg_merge_existing_match_count),
            "athyg_merge_inserted_rows": int(athyg_merge_insert_count),
            "athyg_merge_quarantined_rows": int(athyg_merge_quarantine_count),
            "athyg_merge_unresolved_rows": int(athyg_merge_unresolved_count),
            "athyg_merge_gaia_exact_rows": int(athyg_merge_direct_gaia_count),
            "athyg_merge_gaia_remap_rows": int(athyg_merge_remap_count),
            "athyg_merge_direct_legacy_rows": int(athyg_merge_direct_legacy_id_count),
            "athyg_merge_positional_rows": int(athyg_merge_positional_count),
            "athyg_merge_positional_ambiguous_rows": int(athyg_merge_positional_ambiguous_count),
            "athyg_alias_candidates": int(alias_crosswalk_candidate_count),
            "athyg_alias_matched_stars": int(alias_crosswalk_matched_star_count),
        },
        "retirement_readiness": {
            "default_path_has_no_athyg_dependency": not (
                (enable_aliases and enable_athyg_alias_crosswalk)
                or (enable_gaia_backbone and enable_athyg_supplement_merge)
            ),
            "athyg_rows_present_in_core": int(star_source_counts.get("athyg", 0)) > 0,
        },
        "notes": [
            "AT-HYG is transitional compatibility input only; canonical inventory is Gaia-first.",
            "Use this report when deciding whether compatibility toggles can stay disabled in production defaults.",
        ],
    }

    determinism_report = {
        "build_id": build_id,
        "generated_at": ingested_at,
        "transform_version": transform_version,
        "build_layer": build_layer,
        "slice_profile_id": slice_profile_id,
        "slice_profile_version": slice_profile_version,
        "source_inputs_fingerprint": source_inputs_fingerprint,
        "table_fingerprints": {
            "stars": stars_fingerprint,
            "systems": systems_fingerprint,
            "planets": planets_fingerprint,
        },
        "notes": [
            "Fingerprints are deterministic for identical inputs/transforms and are used for rerun-proof checks.",
            "Comparison key: source_inputs_fingerprint + transform_version + build_layer + slice profile.",
        ],
    }

    con.execute(
        f"""
        insert into build_metadata values
          ('source_inputs_fingerprint', {sql_literal(source_inputs_fingerprint)}),
          ('determinism_stars_xor_hash', {sql_literal(stars_fingerprint['xor_hash_hex'])}),
          ('determinism_systems_xor_hash', {sql_literal(systems_fingerprint['xor_hash_hex'])}),
          ('determinism_planets_xor_hash', {sql_literal(planets_fingerprint['xor_hash_hex'])})
        """
    )

    write_json(reports_dir / "qc_report.json", qc_report)
    write_json(reports_dir / "match_report.json", match_report)
    write_json(reports_dir / "duplicate_trap_report.json", duplicate_trap_report)
    write_json(reports_dir / "planet_catalog_delta_report.json", planet_catalog_delta_report)
    write_json(reports_dir / "planet_reclassification_report.json", planet_reclassification_report)
    write_json(reports_dir / "catalog_contribution_report.json", catalog_contribution_report)
    write_json(reports_dir / "athyg_retirement_report.json", athyg_retirement_report)
    write_json(reports_dir / "determinism_report.json", determinism_report)
    pipeline_report_script = root / "scripts" / "update_catalog_pipeline_report.py"
    if pipeline_report_script.exists():
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(pipeline_report_script),
                    "--stage",
                    "ingest",
                    "--build-id",
                    build_id,
                    "--catalog-contribution-report",
                    str(reports_dir / "catalog_contribution_report.json"),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            stage_report_path = (result.stdout or "").strip()
            if stage_report_path:
                log(f"Updated catalog pipeline report: {stage_report_path}")
        except Exception as exc:
            log(f"Warning: failed to update catalog pipeline report at ingest stage ({exc})")
    log_stage_complete(
        "QC stage",
        qc_stage_started,
        stage_totals,
        extra=(
            f"dist_invariant_violations={format_count(dist_violations_stars + dist_violations_systems)}, "
            f"provenance_missing_stars={format_count(provenance_missing)}, "
            f"identifier_quarantine={format_count(identifier_quarantine_count)}, "
            f"duplicate_high_confidence_pairs={format_count(duplicate_high_confidence_pair_count)}"
        ),
    )

    if duplicate_fail_on_high and duplicate_high_confidence_pair_count > duplicate_high_pair_max:
        raise SystemExit(
            "QC failed: high-confidence duplicate pair gate exceeded. "
            f"pairs={duplicate_high_confidence_pair_count} limit={duplicate_high_pair_max}. "
            f"See {reports_dir / 'duplicate_trap_report.json'}"
        )
    if multiplicity_gaia_duplicate_extra_rows > multiplicity_gaia_duplicate_max:
        raise SystemExit(
            "QC failed: multiplicity Gaia exact-duplicate gate exceeded. "
            f"extra_rows={multiplicity_gaia_duplicate_extra_rows} "
            f"limit={multiplicity_gaia_duplicate_max}. "
            f"See {reports_dir / 'duplicate_trap_report.json'}"
        )
    if (
        multiplicity_wds_component_duplicate_extra_rows
        > multiplicity_wds_component_duplicate_max
    ):
        raise SystemExit(
            "QC failed: multiplicity WDS-component exact-duplicate gate exceeded. "
            f"extra_rows={multiplicity_wds_component_duplicate_extra_rows} "
            f"limit={multiplicity_wds_component_duplicate_max}. "
            f"See {reports_dir / 'duplicate_trap_report.json'}"
        )
    if dist_violations_stars + dist_violations_systems > 0:
        raise SystemExit(
            "QC failed: distance invariant violations detected. "
            f"See {reports_dir / 'qc_report.json'}"
        )
    if wd_mismatch_count > 0:
        raise SystemExit(
            "QC failed: white dwarf classification invariant violations detected. "
            f"See {reports_dir / 'classification_safety_report.json'}"
        )
    if identifier_quarantine_count > athyg_merge_ambiguous_limit:
        raise SystemExit(
            "QC failed: identifier ambiguity gate exceeded. "
            f"quarantined={identifier_quarantine_count} limit={athyg_merge_ambiguous_limit}. "
            f"See {reports_dir / 'identifier_report.json'}"
        )
    if identifier_gaia_collision_count > athyg_merge_gaia_collision_max:
        raise SystemExit(
            "QC failed: Gaia identifier collision gate exceeded. "
            f"collisions={identifier_gaia_collision_count} limit={athyg_merge_gaia_collision_max}. "
            f"See {reports_dir / 'identifier_report.json'}"
        )
    if identifier_hip_collision_count > athyg_merge_hip_collision_max:
        raise SystemExit(
            "QC failed: HIP identifier collision gate exceeded. "
            f"collisions={identifier_hip_collision_count} limit={athyg_merge_hip_collision_max}. "
            f"See {reports_dir / 'identifier_report.json'}"
        )
    if identifier_hd_collision_count > athyg_merge_hd_collision_max:
        raise SystemExit(
            "QC failed: HD identifier collision gate exceeded. "
            f"collisions={identifier_hd_collision_count} limit={athyg_merge_hd_collision_max}. "
            f"See {reports_dir / 'identifier_report.json'}"
        )

    # Parquet exports (sorted by spatial_index)
    parquet_stage_started = time.monotonic()
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
    con.execute(
        f"COPY (SELECT * FROM aliases) TO '{parquet_dir / 'aliases.parquet'}' (FORMAT 'parquet')"
    )
    con.execute(
        f"COPY (SELECT * FROM object_identifiers) TO '{parquet_dir / 'object_identifiers.parquet'}' (FORMAT 'parquet')"
    )
    con.execute(
        f"COPY (SELECT * FROM identifier_quarantine) TO '{parquet_dir / 'identifier_quarantine.parquet'}' (FORMAT 'parquet')"
    )
    log_stage_complete("Parquet export stage", parquet_stage_started, stage_totals)

    con.close()

    arm_stage_started = time.monotonic()
    log("Building arm database")
    arm_builder = root / "scripts" / "build_arm.py"
    if not arm_builder.exists():
        raise SystemExit(f"Missing arm builder script: {arm_builder}")
    try:
        arm_proc = subprocess.run(
            [
                sys.executable,
                str(arm_builder),
                "--core-db",
                str(db_path),
                "--arm-db",
                str(arm_db_path),
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
            check=True,
            capture_output=True,
            text=True,
        )
        arm_stdout = (arm_proc.stdout or "").strip()
        if arm_stdout:
            for line in arm_stdout.splitlines():
                log(f"arm: {line}")
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or exc.stdout or "").strip()
        raise SystemExit(f"Arm build failed: {err}") from exc
    log_stage_complete("Arm stage", arm_stage_started, stage_totals)

    if not arm_db_path.exists():
        raise SystemExit(f"Arm build failed: missing output {arm_db_path}")

    tmp_out_dir.rename(final_out_dir)
    log(f"Promoted build output to {final_out_dir}")
    ingest_elapsed_s = time.monotonic() - ingest_started_monotonic
    log(
        "Ingest core complete "
        f"in {ingest_elapsed_s:.1f}s | final totals: {format_stage_totals(stage_totals)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
