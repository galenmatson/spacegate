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
GAIA_CLASSPROB_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
GAIA_CLASSPROB_VERSION = "dr3_astrophysical_parameters_parallax_gte_3.26156"
ATNF_URL = "https://www.atnf.csiro.au/research/pulsar/psrcat/"
ATNF_VERSION = "psrcat_pkg"
MAGNETAR_URL = "https://www.physics.mcgill.ca/~pulsar/magnetar/"
MAGNETAR_VERSION = "TabO1"
CLUSTERS_URL = "https://cdsarc.cds.unistra.fr/ftp/J/A+A/640/A1/"
CLUSTERS_VERSION = "2020A&A...640A...1C"
SNR_URL = "https://www.mrao.cam.ac.uk/surveys/snrs/"
SNR_VERSION = "2024-10"
ATHYG_ALIAS_URL = "https://codeberg.org/astronexus/athyg"
ATHYG_ALIAS_VERSION = "v3.3"
PROX_MAX_DIST_LY = 0.25
PROX_CELL_SIZE_LY = 0.25
PROX_PAIR_ESTIMATE_LIMIT = 50_000_000
WDS_GAIA_MATCH_MAX_ARCSEC_DEFAULT = 2.0
WDS_GAIA_GATE_MAX_DIST_SPREAD_LY_DEFAULT = 10.0
WDS_GAIA_GATE_MAX_PM_DELTA_MASYR_DEFAULT = 25.0
WHITE_DWARF_PROB_THRESHOLD = 0.5
ALIAS_NAME_OVERRIDE_LIMIT = 200000
ALIAS_POS_MAX_DELTA_RA_DEG = 0.12
ALIAS_POS_MAX_DELTA_DEC_DEG = 0.12
ALIAS_POS_MAX_DELTA_DIST_LY = 1.0
ALIAS_POS_MAX_ANG_SEP_ARCSEC = 45.0


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


def format_count(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):,}"


def format_stage_totals(totals: dict[str, int | None]) -> str:
    order = [
        "stars",
        "systems",
        "planets",
        "aliases",
        "compact_objects",
        "superstellar_objects",
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
    enable_gaia_classprob = parse_bool_env("SPACEGATE_ENABLE_GAIA_CLASSPROB", True)
    enable_compact_catalogs = parse_bool_env("SPACEGATE_ENABLE_COMPACT_OBJECT_CATALOGS", True)
    enable_superstellar_catalogs = parse_bool_env("SPACEGATE_ENABLE_SUPERSTELLAR_CATALOGS", True)
    enable_aliases = parse_bool_env("SPACEGATE_ENABLE_ALIASES", True)
    enable_athyg_alias_crosswalk = parse_bool_env("SPACEGATE_ENABLE_ATHYG_ALIAS_CROSSWALK", True)
    open_cluster_member_min_probability = parse_optional_nonnegative_float_env(
        "SPACEGATE_OPEN_CLUSTER_MEMBER_MIN_PROBABILITY"
    )
    if open_cluster_member_min_probability is None:
        open_cluster_member_min_probability = 0.7
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
    cooked_wds_gaia_xmatch = state_dir / "cooked" / "wds_gaia_xmatch" / "wds_gaia_matches.csv"
    cooked_gaia_classprob = (
        state_dir / "cooked" / "gaia_classprob" / "gaia_dr3_astrophysical_classprob.csv"
    )
    cooked_atnf = state_dir / "cooked" / "atnf" / "pulsars.csv"
    cooked_magnetar = state_dir / "cooked" / "magnetar" / "magnetars.csv"
    cooked_open_clusters = state_dir / "cooked" / "clusters" / "open_clusters.csv"
    cooked_open_cluster_members = state_dir / "cooked" / "clusters" / "open_cluster_members.csv"
    cooked_snr = state_dir / "cooked" / "snr" / "green_snr.csv"
    manifest_dir = state_dir / "reports" / "manifests"
    manifest_path = manifest_dir / "core_manifest.json"
    wds_manifest_path = manifest_dir / "wds_manifest.json"
    msc_manifest_path = manifest_dir / "msc_manifest.json"
    orb6_manifest_path = manifest_dir / "orb6_manifest.json"
    gaia_backbone_manifest_path = manifest_dir / "gaia_backbone_manifest.json"
    gaia_nss_manifest_path = manifest_dir / "gaia_nss_manifest.json"
    wds_gaia_xmatch_manifest_path = manifest_dir / "wds_gaia_xmatch_manifest.json"
    gaia_classprob_manifest_path = manifest_dir / "gaia_classprob_manifest.json"
    atnf_manifest_path = manifest_dir / "atnf_manifest.json"
    magnetar_manifest_path = manifest_dir / "magnetar_manifest.json"
    clusters_manifest_path = manifest_dir / "clusters_manifest.json"
    snr_manifest_path = manifest_dir / "snr_manifest.json"

    if not enable_gaia_backbone and not cooked_athyg.exists():
        raise SystemExit(f"Missing cooked AT-HYG: {cooked_athyg}")
    if enable_gaia_backbone and not cooked_gaia_backbone.exists():
        raise SystemExit(f"Missing cooked Gaia backbone: {cooked_gaia_backbone}")
    if enable_aliases and enable_athyg_alias_crosswalk and not cooked_athyg.exists():
        raise SystemExit(
            f"Missing cooked AT-HYG alias crosswalk source: {cooked_athyg}"
        )
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
    if enable_gaia_backbone and enable_gaia_classprob and not cooked_gaia_classprob.exists():
        raise SystemExit(f"Missing cooked Gaia classifier probabilities: {cooked_gaia_classprob}")
    if enable_compact_catalogs and not cooked_atnf.exists():
        raise SystemExit(f"Missing cooked ATNF pulsars: {cooked_atnf}")
    if enable_compact_catalogs and not cooked_magnetar.exists():
        raise SystemExit(f"Missing cooked magnetars: {cooked_magnetar}")
    if enable_superstellar_catalogs and not cooked_open_clusters.exists():
        raise SystemExit(f"Missing cooked open clusters: {cooked_open_clusters}")
    if enable_superstellar_catalogs and not cooked_open_cluster_members.exists():
        raise SystemExit(f"Missing cooked open-cluster members: {cooked_open_cluster_members}")
    if enable_superstellar_catalogs and not cooked_snr.exists():
        raise SystemExit(f"Missing cooked SNR catalog: {cooked_snr}")

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
        f"compact_catalogs={'1' if enable_compact_catalogs else '0'} "
        f"superstellar_catalogs={'1' if enable_superstellar_catalogs else '0'} "
        f"aliases={'1' if enable_aliases else '0'} "
        f"athyg_alias_crosswalk={'1' if (enable_aliases and enable_athyg_alias_crosswalk) else '0'} "
        f"open_cluster_member_min_probability={open_cluster_member_min_probability}"
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
    if enable_gaia_backbone and enable_gaia_classprob:
        manifest_paths.append(gaia_classprob_manifest_path)
    if enable_compact_catalogs:
        manifest_paths.extend([atnf_manifest_path, magnetar_manifest_path])
    if enable_superstellar_catalogs:
        manifest_paths.extend([clusters_manifest_path, snr_manifest_path])
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
    con.execute("SET preserve_insertion_order=false")
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
          ('slice_profile_id', {sql_literal(slice_profile_id)}),
          ('slice_profile_version', {sql_literal(slice_profile_version)}),
          ('build_layer', {sql_literal(build_layer)}),
          ('source_galaxy_build_id', {sql_literal(source_galaxy_build_id)}),
          ('wds_gaia_match_max_arcsec', {sql_literal(str(wds_gaia_match_max_arcsec))}),
          ('wds_gaia_gate_max_dist_spread_ly', {sql_literal(str(wds_gaia_gate_max_dist_spread_ly))}),
          ('wds_gaia_gate_max_pm_delta_mas_yr', {sql_literal(str(wds_gaia_gate_max_pm_delta_mas_yr))}),
          ('gaia_classprob_enabled', {sql_literal("1" if (enable_gaia_backbone and enable_gaia_classprob) else "0")}),
          ('compact_catalogs_enabled', {sql_literal("1" if enable_compact_catalogs else "0")}),
          ('superstellar_catalogs_enabled', {sql_literal("1" if enable_superstellar_catalogs else "0")}),
          ('aliases_enabled', {sql_literal("1" if enable_aliases else "0")}),
          ('athyg_alias_crosswalk_enabled', {sql_literal("1" if (enable_aliases and enable_athyg_alias_crosswalk) else "0")}),
          ('open_cluster_member_min_probability', {sql_literal(str(open_cluster_member_min_probability))})
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
    gaia_classprob_manifest = (
        require_manifest_entry(
            manifest,
            "gaia_dr3_astrophysical_classprob",
            "Gaia DR3 astrophysical classifier probabilities",
        )
        if enable_gaia_backbone and enable_gaia_classprob
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
              null::varchar as ruwe
            from athyg_raw_source
            """
        )

    if enable_aliases and enable_athyg_alias_crosswalk:
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
    wds_gaia_xmatch_path = str(cooked_wds_gaia_xmatch).replace("'", "''")
    gaia_classprob_path = str(cooked_gaia_classprob).replace("'", "''")
    atnf_path = str(cooked_atnf).replace("'", "''")
    magnetar_path = str(cooked_magnetar).replace("'", "''")
    open_clusters_path = str(cooked_open_clusters).replace("'", "''")
    open_cluster_members_path = str(cooked_open_cluster_members).replace("'", "''")
    snr_path = str(cooked_snr).replace("'", "''")

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

    stage_totals: dict[str, int | None] = {
        "stars": None,
        "systems": None,
        "planets": None,
        "aliases": None,
        "compact_objects": None,
        "superstellar_objects": None,
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
            f"""
            update stars
            set
              object_family = 'white_dwarf',
              object_type = 'white_dwarf',
              classification_evidence_json = json_object(
                'method', 'gaia_classprob',
                'classprob_dsc_combmod_whitedwarf', classprob_dsc_combmod_whitedwarf,
                'classprob_dsc_specmod_whitedwarf', classprob_dsc_specmod_whitedwarf,
                'threshold', {WHITE_DWARF_PROB_THRESHOLD}
              )
            where greatest(
              coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
              coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
            ) >= {WHITE_DWARF_PROB_THRESHOLD}
            """
        )

    alias_crosswalk_candidate_count = 0
    alias_crosswalk_matched_star_count = 0
    alias_name_override_count = 0
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
                1 as source_priority,
                row_number() over (
                  partition by source_pk
                  order by dist_delta_ly asc, ang_sep_arcsec asc, gaia_id asc
                ) as rn
              from positional_candidates
              where ang_sep_arcsec <= {ALIAS_POS_MAX_ANG_SEP_ARCSEC}
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
                'wds_component', coalesce(stars.component, json_extract_string(stars.catalog_ids_json, '$.wds_component'))
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
            f"alias_name_overrides={format_count(alias_name_override_count)}"
        ),
    )

    # System grouping: WDS first, then name-root, then optional proximity for remaining stars.
    system_grouping_stage_started = time.monotonic()
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
    stage_totals["planets"] = con.execute("select count(*) from planets").fetchone()[0]
    matched_planet_count = con.execute(
        "select count(*) from planets where match_method <> 'unmatched'"
    ).fetchone()[0]
    log_stage_complete(
        "Planets stage",
        planets_stage_started,
        stage_totals,
        extra=f"matched_planets={format_count(matched_planet_count)}",
    )

    aliases_stage_started = time.monotonic()
    log("Building alias tables")
    if enable_aliases:
        con.execute(
            """
            create or replace temp view star_alias_seed as
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
              s.star_id as target_id,
              s.system_id,
              s.star_id,
              a.proper_name as alias_raw,
              'proper_name' as alias_kind,
              1 as alias_priority,
              'athyg_crosswalk' as source_catalog,
              'v3.3' as source_version,
              a.source_pk as source_pk
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
            where a.proper_name is not null

            union all

            select
              'star',
              s.star_id,
              s.system_id,
              s.star_id,
              a.bayer_name as alias_raw,
              'bayer_name',
              2,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
            where a.bayer_name is not null

            union all

            select
              'star',
              s.star_id,
              s.system_id,
              s.star_id,
              a.flam_name as alias_raw,
              'flamsteed_name',
              3,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
            where a.flam_name is not null

            union all

            select
              'star',
              s.star_id,
              s.system_id,
              s.star_id,
              'HR ' || a.hr_id::varchar as alias_raw,
              'hr_id',
              10,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
            where a.hr_id is not null

            union all

            select
              'star',
              s.star_id,
              s.system_id,
              s.star_id,
              a.gl_id as alias_raw,
              'gl_id',
              11,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
            where a.gl_id is not null

            union all

            select
              'star',
              s.star_id,
              s.system_id,
              s.star_id,
              'TYC ' || a.tyc_id as alias_raw,
              'tyc_id',
              12,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
            where a.tyc_id is not null

            union all

            select
              'star',
              s.star_id,
              s.system_id,
              s.star_id,
              'HYG ' || a.hyg_id::varchar as alias_raw,
              'hyg_id',
              13,
              'athyg_crosswalk',
              'v3.3',
              a.source_pk
            from stars s
            join athyg_alias_candidates a on a.gaia_id = s.gaia_id
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
            'magnetar' as object_type,
            'neutron_star' as object_family,
            json_object(
              'name', nullif(name, ''),
              'assoc_raw', nullif(assoc_raw, ''),
              'activity_raw', nullif(activity_raw, '')
            ) as catalog_ids_json
          from magnetar_raw
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
          object_type,
          object_family,
          catalog_ids_json
        from magnetar_norm
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
        with candidates as (
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
            end as dist_delta_ly
          from compact_catalog_bins c
          join star_sky_bins s
            on s.ra_bin between c.ra_bin - 1 and c.ra_bin + 1
           and s.dec_bin between c.dec_bin - 1 and c.dec_bin + 1
           and s.dec_deg between c.dec_deg - 1.0 and c.dec_deg + 1.0
           and (
             abs(s.ra_deg - c.ra_deg) <= 1.0
             or abs(abs(s.ra_deg - c.ra_deg) - 360.0) <= 1.0
           )
        ), ranked as (
          select
            *,
            row_number() over (
              partition by source_catalog, source_key
              order by ang_dist_arcsec asc, dist_delta_ly asc nulls last, star_id asc
            ) as rn
          from candidates
          where ang_dist_arcsec <= 5.0
            and (dist_delta_ly is null or dist_delta_ly <= 200.0)
        )
        select
          source_catalog,
          source_key,
          star_id,
          system_id,
          ang_dist_arcsec,
          dist_delta_ly
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
          case when m.star_id is not null then 'sky_position' else 'unmatched' end as match_method,
          case
            when m.star_id is null then 0.0
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
            else {sql_literal(MAGNETAR_VERSION)}
          end as source_version,
          case
            when c.source_catalog = 'atnf' then {sql_literal(ATNF_URL)}
            else {sql_literal(MAGNETAR_URL)}
          end as source_url,
          case
            when c.source_catalog = 'atnf' then {sql_literal(ATNF_URL)}
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
            else {sql_literal(magnetar_sha)}
          end as retrieval_checksum,
          case
            when c.source_catalog = 'atnf' then {sql_literal(atnf_retrieved)}
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
          and c.match_method = 'sky_position'
          and c.match_angular_distance_arcsec is not null
          and c.match_angular_distance_arcsec <= 1.0
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

    compact_count = con.execute("select count(*) from compact_objects").fetchone()[0]
    open_clusters_count = con.execute("select count(*) from open_clusters").fetchone()[0]
    open_cluster_memberships_count = con.execute(
        "select count(*) from open_cluster_memberships"
    ).fetchone()[0]
    superstellar_count = con.execute(
        "select count(*) from superstellar_objects"
    ).fetchone()[0]
    stage_totals["compact_objects"] = compact_count
    stage_totals["superstellar_objects"] = superstellar_count
    log_stage_complete(
        "Science side tables stage",
        science_side_stage_started,
        stage_totals,
        extra=(
            f"open_clusters={format_count(open_clusters_count)}, "
            f"open_cluster_memberships={format_count(open_cluster_memberships_count)}"
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
            "compact_objects": table_provenance_report("compact_objects", True),
            "superstellar_objects": table_provenance_report("superstellar_objects", True),
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
    if gaia_classprob_manifest:
        provenance_report["gaia_dr3_astrophysical_classprob"] = gaia_classprob_manifest
    if atnf_manifest:
        provenance_report["atnf"] = atnf_manifest
    if magnetar_manifest:
        provenance_report["magnetar"] = magnetar_manifest
    if clusters_table1_manifest:
        provenance_report["open_clusters_table1"] = clusters_table1_manifest
    if clusters_members_manifest:
        provenance_report["open_clusters_members"] = clusters_members_manifest
    if snr_manifest:
        provenance_report["snr"] = snr_manifest

    write_json(reports_dir / "provenance_report.json", provenance_report)

    total_failures = sum(
        provenance_report["tables"][name]["failures"]
        for name in ("stars", "systems", "planets", "compact_objects", "superstellar_objects")
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
          (select count(*) from superstellar_objects) as superstellar_objects
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

    wd_evidence_count = con.execute(
        f"""
        select count(*) from stars
        where greatest(
          coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
          coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
        ) >= {WHITE_DWARF_PROB_THRESHOLD}
        """
    ).fetchone()[0]
    wd_emitted_count = con.execute(
        "select count(*) from stars where object_family = 'white_dwarf'"
    ).fetchone()[0]
    wd_mismatch_count = con.execute(
        f"""
        select count(*) from stars
        where greatest(
          coalesce(classprob_dsc_combmod_whitedwarf, 0.0),
          coalesce(classprob_dsc_specmod_whitedwarf, 0.0)
        ) >= {WHITE_DWARF_PROB_THRESHOLD}
          and coalesce(object_family, '') <> 'white_dwarf'
        """
    ).fetchone()[0]
    classification_safety_report = {
        "build_id": build_id,
        "white_dwarf_probability_threshold": WHITE_DWARF_PROB_THRESHOLD,
        "white_dwarf_evidence_count": wd_evidence_count,
        "white_dwarf_emitted_count": wd_emitted_count,
        "white_dwarf_mismatch_count": wd_mismatch_count,
        "notes": [
            "Invariant: rows with strong Gaia white-dwarf class probability must emit object_family=white_dwarf.",
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
            "aliases": alias_total_count,
            "system_aliases": alias_system_count,
            "star_aliases": alias_star_count,
        },
        "object_family_counts": [
            {"object_family": row[0], "count": row[1]} for row in object_family_counts
        ],
        "gaia_backbone_enabled": enable_gaia_backbone,
        "base_source_catalog": base_source_catalog,
        "gaia_classprob_enabled": enable_gaia_backbone and enable_gaia_classprob,
        "compact_catalogs_enabled": enable_compact_catalogs,
        "superstellar_catalogs_enabled": enable_superstellar_catalogs,
        "open_cluster_member_min_probability": open_cluster_member_min_probability,
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
        "white_dwarf_probability_threshold": WHITE_DWARF_PROB_THRESHOLD,
        "white_dwarf_evidence_count": wd_evidence_count,
        "white_dwarf_emitted_count": wd_emitted_count,
        "white_dwarf_mismatch_count": wd_mismatch_count,
        "aliases_enabled": enable_aliases,
        "athyg_alias_crosswalk_enabled": enable_aliases and enable_athyg_alias_crosswalk,
        "athyg_alias_candidate_count": alias_crosswalk_candidate_count,
        "athyg_alias_matched_star_count": alias_crosswalk_matched_star_count,
        "alias_name_override_count": alias_name_override_count,
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
    }

    write_json(reports_dir / "qc_report.json", qc_report)
    write_json(reports_dir / "match_report.json", match_report)
    log_stage_complete(
        "QC stage",
        qc_stage_started,
        stage_totals,
        extra=(
            f"dist_invariant_violations={format_count(dist_violations_stars + dist_violations_systems)}, "
            f"provenance_missing_stars={format_count(provenance_missing)}"
        ),
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
    log_stage_complete("Parquet export stage", parquet_stage_started, stage_totals)

    con.close()
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
