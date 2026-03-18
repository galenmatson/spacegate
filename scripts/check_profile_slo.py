#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_STATE_DIR = Path(
    os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or ROOT_DIR / "data"
)

PROFILE_SLO: Dict[str, Dict[str, float]] = {
    "core.default": {
        "search_p95_ms": 1200.0,
        "search_p99_ms": 2500.0,
        "detail_p95_ms": 900.0,
        "error_rate_pct": 1.0,
        "api_steady_mem_mib": 3584.0,
        "api_peak_mem_mib": 8192.0,
    },
    "core.public": {
        "search_p95_ms": 1200.0,
        "search_p99_ms": 2500.0,
        "detail_p95_ms": 900.0,
        "error_rate_pct": 1.0,
        "api_steady_mem_mib": 3584.0,
        "api_peak_mem_mib": 8192.0,
    },
    "core.performance": {
        "search_p95_ms": 800.0,
        "search_p99_ms": 1600.0,
        "detail_p95_ms": 700.0,
        "error_rate_pct": 0.8,
        "api_steady_mem_mib": 2560.0,
        "api_peak_mem_mib": 6144.0,
    },
    "core.precision": {
        "search_p95_ms": 1000.0,
        "search_p99_ms": 2000.0,
        "detail_p95_ms": 800.0,
        "error_rate_pct": 1.0,
        "api_steady_mem_mib": 3072.0,
        "api_peak_mem_mib": 7168.0,
    },
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Validate profile-specific API SLOs against a promoted Spacegate build."
    )
    p.add_argument("--build-id", default="", help="Target build id. Defaults to served/current.")
    p.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR), help="Spacegate state directory.")
    p.add_argument(
        "--base-url",
        default=os.getenv("SPACEGATE_SLO_BASE_URL", "http://127.0.0.1:8000"),
        help="API base URL.",
    )
    p.add_argument(
        "--profile-id",
        default="",
        help="Override profile id (for unsliced/manual validation).",
    )
    p.add_argument("--workers", type=int, default=16, help="Concurrent workers per benchmark phase.")
    p.add_argument("--search-requests", type=int, default=160, help="Search request sample size.")
    p.add_argument("--detail-requests", type=int, default=120, help="Detail request sample size.")
    p.add_argument("--timeout-s", type=float, default=6.0, help="Per-request timeout in seconds.")
    p.add_argument(
        "--report-path",
        default="",
        help="Output report path. Defaults to reports/<build_id>/slo_profile_report.json.",
    )
    p.add_argument(
        "--require-profile",
        action="store_true",
        help="Fail if no profile id can be resolved.",
    )
    return p.parse_args()


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    data = sorted(values)
    if len(data) == 1:
        return float(data[0])
    rank = (p / 100.0) * (len(data) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return float(data[lo])
    return float(data[lo] + (data[hi] - data[lo]) * (rank - lo))


def normalize_profile_class(profile_id: str) -> str:
    normalized = (profile_id or "").strip().lower()
    if normalized.startswith("core.default"):
        return "core.default"
    if normalized.startswith("core.public"):
        return "core.public"
    if normalized.startswith("core.performance"):
        return "core.performance"
    if normalized.startswith("core.precision"):
        return "core.precision"
    if normalized.startswith("core.visual"):
        return "core.default"
    return "core.default"


def resolve_build_id_and_dir(state_dir: Path, build_id: str) -> Tuple[str, Path]:
    out_dir = state_dir / "out"
    served_current = state_dir / "served" / "current"
    if build_id:
        candidate = out_dir / build_id
        if not candidate.is_dir():
            raise SystemExit(f"build directory not found: {candidate}")
        return build_id, candidate

    try:
        resolved = served_current.resolve(strict=True)
    except FileNotFoundError as exc:
        raise SystemExit(f"served/current not found under {state_dir}") from exc
    return resolved.name, resolved


def load_build_metadata(core_db: Path) -> Dict[str, str]:
    con = duckdb.connect(str(core_db), read_only=True)
    try:
        rows = con.execute("select key, value from build_metadata").fetchall()
    finally:
        con.close()
    return {str(k): str(v or "") for k, v in rows}


def http_json(url: str, timeout_s: float) -> Dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        payload = resp.read()
    return json.loads(payload.decode("utf-8"))


def parse_mem_mib(mem_usage: str) -> float:
    left = (mem_usage or "").split("/", 1)[0].strip()
    if not left:
        return 0.0
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMG]iB|B)\s*$", left)
    if not m:
        return 0.0
    value_txt = m.group(1)
    unit = m.group(2)
    try:
        value = float(value_txt)
    except ValueError:
        return 0.0
    unit = unit.strip()
    if unit == "B":
        return value / (1024.0 * 1024.0)
    if unit == "KiB":
        return value / 1024.0
    if unit == "MiB":
        return value
    if unit == "GiB":
        return value * 1024.0
    return 0.0


class DockerSampler:
    def __init__(self, interval_s: float = 1.0) -> None:
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.mem_samples_mib: List[float] = []
        self.container_name: Optional[str] = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                proc = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            except Exception:
                return
            if proc.returncode == 0 and proc.stdout:
                for raw in proc.stdout.splitlines():
                    if "|" not in raw:
                        continue
                    name, mem_usage = raw.split("|", 1)
                    if "api" not in name:
                        continue
                    mem_mib = parse_mem_mib(mem_usage)
                    if mem_mib > 0:
                        self.container_name = name.strip()
                        self.mem_samples_mib.append(mem_mib)
            self._stop.wait(self.interval_s)

    def start(self) -> None:
        if not shutil_which("docker"):
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def metrics(self) -> Dict[str, Any]:
        if not self.mem_samples_mib:
            return {
                "container_name": self.container_name,
                "sample_count": 0,
                "steady_mem_mib": None,
                "peak_mem_mib": None,
            }
        return {
            "container_name": self.container_name,
            "sample_count": len(self.mem_samples_mib),
            "steady_mem_mib": float(self.mem_samples_mib[-1]),
            "peak_mem_mib": float(max(self.mem_samples_mib)),
        }


def shutil_which(name: str) -> Optional[str]:
    paths = os.getenv("PATH", "").split(os.pathsep)
    for base in paths:
        if not base:
            continue
        candidate = Path(base) / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def run_load(
    urls: Iterable[str], timeout_s: float, workers: int
) -> Tuple[List[float], Dict[int, int], int]:
    latencies: List[float] = []
    status_counts: Dict[int, int] = {}
    failures = 0

    def one(url: str) -> Tuple[float, int]:
        started = time.perf_counter()
        status = 0
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                status = int(resp.status)
                _ = resp.read()
        except urllib.error.HTTPError as exc:
            status = int(exc.code or 0)
            failures_local = 1
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return elapsed_ms, -status if failures_local else status
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return elapsed_ms, -1
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return elapsed_ms, status

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = [ex.submit(one, url) for url in urls]
        for fut in as_completed(futures):
            elapsed_ms, status = fut.result()
            latencies.append(elapsed_ms)
            if status > 0:
                status_counts[status] = status_counts.get(status, 0) + 1
                if status < 200 or status >= 300:
                    failures += 1
            else:
                failures += 1
                status_counts[0] = status_counts.get(0, 0) + 1

    return latencies, status_counts, failures


def evaluate(args: argparse.Namespace) -> Dict[str, Any]:
    state_dir = Path(args.state_dir).resolve()
    build_id, build_dir = resolve_build_id_and_dir(state_dir, args.build_id)
    core_db = build_dir / "core.duckdb"
    if not core_db.is_file():
        raise SystemExit(f"missing core.duckdb in {build_dir}")

    metadata = load_build_metadata(core_db)
    profile_id = (args.profile_id or metadata.get("slice_profile_id") or "").strip()
    if not profile_id and args.require_profile:
        raise SystemExit("slice profile id is missing for this build and --require-profile is set")

    if not profile_id:
        return {
            "build_id": build_id,
            "generated_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "base_url": args.base_url,
            "profile_id": "",
            "profile_class": None,
            "status": "skipped",
            "reason": "no_slice_profile_id",
            "gates": {"passed": True},
            "measurements": {},
            "thresholds": {},
        }

    profile_class = normalize_profile_class(profile_id)
    thresholds = PROFILE_SLO[profile_class]

    base_url = args.base_url.rstrip("/")
    health = http_json(f"{base_url}/api/v1/health", timeout_s=args.timeout_s)
    served_build = str(health.get("build_id") or "")
    if served_build and served_build != build_id:
        raise SystemExit(
            f"health build mismatch: API serves {served_build}, expected {build_id}. "
            "Promote this build before running SLO check."
        )

    seed = http_json(
        f"{base_url}/api/v1/systems/search?q=a&limit=25&include_total=false",
        timeout_s=args.timeout_s,
    )
    seed_items = seed.get("items") or []
    seed_ids = [int(row["system_id"]) for row in seed_items if row.get("system_id") is not None]
    if not seed_ids:
        seed_alt = http_json(
            f"{base_url}/api/v1/systems/search?limit=25&include_total=false",
            timeout_s=args.timeout_s,
        )
        seed_ids = [int(row["system_id"]) for row in (seed_alt.get("items") or []) if row.get("system_id") is not None]
    if not seed_ids:
        raise SystemExit("unable to sample system ids for detail benchmark")

    search_url = f"{base_url}/api/v1/systems/search?q=a&limit=50&include_total=false"
    search_urls = [search_url for _ in range(max(1, args.search_requests))]
    detail_urls = [
        f"{base_url}/api/v1/systems/{seed_ids[i % len(seed_ids)]}"
        for i in range(max(1, args.detail_requests))
    ]

    sampler = DockerSampler(interval_s=1.0)
    sampler.start()
    try:
        search_lat_ms, search_status, search_fail = run_load(
            search_urls, timeout_s=args.timeout_s, workers=args.workers
        )
        detail_lat_ms, detail_status, detail_fail = run_load(
            detail_urls, timeout_s=args.timeout_s, workers=args.workers
        )
    finally:
        sampler.stop()

    total_requests = len(search_lat_ms) + len(detail_lat_ms)
    total_failures = search_fail + detail_fail
    error_rate_pct = (float(total_failures) / float(total_requests) * 100.0) if total_requests else 0.0

    search_metrics = {
        "requests": len(search_lat_ms),
        "status_counts": {str(k): v for k, v in sorted(search_status.items())},
        "failures": search_fail,
        "p50_ms": percentile(search_lat_ms, 50),
        "p95_ms": percentile(search_lat_ms, 95),
        "p99_ms": percentile(search_lat_ms, 99),
    }
    detail_metrics = {
        "requests": len(detail_lat_ms),
        "status_counts": {str(k): v for k, v in sorted(detail_status.items())},
        "failures": detail_fail,
        "p50_ms": percentile(detail_lat_ms, 50),
        "p95_ms": percentile(detail_lat_ms, 95),
        "p99_ms": percentile(detail_lat_ms, 99),
    }
    mem = sampler.metrics()

    gate_search_p95 = search_metrics["p95_ms"] <= thresholds["search_p95_ms"]
    gate_search_p99 = search_metrics["p99_ms"] <= thresholds["search_p99_ms"]
    gate_detail_p95 = detail_metrics["p95_ms"] <= thresholds["detail_p95_ms"]
    gate_error = error_rate_pct <= thresholds["error_rate_pct"]

    steady_mem_mib = mem.get("steady_mem_mib")
    peak_mem_mib = mem.get("peak_mem_mib")
    gate_mem_steady = (
        True if steady_mem_mib is None else float(steady_mem_mib) <= thresholds["api_steady_mem_mib"]
    )
    gate_mem_peak = (
        True if peak_mem_mib is None else float(peak_mem_mib) <= thresholds["api_peak_mem_mib"]
    )

    passed = all(
        [
            gate_search_p95,
            gate_search_p99,
            gate_detail_p95,
            gate_error,
            gate_mem_steady,
            gate_mem_peak,
        ]
    )

    return {
        "build_id": build_id,
        "generated_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "base_url": base_url,
        "profile_id": profile_id,
        "profile_class": profile_class,
        "status": "evaluated",
        "thresholds": thresholds,
        "measurements": {
            "search": search_metrics,
            "detail": detail_metrics,
            "total_requests": total_requests,
            "total_failures": total_failures,
            "error_rate_pct": error_rate_pct,
            "api_memory": mem,
        },
        "gates": {
            "search_p95_ok": gate_search_p95,
            "search_p99_ok": gate_search_p99,
            "detail_p95_ok": gate_detail_p95,
            "error_rate_ok": gate_error,
            "api_steady_mem_ok": gate_mem_steady,
            "api_peak_mem_ok": gate_mem_peak,
            "passed": passed,
        },
    }


def main() -> int:
    args = parse_args()
    report = evaluate(args)

    build_id = str(report.get("build_id") or args.build_id or "unknown")
    state_dir = Path(args.state_dir).resolve()
    report_path = (
        Path(args.report_path).resolve()
        if args.report_path
        else (state_dir / "reports" / build_id / "slo_profile_report.json")
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    status = report.get("status")
    if status == "skipped":
        print(f"SLO check skipped (no profile id): {report_path}")
        return 0

    gates = report.get("gates") or {}
    passed = bool(gates.get("passed"))
    search = ((report.get("measurements") or {}).get("search") or {})
    detail = ((report.get("measurements") or {}).get("detail") or {})
    error_rate = float(((report.get("measurements") or {}).get("error_rate_pct") or 0.0))

    print(f"SLO check report: {report_path}")
    print(
        "search p95={:.1f} p99={:.1f} | detail p95={:.1f} | error_rate={:.3f}% | pass={}".format(
            float(search.get("p95_ms") or 0.0),
            float(search.get("p99_ms") or 0.0),
            float(detail.get("p95_ms") or 0.0),
            error_rate,
            "yes" if passed else "no",
        )
    )
    return 0 if passed else 2


if __name__ == "__main__":
    sys.exit(main())
