#!/usr/bin/env python3
"""Reproducible HTTP and cgroup capacity measurements for Spacegate."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import random
import shutil
import statistics
import subprocess
import threading
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = (
    ROOT / "config/runtime_capacity/workload_e7_24cb15211f430a37.json"
)
DEFAULT_STATE = Path(
    os.getenv("SPACEGATE_STATE_DIR")
    or os.getenv("SPACEGATE_DATA_DIR")
    or "/data/spacegate/state"
)
SCHEMA_VERSION = "spacegate.runtime_capacity_report.v1"
SAMPLE_SCHEMA_VERSION = "spacegate.runtime_capacity_samples.v1"
REQUEST_SCHEMA_VERSION = "spacegate.runtime_capacity_requests.v1"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def percentile(values: Iterable[float], p: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    rank = (p / 100.0) * (len(ordered) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def parse_key_values(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError):
        return values
    for line in lines:
        parts = line.split()
        if len(parts) == 2:
            try:
                values[parts[0]] = int(parts[1])
            except ValueError:
                continue
    return values


def read_int(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except (FileNotFoundError, PermissionError):
        return None
    if raw == "max":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except (FileNotFoundError, PermissionError):
        return None


def cpu_quota_cores(cpu_max: str | None) -> float | None:
    parts = (cpu_max or "").split()
    if len(parts) != 2 or parts[0] == "max":
        return None
    try:
        quota = int(parts[0])
        period = int(parts[1])
    except ValueError:
        return None
    if quota <= 0 or period <= 0:
        return None
    return quota / period


def read_meminfo() -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        lines = Path("/proc/meminfo").read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return result
    for line in lines:
        key, _, raw = line.partition(":")
        try:
            result[key] = int(raw.strip().split()[0]) * 1024
        except (ValueError, IndexError):
            continue
    return result


def read_host_cpu() -> dict[str, int]:
    try:
        line = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0]
    except (FileNotFoundError, IndexError):
        return {}
    parts = line.split()
    if not parts or parts[0] != "cpu":
        return {}
    labels = ["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"]
    values = [int(value) for value in parts[1 : 1 + len(labels)]]
    result = dict(zip(labels, values))
    result["total"] = sum(values)
    result["idle_total"] = result.get("idle", 0) + result.get("iowait", 0)
    return result


def read_psi(path: Path) -> dict[str, dict[str, float | int]]:
    result: dict[str, dict[str, float | int]] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError):
        return result
    for line in lines:
        parts = line.split()
        if not parts:
            continue
        values: dict[str, float | int] = {}
        for part in parts[1:]:
            key, _, raw = part.partition("=")
            try:
                values[key] = int(raw) if key == "total" else float(raw)
            except ValueError:
                continue
        result[parts[0]] = values
    return result


def read_netdev() -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    try:
        lines = Path("/proc/net/dev").read_text(encoding="utf-8").splitlines()[2:]
    except FileNotFoundError:
        return result
    for line in lines:
        interface, _, payload = line.partition(":")
        values = payload.split()
        if len(values) < 16:
            continue
        result[interface.strip()] = {
            "rx_bytes": int(values[0]),
            "rx_packets": int(values[1]),
            "rx_errors": int(values[2]),
            "rx_drops": int(values[3]),
            "tx_bytes": int(values[8]),
            "tx_packets": int(values[9]),
            "tx_errors": int(values[10]),
            "tx_drops": int(values[11]),
        }
    return result


def read_process_status(pid: int) -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        lines = Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError):
        return result
    for line in lines:
        key, _, raw = line.partition(":")
        if key in {"VmRSS", "VmHWM", "VmSize", "RssAnon", "RssFile", "RssShmem"}:
            try:
                result[f"{key}_bytes"] = int(raw.strip().split()[0]) * 1024
            except (ValueError, IndexError):
                pass
        elif key == "Threads":
            try:
                result["threads"] = int(raw.strip())
            except ValueError:
                pass
    return result


def read_process_group_status(cgroup_path: Path) -> dict[str, int]:
    try:
        pids = {
            int(raw)
            for raw in (cgroup_path / "cgroup.procs")
            .read_text(encoding="utf-8")
            .splitlines()
            if raw.strip()
        }
    except (FileNotFoundError, PermissionError, ValueError):
        return {}
    statuses = [read_process_status(pid) for pid in sorted(pids)]
    return {
        "process_count": len(statuses),
        "rss_bytes_sum": sum(row.get("VmRSS_bytes", 0) for row in statuses),
        "hwm_bytes_sum": sum(row.get("VmHWM_bytes", 0) for row in statuses),
        "rss_anon_bytes_sum": sum(row.get("RssAnon_bytes", 0) for row in statuses),
        "rss_file_bytes_sum": sum(row.get("RssFile_bytes", 0) for row in statuses),
        "rss_shmem_bytes_sum": sum(row.get("RssShmem_bytes", 0) for row in statuses),
        "threads_sum": sum(row.get("threads", 0) for row in statuses),
        "largest_process_rss_bytes": max(
            (row.get("VmRSS_bytes", 0) for row in statuses),
            default=0,
        ),
    }


def read_cgroup_io(path: Path) -> dict[str, int]:
    totals: Counter[str] = Counter()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError):
        return {}
    for line in lines:
        for part in line.split()[1:]:
            key, _, raw = part.partition("=")
            try:
                totals[key] += int(raw)
            except ValueError:
                continue
    return dict(totals)


def docker_inspect(container: str) -> dict[str, Any] | None:
    try:
        completed = subprocess.run(
            ["docker", "inspect", container],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    payload = json.loads(completed.stdout)
    return payload[0] if payload else None


@dataclass(frozen=True)
class ContainerProbe:
    name: str
    container_id: str
    pid: int
    cgroup_path: Path
    cpu_quota: str | None
    memory_limit: int | None

    @classmethod
    def create(cls, name: str) -> "ContainerProbe | None":
        info = docker_inspect(name)
        if not info:
            return None
        pid = int((info.get("State") or {}).get("Pid") or 0)
        if pid <= 0:
            return None
        cgroup_line = Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8").strip()
        relative = cgroup_line.split("::", 1)[-1].lstrip("/")
        cgroup_path = Path("/sys/fs/cgroup") / relative
        return cls(
            name=name,
            container_id=str(info.get("Id") or ""),
            pid=pid,
            cgroup_path=cgroup_path,
            cpu_quota=read_text(cgroup_path / "cpu.max"),
            memory_limit=read_int(cgroup_path / "memory.max"),
        )

    def sample(self) -> dict[str, Any]:
        memory_stat = parse_key_values(self.cgroup_path / "memory.stat")
        return {
            "name": self.name,
            "container_id": self.container_id,
            "pid": self.pid,
            "process": read_process_status(self.pid),
            "process_group": read_process_group_status(self.cgroup_path),
            "cgroup": {
                "cpu": parse_key_values(self.cgroup_path / "cpu.stat"),
                "memory_current_bytes": read_int(self.cgroup_path / "memory.current"),
                "memory_peak_bytes": read_int(self.cgroup_path / "memory.peak"),
                "memory_events": parse_key_values(self.cgroup_path / "memory.events"),
                "memory_stat": memory_stat,
                "page_cache_bytes": memory_stat.get("file"),
                "io": read_cgroup_io(self.cgroup_path / "io.stat"),
                "pids_current": read_int(self.cgroup_path / "pids.current"),
                "cpu_pressure": read_psi(self.cgroup_path / "cpu.pressure"),
                "memory_pressure": read_psi(self.cgroup_path / "memory.pressure"),
                "io_pressure": read_psi(self.cgroup_path / "io.pressure"),
            },
        }


class ResourceMonitor:
    def __init__(
        self,
        probes: list[ContainerProbe],
        interval_seconds: float,
        stop_conditions: dict[str, Any],
        stop_event: threading.Event,
        stop_reason: list[str],
    ) -> None:
        self.probes = probes
        self.interval_seconds = interval_seconds
        self.stop_conditions = stop_conditions
        self.stop_event = stop_event
        self.stop_reason = stop_reason
        self.samples: list[dict[str, Any]] = []
        self.thread: threading.Thread | None = None
        self.initial_oom_kills = {
            probe.name: int(
                (
                    (probe.sample().get("cgroup") or {}).get("memory_events")
                    or {}
                ).get("oom_kill", 0)
            )
            for probe in probes
        }

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name="capacity-monitor", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        if self.thread is not None:
            self.thread.join(timeout=max(5.0, self.interval_seconds * 3))

    def _run(self) -> None:
        while not self.stop_event.is_set():
            sample = {
                "ts_utc": utc_now(),
                "monotonic_seconds": time.monotonic(),
                "host": {
                    "cpu": read_host_cpu(),
                    "memory": read_meminfo(),
                    "pressure": {
                        "cpu": read_psi(Path("/proc/pressure/cpu")),
                        "memory": read_psi(Path("/proc/pressure/memory")),
                        "io": read_psi(Path("/proc/pressure/io")),
                    },
                    "network": read_netdev(),
                    "loadavg": read_text(Path("/proc/loadavg")),
                },
                "containers": [probe.sample() for probe in self.probes],
            }
            self.samples.append(sample)
            aggregate_memory = sum(
                int((row.get("cgroup") or {}).get("memory_current_bytes") or 0)
                for row in sample["containers"]
            )
            available = int((sample["host"]["memory"] or {}).get("MemAvailable") or 0)
            memory_limit = int(
                self.stop_conditions.get("aggregate_cgroup_memory_bytes") or 0
            )
            host_floor = int(
                self.stop_conditions.get("minimum_host_available_bytes") or 0
            )
            if memory_limit and aggregate_memory >= memory_limit:
                self.stop_reason.append("aggregate_cgroup_memory_limit")
                self.stop_event.set()
                break
            if host_floor and available and available <= host_floor:
                self.stop_reason.append("minimum_host_available_memory")
                self.stop_event.set()
                break
            if any(
                int(
                    ((row.get("cgroup") or {}).get("memory_events") or {}).get(
                        "oom_kill", 0
                    )
                )
                > self.initial_oom_kills.get(str(row.get("name")), 0)
                for row in sample["containers"]
            ):
                self.stop_reason.append("container_oom_kill")
                self.stop_event.set()
                break
            self.stop_event.wait(self.interval_seconds)


def fincore(path: Path) -> dict[str, Any]:
    if not shutil.which("fincore") or not path.is_file():
        return {}
    try:
        completed = subprocess.run(
            ["fincore", "-J", "-b", "-o", "FILE,SIZE,RES,PAGES", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
        rows = json.loads(completed.stdout).get("fincore") or []
        return rows[0] if rows else {}
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return {}


def resolve_artifacts(
    state_dir: Path, manifest: dict[str, Any]
) -> list[tuple[str, Path]]:
    state_real = state_dir.resolve(strict=True)
    active_real = (state_dir / "served/current").resolve(strict=True)
    result: list[tuple[str, Path]] = []
    for relative in manifest.get("artifacts") or []:
        path = state_dir / str(relative)
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(state_real):
            raise ValueError(f"artifact escapes state directory: {relative}")
        if not resolved.is_relative_to(active_real):
            raise ValueError(f"artifact is outside the active served build: {relative}")
        if not resolved.is_file():
            raise ValueError(f"artifact is not a file: {relative}")
        result.append((str(relative), resolved))
    return result


def inventory_artifacts(artifacts: list[tuple[str, Path]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for relative, path in artifacts:
        info = path.stat()
        rows.append(
            {
                "relative_path": relative,
                "resolved_path": str(path),
                "size_bytes": info.st_size,
                "allocated_bytes": info.st_blocks * 512,
                "fincore": fincore(path),
            }
        )
    return rows


def evict_artifact_pages(
    artifacts: list[tuple[str, Path]],
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "method": "targeted_posix_fadvise_dontneed",
        "global_cache_drop": False,
        "started_at_utc": utc_now(),
        "files": [],
    }
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        report["status"] = "unsupported"
        return report
    for relative, path in artifacts:
        before = fincore(path)
        status = "pass"
        error = None
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
        except OSError as exc:
            status = "error"
            error = str(exc)
        after = fincore(path)
        report["files"].append(
            {
                "relative_path": relative,
                "size_bytes": path.stat().st_size,
                "before_resident_bytes": int(before.get("res") or 0),
                "after_resident_bytes": int(after.get("res") or 0),
                "status": status,
                "error": error,
            }
        )
    report["status"] = (
        "pass" if all(row["status"] == "pass" for row in report["files"]) else "partial"
    )
    report["completed_at_utc"] = utc_now()
    report["before_resident_bytes"] = sum(
        row["before_resident_bytes"] for row in report["files"]
    )
    report["after_resident_bytes"] = sum(
        row["after_resident_bytes"] for row in report["files"]
    )
    return report


def choose_weighted(
    rng: random.Random,
    entries: list[tuple[str, int]],
) -> str:
    total = sum(weight for _, weight in entries)
    choice = rng.randrange(total)
    for endpoint_id, weight in entries:
        if choice < weight:
            return endpoint_id
        choice -= weight
    return entries[-1][0]


def request_once(
    *,
    sequence: int,
    worker_id: int,
    endpoint_id: str,
    endpoint: dict[str, Any],
    base_url: str,
    timeout_seconds: float,
    scheduled_at: float | None,
    session: requests.Session,
) -> dict[str, Any]:
    started = time.monotonic()
    start_utc = utc_now()
    queue_delay_ms = (
        max(0.0, (started - scheduled_at) * 1000.0)
        if scheduled_at is not None
        else 0.0
    )
    status = 0
    response_bytes = 0
    content_length = None
    cache_status = None
    error = None
    timed_out = False
    try:
        response = session.get(
            f"{base_url.rstrip('/')}{endpoint['path']}",
            timeout=timeout_seconds,
        )
        status = int(response.status_code)
        response_bytes = len(response.content)
        content_length_raw = response.headers.get("Content-Length")
        if content_length_raw and content_length_raw.isdigit():
            content_length = int(content_length_raw)
        cache_status = response.headers.get("X-Spacegate-Simulation-Scene-Cache")
    except requests.Timeout as exc:
        error = str(exc)
        timed_out = True
    except requests.RequestException as exc:
        error = str(exc)
    elapsed_ms = (time.monotonic() - started) * 1000.0
    expected_status = {int(value) for value in endpoint.get("expected_status") or [200]}
    expected_cache = {str(value) for value in endpoint.get("expected_cache") or []}
    status_ok = status in expected_status
    cache_ok = not expected_cache or cache_status in expected_cache
    return {
        "sequence": sequence,
        "worker_id": worker_id,
        "start_utc": start_utc,
        "endpoint_id": endpoint_id,
        "category": endpoint.get("category") or "unknown",
        "path": endpoint["path"],
        "status": status,
        "ok": status_ok and cache_ok and error is None,
        "status_ok": status_ok,
        "cache_ok": cache_ok,
        "timed_out": timed_out,
        "error": error,
        "latency_ms": round(elapsed_ms, 3),
        "queue_delay_ms": round(queue_delay_ms, 3),
        "response_bytes": response_bytes,
        "content_length_bytes": content_length,
        "scene_cache_status": cache_status,
    }


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [float(row["latency_ms"]) for row in rows if row.get("ok")]
    queue_delays = [float(row["queue_delay_ms"]) for row in rows]
    errors = [row for row in rows if not row.get("ok")]
    elapsed_bounds = [row.get("start_utc") for row in rows]
    return {
        "request_count": len(rows),
        "success_count": len(rows) - len(errors),
        "error_count": len(errors),
        "error_rate_pct": round(100.0 * len(errors) / len(rows), 6) if rows else 0.0,
        "latency_ms": {
            "min": round(min(latencies), 3) if latencies else 0.0,
            "mean": round(statistics.fmean(latencies), 3) if latencies else 0.0,
            "p50": round(percentile(latencies, 50), 3),
            "p95": round(percentile(latencies, 95), 3),
            "p99": round(percentile(latencies, 99), 3),
            "max": round(max(latencies), 3) if latencies else 0.0,
        },
        "queue_delay_ms": {
            "p50": round(percentile(queue_delays, 50), 3),
            "p95": round(percentile(queue_delays, 95), 3),
            "p99": round(percentile(queue_delays, 99), 3),
            "max": round(max(queue_delays), 3) if queue_delays else 0.0,
        },
        "status_counts": dict(
            sorted(Counter(str(row.get("status") or 0) for row in rows).items())
        ),
        "timeout_count": sum(bool(row.get("timed_out")) for row in rows),
        "response_bytes": sum(int(row.get("response_bytes") or 0) for row in rows),
        "scene_cache_counts": dict(
            sorted(
                Counter(
                    str(row.get("scene_cache_status") or "none") for row in rows
                ).items()
            )
        ),
        "first_request_utc": min(elapsed_bounds) if elapsed_bounds else None,
        "last_request_utc": max(elapsed_bounds) if elapsed_bounds else None,
    }


def nested_delta(
    first: dict[str, Any], last: dict[str, Any], path: list[str]
) -> int:
    def lookup(value: dict[str, Any]) -> int:
        current: Any = value
        for key in path:
            if not isinstance(current, dict):
                return 0
            current = current.get(key)
        return int(current or 0)

    return lookup(last) - lookup(first)


def summarize_resources(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {"sample_count": 0}
    containers: dict[str, list[dict[str, Any]]] = defaultdict(list)
    aggregate_memory_values: list[int] = []
    for sample in samples:
        aggregate_memory_values.append(
            sum(
                int((row.get("cgroup") or {}).get("memory_current_bytes") or 0)
                for row in sample.get("containers") or []
            )
        )
        for row in sample.get("containers") or []:
            containers[str(row.get("name"))].append(row)
    container_summary: dict[str, Any] = {}
    for name, rows in containers.items():
        first = rows[0]
        last = rows[-1]
        memory_values = [
            int((row.get("cgroup") or {}).get("memory_current_bytes") or 0)
            for row in rows
        ]
        rss_values = [
            int((row.get("process_group") or {}).get("rss_bytes_sum") or 0)
            for row in rows
        ]
        process_count_values = [
            int((row.get("process_group") or {}).get("process_count") or 0)
            for row in rows
        ]
        cache_values = [
            int((row.get("cgroup") or {}).get("page_cache_bytes") or 0)
            for row in rows
        ]
        dirty_values = [
            int(
                ((row.get("cgroup") or {}).get("memory_stat") or {}).get(
                    "file_dirty", 0
                )
            )
            for row in rows
        ]
        shmem_values = [
            int(
                ((row.get("cgroup") or {}).get("memory_stat") or {}).get(
                    "shmem", 0
                )
            )
            for row in rows
        ]
        pids_values = [
            int((row.get("cgroup") or {}).get("pids_current") or 0)
            for row in rows
        ]

        def container_pressure_delta(resource: str, level: str) -> int:
            first_pressure = (
                ((first.get("cgroup") or {}).get(f"{resource}_pressure") or {}).get(
                    level
                )
                or {}
            )
            last_pressure = (
                ((last.get("cgroup") or {}).get(f"{resource}_pressure") or {}).get(
                    level
                )
                or {}
            )
            return int(last_pressure.get("total") or 0) - int(
                first_pressure.get("total") or 0
            )

        container_summary[name] = {
            "memory_current_peak_bytes": max(memory_values, default=0),
            "process_rss_peak_sampled_bytes": max(rss_values, default=0),
            "process_count_peak_sampled": max(process_count_values, default=0),
            "page_cache_peak_bytes": max(cache_values, default=0),
            "file_dirty_peak_bytes": max(dirty_values, default=0),
            "shmem_peak_bytes": max(shmem_values, default=0),
            "pids_peak_sampled": max(pids_values, default=0),
            "cpu_usage_usec_delta": nested_delta(
                first, last, ["cgroup", "cpu", "usage_usec"]
            ),
            "cpu_user_usec_delta": nested_delta(
                first, last, ["cgroup", "cpu", "user_usec"]
            ),
            "cpu_system_usec_delta": nested_delta(
                first, last, ["cgroup", "cpu", "system_usec"]
            ),
            "cpu_throttled_usec_delta": nested_delta(
                first, last, ["cgroup", "cpu", "throttled_usec"]
            ),
            "cpu_nr_throttled_delta": nested_delta(
                first, last, ["cgroup", "cpu", "nr_throttled"]
            ),
            "io_read_bytes_delta": nested_delta(
                first, last, ["cgroup", "io", "rbytes"]
            ),
            "io_write_bytes_delta": nested_delta(
                first, last, ["cgroup", "io", "wbytes"]
            ),
            "oom_kill_delta": nested_delta(
                first, last, ["cgroup", "memory_events", "oom_kill"]
            ),
            "memory_event_deltas": {
                key: nested_delta(
                    first, last, ["cgroup", "memory_events", key]
                )
                for key in ("low", "high", "max", "oom", "oom_kill")
            },
            "pressure_total_usec_delta": {
                resource: {
                    level: container_pressure_delta(resource, level)
                    for level in ("some", "full")
                }
                for resource in ("cpu", "memory", "io")
            },
        }
    host_available = [
        int(((sample.get("host") or {}).get("memory") or {}).get("MemAvailable") or 0)
        for sample in samples
    ]
    host_cached = [
        int(((sample.get("host") or {}).get("memory") or {}).get("Cached") or 0)
        for sample in samples
    ]
    host_first = samples[0].get("host") or {}
    host_last = samples[-1].get("host") or {}
    first_cpu = host_first.get("cpu") or {}
    last_cpu = host_last.get("cpu") or {}
    cpu_total_delta = int(last_cpu.get("total") or 0) - int(first_cpu.get("total") or 0)
    cpu_idle_delta = int(last_cpu.get("idle_total") or 0) - int(
        first_cpu.get("idle_total") or 0
    )
    host_cpu_busy_pct = (
        100.0 * max(0, cpu_total_delta - cpu_idle_delta) / cpu_total_delta
        if cpu_total_delta > 0
        else 0.0
    )

    def pressure_delta(resource: str, level: str) -> int:
        first = (((host_first.get("pressure") or {}).get(resource) or {}).get(level) or {})
        last = (((host_last.get("pressure") or {}).get(resource) or {}).get(level) or {})
        return int(last.get("total") or 0) - int(first.get("total") or 0)

    network_delta: dict[str, dict[str, int]] = {}
    first_network = host_first.get("network") or {}
    last_network = host_last.get("network") or {}
    for interface in sorted(set(first_network) | set(last_network)):
        before = first_network.get(interface) or {}
        after = last_network.get(interface) or {}
        network_delta[interface] = {
            key: int(after.get(key) or 0) - int(before.get(key) or 0)
            for key in (
                "rx_bytes",
                "rx_packets",
                "rx_errors",
                "rx_drops",
                "tx_bytes",
                "tx_packets",
                "tx_errors",
                "tx_drops",
            )
        }

    return {
        "sample_count": len(samples),
        "aggregate_cgroup_memory_peak_bytes": max(
            aggregate_memory_values, default=0
        ),
        "containers": container_summary,
        "host": {
            "memory_available_min_bytes": min(host_available, default=0),
            "page_cache_peak_bytes": max(host_cached, default=0),
            "cpu_busy_pct": round(host_cpu_busy_pct, 3),
            "pressure_total_usec_delta": {
                resource: {
                    level: pressure_delta(resource, level)
                    for level in ("some", "full")
                }
                for resource in ("cpu", "memory", "io")
            },
            "network": {
                "start": host_first.get("network") or {},
                "end": host_last.get("network") or {},
                "delta": network_delta,
            },
        },
    }


def validate_manifest(manifest: dict[str, Any], profile_name: str) -> dict[str, Any]:
    if manifest.get("schema_version") != "spacegate.runtime_capacity_workload.v1":
        raise ValueError("unsupported workload manifest")
    endpoints = manifest.get("endpoints")
    profiles = manifest.get("profiles")
    if not isinstance(endpoints, dict) or not isinstance(profiles, dict):
        raise ValueError("workload manifest requires endpoints and profiles")
    if profile_name not in profiles:
        raise ValueError(f"unknown profile: {profile_name}")
    profile = profiles[profile_name]
    for endpoint_id, weight in profile.get("entries") or []:
        if endpoint_id not in endpoints:
            raise ValueError(f"profile references unknown endpoint: {endpoint_id}")
        if int(weight) <= 0:
            raise ValueError(f"profile has invalid weight: {endpoint_id}")
    if float(profile.get("duration_seconds") or 0) <= 0:
        raise ValueError(f"profile has invalid duration: {profile_name}")
    if int(profile.get("concurrency") or 0) < 0:
        raise ValueError(f"profile has invalid concurrency: {profile_name}")
    return profile


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--profile", default="mixed")
    parser.add_argument("--base-url", default="http://127.0.0.1:18081")
    parser.add_argument("--state-dir", default=str(DEFAULT_STATE))
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--request-limit", type=int)
    parser.add_argument("--target-rps", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=1.0)
    parser.add_argument("--seed", type=int)
    parser.add_argument(
        "--containers",
        default="spacegate-capacity-api-1,spacegate-capacity-web-1",
    )
    parser.add_argument(
        "--cache-state",
        choices=("warm", "application_cold", "targeted_eviction", "idle"),
        default="warm",
    )
    parser.add_argument("--evict-file-cache", action="store_true")
    parser.add_argument("--label", default="")
    parser.add_argument(
        "--environment-profile",
        choices=("antiproton_like", "unconstrained_photon"),
        default="antiproton_like",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest).resolve(strict=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    profile = validate_manifest(manifest, args.profile)
    state_dir = Path(args.state_dir).resolve(strict=True)
    artifacts = resolve_artifacts(state_dir, manifest)
    seed = int(args.seed if args.seed is not None else manifest["random_seed"])
    duration = float(
        args.duration_seconds
        if args.duration_seconds is not None
        else profile.get("duration_seconds") or 60
    )
    concurrency = int(
        args.concurrency
        if args.concurrency is not None
        else profile.get("concurrency") or 0
    )
    request_limit = (
        int(args.request_limit)
        if args.request_limit is not None
        else int(profile.get("request_limit") or 0)
    )
    synchronized_start = bool(profile.get("synchronized_start"))
    if duration <= 0:
        raise SystemExit("--duration-seconds must be positive")
    if concurrency < 0:
        raise SystemExit("--concurrency cannot be negative")
    if request_limit < 0:
        raise SystemExit("--request-limit cannot be negative")
    if args.target_rps < 0:
        raise SystemExit("--target-rps cannot be negative")
    if synchronized_start and concurrency <= 0:
        raise SystemExit("synchronized profiles require positive concurrency")
    entries = [(str(key), int(weight)) for key, weight in profile.get("entries") or []]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.label:
        run_id = f"{run_id}_{args.label}"
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else state_dir
        / "reports/runtime_capacity_gate"
        / str(manifest["build_id"])
        / f"{run_id}_{args.profile}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)
    atomic_write_json(output_dir / "workload_manifest.snapshot.json", manifest)

    probes = [
        probe
        for name in (value.strip() for value in args.containers.split(","))
        if name and (probe := ContainerProbe.create(name)) is not None
    ]
    if not probes:
        raise SystemExit("No benchmark containers are running")

    health = requests.get(
        f"{args.base_url.rstrip('/')}/api/v1/health", timeout=args.timeout_seconds
    )
    health.raise_for_status()
    health_payload = health.json()
    active_build_id = str(health_payload.get("build_id") or "")
    if active_build_id != str(manifest["build_id"]):
        raise SystemExit(
            f"Build mismatch: workload={manifest['build_id']} active={active_build_id}"
        )

    artifact_before = inventory_artifacts(artifacts)
    eviction = None
    if args.evict_file_cache:
        eviction = evict_artifact_pages(artifacts)
    artifact_after_eviction = inventory_artifacts(artifacts)

    stop_event = threading.Event()
    stop_reason: list[str] = []
    stop_conditions = dict(manifest.get("stop_conditions") or {})
    monitor = ResourceMonitor(
        probes,
        args.sample_interval_seconds,
        stop_conditions,
        stop_event,
        stop_reason,
    )
    rows: list[dict[str, Any]] = []
    rows_lock = threading.Lock()
    rolling: deque[dict[str, Any]] = deque(
        maxlen=int(stop_conditions.get("rolling_window_requests") or 40)
    )
    sequence_lock = threading.Lock()
    sequence = 0
    issued_lock = threading.Lock()
    issued = 0

    def next_sequence() -> int:
        nonlocal sequence
        with sequence_lock:
            sequence += 1
            return sequence

    def record(row: dict[str, Any]) -> None:
        with rows_lock:
            rows.append(row)
            rolling.append(row)
            if len(rolling) == rolling.maxlen:
                error_rate = 100.0 * sum(not value.get("ok") for value in rolling) / len(
                    rolling
                )
                rolling_p95 = percentile(
                    [float(value["latency_ms"]) for value in rolling], 95
                )
                if error_rate >= float(
                    stop_conditions.get("rolling_error_rate_pct") or 100.0
                ):
                    stop_reason.append("rolling_error_rate")
                    stop_event.set()
                elif rolling_p95 >= float(
                    stop_conditions.get("rolling_p95_latency_ms") or 1e12
                ):
                    stop_reason.append("rolling_latency_collapse")
                    stop_event.set()

    thread_local = threading.local()

    def get_session() -> requests.Session:
        session = getattr(thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers["User-Agent"] = (
                f"SpacegateRuntimeCapacity/{manifest['workload_id']}"
            )
            thread_local.session = session
        return session

    def execute(
        endpoint_id: str, worker_id: int, scheduled_at: float | None = None
    ) -> dict[str, Any]:
        row = request_once(
            sequence=next_sequence(),
            worker_id=worker_id,
            endpoint_id=endpoint_id,
            endpoint=manifest["endpoints"][endpoint_id],
            base_url=args.base_url,
            timeout_seconds=args.timeout_seconds,
            scheduled_at=scheduled_at,
            session=get_session(),
        )
        record(row)
        return row

    started_at_utc = utc_now()
    started = time.monotonic()
    monitor.start()
    try:
        if concurrency <= 0 or not entries:
            stop_event.wait(duration)
        elif synchronized_start:
            expanded = [
                endpoint_id
                for endpoint_id, weight in entries
                for _ in range(weight)
            ]
            count = request_limit or concurrency
            barrier = threading.Barrier(min(concurrency, count))

            def synchronized_request(index: int) -> dict[str, Any]:
                barrier.wait(timeout=max(10.0, args.timeout_seconds))
                return execute(expanded[index % len(expanded)], index)

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(synchronized_request, index) for index in range(count)]
                for future in as_completed(futures):
                    future.result()
        elif args.target_rps > 0:
            interval = 1.0 / args.target_rps
            end_at = started + duration
            rng = random.Random(seed)
            pending: set[Future[dict[str, Any]]] = set()
            queue_limit = max(
                concurrency,
                concurrency
                * int(stop_conditions.get("client_queue_depth_per_worker") or 4),
            )
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                scheduled_at = started
                worker_id = 0
                while time.monotonic() < end_at and not stop_event.is_set():
                    now = time.monotonic()
                    if now < scheduled_at:
                        time.sleep(min(scheduled_at - now, 0.05))
                        continue
                    completed = {future for future in pending if future.done()}
                    for future in completed:
                        future.result()
                    pending.difference_update(completed)
                    if len(pending) >= queue_limit:
                        stop_reason.append("runaway_client_queue")
                        stop_event.set()
                        break
                    endpoint_id = choose_weighted(rng, entries)
                    pending.add(
                        executor.submit(execute, endpoint_id, worker_id, scheduled_at)
                    )
                    worker_id = (worker_id + 1) % concurrency
                    scheduled_at += interval
                for future in as_completed(pending):
                    future.result()
        else:
            end_at = started + duration

            def closed_loop_worker(worker_id: int) -> None:
                nonlocal issued
                rng = random.Random(seed + worker_id * 1_000_003)
                while time.monotonic() < end_at and not stop_event.is_set():
                    with issued_lock:
                        if request_limit and issued >= request_limit:
                            return
                        issued += 1
                    endpoint_id = choose_weighted(rng, entries)
                    execute(endpoint_id, worker_id)

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(closed_loop_worker, worker_id)
                    for worker_id in range(concurrency)
                ]
                for future in as_completed(futures):
                    future.result()
    finally:
        elapsed = time.monotonic() - started
        stop_event.set()
        monitor.stop()
    completed_at_utc = utc_now()

    rows.sort(key=lambda row: int(row["sequence"]))
    request_summary = summarize_rows(rows)
    request_summary["throughput_rps"] = round(
        len(rows) / elapsed if elapsed > 0 else 0.0, 6
    )
    by_category: dict[str, Any] = {}
    for category in sorted({str(row["category"]) for row in rows}):
        by_category[category] = summarize_rows(
            [row for row in rows if row["category"] == category]
        )
    by_endpoint: dict[str, Any] = {}
    for endpoint_id in sorted({str(row["endpoint_id"]) for row in rows}):
        by_endpoint[endpoint_id] = summarize_rows(
            [row for row in rows if row["endpoint_id"] == endpoint_id]
        )

    aggregate_error_rate = float(request_summary["error_rate_pct"])
    resources = summarize_resources(monitor.samples)
    gates = {
        "build_identity_match": active_build_id == str(manifest["build_id"]),
        "requests_executed_or_idle": bool(rows) or concurrency == 0,
        "error_rate_within_stop_budget": aggregate_error_rate
        <= float(stop_conditions.get("error_rate_pct") or 100.0),
        "no_timeout": int(request_summary["timeout_count"]) == 0,
        "no_oom_kill": all(
            int(value.get("oom_kill_delta") or 0) == 0
            for value in (
                resources.get("containers") or {}
            ).values()
        ),
        "no_safety_stop": not stop_reason,
    }
    cpu_limits = [
        value
        for probe in probes
        if (value := cpu_quota_cores(probe.cpu_quota)) is not None
    ]
    memory_limits = [
        int(probe.memory_limit)
        for probe in probes
        if probe.memory_limit is not None
    ]
    resource_model = {
        "profile": args.environment_profile,
        "aggregate_cpu_quota_cores": (
            round(sum(cpu_limits), 3) if len(cpu_limits) == len(probes) else None
        ),
        "aggregate_hard_memory_limit_bytes": (
            sum(memory_limits) if len(memory_limits) == len(probes) else None
        ),
        "modeled_host_memory_bytes": (
            12 * 1024**3 if args.environment_profile == "antiproton_like" else None
        ),
        "host_reserve_bytes": (
            3 * 1024**3 if args.environment_profile == "antiproton_like" else None
        ),
        "api_duckdb_memory_limit": (
            "5GB" if args.environment_profile == "antiproton_like" else "8GB"
        ),
        "api_duckdb_threads": (
            4 if args.environment_profile == "antiproton_like" else 8
        ),
        "api_worker_count": 1,
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "pass" if all(gates.values()) else "fail",
        "run_id": run_id,
        "profile": args.profile,
        "label": args.label or None,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "elapsed_seconds": round(elapsed, 6),
        "workload": {
            "manifest_path": str(manifest_path),
            "manifest_sha256": file_sha256(manifest_path),
            "workload_id": manifest["workload_id"],
            "random_seed": seed,
            "build_id": manifest["build_id"],
            "active_build_id": active_build_id,
            "cache_state": args.cache_state,
            "duration_seconds": duration,
            "concurrency": concurrency,
            "request_limit": request_limit or None,
            "target_rps": args.target_rps or None,
            "client_model": (
                "synchronized_burst"
                if synchronized_start
                else ("open_loop" if args.target_rps > 0 else "closed_loop")
            ),
            "synchronized_start": synchronized_start,
            "timeout_seconds": args.timeout_seconds,
            "acceptance": profile.get("acceptance") or {},
        },
        "environment": {
            "hostname": platform.node(),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "logical_cpu_count": os.cpu_count(),
            "host_memory_total_bytes": read_meminfo().get("MemTotal"),
            "note": (
                "Photon cgroup quotas model resource quantity, not the exact per-core "
                "performance of the OVH antiproton host."
            ),
            "containers": [
                {
                    "name": probe.name,
                    "container_id": probe.container_id,
                    "pid": probe.pid,
                    "cpu_max": probe.cpu_quota,
                    "memory_limit_bytes": probe.memory_limit,
                    "cgroup_path": str(probe.cgroup_path),
                }
                for probe in probes
            ],
            "resource_model": resource_model,
        },
        "cache_preparation": eviction,
        "artifacts_before": artifact_before,
        "artifacts_after_cache_preparation": artifact_after_eviction,
        "requests": request_summary,
        "by_category": by_category,
        "by_endpoint": by_endpoint,
        "resources": resources,
        "stop": {
            "triggered": bool(stop_reason),
            "reasons": list(dict.fromkeys(stop_reason)),
            "conditions": stop_conditions,
        },
        "gates": gates,
    }

    with (output_dir / "requests.jsonl").open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    with (output_dir / "requests.csv").open("w", encoding="utf-8", newline="") as handle:
        fieldnames = list(rows[0].keys()) if rows else [
            "sequence",
            "worker_id",
            "start_utc",
            "endpoint_id",
            "category",
            "path",
            "status",
            "ok",
            "latency_ms",
            "queue_delay_ms",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    with (output_dir / "resource_samples.jsonl").open("w", encoding="utf-8") as handle:
        handle.write(json.dumps({"schema_version": SAMPLE_SCHEMA_VERSION}) + "\n")
        for sample in monitor.samples:
            handle.write(json.dumps(sample, sort_keys=True) + "\n")
    atomic_write_json(
        output_dir / "request_contract.json",
        {"schema_version": REQUEST_SCHEMA_VERSION, "columns": list(rows[0]) if rows else []},
    )
    atomic_write_json(output_dir / "summary.json", report)
    print(json.dumps(
        {
            "status": report["status"],
            "profile": args.profile,
            "requests": len(rows),
            "rps": request_summary["throughput_rps"],
            "p95_ms": request_summary["latency_ms"]["p95"],
            "errors": request_summary["error_count"],
            "stop_reasons": report["stop"]["reasons"],
            "output_dir": str(output_dir),
        },
        indent=2,
        sort_keys=True,
    ))
    return 0 if report["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
