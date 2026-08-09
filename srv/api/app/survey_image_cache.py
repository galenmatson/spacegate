from __future__ import annotations

import contextlib
import json
import os
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator
from urllib.request import Request, urlopen


class NegativeCacheHit(RuntimeError):
    """A recent upstream failure is cached so callers do not hammer the provider."""


_LOCKS_GUARD = threading.Lock()
_KEY_LOCKS: dict[str, threading.Lock] = {}
_METRICS_GUARD = threading.Lock()
_METRICS: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
_REMOTE_SEMAPHORES: dict[str, threading.BoundedSemaphore] = {}
_RATE_GUARD = threading.Lock()
_RATE_LOCKS: dict[str, threading.Lock] = {}
_LAST_REMOTE_START: dict[str, float] = defaultdict(float)


def _metric(provider: str, key: str, amount: float = 1.0) -> None:
    with _METRICS_GUARD:
        _METRICS[provider][key] += amount


def metrics(provider: str) -> dict[str, int | float]:
    with _METRICS_GUARD:
        values = dict(_METRICS.get(provider, {}))
    result: dict[str, int | float] = {}
    for key, value in values.items():
        result[key] = round(value, 6) if key.endswith("_seconds") else int(value)
    requests = int(result.get("remote_requests", 0))
    if requests:
        result["remote_latency_mean_seconds"] = round(
            float(values.get("remote_latency_seconds", 0.0)) / requests,
            6,
        )
    return result


@contextlib.contextmanager
def coalesced(provider: str, key: str) -> Iterator[bool]:
    lock_key = f"{provider}:{key}"
    with _LOCKS_GUARD:
        lock = _KEY_LOCKS.setdefault(lock_key, threading.Lock())
    waited = not lock.acquire(blocking=False)
    if waited:
        _metric(provider, "coalesced_waits")
        lock.acquire()
    try:
        yield waited
    finally:
        lock.release()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def read_negative(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if float(payload.get("expires_unix") or 0) <= time.time():
            path.unlink(missing_ok=True)
            return None
        return payload
    except Exception:
        path.unlink(missing_ok=True)
        return None


def write_negative(path: Path, *, provider: str, ttl_seconds: int, error: Exception) -> None:
    payload = {
        "schema_version": "survey_image_negative_cache_v1",
        "provider": provider,
        "created_unix": time.time(),
        "expires_unix": time.time() + max(30, int(ttl_seconds)),
        "error_type": type(error).__name__,
        "message": str(error)[:500],
    }
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _metric(provider, "negative_cache_writes")


def raise_if_negative(path: Path, *, provider: str) -> None:
    payload = read_negative(path)
    if not payload:
        return
    _metric(provider, "negative_cache_hits")
    raise NegativeCacheHit(str(payload.get("message") or "Recent provider failure is cached."))


def clear_negative(path: Path) -> None:
    path.unlink(missing_ok=True)


def record_cache(provider: str, kind: str, hit: bool) -> None:
    _metric(provider, f"{kind}_{'hits' if hit else 'misses'}")


def record_client_event(provider: str, event: str) -> None:
    allowed = {"metadata_started", "metadata_abandoned", "preview_started", "preview_abandoned", "preview_loaded", "preview_failed"}
    if event not in allowed:
        raise ValueError("Unsupported survey image client event")
    _metric(provider, f"client_{event}")


def enforce_oldest_first(root: Path, limit_bytes: int) -> dict[str, int]:
    root.mkdir(parents=True, exist_ok=True)
    files: list[tuple[float, int, Path]] = []
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file() and not (path.name.startswith(".") and path.name.endswith(".tmp")):
                stat = path.stat()
                total += stat.st_size
                files.append((stat.st_mtime, stat.st_size, path))
        except OSError:
            continue
    removed_files = 0
    removed_bytes = 0
    for _, size, path in sorted(files):
        if total <= limit_bytes:
            break
        try:
            path.unlink()
            total -= size
            removed_files += 1
            removed_bytes += size
        except OSError:
            continue
    return {
        "limit_bytes": int(limit_bytes),
        "total_bytes": total,
        "removed_files": removed_files,
        "removed_bytes": removed_bytes,
    }


def fetch_bytes(
    url: str,
    *,
    provider: str,
    user_agent: str,
    timeout_seconds: float,
    attempts: int = 3,
    max_concurrency: int = 2,
    min_interval_seconds: float = 0.15,
) -> bytes:
    with _LOCKS_GUARD:
        semaphore = _REMOTE_SEMAPHORES.setdefault(
            provider,
            threading.BoundedSemaphore(max(1, int(max_concurrency))),
        )
        rate_lock = _RATE_LOCKS.setdefault(provider, threading.Lock())
    last_error: Exception | None = None
    for attempt in range(max(1, int(attempts))):
        if attempt:
            _metric(provider, "remote_retries")
            time.sleep(min(2.0, 0.25 * (2 ** (attempt - 1))))
        with semaphore:
            with rate_lock:
                with _RATE_GUARD:
                    last_start = _LAST_REMOTE_START[provider]
                delay = max(0.0, min_interval_seconds - (time.monotonic() - last_start))
                if delay:
                    time.sleep(delay)
                with _RATE_GUARD:
                    _LAST_REMOTE_START[provider] = time.monotonic()
            started = time.monotonic()
            try:
                request = Request(url, headers={"User-Agent": user_agent})
                with urlopen(request, timeout=timeout_seconds) as response:
                    payload = response.read()
                _metric(provider, "remote_requests")
                _metric(provider, "remote_bytes", len(payload))
                _metric(provider, "remote_latency_seconds", time.monotonic() - started)
                return payload
            except Exception as exc:  # provider/network errors are retried and then negatively cached by caller
                last_error = exc
                _metric(provider, "remote_requests")
                _metric(provider, "remote_failures")
                _metric(provider, "remote_latency_seconds", time.monotonic() - started)
    assert last_error is not None
    raise last_error
