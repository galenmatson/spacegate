import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Iterator

import duckdb


ROOT_DIR = Path(__file__).resolve().parents[3]
DEFAULT_STATE_DIR = Path(
    os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or ROOT_DIR / "data"
)
DEFAULT_DB_PATH = str(DEFAULT_STATE_DIR / "served" / "current" / "core.duckdb")
DEFAULT_DUCKDB_MEMORY_LIMIT = os.getenv("SPACEGATE_API_DUCKDB_MEMORY_LIMIT", "").strip()
DEFAULT_DUCKDB_THREADS = os.getenv("SPACEGATE_API_DUCKDB_THREADS", "").strip()
DEFAULT_DB_MAX_CONCURRENT_CONNECTIONS = os.getenv(
    "SPACEGATE_API_DB_MAX_CONCURRENT_CONNECTIONS", ""
).strip()
DEFAULT_DB_POOL_SIZE = os.getenv("SPACEGATE_API_DB_POOL_SIZE", "").strip()
DEFAULT_DB_ACQUIRE_TIMEOUT_SECONDS = os.getenv(
    "SPACEGATE_API_DB_ACQUIRE_TIMEOUT_SECONDS", "30"
).strip()


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _positive_float(value: str, fallback: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    return parsed if parsed > 0 else fallback


DB_MAX_CONCURRENT_CONNECTIONS = _positive_int(
    DEFAULT_DB_MAX_CONCURRENT_CONNECTIONS
)
DB_POOL_SIZE = _positive_int(DEFAULT_DB_POOL_SIZE)
DB_ACQUIRE_TIMEOUT_SECONDS = _positive_float(
    DEFAULT_DB_ACQUIRE_TIMEOUT_SECONDS, 30.0
)
_CONNECTION_SEMAPHORE = (
    threading.BoundedSemaphore(DB_MAX_CONCURRENT_CONNECTIONS)
    if DB_MAX_CONCURRENT_CONNECTIONS and not DB_POOL_SIZE
    else None
)


class DatabaseUnavailable(RuntimeError):
    pass


def get_db_path() -> str:
    return os.getenv("SPACEGATE_DB_PATH", DEFAULT_DB_PATH)


def _database_fingerprint() -> tuple[str, int, int, int, int]:
    path = Path(get_db_path())
    try:
        resolved = path.resolve(strict=True)
        info = resolved.stat()
    except FileNotFoundError as exc:
        raise DatabaseUnavailable("Database not found") from exc
    return (
        str(resolved),
        int(info.st_dev),
        int(info.st_ino),
        int(info.st_size),
        int(info.st_mtime_ns),
    )


def _apply_runtime_limits(con: duckdb.DuckDBPyConnection) -> None:
    # Use explicit API caps so concurrent search traffic cannot consume the full host.
    if DEFAULT_DUCKDB_MEMORY_LIMIT:
        try:
            con.execute(f"SET memory_limit='{DEFAULT_DUCKDB_MEMORY_LIMIT}'")
        except Exception:
            pass
    if DEFAULT_DUCKDB_THREADS:
        try:
            threads = int(DEFAULT_DUCKDB_THREADS)
            if threads >= 1:
                con.execute(f"SET threads TO {threads}")
        except Exception:
            pass


def _open_connection(path: str) -> duckdb.DuckDBPyConnection:
    if not os.path.exists(path):
        raise DatabaseUnavailable("Database not found")
    con = duckdb.connect(path, read_only=True)
    _apply_runtime_limits(con)
    return con


def get_connection() -> duckdb.DuckDBPyConnection:
    return _open_connection(get_db_path())


@dataclass
class _PooledConnection:
    connection: duckdb.DuckDBPyConnection
    fingerprint: tuple[str, int, int, int, int]


class _ConnectionPool:
    def __init__(self, size: int, timeout_seconds: float) -> None:
        self.size = size
        self.timeout_seconds = timeout_seconds
        self._condition = threading.Condition()
        self._idle: list[_PooledConnection] = []
        self._total = 0
        self._active = 0
        self._fingerprint: tuple[str, int, int, int, int] | None = None
        self._waiters = 0
        self._checkout_count = 0
        self._create_count = 0
        self._reuse_count = 0
        self._invalidation_count = 0
        self._timeout_count = 0
        self._wait_seconds_total = 0.0
        self._peak_active = 0
        self._peak_waiters = 0

    def _refresh_locked(
        self, fingerprint: tuple[str, int, int, int, int]
    ) -> list[duckdb.DuckDBPyConnection]:
        if self._fingerprint == fingerprint:
            return []
        stale = [item.connection for item in self._idle]
        self._total -= len(self._idle)
        self._idle.clear()
        if self._fingerprint is not None:
            self._invalidation_count += 1
        self._fingerprint = fingerprint
        return stale

    def checkout(self) -> _PooledConnection:
        fingerprint = _database_fingerprint()
        deadline = time.monotonic() + self.timeout_seconds
        wait_started = time.monotonic()
        waiting = False
        stale: list[duckdb.DuckDBPyConnection] = []
        create = False
        item: _PooledConnection | None = None
        try:
            with self._condition:
                while True:
                    stale.extend(self._refresh_locked(fingerprint))
                    if self._idle:
                        item = self._idle.pop()
                        self._reuse_count += 1
                        break
                    if self._total < self.size:
                        self._total += 1
                        create = True
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._timeout_count += 1
                        raise DatabaseUnavailable(
                            "Database connection pool is busy; retry the request"
                        )
                    if not waiting:
                        waiting = True
                        self._waiters += 1
                        self._peak_waiters = max(
                            self._peak_waiters, self._waiters
                        )
                    self._condition.wait(timeout=remaining)
                    fingerprint = _database_fingerprint()
                if waiting:
                    self._waiters -= 1
                    waiting = False
                self._active += 1
                self._peak_active = max(self._peak_active, self._active)
                self._checkout_count += 1
                self._wait_seconds_total += time.monotonic() - wait_started
        finally:
            if waiting:
                with self._condition:
                    self._waiters -= 1
                    self._condition.notify()
            for connection in stale:
                connection.close()

        if item is not None:
            return item
        if not create:
            raise DatabaseUnavailable("Database connection pool checkout failed")
        try:
            connection = _open_connection(fingerprint[0])
        except Exception:
            with self._condition:
                self._total -= 1
                self._active -= 1
                self._condition.notify()
            raise
        with self._condition:
            self._create_count += 1
        return _PooledConnection(connection=connection, fingerprint=fingerprint)

    def release(self, item: _PooledConnection) -> None:
        close = False
        try:
            current = _database_fingerprint()
        except DatabaseUnavailable:
            current = None
        with self._condition:
            self._active -= 1
            if (
                current is not None
                and current == item.fingerprint
                and current == self._fingerprint
            ):
                self._idle.append(item)
            else:
                self._total -= 1
                close = True
            self._condition.notify()
        if close:
            item.connection.close()

    def stats(self) -> dict[str, Any]:
        with self._condition:
            return {
                "enabled": True,
                "size": self.size,
                "open_connections": self._total,
                "active_connections": self._active,
                "idle_connections": len(self._idle),
                "waiters": self._waiters,
                "peak_active_connections": self._peak_active,
                "peak_waiters": self._peak_waiters,
                "checkout_count": self._checkout_count,
                "created_connections": self._create_count,
                "reused_checkouts": self._reuse_count,
                "build_invalidations": self._invalidation_count,
                "checkout_timeouts": self._timeout_count,
                "wait_seconds_total": round(self._wait_seconds_total, 6),
            }


_CONNECTION_POOL = (
    _ConnectionPool(DB_POOL_SIZE, DB_ACQUIRE_TIMEOUT_SECONDS)
    if DB_POOL_SIZE
    else None
)
_BUILD_ID_LOCK = threading.Lock()
_BUILD_ID_FINGERPRINT: tuple[str, int, int, int, int] | None = None
_BUILD_ID: str | None = None


def build_id() -> str | None:
    """Return immutable build identity without consuming the request pool."""
    global _BUILD_ID_FINGERPRINT, _BUILD_ID

    fingerprint = _database_fingerprint()
    with _BUILD_ID_LOCK:
        if fingerprint == _BUILD_ID_FINGERPRINT:
            return _BUILD_ID
        con = _open_connection(fingerprint[0])
        try:
            row = con.execute(
                "SELECT value FROM build_metadata WHERE key = 'build_id'"
            ).fetchone()
        finally:
            con.close()
        _BUILD_ID = str(row[0]) if row and row[0] is not None else None
        _BUILD_ID_FINGERPRINT = fingerprint
        return _BUILD_ID


def runtime_stats() -> dict[str, Any]:
    if _CONNECTION_POOL is not None:
        return _CONNECTION_POOL.stats()
    return {
        "enabled": False,
        "max_concurrent_connections": DB_MAX_CONCURRENT_CONNECTIONS or None,
    }


@contextmanager
def connection_scope() -> Iterator[duckdb.DuckDBPyConnection]:
    if _CONNECTION_POOL is not None:
        item = _CONNECTION_POOL.checkout()
        try:
            yield item.connection
        finally:
            _CONNECTION_POOL.release(item)
        return

    acquired = False
    if _CONNECTION_SEMAPHORE is not None:
        acquired = _CONNECTION_SEMAPHORE.acquire(
            timeout=DB_ACQUIRE_TIMEOUT_SECONDS
        )
        if not acquired:
            raise DatabaseUnavailable(
                "Database concurrency limit reached; retry the request"
            )
    con = None
    try:
        con = get_connection()
        yield con
    finally:
        if con is not None:
            con.close()
        if acquired and _CONNECTION_SEMAPHORE is not None:
            _CONNECTION_SEMAPHORE.release()
