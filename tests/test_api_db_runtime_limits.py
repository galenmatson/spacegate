from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app import db  # noqa: E402


class FakeSemaphore:
    def __init__(self, acquired: bool = True) -> None:
        self.acquired = acquired
        self.acquire_timeout: float | None = None
        self.release_count = 0

    def acquire(self, *, timeout: float) -> bool:
        self.acquire_timeout = timeout
        return self.acquired

    def release(self) -> None:
        self.release_count += 1


class FakeConnection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_runtime_limit_parsers_are_fail_closed() -> None:
    assert db._positive_int("4") == 4
    assert db._positive_int("0") == 0
    assert db._positive_int("invalid") == 0
    assert db._positive_float("1.5", 30.0) == 1.5
    assert db._positive_float("-2", 30.0) == 30.0
    assert db._positive_float("invalid", 30.0) == 30.0


def test_connection_scope_releases_capacity_and_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    semaphore = FakeSemaphore()
    connection = FakeConnection()
    monkeypatch.setattr(db, "_CONNECTION_SEMAPHORE", semaphore)
    monkeypatch.setattr(db, "DB_ACQUIRE_TIMEOUT_SECONDS", 7.0)
    monkeypatch.setattr(db, "get_connection", lambda: connection)

    with db.connection_scope() as yielded:
        assert yielded is connection

    assert semaphore.acquire_timeout == 7.0
    assert semaphore.release_count == 1
    assert connection.closed is True


def test_connection_scope_returns_capacity_when_open_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    semaphore = FakeSemaphore()
    monkeypatch.setattr(db, "_CONNECTION_SEMAPHORE", semaphore)

    def fail() -> None:
        raise db.DatabaseUnavailable("missing")

    monkeypatch.setattr(db, "get_connection", fail)
    with pytest.raises(db.DatabaseUnavailable, match="missing"):
        with db.connection_scope():
            pass
    assert semaphore.release_count == 1


def test_connection_scope_fails_with_explicit_retry_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    semaphore = FakeSemaphore(acquired=False)
    monkeypatch.setattr(db, "_CONNECTION_SEMAPHORE", semaphore)
    monkeypatch.setattr(db, "DB_ACQUIRE_TIMEOUT_SECONDS", 0.5)

    with pytest.raises(db.DatabaseUnavailable, match="concurrency limit"):
        with db.connection_scope():
            pass
    assert semaphore.acquire_timeout == 0.5
    assert semaphore.release_count == 0


def test_connection_pool_reuses_connections_and_invalidates_on_build_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fingerprint = [("/build-a/core.duckdb", 1, 2, 3, 4)]
    opened: list[FakeConnection] = []

    def open_connection(_path: str) -> FakeConnection:
        connection = FakeConnection()
        opened.append(connection)
        return connection

    monkeypatch.setattr(db, "_database_fingerprint", lambda: fingerprint[0])
    monkeypatch.setattr(db, "_open_connection", open_connection)
    pool = db._ConnectionPool(size=1, timeout_seconds=0.1)

    first = pool.checkout()
    pool.release(first)
    second = pool.checkout()
    assert second.connection is first.connection
    pool.release(second)
    assert pool.stats()["reused_checkouts"] == 1

    fingerprint[0] = ("/build-b/core.duckdb", 1, 5, 6, 7)
    replacement = pool.checkout()
    assert replacement.connection is not first.connection
    assert first.connection.closed is True
    pool.release(replacement)
    assert pool.stats()["build_invalidations"] == 1


def test_connection_pool_times_out_when_all_connections_are_checked_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fingerprint = ("/build-a/core.duckdb", 1, 2, 3, 4)
    monkeypatch.setattr(db, "_database_fingerprint", lambda: fingerprint)
    monkeypatch.setattr(db, "_open_connection", lambda _path: FakeConnection())
    pool = db._ConnectionPool(size=1, timeout_seconds=0.01)
    checked_out = pool.checkout()

    with pytest.raises(db.DatabaseUnavailable, match="pool is busy"):
        pool.checkout()

    pool.release(checked_out)
    assert pool.stats()["checkout_timeouts"] == 1


def test_build_id_is_cached_by_database_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fingerprint = [("/build-a/core.duckdb", 1, 2, 3, 4)]
    opened: list[FakeConnection] = []

    class BuildConnection(FakeConnection):
        def execute(self, _query: str) -> BuildConnection:
            return self

        def fetchone(self) -> tuple[str]:
            return ("build-a",)

    def open_connection(_path: str) -> BuildConnection:
        connection = BuildConnection()
        opened.append(connection)
        return connection

    monkeypatch.setattr(db, "_BUILD_ID_FINGERPRINT", None)
    monkeypatch.setattr(db, "_BUILD_ID", None)
    monkeypatch.setattr(db, "_database_fingerprint", lambda: fingerprint[0])
    monkeypatch.setattr(db, "_open_connection", open_connection)

    assert db.build_id() == "build-a"
    assert db.build_id() == "build-a"
    assert len(opened) == 1
    assert opened[0].closed is True

    fingerprint[0] = ("/build-b/core.duckdb", 1, 5, 6, 7)
    assert db.build_id() == "build-a"
    assert len(opened) == 2
