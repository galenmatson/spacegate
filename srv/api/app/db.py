import os
from pathlib import Path
from contextlib import contextmanager
from typing import Iterator

import duckdb


ROOT_DIR = Path(__file__).resolve().parents[3]
DEFAULT_STATE_DIR = Path(
    os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or ROOT_DIR / "data"
)
DEFAULT_DB_PATH = str(DEFAULT_STATE_DIR / "served" / "current" / "core.duckdb")
DEFAULT_DUCKDB_MEMORY_LIMIT = os.getenv("SPACEGATE_API_DUCKDB_MEMORY_LIMIT", "").strip()
DEFAULT_DUCKDB_THREADS = os.getenv("SPACEGATE_API_DUCKDB_THREADS", "").strip()


class DatabaseUnavailable(RuntimeError):
    pass


def get_db_path() -> str:
    return os.getenv("SPACEGATE_DB_PATH", DEFAULT_DB_PATH)


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


def get_connection() -> duckdb.DuckDBPyConnection:
    path = get_db_path()
    if not os.path.exists(path):
        raise DatabaseUnavailable(f"Database not found at {path}")
    con = duckdb.connect(path, read_only=True)
    _apply_runtime_limits(con)
    return con


@contextmanager
def connection_scope() -> Iterator[duckdb.DuckDBPyConnection]:
    con = get_connection()
    try:
        yield con
    finally:
        con.close()
