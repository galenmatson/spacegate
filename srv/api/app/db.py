import os
from pathlib import Path
from contextlib import contextmanager
from typing import Iterator

import duckdb


ROOT_DIR = Path(__file__).resolve().parents[3]
DEFAULT_STATE_DIR = Path(os.getenv("SPACEGATE_STATE_DIR") or ROOT_DIR / "data")
DEFAULT_DB_PATH = str(DEFAULT_STATE_DIR / "served" / "current" / "core.duckdb")


class DatabaseUnavailable(RuntimeError):
    pass


def get_db_path() -> str:
    return os.getenv("SPACEGATE_DB_PATH", DEFAULT_DB_PATH)


def get_connection() -> duckdb.DuckDBPyConnection:
    path = get_db_path()
    if not os.path.exists(path):
        raise DatabaseUnavailable(f"Database not found at {path}")
    return duckdb.connect(path, read_only=True)


@contextmanager
def connection_scope() -> Iterator[duckdb.DuckDBPyConnection]:
    con = get_connection()
    try:
        yield con
    finally:
        con.close()
