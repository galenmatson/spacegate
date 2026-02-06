import os
from contextlib import contextmanager
from typing import Iterator

import duckdb


DEFAULT_DB_PATH = "/data/spacegate/served/current/core.duckdb"


def get_db_path() -> str:
    return os.getenv("SPACEGATE_DB_PATH", DEFAULT_DB_PATH)


def get_connection() -> duckdb.DuckDBPyConnection:
    path = get_db_path()
    return duckdb.connect(path, read_only=True)


@contextmanager
def connection_scope() -> Iterator[duckdb.DuckDBPyConnection]:
    con = get_connection()
    try:
        yield con
    finally:
        con.close()
