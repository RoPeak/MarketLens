import duckdb
import pytest

from marketlens.db import bootstrap_schema


@pytest.fixture
def db_conn():
    """In-memory DuckDB connection with schema bootstrapped. Isolated per test."""
    conn = duckdb.connect(":memory:")
    bootstrap_schema(conn)
    yield conn
    conn.close()
