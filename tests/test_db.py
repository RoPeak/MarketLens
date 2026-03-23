import duckdb

from marketlens.db import ALL_TABLES, BRONZE_TABLES, bootstrap_schema, get_connection


def test_get_connection_memory():
    conn = get_connection(":memory:")
    assert conn is not None
    conn.close()


def test_bootstrap_schema_creates_all_tables(db_conn):
    """All expected tables exist after bootstrap_schema runs."""
    result = db_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    existing = {row[0] for row in result}

    for table in ALL_TABLES:
        assert table in existing, f"Expected table '{table}' to exist after bootstrap"


def test_bronze_tables_have_expected_columns(db_conn):
    """Each bronze table has the standard OHLCV + metadata columns."""
    expected_columns = {
        "source",
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ingested_at",
    }

    for table in BRONZE_TABLES:
        result = db_conn.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
        ).fetchall()
        cols = {row[0] for row in result}
        assert expected_columns.issubset(cols), f"{table} missing: {expected_columns - cols}"


def test_silver_prices_has_expected_columns(db_conn):
    """silver_prices has asset_class, return columns, and outlier flag."""
    expected_columns = {
        "source",
        "symbol",
        "asset_class",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "log_return",
        "is_outlier",
        "transformed_at",
    }
    result = db_conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'silver_prices'"
    ).fetchall()
    cols = {row[0] for row in result}
    assert expected_columns.issubset(cols), f"silver_prices missing: {expected_columns - cols}"


def test_bootstrap_is_idempotent(db_conn):
    """Calling bootstrap_schema twice does not raise."""
    bootstrap_schema(db_conn)  # second call
    result = db_conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchone()
    assert result[0] == len(ALL_TABLES)


def test_bronze_primary_keys_enforce_uniqueness(db_conn):
    """Inserting duplicate (symbol, date) into a bronze table raises."""
    db_conn.execute(
        "INSERT INTO bronze_equities (source, symbol, date, close)"
        " VALUES ('test', 'SPY', '2024-01-01', 100.0)"
    )
    try:
        db_conn.execute(
            "INSERT INTO bronze_equities (source, symbol, date, close)"
            " VALUES ('test', 'SPY', '2024-01-01', 101.0)"
        )
        raise AssertionError("Expected a constraint violation")
    except duckdb.ConstraintException:
        pass  # expected
