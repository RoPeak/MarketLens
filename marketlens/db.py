from pathlib import Path

import duckdb
from loguru import logger


def get_connection(db_path: Path | str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection. Use ':memory:' for tests."""
    if db_path != ":memory:":
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    return conn


def bootstrap_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all Bronze and Silver tables if they do not exist."""
    _create_bronze_tables(conn)
    _create_silver_tables(conn)
    logger.info("Schema bootstrapped successfully")


# ---------------------------------------------------------------------------
# Bronze layer — raw ingested data, one table per source
# ---------------------------------------------------------------------------

_BRONZE_EQUITIES_DDL = """
CREATE TABLE IF NOT EXISTS bronze_equities (
    source      VARCHAR     NOT NULL,
    symbol      VARCHAR     NOT NULL,
    date        DATE        NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      DOUBLE,
    ingested_at TIMESTAMPTZ DEFAULT current_timestamp,
    PRIMARY KEY (symbol, date)
);
"""

_BRONZE_CRYPTO_DDL = """
CREATE TABLE IF NOT EXISTS bronze_crypto (
    source      VARCHAR     NOT NULL,
    symbol      VARCHAR     NOT NULL,
    date        DATE        NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      DOUBLE,
    ingested_at TIMESTAMPTZ DEFAULT current_timestamp,
    PRIMARY KEY (symbol, date)
);
"""

_BRONZE_MACRO_DDL = """
CREATE TABLE IF NOT EXISTS bronze_macro (
    source      VARCHAR     NOT NULL,
    symbol      VARCHAR     NOT NULL,
    date        DATE        NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      DOUBLE,
    ingested_at TIMESTAMPTZ DEFAULT current_timestamp,
    PRIMARY KEY (symbol, date)
);
"""

# ---------------------------------------------------------------------------
# Silver layer — cleaned, normalized, enriched data
# ---------------------------------------------------------------------------

_SILVER_PRICES_DDL = """
CREATE TABLE IF NOT EXISTS silver_prices (
    source          VARCHAR     NOT NULL,
    symbol          VARCHAR     NOT NULL,
    asset_class     VARCHAR     NOT NULL,
    date            DATE        NOT NULL,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE      NOT NULL,
    volume          DOUBLE,
    daily_return    DOUBLE,
    log_return      DOUBLE,
    is_outlier      BOOLEAN     DEFAULT FALSE,
    transformed_at  TIMESTAMPTZ DEFAULT current_timestamp,
    PRIMARY KEY (symbol, date)
);
"""


def _create_bronze_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(_BRONZE_EQUITIES_DDL)
    conn.execute(_BRONZE_CRYPTO_DDL)
    conn.execute(_BRONZE_MACRO_DDL)
    logger.debug("Bronze tables ready")


def _create_silver_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(_SILVER_PRICES_DDL)
    logger.debug("Silver tables ready")


BRONZE_TABLES = ["bronze_equities", "bronze_crypto", "bronze_macro"]
SILVER_TABLES = ["silver_prices"]
ALL_TABLES = BRONZE_TABLES + SILVER_TABLES
