"""
Centralised data access layer for the Streamlit dashboard.

All functions:
  - Return pandas DataFrames (Streamlit cache cannot serialise Polars without
    a custom hash function)
  - Are decorated with @st.cache_data so repeated UI interactions don't hit DuckDB
  - Use read_only=True connections to avoid accidental writes from the dashboard
  - Query the main_main schema where dbt materialised the Gold tables

The TTL (time-to-live) of 300 seconds means data refreshes every 5 minutes
while the dashboard is running.
"""

from datetime import date

import duckdb
import pandas as pd
import streamlit as st

from marketlens.config import settings

# dbt materialises into main_main schema when dbt_project.yml has +schema: main
_SCHEMA = "main_main"


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(settings.db_path), read_only=True)


# ---------------------------------------------------------------------------
# Price Explorer queries
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def get_daily_returns(
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    """Long-format daily returns for selected symbols between start and end."""
    filters = []
    params: list = []

    if symbols:
        placeholders = ", ".join(["?"] * len(symbols))
        filters.append(f"symbol IN ({placeholders})")
        params.extend(symbols)
    if start:
        filters.append("date >= ?")
        params.append(start)
    if end:
        filters.append("date <= ?")
        params.append(end)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    with _conn() as conn:
        return conn.execute(
            f"SELECT * FROM {_SCHEMA}.mart_daily_returns {where} ORDER BY date, symbol",  # noqa: S608
            params or None,
        ).df()


@st.cache_data(ttl=300)
def get_available_symbols() -> dict[str, list[str]]:
    """Return symbols grouped by asset class."""
    with _conn() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT symbol, asset_class FROM {_SCHEMA}.mart_daily_returns"  # noqa: S608
            " ORDER BY asset_class, symbol"
        ).fetchall()
    result: dict[str, list[str]] = {}
    for symbol, asset_class in rows:
        result.setdefault(asset_class, []).append(symbol)
    return result


@st.cache_data(ttl=300)
def get_ohlcv(symbol: str, start: date | None = None, end: date | None = None) -> pd.DataFrame:
    """OHLCV data for a single symbol from the Silver layer (for candlestick)."""
    filters = ["symbol = ?", "asset_class = 'equity'"]
    params: list = [symbol]
    if start:
        filters.append("date >= ?")
        params.append(start)
    if end:
        filters.append("date <= ?")
        params.append(end)

    where = f"WHERE {' AND '.join(filters)}"
    with _conn() as conn:
        return conn.execute(
            f"SELECT date, open, high, low, close, volume FROM main.silver_prices {where}"  # noqa: S608
            " ORDER BY date",
            params,
        ).df()


# ---------------------------------------------------------------------------
# Volatility queries
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def get_volatility(
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    """Rolling vol and Garman-Klass vol for selected symbols."""
    filters = []
    params: list = []

    if symbols:
        placeholders = ", ".join(["?"] * len(symbols))
        filters.append(f"symbol IN ({placeholders})")
        params.extend(symbols)
    if start:
        filters.append("date >= ?")
        params.append(start)
    if end:
        filters.append("date <= ?")
        params.append(end)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    with _conn() as conn:
        return conn.execute(
            f"SELECT * FROM {_SCHEMA}.mart_volatility {where} ORDER BY date, symbol",  # noqa: S608
            params or None,
        ).df()


# ---------------------------------------------------------------------------
# Correlation queries
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def get_correlations(
    as_of: date | None = None,
    window: str = "90d",
) -> pd.DataFrame:
    """
    Correlation matrix as of a specific date.

    Returns a long-format DataFrame with symbol_a, symbol_b, correlation columns,
    filtered to the single most recent date <= as_of.
    """
    col = "rolling_corr_90d" if window == "90d" else "rolling_corr_30d"
    date_filter = "date <= ?" if as_of else "1=1"
    params = [as_of] if as_of else []

    query = f"""
        WITH latest AS (
            SELECT MAX(date) AS max_date
            FROM {_SCHEMA}.mart_correlations
            WHERE {date_filter}
        )
        SELECT c.symbol_a, c.symbol_b, c.{col} AS correlation
        FROM {_SCHEMA}.mart_correlations c
        JOIN latest ON c.date = latest.max_date
        WHERE c.{col} IS NOT NULL
          AND NOT isnan(c.{col})
        ORDER BY c.symbol_a, c.symbol_b
    """  # noqa: S608
    with _conn() as conn:
        return conn.execute(query, params or None).df()


@st.cache_data(ttl=300)
def get_correlation_timeseries(symbol_a: str, symbol_b: str) -> pd.DataFrame:
    """90-day rolling correlation time series between two specific symbols."""
    with _conn() as conn:
        return conn.execute(
            f"""
            SELECT date, rolling_corr_90d, rolling_corr_30d
            FROM {_SCHEMA}.mart_correlations
            WHERE symbol_a = ? AND symbol_b = ?
            ORDER BY date
            """,  # noqa: S608
            [symbol_a, symbol_b],
        ).df()


# ---------------------------------------------------------------------------
# Technical indicator queries
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def get_technical_indicators(symbol: str, start: date | None = None) -> pd.DataFrame:
    """RSI, MACD, and Bollinger Bands for a single symbol."""
    filters = ["symbol = ?"]
    params: list = [symbol]
    if start:
        filters.append("date >= ?")
        params.append(start)
    where = f"WHERE {' AND '.join(filters)}"
    with _conn() as conn:
        return conn.execute(
            f"SELECT * FROM {_SCHEMA}.mart_technical_indicators {where} ORDER BY date",  # noqa: S608
            params,
        ).df()


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60)
def get_date_range() -> tuple[date, date]:
    """Return the min and max dates available across all mart data."""
    with _conn() as conn:
        row = conn.execute(
            f"SELECT MIN(date), MAX(date) FROM {_SCHEMA}.mart_daily_returns"  # noqa: S608
        ).fetchone()
    return row[0], row[1]


def tables_exist() -> bool:
    """Return True if the Gold tables have been materialised by dbt."""
    try:
        with _conn() as conn:
            conn.execute(f"SELECT 1 FROM {_SCHEMA}.mart_daily_returns LIMIT 1")  # noqa: S608
        return True
    except Exception:
        return False
