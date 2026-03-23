"""Tests for EquitiesIngester — unit tests use fixture data, network tests hit Yahoo Finance."""

from datetime import date, timedelta
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest

from marketlens.config import Settings
from marketlens.ingestion.equities import EquitiesIngester


def _make_yf_response(tickers=("SPY", "QQQ"), days=3):
    """
    Build a MultiIndex DataFrame that matches actual yfinance output structure.

    yf.download() with multiple tickers returns:
      - Index: DatetimeIndex named "Date"
      - Columns: MultiIndex (level 0 = Price metric, level 1 = Ticker)

    We assign the MultiIndex directly after construction so that level names
    are preserved (passing names= to from_arrays is lost when dict keys are tuples).
    """
    dates = pd.date_range("2024-01-02", periods=days, freq="B", name="Date")
    metrics = ["Close", "High", "Low", "Open", "Volume"]

    tuples = [(m, t) for m in metrics for t in tickers]
    multi_idx = pd.MultiIndex.from_tuples(tuples, names=["Price", "Ticker"])

    data = {t: [100.0 + i for i in range(days)] for t in tuples}
    df = pd.DataFrame(data, index=dates)
    df.columns = multi_idx
    return df


@pytest.fixture
def settings():
    s = Settings()
    s.yfinance_tickers = ["SPY", "QQQ"]
    return s


def test_fetch_returns_polars_dataframe(db_conn, settings):
    mock_df = _make_yf_response()
    with patch("marketlens.ingestion.equities.yf.download", return_value=mock_df):
        ingester = EquitiesIngester(db_conn, settings)
        result = ingester.fetch(date(2024, 1, 2), date(2024, 1, 4))

    assert isinstance(result, pl.DataFrame)
    assert not result.is_empty()


def test_fetch_has_required_columns(db_conn, settings):
    mock_df = _make_yf_response()
    with patch("marketlens.ingestion.equities.yf.download", return_value=mock_df):
        ingester = EquitiesIngester(db_conn, settings)
        result = ingester.fetch(date(2024, 1, 2), date(2024, 1, 4))

    for col in ("source", "symbol", "date", "close"):
        assert col in result.columns, f"Missing column: {col}"


def test_fetch_returns_correct_symbols(db_conn, settings):
    mock_df = _make_yf_response(tickers=("SPY", "QQQ"))
    with patch("marketlens.ingestion.equities.yf.download", return_value=mock_df):
        ingester = EquitiesIngester(db_conn, settings)
        result = ingester.fetch(date(2024, 1, 2), date(2024, 1, 4))

    symbols = set(result["symbol"].to_list())
    assert "SPY" in symbols
    assert "QQQ" in symbols


def test_fetch_empty_yfinance_response(db_conn, settings):
    empty_df = pd.DataFrame()
    with patch("marketlens.ingestion.equities.yf.download", return_value=empty_df):
        ingester = EquitiesIngester(db_conn, settings)
        result = ingester.fetch(date(2024, 1, 2), date(2024, 1, 4))

    assert result.is_empty()


def test_ingest_writes_to_bronze_equities(db_conn, settings):
    mock_df = _make_yf_response(days=3)
    with patch("marketlens.ingestion.equities.yf.download", return_value=mock_df):
        ingester = EquitiesIngester(db_conn, settings)
        count = ingester.ingest(date(2024, 1, 2), date(2024, 1, 4))

    assert count > 0
    rows = db_conn.execute("SELECT COUNT(*) FROM bronze_equities").fetchone()[0]
    assert rows == count


@pytest.mark.network
def test_live_fetch_spy(db_conn):
    """Integration test: fetch 5 days of SPY from Yahoo Finance."""
    s = Settings()
    s.yfinance_tickers = ["SPY"]
    ingester = EquitiesIngester(db_conn, s)

    end = date.today()
    start = end - timedelta(days=7)
    result = ingester.fetch(start, end)

    assert not result.is_empty(), "Expected real SPY data"
    assert "close" in result.columns
    assert result["close"].min() > 0
