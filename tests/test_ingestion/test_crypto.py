"""Tests for CryptoIngester — unit tests mock HTTP, network tests hit CoinGecko."""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from marketlens.config import Settings
from marketlens.ingestion.crypto import CryptoIngester, _coin_id_to_symbol


# CoinGecko OHLC response format: [[timestamp_ms, open, high, low, close], ...]
def _make_ohlc_response(n=5, base_price=40000.0):
    base_ts = 1704153600000  # 2024-01-02 00:00:00 UTC
    day_ms = 86_400_000
    return [
        [
            base_ts + i * day_ms,
            base_price + i,
            base_price + i + 500,
            base_price + i - 500,
            base_price + i + 100,
        ]
        for i in range(n)
    ]


@pytest.fixture
def settings():
    s = Settings()
    s.crypto_ids = ["bitcoin", "ethereum"]
    return s


def test_coin_id_to_symbol_known():
    assert _coin_id_to_symbol("bitcoin") == "BTC"
    assert _coin_id_to_symbol("ethereum") == "ETH"
    assert _coin_id_to_symbol("solana") == "SOL"


def test_coin_id_to_symbol_unknown_uppercases():
    assert _coin_id_to_symbol("dogecoin") == "DOGECO"


def test_fetch_returns_polars_dataframe(db_conn, settings):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = _make_ohlc_response()

    with patch("marketlens.ingestion.crypto.requests.get", return_value=mock_resp):
        ingester = CryptoIngester(db_conn, settings)
        result = ingester.fetch(date(2024, 1, 2), date(2024, 1, 6))

    assert isinstance(result, pl.DataFrame)
    assert not result.is_empty()


def test_fetch_has_required_columns(db_conn, settings):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = _make_ohlc_response()

    with patch("marketlens.ingestion.crypto.requests.get", return_value=mock_resp):
        ingester = CryptoIngester(db_conn, settings)
        result = ingester.fetch(date(2024, 1, 2), date(2024, 1, 6))

    for col in ("source", "symbol", "date", "open", "high", "low", "close"):
        assert col in result.columns


def test_fetch_handles_http_error(db_conn, settings):
    """Non-200 response from CoinGecko should return empty DataFrame (not raise)."""
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"

    with patch("marketlens.ingestion.crypto.requests.get", return_value=mock_resp):
        ingester = CryptoIngester(db_conn, settings)
        result = ingester.fetch(date(2024, 1, 2), date(2024, 1, 6))

    assert result.is_empty()


def test_fetch_deduplicates_chunk_boundary_overlap(db_conn):
    """Rows returned at chunk boundaries should be deduplicated."""
    settings = Settings()
    settings.crypto_ids = ["bitcoin"]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    # Return the same 5 rows twice (simulates chunk overlap)
    mock_resp.json.return_value = _make_ohlc_response(n=5)

    with patch("marketlens.ingestion.crypto.requests.get", return_value=mock_resp):
        ingester = CryptoIngester(db_conn, settings)
        result = ingester.fetch(date(2024, 1, 2), date(2024, 1, 6))

    assert result["date"].n_unique() == result.height, "Duplicate dates found after dedup"


def test_ingest_writes_to_bronze_crypto(db_conn, settings):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = _make_ohlc_response()

    with patch("marketlens.ingestion.crypto.requests.get", return_value=mock_resp):
        ingester = CryptoIngester(db_conn, settings)
        count = ingester.ingest(date(2024, 1, 2), date(2024, 1, 6))

    assert count > 0
    rows = db_conn.execute("SELECT COUNT(*) FROM bronze_crypto").fetchone()[0]
    assert rows == count


@pytest.mark.network
def test_live_fetch_bitcoin(db_conn):
    """Integration test: fetch 7 days of BTC from CoinGecko."""
    s = Settings()
    s.crypto_ids = ["bitcoin"]
    ingester = CryptoIngester(db_conn, s)

    end = date.today()
    start = end - timedelta(days=7)
    result = ingester.fetch(start, end)

    assert not result.is_empty(), "Expected real Bitcoin OHLC data"
    assert result["close"].min() > 0
