"""Tests for BaseIngester shared logic — upsert, validation, idempotency."""

from datetime import date

import polars as pl
import pytest

from marketlens.config import Settings
from marketlens.ingestion.base import BaseIngester

# ---------------------------------------------------------------------------
# Minimal concrete ingester for testing the base class in isolation
# ---------------------------------------------------------------------------


class _StubIngester(BaseIngester):
    """Ingester that returns a fixed DataFrame — no network calls."""

    target_table = "bronze_equities"

    def __init__(self, conn, settings, rows):
        super().__init__(conn, settings)
        self._rows = rows

    def fetch(self, start: date, end: date) -> pl.DataFrame:
        if not self._rows:
            return pl.DataFrame()
        return pl.DataFrame(self._rows).with_columns(
            [
                pl.col("date").cast(pl.Date),
                pl.col("close").cast(pl.Float64),
            ]
        )


def _make_rows(symbol="SPY", n=3):
    return [
        {
            "source": "test",
            "symbol": symbol,
            "date": date(2024, 1, i + 1),
            "open": 100.0 + i,
            "high": 101.0 + i,
            "low": 99.0 + i,
            "close": 100.5 + i,
            "volume": 1_000_000.0,
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_ingest_returns_row_count(db_conn):
    ingester = _StubIngester(db_conn, Settings(), _make_rows())
    count = ingester.ingest(date(2024, 1, 1), date(2024, 1, 3))
    assert count == 3


def test_ingest_writes_to_table(db_conn):
    ingester = _StubIngester(db_conn, Settings(), _make_rows())
    ingester.ingest(date(2024, 1, 1), date(2024, 1, 3))

    result = db_conn.execute("SELECT COUNT(*) FROM bronze_equities").fetchone()
    assert result[0] == 3


def test_ingest_is_idempotent(db_conn):
    """Running ingest twice for the same data should not duplicate rows."""
    rows = _make_rows()
    ingester = _StubIngester(db_conn, Settings(), rows)
    ingester.ingest(date(2024, 1, 1), date(2024, 1, 3))
    ingester.ingest(date(2024, 1, 1), date(2024, 1, 3))

    result = db_conn.execute("SELECT COUNT(*) FROM bronze_equities").fetchone()
    assert result[0] == 3  # not 6


def test_ingest_upserts_updated_close(db_conn):
    """Second ingest with updated close value should overwrite the first."""
    rows_v1 = _make_rows()
    rows_v2 = [{**r, "close": r["close"] + 10} for r in rows_v1]

    _StubIngester(db_conn, Settings(), rows_v1).ingest(date(2024, 1, 1), date(2024, 1, 3))
    _StubIngester(db_conn, Settings(), rows_v2).ingest(date(2024, 1, 1), date(2024, 1, 3))

    closes = db_conn.execute(
        "SELECT close FROM bronze_equities WHERE symbol = 'SPY' ORDER BY date"
    ).fetchall()
    assert all(c[0] > 105 for c in closes), "Expected updated close values after upsert"


def test_ingest_empty_fetch_returns_zero(db_conn):
    ingester = _StubIngester(db_conn, Settings(), [])
    count = ingester.ingest(date(2024, 1, 1), date(2024, 1, 3))
    assert count == 0


def test_validate_schema_raises_on_missing_close(db_conn):
    """Ingesting a DataFrame without 'close' should raise ValueError."""

    class _NoCloseIngester(_StubIngester):
        def fetch(self, start, end):
            return pl.DataFrame({"source": ["x"], "symbol": ["X"], "date": [date(2024, 1, 1)]})

    ingester = _NoCloseIngester(db_conn, Settings(), [])
    with pytest.raises(ValueError, match="missing required columns"):
        ingester.ingest(date(2024, 1, 1), date(2024, 1, 1))


def test_ingested_at_is_stamped(db_conn):
    """Rows written to DuckDB should have a non-null ingested_at timestamp."""
    _StubIngester(db_conn, Settings(), _make_rows()).ingest(date(2024, 1, 1), date(2024, 1, 3))

    result = db_conn.execute("SELECT ingested_at FROM bronze_equities LIMIT 1").fetchone()
    assert result[0] is not None
