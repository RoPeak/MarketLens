"""Tests for the normalize transform — schema unification across sources."""

from datetime import date

import polars as pl

from marketlens.transforms.normalize import (
    SILVER_COLS,
    combine,
    normalize_crypto,
    normalize_equities,
    normalize_macro,
)


def _equity_rows(n=3, symbol="SPY"):
    return pl.DataFrame(
        {
            "source": ["yahoo_finance"] * n,
            "symbol": [symbol] * n,
            "date": [date(2024, 1, i + 1) for i in range(n)],
            "open": [99.0 + i for i in range(n)],
            "high": [101.0 + i for i in range(n)],
            "low": [98.0 + i for i in range(n)],
            "close": [100.0 + i for i in range(n)],
            "volume": [1_000_000.0] * n,
            "ingested_at": [None] * n,
        }
    )


def _crypto_rows(n=3, symbol="BTC"):
    return pl.DataFrame(
        {
            "source": ["coingecko"] * n,
            "symbol": [symbol] * n,
            "date": [date(2024, 1, i + 1) for i in range(n)],
            "open": [40000.0 + i * 100 for i in range(n)],
            "high": [41000.0 + i * 100 for i in range(n)],
            "low": [39000.0 + i * 100 for i in range(n)],
            "close": [40500.0 + i * 100 for i in range(n)],
            "volume": [None] * n,
            "ingested_at": [None] * n,
        }
    )


def _macro_rows(n=3, symbol="DGS10"):
    return pl.DataFrame(
        {
            "source": ["fred"] * n,
            "symbol": [symbol] * n,
            "date": [date(2024, 1, i + 1) for i in range(n)],
            "open": [None] * n,
            "high": [None] * n,
            "low": [None] * n,
            "close": [4.2 + i * 0.01 for i in range(n)],
            "volume": [None] * n,
            "ingested_at": [None] * n,
        }
    )


# ---------------------------------------------------------------------------


def test_normalize_equities_has_silver_cols():
    result = normalize_equities(_equity_rows())
    for col in SILVER_COLS:
        assert col in result.columns


def test_normalize_equities_asset_class():
    result = normalize_equities(_equity_rows())
    assert all(v == "equity" for v in result["asset_class"].to_list())


def test_normalize_crypto_asset_class():
    result = normalize_crypto(_crypto_rows())
    assert all(v == "crypto" for v in result["asset_class"].to_list())


def test_normalize_macro_asset_class():
    result = normalize_macro(_macro_rows())
    assert all(v == "macro" for v in result["asset_class"].to_list())


def test_normalize_drops_ingested_at():
    """ingested_at is a Bronze-only column and should not appear in Silver."""
    result = normalize_equities(_equity_rows())
    assert "ingested_at" not in result.columns


def test_normalize_preserves_row_count():
    rows = _equity_rows(5)
    result = normalize_equities(rows)
    assert len(result) == 5


def test_combine_concatenates_all_sources():
    eq = normalize_equities(_equity_rows(3))
    cr = normalize_crypto(_crypto_rows(3))
    ma = normalize_macro(_macro_rows(3))

    combined = combine([eq, cr, ma])
    assert len(combined) == 9
    assert set(combined["asset_class"].to_list()) == {"equity", "crypto", "macro"}


def test_combine_empty_list():
    result = combine([])
    assert result.is_empty()


def test_normalize_date_column_is_date_type():
    result = normalize_equities(_equity_rows())
    assert result["date"].dtype == pl.Date


def test_normalize_close_is_float64():
    result = normalize_macro(_macro_rows())
    assert result["close"].dtype == pl.Float64
