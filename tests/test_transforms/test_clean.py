"""Tests for the clean transform — dedup, null handling, outlier flagging."""

from datetime import date

import polars as pl
import pytest

from marketlens.transforms.clean import deduplicate, flag_outliers, handle_nulls


def _prices(symbol="SPY", closes=None, dates=None):
    n = len(closes) if closes is not None else 5
    closes = closes or [100.0 + i for i in range(n)]
    dates = dates or [date(2024, 1, i + 1) for i in range(n)]
    return pl.DataFrame(
        {
            "symbol": [symbol] * n,
            "date": dates,
            "close": closes,
            "asset_class": ["equity"] * n,
        }
    )


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------


def test_deduplicate_removes_exact_duplicates():
    df = pl.concat([_prices(), _prices()])  # same rows twice
    result = deduplicate(df)
    assert len(result) == 5


def test_deduplicate_keeps_different_symbols():
    spy = _prices("SPY")
    qqq = _prices("QQQ")
    combined = pl.concat([spy, qqq])
    result = deduplicate(combined)
    assert len(result) == 10


def test_deduplicate_no_duplicates_is_noop():
    df = _prices()
    result = deduplicate(df)
    assert len(result) == len(df)


# ---------------------------------------------------------------------------
# handle_nulls
# ---------------------------------------------------------------------------


def test_handle_nulls_forward_fills_close():
    closes = [100.0, None, None, 103.0, 104.0]
    df = _prices(closes=closes)
    result = handle_nulls(df, max_fill_days=3)
    assert result["close"].null_count() == 0
    # First null should be filled with 100.0 (forward fill)
    assert result["close"][1] == pytest.approx(100.0)
    assert result["close"][2] == pytest.approx(100.0)


def test_handle_nulls_respects_max_fill_days():
    """Gaps longer than max_fill_days should still be null."""
    closes = [100.0, None, None, None, None, 105.0]
    df = _prices(closes=closes)
    result = handle_nulls(df, max_fill_days=2)
    # Positions 1 and 2 should be filled, positions 3 and 4 should remain null
    assert result["close"][1] == pytest.approx(100.0)
    assert result["close"][2] == pytest.approx(100.0)
    assert result["close"][3] is None
    assert result["close"][4] is None


def test_handle_nulls_does_not_fill_across_symbols():
    """Forward fill must not bleed across different symbols."""
    spy = _prices("SPY", closes=[100.0, None, 102.0])
    qqq = _prices("QQQ", closes=[None, 200.0, 201.0])
    combined = pl.concat([spy, qqq])
    result = handle_nulls(combined)

    spy_result = result.filter(pl.col("symbol") == "SPY")
    qqq_result = result.filter(pl.col("symbol") == "QQQ")

    # SPY null should be filled from 100.0
    assert spy_result["close"][1] == pytest.approx(100.0)
    # QQQ first row was null with no prior value — should stay null
    assert qqq_result["close"][0] is None


# ---------------------------------------------------------------------------
# flag_outliers
# ---------------------------------------------------------------------------


def test_flag_outliers_adds_is_outlier_column():
    df = _prices()
    result = flag_outliers(df)
    assert "is_outlier" in result.columns


def test_flag_outliers_normal_data_not_flagged():
    """Normal price series should produce no outliers."""
    closes = [100.0 + i * 0.5 for i in range(30)]
    df = _prices(closes=closes)
    result = flag_outliers(df)
    assert result["is_outlier"].sum() == 0


def test_flag_outliers_detects_spike():
    """A single extreme price spike should be flagged."""
    closes = [100.0] * 29 + [100_000.0]  # last value is extreme
    df = _prices(closes=closes)
    result = flag_outliers(df)
    assert result["is_outlier"].sum() >= 1
    assert result["is_outlier"][-1]


def test_flag_outliers_does_not_remove_rows():
    """Flagging is non-destructive — row count must be unchanged."""
    df = _prices(closes=[100.0] * 4 + [999_999.0])
    result = flag_outliers(df)
    assert len(result) == len(df)
