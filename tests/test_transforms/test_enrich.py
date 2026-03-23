"""Tests for the enrich transform — returns and rolling stats."""

from datetime import date

import polars as pl
import pytest

from marketlens.transforms.enrich import compute_returns, compute_rolling_stats


def _prices(closes, symbol="SPY"):
    from datetime import timedelta

    n = len(closes)
    start = date(2024, 1, 1)
    return pl.DataFrame(
        {
            "symbol": [symbol] * n,
            "date": [start + timedelta(days=i) for i in range(n)],
            "close": [float(c) for c in closes],
        }
    )


# ---------------------------------------------------------------------------
# compute_returns
# ---------------------------------------------------------------------------


def test_compute_returns_adds_columns():
    df = _prices([100, 101, 102])
    result = compute_returns(df)
    assert "daily_return" in result.columns
    assert "log_return" in result.columns


def test_compute_returns_first_row_is_null():
    """pct_change() produces null for the first row — no prior price."""
    df = _prices([100, 110, 121])
    result = compute_returns(df)
    assert result["daily_return"][0] is None
    assert result["log_return"][0] is None


def test_compute_returns_correct_values():
    """Verify exact return values: 100→110 = 10% daily return."""
    df = _prices([100, 110])
    result = compute_returns(df)
    assert result["daily_return"][1] == pytest.approx(0.10, rel=1e-6)


def test_compute_log_return_correct_values():
    """log_return = ln(close / prev_close) = ln(1 + daily_return)."""
    import math

    df = _prices([100, 110])
    result = compute_returns(df)
    expected = math.log(110 / 100)
    assert result["log_return"][1] == pytest.approx(expected, rel=1e-6)


def test_compute_returns_independent_per_symbol():
    """Returns must be computed within each symbol — no cross-symbol bleed."""
    spy = _prices([100, 110], symbol="SPY")
    qqq = _prices([200, 220], symbol="QQQ")
    combined = pl.concat([spy, qqq])
    result = compute_returns(combined)

    spy_r = result.filter(pl.col("symbol") == "SPY")["daily_return"]
    qqq_r = result.filter(pl.col("symbol") == "QQQ")["daily_return"]

    assert spy_r[0] is None
    assert qqq_r[0] is None
    assert spy_r[1] == pytest.approx(0.10, rel=1e-6)
    assert qqq_r[1] == pytest.approx(0.10, rel=1e-6)


def test_compute_returns_preserves_row_count():
    df = _prices([100, 101, 102, 103])
    result = compute_returns(df)
    assert len(result) == 4


# ---------------------------------------------------------------------------
# compute_rolling_stats
# ---------------------------------------------------------------------------


def test_compute_rolling_stats_adds_vol_columns():
    closes = [100.0 + i * 0.5 for i in range(60)]
    df = compute_returns(_prices(closes))
    result = compute_rolling_stats(df)
    assert "rolling_vol_30d" in result.columns
    assert "rolling_vol_90d" in result.columns


def test_rolling_vol_30d_null_before_min_samples():
    """Rolling vol requires min_samples=20 — first 19 rows should be null."""
    closes = [100.0 + i * 0.3 for i in range(60)]
    df = compute_returns(_prices(closes))
    result = compute_rolling_stats(df)
    # First row always null from pct_change, plus next 19 from rolling_std
    assert result["rolling_vol_30d"][1:20].null_count() > 0


def test_rolling_vol_is_positive_when_computed():
    """Annualised vol should be positive for a non-flat series."""
    closes = [100.0 + i * 0.5 for i in range(60)]
    df = compute_returns(_prices(closes))
    result = compute_rolling_stats(df)
    non_null = result["rolling_vol_30d"].drop_nulls()
    assert (non_null > 0).all()
