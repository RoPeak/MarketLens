"""
Seed the DuckDB database with 2 years of synthetic sample data.

This script populates all three Bronze tables and the Silver table so that
the dbt Gold models and Streamlit dashboard work without running a live ingest.
Useful for demos, CI, and Docker first-run.

Usage:
    python scripts/seed_sample_data.py
    python scripts/seed_sample_data.py --days 365

After seeding, run:
    make dbt-run      # materialise Gold tables from the seeded Silver data
    make dashboard    # launch the dashboard
"""

from __future__ import annotations

import argparse
import math
import random
from datetime import UTC, date, datetime, timedelta

import polars as pl
from loguru import logger

from marketlens.config import settings
from marketlens.db import bootstrap_schema, get_connection
from marketlens.transforms.clean import deduplicate, flag_outliers, handle_nulls
from marketlens.transforms.enrich import compute_returns, compute_rolling_stats
from marketlens.transforms.normalize import (
    combine,
    load_bronze,
    normalize_crypto,
    normalize_equities,
    normalize_macro,
)

# ---------------------------------------------------------------------------
# Synthetic price generators
# ---------------------------------------------------------------------------

# Realistic starting prices and annualised vol assumptions
_EQUITY_SEEDS: dict[str, tuple[float, float]] = {
    "SPY": (400.0, 0.16),
    "QQQ": (320.0, 0.22),
    "GLD": (180.0, 0.14),
    "TLT": (100.0, 0.18),
    "IWM": (190.0, 0.20),
}

_CRYPTO_SEEDS: dict[str, tuple[float, float]] = {
    "bitcoin": (30_000.0, 0.70),
    "ethereum": (1_800.0, 0.80),
    "solana": (25.0, 1.10),
}

_MACRO_SEEDS: dict[str, float] = {
    "DGS10": 3.5,  # 10-Year Treasury yield (%)
    "FEDFUNDS": 4.5,  # Federal funds rate (%)
    "UNRATE": 3.8,  # Unemployment rate (%)
}


def _trading_days(start: date, end: date) -> list[date]:
    """Return weekdays between start and end (inclusive)."""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon–Fri
            days.append(current)
        current += timedelta(days=1)
    return days


def _gbm_ohlcv(
    symbol: str,
    start_price: float,
    annual_vol: float,
    dates: list[date],
    rng: random.Random,
) -> pl.DataFrame:
    """
    Generate synthetic OHLCV data via Geometric Brownian Motion.

    Daily drift is 0 (pure random walk). High/low are drawn from the intraday
    range implied by vol. Volume is log-normal noise around a base level.
    """
    dt = 1 / 252
    daily_vol = annual_vol * math.sqrt(dt)
    daily_drift = -0.5 * annual_vol**2 * dt  # Ito correction for log-normal

    closes = [start_price]
    for _ in range(len(dates) - 1):
        z = rng.gauss(0, 1)
        log_ret = daily_drift + daily_vol * z
        closes.append(closes[-1] * math.exp(log_ret))

    rows = []
    for d, close in zip(dates, closes, strict=True):
        intraday_range = close * daily_vol * rng.uniform(0.5, 1.5)
        high = close + intraday_range * rng.uniform(0.3, 1.0)
        low = close - intraday_range * rng.uniform(0.3, 1.0)
        open_ = low + (high - low) * rng.uniform(0.1, 0.9)
        volume = max(0, rng.lognormvariate(15, 1))  # log-normal around ~3.3M
        rows.append(
            {
                "symbol": symbol,
                "date": d,
                "open": round(open_, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(close, 4),
                "volume": round(volume),
            }
        )

    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


def _macro_series(
    symbol: str,
    start_value: float,
    dates: list[date],
    rng: random.Random,
) -> pl.DataFrame:
    """Generate a slowly mean-reverting macro series (e.g. interest rate)."""
    mean_reversion = 0.02
    daily_vol = 0.03
    values = [start_value]
    for _ in range(len(dates) - 1):
        drift = mean_reversion * (start_value - values[-1])
        shock = rng.gauss(0, daily_vol)
        values.append(max(0.01, values[-1] + drift + shock))

    return pl.DataFrame(
        {
            "symbol": symbol,
            "date": dates,
            "close": [round(v, 4) for v in values],
            "open": None,
            "high": None,
            "low": None,
            "volume": None,
        }
    ).with_columns(pl.col("date").cast(pl.Date))


# ---------------------------------------------------------------------------
# Upsert helpers — mirror the production ingest pattern
# ---------------------------------------------------------------------------

_BRONZE_COLS = ["source", "symbol", "date", "open", "high", "low", "close", "volume", "ingested_at"]


def _upsert_bronze(conn, table: str, df: pl.DataFrame, source: str) -> int:
    now = datetime.now(tz=UTC)
    df = df.with_columns(
        [
            pl.lit(source).alias("source"),
            pl.lit(now).alias("ingested_at"),
        ]
    ).select(_BRONZE_COLS)
    conn.register("_seed_batch", df)
    cols = ", ".join(_BRONZE_COLS)
    conn.execute(  # noqa: S608
        f"INSERT OR REPLACE INTO {table} ({cols}) SELECT {cols} FROM _seed_batch"
    )
    conn.unregister("_seed_batch")
    return len(df)


def _upsert_silver(conn, df: pl.DataFrame) -> int:
    silver_cols = [
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
    ]
    now = datetime.now(tz=UTC)
    df = df.with_columns(pl.lit(now).alias("transformed_at"))
    available = [c for c in silver_cols if c in df.columns]
    df_out = df.select(available)
    conn.register("_seed_silver", df_out)
    cols = ", ".join(available)
    conn.execute(f"INSERT OR REPLACE INTO silver_prices SELECT {cols} FROM _seed_silver")  # noqa: S608
    conn.unregister("_seed_silver")
    return len(df_out)


# ---------------------------------------------------------------------------
# Main seeding routine
# ---------------------------------------------------------------------------


def seed(lookback_days: int = 730) -> None:
    rng = random.Random(42)  # deterministic seed for reproducibility
    end = date.today()
    start = end - timedelta(days=lookback_days)

    conn = get_connection(settings.db_path)
    bootstrap_schema(conn)
    logger.info(f"Seeding {lookback_days} days of data: {start} → {end}")

    equity_dates = _trading_days(start, end)
    # Crypto trades 7 days a week
    crypto_dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    # Macro is roughly weekly
    macro_dates = equity_dates[::5]  # every 5th trading day

    # --- Bronze: equities ---
    eq_frames = []
    for symbol, (price, vol) in _EQUITY_SEEDS.items():
        df = _gbm_ohlcv(symbol, price, vol, equity_dates, rng)
        eq_frames.append(df)
        n = _upsert_bronze(conn, "bronze_equities", df, "yfinance")
        logger.info(f"  bronze_equities: {symbol} → {n} rows")

    # --- Bronze: crypto ---
    cr_frames = []
    for symbol, (price, vol) in _CRYPTO_SEEDS.items():
        df = _gbm_ohlcv(symbol, price, vol, crypto_dates, rng)
        cr_frames.append(df)
        n = _upsert_bronze(conn, "bronze_crypto", df, "coingecko")
        logger.info(f"  bronze_crypto: {symbol} → {n} rows")

    # --- Bronze: macro ---
    ma_frames = []
    for symbol, start_val in _MACRO_SEEDS.items():
        df = _macro_series(symbol, start_val, macro_dates, rng)
        ma_frames.append(df)
        n = _upsert_bronze(conn, "bronze_macro", df, "fred")
        logger.info(f"  bronze_macro: {symbol} → {n} rows")

    # --- Silver: full transform pipeline ---
    frames = []
    for table, fn in [
        ("bronze_equities", normalize_equities),
        ("bronze_crypto", normalize_crypto),
        ("bronze_macro", normalize_macro),
    ]:
        raw = load_bronze(conn, table)
        if not raw.is_empty():
            frames.append(fn(raw))

    if frames:
        combined = combine(frames)
        combined = deduplicate(combined)
        combined = handle_nulls(combined)
        combined = flag_outliers(combined)
        combined = compute_returns(combined)
        combined = compute_rolling_stats(combined)
        n_silver = _upsert_silver(conn, combined)
        logger.success(f"silver_prices: {n_silver} rows written")
    else:
        logger.warning("No bronze data — silver skipped")

    conn.close()
    logger.success(
        "Seed complete. Run 'make dbt-run' to materialise Gold tables, "
        "then 'make dashboard' to launch the dashboard."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed MarketLens with synthetic sample data")
    parser.add_argument("--days", type=int, default=730, help="Days of history to generate")
    args = parser.parse_args()
    seed(args.days)


if __name__ == "__main__":
    main()
