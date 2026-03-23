"""
Silver layer enrichment — computed columns derived from cleaned price data.

Why Polars window functions over pandas groupby:
  - .over("symbol") applies the expression independently within each symbol
    group without a separate groupby/apply round-trip
  - Sorting is explicit: .sort("date").over("symbol") ensures correct temporal
    order regardless of the order rows arrive in the combined Silver frame
  - The lazy API can push these expressions into a single pass over the data

All functions are pure (DataFrame in → DataFrame out).
"""

import math

import polars as pl
from loguru import logger


def compute_returns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add daily_return and log_return columns.

    daily_return = (close_t - close_{t-1}) / close_{t-1}   (percentage change)
    log_return   = ln(close_t / close_{t-1})                (continuously compounded)

    We sort by date within each symbol group before computing pct_change to
    guarantee correct ordering — this is the canonical Polars window pattern.
    """
    df = (
        df.sort("date")
        .with_columns(
            [
                pl.col("close").pct_change().over("symbol").alias("daily_return"),
            ]
        )
        .with_columns(
            [
                # log_return = ln(1 + daily_return) — numerically stable for small returns
                (pl.col("daily_return") + 1.0).log(math.e).alias("log_return"),
            ]
        )
    )

    computed = df["daily_return"].drop_nulls().len()
    logger.debug(f"[enrich] compute_returns: {computed} non-null return values")
    return df


def compute_rolling_stats(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add rolling volatility columns used by the dbt Gold models.

    rolling_vol_30d  — annualised 30-day rolling std of log_return  (√252 scaling)
    rolling_vol_90d  — annualised 90-day rolling std of log_return

    These are pre-computed in Silver so dbt Gold models can reference them
    directly without repeating the window logic in SQL.
    """
    ann = math.sqrt(252)

    df = df.sort("date").with_columns(
        [
            (
                pl.col("log_return").rolling_std(window_size=30, min_samples=20).over("symbol")
                * ann
            ).alias("rolling_vol_30d"),
            (
                pl.col("log_return").rolling_std(window_size=90, min_samples=60).over("symbol")
                * ann
            ).alias("rolling_vol_90d"),
        ]
    )

    logger.debug("[enrich] compute_rolling_stats: 30d and 90d vol columns added")
    return df
