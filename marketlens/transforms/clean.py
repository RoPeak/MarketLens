"""
Silver layer data cleaning.

Pipeline (applied in order):
  1. deduplicate()    — keep the last-ingested row per (symbol, date)
  2. handle_nulls()   — forward-fill close gaps up to 3 calendar days
  3. flag_outliers()  — mark rows where |z-score of close| > threshold

All functions are pure: they accept and return Polars DataFrames without
side-effects, making them trivial to test and compose.
"""

import polars as pl
from loguru import logger

_MAX_FORWARD_FILL_DAYS = 3
_DEFAULT_Z_THRESHOLD = 4.0


def deduplicate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Keep one row per (symbol, date), preferring the most recently ingested.

    The Bronze layer uses INSERT OR REPLACE, so duplicates should be rare,
    but this guard handles any edge cases from concurrent ingest runs.
    """
    before = len(df)
    df = df.unique(subset=["symbol", "date"], keep="last")
    dropped = before - len(df)
    if dropped:
        logger.debug(f"[clean] deduplicate: removed {dropped} duplicate rows")
    return df


def handle_nulls(df: pl.DataFrame, max_fill_days: int = _MAX_FORWARD_FILL_DAYS) -> pl.DataFrame:
    """
    Forward-fill null 'close' values within each symbol, up to max_fill_days.

    This handles weekends and market holidays where FRED or crypto data may
    have gaps. We sort by date within each symbol before filling to ensure
    correct ordering.

    Only 'close' is filled; open/high/low/volume remain null for filled rows
    to make it clear they are synthetic carry-forward values.
    """
    null_before = df["close"].null_count()

    df = df.sort("date").with_columns(
        pl.col("close").forward_fill(limit=max_fill_days).over("symbol").alias("close")
    )

    null_after = df["close"].null_count()
    filled = null_before - null_after
    if filled:
        logger.debug(f"[clean] handle_nulls: forward-filled {filled} close values")
    if null_after:
        logger.warning(f"[clean] handle_nulls: {null_after} null close values remain after fill")

    return df


def flag_outliers(df: pl.DataFrame, z_threshold: float = _DEFAULT_Z_THRESHOLD) -> pl.DataFrame:
    """
    Add an 'is_outlier' boolean column.

    A row is flagged when the absolute z-score of its 'close' price exceeds
    z_threshold, computed per symbol. This catches data errors (wrong decimal
    point, currency mismatches) without removing the row — downstream users
    can choose to exclude flagged rows.

    We use a robust z-score based on median and MAD (median absolute deviation)
    to avoid inflating the score when there are genuine outliers in the sample.
    """
    df = df.with_columns(pl.lit(False).alias("is_outlier"))

    df = (
        df.with_columns(
            [
                pl.col("close").median().over("symbol").alias("_median"),
                (pl.col("close") - pl.col("close").median().over("symbol"))
                .abs()
                .median()
                .over("symbol")
                .alias("_mad"),
            ]
        )
        .with_columns(
            pl.when(pl.col("_mad") > 0)
            # Normal: MAD-based robust z-score
            .then(((pl.col("close") - pl.col("_median")).abs() / pl.col("_mad")) > z_threshold)
            # Edge case: MAD = 0 (all non-spike values identical) — any deviation is an outlier
            .when(pl.col("close") != pl.col("_median"))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("is_outlier")
        )
        .drop(["_median", "_mad"])
    )

    n_outliers = df["is_outlier"].sum()
    if n_outliers:
        flagged = df.filter(pl.col("is_outlier")).select(["symbol", "date", "close"])
        logger.warning(f"[clean] flag_outliers: {n_outliers} rows flagged\n{flagged}")

    return df
