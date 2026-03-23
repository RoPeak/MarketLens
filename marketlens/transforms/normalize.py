"""
Normalize raw Bronze data into a unified Silver schema.

Each source has its own quirks:
  - Equities: full OHLCV, asset_class='equity'
  - Crypto:   OHLC only (no volume from CoinGecko OHLC endpoint), asset_class='crypto'
  - Macro:    single value stored in 'close', open/high/low/volume null, asset_class='macro'

The output of every normalize_*() function is a Polars DataFrame with exactly
the Silver columns in the correct order, ready for clean.py and enrich.py.
"""

import duckdb
import polars as pl
from loguru import logger

# Canonical Silver column order (excluding derived columns added by enrich.py)
SILVER_COLS = [
    "source",
    "symbol",
    "asset_class",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

_BRONZE_SCHEMA = {
    "source": pl.Utf8,
    "symbol": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}


def load_bronze(conn: duckdb.DuckDBPyConnection, table: str) -> pl.DataFrame:
    """Read a Bronze table into a Polars DataFrame."""
    return conn.execute(f"SELECT * FROM {table}").pl()  # noqa: S608


def normalize_equities(df: pl.DataFrame) -> pl.DataFrame:
    """Add asset_class='equity' and enforce Silver schema."""
    logger.debug(f"[normalize] equities: {len(df)} rows")
    return _add_asset_class(df, "equity")


def normalize_crypto(df: pl.DataFrame) -> pl.DataFrame:
    """Add asset_class='crypto'. Volume is null from CoinGecko OHLC endpoint."""
    logger.debug(f"[normalize] crypto: {len(df)} rows")
    return _add_asset_class(df, "crypto")


def normalize_macro(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add asset_class='macro'. Macro series have a single value per date stored
    in 'close'; open/high/low/volume remain null.
    """
    logger.debug(f"[normalize] macro: {len(df)} rows")
    return _add_asset_class(df, "macro")


def combine(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Concatenate normalized frames from all three sources."""
    if not frames:
        return pl.DataFrame(schema={col: pl.Utf8 for col in SILVER_COLS})
    return pl.concat(frames, how="diagonal_relaxed")


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------


def _add_asset_class(df: pl.DataFrame, asset_class: str) -> pl.DataFrame:
    """Insert asset_class column and select only SILVER_COLS."""
    return (
        df.with_columns(pl.lit(asset_class).alias("asset_class"))
        .select(SILVER_COLS)
        .with_columns(
            [
                pl.col("date").cast(pl.Date),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
            ]
        )
    )
