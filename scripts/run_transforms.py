"""
Run the full Silver transformation pipeline.

Reads from all three Bronze tables, normalises, cleans, and enriches the data,
then writes the result to silver_prices (INSERT OR REPLACE on PK).

Usage:
    python scripts/run_transforms.py
"""

from datetime import UTC, datetime

import duckdb
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


def run_silver_pipeline(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Execute the full Bronze → Silver transformation. Returns rows written.
    """
    logger.info("Silver pipeline: starting")

    # 1. Load & normalise each Bronze source
    frames = []
    for table, normalize_fn in [
        ("bronze_equities", normalize_equities),
        ("bronze_crypto", normalize_crypto),
        ("bronze_macro", normalize_macro),
    ]:
        raw = load_bronze(conn, table)
        if raw.is_empty():
            logger.warning(f"Silver pipeline: {table} is empty, skipping")
            continue
        frames.append(normalize_fn(raw))

    if not frames:
        logger.warning("Silver pipeline: all Bronze tables empty — nothing to transform")
        return 0

    # 2. Combine sources into one frame
    combined = combine(frames)
    logger.info(f"Silver pipeline: combined {len(combined)} rows from {len(frames)} sources")

    # 3. Clean
    combined = deduplicate(combined)
    combined = handle_nulls(combined)
    combined = flag_outliers(combined)

    # 4. Enrich
    combined = compute_returns(combined)
    combined = compute_rolling_stats(combined)

    # 5. Stamp transformed_at and write to silver_prices
    now = datetime.now(tz=UTC)
    combined = combined.with_columns(pl.lit(now).alias("transformed_at"))

    _upsert_silver(conn, combined)
    logger.success(f"Silver pipeline: wrote {len(combined)} rows to silver_prices")
    return len(combined)


def _upsert_silver(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> None:
    """INSERT OR REPLACE into silver_prices from a Polars DataFrame."""
    # Select only the columns that exist in the Silver schema
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
    available = [c for c in silver_cols if c in df.columns]
    df_out = df.select(available)

    conn.register("_silver_batch", df_out)
    conn.execute(f"""
        INSERT OR REPLACE INTO silver_prices
        SELECT {", ".join(available)}
        FROM _silver_batch
    """)  # noqa: S608
    conn.unregister("_silver_batch")


def main() -> None:
    conn = get_connection(settings.db_path)
    bootstrap_schema(conn)
    n = run_silver_pipeline(conn)
    conn.close()
    print(f"Silver pipeline complete. {n} rows in silver_prices.")


if __name__ == "__main__":
    main()
