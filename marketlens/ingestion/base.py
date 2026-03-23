"""Abstract base class for all data ingesters."""

from abc import ABC, abstractmethod
from datetime import UTC, date, datetime

import duckdb
import polars as pl
from loguru import logger

from marketlens.config import Settings


class BaseIngester(ABC):
    """
    Contract for all data ingesters.

    Subclasses implement `fetch()` to retrieve raw data from a source and return
    a normalised Polars DataFrame. The base class handles validation, upsert into
    the appropriate Bronze DuckDB table, and row-count reporting.

    Upserts use INSERT OR REPLACE semantics backed by PRIMARY KEY (symbol, date),
    making every ingestion run idempotent — re-running for an overlapping date
    range will update existing rows rather than duplicate them.
    """

    #: DuckDB table this ingester writes to (set by subclass)
    target_table: str = ""

    def __init__(self, conn: duckdb.DuckDBPyConnection, settings: Settings) -> None:
        self.conn = conn
        self.settings = settings

    @abstractmethod
    def fetch(self, start: date, end: date) -> pl.DataFrame:
        """
        Fetch raw data for the configured assets between start and end (inclusive).

        Returns a Polars DataFrame with at minimum these columns:
            source (str), symbol (str), date (date), close (float)
        Optional: open, high, low, volume
        """

    def ingest(self, start: date, end: date) -> int:
        """
        Orchestrate fetch → validate → upsert. Returns the number of rows written.
        """
        logger.info(
            f"[{self.__class__.__name__}] Ingesting {start} → {end} into {self.target_table}"
        )
        df = self.fetch(start, end)

        if df.is_empty():
            logger.warning(f"[{self.__class__.__name__}] fetch() returned empty DataFrame")
            return 0

        df = self._add_ingested_at(df)
        self._validate_schema(df)
        rows = self._upsert(df)
        logger.success(f"[{self.__class__.__name__}] Upserted {rows} rows into {self.target_table}")
        return rows

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _add_ingested_at(self, df: pl.DataFrame) -> pl.DataFrame:
        """Stamp every row with the current UTC time."""
        now = datetime.now(tz=UTC)
        return df.with_columns(pl.lit(now).alias("ingested_at"))

    def _validate_schema(self, df: pl.DataFrame) -> None:
        """Assert the minimum required columns are present."""
        required = {"source", "symbol", "date", "close"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"[{self.__class__.__name__}] DataFrame missing required columns: {missing}"
            )

    def _upsert(self, df: pl.DataFrame) -> int:
        """
        Insert rows into the target Bronze table, replacing on PK conflict.

        DuckDB can query a Polars DataFrame directly via `duckdb.table()`.
        We register the frame as a view then run INSERT OR REPLACE.
        """
        # Ensure optional columns exist with nulls rather than missing entirely
        df = _ensure_ohlcv_columns(df)

        self.conn.register("_ingest_batch", df)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO {self.target_table}
            SELECT source, symbol, date, open, high, low, close, volume, ingested_at
            FROM _ingest_batch
        """)
        self.conn.unregister("_ingest_batch")
        return len(df)


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

_OHLCV_COLS = ["open", "high", "low", "close", "volume"]


def _ensure_ohlcv_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add any missing OHLCV columns as null Float64."""
    for col in _OHLCV_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
    return df
