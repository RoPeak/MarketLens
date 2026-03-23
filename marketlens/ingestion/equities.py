"""Equity OHLCV ingester — fetches data from Yahoo Finance via yfinance."""

from datetime import date

import pandas as pd
import polars as pl
import yfinance as yf
from loguru import logger

from marketlens.ingestion.base import BaseIngester

_SOURCE = "yahoo_finance"

# Columns we want from yfinance (lowercase, after stacking)
_KEEP_COLS = ["source", "symbol", "date", "open", "high", "low", "close", "volume"]


class EquitiesIngester(BaseIngester):
    """
    Downloads daily OHLCV bars for configured equity tickers using yfinance.

    yfinance returns a MultiIndex DataFrame (Price × Ticker) when multiple
    tickers are requested. We stack the Ticker level to produce a tidy long-
    format frame with one row per (date, symbol), then convert to Polars.

    Tickers are configured via Settings.yfinance_tickers.
    """

    target_table = "bronze_equities"

    def fetch(self, start: date, end: date) -> pl.DataFrame:
        tickers = self.settings.yfinance_tickers
        logger.debug(f"[EquitiesIngester] Downloading {tickers} from {start} to {end}")

        raw = yf.download(
            tickers=tickers,
            start=str(start),
            end=str(end),
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        if raw.empty:
            logger.warning("[EquitiesIngester] yfinance returned empty DataFrame")
            return pl.DataFrame()

        # Stack the Ticker level (level=1) → one row per (date, symbol).
        # Use positional index rather than level name since yfinance may label
        # the level as "Ticker", "ticker", or None depending on version.
        df_long = raw.stack(level=1, future_stack=True).reset_index()
        df_long.columns = [str(c).lower().replace(" ", "_") for c in df_long.columns]

        # After stacking, the date index becomes the first column and the ticker
        # becomes the second column (named "ticker", "level_1", or the level name).
        # Normalise both to canonical column names.
        cols = list(df_long.columns)
        if cols[0] != "date":
            df_long = df_long.rename(columns={cols[0]: "date"})
        ticker_col = next((c for c in df_long.columns if c in ("ticker", "level_1")), None)
        if ticker_col and ticker_col != "symbol":
            df_long = df_long.rename(columns={ticker_col: "symbol"})

        df_long["source"] = _SOURCE
        df_long["date"] = pd.to_datetime(df_long["date"]).dt.date

        # Drop rows where close is missing (holidays, unlisted tickers)
        df_long = df_long.dropna(subset=["close"])

        # Select only the columns we need (some may be absent e.g. no volume)
        available = [c for c in _KEEP_COLS if c in df_long.columns]
        return pl.from_pandas(df_long[available]).with_columns(
            [
                pl.col("date").cast(pl.Date),
                pl.col("close").cast(pl.Float64),
            ]
        )
