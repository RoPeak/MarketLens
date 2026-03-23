"""Macroeconomic indicator ingester — fetches from FRED via pandas-datareader."""

from datetime import date

import pandas_datareader.data as pdr
import polars as pl
from loguru import logger

from marketlens.ingestion.base import BaseIngester

_SOURCE = "fred"

# Human-readable names for FRED series IDs used in logging
_SERIES_LABELS = {
    "DGS10": "US 10Y Treasury Yield",
    "FEDFUNDS": "Federal Funds Rate",
    "UNRATE": "US Unemployment Rate",
}


class MacroIngester(BaseIngester):
    """
    Downloads macroeconomic indicator series from FRED (Federal Reserve Economic Data)
    using pandas-datareader. No API key is required.

    Series are configured via Settings.macro_series (default: DGS10, FEDFUNDS, DPCCRV).

    FRED series are not OHLCV data — each series has a single value per date.
    We store it as `close` and leave open/high/low/volume as null. This lets the
    Silver transform normalise macro alongside price data using a unified schema.
    """

    target_table = "bronze_macro"

    def fetch(self, start: date, end: date) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []

        for series_id in self.settings.macro_series:
            label = _SERIES_LABELS.get(series_id, series_id)
            logger.debug(f"[MacroIngester] Fetching FRED/{series_id} ({label}) {start}→{end}")

            df = self._fetch_series(series_id, start, end)
            if df is not None and not df.is_empty():
                frames.append(df)

        if not frames:
            return pl.DataFrame()

        return pl.concat(frames)

    def _fetch_series(self, series_id: str, start: date, end: date) -> pl.DataFrame | None:
        try:
            raw = pdr.DataReader(series_id, "fred", start, end)
        except Exception as exc:
            logger.error(f"[MacroIngester] Failed to fetch {series_id}: {exc}")
            return None

        if raw.empty:
            logger.warning(f"[MacroIngester] No data returned for {series_id}")
            return None

        # raw is a pandas DataFrame with DatetimeIndex and one column named series_id
        rows = []
        for idx, row in raw.iterrows():
            value = row.iloc[0]
            if value is None or (hasattr(value, "__float__") and value != value):
                # Skip NaN rows (FRED uses NaN for missing observations)
                continue
            rows.append(
                {
                    "source": _SOURCE,
                    "symbol": series_id,
                    "date": idx.date(),
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": float(value),
                    "volume": None,
                }
            )

        if not rows:
            return None

        return pl.DataFrame(rows).with_columns(
            [
                pl.col("date").cast(pl.Date),
                pl.col("close").cast(pl.Float64),
            ]
        )
