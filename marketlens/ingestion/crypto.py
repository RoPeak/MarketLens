"""Cryptocurrency OHLC ingester — fetches from the CoinGecko public API."""

import time
from datetime import date, timedelta

import polars as pl
import requests
from loguru import logger

from marketlens.ingestion.base import BaseIngester

_SOURCE = "coingecko"

# CoinGecko free tier: 10–30 requests/min depending on endpoint.
# We sleep briefly between coins to avoid 429s.
_REQUEST_DELAY_SECONDS = 1.5
_MAX_RETRIES = 3

# The /coins/{id}/ohlc endpoint only accepts these specific day values.
# We round up our requested range to the nearest valid value.
_VALID_DAYS = [1, 7, 14, 30, 90, 180, 365]


def _round_up_days(days: int) -> int:
    """Return the smallest CoinGecko-valid days value >= the requested number."""
    for valid in _VALID_DAYS:
        if days <= valid:
            return valid
    return 365  # max supported by this endpoint


class CryptoIngester(BaseIngester):
    """
    Downloads daily OHLC data for configured crypto coins from the CoinGecko
    public API (no API key required for the free tier).

    Endpoint used: GET /coins/{id}/ohlc?vs_currency=usd&days={n}
    This always returns daily candles for ranges up to 180 days.
    For ranges > 180 days we make multiple requests and concatenate.

    Note: CoinGecko's OHLC endpoint returns [timestamp_ms, open, high, low, close]
    tuples. Volume is not available via this endpoint; we store it as null.
    """

    target_table = "bronze_crypto"

    def fetch(self, start: date, end: date) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []

        for coin_id in self.settings.crypto_ids:
            logger.debug(f"[CryptoIngester] Fetching {coin_id} from {start} to {end}")
            df = self._fetch_coin(coin_id, start, end)
            if df is not None and not df.is_empty():
                frames.append(df)
            time.sleep(_REQUEST_DELAY_SECONDS)

        if not frames:
            return pl.DataFrame()

        return pl.concat(frames)

    def _fetch_coin(self, coin_id: str, start: date, end: date) -> pl.DataFrame | None:
        """
        Fetch OHLC data for a single coin, chunking into ≤180-day windows if needed.
        """
        all_rows: list[dict] = []
        chunk_start = start

        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=179), end)
            raw_days = (chunk_end - chunk_start).days + 1
            days = _round_up_days(raw_days)

            rows = self._get_ohlc_with_retry(coin_id, days)
            if rows is None:
                return None

            # Filter to the requested date window
            for ts_ms, o, h, lo, c in rows:
                row_date = date.fromtimestamp(ts_ms / 1000)
                if chunk_start <= row_date <= chunk_end:
                    all_rows.append(
                        {
                            "source": _SOURCE,
                            "symbol": _coin_id_to_symbol(coin_id),
                            "date": row_date,
                            "open": float(o),
                            "high": float(h),
                            "low": float(lo),
                            "close": float(c),
                            "volume": None,
                        }
                    )

            chunk_start = chunk_end + timedelta(days=1)

        if not all_rows:
            return None

        df = (
            pl.DataFrame(all_rows)
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
            # CoinGecko may return duplicate candles at chunk boundaries
            .unique(subset=["symbol", "date"])
        )
        return df

    def _get_ohlc_with_retry(self, coin_id: str, days: int) -> list[list[float]] | None:
        """GET /coins/{id}/ohlc with exponential backoff on 429/5xx."""
        url = f"{self.settings.coingecko_base_url}/coins/{coin_id}/ohlc"
        params = {"vs_currency": "usd", "days": days}

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = requests.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429:
                    wait = 2**attempt * 10
                    logger.warning(
                        f"[CryptoIngester] Rate limited on {coin_id}, "
                        f"waiting {wait}s (attempt {attempt}/{_MAX_RETRIES})"
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        f"[CryptoIngester] HTTP {response.status_code} for {coin_id}: "
                        f"{response.text[:200]}"
                    )
                    return None

            except requests.RequestException as exc:
                logger.error(f"[CryptoIngester] Request error for {coin_id}: {exc}")
                if attempt < _MAX_RETRIES:
                    time.sleep(2**attempt)

        logger.error(f"[CryptoIngester] All retries exhausted for {coin_id}")
        return None


def _coin_id_to_symbol(coin_id: str) -> str:
    """Map CoinGecko coin IDs to short ticker symbols."""
    _MAP = {
        "bitcoin": "BTC",
        "ethereum": "ETH",
        "solana": "SOL",
        "cardano": "ADA",
        "polkadot": "DOT",
        "chainlink": "LINK",
    }
    return _MAP.get(coin_id, coin_id.upper()[:6])
