"""
Run all three ingesters (equities, crypto, macro) for the configured lookback window.

Usage:
    python scripts/run_ingest.py              # uses Settings.lookback_days
    python scripts/run_ingest.py --days 30    # override lookback
"""

import argparse
from datetime import date, timedelta

from loguru import logger

from marketlens.config import settings
from marketlens.db import bootstrap_schema, get_connection
from marketlens.ingestion.crypto import CryptoIngester
from marketlens.ingestion.equities import EquitiesIngester
from marketlens.ingestion.macro import MacroIngester


def main(lookback_days: int | None = None) -> None:
    days = lookback_days or settings.lookback_days
    end = date.today()
    start = end - timedelta(days=days)

    logger.info(f"Starting ingest run: {start} → {end} ({days} days)")

    conn = get_connection(settings.db_path)
    bootstrap_schema(conn)

    total = 0
    for ingester_cls in (EquitiesIngester, CryptoIngester, MacroIngester):
        ingester = ingester_cls(conn, settings)
        try:
            n = ingester.ingest(start, end)
            total += n
        except Exception as exc:
            logger.error(f"{ingester_cls.__name__} failed: {exc}")

    conn.close()
    logger.success(f"Ingest complete. Total rows written: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MarketLens data ingest")
    parser.add_argument("--days", type=int, default=None, help="Lookback window in days")
    args = parser.parse_args()
    main(args.days)
