"""
MarketLens Prefect pipeline flow.

Architecture
------------
Three ingest tasks run concurrently (equities, crypto, macro) via .submit().
Once all three complete, the Silver transform runs sequentially, followed by
dbt run + dbt test to materialise the Gold layer.

Scheduling
----------
Running this file directly starts a `flow.serve()` process that schedules the
pipeline at 06:00 UTC on weekdays (Mon–Fri), which is before US markets open.
No Prefect Cloud or Prefect agent/work-pool is required — `flow.serve()` is the
current recommended OSS pattern in Prefect 2.14+.

Usage
-----
    # Start the scheduler (blocks — keep running in a terminal)
    python marketlens/flows/pipeline_flow.py

    # Or run a one-shot manual execution without the scheduler
    python -c "from marketlens.flows.pipeline_flow import pipeline; pipeline()"

    # View the local UI (optional — separate terminal)
    prefect server start
"""

from __future__ import annotations

import subprocess
from datetime import date, timedelta

from loguru import logger
from prefect import flow, task
from prefect.tasks import exponential_backoff

from marketlens.config import settings
from marketlens.db import bootstrap_schema, get_connection
from marketlens.ingestion.crypto import CryptoIngester
from marketlens.ingestion.equities import EquitiesIngester
from marketlens.ingestion.macro import MacroIngester
from scripts.run_transforms import run_silver_pipeline

# ---------------------------------------------------------------------------
# Ingest tasks — each retries 3× with exponential back-off
# ---------------------------------------------------------------------------

_RETRY_DELAYS = exponential_backoff(backoff_factor=60)  # 60s, 120s, 240s


@task(
    name="ingest-equities",
    retries=3,
    retry_delay_seconds=_RETRY_DELAYS,
    log_prints=True,
)
def ingest_equities(start: date, end: date) -> int:
    """Download OHLCV data for configured equity tickers via yfinance."""
    conn = get_connection(settings.db_path)
    try:
        n = EquitiesIngester(conn, settings).ingest(start, end)
        logger.info(f"Equities ingest: {n} rows written")
        return n
    finally:
        conn.close()


@task(
    name="ingest-crypto",
    retries=3,
    retry_delay_seconds=_RETRY_DELAYS,
    log_prints=True,
)
def ingest_crypto(start: date, end: date) -> int:
    """Download OHLC data for configured crypto assets via CoinGecko."""
    conn = get_connection(settings.db_path)
    try:
        n = CryptoIngester(conn, settings).ingest(start, end)
        logger.info(f"Crypto ingest: {n} rows written")
        return n
    finally:
        conn.close()


@task(
    name="ingest-macro",
    retries=3,
    retry_delay_seconds=_RETRY_DELAYS,
    log_prints=True,
)
def ingest_macro(start: date, end: date) -> int:
    """Download macro indicator series (FRED) via pandas-datareader."""
    conn = get_connection(settings.db_path)
    try:
        n = MacroIngester(conn, settings).ingest(start, end)
        logger.info(f"Macro ingest: {n} rows written")
        return n
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Transform task — Silver layer (Polars)
# ---------------------------------------------------------------------------


@task(name="transform-silver", log_prints=True)
def transform_silver() -> int:
    """Run the Bronze → Silver Polars transformation pipeline."""
    conn = get_connection(settings.db_path)
    try:
        n = run_silver_pipeline(conn)
        logger.info(f"Silver transform: {n} rows written to silver_prices")
        return n
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# dbt tasks — Gold layer
# ---------------------------------------------------------------------------


def _dbt(*args: str) -> None:
    """Run a dbt command inside the dbt/ directory, streaming output."""
    cmd = ["dbt", *args]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(  # noqa: S603
        cmd,
        cwd=str(settings.db_path.parent.parent / "dbt"),
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} exited with code {result.returncode}")


@task(name="dbt-run", log_prints=True)
def dbt_run() -> None:
    """Materialise all dbt models (Gold layer)."""
    _dbt("run")


@task(name="dbt-test", log_prints=True)
def dbt_test() -> None:
    """Run dbt schema + singular tests against Gold tables."""
    _dbt("test")


# ---------------------------------------------------------------------------
# Parent flow
# ---------------------------------------------------------------------------


@flow(
    name="marketlens-pipeline",
    description=(
        "End-to-end MarketLens pipeline: "
        "concurrent Bronze ingest → Polars Silver transform → dbt Gold materialisation"
    ),
    log_prints=True,
)
def pipeline(lookback_days: int | None = None) -> dict[str, int]:
    """
    Run the full MarketLens pipeline.

    Parameters
    ----------
    lookback_days:
        How many calendar days of history to ingest. Defaults to
        ``settings.lookback_days`` (730 days on first run).
        Daily scheduled runs should use a smaller window (e.g. 3 days)
        to catch any missed trading days without re-downloading 2 years.
    """
    days = lookback_days if lookback_days is not None else settings.lookback_days
    end = date.today()
    start = end - timedelta(days=days)

    logger.info(f"Pipeline starting — window: {start} → {end} ({days} days)")

    # Schema bootstrap (idempotent)
    conn = get_connection(settings.db_path)
    bootstrap_schema(conn)
    conn.close()

    # --- Stage 1: concurrent ingest ---
    eq_future = ingest_equities.submit(start, end)
    cr_future = ingest_crypto.submit(start, end)
    ma_future = ingest_macro.submit(start, end)

    eq_rows = eq_future.result(raise_on_failure=False) or 0
    cr_rows = cr_future.result(raise_on_failure=False) or 0
    ma_rows = ma_future.result(raise_on_failure=False) or 0

    ingest_total = eq_rows + cr_rows + ma_rows
    logger.info(f"Ingest complete — {ingest_total} total rows across all sources")

    if ingest_total == 0:
        logger.warning("No data ingested — skipping downstream steps")
        return {"ingest_rows": 0, "silver_rows": 0}

    # --- Stage 2: Silver transformation (sequential — depends on all ingest) ---
    silver_rows = transform_silver()

    # --- Stage 3: Gold materialisation via dbt ---
    dbt_run()
    dbt_test()

    logger.success(
        f"Pipeline complete — ingest: {ingest_total} rows, silver: {silver_rows} rows"
    )
    return {"ingest_rows": ingest_total, "silver_rows": silver_rows}


# ---------------------------------------------------------------------------
# Entrypoint — starts the weekday scheduler via flow.serve()
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starting MarketLens pipeline scheduler (06:00 UTC, Mon–Fri)")
    logger.info("Press Ctrl+C to stop.")

    pipeline.serve(
        name="marketlens-weekday-scheduler",
        cron="0 6 * * 1-5",
        # Daily runs only need a small look-back to catch any missed sessions
        parameters={"lookback_days": 3},
    )
