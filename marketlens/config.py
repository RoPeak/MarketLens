from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database
    db_path: Path = Path("data/marketlens.duckdb")

    # Logging
    log_level: str = "INFO"

    # Prefect
    prefect_api_url: str = ""

    # Data sources
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"

    # Assets to track
    yfinance_tickers: list[str] = ["SPY", "QQQ", "GLD", "TLT", "IWM"]
    crypto_ids: list[str] = ["bitcoin", "ethereum", "solana"]
    macro_series: list[str] = ["DGS10", "FEDFUNDS", "UNRATE"]

    # How far back to ingest on first run
    lookback_days: int = 730

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
