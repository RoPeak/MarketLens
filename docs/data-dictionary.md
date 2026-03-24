# Data Dictionary

All tables live in a single DuckDB file at `data/marketlens.duckdb`.

---

## Bronze Layer

Raw source data. Never modified after ingestion. Re-ingesting overwrites via `INSERT OR REPLACE`.

### `bronze_equities`

| Column | Type | Description |
|---|---|---|
| `source` | VARCHAR | Always `'yfinance'` |
| `symbol` | VARCHAR | Ticker symbol (e.g. `SPY`, `QQQ`) |
| `date` | DATE | Trading date (**Primary Key** with `symbol`) |
| `open` | DOUBLE | Opening price (USD) |
| `high` | DOUBLE | Intraday high price (USD) |
| `low` | DOUBLE | Intraday low price (USD) |
| `close` | DOUBLE | Closing price (USD) |
| `volume` | BIGINT | Shares traded |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | UTC timestamp of ingest run |

### `bronze_crypto`

Identical schema to `bronze_equities`. Symbol is the CoinGecko ID (e.g. `bitcoin`).
Prices are in USD. CoinGecko OHLC endpoint returns 4-hour candles for ≤90-day windows,
which are aggregated to daily OHLC by taking the last close of each UTC day.

### `bronze_macro`

| Column | Type | Description |
|---|---|---|
| `source` | VARCHAR | Always `'fred'` |
| `symbol` | VARCHAR | FRED series ID (e.g. `DGS10`, `FEDFUNDS`, `UNRATE`) |
| `date` | DATE | Observation date (**Primary Key** with `symbol`) |
| `open` | DOUBLE | NULL (not applicable for macro series) |
| `high` | DOUBLE | NULL |
| `low` | DOUBLE | NULL |
| `close` | DOUBLE | Series value (yield %, rate %, percentage points) |
| `volume` | BIGINT | NULL |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | UTC timestamp of ingest run |

---

## Silver Layer

Cleaned, normalised, enriched view across all three Bronze sources.

### `silver_prices`

| Column | Type | Description |
|---|---|---|
| `source` | VARCHAR | Origin source identifier |
| `symbol` | VARCHAR | Asset identifier (**Primary Key** with `date`) |
| `asset_class` | VARCHAR | One of `'equity'`, `'crypto'`, `'macro'` |
| `date` | DATE | Trading/observation date (**Primary Key** with `symbol`) |
| `open` | DOUBLE | Opening price; NULL for macro |
| `high` | DOUBLE | Intraday high; NULL for macro |
| `low` | DOUBLE | Intraday low; NULL for macro |
| `close` | DOUBLE | Closing price or series value |
| `volume` | BIGINT | Volume; NULL for macro and crypto (CoinGecko endpoint) |
| `daily_return` | DOUBLE | `(close_t / close_t-1) - 1`; NULL on first observation per symbol |
| `log_return` | DOUBLE | `log(1 + daily_return)`; NULL on first observation per symbol |
| `is_outlier` | BOOLEAN | TRUE if close is a MAD-based outlier (excluded by Gold models) |
| `transformed_at` | TIMESTAMP WITH TIME ZONE | UTC timestamp of transformation run |

---

## Gold Layer (dbt — `main_main` schema)

Analytics mart tables consumed by the Streamlit dashboard.

### `mart_daily_returns`

One row per `(symbol, date)`. Used for normalised cumulative return charts.

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Trading date |
| `symbol` | VARCHAR | Asset identifier |
| `asset_class` | VARCHAR | `'equity'` or `'crypto'` (macro excluded) |
| `daily_return` | DOUBLE | Arithmetic daily return |
| `log_return` | DOUBLE | Log daily return |
| `cumulative_return` | DOUBLE | `EXP(SUM(log_return) OVER (...))` — rebased to 1.0 at first observation |

### `mart_volatility`

One row per `(symbol, date)`. Equities only for Garman-Klass (requires OHLC).

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Trading date |
| `symbol` | VARCHAR | Asset identifier |
| `asset_class` | VARCHAR | Asset classification |
| `close` | DOUBLE | Closing price |
| `log_return` | DOUBLE | Daily log return |
| `rolling_vol_30d` | DOUBLE | Annualised realised vol over 30-day window (`STDDEV * SQRT(252)`) |
| `rolling_vol_90d` | DOUBLE | Annualised realised vol over 90-day window |
| `gk_vol` | DOUBLE | Garman-Klass volatility estimator (NULL for macro and crypto) |
| `vol_regime_ratio` | DOUBLE | `rolling_vol_30d / rolling_vol_90d` — ratio > 1 = rising vol regime |

**Garman-Klass estimator:**
```
GK = SQRT(252 * (0.5 * ln(H/L)^2 - (2*ln2 - 1) * ln(C/O)^2))
```

### `mart_correlations`

One row per `(symbol_a, symbol_b, date)`. Only pairs where `symbol_a < symbol_b`
(upper triangle of the correlation matrix).

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Date of the rolling window endpoint |
| `symbol_a` | VARCHAR | First symbol (alphabetically smaller) |
| `symbol_b` | VARCHAR | Second symbol |
| `rolling_corr_90d` | DOUBLE | Pearson correlation over 90-day window; NULL/NaN during warm-up |
| `rolling_corr_30d` | DOUBLE | Pearson correlation over 30-day window |

**Notes:**
- DuckDB's `CORR()` returns `NaN` (not NULL) when the window is too short for meaningful correlation.
  Use `NOT isnan(rolling_corr_90d)` rather than `IS NOT NULL` to filter these.
- Macro series are excluded (no meaningful return correlation with price series).

### `mart_technical_indicators`

One row per `(symbol, date)`. Equities and crypto only.

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Trading date |
| `symbol` | VARCHAR | Asset identifier |
| `close` | DOUBLE | Closing price |
| `rsi_14` | DOUBLE | RSI with 14-period lookback (0–100); NULL during warm-up |
| `macd_line` | DOUBLE | MACD line (12-period SMA − 26-period SMA) |
| `signal_line` | DOUBLE | 9-period SMA of MACD line |
| `macd_histogram` | DOUBLE | `macd_line − signal_line` |
| `bb_upper` | DOUBLE | Bollinger upper band (20-period SMA + 2σ) |
| `bb_middle` | DOUBLE | Bollinger middle band (20-period SMA) |
| `bb_lower` | DOUBLE | Bollinger lower band (20-period SMA − 2σ) |
| `bb_pct_b` | DOUBLE | %B position: `(close − bb_lower) / (bb_upper − bb_lower)` |

**RSI formula (implemented in SQL):**
```sql
avg_gain_14 = AVG(CASE WHEN log_return > 0 THEN log_return ELSE 0 END)
              OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)

rsi_14 = CASE
    WHEN avg_loss_14 = 0 THEN 100.0
    ELSE 100.0 - (100.0 / (1.0 + avg_gain_14 / avg_loss_14))
END
```

---

## Default Asset Universe

Configured in `marketlens/config.py` (overridable via `.env`):

| Asset class | Symbols |
|---|---|
| Equity | SPY, QQQ, GLD, TLT, IWM |
| Crypto | bitcoin, ethereum, solana |
| Macro | DGS10 (10Y Treasury yield), FEDFUNDS (Fed Funds rate), UNRATE (unemployment rate) |
