/*
  mart_daily_returns — Unified daily returns table, long format.

  Combines equities and crypto into a single normalised returns series.
  Macro indicators are excluded as their return series is not economically
  comparable to price returns (rates are already in % terms).

  DuckDB showcase: UNION ALL with consistent schema across asset classes,
  providing a single table for cross-asset return comparison.

  Consumers (dashboard, dbt tests) can pivot this on-the-fly or use it
  as long-format input for Plotly charts.
*/

WITH equities AS (
    SELECT
        date,
        symbol,
        'equity'      AS asset_class,
        close,
        daily_return,
        log_return
    FROM {{ ref('stg_equities') }}
    WHERE daily_return IS NOT NULL
),

crypto AS (
    SELECT
        date,
        symbol,
        'crypto'      AS asset_class,
        close,
        daily_return,
        log_return
    FROM {{ ref('stg_crypto') }}
    WHERE daily_return IS NOT NULL
),

combined AS (
    SELECT * FROM equities
    UNION ALL
    SELECT * FROM crypto
)

SELECT
    date,
    symbol,
    asset_class,
    close,
    daily_return,
    log_return,
    -- Cumulative return indexed to 1.0 at first observation for each symbol
    EXP(SUM(log_return) OVER (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )) AS cumulative_return
FROM combined
ORDER BY date, symbol
