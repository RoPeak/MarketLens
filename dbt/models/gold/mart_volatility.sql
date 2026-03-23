/*
  mart_volatility — Rolling realised volatility for equities and crypto.

  DuckDB showcase:
    1. Rolling STDDEV with ROWS BETWEEN window frames (annualised via √252)
    2. Garman-Klass volatility estimator using OHLC (more efficient than
       close-to-close; exploits intraday range information)
    3. Vol-regime ratio (30d/90d) — >1 indicates elevated near-term risk

  Garman-Klass formula (annualised):
    GK_var = 0.5 * ln(H/L)² - (2·ln2 - 1) · ln(C/O)²
    GK_vol = √(252 · rolling_avg(GK_var, 30 days))

  Reference: Garman & Klass (1980), "On the Estimation of Security Price Volatilities"
*/

WITH price_data AS (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        log_return
    FROM {{ ref('stg_equities') }}
    WHERE log_return IS NOT NULL

    UNION ALL

    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        log_return
    FROM {{ ref('stg_crypto') }}
    WHERE log_return IS NOT NULL
),

with_vol AS (
    SELECT
        symbol,
        date,
        close,
        log_return,
        open,
        high,
        low,

        -- Close-to-close realised vol (annualised), computed in SQL
        STDDEV(log_return) OVER (
            PARTITION BY symbol ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) * SQRT(252) AS rolling_vol_30d,

        STDDEV(log_return) OVER (
            PARTITION BY symbol ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) * SQRT(252) AS rolling_vol_90d,

        -- Garman-Klass daily variance component
        CASE
            WHEN open > 0 AND low > 0
            THEN 0.5 * POW(LN(high / low), 2)
                 - (2 * LN(2) - 1) * POW(LN(close / open), 2)
            ELSE NULL
        END AS gk_daily_var

    FROM price_data
)

SELECT
    symbol,
    date,
    close,
    log_return,
    rolling_vol_30d,
    rolling_vol_90d,

    -- Garman-Klass annualised vol (30-day)
    SQRT(252 * AVG(gk_daily_var) OVER (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )) AS gk_vol_30d,

    -- Vol-regime ratio: short-term vs long-term vol (>1 = elevated risk)
    CASE
        WHEN rolling_vol_90d > 0
        THEN rolling_vol_30d / rolling_vol_90d
        ELSE NULL
    END AS vol_regime_ratio

FROM with_vol
ORDER BY symbol, date
