/*
  mart_correlations — 90-day rolling Pearson correlations between all asset pairs.

  DuckDB showcase:
    1. Self-join to enumerate all unique asset pairs (a.symbol < b.symbol)
    2. CORR() used as a window aggregate — fewer SQL engines support this;
       DuckDB handles it natively with ROWS BETWEEN framing
    3. Produces a long-format table: one row per (date, symbol_a, symbol_b)
       suitable for heatmap visualisations

  A 90-day window balances stability (enough data) against recency (regime changes).
  The minimum of 30 observations is enforced by setting short windows to NULL.
*/

WITH returns AS (
    SELECT date, symbol, daily_return
    FROM {{ ref('stg_equities') }}
    WHERE daily_return IS NOT NULL

    UNION ALL

    SELECT date, symbol, daily_return
    FROM {{ ref('stg_crypto') }}
    WHERE daily_return IS NOT NULL
),

-- All unique ordered asset pairs for a given date
pairs AS (
    SELECT
        a.date,
        a.symbol         AS symbol_a,
        b.symbol         AS symbol_b,
        a.daily_return   AS return_a,
        b.daily_return   AS return_b
    FROM returns a
    INNER JOIN returns b
        ON  a.date   = b.date
        AND a.symbol < b.symbol   -- upper-triangle only; avoids duplicates
)

SELECT
    date,
    symbol_a,
    symbol_b,
    -- 90-day rolling Pearson correlation
    CORR(return_a, return_b) OVER (
        PARTITION BY symbol_a, symbol_b
        ORDER BY date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS rolling_corr_90d,

    -- 30-day rolling correlation for shorter-term regime detection
    CORR(return_a, return_b) OVER (
        PARTITION BY symbol_a, symbol_b
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_corr_30d

FROM pairs
ORDER BY date, symbol_a, symbol_b
