/*
  stg_equities — Equity price staging model.

  Filters silver_prices to the equity asset class. Rolling vol and other
  analytics are computed in the Gold layer by mart_volatility.
*/

SELECT
    source,
    symbol,
    asset_class,
    date,
    open,
    high,
    low,
    close,
    volume,
    daily_return,
    log_return,
    is_outlier
FROM {{ source('marketlens', 'silver_prices') }}
WHERE asset_class = 'equity'
  AND is_outlier = FALSE
