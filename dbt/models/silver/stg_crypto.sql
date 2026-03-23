/*
  stg_crypto — Cryptocurrency price staging model.

  CoinGecko OHLC endpoint does not provide volume, so volume is null
  throughout. Downstream Gold models use close and return columns only.
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
WHERE asset_class = 'crypto'
  AND is_outlier = FALSE
