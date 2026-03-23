/*
  stg_macro — Macroeconomic indicator staging model.

  FRED series are stored with only a 'close' value; open/high/low/volume are
  null by design. The 'close' column holds the indicator value (yield %, rate %).
*/

SELECT
    source,
    symbol,
    asset_class,
    date,
    close,
    daily_return,
    log_return
FROM {{ source('marketlens', 'silver_prices') }}
WHERE asset_class = 'macro'
  AND is_outlier = FALSE
