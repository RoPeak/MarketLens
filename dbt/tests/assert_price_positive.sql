/*
  Singular test: all close prices must be strictly positive.

  Zero or negative prices indicate corrupted data that would produce
  undefined log returns (ln(0) = -∞) and nonsensical RSI values.
  We allow nulls (macro series may have gaps) but not zeros or negatives.
*/

SELECT *
FROM {{ source('marketlens', 'silver_prices') }}
WHERE close IS NOT NULL
  AND close <= 0
