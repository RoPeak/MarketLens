/*
  Singular test: RSI must be in [0, 100].

  RSI is mathematically bounded between 0 and 100. Any value outside this
  range indicates a bug in the avg_gain / avg_loss window computation.
*/

SELECT *
FROM {{ ref('mart_technical_indicators') }}
WHERE rsi_14 IS NOT NULL
  AND (rsi_14 < 0 OR rsi_14 > 100)
