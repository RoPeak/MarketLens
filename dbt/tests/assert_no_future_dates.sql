/*
  Singular test: no rows should have a date in the future.

  Future dates indicate a data ingestion error (wrong timestamp parsing,
  timezone issues) that would corrupt returns and rolling windows.
  This test runs against silver_prices which feeds all Gold models.
*/

SELECT *
FROM {{ source('marketlens', 'silver_prices') }}
WHERE date > CURRENT_DATE
