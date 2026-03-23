/*
  Singular test: Pearson correlations must be in [-1, 1].

  Correlations outside this range indicate a numerical issue. NaN values are
  excluded — they arise naturally at the start of each series when the rolling
  window has fewer than 2 data points, which is expected behaviour.

  Note: In IEEE 754, NaN != NaN evaluates to TRUE, so NaN values pass an
  IS NOT NULL check. We use isnan() to exclude them explicitly.
*/

SELECT *
FROM {{ ref('mart_correlations') }}
WHERE rolling_corr_90d IS NOT NULL
  AND NOT isnan(rolling_corr_90d)
  AND (rolling_corr_90d < -1.01 OR rolling_corr_90d > 1.01)
