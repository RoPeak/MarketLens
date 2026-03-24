"""
Correlations — Page 3 of MarketLens dashboard.

Sections:
  1. Rolling correlation heatmap (snapshot at selected date)
  2. Hierarchical clustering dendrogram
  3. Pair correlation time series
"""

import streamlit as st

from dashboard.components.charts import (
    correlation_dendrogram,
    correlation_heatmap,
    correlation_timeseries_chart,
)
from dashboard.components.data_access import (
    get_available_symbols,
    get_correlation_timeseries,
    get_correlations,
    get_date_range,
)

st.set_page_config(page_title="Correlations — MarketLens", layout="wide")
st.title("Correlation Analysis")

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Filters")

    min_date, max_date = get_date_range()

    as_of_date = st.date_input(
        "Correlation matrix as of",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        help=(
            "Shows the rolling correlation matrix computed on this date "
            "(or the most recent date before it)."
        ),
    )

    window = st.radio(
        "Rolling window",
        options=["90d", "30d"],
        index=0,
        horizontal=True,
    )

    st.divider()
    st.caption("Pair correlation time series")

    symbols_by_class = get_available_symbols()
    all_symbols = sorted(sym for syms in symbols_by_class.values() for sym in syms)

    if len(all_symbols) < 2:
        st.warning("Need at least 2 symbols for a pair chart.")
        pair_a = pair_b = None
    else:
        pair_a = st.selectbox("Symbol A", options=all_symbols, index=0)
        pair_b_options = [s for s in all_symbols if s != pair_a]
        pair_b = st.selectbox("Symbol B", options=pair_b_options, index=0)

# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

corr_df = get_correlations(as_of=as_of_date, window=window)

if corr_df.empty:
    st.warning(
        "No correlation data found. Ensure `mart_correlations` is populated (`make dbt-run`)."
    )
    st.stop()

# --- Section 1: Correlation heatmap ---
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Rolling Correlation Matrix")
    st.caption(
        f"Pearson correlations over a {window} rolling window. "
        f"Snapshot: most recent date ≤ {as_of_date}. "
        "Values closer to +1 (blue) indicate strong positive co-movement; "
        "−1 (red) indicates inverse movement."
    )
    st.plotly_chart(correlation_heatmap(corr_df), use_container_width=True)

with col_right:
    st.subheader("Top Correlations")
    top_n = corr_df.copy()
    top_n["abs_corr"] = top_n["correlation"].abs()
    top_n = top_n.sort_values("abs_corr", ascending=False).head(10)
    top_n["Pair"] = top_n["symbol_a"] + " / " + top_n["symbol_b"]
    top_n["ρ"] = top_n["correlation"].round(3)
    st.dataframe(top_n[["Pair", "ρ"]].reset_index(drop=True), use_container_width=True)

    st.subheader("Most Negative")
    bottom_n = corr_df.sort_values("correlation").head(5)
    bottom_n["Pair"] = bottom_n["symbol_a"] + " / " + bottom_n["symbol_b"]
    bottom_n["ρ"] = bottom_n["correlation"].round(3)
    st.dataframe(bottom_n[["Pair", "ρ"]].reset_index(drop=True), use_container_width=True)

st.divider()

# --- Section 2: Dendrogram ---
st.subheader("Hierarchical Clustering")
st.caption(
    "Ward linkage clustering using distance = 1 − |ρ|. "
    "Assets that cluster together tend to move more similarly. "
    "Useful for identifying diversification opportunities."
)
st.plotly_chart(correlation_dendrogram(corr_df), use_container_width=True)

st.divider()

# --- Section 3: Pair time series ---
if pair_a and pair_b:
    st.subheader(f"Correlation Over Time: {pair_a} × {pair_b}")
    st.caption(
        "Rolling 30-day and 90-day Pearson correlation between the selected pair. "
        "Regime shifts (e.g. sudden spikes toward 1.0 during market stress) are visible here."
    )

    # Try both orderings — pairs are stored with symbol_a < symbol_b
    sym_a_ordered, sym_b_ordered = sorted([pair_a, pair_b])
    pair_ts = get_correlation_timeseries(sym_a_ordered, sym_b_ordered)

    if pair_ts.empty:
        st.info(
            f"No time series data found for {pair_a} × {pair_b}. "
            "The pair may not share enough overlapping dates for a rolling window."
        )
    else:
        st.plotly_chart(
            correlation_timeseries_chart(pair_ts, pair_a, pair_b),
            use_container_width=True,
        )

        with st.expander("Raw data"):
            st.dataframe(pair_ts, use_container_width=True)
