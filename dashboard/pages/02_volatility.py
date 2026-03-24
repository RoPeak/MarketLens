"""
Volatility — Page 2 of MarketLens dashboard.

Sections:
  1. Rolling volatility time series (30d vs 90d selectable)
  2. Monthly volatility calendar heatmap
  3. Garman-Klass vs realised volatility for a single symbol
"""

from datetime import timedelta

import streamlit as st

from dashboard.components.charts import (
    garman_klass_vs_realised,
    volatility_calendar_heatmap,
    volatility_timeseries,
)
from dashboard.components.data_access import (
    get_available_symbols,
    get_date_range,
    get_volatility,
)

st.set_page_config(page_title="Volatility — MarketLens", layout="wide")
st.title("Volatility Analysis")

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Filters")

    symbols_by_class = get_available_symbols()
    min_date, max_date = get_date_range()

    default_start = max_date - timedelta(days=730)
    if default_start < min_date:
        default_start = min_date

    start_date = st.date_input(
        "Start date", value=default_start, min_value=min_date, max_value=max_date
    )
    end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)

    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    # Symbol selection — default to equities
    all_symbols = sorted(sym for syms in symbols_by_class.values() for sym in syms)
    default_vol_symbols = sorted(symbols_by_class.get("equity", all_symbols[:5]))[:5]

    selected_symbols = st.multiselect(
        "Symbols",
        options=all_symbols,
        default=default_vol_symbols,
    )

    vol_window = st.radio(
        "Rolling window",
        options=["30d", "90d"],
        index=0,
        horizontal=True,
    )

    st.divider()
    st.caption("Calendar heatmap & GK estimator")
    equity_syms = symbols_by_class.get("equity", [])
    detail_symbol = st.selectbox(
        "Detail symbol",
        options=sorted(equity_syms) if equity_syms else all_symbols,
        index=0,
    )

# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

if not selected_symbols:
    st.info("Select at least one symbol in the sidebar.")
    st.stop()

vol_df = get_volatility(symbols=selected_symbols, start=start_date, end=end_date)

if vol_df.empty:
    st.warning("No volatility data found for the selected filters.")
    st.stop()

vol_col = "rolling_vol_30d" if vol_window == "30d" else "rolling_vol_90d"

if vol_col not in vol_df.columns:
    st.error(f"Column `{vol_col}` not found. Ensure dbt Gold tables are up to date.")
    st.stop()

# --- Section 1: Rolling vol time series ---
st.subheader(f"Rolling {vol_window} Annualised Volatility")
st.caption(
    "Realised volatility computed as the annualised standard deviation of daily log returns "
    f"over a {vol_window} rolling window. Higher values indicate more uncertain price action."
)
st.plotly_chart(volatility_timeseries(vol_df, vol_col=vol_col), use_container_width=True)

# Summary table
with st.expander("Summary statistics"):
    valid = vol_df[vol_df[vol_col].notna()]
    stats = (
        valid.groupby("symbol")[vol_col]
        .agg(
            mean=lambda x: x.mean() * 100,
            median=lambda x: x.median() * 100,
            std=lambda x: x.std() * 100,
            min=lambda x: x.min() * 100,
            max=lambda x: x.max() * 100,
        )
        .round(2)
    )
    stats.columns = ["Mean (%)", "Median (%)", "Std Dev (%)", "Min (%)", "Max (%)"]
    st.dataframe(stats, use_container_width=True)

st.divider()

# --- Section 2: Calendar heatmap ---
if detail_symbol:
    st.subheader(f"Monthly Volatility Regime — {detail_symbol}")
    st.caption(
        "Average 30-day rolling volatility grouped by month and year. "
        "Red = high-volatility regime, green = low-volatility regime."
    )

    detail_vol = get_volatility(symbols=[detail_symbol], start=min_date, end=end_date)
    if detail_vol.empty or "rolling_vol_30d" not in detail_vol.columns:
        st.warning(f"No data available for {detail_symbol}.")
    else:
        st.plotly_chart(
            volatility_calendar_heatmap(detail_vol, detail_symbol),
            use_container_width=True,
        )

    st.divider()

    # --- Section 3: GK vs realised ---
    st.subheader(f"Garman-Klass vs Realised Volatility — {detail_symbol}")
    st.caption(
        "The Garman-Klass estimator uses open, high, low, close to produce a more efficient "
        "volatility estimate than close-to-close realised vol. Available for equities only."
    )

    detail_all = get_volatility(symbols=[detail_symbol], start=start_date, end=end_date)

    if "gk_vol" not in detail_all.columns:
        st.info("Garman-Klass vol not available (macro / crypto symbols have no OHLC data).")
    elif detail_all["gk_vol"].isna().all():
        st.info(f"Garman-Klass vol is null for {detail_symbol} — OHLC data may be missing.")
    else:
        st.plotly_chart(
            garman_klass_vs_realised(detail_all, detail_symbol),
            use_container_width=True,
        )
