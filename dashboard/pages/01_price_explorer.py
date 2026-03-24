"""
Price Explorer — Page 1 of MarketLens dashboard.

Sections:
  1. Multi-symbol normalised cumulative return chart
  2. Candlestick + volume chart (equities only)
  3. Daily return distribution histogram
"""

from datetime import timedelta

import streamlit as st

from dashboard.components.charts import (
    candlestick_chart,
    daily_returns_distribution,
    normalised_price_chart,
)
from dashboard.components.data_access import (
    get_available_symbols,
    get_daily_returns,
    get_date_range,
    get_ohlcv,
)

st.set_page_config(page_title="Price Explorer — MarketLens", layout="wide")
st.title("Price Explorer")

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Filters")

    symbols_by_class = get_available_symbols()
    min_date, max_date = get_date_range()

    # Default to last 1 year
    default_start = max_date - timedelta(days=365)
    if default_start < min_date:
        default_start = min_date

    start_date = st.date_input(
        "Start date", value=default_start, min_value=min_date, max_value=max_date
    )
    end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)

    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    # Symbol multi-select grouped by asset class
    all_symbols: list[str] = []
    default_selected: list[str] = []
    for asset_class, syms in sorted(symbols_by_class.items()):
        all_symbols.extend(syms)
        if asset_class in ("equity", "crypto"):
            default_selected.extend(syms[:3])  # first 3 per class as default

    selected_symbols = st.multiselect(
        "Symbols",
        options=sorted(all_symbols),
        default=sorted(set(default_selected))[:6],
    )

    st.divider()
    st.caption("Candlestick (equities only)")
    equity_syms = symbols_by_class.get("equity", [])
    candle_symbol = st.selectbox(
        "Candlestick symbol",
        options=sorted(equity_syms),
        index=0 if equity_syms else None,
        disabled=not equity_syms,
    )

# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

if not selected_symbols:
    st.info("Select at least one symbol in the sidebar.")
    st.stop()

returns_df = get_daily_returns(symbols=selected_symbols, start=start_date, end=end_date)

if returns_df.empty:
    st.warning("No data found for the selected symbols and date range.")
    st.stop()

# --- Section 1: Normalised cumulative return ---
st.subheader("Normalised Cumulative Return")
st.caption(
    "Each series is rebased to 1.0 at the start of the selected window. "
    "Values > 1.0 indicate gains relative to that starting point."
)

if "cumulative_return" not in returns_df.columns:
    st.error("Column `cumulative_return` not found. Ensure dbt Gold tables are up to date.")
else:
    st.plotly_chart(normalised_price_chart(returns_df), use_container_width=True)

# --- Section 2: Return statistics table ---
with st.expander("Summary statistics"):
    stats = (
        returns_df.groupby("symbol")["daily_return"]
        .agg(
            count="count",
            mean=lambda x: x.mean() * 100,
            std=lambda x: x.std() * 100,
            min=lambda x: x.min() * 100,
            max=lambda x: x.max() * 100,
        )
        .round(3)
    )
    stats.columns = ["Days", "Mean (%)", "Std Dev (%)", "Min (%)", "Max (%)"]
    st.dataframe(stats, use_container_width=True)

st.divider()

# --- Section 3: Daily return distribution ---
st.subheader("Daily Return Distribution")
st.plotly_chart(daily_returns_distribution(returns_df), use_container_width=True)

st.divider()

# --- Section 4: Candlestick ---
if candle_symbol:
    st.subheader(f"OHLCV — {candle_symbol}")

    ohlcv_df = get_ohlcv(symbol=candle_symbol, start=start_date, end=end_date)

    if ohlcv_df.empty:
        st.warning(f"No OHLCV data found for {candle_symbol} in the selected date range.")
    else:
        st.plotly_chart(candlestick_chart(ohlcv_df, candle_symbol), use_container_width=True)
