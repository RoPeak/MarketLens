"""
Technical Indicators — Page 4 of MarketLens dashboard.

Shows RSI, MACD, and Bollinger Bands for a single equity symbol.
All indicators are computed in SQL via dbt window functions (mart_technical_indicators).
"""

from datetime import timedelta

import streamlit as st

from dashboard.components.charts import technical_indicators_chart
from dashboard.components.data_access import (
    get_available_symbols,
    get_date_range,
    get_technical_indicators,
)

st.set_page_config(page_title="Technical Indicators — MarketLens", layout="wide")
st.title("Technical Indicators")
st.caption(
    "RSI, MACD, and Bollinger Bands computed entirely in SQL via dbt window functions. "
    "All indicators run on the Gold `mart_technical_indicators` table."
)

# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Filters")

    symbols_by_class = get_available_symbols()
    min_date, max_date = get_date_range()

    # Only equities and crypto have OHLC; macro has no useful technicals
    tradeable = sorted(
        sym for cls, syms in symbols_by_class.items() if cls in ("equity", "crypto") for sym in syms
    )
    if not tradeable:
        tradeable = sorted(sym for syms in symbols_by_class.values() for sym in syms)

    symbol = st.selectbox("Symbol", options=tradeable, index=0)

    default_start = max_date - timedelta(days=365)
    if default_start < min_date:
        default_start = min_date

    start_date = st.date_input(
        "Start date",
        value=default_start,
        min_value=min_date,
        max_value=max_date,
        help="RSI and MACD need a warm-up period; starting earlier gives more reliable values.",
    )

# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

df = get_technical_indicators(symbol=symbol, start=start_date)

if df.empty:
    st.warning(
        f"No technical indicator data found for **{symbol}**. "
        "Ensure `dbt run` has been executed and `mart_technical_indicators` is populated."
    )
    st.stop()

# Drop the warm-up rows (RSI is null for first ~14 bars)
df_display = df.dropna(subset=["rsi_14"])

if df_display.empty:
    st.warning(f"All RSI values are null for {symbol} — not enough price history.")
    st.stop()

st.plotly_chart(technical_indicators_chart(df_display, symbol), width="stretch")

# ---------------------------------------------------------------------------
# Indicator explainer
# ---------------------------------------------------------------------------

with st.expander("How these indicators are calculated"):
    st.markdown(
        """
**RSI-14 (Relative Strength Index)**
- Measures the speed and magnitude of recent price changes
- RSI > 70 → potentially overbought; RSI < 30 → potentially oversold
- Computed via SQL: `AVG(gain) OVER 14-row window / AVG(loss) OVER 14-row window`

**MACD (Moving Average Convergence/Divergence)**
- MACD Line = 12-period EMA minus 26-period EMA (approximated with SMA in SQL)
- Signal Line = 9-period EMA of MACD Line
- Histogram = MACD Line minus Signal Line
- Crossovers between MACD and Signal are classic entry/exit signals

**Bollinger Bands (20-period, 2σ)**
- Middle band = 20-day SMA of close price
- Upper/Lower = Middle ± 2 × 20-day rolling standard deviation
- **%B** = position of close within the bands: 0 = lower band, 1 = upper band
- Prices near the bands signal potential reversals or breakouts
        """
    )

# Latest values snapshot
st.subheader("Latest Values")
latest = df_display.iloc[-1]
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Close", f"${latest['close']:.2f}" if "close" in latest else "—")
with col2:
    rsi = latest.get("rsi_14")
    if rsi is not None and not __import__("math").isnan(rsi):
        delta_label = "Overbought" if rsi > 70 else ("Oversold" if rsi < 30 else "Neutral")
        st.metric("RSI-14", f"{rsi:.1f}", delta=delta_label)
    else:
        st.metric("RSI-14", "—")
with col3:
    macd = latest.get("macd_line")
    signal = latest.get("macd_signal")
    if macd is not None and signal is not None:
        crossover = "Bullish" if macd > signal else "Bearish"
        st.metric("MACD", f"{macd:.4f}", delta=crossover)
    else:
        st.metric("MACD", "—")
with col4:
    bb_pct = latest.get("bb_pct_b")
    if bb_pct is not None and not __import__("math").isnan(bb_pct):
        pos = "Above upper" if bb_pct > 1 else ("Below lower" if bb_pct < 0 else f"{bb_pct:.0%}")
        st.metric("BB %B", f"{bb_pct:.2f}", delta=pos)
    else:
        st.metric("BB %B", "—")
