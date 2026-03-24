"""
MarketLens — Streamlit dashboard entry point.

Run with:
    streamlit run dashboard/app.py --server.port 8501
Or:
    make dashboard
"""

import streamlit as st

from dashboard.components.data_access import tables_exist

st.set_page_config(
    page_title="MarketLens",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Guard: check dbt Gold tables exist before rendering anything
# ---------------------------------------------------------------------------

if not tables_exist():
    st.error(
        "**Gold tables not found.** Run the pipeline first:\n\n"
        "```bash\n"
        "make pipeline   # ingest + transform + dbt-run + dbt-test\n"
        "```\n\n"
        "Or load sample data:\n\n"
        "```bash\n"
        "python scripts/seed_sample_data.py\n"
        "make dbt-run\n"
        "```"
    )
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar navigation (Streamlit multi-page apps handle pages/ automatically;
# this home page just shows an overview and lets the sidebar links do routing)
# ---------------------------------------------------------------------------

st.title("📈 MarketLens")
st.markdown(
    """
**End-to-end financial market data pipeline** — DuckDB · dbt · Polars · Streamlit

Navigate using the **sidebar** to explore:

| Page | Description |
|---|---|
| **Price Explorer** | Normalised cumulative returns, candlestick charts, return distributions |
| **Volatility** | Rolling realised vol, Garman-Klass estimator, monthly regime heatmap |
| **Correlations** | Rolling correlation matrix, hierarchical dendrogram, pair time series |

---
"""
)

# Quick dataset summary
col1, col2, col3 = st.columns(3)

try:
    from dashboard.components.data_access import get_available_symbols, get_date_range

    symbols_by_class = get_available_symbols()
    min_date, max_date = get_date_range()

    total_symbols = sum(len(v) for v in symbols_by_class.values())

    with col1:
        st.metric("Total Symbols", total_symbols)
    with col2:
        st.metric("Data From", str(min_date))
    with col3:
        st.metric("Data Through", str(max_date))

    st.markdown("### Coverage")
    for asset_class, syms in sorted(symbols_by_class.items()):
        st.markdown(f"**{asset_class.title()}:** {', '.join(sorted(syms))}")

except Exception as e:
    st.warning(f"Could not load summary metrics: {e}")
