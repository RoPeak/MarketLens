"""
Reusable Plotly figure builders for the MarketLens dashboard.

All functions accept pandas DataFrames (as returned by data_access) and return
plotly.graph_objects.Figure objects ready for st.plotly_chart().
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Colour palette — consistent across all pages
_PALETTE = [
    "#4C72B0",
    "#DD8452",
    "#55A868",
    "#C44E52",
    "#8172B3",
    "#937860",
    "#DA8BC3",
    "#8C8C8C",
    "#CCB974",
    "#64B5CD",
]


def _symbol_colour(symbols: list[str]) -> dict[str, str]:
    return {s: _PALETTE[i % len(_PALETTE)] for i, s in enumerate(sorted(symbols))}


# ---------------------------------------------------------------------------
# Price Explorer charts
# ---------------------------------------------------------------------------


def normalised_price_chart(df: pd.DataFrame) -> go.Figure:
    """
    Multi-symbol normalised cumulative return chart.

    Expects columns: date, symbol, cumulative_return (from mart_daily_returns).
    Returns are expressed as multiples of initial value (1.0 = starting price).
    """
    fig = go.Figure()
    colours = _symbol_colour(df["symbol"].unique().tolist())

    for symbol, group in df.groupby("symbol"):
        fig.add_trace(
            go.Scatter(
                x=group["date"],
                y=group["cumulative_return"],
                name=symbol,
                mode="lines",
                line={"color": colours[symbol], "width": 1.8},
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:.3f}x<extra>%{fullData.name}</extra>",
            )
        )

    fig.update_layout(
        title="Normalised Cumulative Return",
        xaxis_title=None,
        yaxis_title="Return (1.0 = start)",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        hovermode="x unified",
        height=420,
        margin={"l": 50, "r": 20, "t": 60, "b": 40},
    )
    fig.add_hline(y=1.0, line_dash="dot", line_color="gray", line_width=1, opacity=0.5)
    return fig


def candlestick_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """
    OHLCV candlestick with volume subplot.

    Expects columns: date, open, high, low, close, volume.
    """
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.04,
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name=symbol,
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=1,
        col=1,
    )

    # Volume bars
    colours = np.where(df["close"] >= df["open"], "#26a69a", "#ef5350")
    fig.add_trace(
        go.Bar(
            x=df["date"],
            y=df["volume"],
            marker_color=colours,
            name="Volume",
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        title=f"{symbol} — OHLCV",
        xaxis_rangeslider_visible=False,
        height=500,
        margin={"l": 50, "r": 20, "t": 60, "b": 40},
        hovermode="x unified",
    )
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    return fig


def daily_returns_distribution(df: pd.DataFrame) -> go.Figure:
    """Histogram of daily returns for selected symbols (overlaid, semi-transparent)."""
    fig = go.Figure()
    colours = _symbol_colour(df["symbol"].unique().tolist())

    for symbol, group in df.groupby("symbol"):
        returns = group["daily_return"].dropna() * 100  # as %
        fig.add_trace(
            go.Histogram(
                x=returns,
                name=symbol,
                nbinsx=80,
                opacity=0.65,
                marker_color=colours[symbol],
                hovertemplate="Return: %{x:.2f}%<br>Count: %{y}<extra>%{fullData.name}</extra>",
            )
        )

    fig.update_layout(
        barmode="overlay",
        title="Daily Returns Distribution",
        xaxis_title="Daily Return (%)",
        yaxis_title="Count",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        height=360,
        margin={"l": 50, "r": 20, "t": 60, "b": 40},
    )
    return fig


# ---------------------------------------------------------------------------
# Volatility charts
# ---------------------------------------------------------------------------


def volatility_timeseries(df: pd.DataFrame, vol_col: str = "rolling_vol_30d") -> go.Figure:
    """
    Multi-symbol rolling volatility time series.

    Expects columns: date, symbol, rolling_vol_30d, rolling_vol_90d.
    Volatility values are annualised (e.g. 0.15 = 15%).
    """
    fig = go.Figure()
    colours = _symbol_colour(df["symbol"].unique().tolist())
    label = "30-day" if "30d" in vol_col else "90-day"

    for symbol, group in df.groupby("symbol"):
        fig.add_trace(
            go.Scatter(
                x=group["date"],
                y=group[vol_col] * 100,  # convert to %
                name=symbol,
                mode="lines",
                line={"color": colours[symbol], "width": 1.8},
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:.1f}%<extra>%{fullData.name}</extra>",
            )
        )

    fig.update_layout(
        title=f"Annualised {label} Rolling Volatility",
        xaxis_title=None,
        yaxis_title="Volatility (% annualised)",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        hovermode="x unified",
        height=420,
        margin={"l": 50, "r": 20, "t": 60, "b": 40},
    )
    return fig


def volatility_calendar_heatmap(df: pd.DataFrame, symbol: str) -> go.Figure:
    """
    Monthly average volatility heatmap (year × month grid).

    Expects columns: date, symbol, rolling_vol_30d.
    """
    sub = df[df["symbol"] == symbol].copy()
    sub["date"] = pd.to_datetime(sub["date"])
    sub["year"] = sub["date"].dt.year
    sub["month"] = sub["date"].dt.month

    pivot = sub.groupby(["year", "month"])["rolling_vol_30d"].mean().unstack(fill_value=None)
    pivot = pivot * 100  # as %

    month_labels = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    x_labels = [month_labels[m - 1] for m in pivot.columns]
    y_labels = [str(y) for y in pivot.index]
    z_values = pivot.values.tolist()

    fig = go.Figure(
        data=go.Heatmap(
            z=z_values,
            x=x_labels,
            y=y_labels,
            colorscale="RdYlGn_r",
            hoverongaps=False,
            hovertemplate="Month: %{x}<br>Year: %{y}<br>Vol: %{z:.1f}%<extra></extra>",
            colorbar={"title": "Vol %"},
        )
    )
    fig.update_layout(
        title=f"{symbol} — Monthly Average Volatility Regime",
        height=max(200, 60 * len(y_labels) + 100),
        margin={"l": 60, "r": 20, "t": 60, "b": 40},
        yaxis={"autorange": "reversed"},
    )
    return fig


def garman_klass_vs_realised(df: pd.DataFrame, symbol: str) -> go.Figure:
    """
    Overlaid Garman-Klass vs realised 30d vol for a single symbol.

    Expects columns: date, symbol, rolling_vol_30d, gk_vol.
    """
    sub = df[df["symbol"] == symbol]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=sub["date"],
            y=sub["rolling_vol_30d"] * 100,
            name="Realised (30d)",
            mode="lines",
            line={"color": "#4C72B0", "width": 1.8},
            hovertemplate="%{x|%Y-%m-%d}<br>Realised: %{y:.1f}%<extra></extra>",
        )
    )
    if "gk_vol" in sub.columns:
        fig.add_trace(
            go.Scatter(
                x=sub["date"],
                y=sub["gk_vol"] * 100,
                name="Garman-Klass",
                mode="lines",
                line={"color": "#DD8452", "width": 1.8, "dash": "dash"},
                hovertemplate="%{x|%Y-%m-%d}<br>GK: %{y:.1f}%<extra></extra>",
            )
        )

    fig.update_layout(
        title=f"{symbol} — Volatility Estimators",
        xaxis_title=None,
        yaxis_title="Volatility (% annualised)",
        hovermode="x unified",
        height=380,
        margin={"l": 50, "r": 20, "t": 60, "b": 40},
    )
    return fig


# ---------------------------------------------------------------------------
# Correlation charts
# ---------------------------------------------------------------------------


def correlation_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Symmetric correlation heatmap from long-format DataFrame.

    Expects columns: symbol_a, symbol_b, correlation.
    Builds a square matrix by reflecting across the diagonal.
    """
    # Build square matrix
    symbols = sorted(set(df["symbol_a"].tolist() + df["symbol_b"].tolist()))
    n = len(symbols)
    idx = {s: i for i, s in enumerate(symbols)}
    matrix = np.full((n, n), np.nan)
    np.fill_diagonal(matrix, 1.0)

    for _, row in df.iterrows():
        i, j = idx[row["symbol_a"]], idx[row["symbol_b"]]
        matrix[i, j] = row["correlation"]
        matrix[j, i] = row["correlation"]

    # Mask upper triangle for cleaner display
    mask = np.triu(np.ones_like(matrix, dtype=bool), k=1)
    display = matrix.copy()
    display[mask] = np.nan

    text = np.where(
        np.isnan(display),
        "",
        np.vectorize(lambda v: f"{v:.2f}")(display),
    )

    fig = go.Figure(
        data=go.Heatmap(
            z=display,
            x=symbols,
            y=symbols,
            text=text,
            texttemplate="%{text}",
            colorscale="RdBu",
            zmid=0,
            zmin=-1,
            zmax=1,
            hoverongaps=False,
            hovertemplate="%{y} / %{x}<br>Correlation: %{z:.3f}<extra></extra>",
            colorbar={"title": "ρ"},
        )
    )
    fig.update_layout(
        title="Rolling Correlation Matrix",
        height=max(400, 35 * n + 120),
        margin={"l": 80, "r": 20, "t": 60, "b": 80},
        xaxis={"side": "bottom"},
        yaxis={"autorange": "reversed"},
    )
    return fig


def correlation_timeseries_chart(df: pd.DataFrame, symbol_a: str, symbol_b: str) -> go.Figure:
    """
    30d and 90d rolling correlation between two symbols over time.

    Expects columns: date, rolling_corr_30d, rolling_corr_90d.
    """
    fig = go.Figure()

    if "rolling_corr_90d" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["rolling_corr_90d"],
                name="90-day",
                mode="lines",
                line={"color": "#4C72B0", "width": 2},
                hovertemplate="%{x|%Y-%m-%d}<br>90d ρ: %{y:.3f}<extra></extra>",
            )
        )
    if "rolling_corr_30d" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["rolling_corr_30d"],
                name="30-day",
                mode="lines",
                line={"color": "#DD8452", "width": 1.5, "dash": "dot"},
                hovertemplate="%{x|%Y-%m-%d}<br>30d ρ: %{y:.3f}<extra></extra>",
            )
        )

    fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1, opacity=0.4)

    fig.update_layout(
        title=f"Rolling Correlation: {symbol_a} × {symbol_b}",
        xaxis_title=None,
        yaxis_title="Pearson ρ",
        yaxis={"range": [-1.05, 1.05]},
        hovermode="x unified",
        height=380,
        margin={"l": 50, "r": 20, "t": 60, "b": 40},
    )
    return fig


def correlation_dendrogram(df: pd.DataFrame) -> go.Figure:
    """
    Hierarchical clustering dendrogram from correlation matrix.

    Uses scipy linkage on the distance matrix (distance = 1 - |correlation|).
    Falls back gracefully if fewer than 3 symbols are available.
    """
    try:
        from scipy.cluster import hierarchy
        from scipy.spatial.distance import squareform
    except ImportError:
        fig = go.Figure()
        fig.add_annotation(
            text="scipy not installed — dendrogram unavailable",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )
        return fig

    symbols = sorted(set(df["symbol_a"].tolist() + df["symbol_b"].tolist()))
    n = len(symbols)

    if n < 3:
        fig = go.Figure()
        fig.add_annotation(
            text="Need at least 3 symbols for a dendrogram",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )
        return fig

    idx = {s: i for i, s in enumerate(symbols)}
    matrix = np.zeros((n, n))
    np.fill_diagonal(matrix, 1.0)
    for _, row in df.iterrows():
        i, j = idx[row["symbol_a"]], idx[row["symbol_b"]]
        v = row["correlation"] if not np.isnan(row["correlation"]) else 0.0
        matrix[i, j] = v
        matrix[j, i] = v

    distance = 1 - np.abs(matrix)
    np.fill_diagonal(distance, 0.0)
    condensed = squareform(distance, checks=False)
    linkage = hierarchy.ward(condensed)
    dendro = hierarchy.dendrogram(linkage, labels=symbols, no_plot=True)

    icoord = np.array(dendro["icoord"])
    dcoord = np.array(dendro["dcoord"])
    leaves = dendro["ivl"]

    traces = []
    for xs, ys in zip(icoord, dcoord, strict=True):
        traces.append(
            go.Scatter(
                x=xs,
                y=ys,
                mode="lines",
                line={"color": "#4C72B0", "width": 1.5},
                showlegend=False,
                hoverinfo="skip",
            )
        )

    fig = go.Figure(traces)
    tick_positions = [10 * (i + 1) - 5 for i in range(len(leaves))]
    fig.update_layout(
        title="Hierarchical Clustering (Ward, 1 − |ρ|)",
        xaxis={
            "tickvals": tick_positions,
            "ticktext": leaves,
            "tickangle": -45,
        },
        yaxis_title="Distance",
        height=420,
        margin={"l": 50, "r": 20, "t": 60, "b": 100},
    )
    return fig


# ---------------------------------------------------------------------------
# Technical indicator chart
# ---------------------------------------------------------------------------


def technical_indicators_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """
    Four-panel chart: price + Bollinger Bands, Volume, RSI, MACD.

    Expects columns from mart_technical_indicators:
    date, close, bb_upper, bb_middle, bb_lower, rsi_14,
    macd_line, signal_line, macd_histogram.
    """
    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.40, 0.15, 0.20, 0.25],
        vertical_spacing=0.03,
        subplot_titles=("Price + Bollinger Bands", "", "RSI (14)", "MACD"),
    )

    # --- Row 1: Price + Bollinger Bands ---
    if "bb_upper" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["bb_upper"],
                name="BB Upper",
                line={"color": "rgba(100,150,200,0.4)", "width": 1},
                showlegend=False,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["bb_lower"],
                name="BB Lower",
                fill="tonexty",
                fillcolor="rgba(100,150,200,0.1)",
                line={"color": "rgba(100,150,200,0.4)", "width": 1},
                showlegend=False,
            ),
            row=1,
            col=1,
        )
    if "bb_middle" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["bb_middle"],
                name="BB Middle",
                line={"color": "rgba(100,150,200,0.6)", "width": 1, "dash": "dash"},
                showlegend=False,
            ),
            row=1,
            col=1,
        )
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["close"],
            name=symbol,
            line={"color": "#333333", "width": 1.8},
        ),
        row=1,
        col=1,
    )

    # --- Row 2: BB %B ---
    if "bb_pct_b" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["bb_pct_b"],
                name="%B",
                line={"color": "#8172B3", "width": 1.5},
                hovertemplate="%{x|%Y-%m-%d}<br>%%B: %{y:.2f}<extra></extra>",
            ),
            row=2,
            col=1,
        )
        fig.add_hline(y=1.0, line_dash="dot", line_color="red", line_width=1, row=2, col=1)
        fig.add_hline(y=0.0, line_dash="dot", line_color="green", line_width=1, row=2, col=1)
        fig.update_yaxes(title_text="%B", row=2, col=1)

    # --- Row 3: RSI ---
    if "rsi_14" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["rsi_14"],
                name="RSI-14",
                line={"color": "#C44E52", "width": 1.8},
                hovertemplate="%{x|%Y-%m-%d}<br>RSI: %{y:.1f}<extra></extra>",
            ),
            row=3,
            col=1,
        )
        fig.add_hline(y=70, line_dash="dot", line_color="red", line_width=1, row=3, col=1)
        fig.add_hline(y=30, line_dash="dot", line_color="green", line_width=1, row=3, col=1)
        fig.update_yaxes(title_text="RSI", range=[0, 100], row=3, col=1)

    # --- Row 4: MACD ---
    if "macd_line" in df.columns:
        hist_colours = np.where(df["macd_histogram"].fillna(0) >= 0, "#26a69a", "#ef5350")
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=df["macd_histogram"],
                name="Histogram",
                marker_color=hist_colours,
                showlegend=False,
            ),
            row=4,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["macd_line"],
                name="MACD",
                line={"color": "#4C72B0", "width": 1.5},
                hovertemplate="%{x|%Y-%m-%d}<br>MACD: %{y:.4f}<extra></extra>",
            ),
            row=4,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["signal_line"],
                name="Signal",
                line={"color": "#DD8452", "width": 1.5, "dash": "dot"},
                hovertemplate="%{x|%Y-%m-%d}<br>Signal: %{y:.4f}<extra></extra>",
            ),
            row=4,
            col=1,
        )
        fig.update_yaxes(title_text="MACD", row=4, col=1)

    fig.update_layout(
        title=f"{symbol} — Technical Indicators",
        height=700,
        hovermode="x unified",
        margin={"l": 60, "r": 20, "t": 80, "b": 40},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.01, "xanchor": "right", "x": 1},
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    return fig
