"""Shared look of the dashboard: one light theme, a restrained palette and one chart theme, applied once in app.py.

Colours are the validated categorical order of the dataviz method (blue, orange, aqua, ...), used in that fixed order.
No web fonts and no external requests: the dashboard must look the same offline during a demo.
"""
from __future__ import annotations

import altair as alt
import streamlit as st

SURFACE = "#fcfcfb"
SURFACE_2 = "#f3f2ef"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
BORDER = "#dedcd6"
GRID = "#ebe9e4"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
FONT = "'Segoe UI', system-ui, -apple-system, 'Helvetica Neue', Arial, sans-serif"

CSS = """
<style>
html, body, [class*="css"], .stApp { font-family: %(font)s; }
.stApp { background: %(surface)s; color: %(text)s; }
[data-testid="stSidebar"] { background: %(surface2)s; border-right: 1px solid %(border)s; }
[data-testid="stHeader"] { background: transparent; }
.stDeployButton, [data-testid="stToolbar"] { display: none; }
.block-container { padding-top: 2.2rem; max-width: 1180px; }
h1 { font-size: 1.85rem !important; font-weight: 650 !important; letter-spacing: -0.01em; color: %(text)s; padding-bottom: .2rem; }
h2, h3 { font-weight: 620 !important; letter-spacing: -0.005em; color: %(text)s; }
h3 { font-size: 1.18rem !important; margin-top: 1.4rem; padding-top: .9rem; border-top: 1px solid %(border)s; }
[data-testid="stCaptionContainer"], .stCaption { color: %(text2)s; }
[data-testid="stMetric"] { background: %(surface)s; border: 1px solid %(border)s; border-radius: 8px; padding: .8rem 1rem; }
[data-testid="stMetricLabel"] { color: %(text2)s; font-size: .82rem; }
[data-testid="stMetricValue"] { font-weight: 650; font-variant-numeric: tabular-nums; font-size: 1.55rem; }
[data-testid="stMetricValue"] > div { overflow: visible; text-overflow: clip; white-space: normal; word-break: break-word; }
[data-testid="stDataFrame"] { border: 1px solid %(border)s; border-radius: 8px; }
[data-testid="stTable"] table { font-size: .86rem; border: 1px solid %(border)s; border-radius: 8px; }
[data-testid="stTable"] th { background: %(surface2)s; color: %(text2)s; font-weight: 600; }
[data-testid="stTable"] td, [data-testid="stTable"] th { vertical-align: top; padding: .5rem .7rem; border-color: %(border)s; }
[data-testid="stExpander"] { border: 1px solid %(border)s; border-radius: 8px; background: %(surface)s; }
[data-testid="stSidebarNav"] a { border-radius: 6px; }
[data-testid="stSidebarNav"] a[aria-current="page"] { background: #e6eefb; color: #1b4f99; font-weight: 600; }
.stButton > button { border-radius: 6px; border: 1px solid %(border)s; font-weight: 550; }
.stButton > button[kind="primary"] { background: #2a78d6; border-color: #2a78d6; color: #fff; }
.stButton > button[kind="primary"]:hover { background: #1f66bb; border-color: #1f66bb; }
code { background: %(surface2)s; border-radius: 4px; }
hr { border-color: %(border)s; }
</style>
""" % {"font": FONT, "surface": SURFACE, "surface2": SURFACE_2, "text": TEXT, "text2": TEXT_2, "border": BORDER}


def _chart_theme():
    return {
        "config": {
            "background": SURFACE,
            "font": "Segoe UI",
            "view": {"stroke": None},
            "range": {"category": SERIES},
            "mark": {"color": SERIES[0]},
            "bar": {"cornerRadiusTopRight": 3, "cornerRadiusBottomRight": 3},
            "line": {"strokeWidth": 2},
            "point": {"size": 36},
            "axis": {
                "labelColor": TEXT_2, "titleColor": TEXT_2, "titleFontWeight": "normal", "labelFontSize": 11, "titleFontSize": 12,
                "domainColor": BORDER, "tickColor": BORDER, "gridColor": GRID, "gridWidth": 1,
            },
            "axisX": {"grid": False},
            "legend": {"labelColor": TEXT_2, "titleColor": TEXT_2, "labelFontSize": 11, "symbolType": "square"},
            "title": {"color": TEXT, "fontSize": 13, "fontWeight": 600},
        }
    }


def apply() -> None:
    """Inject the stylesheet and make every Altair chart use the dashboard theme instead of Streamlit's colours."""
    st.markdown(CSS, unsafe_allow_html=True)
    alt.themes.register("smart_drive", _chart_theme)
    alt.themes.enable("smart_drive")
    if not getattr(st, "_smart_altair_wrapped", False):
        original = st.altair_chart

        def altair_chart(chart, *args, **kwargs):
            kwargs.setdefault("theme", None)  # theme=None keeps our colours; "streamlit" would repaint them
            return original(chart, *args, **kwargs)

        st.altair_chart = altair_chart
        st._smart_altair_wrapped = True
