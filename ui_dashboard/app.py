"""Streamlit entry point: a thin router. Pages only read artifacts/dashboard (HD6-HD8), never HDFS.

Page scripts live in views/, NOT pages/: with a pages/ directory Streamlit runs a deep-linked page
script directly (bypassing this file), so imports fail on refresh or when opening a page URL."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st  # noqa: E402

st.set_page_config(page_title="SMART Drive Failure", page_icon="💽", layout="wide")

PAGES = [
    st.Page("views/overview.py", title="Tổng quan", icon="📊", default=True),
    st.Page("views/smart_analysis.py", title="Phân tích SMART", icon="🔬"),
    st.Page("views/data_analytics.py", title="Tình trạng ổ cứng", icon="🩺"),
    st.Page("views/failure_prediction.py", title="Dự đoán hỏng 7 ngày", icon="⚠️"),
    st.Page("views/cluster_performance.py", title="Hiệu năng cụm", icon="⚙️"),
]

st.navigation(PAGES).run()
