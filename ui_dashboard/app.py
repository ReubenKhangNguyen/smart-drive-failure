"""Streamlit entry point: a thin router. Pages only read artifacts/dashboard (HD6-HD8), never HDFS."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st  # noqa: E402

st.set_page_config(page_title="SMART Drive Failure", page_icon="💽", layout="wide")

PAGES = [
    st.Page("pages/overview.py", title="Tổng quan", icon="📊", default=True),
    st.Page("pages/smart_analysis.py", title="Phân tích SMART", icon="🔬"),
    st.Page("pages/data_analytics.py", title="Tình trạng ổ cứng", icon="🩺"),
    st.Page("pages/failure_prediction.py", title="Dự đoán hỏng 7 ngày", icon="⚠️"),
    st.Page("pages/cluster_performance.py", title="Hiệu năng cụm", icon="⚙️"),
]

st.navigation(PAGES).run()
