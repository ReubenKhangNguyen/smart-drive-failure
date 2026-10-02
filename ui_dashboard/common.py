from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st

from ui_dashboard import data


@st.cache_data(ttl=30, show_spinner=False)
def _cached_table(name: str, base: str, mtime: float) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    return data.load_table(name, Path(base))


def load(name: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Cached load keyed by file mtime, so a re-export shows up without restarting the app."""
    base = data.dashboard_dir()
    return _cached_table(name, str(base), data.table_mtime(name, base))


def require(name: str) -> Optional[pd.DataFrame]:
    """The table, or None after showing a how-to-create hint (a page never crashes on missing data)."""
    df, hint = load(name)
    if df is None:
        st.info(hint)
        return None
    return df


def pct(value: float, digits: int = 2) -> str:
    return "—" if value is None or pd.isna(value) else "{:.{d}f}%".format(value * 100, d=digits)
