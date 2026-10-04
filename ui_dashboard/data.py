from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Dashboard chi doc artifacts/dashboard (HD6, HD7, HD8) va artifacts/reports; khong doc HDFS, khong import config/.
DEFAULT_DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "dashboard"
DEFAULT_REPORTS_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports"

EXPORT_JOB = "analytics/export_dashboard.py"
TABLES: Dict[str, Tuple[str, str]] = {
    # name -> (job that produces it, contract)
    "afr_by_model": ("analytics/build_analytics.py", "HĐ6"),
    "afr_by_manufacturer": ("analytics/build_analytics.py", "HĐ6"),
    "smart_distribution": ("analytics/build_analytics.py", "HĐ6"),
    "pre_failure_signal": ("analytics/build_analytics.py", "HĐ6"),
    "kmeans_k_selection": ("analytics/kmeans_segmentation.py", "HĐ7"),
    "kmeans_clusters": ("analytics/kmeans_segmentation.py", "HĐ7"),
    "kmeans_health_crosstab": ("analytics/kmeans_segmentation.py", "HĐ7"),
    "dashboard_overview": (EXPORT_JOB, "HĐ8"),
    "health_distribution": (EXPORT_JOB, "HĐ8"),
    "health_snapshot": (EXPORT_JOB, "HĐ8"),
    "predictions_topk": (EXPORT_JOB, "HĐ8"),
    "smart_history_topk": (EXPORT_JOB, "HĐ8"),
    "model_metrics": (EXPORT_JOB, "HĐ8"),
    "model_feature_importance": (EXPORT_JOB, "HĐ8"),
    "benchmark": ("scripts/run_benchmark.py (rồi scripts/build_benchmark_report.py)", "HĐ9"),
    "benchmark_environment": ("scripts/run_benchmark.py (rồi scripts/build_benchmark_report.py)", "HĐ9"),
}

MODEL_LABELS = {
    "logistic_regression": "Logistic Regression (chính thức)",
    "random_forest": "Random Forest (so sánh)",
    "baseline_rules_v1": "Luật rules_v1 (baseline)",
}
HEALTH_ORDER = ["HEALTHY", "WATCH", "CRITICAL"]
HEALTH_SHORT_LABELS = {"HEALTHY": "Khỏe", "WATCH": "Cần theo dõi", "CRITICAL": "Nguy hiểm"}
HEALTH_LABELS = {"HEALTHY": "Khỏe (HEALTHY)", "WATCH": "Cần theo dõi (WATCH)", "CRITICAL": "Nguy hiểm (CRITICAL)"}

# Ten cot hien thi. Khong dung chu "xac suat"/"probability": risk_score la diem rui ro, khong phai xac suat hieu chinh.
DISPLAY_COLUMNS = {
    "risk_rank": "Hạng",
    "serial_number": "Serial",
    "model": "Model ổ",
    "risk_score": "Điểm rủi ro",
    "health_level": "Mức tình trạng",
    "reasons": "Lý do (luật rules_v1)",
    "note": "Ghi chú",
    "outcome": "Thực tế (hỏng trong 7 ngày sau)",
}

# Ngay chon duoc trong o chon ngay: ten tap du lieu cua ngay do, de nguoi xem biet diem co phai "ngoai mau" khong.
SPLIT_LABELS = {
    "train": "tập huấn luyện",
    "val": "tập validation",
    "test": "tập test Q1",
    "oot": "Q2 ngoài thời gian",
}
IN_SAMPLE_SPLITS = ("train", "val")


def dashboard_dir() -> Path:
    return Path(os.environ.get("SMART_DASHBOARD_DIR", str(DEFAULT_DASHBOARD_DIR)))


def reports_dir() -> Path:
    return Path(os.environ.get("SMART_REPORTS_DIR", str(DEFAULT_REPORTS_DIR)))


def table_path(name: str, base: Optional[Path] = None) -> Path:
    return (Path(base) if base else dashboard_dir()) / (name + ".parquet")


def missing_hint(name: str, reason: Optional[str] = None) -> str:
    job, contract = TABLES.get(name, ("(không rõ)", "?"))
    text = "Chưa có bảng `{}` ({}). Tạo bằng: `spark-submit {}` (xem `docs/quick-start.md`).".format(name, contract, job)
    return text + (" Lý do: {}".format(reason) if reason else "")


def load_table(name: str, base: Optional[Path] = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """(DataFrame, None) when the table exists; (None, hint) otherwise. Never raises."""
    path = table_path(name, base)
    if not path.exists():
        return None, missing_hint(name)
    try:
        return pd.read_parquet(str(path)), None
    except Exception as exc:  # noqa: BLE001 - a broken file must show a hint, not crash the page
        return None, missing_hint(name, "không đọc được file ({})".format(type(exc).__name__))


def table_mtime(name: str, base: Optional[Path] = None) -> float:
    path = table_path(name, base)
    return path.stat().st_mtime if path.exists() else 0.0


def table_status(base: Optional[Path] = None) -> pd.DataFrame:
    rows = [(name, contract, job, table_path(name, base).exists()) for name, (job, contract) in TABLES.items()]
    return pd.DataFrame(rows, columns=["Bảng", "Hợp đồng", "Tạo bằng", "Có dữ liệu"])


def list_pipeline_runs(base: Optional[Path] = None) -> List[Path]:
    directory = Path(base) if base else reports_dir()
    return sorted(directory.glob("pipeline_run_*.md"), reverse=True) if directory.exists() else []


# ---------- display shaping ----------

def health_sorted(df: pd.DataFrame, column: str = "health_level") -> pd.DataFrame:
    order = {level: i for i, level in enumerate(HEALTH_ORDER)}
    return df.sort_values(column, key=lambda s: s.map(order)).reset_index(drop=True)


def reasons_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        return "; ".join(str(v) for v in list(value))
    except TypeError:
        return str(value)


def topk_display(topk: pd.DataFrame, notes: Dict[str, str]) -> pd.DataFrame:
    """Top-K table with Vietnamese column names; `notes` maps serial -> conflict explanation (short)."""
    out = topk.sort_values("risk_rank").copy()
    out["reasons"] = out["reasons"].map(reasons_text)
    out["note"] = out["serial_number"].map(lambda s: notes.get(s, ""))
    columns = ["risk_rank", "serial_number", "model", "risk_score", "health_level", "reasons", "note"]
    if "failed_within_7d" in out.columns:  # per-day tables know the outcome in hindsight
        out["outcome"] = out["failed_within_7d"].map(lambda v: "Có" if v == 1 else "Không")
        columns.append("outcome")
    return out[columns].rename(columns=DISPLAY_COLUMNS)


def conflict_summary(topk: pd.DataFrame, snapshot: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """Counts that show how the rule baseline and the risk-score ranking agree on the scored day."""
    by_level = topk["health_level"].fillna("(không có)").value_counts().to_dict()
    summary = {"topk_total": int(len(topk)), "topk_by_level": by_level}
    if snapshot is not None:
        critical = set(snapshot.loc[snapshot["health_level"] == "CRITICAL", "serial_number"])
        in_topk = critical & set(topk["serial_number"])
        summary.update(critical_total=len(critical), critical_in_topk=len(in_topk),
                       critical_outside_topk=len(critical) - len(in_topk))
    return summary


def daily_conflict_summary(topk: pd.DataFrame, day: pd.Series) -> Dict[str, Any]:
    """conflict_summary for a picked day: the per-day table already carries the CRITICAL counts."""
    summary = conflict_summary(topk, None)
    if pd.isna(day["critical_total"]):  # no rules_v1 rows for this day: say nothing rather than "0 CRITICAL"
        return summary
    total, inside = int(day["critical_total"]), int(day["critical_in_topk"])
    summary.update(critical_total=total, critical_in_topk=inside, critical_outside_topk=total - inside)
    return summary


def day_hindsight(day: pd.Series, k: int) -> str:
    """One sentence about how the Top-K of a day turned out, in hindsight (descriptive, not an evaluation)."""
    hits, positives = int(day["topk_hits"]), int(day["positives"])
    text = "Nhìn lại: trong Top-{} có {} ổ thực tế hỏng trong 7 ngày sau (precision {:.1f}%); toàn ngày có {:,} ổ hỏng trong 7 ngày sau".format(
        k, hits, hits / k * 100 if k else 0.0, positives)
    if positives:
        text += " (recall {:.1f}%)".format(hits / positives * 100)
    return text + "."


def metrics_views(metrics: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Split model_metrics into display tables. recall@K / precision@K of the FULL test are never
    returned (right-censoring tail dominates them); the headline test number is the 'normal' segment."""
    def label(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["Model"] = out["model"].map(MODEL_LABELS).fillna(out["model"])
        order = {m: i for i, m in enumerate(MODEL_LABELS)}
        return out.sort_values("model", key=lambda s: s.map(order)).reset_index(drop=True)

    def pct(df: pd.DataFrame, k: int) -> pd.DataFrame:
        return pd.DataFrame({
            "Model": df["Model"],
            "Recall@{} (%)".format(k): df["recall_at_k"] * 100,
            "Precision@{} (%)".format(k): df["precision_at_k"] * 100,
        })

    k = int(metrics["k"].iloc[0]) if len(metrics) else 100
    val = label(metrics[(metrics["split"] == "val") & (metrics["segment"] == "all")])
    normal = label(metrics[(metrics["split"] == "test") & (metrics["segment"] == "normal")])
    tail = label(metrics[(metrics["split"] == "test") & (metrics["segment"] == "tail")])
    full = label(metrics[(metrics["split"] == "test") & (metrics["segment"] == "full")])

    val_table = pct(val, k)
    val_table.insert(1, "PR-AUC", val["pr_auc"])
    val_table.insert(2, "ROC-AUC", val["roc_auc"])
    auc_table = pd.DataFrame({"Model": full["Model"], "PR-AUC (toàn test)": full["pr_auc"], "ROC-AUC (toàn test)": full["roc_auc"]})
    auc_table = auc_table.dropna(subset=["PR-AUC (toàn test)", "ROC-AUC (toàn test)"], how="all")
    return {
        "k": pd.DataFrame({"k": [k]}),
        "val": val_table,
        "test_normal": pct(normal, k),
        "test_tail": pct(tail, k),
        "test_auc": auc_table.reset_index(drop=True),
        "tail_size": pd.DataFrame({"rows": tail["rows"].iloc[:1], "positives": tail["positives"].iloc[:1]}).reset_index(drop=True),
        "normal_size": pd.DataFrame({"rows": normal["rows"].iloc[:1], "positives": normal["positives"].iloc[:1]}).reset_index(drop=True),
    }


def style_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Metric table as display strings: fixed decimals and an em dash for missing values. (Streamlit ignores
    Styler `na_rep` and would show None, so the formatting is done here.)"""
    out = df.copy()
    for column in out.columns:
        if column == "Model":
            continue
        decimals = 4 if "AUC" in column else 2
        out[column] = out[column].map(lambda v, d=decimals: "—" if pd.isna(v) else "{:.{d}f}".format(float(v), d=d))
    return out


BENCHMARK_TITLES = {
    "format": "Định dạng: CSV so với Parquet",
    "workers": "Số worker: 1 so với 2",
    "small_files": "File nhỏ: Gold features gốc so với bản gộp",
    "pipeline_steps": "Thời gian từng bước pipeline (từ nhật ký đã có)",
}


def _fmt(value: Any, spec: str) -> str:
    return "—" if value is None or pd.isna(value) else spec.format(value)


def benchmark_label(variant: str, query: str) -> str:
    """Row/bar label: the variant, plus the workload when the query names one (`... @ csv_7d`)."""
    return "{} @ {}".format(variant, query.split("@", 1)[1].strip()) if "@" in str(query) else str(variant)


def benchmark_chart_frame(part: pd.DataFrame) -> pd.DataFrame:
    """Rows of one experiment with a unique `label` (two workloads of one variant must not share a bar)."""
    out = part.copy()
    out["label"] = [benchmark_label(v, q) for v, q in zip(out["variant"], out["query"])]
    return out


def benchmark_views(benchmark: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """experiment -> display table (strings, an em dash for missing values), in the order of BENCHMARK_TITLES.
    Columns that are empty for a whole experiment are dropped so nothing is cut off on the right."""
    views = {}  # type: Dict[str, pd.DataFrame]
    for experiment in BENCHMARK_TITLES:
        part = benchmark[benchmark["experiment"] == experiment]
        if part.empty:
            continue
        views[experiment] = pd.DataFrame({
            "Biến thể": [benchmark_label(v, q) for v, q in zip(part["variant"], part["query"])],
            "Lần": [_fmt(v, "{:.0f}") for v in part["runs"]],
            "Trung vị (s)": [_fmt(v, "{:.2f}") for v in part["median_seconds"]],
            "Min–Max (s)": ["{:.2f}–{:.2f}".format(lo, hi) for lo, hi in zip(part["min_seconds"], part["max_seconds"])],
            "MB": [_fmt(None if pd.isna(v) else v / 1048576.0, "{:.1f}") for v in part["size_bytes"]],
            "File": [_fmt(v, "{:,.0f}") for v in part["file_count"]],
            "Partition": [_fmt(v, "{:.0f}") for v in part["input_partitions"]],
            "Executor": [_fmt(v, "{:.0f}") for v in part["executors"]],
            "Số dòng": [_fmt(v, "{:,.0f}") for v in part["rows"]],
            "Ghi chú": [v if isinstance(v, str) and v else "—" for v in part["note"]],
        })
        # drop columns that are empty ("—") for every row of this experiment, so the useful ones are not squeezed/cut off
        keep = [c for c in views[experiment].columns if c == "Biến thể" or (views[experiment][c] != "—").any()]
        views[experiment] = views[experiment][keep].reset_index(drop=True)
    return views
