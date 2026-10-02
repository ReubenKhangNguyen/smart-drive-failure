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
    "benchmark": ("analytics/benchmark.py (Phase 9, chưa làm)", "Phase 9"),
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
}


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
    return out[["risk_rank", "serial_number", "model", "risk_score", "health_level", "reasons", "note"]].rename(
        columns=DISPLAY_COLUMNS
    )


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
