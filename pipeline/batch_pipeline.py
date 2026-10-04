from __future__ import annotations

import datetime as dt
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from config.settings import enable_dynamic_overwrite, hdfs_uri

PipelineStep = Tuple[str, Callable[[], Dict[str, Any]]]

DEFAULT_REPORT_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports"
NOT_APPLICABLE = "không áp dụng"

# Names of the batch steps, in pipeline order (see build_steps); `--steps` selects a subset of them.
STEP_NAMES = ("silver_etl", "analytics", "health_status", "features")


def run_steps(steps: List[PipelineStep]) -> Dict[str, Any]:
    """Run (name, callable) steps in order, stop on the first exception, and log the
    elapsed time of every step that ran (including the one that failed)."""
    log: List[Dict[str, Any]] = []
    for name, step in steps:
        start = time.time()
        try:
            result = step()
        except Exception as exc:  # noqa: BLE001 - re-raised after logging which step failed
            log.append({"step": name, "elapsed_seconds": round(time.time() - start, 2), "status": "failed", "error": str(exc)})
            return {"steps": log, "status": "failed", "failed_step": name}
        log.append({"step": name, "elapsed_seconds": round(time.time() - start, 2), "status": "ok", "result": result})
    return {"steps": log, "status": "ok"}


def parse_steps(text: Optional[str]) -> Optional[List[str]]:
    """`--steps a,b` -> ['a', 'b']; None means every step. Unknown or empty names raise ValueError."""
    if text is None:
        return None
    names = [n.strip() for n in text.split(",") if n.strip()]
    if not names:
        raise ValueError("--steps is empty; valid steps: {}".format(", ".join(STEP_NAMES)))
    unknown = [n for n in names if n not in STEP_NAMES]
    if unknown:
        raise ValueError("unknown step(s): {}; valid steps: {}".format(", ".join(unknown), ", ".join(STEP_NAMES)))
    return names


def select_steps(steps: List[PipelineStep], names: Optional[List[str]] = None) -> List[PipelineStep]:
    """Keep only the named steps, always in pipeline order (the order of `names` is ignored)."""
    if names is None:
        return list(steps)
    available = {name for name, _ in steps}
    missing = sorted(set(names) - available)
    if missing:
        raise ValueError("steps not in this pipeline: {}".format(", ".join(missing)))
    wanted = set(names)
    return [step for step in steps if step[0] in wanted]


def _rows_in_out(result: Optional[Dict[str, Any]]) -> Tuple[Any, Any]:
    """Pick (rows_in, rows_out) out of a step result; None when the step reports neither."""
    if not result:
        return None, None
    rows_in = result.get("rows_in")
    rows_out = result.get("rows_out", result.get("rows", result.get("rows_scored")))
    if rows_out is None and "distribution" in result:
        rows_out = sum(result["distribution"].values())
    return rows_in, rows_out


def _fmt_rows(value: Any) -> str:
    return NOT_APPLICABLE if value is None else "{:,}".format(value)


def write_run_report(
    pipeline_name: str,
    run_result: Dict[str, Any],
    report_dir: Optional[Path] = None,
    run_date: Optional[str] = None,
) -> Path:
    """Append one section per pipeline run to artifacts/reports/pipeline_run_<ngày>.md:
    step name, elapsed seconds, status, rows in/out (docs/ROADMAP.md Phase 7 DoD)."""
    directory = Path(report_dir) if report_dir else DEFAULT_REPORT_DIR
    day = run_date or dt.date.today().isoformat()
    path = directory / "pipeline_run_{}.md".format(day)
    directory.mkdir(parents=True, exist_ok=True)

    lines = []
    if not path.exists():
        lines.append("# Nhật ký chạy pipeline — {}\n".format(day))
    status = "thành công" if run_result["status"] == "ok" else "THẤT BẠI tại bước {}".format(run_result.get("failed_step"))
    lines.append("\n## {} — {}\n".format(pipeline_name, status))
    lines.append("| Bước | Thời gian (giây) | Trạng thái | Dòng vào | Dòng ra |")
    lines.append("|---|---|---|---|---|")
    for step in run_result["steps"]:
        rows_in, rows_out = _rows_in_out(step.get("result"))
        state = "ok" if step["status"] == "ok" else "lỗi: {}".format(step.get("error"))
        lines.append("| {} | {} | {} | {} | {} |".format(
            step["step"], step["elapsed_seconds"], state, _fmt_rows(rows_in), _fmt_rows(rows_out)
        ))
    with open(str(path), "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def build_steps(spark, config: Dict[str, Any]) -> List[PipelineStep]:
    """Wire up the real Bronze->Silver->Gold steps (Phase 3-5). Never deletes
    existing Silver/Gold (CLAUDE.md luat 3): writes use mode("overwrite") with
    partitionOverwriteMode=dynamic, so re-running only overwrites the partitions
    this run touches. There is deliberately no sample mode: a partial run would
    write into the real Gold paths (see docs/decisions.md, 2026-10-02)."""
    from pyspark.sql import functions as F

    from analytics.build_analytics import run as run_build_analytics
    from analytics.health_status import run as run_health_status
    from features.build_features import build_features, feature_columns
    from features.label import assign_split, build_labeled_dataset
    from processing.spark_jobs.smart_etl import run as run_smart_etl

    data_cfg = config["data"]
    bronze_path = hdfs_uri(config, "bronze")
    silver_path = hdfs_uri(config, "silver")
    features_path = hdfs_uri(config, "features")
    health_status_path = hdfs_uri(config, "health_status")
    analytics_path = hdfs_uri(config, "analytics")
    smart_columns = [c for c in data_cfg["required_columns"] if c.startswith("smart_")]

    def step_silver():
        return run_smart_etl(spark, bronze_path, silver_path)

    def step_analytics():
        return run_build_analytics(spark, silver_path, analytics_path, smart_columns)

    def step_health_status():
        return run_health_status(spark, silver_path, health_status_path)

    def step_features():
        enable_dynamic_overwrite(spark)
        silver_df = spark.read.parquet(silver_path)
        rows_in = silver_df.count()

        labeled = build_labeled_dataset(silver_df, data_cfg["end_date"], config["project"]["horizon_days"])
        split_df = assign_split(
            labeled,
            warmup_end_date=data_cfg["warmup_end_date"],
            train_end_date=data_cfg["train_end_date"],
            val_end_date=data_cfg["val_end_date"],
        )
        train_df = split_df.where(F.col("split") == "train")
        features_df = build_features(split_df, train_df)

        cols = feature_columns(features_df)
        final_df = features_df.select("date", "serial_number", "model", *cols, "fail_within_7_days", "split")
        final_df.write.mode("overwrite").partitionBy("date").parquet(features_path)
        return {"rows_in": rows_in, "rows_out": final_df.count()}

    return [
        ("silver_etl", step_silver),
        ("analytics", step_analytics),
        ("health_status", step_health_status),
        ("features", step_features),
    ]


def build_quarter_steps(
    spark, config: Dict[str, Any], quarter_id: Optional[str] = None, dry_run: bool = False
) -> List[PipelineStep]:
    """Features of one out-of-time quarter (data.quarters): only that quarter's date partitions of Gold features
    are written (dynamic overwrite), with the quarter's own split ('oot', 'oot2', ...). Earlier quarters are never
    rewritten, and the default batch steps / Airflow DAG are unchanged (docs/decisions.md, 2026-10-04).

    Guards: the whole quarter must be in Silver (no partial features), and the dates must not already hold rows of
    another split (so Q1 train/val/test can never be overwritten). With dry_run nothing is written."""
    import datetime as dt

    from pyspark.sql import functions as F

    from config.settings import find_quarter
    from features.oot import build_quarter_features

    data_cfg = config["data"]
    quarter = find_quarter(config, quarter_id)
    silver_path = hdfs_uri(config, "silver")
    features_path = hdfs_uri(config, "features")
    start = F.lit(quarter["start_date"]).cast("date")
    end = F.lit(quarter["end_date"]).cast("date")
    expected_days = (dt.date.fromisoformat(quarter["end_date"]) - dt.date.fromisoformat(quarter["start_date"])).days + 1

    def step_features_quarter():
        enable_dynamic_overwrite(spark)
        silver_df = spark.read.parquet(silver_path)

        found_days = silver_df.where((F.col("date") >= start) & (F.col("date") <= end)).select("date").distinct().count()
        if found_days != expected_days:
            raise ValueError("Silver has {} of the {} days of {} ({}..{}): load and clean the whole quarter first".format(
                found_days, expected_days, quarter["id"], quarter["start_date"], quarter["end_date"]))

        existing = spark.read.parquet(features_path)
        in_range = existing.where((F.col("date") >= start) & (F.col("date") <= end))
        foreign = in_range.where(F.col("split") != quarter["split"]).limit(1).count()
        if foreign:
            raise ValueError("Gold features already hold rows of another split between {} and {}; refusing to overwrite them".format(
                quarter["start_date"], quarter["end_date"]))
        expected_columns = [c for c in existing.columns if c != "date"]

        plan = {"quarter": quarter["id"], "split": quarter["split"], "start_date": quarter["start_date"],
                "end_date": quarter["end_date"], "silver_days": found_days,
                "existing_feature_days": in_range.select("date").distinct().count()}
        if dry_run:
            plan["dry_run"] = True
            return plan

        out_df = build_quarter_features(silver_df, data_cfg, quarter, config["project"]["horizon_days"])
        got_columns = [c for c in out_df.columns if c != "date"]
        if sorted(got_columns) != sorted(expected_columns):
            raise ValueError(
                "quarter features columns differ from Gold features (HD2): only in quarter={}, only in Gold={}".format(
                    sorted(set(got_columns) - set(expected_columns)), sorted(set(expected_columns) - set(got_columns))
                )
            )
        out_df = out_df.select("date", *expected_columns).repartition("date")
        out_df.write.mode("overwrite").partitionBy("date").parquet(features_path)

        written = spark.read.parquet(features_path).where(F.col("split") == quarter["split"])
        stats = written.agg(
            F.count("*").alias("rows"),
            F.sum("fail_within_7_days").alias("positives"),
            F.min("date").alias("min_date"),
            F.max("date").alias("max_date"),
        ).collect()[0]
        plan.update(rows_out=stats["rows"], positives=stats["positives"],
                    min_date=str(stats["min_date"]), max_date=str(stats["max_date"]))
        return plan

    return [("features_{}".format(quarter["id"]), step_features_quarter)]


def build_oot_steps(spark, config: Dict[str, Any], quarter_id: Optional[str] = None, dry_run: bool = False) -> List[PipelineStep]:
    """Backward-compatible name of build_quarter_steps."""
    return build_quarter_steps(spark, config, quarter_id, dry_run)
