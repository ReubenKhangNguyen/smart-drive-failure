"""Phase 9 benchmark library (HD9): timing helpers, benchmark inputs, aggregation into the HD9 tables.

Measurement protocol (docs/contracts.md HD9): per variant 1 warm-up run that is NOT counted, then 3 counted
runs, variants of one experiment interleaved (A, B, A, B, ...). A run longer than the limit (10 minutes) stops
further runs of that variant, and the row says so in `note`. Numbers are with a warm OS cache.
"""
from __future__ import annotations

import json
import re
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

QUERY_NAME = "count+groupBy(model)"
STEP_QUERY = "pipeline step"
NOTE_SINGLE_RUN = "1 lần, không phải trung vị"
LIMIT_SECONDS = 600.0
DEFAULT_RUNS = 3
DEFAULT_WARMUP = 1
CACHE_NOTE = "đo với bộ nhớ đệm hệ điều hành đã ấm"

BENCHMARK_SCHEMA = (
    "experiment string, variant string, query string, runs int, median_seconds double, min_seconds double, "
    "max_seconds double, size_bytes bigint, file_count bigint, input_partitions int, executors int, rows bigint, note string"
)
ENVIRONMENT_SCHEMA = "item string, value string"
ROW_FIELDS = ("experiment", "variant", "query", "runs", "median_seconds", "min_seconds", "max_seconds", "size_bytes",
              "file_count", "input_partitions", "executors", "rows", "note")
KEY_FIELDS = ("experiment", "variant", "query")
EXPERIMENT_ORDER = ["format", "workers", "small_files", "pipeline_steps"]
EXPERIMENT_TITLES = {
    "format": "Định dạng: CSV so với Parquet",
    "workers": "Số worker: 1 so với 2",
    "small_files": "File nhỏ: Gold features gốc so với bản gộp",
    "pipeline_steps": "Thời gian từng bước pipeline (từ nhật ký đã có)",
}

DEFAULT_RAW_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports" / "benchmark_raw"
DEFAULT_REPORTS_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "reports"
DEFAULT_EXPORT_DIR = Path(__file__).resolve().parent.parent / "artifacts" / "dashboard"


# ---------- statistics and timing (pure) ----------

def median(values: Sequence[float]) -> float:
    if not values:
        raise ValueError("median of an empty sequence")
    ordered = sorted(values)
    mid = len(ordered) // 2
    return float(ordered[mid]) if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0


def summarize_runs(seconds: Sequence[float]) -> Dict[str, float]:
    return {"runs": len(seconds), "median": median(seconds), "min": float(min(seconds)), "max": float(max(seconds))}


def time_call(fn: Callable[[], Any], timer: Callable[[], float] = time.perf_counter) -> Tuple[float, Any]:
    start = timer()
    result = fn()
    return timer() - start, result


def measure_variants(
    variants: Dict[str, Callable[[], Any]],
    runs: int = DEFAULT_RUNS,
    warmup: int = DEFAULT_WARMUP,
    limit_seconds: float = LIMIT_SECONDS,
    timer: Callable[[], float] = time.perf_counter,
) -> Dict[str, Dict[str, Any]]:
    """Run every variant `warmup` times (not counted), then `runs` counted rounds, interleaved across variants.
    A variant whose warm-up or counted run exceeds `limit_seconds` gets exactly the runs already done (at least one)."""
    names = list(variants)
    out = {n: {"seconds": [], "warmup_seconds": [], "result": None, "limited": False} for n in names}  # type: Dict[str, Dict[str, Any]]
    for _ in range(warmup):
        for name in names:
            elapsed, result = time_call(variants[name], timer)
            out[name]["warmup_seconds"].append(elapsed)
            out[name]["result"] = result
            if elapsed > limit_seconds:
                out[name]["limited"] = True
    for _ in range(runs):
        for name in names:
            if out[name]["limited"] and out[name]["seconds"]:
                continue
            elapsed, result = time_call(variants[name], timer)
            out[name]["seconds"].append(elapsed)
            out[name]["result"] = result
            if elapsed > limit_seconds:
                out[name]["limited"] = True
    return out


# ---------- pipeline_run_<date>.md ----------

_SECTION = re.compile(r"^##\s+(\S+)\s+—\s+(.*)$")


def parse_pipeline_run_md(text: str) -> List[Dict[str, Any]]:
    """Rows of every step table in a pipeline_run_<date>.md written by pipeline.batch_pipeline.write_run_report."""
    entries = []  # type: List[Dict[str, Any]]
    pipeline = None  # type: Optional[str]
    for line in text.splitlines():
        match = _SECTION.match(line.strip())
        if match:
            pipeline = match.group(1)
            continue
        if pipeline and line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 3:
                continue
            try:
                seconds = float(cells[1])
            except ValueError:
                continue  # header and separator rows
            entries.append({"pipeline": pipeline, "step": cells[0], "seconds": seconds, "status": cells[2]})
    return entries


def read_pipeline_runs(reports_dir: Path) -> List[Dict[str, Any]]:
    """Entries of all generated logs. A log that says its table was written by hand ("ghi tay") is skipped."""
    entries = []  # type: List[Dict[str, Any]]
    for path in sorted(Path(reports_dir).glob("pipeline_run_*.md")):
        text = path.read_text(encoding="utf-8")
        if "ghi tay" in text:
            continue
        entries.extend(parse_pipeline_run_md(text))
    return entries


def pipeline_step_rows(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped = {}  # type: Dict[Tuple[str, str], List[float]]
    for entry in entries:
        if entry["status"] != "ok":
            continue
        grouped.setdefault((entry["pipeline"], entry["step"]), []).append(entry["seconds"])
    rows = []
    for (pipeline, step), seconds in grouped.items():
        note = NOTE_SINGLE_RUN if len(seconds) == 1 else "{} lần ghi nhận trong nhật ký pipeline".format(len(seconds))
        rows.append(make_row("pipeline_steps", "{}/{}".format(pipeline, step), STEP_QUERY, seconds, note=note))
    return rows


# ---------- HD9 rows ----------

def make_row(
    experiment: str,
    variant: str,
    query: str,
    seconds: Sequence[float],
    size_bytes: Optional[int] = None,
    file_count: Optional[int] = None,
    input_partitions: Optional[int] = None,
    executors: Optional[int] = None,
    rows: Optional[int] = None,
    note: str = "",
) -> Dict[str, Any]:
    stats = summarize_runs(seconds)
    return {
        "experiment": experiment, "variant": variant, "query": query, "runs": int(stats["runs"]),
        "median_seconds": stats["median"], "min_seconds": stats["min"], "max_seconds": stats["max"],
        "size_bytes": size_bytes, "file_count": file_count, "input_partitions": input_partitions,
        "executors": executors, "rows": rows, "note": note,
    }


def validate_rows(rows: Sequence[Dict[str, Any]]) -> None:
    seen = set()
    for row in rows:
        key = tuple(row[f] for f in KEY_FIELDS)
        if key in seen:
            raise ValueError("duplicate benchmark key {}".format(key))
        seen.add(key)


def rows_to_dataframe(spark: SparkSession, rows: Sequence[Dict[str, Any]]) -> DataFrame:
    validate_rows(rows)
    return spark.createDataFrame([tuple(row[f] for f in ROW_FIELDS) for row in rows], BENCHMARK_SCHEMA)


def environment_to_dataframe(spark: SparkSession, items: Sequence[Tuple[str, str]]) -> DataFrame:
    if len({item for item, _ in items}) != len(items):
        raise ValueError("duplicate environment item")
    return spark.createDataFrame([(str(i), str(v)) for i, v in items], ENVIRONMENT_SCHEMA)


# ---------- Spark / HDFS helpers ----------

def _fs(spark: SparkSession, path: str) -> Tuple[Any, Any]:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path, hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())


def path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_path, fs = _fs(spark, path)
    return bool(fs.exists(hadoop_path))


def size_and_files(spark: SparkSession, paths: Sequence[str]) -> Tuple[int, int]:
    """(bytes, file count) of the data files under `paths`, ignoring _SUCCESS/.crc style files; replication not applied."""
    total, files = 0, 0
    for path in paths:
        hadoop_path, fs = _fs(spark, path)
        iterator = fs.listFiles(hadoop_path, True)
        while iterator.hasNext():
            status = iterator.next()
            if status.getPath().getName().startswith(("_", ".")):
                continue
            total += int(status.getLen())
            files += 1
    return total, files


def scan_partitions(df: DataFrame) -> int:
    return int(df.rdd.getNumPartitions())


def executor_count(spark: SparkSession) -> int:
    """Executors that registered (the driver is not counted)."""
    infos = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
    return max(len(infos) - 1, 0)


def csv_dataframe(spark: SparkSession, paths: Sequence[str]) -> DataFrame:
    """Bronze CSV the way the ETL reads it: header, no inferSchema."""
    return spark.read.option("header", "true").csv(list(paths))


def silver_dataframe(spark: SparkSession, silver_path: str, start_date: str, end_date: str) -> DataFrame:
    return spark.read.parquet(silver_path).where(F.col("date").between(start_date, end_date))


def run_query(df: DataFrame) -> Dict[str, int]:
    """The benchmark query: count() plus one groupBy(model)."""
    rows = df.count()
    groups = len(df.groupBy("model").count().collect())
    return {"rows": rows, "groups": groups}


def ensure_parquet_all_columns(spark: SparkSession, csv_paths: Sequence[str], out_path: str) -> bool:
    """Parquet copy of the CSV days with ALL columns, to isolate the format effect. Returns True when it
    was created, False when it already existed (reused, never overwritten or deleted)."""
    if path_exists(spark, out_path):
        if not path_exists(spark, out_path.rstrip("/") + "/_SUCCESS"):
            raise RuntimeError("{} exists without _SUCCESS (partial write?); not touching it, ask the owner".format(out_path))
        return False
    csv_dataframe(spark, csv_paths).write.mode("errorifexists").parquet(out_path)
    return True


def ensure_features_coalesced(spark: SparkSession, features_path: str, out_path: str) -> bool:
    """Copy of Gold features with one file per date partition. Reused if present, never overwritten."""
    if path_exists(spark, out_path):
        if not path_exists(spark, out_path.rstrip("/") + "/_SUCCESS"):
            raise RuntimeError("{} exists without _SUCCESS (partial write?); not touching it, ask the owner".format(out_path))
        return False
    spark.read.parquet(features_path).repartition("date").write.mode("errorifexists").partitionBy("date").parquet(out_path)
    return True


# ---------- guard against other Spark applications ----------

def active_apps_from_json(master_json: Dict[str, Any]) -> List[str]:
    return [str(app.get("name", app.get("id", "?"))) for app in master_json.get("activeapps", [])]


def fetch_master_state(master_http: str, timeout: float = 10.0) -> Dict[str, Any]:
    with urllib.request.urlopen(master_http.rstrip("/") + "/json/", timeout=timeout) as response:  # noqa: S310 - internal URL
        return json.loads(response.read().decode("utf-8"))


def assert_no_other_apps(master_state: Dict[str, Any]) -> None:
    apps = active_apps_from_json(master_state)
    if apps:
        raise RuntimeError("Spark master already runs application(s): {}. Benchmark must run alone.".format(", ".join(apps)))


# ---------- raw records -> HD9 ----------

def raw_record(
    experiment: str, variant: str, query: str, measured: Dict[str, Any], meta: Dict[str, Any],
    executors: Optional[int], valid: bool = True, note: str = "",
) -> Dict[str, Any]:
    seconds = measured["seconds"]
    notes = [note] if note else []
    if len(seconds) == 1:
        notes.append(NOTE_SINGLE_RUN)
    elif measured["limited"]:
        notes.append("một lần chạy vượt 10 phút nên dừng sớm")
    result = measured.get("result") or {}
    return {
        "experiment": experiment, "variant": variant, "query": query, "seconds": seconds,
        "warmup_seconds": measured["warmup_seconds"], "limited": bool(measured["limited"]),
        "size_bytes": meta.get("size_bytes"), "file_count": meta.get("file_count"),
        "input_partitions": meta.get("input_partitions"), "executors": executors,
        "rows": result.get("rows"), "valid": valid, "note": "; ".join(notes),
    }


def append_jsonl(raw_dir: Path, experiment: str, record: Dict[str, Any]) -> Path:
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / (experiment + ".jsonl")
    with open(str(path), "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def load_raw_records(raw_dir: Path) -> List[Dict[str, Any]]:
    """All valid records; when a (experiment, variant, query) appears more than once the LAST line wins."""
    latest = {}  # type: Dict[Tuple[str, str, str], Dict[str, Any]]
    for path in sorted(Path(raw_dir).glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            key = tuple(record[f] for f in KEY_FIELDS)
            latest[key] = record  # type: ignore[index]
    return [r for r in latest.values() if r.get("valid", True)]


def rows_from_records(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for r in records:
        rows.append(make_row(r["experiment"], r["variant"], r["query"], r["seconds"], r.get("size_bytes"), r.get("file_count"),
                             r.get("input_partitions"), r.get("executors"), r.get("rows"), r.get("note", "")))
    return rows


def check_same_rows(rows: Sequence[Dict[str, Any]], variants: Sequence[str]) -> None:
    """Variants that must read the same data have to report the same row count."""
    counts = {r["variant"]: r["rows"] for r in rows if r["variant"] in variants and r["rows"] is not None}
    if len(set(counts.values())) > 1:
        raise ValueError("variants read different row counts: {}".format(counts))


def sort_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    order = {name: i for i, name in enumerate(EXPERIMENT_ORDER)}
    return sorted(rows, key=lambda r: (order.get(r["experiment"], 99), r["variant"], r["query"]))


# ---------- report ----------

def _fmt_bytes(value: Optional[int]) -> str:
    if value is None:
        return "—"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return "{:.1f} {}".format(size, unit) if unit != "B" else "{:.0f} B".format(size)
        size /= 1024.0
    return "{:.1f} GB".format(size)


def _fmt(value: Any, spec: str = "{}") -> str:
    return "—" if value is None else spec.format(value)


def ratio_lines(rows: Sequence[Dict[str, Any]]) -> List[str]:
    """Plain-language ratios computed from the medians (only when both variants were measured)."""
    by = {(r["experiment"], r["variant"], r["query"]): r for r in rows}
    lines = []
    pairs = [
        ("format", "csv_7d", "parquet_all_columns_7d", QUERY_NAME, "Parquet 197 cột nhanh gấp {:.1f} lần CSV (cùng 7 ngày)"),
        ("format", "csv_7d", "silver_parquet_7d", QUERY_NAME, "Silver Parquet nhanh gấp {:.1f} lần CSV (7 ngày)"),
        ("format", "csv_q1", "silver_parquet_q1", QUERY_NAME, "Silver Parquet nhanh gấp {:.1f} lần CSV (toàn Q1)"),
        ("small_files", "features_as_is", "features_coalesced", QUERY_NAME, "Bản gộp file nhanh gấp {:.2f} lần bản gốc"),
    ]
    for experiment, slow, fast, query, template in pairs:
        a, b = by.get((experiment, slow, query)), by.get((experiment, fast, query))
        if a and b and b["median_seconds"] > 0:
            lines.append(template.format(a["median_seconds"] / b["median_seconds"]))
    return lines


def environment_items(
    host: Dict[str, str], spark_env: Dict[str, str], records: Sequence[Dict[str, Any]], run_date: str
) -> List[Tuple[str, str]]:
    """Rows of `benchmark_environment`: host facts, Spark/cluster facts, input sizes, protocol. Keys are unique."""
    merged = {"run_date": run_date}  # type: Dict[str, str]
    merged.update({str(k): str(v) for k, v in sorted(host.items())})
    merged.update({str(k): str(v) for k, v in sorted(spark_env.items())})
    sizes = {r["variant"]: r["size_bytes"] for r in records if r.get("size_bytes") is not None}
    merged["input_sizes"] = "; ".join("{}={}".format(v, _fmt_bytes(b)) for v, b in sorted(sizes.items())) or "—"
    merged["measurement_protocol"] = ("1 lần khởi động không tính + {} lần tính trung vị, xen kẽ giữa các biến thể, {}; "
                                      "một lần chạy >10 phút thì chỉ chạy 1 lần; nhóm workers không xen kẽ (mỗi cấu hình một spark-submit)".format(DEFAULT_RUNS, CACHE_NOTE))
    return list(merged.items())


def format_report_md(rows: Sequence[Dict[str, Any]], environment: Sequence[Tuple[str, str]], run_date: str) -> str:
    lines = ["# Benchmark Big Data — {}\n".format(run_date)]
    lines.append("Truy vấn đo: `{}` (đọc CSV có header, không `inferSchema`). Mỗi biến thể: 1 lần khởi động không tính, rồi "
                 "3 lần tính trung vị (kèm min/max); các biến thể xen kẽ (riêng nhóm workers mỗi cấu hình là một lần spark-submit "
                 "riêng nên không xen kẽ); {}. Một lần chạy quá 10 phút thì chỉ chạy 1 lần "
                 "và ghi rõ ở cột ghi chú. Nhóm `pipeline_steps` lấy từ nhật ký đã có, không chạy lại.\n".format(QUERY_NAME, CACHE_NOTE))
    ratios = ratio_lines(rows)
    if ratios:
        lines.append("Tóm tắt (tính từ trung vị): " + "; ".join(ratios) + ".\n")
    for experiment in EXPERIMENT_ORDER:
        part = [r for r in rows if r["experiment"] == experiment]
        if not part:
            continue
        lines.append("## {}\n".format(EXPERIMENT_TITLES[experiment]))
        lines.append("| Biến thể | Truy vấn | Lần | Trung vị (s) | Min (s) | Max (s) | Dung lượng | Số file | Partition | Executor | Số dòng | Ghi chú |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
        for r in part:
            lines.append("| {} | {} | {} | {:.2f} | {:.2f} | {:.2f} | {} | {} | {} | {} | {} | {} |".format(
                r["variant"], r["query"], r["runs"], r["median_seconds"], r["min_seconds"], r["max_seconds"],
                _fmt_bytes(r["size_bytes"]), _fmt(r["file_count"], "{:,}"), _fmt(r["input_partitions"]),
                _fmt(r["executors"]), _fmt(r["rows"], "{:,}"), r["note"] or "—"))
        lines.append("")
    lines.append("## Cấu hình máy và cụm\n")
    lines.append("| Mục | Giá trị |")
    lines.append("|---|---|")
    for item, value in environment:
        lines.append("| {} | {} |".format(item, value))
    return "\n".join(lines) + "\n"


def build_tables(
    spark: SparkSession,
    raw_dir: Path,
    reports_dir: Path,
    export_dir: Path,
    environment: Sequence[Tuple[str, str]],
    run_date: str,
) -> Dict[str, Any]:
    """raw jsonl + pipeline logs -> artifacts/reports/benchmark.md and the two HD9 Parquet tables for the dashboard."""
    records = load_raw_records(raw_dir)
    rows = rows_from_records(records) + pipeline_step_rows(read_pipeline_runs(reports_dir))
    check_same_rows(rows, ["csv_7d", "parquet_all_columns_7d", "silver_parquet_7d"])
    rows = sort_rows(rows)
    validate_rows(rows)

    export_dir = Path(export_dir)
    counts = {}
    for name, df in (("benchmark", rows_to_dataframe(spark, rows)),
                     ("benchmark_environment", environment_to_dataframe(spark, environment))):
        uri = (export_dir / (name + ".parquet")).as_uri()
        df.coalesce(1).write.mode("overwrite").parquet(uri)
        counts[name] = spark.read.parquet(uri).count()
    report = Path(reports_dir) / "benchmark.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(format_report_md(rows, environment, run_date), encoding="utf-8")
    return {"tables": counts, "report": str(report), "rows": len(rows)}
