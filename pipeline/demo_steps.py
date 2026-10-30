"""The nine course steps, one function per demo task. Each function narrates what it does (ctx.say), calls the REAL
project code (never a copy of it) with the demo config, and returns the numbers it measured.

Spark and the heavy modules are imported inside the functions, so this file imports without Spark (the pure parts
are unit-tested in the `tests` container; the steps themselves are verified by running the demo DAG).
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from pipeline import demo_evidence as evidence
from pipeline import webhdfs
from pipeline.demo_spec import DEMO_ROOT, DEMO_SOURCE_DIR, KAFKA_BOOTSTRAP, DemoStep


class StepContext:
    """What a step may use: the demo config, the evidence folder, a printer and a lazily created Spark session."""

    def __init__(self, config: Dict[str, Any], artifacts: Path, spec: DemoStep, run_id: str,
                 on_update: Optional[Callable[["StepContext"], None]] = None) -> None:
        self.config, self.artifacts, self.spec, self.run_id = config, Path(artifacts), spec, run_id
        self.lines: List[str] = []
        self.metrics: Dict[str, Any] = {}
        self._spark: Any = None
        self._on_update = on_update

    def say(self, text: str = "") -> None:
        print(text, flush=True)
        self.lines.append(text)
        if self._on_update:
            self._on_update(self)

    def table(self, headers: List[str], rows: List[List[Any]]) -> None:
        for line in evidence.format_table(headers, rows):
            self.say("    " + line)

    @property
    def web(self) -> str:
        return self.config["hdfs"]["namenode_web_url"]

    def uri(self, key: str) -> str:
        from config.settings import hdfs_uri
        return hdfs_uri(self.config, key)

    def spark(self) -> Any:
        if self._spark is None:
            from pyspark.sql import SparkSession
            self._spark = SparkSession.builder.appName("smart-demo-" + self.spec.task_id).getOrCreate()
        return self._spark

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None


def _mb(value: float) -> str:
    return "{:,.1f} MB".format(value / 1e6)


def _gb(value: float) -> str:
    return "{:,.1f} GB".format(value / 1e9)


def _guard(path: str) -> str:
    """Every path a demo step writes must be inside the demo area; a mistake here must never reach the real data."""
    if not (path == DEMO_ROOT or path.startswith(DEMO_ROOT + "/")):
        raise RuntimeError("Từ chối ghi ngoài vùng demo: {}".format(path))
    return path


def _tree(web: str, path: str, depth: int, prefix: str = "") -> List[str]:
    lines = []
    entries = sorted(webhdfs.list_status(web, path), key=lambda e: e["pathSuffix"])
    dirs = [e for e in entries if e.get("type") == "DIRECTORY"]
    files = [e for e in entries if e.get("type") == "FILE"]
    for entry in dirs:
        lines.append("{}{}/".format(prefix, entry["pathSuffix"]))
        if depth > 1:
            lines.extend(_tree(web, path.rstrip("/") + "/" + entry["pathSuffix"], depth - 1, prefix + "  "))
    if files:
        lines.append("{}({} file, {})".format(prefix, len(files), _mb(sum(int(f["length"]) for f in files))))
    return lines


# ------------------------------------------------------------------------------------------------ step 1
def step_prepare_hdfs(ctx: StepContext) -> Dict[str, Any]:
    hdfs = ctx.config["hdfs"]
    ctx.say("MỤC TIÊU: dựng chỗ chứa dữ liệu trên HDFS theo ba lớp Bronze (thô) - Silver (sạch) - Gold (kết quả).")
    ctx.say("CHUẨN BỊ: cụm HDFS phải đang chạy. Hỏi NameNode xem có bao nhiêu DataNode còn sống...")
    state = webhdfs.namenode_state(ctx.web)
    free, total = state["capacity_remaining"], state["capacity_total"]
    ctx.say("  NameNode báo {} DataNode đang sống, dung lượng trống {} trên tổng {}".format(state["live_datanodes"], _gb(free), _gb(total)))
    ctx.say("  (lưu ý: 3 DataNode chạy trên cùng một ổ đĩa của máy này nên số dung lượng là tổng cộng dồn, không phải 3 ổ riêng)")
    if state["live_datanodes"] < 1:
        raise RuntimeError("Không có DataNode nào sống: chạy 'docker compose up -d' rồi thử lại.")
    ctx.say("")
    ctx.say("LÀM: tạo cấu trúc thư mục (chỉ trong vùng demo {}, không đụng dữ liệu thật /smart-drive):".format(DEMO_ROOT))
    import posixpath
    targets = [hdfs["bronze"], posixpath.dirname(hdfs["silver"]), posixpath.dirname(hdfs["features"]), hdfs["models_base"]]
    notes = {hdfs["bronze"]: "Bronze: CSV thô, giữ nguyên, không sửa", posixpath.dirname(hdfs["silver"]): "Silver: Parquet đã làm sạch",
             posixpath.dirname(hdfs["features"]): "Gold: bảng phân tích, đặc trưng, dự đoán", hdfs["models_base"]: "Mô hình đã lưu"}
    for path in targets:
        ok = webhdfs.mkdirs(ctx.web, _guard(path))
        ctx.say("  {}  {}  ({})".format("OK " if ok else "LỖI", path, notes[path]))
        if not ok:
            raise RuntimeError("MKDIRS thất bại: " + path)
    ctx.say("")
    ctx.say("KẾT QUẢ: cây thư mục vùng demo hiện tại:")
    for line in _tree(ctx.web, DEMO_ROOT, 3):
        ctx.say("    " + line)
    ctx.say("")
    ctx.say("ĐẦU RA cho bước 2: các thư mục rỗng đúng chỗ; bước 2 sẽ nạp CSV vào " + hdfs["bronze"])
    return {"live_datanodes": state["live_datanodes"], "capacity_free_gb": round(free / 1e9, 1),
            "capacity_total_gb": round(total / 1e9, 1), "dirs_created": len(targets), "bronze_path": hdfs["bronze"]}


# ------------------------------------------------------------------------------------------------ step 2
def step_upload(ctx: StepContext) -> Dict[str, Any]:
    from ingestion.bronze_verifier import check_listing, describe, expected_dates
    from ingestion.dataset_validator import validate_source_dir

    cfg = ctx.config
    start, end = cfg["data"]["start_date"], cfg["data"]["end_date"]
    source = Path(os.environ.get("DEMO_SOURCE_DIR", DEMO_SOURCE_DIR))
    ctx.say("MỤC TIÊU: đưa CSV Backblaze từ đĩa máy vào HDFS Bronze, nguyên bản, và kiểm tra đã đủ.")
    ctx.say("CHUẨN BỊ: thư mục CSV phải được mount vào container (docker-compose.yml) tại " + str(source))
    if not source.is_dir():
        raise RuntimeError("Không thấy thư mục dữ liệu {}: kiểm tra mount /external_data_all trong docker-compose.yml".format(source))
    ctx.say("")
    ctx.say("LÀM 1: kiểm tra nguồn trước khi nạp: đủ ngày {} đến {} và mỗi file có đủ cột bắt buộc.".format(start, end))
    report = validate_source_dir(source, start, end, cfg["data"]["required_columns"])
    ctx.say("  tìm thấy {} file CSV, thiếu {} ngày, {} file sai cột".format(report.total_files, len(report.missing_dates), len(report.bad_header_files)))
    if not report.is_valid:
        raise RuntimeError("Nguồn không hợp lệ: thiếu ngày {} / sai cột {}".format(report.missing_dates[:5], list(report.bad_header_files)[:3]))
    dest = _guard(cfg["hdfs"]["bronze"])
    existing = webhdfs.file_sizes(ctx.web, dest)
    ctx.say("")
    ctx.say("LÀM 2: nạp từng file qua WebHDFS (replication 1: file gốc luôn tải lại được). File đã có cùng kích thước thì bỏ qua (chạy lại không nhân đôi).")
    uploaded = skipped = 0
    total_bytes = 0
    began = time.time()
    for date in report.dates_found:
        local = source / (date + ".csv")
        size = local.stat().st_size
        total_bytes += size
        if existing.get(date + ".csv") == size:
            skipped += 1
            ctx.say("  {}  {}  bỏ qua (đã có đúng kích thước)".format(date, _mb(size)))
            continue
        t0 = time.time()
        webhdfs.put_file(ctx.web, "{}/{}.csv".format(dest, date), local, replication=cfg["hdfs"].get("bronze_replication", 1))
        uploaded += 1
        seconds = max(time.time() - t0, 0.001)
        ctx.say("  {}  {}  đã nạp trong {:.1f}s ({:.0f} MB/s)".format(date, _mb(size), seconds, size / 1e6 / seconds))
    seconds_all = time.time() - began
    ctx.say("")
    ctx.say("LÀM 3: kiểm kê lại trên HDFS (không tin lệnh nạp, đo lại):")
    sizes = webhdfs.file_sizes(ctx.web, dest)
    check = check_listing(sizes, expected_dates(start, end))
    on_hdfs = sum(sizes.get("{}.csv".format(d), 0) for d in report.dates_found)
    ctx.say("  " + describe(check))
    ctx.say("  dung lượng trên HDFS {} so với đĩa máy {}: {}".format(_mb(on_hdfs), _mb(total_bytes), "KHỚP TỪNG BYTE" if on_hdfs == total_bytes else "LỆCH"))
    if check.missing or check.empty or on_hdfs != total_bytes:
        raise RuntimeError("Kiểm kê Bronze không khớp: thiếu {} / rỗng {} / lệch {} byte".format(check.missing[:3], check.empty[:3], on_hdfs - total_bytes))
    ctx.say("")
    ctx.say("ĐẦU RA cho bước 3: {} file CSV nguyên bản ({}) trong {}".format(len(report.dates_found), _mb(on_hdfs), dest))
    return {"files": len(report.dates_found), "bytes": on_hdfs, "uploaded": uploaded, "skipped": skipped,
            "seconds": round(seconds_all, 1), "byte_match": True, "columns_checked": len(cfg["data"]["required_columns"]), "dest": dest}


# ------------------------------------------------------------------------------------------------ step 3
def step_read_spark(ctx: StepContext) -> Dict[str, Any]:
    spark = ctx.spark()
    bronze = ctx.uri("bronze")
    ctx.say("MỤC TIÊU: cho Spark đọc CSV trực tiếp từ HDFS, chia việc ra nhiều partition để chạy song song.")
    ctx.say("CHUẨN BỊ: Spark Master + 2 worker đang chạy; Bronze đã có CSV (bước 2).")
    ctx.say("")
    ctx.say("LÀM: spark.read.option('header','true').csv(...)  (KHÔNG bật inferSchema).")
    t0 = time.time()
    df = spark.read.option("header", "true").csv(bronze)
    ctx.say("  dòng lệnh đọc chạy trong {:.1f}s: Spark mới liệt kê file trên HDFS và đọc dòng tiêu đề để biết tên cột, CHƯA đọc dữ liệu (lazy: phép biến đổi chỉ ghi kế hoạch)".format(time.time() - t0))
    columns, types = df.columns, sorted({t for _, t in df.dtypes})
    partitions = df.rdd.getNumPartitions()
    ctx.say("  lược đồ: {} cột, kiểu dữ liệu của mọi cột: {} (Spark chưa đoán kiểu; bước 4 sẽ ép kiểu tường minh)".format(len(columns), ", ".join(types)))
    ctx.say("  Spark chia đầu vào thành {} partition (mỗi partition khoảng một khối HDFS 128 MB), 2 worker xử lý song song".format(partitions))
    t1 = time.time()
    rows = df.count()
    count_seconds = time.time() - t1
    ctx.say("  count() là phép hành động nên lúc này Spark mới thật sự đọc: {:,} dòng trong {:.1f}s".format(rows, count_seconds))
    ctx.say("")
    ctx.say("3 dòng mẫu, mỗi dòng là một ổ-ngày (chọn dòng có đủ giá trị SMART; mọi giá trị vẫn là chuỗi):")
    sample = df.where("smart_5_raw is not null and smart_197_raw is not null").select("date", "serial_number", "model", "failure", "smart_5_raw", "smart_197_raw").limit(3).collect()
    ctx.table(["date", "serial_number", "model", "failure", "smart_5_raw", "smart_197_raw"], [[r[c] for c in ("date", "serial_number", "model", "failure", "smart_5_raw", "smart_197_raw")] for r in sample])
    ctx.say("")
    ctx.say("VÌ SAO TẮT inferSchema? thử đo trên MỘT file (đã chạy nóng một lần để công bằng):")
    one = "{}/{}.csv".format(bronze, ctx.config["data"]["start_date"])
    spark.read.option("header", "true").csv(one).count()
    t2 = time.time()
    spark.read.option("header", "true").csv(one).count()
    plain = time.time() - t2
    t3 = time.time()
    inferred = spark.read.option("header", "true").option("inferSchema", "true").csv(one)
    inferred.count()
    infer = time.time() - t3
    ctx.say("  không inferSchema: {:.1f}s   |   inferSchema=true: {:.1f}s (phải quét dữ liệu thêm một lượt để đoán kiểu)".format(plain, infer))
    ctx.say("  Số đo một file nhỏ chỉ để minh họa; điểm chính là inferSchema tốn thêm một lượt quét và có thể đoán sai kiểu.")
    ctx.say("")
    ctx.say("ĐẦU RA cho bước 4: một DataFrame {} cột, toàn chuỗi, {:,} dòng.".format(len(columns), rows))
    return {"rows": rows, "columns": len(columns), "partitions": partitions, "count_seconds": round(count_seconds, 1),
            "one_file_plain_s": round(plain, 2), "one_file_infer_s": round(infer, 2)}


# ------------------------------------------------------------------------------------------------ step 4
def step_clean(ctx: StepContext) -> Dict[str, Any]:
    from pyspark.sql import functions as F

    from processing.spark_jobs.smart_etl import run as run_etl

    spark = ctx.spark()
    ctx.say("MỤC TIÊU: biến CSV thô thành bảng Silver đáng tin: đúng kiểu, không trùng, chuẩn hóa, lưu Parquet.")
    ctx.say("CHUẨN BỊ: DataFrame thô của bước 3 (Spark đọc lại từ Bronze).")
    ctx.say("")
    ctx.say("LÀM (processing/spark_jobs/smart_etl.py):")
    for text in ("  1. chọn 13 cột cần dùng trong 197 cột",
                 "  2. ép kiểu tường minh: ngày -> date, failure -> int, capacity -> bigint, 8 cột SMART -> double",
                 "  3. dung lượng âm -> null; giá trị khuyết GIỮ NGUYÊN null (thiếu khác với bằng 0)",
                 "  4. suy ra hãng sản xuất từ tiền tố tên model",
                 "  5. loại dòng trùng khóa (serial_number, date) bằng hàm cửa sổ",
                 "  6. ghi Parquet phân vùng theo ngày (ghi đè phân vùng động: chạy lại không xóa ngày khác)"):
        ctx.say(text)
    stats = run_etl(spark, ctx.uri("bronze"), ctx.uri("silver"))
    ctx.say("")
    ctx.say("KẾT QUẢ: dòng vào {:,} -> dòng ra {:,}, trùng bị loại {:,}, mất {}s".format(stats["rows_in"], stats["rows_out"], stats["duplicates_removed"], stats["elapsed_seconds"]))
    silver = spark.read.parquet(ctx.uri("silver"))
    core = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"]
    agg = silver.agg(
        F.countDistinct("date").alias("days"), F.sum("failure").alias("failures"),
        F.sum(F.when(F.col("smart_5_raw") < 0, 1).otherwise(0)).alias("negative_smart_5"),
        *[F.avg(F.col(c).isNull().cast("int")).alias(c) for c in core]).collect()[0]
    dup = silver.groupBy("serial_number", "date").count().where("count > 1").count()
    bronze_bytes = webhdfs.content_summary(ctx.web, ctx.config["hdfs"]["bronze"])["length"]
    silver_bytes = webhdfs.content_summary(ctx.web, ctx.config["hdfs"]["silver"])["length"]
    ctx.say("KIỂM TRA CHẤT LƯỢNG Silver:")
    ctx.say("  {} phân vùng ngày, {:,} lượt hỏng, khóa (serial, ngày) trùng: {}, smart_5 âm: {}".format(agg["days"], agg["failures"], dup, agg["negative_smart_5"]))
    ctx.say("  tỷ lệ null của 4 chỉ số lõi (null giữ nguyên, không điền 0):")
    ctx.table(["cột", "tỷ lệ null"], [[c, "{:.2%}".format(agg[c])] for c in core])
    ratio = bronze_bytes / silver_bytes if silver_bytes else 0
    ctx.say("  dung lượng: Bronze CSV {} -> Silver Parquet {} (nhỏ hơn {:.0f} lần nhờ lưu theo cột, nén và bỏ 184 cột không dùng)".format(_mb(bronze_bytes), _mb(silver_bytes), ratio))
    ctx.say("")
    ctx.say("ĐẦU RA cho bước 5 và 6: Silver {:,} dòng, khóa (serial_number, date), {}".format(stats["rows_out"], ctx.config["hdfs"]["silver"]))
    return {"rows_in": stats["rows_in"], "rows_out": stats["rows_out"], "duplicates_removed": stats["duplicates_removed"], "days": int(agg["days"]),
            "failures": int(agg["failures"]), "duplicate_keys_after": dup, "bronze_mb": round(bronze_bytes / 1e6, 1), "silver_mb": round(silver_bytes / 1e6, 1),
            "size_ratio": round(ratio, 1), "etl_seconds": stats["elapsed_seconds"]}


# ------------------------------------------------------------------------------------------------ step 5
def step_sql(ctx: StepContext) -> Dict[str, Any]:
    from analytics.build_analytics import run as run_analytics
    from analytics.health_status import run as run_health

    spark = ctx.spark()
    smart_columns = [c for c in ctx.config["data"]["required_columns"] if c.startswith("smart_")]
    analytics_uri = ctx.uri("analytics")
    ctx.say("MỤC TIÊU: trả lời câu hỏi nghiệp vụ bằng truy vấn SQL trên Silver: ổ hãng nào hỏng nhiều, chỉ số nào báo trước.")
    ctx.say("CHUẨN BỊ: Silver (bước 4). Spark SQL đăng ký Silver thành 'view tạm' rồi truy vấn như một bảng.")
    ctx.say("")
    ctx.say("LÀM 1 (analytics/build_analytics.py, spark.sql): AFR theo hãng/model, phân phối chỉ số, tín hiệu trước khi hỏng.")
    ctx.say("  công thức AFR = số lượt hỏng / (số ổ-ngày / 365)  (quy về tỷ lệ hỏng mỗi năm)")
    stats = run_analytics(spark, ctx.uri("silver"), analytics_uri, smart_columns)
    ctx.say("  ghi {} bảng Gold nhỏ: {}".format(len(stats["tables"]), ", ".join("{} ({} dòng)".format(k, v) for k, v in stats["tables"].items())))
    afr = spark.read.parquet(analytics_uri + "/afr_by_manufacturer").orderBy("afr", ascending=False).collect()
    ctx.say("")
    ctx.say("KẾT QUẢ 1: AFR theo hãng (mẫu demo 28 ngày, chỉ minh họa cách tính):")
    ctx.table(["hãng", "ổ-ngày", "lượt hỏng", "AFR"], [[r["manufacturer"], "{:,}".format(r["drive_days"]), r["failures"], "{:.2%}".format(r["afr"] or 0)] for r in afr])
    signal = spark.read.parquet(analytics_uri + "/pre_failure_signal").where("window_days = 7").orderBy("signal_rate", ascending=False).collect()
    ctx.say("")
    ctx.say("KẾT QUẢ 2: tỷ lệ ổ hỏng có chỉ số DƯƠNG trong 7 ngày trước khi hỏng (chỉ số nào báo trước?):")
    ctx.table(["chỉ số", "số ổ hỏng", "có tín hiệu", "tỷ lệ"], [[r["smart_attribute"], r["failed_serials"], r["signalled_serials"], "{:.1%}".format(r["signal_rate"])] for r in signal])
    ctx.say("  smart_9 (giờ chạy) và smart_194 (nhiệt độ) gần như luôn dương nên không phân biệt được; 4 chỉ số lõi là smart_5, 187, 197, 198.")
    ctx.say("")
    ctx.say("LÀM 2 (analytics/health_status.py): luật rules_v1 xếp mỗi ổ-ngày vào HEALTHY / WATCH / CRITICAL từ 4 chỉ số lõi.")
    health = run_health(spark, ctx.uri("silver"), ctx.uri("health_status"))
    total = sum(health["distribution"].values())
    ctx.say("KẾT QUẢ 3: phân bố mức tình trạng và tỷ lệ hỏng thật trong 7 ngày sau:")
    rows = []
    for level in ("HEALTHY", "WATCH", "CRITICAL"):
        base = health["baseline_failure_rate_7d"].get(level, {})
        rows.append([level, "{:,}".format(health["distribution"].get(level, 0)), "{:.2%}".format(health["distribution"].get(level, 0) / total if total else 0),
                     "{:.4%}".format(base.get("failure_rate") or 0)])
    ctx.table(["mức", "ổ-ngày", "tỷ lệ", "hỏng trong 7 ngày sau"], rows)
    ctx.say("")
    ctx.say("ĐẦU RA: bảng Gold phân tích ({}) và health_status ({:,} dòng) cho bước 6 và 7.".format(analytics_uri.split("/", 3)[-1], total))
    return {"analytics_tables": stats["tables"], "analytics_seconds": stats["elapsed_seconds"], "health_rows": total,
            "distribution": health["distribution"], "afr_manufacturers": len(afr)}


# ------------------------------------------------------------------------------------------------ step 6a
def step_kmeans(ctx: StepContext) -> Dict[str, Any]:
    from analytics.kmeans_segmentation import run as run_kmeans
    from config.settings import artifacts_dir

    spark = ctx.spark()
    km, seed = ctx.config["kmeans"], ctx.config["project"]["random_seed"]
    ctx.say("MỤC TIÊU: nhóm các ổ theo hành vi S.M.A.R.T. để MÔ TẢ (không dùng để dự đoán, mã cụm không làm đặc trưng).")
    ctx.say("CHUẨN BỊ: Silver và health_status (bước 4, 5).")
    ctx.say("")
    ctx.say("LÀM: K-Means trên 4 chỉ số lõi, thử K từ {} đến {}, chọn K có hệ số silhouette lớn nhất.".format(km["k_min"], km["k_max"]))
    ctx.say("  Ổ có cả 4 chỉ số bằng 0 (đa số) được tách riêng thành nhóm 'zero_signal', nếu không chúng chi phối mọi cụm.")
    stats = run_kmeans(spark, ctx.uri("silver"), ctx.uri("health_status"), ctx.uri("analytics"), km["columns"], km["k_min"], km["k_max"], seed, km["max_iter"],
                       export_dir=artifacts_dir() / "dashboard", report_dir=artifacts_dir() / "reports")
    ks = spark.read.parquet(ctx.uri("analytics") + "/kmeans_k_selection").orderBy("k").collect()
    ctx.say("")
    ctx.say("KẾT QUẢ 1: chọn K bằng silhouette (càng gần 1 càng tách biệt):")
    ctx.table(["K", "silhouette", "được chọn"], [[r["k"], "{:.4f}".format(r["silhouette"]), "x" if r["is_selected"] else ""] for r in ks])
    clusters = spark.read.parquet(ctx.uri("analytics") + "/kmeans_clusters").orderBy("cluster_id").collect()
    ctx.say("KẾT QUẢ 2: các cụm và tỷ lệ ổ hỏng trong mẫu (mô tả hồi cứu, không phải xác suất hỏng):")
    ctx.table(["cụm", "loại", "số ổ", "hỏng", "tỷ lệ hỏng"], [[r["cluster_id"], r["cluster_type"], "{:,}".format(r["size"]), r["failed_count"], "{:.2%}".format(r["failed_rate"] or 0)] for r in clusters])
    ctx.say("")
    ctx.say("ĐẦU RA: 3 bảng HĐ7 (chọn K, cụm, đối chiếu với mức tình trạng) và báo cáo kmeans_segmentation.md.")
    chosen = [r["k"] for r in ks if r["is_selected"]]
    return {"chosen_k": chosen[0] if chosen else None, "clusters": len(clusters), "k_tried": len(ks), "elapsed_seconds": stats.get("elapsed_seconds")}


# ------------------------------------------------------------------------------------------------ step 6b
def step_features(ctx: StepContext) -> Dict[str, Any]:
    from pyspark.sql import functions as F

    from features.build_features import feature_columns
    from pipeline.batch_pipeline import build_steps, run_steps, select_steps

    spark = ctx.spark()
    data = ctx.config["data"]
    ctx.say("MỤC TIÊU: chuẩn bị dữ liệu cho mô hình dự đoán 'ổ hỏng trong 7 ngày tới': nhãn, đặc trưng và chia tập.")
    ctx.say("CHUẨN BỊ: Silver (bước 4).")
    ctx.say("")
    ctx.say("LÀM (features/label.py, features/build_features.py):")
    for text in ("  1. NHÃN fail_within_7_days = 1 nếu ổ hỏng trong (ngày, ngày + 7]",
                 "  2. loại 7 ngày cuối (chưa biết tương lai: right-censoring) và ổ biến mất không do hỏng",
                 "  3. loại giai đoạn khởi động {} -> {} (chưa đủ lịch sử cho cửa sổ)".format(data["start_date"], data["warmup_end_date"]),
                 "  4. ĐẶC TRƯNG: 8 giá trị SMART + 8 cờ 'thiếu dữ liệu' + 72 đặc trưng cửa sổ 7/14/30 ngày (max, delta, số ngày tăng) + 2 mã hóa nhóm = 90",
                 "     mỗi cửa sổ chỉ nhìn về QUÁ KHỨ, không bao giờ sang ngày sau (chống rò rỉ tương lai)",
                 "  5. CHIA TẬP THEO THỜI GIAN (không ngẫu nhiên): train đến {}, validation đến {}, phần còn lại là test".format(data["train_end_date"], data["val_end_date"])):
        ctx.say(text)
    result = run_steps(select_steps(build_steps(spark, ctx.config), ["features"]))
    if result["status"] != "ok":
        raise RuntimeError("Bước tạo đặc trưng lỗi: {}".format(result.get("steps")))
    info = result["steps"][0]
    features = spark.read.parquet(ctx.uri("features"))
    cols = feature_columns(features)
    per_split = {r["split"]: (r["rows"], r["positives"]) for r in features.groupBy("split").agg(F.count("*").alias("rows"), F.sum("fail_within_7_days").alias("positives")).collect()}
    ctx.say("")
    ctx.say("KẾT QUẢ: Silver {:,} dòng -> Gold features {:,} dòng, {} đặc trưng, mất {}s".format(info["result"]["rows_in"], info["result"]["rows_out"], len(cols), info["elapsed_seconds"]))
    rows = []
    for split in ("train", "val", "test"):
        n, p = per_split.get(split, (0, 0))
        rows.append([split, "{:,}".format(n), "{:,}".format(p or 0), "{:.3%}".format((p or 0) / n) if n else "-"])
    ctx.table(["tập", "số dòng", "nhãn dương", "tỷ lệ dương"], rows)
    ctx.say("  Nhãn dương RẤT HIẾM (cỡ vài phần mười nghìn): vì vậy không dùng accuracy mà dùng recall@K, và huấn luyện có trọng số lớp.")
    ctx.say("  Tập test của bản demo chỉ chứa ổ đã biết chắc hỏng (7 ngày cuối bị censoring) nên demo không đánh giá trên test.")
    ctx.say("")
    ctx.say("ĐẦU RA cho bước 6c: Gold features, khóa (serial_number, date), cột split = train / val / test.")
    return {"rows_in": info["result"]["rows_in"], "rows_out": info["result"]["rows_out"], "features": len(cols), "elapsed_seconds": info["elapsed_seconds"],
            "splits": {k: {"rows": v[0], "positives": int(v[1] or 0)} for k, v in per_split.items()}}


# ------------------------------------------------------------------------------------------------ step 6c
def step_train(ctx: StepContext) -> Dict[str, Any]:
    from pyspark.sql import functions as F

    from analytics.health_status import rule_risk_score
    from features.build_features import feature_columns
    from ml.evaluate import extract_risk_score, macro_average_at_k, pr_auc, recall_precision_at_k_by_day, roc_auc
    from ml.save_model import save_model_bundle
    from ml.train import train_model

    spark = ctx.spark()
    cfg = ctx.config
    k, version, horizon = cfg["ml"]["k"], cfg["ml"]["model_version"], cfg["project"]["horizon_days"]
    features = spark.read.parquet(ctx.uri("features"))
    cols = feature_columns(features)
    train_df, val_df = features.where(F.col("split") == "train"), features.where(F.col("split") == "val")
    pos = train_df.where("fail_within_7_days = 1").count()
    total = train_df.count()
    ctx.say("MỤC TIÊU: huấn luyện mô hình xếp hạng ổ theo nguy cơ hỏng trong 7 ngày, đo trên tập validation, rồi LƯU lại.")
    ctx.say("CHUẨN BỊ: Gold features (bước 6b) có tập train và validation.")
    ctx.say("")
    ctx.say("LÀM 1: Logistic Regression (Spark MLlib, ml/train.py) với trọng số lớp, vì lớp dương cực hiếm:")
    ctx.say("  train: {:,} dòng, {:,} nhãn dương ({:.3%})".format(total, pos, pos / total if total else 0))
    if pos:
        ctx.say("  trọng số: dòng dương = {:.0f}, dòng âm = {:.2f} (tỷ lệ nghịch với tần suất lớp, nếu không mô hình chỉ đoán 'không hỏng')".format(total / (2.0 * pos), total / (2.0 * (total - pos))))
    t0 = time.time()
    model = train_model(train_df, cols, "logistic_regression")
    ctx.say("  huấn luyện xong trong {:.0f}s".format(time.time() - t0))
    ctx.say("")
    ctx.say("LÀM 2: chấm điểm tập validation và đo recall@{0} theo TỪNG NGÀY (mỗi ngày chọn {0} ổ rủi ro nhất để kiểm tra) rồi lấy trung bình:".format(k))
    scored = extract_risk_score(model.transform(val_df))
    metrics = {"pr_auc": pr_auc(scored), "roc_auc": roc_auc(scored)}
    metrics.update(macro_average_at_k(recall_precision_at_k_by_day(scored, k=k)))
    rules = macro_average_at_k(recall_precision_at_k_by_day(rule_risk_score(scored), k=k, score_col="rule_risk_score"))
    ctx.say("KẾT QUẢ: mô hình so với luật rules_v1 (bước 5) trên cùng validation:")
    ctx.table(["phương pháp", "PR-AUC", "ROC-AUC", "recall@{}".format(k), "precision@{}".format(k)], [
        ["Logistic Regression", "{:.4f}".format(metrics["pr_auc"]), "{:.4f}".format(metrics["roc_auc"]), "{:.2%}".format(metrics["recall_at_k"] or 0), "{:.2%}".format(metrics["precision_at_k"] or 0)],
        ["Luật rules_v1", "-", "-", "{:.2%}".format(rules["recall_at_k"] or 0), "{:.2%}".format(rules["precision_at_k"] or 0)]])
    ctx.say("  Validation của demo chỉ có vài ngày nên con số dao động mạnh; số chính thức nằm ở lần chạy đầy đủ (báo cáo).")
    ctx.say("")
    path = "{}/{}".format(ctx.uri("models_base"), version)
    ctx.say("LÀM 3: lưu mô hình tại {} cùng feature_list.json, threshold.json, metrics.json".format(path.split("9000", 1)[-1]))
    written = save_model_bundle(spark, model, cols, path, version, "logistic_regression", k, horizon, {"logistic_regression": metrics, "baseline_rules_v1": rules})
    ctx.say("  đã lưu PipelineModel + {}".format(", ".join(sorted(written))))
    ctx.say("")
    ctx.say("ĐẦU RA cho bước 6d: mô hình '{}' (không ghi đè mô hình chính thức v20260930-1, nằm ở vùng khác).".format(version))
    return {"train_rows": total, "train_positives": pos, "model_version": version, "features": len(cols),
            "pr_auc": metrics["pr_auc"], "roc_auc": metrics["roc_auc"], "recall_at_k": metrics["recall_at_k"], "precision_at_k": metrics["precision_at_k"],
            "rules_recall_at_k": rules["recall_at_k"], "rules_precision_at_k": rules["precision_at_k"], "k": k}


# ------------------------------------------------------------------------------------------------ step 6d
def step_score(ctx: StepContext) -> Dict[str, Any]:
    from ml.score import run as run_score
    from ml.score_daily import run as run_daily, scorable_date_ranges

    spark = ctx.spark()
    cfg = ctx.config
    k, version, horizon = cfg["ml"]["k"], cfg["ml"]["model_version"], cfg["project"]["horizon_days"]
    model_path = "{}/{}".format(ctx.uri("models_base"), version)
    day = cfg["data"]["val_end_date"]
    ctx.say("MỤC TIÊU: dùng mô hình đã lưu để chấm điểm rủi ro cho từng ổ và chọn Top-{} ổ cần kiểm tra mỗi ngày.".format(k))
    ctx.say("CHUẨN BỊ: mô hình '{}' (bước 6c) và Gold features.".format(version))
    ctx.say("")
    ctx.say("LÀM 1: tải mô hình từ HDFS và chấm điểm MỘT ngày ({}), xếp hạng, đánh dấu Top-{} (ml/score.py).".format(day, k))
    stats = run_score(spark, ctx.uri("features"), model_path, ctx.uri("predictions"), day, k, version)
    ctx.say("  chấm {:,} ổ, {} ổ được cảnh báo (Top-{})".format(stats["rows_scored"], stats["alerts"], k))
    top = spark.read.parquet(ctx.uri("predictions")).where("date = '{}'".format(day)).orderBy("risk_rank").limit(5).collect()
    ctx.say("KẾT QUẢ 1: 5 ổ có điểm rủi ro cao nhất ngày {}:".format(day))
    ctx.table(["hạng", "serial", "model ổ", "điểm rủi ro"], [[r["risk_rank"], r["serial_number"], r["model"], "{:.6f}".format(r["risk_score"])] for r in top])
    ctx.say("  Điểm rủi ro là điểm XẾP HẠNG (có trọng số lớp), không phải xác suất hỏng; nhiều ổ bão hòa ở 1.0 nên khi hòa điểm xếp theo margin của mô hình.")
    ctx.say("")
    ranges = scorable_date_ranges(cfg["data"], horizon)
    ctx.say("LÀM 2: chấm điểm MỌI ngày có đủ 7 ngày tương lai ({}) để dashboard có ô chọn ngày; chỉ giữ Top-{} mỗi ngày (ml/score_daily.py).".format(
        ", ".join("{} đến {}".format(a, b) for a, b in ranges), k))
    daily = run_daily(spark, ctx.uri("features"), model_path, ctx.uri("predictions_daily_topk"), ctx.uri("scored_days"), ranges, k, version)
    ctx.say("  {} ngày đã chấm, {:,} dòng Top-K".format(daily["days"], daily["topk_rows"]))
    ctx.say("")
    ctx.say("ĐẦU RA cho bước 7: Gold predictions (một ngày), predictions_daily_topk và scored_days (mọi ngày).")
    return {"score_date": day, "rows_scored": stats["rows_scored"], "alerts": stats["alerts"], "days_scored": daily["days"], "topk_rows": daily["topk_rows"]}


# ------------------------------------------------------------------------------------------------ step 7
def step_save(ctx: StepContext) -> Dict[str, Any]:
    from analytics.export_dashboard import run as run_export
    from config.settings import artifacts_dir

    spark = ctx.spark()
    cfg = ctx.config
    version, k = cfg["ml"]["model_version"], cfg["ml"]["k"]
    ctx.say("MỤC TIÊU: lưu kết quả đúng nơi và đúng dạng để người dùng xem: Gold trên HDFS, và các bảng nhỏ cho bảng điều khiển.")
    ctx.say("CHUẨN BỊ: các bảng Gold của bước 5, 6 (analytics, health_status, features, predictions) và mô hình.")
    ctx.say("")
    ctx.say("LÀM: gom thành bảng nhỏ (Parquet một file) cho dashboard (analytics/export_dashboard.py). Dashboard chỉ đọc các bảng này,")
    ctx.say("  không đọc HDFS và không cần Spark chạy nên mở nhanh. Mỗi bảng có hợp đồng dữ liệu (cột, kiểu, khóa).")
    stats = run_export(
        spark, ctx.uri("silver"), ctx.uri("health_status"), ctx.uri("predictions"), ctx.uri("features"),
        "{}/{}".format(ctx.uri("models_base"), version), version, k, reports_dir=artifacts_dir() / "reports", export_dir=artifacts_dir() / "dashboard",
        daily_topk_path=ctx.uri("predictions_daily_topk"), scored_days_path=ctx.uri("scored_days"), analysis_end_date=cfg["data"]["end_date"], data_cfg=cfg["data"])
    ctx.say("")
    ctx.say("KẾT QUẢ 1: {} bảng xuất cho dashboard (dòng mỗi bảng):".format(len(stats["tables"])))
    ctx.table(["bảng", "số dòng"], [[name, "{:,}".format(rows)] for name, rows in sorted(stats["tables"].items())])
    for reason in stats.get("skipped", []):
        ctx.say("  (bỏ qua: {})".format(reason))
    ctx.say("")
    ctx.say("KẾT QUẢ 2: dữ liệu của từng lớp trên HDFS (đo lại bằng WebHDFS):")
    layers = [("Bronze (CSV thô)", cfg["hdfs"]["bronze"]), ("Silver (đã làm sạch)", cfg["hdfs"]["silver"]), ("Gold: đặc trưng", cfg["hdfs"]["features"]),
              ("Gold: phân tích", cfg["hdfs"]["analytics"]), ("Gold: tình trạng ổ", cfg["hdfs"]["health_status"]), ("Gold: dự đoán", cfg["hdfs"]["predictions"]),
              ("Mô hình", cfg["hdfs"]["models_base"])]
    rows, layer_metrics = [], {}
    for name, path in layers:
        summary = webhdfs.content_summary(ctx.web, path)
        rows.append([name, path.replace(DEMO_ROOT, "…"), summary["fileCount"], _mb(summary["length"])])
        layer_metrics[name] = {"files": summary["fileCount"], "mb": round(summary["length"] / 1e6, 1)}
    ctx.table(["lớp", "đường dẫn", "số file", "dung lượng"], rows)
    ctx.say("")
    ctx.say("ĐẦU RA: {} bảng trong {} (trang web đọc từ đây) và các bảng Gold trên HDFS.".format(len(stats["tables"]), artifacts_dir() / "dashboard"))
    return {"dashboard_tables": stats["tables"], "skipped": stats.get("skipped", []), "layers": layer_metrics, "elapsed_seconds": stats.get("elapsed_seconds")}


# ------------------------------------------------------------------------------------------------ step 8
def kafka_host_port(bootstrap: str):
    host, _, port = bootstrap.rpartition(":")
    return host, int(port)


def _require_kafka(ctx: StepContext) -> None:
    host, port = kafka_host_port(KAFKA_BOOTSTRAP)
    try:
        socket.create_connection((host, port), timeout=5).close()
    except OSError:
        raise RuntimeError("Không kết nối được Kafka tại {}:{}. Bật trước bằng: docker compose --profile streaming up -d zookeeper kafka".format(host, port))
    ctx.say("  Kafka trả lời tại {}:{}".format(host, port))


def step_kafka_produce(ctx: StepContext) -> Dict[str, Any]:
    cfg = ctx.config
    day = cfg["data"]["val_end_date"]
    source = Path(os.environ.get("DEMO_SOURCE_DIR", DEMO_SOURCE_DIR))
    ctx.say("MỤC TIÊU: mô phỏng dữ liệu SMART đổ về LIÊN TỤC: producer đọc dữ liệu một ngày và gửi từng dòng vào Kafka.")
    ctx.say("CHUẨN BỊ: Kafka + ZooKeeper phải đang chạy (profile streaming); thư mục CSV nguồn đã mount.")
    _require_kafka(ctx)
    ctx.say("")
    ctx.say("LÀM: producer gửi ngày {} vào một topic MỚI, theo lô 2.000 message mỗi 0,2 giây (để dữ liệu 'đổ về dần', không dồn một lượt).".format(day))
    ctx.say("  mỗi message chỉ gồm 13 cột consumer cần (~314 byte) thay vì cả 197 cột (~4.885 byte): 106 MB thay vì 1,65 GB cho một ngày.")
    target = "/tmp/kafka_py"
    try:
        import kafka  # noqa: F401
    except ImportError:
        ctx.say("  cài kafka-python vào {} (cần internet, một lần)...".format(target))
        subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--target", target, "kafka-python==3.0.11"], check=True)
        sys.path.insert(0, target)
    from kafka import KafkaProducer

    from ingestion.kafka_producer import DeliveryCounter, produce_dates

    topic = "smart-demo-{}".format(ctx.run_id)
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, linger_ms=20)
    counter = DeliveryCounter()
    send_raw = counter.wrap(producer, topic)
    milestone = {"next": 40000}

    def send(message: bytes) -> None:
        send_raw(message)
        if counter.sent >= milestone["next"]:
            ctx.say("  đã gửi {:,} message ({:.0f}s)".format(counter.sent, time.time() - began))
            milestone["next"] += 40000

    began = time.time()
    result = produce_dates(send, source, [day], batch_size=2000, batch_delay=0.2)
    producer.flush()
    seconds = time.time() - began
    ctx.say("")
    ctx.say("KẾT QUẢ: gửi {:,} message, broker xác nhận {:,}, lỗi {}, mất {:.0f}s".format(counter.sent, counter.delivered, counter.failed, seconds))
    if counter.failed or counter.delivered != counter.sent:
        raise RuntimeError("Producer: gửi {} / xác nhận {} / lỗi {}".format(counter.sent, counter.delivered, counter.failed))
    ctx.say("")
    ctx.say("ĐẦU RA cho bước 8b: topic '{}' chứa {:,} message của ngày {} (Kafka giữ lại cho đến khi consumer đọc).".format(topic, counter.sent, day))
    return {"topic": topic, "date": day, "sent": counter.sent, "delivered": counter.delivered, "failed": counter.failed, "seconds": round(seconds, 1), "per_date": result["per_date"]}


def step_kafka_consume(ctx: StepContext) -> Dict[str, Any]:
    from pipeline.streaming_consumer import ProgressTracker, compare_with_silver, start_stream, wait_until_idle

    spark = ctx.spark()
    produced = evidence.read_step(ctx.artifacts, "kafka_produce")
    if not produced or produced.get("status") != "ok":
        raise RuntimeError("Chưa có topic: bước kafka_produce phải thành công trước.")
    topic, day = produced["metrics"]["topic"], produced["metrics"]["date"]
    output = "{}/{}".format(ctx.uri("streaming_output"), ctx.run_id)
    checkpoint = "{}/{}".format(ctx.uri("streaming_checkpoint"), ctx.run_id)
    ctx.say("MỤC TIÊU: Spark Structured Streaming nhận message từ Kafka, làm sạch bằng CHÍNH hàm của bước 4 rồi ghi Parquet.")
    ctx.say("CHUẨN BỊ: topic '{}' (bước 8a) và gói spark-sql-kafka (tải khi spark-submit).".format(topic))
    ctx.say("")
    ctx.say("LÀM: Spark hỏi Kafka theo vòng lặp micro-batch (Kafka không đẩy, Spark kéo):")
    ctx.say("  hỏi offset mới nhất -> đọc khoảng chưa xử lý (tối đa 50.000 message) -> làm sạch -> ghi Parquet -> ghi offset vào checkpoint")
    ctx.say("  chỉ khi xử lý xong một lô thì offset mới được ghi nhận, nên làm tiếp đúng chỗ và không mất dữ liệu.")
    began = time.time()
    query = start_stream(spark, KAFKA_BOOTSTRAP, topic, output, checkpoint, 50000)
    tracker = ProgressTracker()
    reported = {"n": 0}

    def sleeper(seconds: float) -> None:
        tracker.update(list(query.recentProgress))
        batches = tracker.batches
        for batch in batches[reported["n"]:]:
            ctx.say("  micro-batch {}: {:,} dòng, xử lý {} ms".format(batch.get("batchId"), batch.get("numInputRows", 0), batch.get("triggerExecutionMs")))
        reported["n"] = len(batches)
        time.sleep(seconds)

    reason = wait_until_idle(lambda: list(query.recentProgress), 30.0, 900.0, is_active=lambda: query.isActive, tracker=tracker, sleep=sleeper)
    tracker.update(list(query.recentProgress))
    query.stop()
    out_rows = spark.read.parquet(output).count()
    seconds = time.time() - began
    ctx.say("")
    ctx.say("KẾT QUẢ 1: dừng vì '{}' (đã đọc xong và 30 giây không có thêm dữ liệu); {} micro-batch, đọc {:,} dòng, ghi {:,} dòng Parquet, mất {:.0f}s".format(
        reason, len(tracker.batches), tracker.total_input_rows, out_rows, seconds))
    comparison = compare_with_silver(spark, output, ctx.uri("silver"), day)
    ctx.say("KẾT QUẢ 2: đối chiếu với Silver cùng ngày {} (bằng chứng luồng làm đúng):".format(day))
    ctx.table(["chỉ số", "luồng Kafka", "Silver (batch)"], [[name, "{:,}".format(comparison["streamed"][name]), "{:,}".format(comparison["silver"][name])] for name in ("rows", "serials", "failures")])
    ctx.say("  {}".format("KHỚP: cùng logic làm sạch cho cùng kết quả dù chạy theo luồng hay theo lô." if comparison["match"] else "KHÔNG KHỚP"))
    if not comparison["match"]:
        raise RuntimeError("Luồng Kafka không khớp Silver: {}".format(comparison))
    ctx.say("")
    ctx.say("LƯU Ý: đây là lớp MINH HỌA luồng; batch (bước 2 đến 7) vẫn là đường chính, và luồng này mới làm sạch, chưa chấm điểm bằng mô hình.")
    return {"topic": topic, "micro_batches": len(tracker.batches), "input_rows": tracker.total_input_rows, "output_rows": out_rows,
            "stopped_because": reason, "seconds": round(seconds, 1), "silver_match": comparison["match"], "streamed": comparison["streamed"], "silver": comparison["silver"]}


# ------------------------------------------------------------------------------------------------ step 9
def step_finish(ctx: StepContext) -> Dict[str, Any]:
    from pipeline.demo_spec import DEMO_STEPS

    ctx.say("MỤC TIÊU: Airflow điều phối cả chuỗi: mỗi bước là một tác vụ, mũi tên là 'xong bước này mới chạy bước sau', bước lỗi thì dừng cả chuỗi.")
    ctx.say("")
    ctx.say("LUỒNG ĐÃ CHẠY, đầu ra của bước trước là đầu vào của bước sau:")
    done = evidence.read_all(ctx.artifacts)
    rows, total = [], 0.0
    for step in DEMO_STEPS:
        payload = done.get(step.task_id, {})
        seconds = payload.get("seconds")
        if step.task_id != "finish_summary":
            total += float(seconds or 0)
        rows.append([step.label, step.title, payload.get("status", "-") if step.task_id != "finish_summary" else "đang chạy", "{:.0f}s".format(seconds) if seconds else "-"])
    ctx.table(["bước", "việc", "trạng thái", "thời gian"], rows)
    failed = [s.task_id for s in DEMO_STEPS if s.task_id != "finish_summary" and done.get(s.task_id, {}).get("status") != "ok"]
    ctx.say("")
    ctx.say("TỔNG: {} bước thành công trên {}, tổng thời gian các bước {:.0f}s ({:.1f} phút)".format(len(DEMO_STEPS) - 1 - len(failed), len(DEMO_STEPS) - 1, total, total / 60))
    if failed:
        raise RuntimeError("Còn bước chưa thành công: {}".format(", ".join(failed)))
    ctx.say("")
    ctx.say("Các chốt an toàn của DAG: mỗi lần chỉ một tác vụ Spark (cụm nhỏ), không tự thử lại khi lỗi, mỗi tác vụ có giới hạn thời gian.")
    return {"steps_ok": len(DEMO_STEPS) - 1, "total_seconds": round(total, 1)}


STEP_FUNCTIONS: Dict[str, Callable[[StepContext], Dict[str, Any]]] = {
    "prepare_hdfs": step_prepare_hdfs, "upload_data": step_upload, "read_spark": step_read_spark, "clean_data": step_clean,
    "spark_sql": step_sql, "kmeans": step_kmeans, "build_features": step_features, "train_model": step_train, "score_days": step_score,
    "save_results": step_save, "kafka_produce": step_kafka_produce, "kafka_consume": step_kafka_consume, "finish_summary": step_finish,
}
