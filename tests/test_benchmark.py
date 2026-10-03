from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from analytics import benchmark as bm

ROOT = Path(__file__).resolve().parent.parent


# ---------- statistics ----------

def test_median_odd_even_single_and_empty():
    assert bm.median([3.0, 1.0, 2.0]) == 2.0
    assert bm.median([4.0, 1.0, 2.0, 3.0]) == 2.5
    assert bm.median([7.5]) == 7.5
    with pytest.raises(ValueError):
        bm.median([])


def test_summarize_runs_reports_median_min_max_and_count():
    assert bm.summarize_runs([10.0, 30.0, 20.0]) == {"runs": 3, "median": 20.0, "min": 10.0, "max": 30.0}


# ---------- timing protocol with a fake clock ----------

class FakeClock:
    """Each call of a variant advances the clock by that variant's duration; records the call order."""

    def __init__(self, durations):
        self.now = 0.0
        self.durations = durations
        self.calls = []

    def timer(self):
        return self.now

    def variant(self, name):
        def run():
            self.calls.append(name)
            index = sum(1 for c in self.calls if c == name) - 1
            self.now += self.durations[name][min(index, len(self.durations[name]) - 1)]
            return {"rows": 10}
        return run


def test_measure_variants_interleaves_and_does_not_count_the_warmup():
    clock = FakeClock({"A": [100.0, 1.0, 2.0, 3.0], "B": [50.0, 10.0, 20.0, 30.0]})

    out = bm.measure_variants({"A": clock.variant("A"), "B": clock.variant("B")}, runs=3, warmup=1, timer=clock.timer)

    assert clock.calls == ["A", "B", "A", "B", "A", "B", "A", "B"]  # warm-up round, then 3 interleaved rounds
    assert out["A"]["warmup_seconds"] == [100.0] and out["A"]["seconds"] == [1.0, 2.0, 3.0]
    assert out["B"]["seconds"] == [10.0, 20.0, 30.0]
    assert bm.median(out["A"]["seconds"]) == 2.0  # the 100 s warm-up is not in the median
    assert out["A"]["result"] == {"rows": 10}


def test_a_warmup_over_the_limit_gives_exactly_one_counted_run():
    clock = FakeClock({"slow": [700.0, 650.0, 640.0, 630.0], "fast": [1.0]})

    out = bm.measure_variants({"slow": clock.variant("slow"), "fast": clock.variant("fast")},
                              runs=3, warmup=1, limit_seconds=600.0, timer=clock.timer)

    assert out["slow"]["limited"] is True and out["slow"]["seconds"] == [650.0]
    assert len(out["fast"]["seconds"]) == 3  # the other variant still gets all its runs


def test_a_counted_run_over_the_limit_stops_further_runs_of_that_variant():
    clock = FakeClock({"x": [100.0, 400.0, 650.0, 500.0, 500.0]})

    out = bm.measure_variants({"x": clock.variant("x")}, runs=3, warmup=1, limit_seconds=600.0, timer=clock.timer)

    assert out["x"]["seconds"] == [400.0, 650.0] and out["x"]["limited"] is True  # stopped after the 650 s run


def test_raw_record_notes_a_single_run_and_an_early_stop():
    single = bm.raw_record("format", "csv_q1", bm.QUERY_NAME,
                           {"seconds": [700.0], "warmup_seconds": [710.0], "limited": True, "result": {"rows": 5}},
                           {"size_bytes": 10}, executors=2)
    early = bm.raw_record("format", "csv_q1", bm.QUERY_NAME,
                          {"seconds": [400.0, 650.0], "warmup_seconds": [1.0], "limited": True, "result": {"rows": 5}}, {}, executors=2)

    assert bm.NOTE_SINGLE_RUN in single["note"] and single["rows"] == 5 and single["size_bytes"] == 10
    assert "vượt 10 phút" in early["note"] and bm.NOTE_SINGLE_RUN not in early["note"]


# ---------- pipeline_run_<date>.md ----------

PIPELINE_MD = """# Nhật ký chạy pipeline — 2026-10-02


## scoring_pipeline — thành công

| Bước | Thời gian (giây) | Trạng thái | Dòng vào | Dòng ra |
|---|---|---|---|---|
| score | 37.91 | ok | không áp dụng | 342,662 |

## batch_pipeline — thành công

| Bước | Thời gian (giây) | Trạng thái | Dòng vào | Dòng ra |
|---|---|---|---|---|
| silver_etl | 269.35 | ok | 30,597,484 | 30,597,484 |
| analytics | 408.99 | ok | không áp dụng | 118 |

## scoring_pipeline — thành công

| Bước | Thời gian (giây) | Trạng thái | Dòng vào | Dòng ra |
|---|---|---|---|---|
| score | 27.63 | ok | không áp dụng | 342,662 |

## training_pipeline — THẤT BẠI tại bước t

| Bước | Thời gian (giây) | Trạng thái | Dòng vào | Dòng ra |
|---|---|---|---|---|
| t | 5.0 | lỗi: boom | không áp dụng | không áp dụng |
"""


def test_parse_pipeline_run_md_reads_every_step_row():
    entries = bm.parse_pipeline_run_md(PIPELINE_MD)

    assert [(e["pipeline"], e["step"], e["seconds"], e["status"]) for e in entries] == [
        ("scoring_pipeline", "score", 37.91, "ok"), ("batch_pipeline", "silver_etl", 269.35, "ok"),
        ("batch_pipeline", "analytics", 408.99, "ok"), ("scoring_pipeline", "score", 27.63, "ok"),
        ("training_pipeline", "t", 5.0, "lỗi: boom"),
    ]


def test_pipeline_step_rows_group_repeated_steps_and_skip_failures():
    rows = {r["variant"]: r for r in bm.pipeline_step_rows(bm.parse_pipeline_run_md(PIPELINE_MD))}

    assert set(rows) == {"scoring_pipeline/score", "batch_pipeline/silver_etl", "batch_pipeline/analytics"}  # failed step dropped
    score = rows["scoring_pipeline/score"]
    assert score["runs"] == 2 and score["median_seconds"] == pytest.approx(32.77) and "2 lần" in score["note"]
    silver = rows["batch_pipeline/silver_etl"]
    assert silver["runs"] == 1 and silver["median_seconds"] == 269.35 and bm.NOTE_SINGLE_RUN in silver["note"]
    assert silver["experiment"] == "pipeline_steps" and silver["query"] == bm.STEP_QUERY and silver["executors"] is None


def test_read_pipeline_runs_skips_logs_written_by_hand(tmp_path):
    (tmp_path / "pipeline_run_2026-10-01.md").write_text(PIPELINE_MD + "\nBảng được ghi tay từ lần chạy thật.\n", encoding="utf-8")
    (tmp_path / "pipeline_run_2026-10-02.md").write_text(PIPELINE_MD.replace("37.91", "40.00"), encoding="utf-8")

    entries = bm.read_pipeline_runs(tmp_path)

    assert sorted(e["seconds"] for e in entries if e["step"] == "score") == [27.63, 40.0]  # only the generated log is read


# ---------- HD9 rows ----------

def _row(experiment="format", variant="csv_7d", query=bm.QUERY_NAME, seconds=(3.0, 1.0, 2.0), rows=10):
    return bm.make_row(experiment, variant, query, list(seconds), size_bytes=100, file_count=2, input_partitions=4, executors=2, rows=rows)


def test_rows_to_dataframe_matches_the_hd9_schema(spark):
    df = bm.rows_to_dataframe(spark, [_row(), _row(variant="parquet_all_columns_7d", seconds=(0.5,))])

    assert df.columns == list(bm.ROW_FIELDS)
    assert dict(df.dtypes) == {
        "experiment": "string", "variant": "string", "query": "string", "runs": "int", "median_seconds": "double",
        "min_seconds": "double", "max_seconds": "double", "size_bytes": "bigint", "file_count": "bigint",
        "input_partitions": "int", "executors": "int", "rows": "bigint", "note": "string",
    }
    first = df.where("variant = 'csv_7d'").collect()[0]
    assert (first["runs"], first["median_seconds"], first["min_seconds"], first["max_seconds"]) == (3, 2.0, 1.0, 3.0)


def test_duplicate_keys_are_rejected_and_null_columns_are_allowed(spark):
    with pytest.raises(ValueError):
        bm.validate_rows([_row(), _row()])
    pipeline_row = bm.make_row("pipeline_steps", "b/x", bm.STEP_QUERY, [1.0])
    df = bm.rows_to_dataframe(spark, [pipeline_row])
    assert df.collect()[0]["size_bytes"] is None and df.collect()[0]["executors"] is None


def test_environment_table_has_unique_items(spark):
    items = bm.environment_items({"host_ram_gb": "15.2"}, {"spark_version": "3.5.1"},
                                 [{"variant": "csv_7d", "size_bytes": 2048}], "2026-10-03")
    df = bm.environment_to_dataframe(spark, items)

    assert df.columns == ["item", "value"]
    keys = [i for i, _ in items]
    assert len(keys) == len(set(keys)) and {"run_date", "host_ram_gb", "spark_version", "input_sizes", "measurement_protocol"} <= set(keys)
    assert dict(items)["input_sizes"] == "csv_7d=2.0 KB"
    with pytest.raises(ValueError):
        bm.environment_to_dataframe(spark, [("a", "1"), ("a", "2")])


# ---------- raw records -> report ----------

def _record(variant, seconds, rows=10, valid=True, experiment="format", query=bm.QUERY_NAME):
    return {"experiment": experiment, "variant": variant, "query": query, "seconds": seconds, "warmup_seconds": [9.0],
            "limited": False, "size_bytes": 1000, "file_count": 3, "input_partitions": 4, "executors": 2, "rows": rows,
            "valid": valid, "note": ""}


def test_load_raw_records_last_line_wins_and_invalid_are_dropped(tmp_path):
    bm.append_jsonl(tmp_path, "format", _record("csv_7d", [10.0, 11.0, 12.0]))
    bm.append_jsonl(tmp_path, "format", _record("csv_7d", [1.0, 2.0, 3.0]))  # a re-run: replaces the earlier line
    bm.append_jsonl(tmp_path, "workers", _record("cores_2_executors_1", [5.0], valid=False, experiment="workers"))

    records = bm.load_raw_records(tmp_path)

    assert [(r["variant"], r["seconds"]) for r in records] == [("csv_7d", [1.0, 2.0, 3.0])]


def test_check_same_rows_flags_variants_that_read_different_data():
    rows = [_row(variant="csv_7d", rows=10), _row(variant="silver_parquet_7d", rows=10)]
    bm.check_same_rows(rows, ["csv_7d", "silver_parquet_7d"])

    with pytest.raises(ValueError):
        bm.check_same_rows([_row(variant="csv_7d", rows=10), _row(variant="silver_parquet_7d", rows=11)], ["csv_7d", "silver_parquet_7d"])


def test_ratio_lines_and_report_markdown_show_medians_environment_and_protocol():
    rows = bm.sort_rows([
        _row(variant="csv_7d", seconds=(40.0, 41.0, 39.0)),
        _row(variant="parquet_all_columns_7d", seconds=(8.0, 9.0, 8.5)),
        bm.make_row("small_files", "features_as_is", bm.QUERY_NAME, [10.0], note=bm.NOTE_SINGLE_RUN),
        bm.make_row("pipeline_steps", "batch_pipeline/silver_etl", bm.STEP_QUERY, [269.35], note=bm.NOTE_SINGLE_RUN),
    ])

    assert bm.ratio_lines(rows) == ["Parquet 197 cột nhanh gấp 4.7 lần CSV (cùng 7 ngày)"]
    md = bm.format_report_md(rows, [("host_ram_gb", "15.2"), ("docker_ncpu", "12")], "2026-10-03")

    assert "# Benchmark Big Data — 2026-10-03" in md
    assert "| csv_7d | count+groupBy(model) | 3 | 40.00 | 39.00 | 41.00 |" in md
    assert "Parquet 197 cột nhanh gấp 4.7 lần CSV" in md
    assert "| host_ram_gb | 15.2 |" in md and "| docker_ncpu | 12 |" in md
    assert "1 lần khởi động không tính" in md and "bộ nhớ đệm" in md
    assert md.index("Định dạng") < md.index("File nhỏ") < md.index("Thời gian từng bước pipeline")  # experiment order
    assert bm.NOTE_SINGLE_RUN in md


def test_build_tables_writes_markdown_and_both_dashboard_tables(spark, tmp_path):
    raw, reports, export = tmp_path / "raw", tmp_path / "reports", tmp_path / "dashboard"
    reports.mkdir()
    bm.append_jsonl(raw, "format", _record("csv_7d", [4.0, 5.0, 6.0]))
    bm.append_jsonl(raw, "format", _record("silver_parquet_7d", [1.0, 1.0, 1.0]))
    (reports / "pipeline_run_2026-10-02.md").write_text(PIPELINE_MD, encoding="utf-8")
    env = bm.environment_items({"host_ram_gb": "15.2"}, {"spark_version": "3.5.1"}, bm.load_raw_records(raw), "2026-10-03")

    stats = bm.build_tables(spark, raw, reports, export, env, "2026-10-03")

    assert stats["rows"] == 2 + 3  # two measured variants + three distinct pipeline steps
    assert stats["tables"]["benchmark"] == 5 and stats["tables"]["benchmark_environment"] == len(env)
    assert "csv_7d" in (reports / "benchmark.md").read_text(encoding="utf-8")
    assert spark.read.parquet((export / "benchmark.parquet").as_uri()).where("experiment = 'format'").count() == 2


# ---------- Spark helpers on small fixtures ----------

COLUMNS = ["date", "serial_number", "model", "failure", "smart_5_raw"]


def _write_csv_days(spark, folder, days=3):
    paths = []
    for d in range(1, days + 1):
        date = "2026-01-0{}".format(d)
        rows = [(date, "S{}".format(i), "ModelA" if i % 3 else "ModelB", 0, i) for i in range(6)]
        path = (folder / "{}.csv".format(date)).as_uri()
        spark.createDataFrame(rows, COLUMNS).coalesce(1).write.option("header", "true").csv(path)
        paths.append(path)
    return paths


def test_run_query_gives_the_same_answer_for_csv_and_parquet(spark, tmp_path):
    csv_paths = _write_csv_days(spark, tmp_path)
    parquet_path = (tmp_path / "all_columns").as_uri()

    created = bm.ensure_parquet_all_columns(spark, csv_paths, parquet_path)
    from_csv = bm.run_query(bm.csv_dataframe(spark, csv_paths))
    from_parquet = bm.run_query(spark.read.parquet(parquet_path))

    assert created is True
    assert from_csv == from_parquet == {"rows": 18, "groups": 2}
    assert bm.scan_partitions(bm.csv_dataframe(spark, csv_paths)) >= 1


def test_ensure_helpers_reuse_existing_output_and_refuse_a_partial_one(spark, tmp_path):
    csv_paths = _write_csv_days(spark, tmp_path, days=2)
    out = (tmp_path / "copy").as_uri()
    assert bm.ensure_parquet_all_columns(spark, csv_paths, out) is True
    size_before = bm.size_and_files(spark, [out])

    assert bm.ensure_parquet_all_columns(spark, csv_paths, out) is False  # reused, not rewritten
    assert bm.size_and_files(spark, [out]) == size_before

    partial = tmp_path / "partial"
    partial.mkdir()
    (partial / "part-0.parquet").write_text("x", encoding="utf-8")  # a directory without _SUCCESS
    with pytest.raises(RuntimeError):
        bm.ensure_parquet_all_columns(spark, csv_paths, partial.as_uri())
    assert (partial / "part-0.parquet").exists()  # never deleted


def test_ensure_features_coalesced_gives_one_file_per_partition(spark, tmp_path):
    rows = [("2026-01-0{}".format(d), "S{}".format(i), "M", i) for d in (1, 2, 3) for i in range(30)]
    source = (tmp_path / "features").as_uri()
    spark.createDataFrame(rows, "date string, serial_number string, model string, x int").repartition(9).write.partitionBy("date").parquet(source)
    _, files_before = bm.size_and_files(spark, [source])
    out = (tmp_path / "features_coalesced").as_uri()

    assert bm.ensure_features_coalesced(spark, source, out) is True
    size_after, files_after = bm.size_and_files(spark, [out])

    assert files_before > 3 and files_after == 3 and size_after > 0  # _SUCCESS/.crc files are not counted
    assert bm.run_query(spark.read.parquet(out)) == bm.run_query(spark.read.parquet(source))


def test_executor_count_is_zero_in_local_mode(spark):
    assert bm.executor_count(spark) == 0


# ---------- guard against other Spark applications ----------

def test_assert_no_other_apps_stops_when_the_master_runs_an_application():
    bm.assert_no_other_apps({"activeapps": []})
    bm.assert_no_other_apps({})

    with pytest.raises(RuntimeError) as error:
        bm.assert_no_other_apps({"activeapps": [{"name": "smart-kmeans-segmentation", "id": "app-1"}]})

    assert "smart-kmeans-segmentation" in str(error.value)
    assert bm.active_apps_from_json({"activeapps": [{"id": "app-2"}]}) == ["app-2"]


# ---------- host environment collector (standard library only) ----------

def test_benchmark_env_parses_key_value_lines_and_never_needs_pyspark():
    spec = importlib.util.spec_from_file_location("benchmark_env", str(ROOT / "scripts" / "benchmark_env.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    parsed = module.parse_key_values("host_ram_gb=15.2\nnoise line\nhost_cpu=AMD Ryzen 5 6600H\n=bad\n")

    assert parsed == {"host_ram_gb": "15.2", "host_cpu": "AMD Ryzen 5 6600H"}
    source = (ROOT / "scripts" / "benchmark_env.py").read_text(encoding="utf-8")
    assert "pyspark" not in source


def test_benchmark_tmp_path_comes_from_config_and_stays_under_smart_drive():
    from config.settings import hdfs_uri, load_config

    config = load_config()

    assert config["hdfs"]["benchmark_tmp"] == "/smart-drive/benchmark_tmp"
    assert hdfs_uri(config, "benchmark_tmp").endswith("/smart-drive/benchmark_tmp")
    assert json.dumps(config)  # the config stays serializable
