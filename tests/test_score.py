from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from ml.score import run, score_day

COLUMNS = ["date", "serial_number", "model", "f1", "f2", "fail_within_7_days"]


def _row(date, serial, f1, f2, label=0):
    return (date, serial, "ModelA", float(f1), float(f2), label)


def test_score_day_ranks_by_risk_and_flags_topk(spark):
    train_rows = [
        _row("2026-01-01", "SN_TRAIN1", 10.0, 1.0, 1),
        _row("2026-01-01", "SN_TRAIN2", 0.0, 0.0, 0),
        _row("2026-01-02", "SN_TRAIN3", 8.0, 1.0, 1),
        _row("2026-01-02", "SN_TRAIN4", 0.0, 0.0, 0),
    ]
    train_df = spark.createDataFrame(train_rows, COLUMNS)

    assembler = VectorAssembler(inputCols=["f1", "f2"], outputCol="features")
    classifier = LogisticRegression(featuresCol="features", labelCol="fail_within_7_days", maxIter=20)
    model = Pipeline(stages=[assembler, classifier]).fit(train_df)

    score_rows = [
        _row("2026-02-01", "SN_A", 9.0, 1.0),  # high risk, should rank first
        _row("2026-02-01", "SN_B", 0.1, 0.0),
        _row("2026-02-01", "SN_C", 0.0, 0.0),
    ]
    features_df = spark.createDataFrame(score_rows, COLUMNS)

    result = score_day(model, features_df, score_date="2026-02-01", k=1).collect()
    by_serial = {r["serial_number"]: r for r in result}

    assert by_serial["SN_A"]["risk_rank"] == 1
    assert by_serial["SN_A"]["alert"] is True
    assert by_serial["SN_B"]["alert"] is False
    assert by_serial["SN_C"]["alert"] is False
    assert {"date", "serial_number", "model", "risk_score", "risk_rank", "alert"} == set(result[0].asDict().keys())


def test_score_day_only_scores_the_requested_date(spark):
    train_rows = [_row("2026-01-01", "SN_TRAIN1", 1.0, 1.0, 1), _row("2026-01-01", "SN_TRAIN2", 0.0, 0.0, 0)]
    train_df = spark.createDataFrame(train_rows, COLUMNS)
    assembler = VectorAssembler(inputCols=["f1", "f2"], outputCol="features")
    classifier = LogisticRegression(featuresCol="features", labelCol="fail_within_7_days", maxIter=20)
    model = Pipeline(stages=[assembler, classifier]).fit(train_df)

    rows = [
        _row("2026-02-01", "SN_A", 1.0, 1.0),
        _row("2026-02-02", "SN_B", 1.0, 1.0),
    ]
    features_df = spark.createDataFrame(rows, COLUMNS)

    result = score_day(model, features_df, score_date="2026-02-01", k=10).collect()

    assert len(result) == 1
    assert result[0]["serial_number"] == "SN_A"


def test_run_keeps_previous_day_predictions_when_scoring_another_day(spark, tmp_path):
    train_rows = [_row("2026-01-01", "SN_TRAIN1", 1.0, 1.0, 1), _row("2026-01-01", "SN_TRAIN2", 0.0, 0.0, 0)]
    train_df = spark.createDataFrame(train_rows, COLUMNS)
    assembler = VectorAssembler(inputCols=["f1", "f2"], outputCol="features")
    classifier = LogisticRegression(featuresCol="features", labelCol="fail_within_7_days", maxIter=20)
    model = Pipeline(stages=[assembler, classifier]).fit(train_df)
    model_path = (tmp_path / "model").as_uri()
    model.save(model_path)

    features_path = (tmp_path / "features").as_uri()
    rows = [_row("2026-02-01", "SN_A", 1.0, 1.0), _row("2026-02-02", "SN_B", 1.0, 1.0)]
    spark.createDataFrame(rows, COLUMNS).write.partitionBy("date").parquet(features_path)

    predictions_path = (tmp_path / "predictions").as_uri()
    try:
        run(spark, features_path, model_path, predictions_path, "2026-02-01", 1, "vtest")
        run(spark, features_path, model_path, predictions_path, "2026-02-02", 1, "vtest")
        dates = sorted(str(r["date"]) for r in spark.read.parquet(predictions_path).select("date").distinct().collect())
    finally:
        spark.conf.unset("spark.sql.sources.partitionOverwriteMode")

    assert dates == ["2026-02-01", "2026-02-02"]


def test_score_day_breaks_saturated_ties_by_margin_before_serial(spark):
    # separable training data so that large feature values saturate the sigmoid at exactly 1.0
    train_rows = [_row("2026-01-01", "T%d" % i, 10.0 + i, 0.0, 1) for i in range(5)] + [
        _row("2026-01-01", "N%d" % i, float(i) / 10.0, 0.0, 0) for i in range(5)
    ]
    train_df = spark.createDataFrame(train_rows, COLUMNS)
    assembler = VectorAssembler(inputCols=["f1", "f2"], outputCol="features")
    classifier = LogisticRegression(featuresCol="features", labelCol="fail_within_7_days", maxIter=50)
    model = Pipeline(stages=[assembler, classifier]).fit(train_df)

    # SN_A sorts first alphabetically but has the LOWER margin; both saturate at risk_score == 1.0
    score_rows = [
        _row("2026-02-01", "SN_A", 1000.0, 0.0),
        _row("2026-02-01", "SN_Z", 2000.0, 0.0),
        _row("2026-02-01", "SN_LOW", 0.0, 0.0),
    ]
    features_df = spark.createDataFrame(score_rows, COLUMNS)

    result = score_day(model, features_df, score_date="2026-02-01", k=1)
    by_serial = {r["serial_number"]: r for r in result.collect()}

    assert by_serial["SN_A"]["risk_score"] == 1.0 and by_serial["SN_Z"]["risk_score"] == 1.0  # the tie precondition
    assert by_serial["SN_Z"]["risk_rank"] == 1 and by_serial["SN_A"]["risk_rank"] == 2 and by_serial["SN_LOW"]["risk_rank"] == 3
    assert by_serial["SN_Z"]["alert"] is True and by_serial["SN_A"]["alert"] is False


def test_score_day_keeps_hd4_columns_only_and_serial_order_when_margins_tie(spark):
    train_df = spark.createDataFrame(
        [_row("2026-01-01", "T1", 10.0, 0.0, 1), _row("2026-01-01", "T2", 9.0, 0.0, 1),
         _row("2026-01-01", "N1", 0.0, 0.0, 0), _row("2026-01-01", "N2", 0.1, 0.0, 0)], COLUMNS)
    model = Pipeline(stages=[VectorAssembler(inputCols=["f1", "f2"], outputCol="features"),
                             LogisticRegression(featuresCol="features", labelCol="fail_within_7_days", maxIter=30)]).fit(train_df)
    features_df = spark.createDataFrame(
        [_row("2026-02-01", "SN_B", 5.0, 0.0), _row("2026-02-01", "SN_A", 5.0, 0.0)], COLUMNS)

    result = score_day(model, features_df, score_date="2026-02-01", k=1)
    ranks = {r["serial_number"]: r["risk_rank"] for r in result.collect()}

    assert result.columns == ["date", "serial_number", "model", "risk_score", "risk_rank", "alert"]  # no margin column
    assert ranks == {"SN_A": 1, "SN_B": 2}  # identical score and margin -> serial_number decides
