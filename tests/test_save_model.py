from __future__ import annotations

import json

from pyspark.ml import PipelineModel

from ml.evaluate import extract_risk_score
from ml.save_model import RULES_VERSION, bundle_files, save_model_bundle, write_text
from ml.train import train_model

COLUMNS = ["serial_number", "date", "f_small", "f_big", "fail_within_7_days"]


def _fixture(spark):
    rows = [("S{}".format(i), "2026-01-01", float(i % 3), float(i * 10), int(i % 4 == 0)) for i in range(40)]
    return spark.createDataFrame(rows, COLUMNS)


def test_bundle_files_have_the_shape_of_the_official_model_files():
    files = bundle_files(["a", "b"], "demo-v1", "logistic_regression", 100, 7, {"logistic_regression": {"pr_auc": 0.1}})

    assert set(files) == {"feature_list.json", "threshold.json", "metrics.json"}
    assert json.loads(files["feature_list.json"]) == {"features": ["a", "b"], "horizon_days": 7, "rules_version": RULES_VERSION}
    assert json.loads(files["threshold.json"]) == {"k": 100, "model_version": "demo-v1", "model_type": "logistic_regression"}
    metrics = json.loads(files["metrics.json"])
    assert metrics["model_version"] == "demo-v1" and metrics["chosen_model"] == "logistic_regression" and metrics["val"]["logistic_regression"]["pr_auc"] == 0.1


def test_without_metrics_there_is_no_metrics_file():
    assert "metrics.json" not in bundle_files(["a"], "v", "logistic_regression", 100, 7)


def test_write_text_creates_a_readable_file_and_overwrites_it(spark, tmp_path):
    path = (tmp_path / "note.json").as_uri()

    write_text(spark, path, '{"first": 1}')
    write_text(spark, path, '{"second": "Việt"}')

    assert json.loads((tmp_path / "note.json").read_text(encoding="utf-8")) == {"second": "Việt"}


def test_saved_model_loads_back_and_scores_the_same(spark, tmp_path):
    df = _fixture(spark)
    cols = ["f_small", "f_big"]
    model = train_model(df, cols, "logistic_regression")
    path = (tmp_path / "model").as_uri()

    written = save_model_bundle(spark, model, cols, path, "demo-v1", "logistic_regression", 100, 7, {"x": 1})
    loaded = PipelineModel.load(path)

    assert sorted(written) == ["feature_list.json", "metrics.json", "threshold.json"]
    assert json.loads((tmp_path / "model" / "feature_list.json").read_text(encoding="utf-8"))["features"] == cols
    before = sorted((r["serial_number"], round(r["risk_score"], 9)) for r in extract_risk_score(model.transform(df)).collect())
    after = sorted((r["serial_number"], round(r["risk_score"], 9)) for r in extract_risk_score(loaded.transform(df)).collect())
    assert before == after


def test_saving_twice_overwrites_instead_of_failing(spark, tmp_path):
    df = _fixture(spark)
    model = train_model(df, ["f_small", "f_big"], "logistic_regression")
    path = (tmp_path / "model").as_uri()

    save_model_bundle(spark, model, ["f_small", "f_big"], path, "v1", "logistic_regression", 100, 7)
    save_model_bundle(spark, model, ["f_small", "f_big"], path, "v2", "logistic_regression", 100, 7)

    assert json.loads((tmp_path / "model" / "threshold.json").read_text(encoding="utf-8"))["model_version"] == "v2"
