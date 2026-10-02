from __future__ import annotations

from typing import Any, Dict, List

from config.settings import hdfs_uri
from pipeline.batch_pipeline import PipelineStep


def build_steps(spark, config: Dict[str, Any], score_date: str) -> List[PipelineStep]:
    from ml.score import run as run_score

    features_path = hdfs_uri(config, "features")
    model_path = "{}/{}".format(hdfs_uri(config, "models_base"), config["ml"]["model_version"])
    predictions_path = hdfs_uri(config, "predictions")

    def step_score() -> Dict[str, Any]:
        return run_score(
            spark,
            features_path,
            model_path,
            predictions_path,
            score_date,
            config["ml"]["k"],
            config["ml"]["model_version"],
        )

    return [("score", step_score)]
