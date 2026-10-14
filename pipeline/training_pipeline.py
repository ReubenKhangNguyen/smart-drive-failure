"""Train both ML baselines and score latest drives with the selected model."""

from __future__ import annotations

import json

from config.settings import ProjectPaths
from ml.score import run as score
from ml.train import run as train

def run(paths: ProjectPaths) -> dict[str, object]:
    """Train, select by PR-AUC and publish a model-driven prediction export."""
    metrics = train(f"{paths.gold}/features", paths.models, "artifacts/metrics/model_metrics.json")
    selected = metrics["selected_model"]
    predictions = score(f"{paths.gold}/features_scoring", f"{paths.models}/{selected}", f"{paths.gold}/predictions", paths.dashboard_dir)
    return {"selected_model": selected, "predictions": predictions, "metrics": metrics}

if __name__ == "__main__":
    print(json.dumps(run(ProjectPaths()), indent=2))
