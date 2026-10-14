"""Read-only API over compact JSON exports prepared by the Spark Gold job.

The service never reads raw Backblaze CSV files or HDFS directly.  Spark writes
small JSON documents into the shared dashboard artifact directory, and this API
validates and returns those documents to the React application.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


DATA_DIR = Path(os.getenv("DASHBOARD_DATA_DIR", "artifacts/dashboard"))
EXPORTS = {
    "overview": "overview.json",
    "failure_by_model": "failure_by_model.json",
    "failure_over_time": "failure_over_time.json",
    "smart_comparison": "smart_comparison.json",
    "drives": "drive_list.json",
    "predictions": "predictions.json",
    "benchmark": "benchmark.json",
}

app = FastAPI(title="SMART Drive Failure API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def read_export(name: str) -> Any:
    """Load one Gold export, or return a clear HTTP 503 while the job has not run."""
    path = DATA_DIR / EXPORTS[name]
    if not path.is_file():
        raise HTTPException(
            status_code=503,
            detail=f"Gold export '{path.name}' is not available. Run the batch pipeline first.",
        )
    try:
        with path.open(encoding="utf-8") as source:
            return json.load(source)
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=500, detail=f"Gold export '{path.name}' is invalid JSON.") from error


@app.get("/api/health")
def health() -> dict[str, Any]:
    """Report API availability and whether all dashboard exports have been produced."""
    available = [filename for filename in EXPORTS.values() if (DATA_DIR / filename).is_file()]
    return {
        "status": "ok",
        "data_directory": str(DATA_DIR),
        "exports_available": available,
        "data_ready": len(available) == len(EXPORTS),
    }


@app.get("/api/overview")
def overview() -> Any:
    return read_export("overview")


@app.get("/api/analytics/failure-by-model")
def failure_by_model() -> Any:
    return read_export("failure_by_model")


@app.get("/api/analytics/failure-over-time")
def failure_over_time() -> Any:
    return read_export("failure_over_time")


@app.get("/api/smart/comparison")
def smart_comparison() -> Any:
    return read_export("smart_comparison")


@app.get("/api/drives")
def drive_list(query: str = "", limit: int = Query(default=50, ge=1, le=500)) -> list[dict[str, Any]]:
    rows = read_export("drives")
    query_normalized = query.strip().lower()
    if query_normalized:
        rows = [row for row in rows if query_normalized in str(row.get("serial_number", "")).lower()]
    return rows[:limit]


@app.get("/api/predictions/{serial_number}")
def prediction(serial_number: str) -> dict[str, Any]:
    rows = read_export("predictions")
    for row in rows:
        if row.get("serial_number") == serial_number:
            return row
    raise HTTPException(status_code=404, detail="Drive serial number is not present in the Gold prediction export.")


@app.get("/api/benchmarks")
def benchmark() -> Any:
    return read_export("benchmark")
