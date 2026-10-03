"""Refuse to start a Spark job while another application runs on the standalone master.

Standard library only: it runs with the system python3 of the Spark image, where `pyspark` is not importable.
Used by the Airflow DAG (scripts/check_spark_idle.py) before every Spark task, so a manual K-Means, export or
benchmark started outside Airflow cannot overlap with a DAG task (RAM is the limit: Docker has 7.36 GiB).
"""
from __future__ import annotations

import json
import time
import urllib.request
from typing import Any, Callable, Dict, List


class ClusterBusyError(RuntimeError):
    pass


def active_app_names(master_state: Dict[str, Any]) -> List[str]:
    return [str(app.get("name", app.get("id", "?"))) for app in master_state.get("activeapps", [])]


def fetch_master_state(master_http: str, timeout: float = 10.0) -> Dict[str, Any]:
    with urllib.request.urlopen(master_http.rstrip("/") + "/json/", timeout=timeout) as response:  # noqa: S310 - internal URL
        return json.loads(response.read().decode("utf-8"))


def wait_until_idle(
    fetch: Callable[[], Dict[str, Any]],
    attempts: int = 5,
    delay: float = 3.0,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Return as soon as the master has no active application. An application that just stopped can linger for
    a moment, so look `attempts` times, `delay` seconds apart, before giving up. `fetch` errors propagate."""
    names: List[str] = []
    for attempt in range(attempts):
        names = active_app_names(fetch())
        if not names:
            return
        if attempt < attempts - 1:
            sleep(delay)
    raise ClusterBusyError("Spark master still runs application(s): {}".format(", ".join(names)))
