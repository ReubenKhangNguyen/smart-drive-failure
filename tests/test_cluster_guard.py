from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from typing import Any, Dict, List

import pytest

from pipeline.cluster_guard import ClusterBusyError, active_app_names, wait_until_idle

ROOT = Path(__file__).resolve().parent.parent
BUSY = {"activeapps": [{"name": "smart-kmeans-segmentation", "id": "app-1"}]}
IDLE = {"activeapps": []}


def test_active_app_names_reads_names_and_falls_back_to_the_id():
    assert active_app_names(BUSY) == ["smart-kmeans-segmentation"]
    assert active_app_names({"activeapps": [{"id": "app-2"}]}) == ["app-2"]
    assert active_app_names({}) == []


def test_wait_until_idle_returns_at_once_when_nothing_runs():
    sleeps: List[float] = []

    wait_until_idle(lambda: IDLE, attempts=5, delay=3.0, sleep=sleeps.append)

    assert sleeps == []


def test_wait_until_idle_tolerates_an_application_that_is_just_stopping():
    states = [BUSY, BUSY, IDLE]
    sleeps: List[float] = []

    wait_until_idle(lambda: states.pop(0), attempts=5, delay=3.0, sleep=sleeps.append)

    assert sleeps == [3.0, 3.0] and states == []  # polled three times, waited twice


def test_wait_until_idle_gives_up_with_the_names_of_the_busy_applications():
    calls: List[Dict[str, Any]] = []

    def always_busy():
        calls.append(BUSY)
        return BUSY

    with pytest.raises(ClusterBusyError) as error:
        wait_until_idle(always_busy, attempts=3, delay=0.0, sleep=lambda _: None)

    assert len(calls) == 3 and "smart-kmeans-segmentation" in str(error.value)


def test_wait_until_idle_does_not_swallow_a_fetch_error():
    def broken():
        raise OSError("master unreachable")

    with pytest.raises(OSError):
        wait_until_idle(broken, attempts=2, delay=0.0, sleep=lambda _: None)


def _load_script():
    spec = importlib.util.spec_from_file_location("check_spark_idle", str(ROOT / "scripts" / "check_spark_idle.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_guard_script_maps_outcomes_to_exit_codes(monkeypatch):
    module = _load_script()
    monkeypatch.setattr("sys.argv", ["check_spark_idle.py", "--attempts", "2", "--delay", "0"])

    monkeypatch.setattr(module, "fetch_master_state", lambda url: IDLE)
    assert module.main() == 0
    monkeypatch.setattr(module, "fetch_master_state", lambda url: BUSY)
    assert module.main() == 2

    def unreachable(url):
        raise OSError("down")

    monkeypatch.setattr(module, "fetch_master_state", unreachable)
    assert module.main() == 3


def test_the_guard_needs_neither_pyspark_nor_yaml():
    # the system python3 of the Spark image can import neither pyspark (not on PYTHONPATH) nor, in the guard, config
    for path in ("pipeline/cluster_guard.py", "scripts/check_spark_idle.py"):
        source = (ROOT / path).read_text(encoding="utf-8")
        imports = re.findall(r"^\s*(?:import|from)\s+([\w.]+)", source, flags=re.MULTILINE)
        assert not [m for m in imports if m.split(".")[0] in ("pyspark", "yaml", "config", "numpy", "pandas")], (path, imports)
