"""Evidence of a demo run: one small JSON file per step under <artifacts>/steps/, plus run.json.

The dashboard page reads these files to show, live, which step is running, what it printed and what it measured.
Files are written atomically (write a temp file, then rename), so the page never reads a half-written file.
Standard library only: the page must not import config/ or Spark.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline.demo_spec import DEMO_STEPS, EVIDENCE_SUBDIR

STATUSES = ("pending", "running", "ok", "failed")
MAX_LINES = 400  # keeps a file small even for a chatty step


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def steps_dir(artifacts: Path) -> Path:
    return Path(artifacts) / EVIDENCE_SUBDIR


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(str(temp), str(path))


def write_step(artifacts: Path, task_id: str, payload: Dict[str, Any]) -> None:
    payload = dict(payload)
    lines = payload.get("lines") or []
    if len(lines) > MAX_LINES:
        payload["lines"] = lines[:20] + ["... ({} dòng được lược bớt)".format(len(lines) - MAX_LINES + 20)] + lines[-(MAX_LINES - 21):]
    _write_json(steps_dir(artifacts) / (task_id + ".json"), payload)


def read_step(artifacts: Path, task_id: str) -> Optional[Dict[str, Any]]:
    path = steps_dir(artifacts) / (task_id + ".json")
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return None  # being replaced right now: the next poll sees the new file


def read_all(artifacts: Path) -> Dict[str, Dict[str, Any]]:
    result = {}
    for step in DEMO_STEPS:
        payload = read_step(artifacts, step.task_id)
        if payload is not None:
            result[step.task_id] = payload
    return result


def read_run(artifacts: Path) -> Optional[Dict[str, Any]]:
    path = Path(artifacts) / "run.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return None


def write_run(artifacts: Path, payload: Dict[str, Any]) -> None:
    _write_json(Path(artifacts) / "run.json", payload)


def init_run(artifacts: Path, run_id: str) -> None:
    """Start a new run: every step becomes 'pending' (so the page lists all nine steps at once)."""
    for step in DEMO_STEPS:
        write_step(artifacts, step.task_id, {
            "task_id": step.task_id, "step": step.step, "label": step.label, "title": step.title,
            "status": "pending", "run_id": run_id, "started_at": None, "finished_at": None, "seconds": None,
            "lines": [], "metrics": {}, "error": None,
        })
    write_run(artifacts, {"run_id": run_id, "started_at": now_iso(), "finished_at": None, "status": "running"})


def summarize(all_steps: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Counts and total time over the steps that have evidence."""
    counts = {status: 0 for status in STATUSES}
    total = 0.0
    for payload in all_steps.values():
        counts[payload.get("status", "pending")] = counts.get(payload.get("status", "pending"), 0) + 1
        total += float(payload.get("seconds") or 0)
    return {"counts": counts, "total_seconds": round(total, 1), "steps": len(all_steps)}


def format_table(headers: List[str], rows: List[List[Any]]) -> List[str]:
    """A plain-text table as a list of lines (monospace), for the step logs."""
    cells = [[str(c) for c in row] for row in rows]
    widths = [max([len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(headers)]

    def line(values: List[str]) -> str:
        return "  ".join(v.ljust(widths[i]) for i, v in enumerate(values)).rstrip()

    return [line(headers), "  ".join("-" * w for w in widths)] + [line(r) for r in cells]
