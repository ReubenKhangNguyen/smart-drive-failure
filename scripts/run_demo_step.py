"""Run ONE step of the demo (see pipeline/demo_spec.py) and record evidence for the dashboard page.

Called by every task of the Airflow DAG `smart_drive_demo`:

    SMART_DRIVE_CONFIG=config/demo.yaml SMART_DRIVE_ARTIFACTS=artifacts/demo \
        PYTHONPATH=. python scripts/run_demo_step.py --step prepare_hdfs

The step prints what it is doing (that is the Airflow task log) and the same text plus its measurements are written
to <artifacts>/steps/<step>.json while it runs, so the web page can follow it live.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
import traceback

from config.settings import artifacts_dir, load_config
from pipeline import demo_evidence as evidence
from pipeline.demo_spec import DEMO_STEPS, assert_demo_config, step_by_id
from pipeline.demo_steps import STEP_FUNCTIONS, StepContext

LIVE_INTERVAL = 0.7  # seconds between live evidence writes while a step is printing


def _new_run_id() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")


def run_step(task_id: str, config: dict, artifacts, functions=None) -> int:
    """Run one step; always leaves a final evidence file ('ok' or 'failed'). Returns the process exit code."""
    functions = functions or STEP_FUNCTIONS
    spec = step_by_id(task_id)
    is_first, is_last = task_id == DEMO_STEPS[0].task_id, task_id == DEMO_STEPS[-1].task_id
    if is_first:
        run_id = _new_run_id()
        evidence.init_run(artifacts, run_id)
    else:
        run = evidence.read_run(artifacts)
        run_id = run["run_id"] if run else _new_run_id()

    started_at, began = evidence.now_iso(), time.time()
    last_write = {"t": 0.0}

    def payload(ctx: StepContext, status: str, error=None) -> dict:
        return {"task_id": spec.task_id, "step": spec.step, "label": spec.label, "title": spec.title, "status": status, "run_id": run_id,
                "started_at": started_at, "finished_at": None if status == "running" else evidence.now_iso(),
                "seconds": round(time.time() - began, 1), "lines": list(ctx.lines), "metrics": dict(ctx.metrics), "error": error}

    def live(ctx: StepContext) -> None:
        if time.time() - last_write["t"] >= LIVE_INTERVAL:
            last_write["t"] = time.time()
            evidence.write_step(artifacts, spec.task_id, payload(ctx, "running"))

    ctx = StepContext(config, artifacts, spec, run_id, on_update=live)
    evidence.write_step(artifacts, spec.task_id, payload(ctx, "running"))
    print("=== Bước {}: {} ===".format(spec.label, spec.title), flush=True)
    try:
        ctx.metrics = functions[task_id](ctx)
    except Exception as exc:  # noqa: BLE001 - the failure is recorded for the page, then the task fails
        message = "{}: {}".format(type(exc).__name__, exc)
        ctx.say("")
        ctx.say("LỖI: " + message)
        evidence.write_step(artifacts, spec.task_id, payload(ctx, "failed", message))
        evidence.write_run(artifacts, dict(evidence.read_run(artifacts) or {}, status="failed", finished_at=evidence.now_iso(), failed_step=task_id))
        traceback.print_exc()
        return 1
    finally:
        ctx.close()
    evidence.write_step(artifacts, spec.task_id, payload(ctx, "ok"))
    if is_last:
        evidence.write_run(artifacts, dict(evidence.read_run(artifacts) or {}, status="ok", finished_at=evidence.now_iso()))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Chay mot buoc cua demo va ghi bang chung cho trang web")
    parser.add_argument("--step", required=True, choices=[s.task_id for s in DEMO_STEPS])
    args = parser.parse_args()
    config = load_config()
    assert_demo_config(config)  # never run against the real config: it would write into the real data
    return run_step(args.step, config, artifacts_dir())


if __name__ == "__main__":
    sys.exit(main())
