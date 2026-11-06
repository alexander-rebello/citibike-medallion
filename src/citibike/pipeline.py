"""Run the pipeline steps in order, stop at the first failure and keep a run log.

Each step is a command. Its output goes to <lake>/_meta/pipeline_logs/<run_id>/<step>.log and one JSON record per
run is appended to <lake>/_meta/pipeline_runs.jsonl. After a failed step no later step runs, so Gold is never
republished from a run that did not pass Bronze and Silver.
"""
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def run(steps: list[tuple[str, list[str]]], meta_dir: str, env_name: str, trigger: str = "manual",
        run_id: str | None = None, env: dict | None = None) -> dict:
    run_id = run_id or new_run_id()
    log_dir = os.path.join(meta_dir, "pipeline_logs", run_id)
    os.makedirs(log_dir, exist_ok=True)
    record = {"run_id": run_id, "env": env_name, "trigger": trigger, "started": _now(), "status": "success", "steps": []}

    for name, cmd in steps:
        log_file = os.path.join(log_dir, f"{name}.log")
        t0 = time.monotonic()
        with open(log_file, "w") as out:
            rc = subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT, env={**os.environ, **(env or {})}).returncode
        record["steps"].append({"name": name, "status": "ok" if rc == 0 else "failed", "returncode": rc,
                                "seconds": round(time.monotonic() - t0, 1), "log": log_file})
        if rc != 0:
            record["status"] = "failed"
            record["failed_step"] = name
            record["skipped"] = [n for n, _ in steps[len(record["steps"]):]]
            break

    record["finished"] = _now()
    with open(os.path.join(meta_dir, "pipeline_runs.jsonl"), "a") as f:
        f.write(json.dumps(record) + "\n")
    return record
