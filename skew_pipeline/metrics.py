"""Best-effort per-task timing from the Spark UI REST API (local mode).

Returns the heaviest stage's median and maximum task run time of one job
group. The max/median ratio is the direct signature of skew: one straggler
task versus a typical task. Returns None if the UI API is not reachable
(e.g. UI disabled, serverless compute), so callers must treat it as optional.

Stages are inspected one by one: adaptive execution leaves skipped or
cancelled stages the API cannot summarise, and one such stage must not throw
away the evidence from the others.
"""
import json
from urllib.request import urlopen


def _task_summary(base: str, app: str, stage_id: int) -> dict | None:
    url = f"{base}/api/v1/applications/{app}/stages/{stage_id}/0/taskSummary?quantiles=0.5,1.0"
    try:
        with urlopen(url, timeout=5) as resp:
            run = json.load(resp)["executorRunTime"]
        return {"median_task_ms": run[0], "max_task_ms": run[1]}
    except Exception:  # skipped / cancelled / no completed tasks
        return None


def stage_task_skew(spark, group_id: str) -> dict | None:
    try:
        sc = spark.sparkContext
        tracker = sc.statusTracker()
        base, app = sc.uiWebUrl, sc.applicationId
        best = None
        for job_id in tracker.getJobIdsForGroup(group_id):
            job = tracker.getJobInfo(job_id)
            for sid in (job.stageIds if job else []):
                info = tracker.getStageInfo(sid)
                if info is None or info.numTasks < 2:
                    continue
                summary = _task_summary(base, app, sid)
                if summary is None:
                    continue
                cand = {"stage_id": sid, "tasks": info.numTasks, **summary}
                if best is None or cand["max_task_ms"] > best["max_task_ms"]:
                    best = cand
        if best:
            best["max_to_median_ratio"] = round(best["max_task_ms"] / max(best["median_task_ms"], 1), 1)
        return best
    except Exception:  # metrics are optional evidence, never a reason to fail
        return None
