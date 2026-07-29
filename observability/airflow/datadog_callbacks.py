"""Datadog observability for migrated DAGs.

Mount this file into the Airflow DAGs (or plugins) folder so generated DAGs can
`from datadog_callbacks import DEFAULT_ARGS, dag_tags`.

Why DogStatsD and not the Datadog Airflow integration alone? The stock
integration gives you infra + scheduler health. It does NOT know that
`eod_load` used to be the AutoSys job `EOD_LOAD` whose SLA was "done by 19:15".
We emit *business-level* custom metrics tagged with the source JIL job so a
migration can be validated against the old scheduler's behavior, not just
"is Airflow up". This is the observability gap left when Orbiter was archived.

All emission is best-effort: if DogStatsD isn't reachable, DAGs still run.
"""

from __future__ import annotations

import os
from datetime import timedelta

try:
    from datadog import DogStatsd  # datadog>=0.49

    _statsd = DogStatsd(
        host=os.getenv("DD_AGENT_HOST", "datadog-agent"),
        port=int(os.getenv("DD_DOGSTATSD_PORT", "8125")),
        namespace="jil2dag",
        constant_tags=[f"env:{os.getenv('DD_ENV', 'dev')}"],
    )
except Exception:  # pragma: no cover - datadog optional in local/dev
    _statsd = None


def _emit(metric: str, value: float, tags: list[str], kind: str = "count") -> None:
    if _statsd is None:
        return
    try:
        getattr(_statsd, kind)(metric, value, tags=tags)
    except Exception:
        pass  # never let telemetry break a task


def _tags(context: dict) -> list[str]:
    ti = context.get("task_instance")
    dag_id = getattr(ti, "dag_id", "unknown")
    task_id = getattr(ti, "task_id", "unknown")
    return [f"dag:{dag_id}", f"task:{task_id}", "source:autosys"]


def on_task_failure(context: dict) -> None:
    _emit("task.failed", 1, _tags(context))


def on_task_success(context: dict) -> None:
    ti = context.get("task_instance")
    tags = _tags(context)
    _emit("task.succeeded", 1, tags)
    try:
        if ti and ti.start_date and ti.end_date:
            secs = (ti.end_date - ti.start_date).total_seconds()
            _emit("task.duration_seconds", secs, tags, kind="gauge")
    except Exception:
        pass


def on_dag_failure(context: dict) -> None:
    dag = context.get("dag")
    _emit("dag.failed", 1, [f"dag:{getattr(dag, 'dag_id', 'unknown')}", "source:autosys"])


def dag_tags(source_jobs: list[str]) -> list[str]:
    """Tags applied at the DAG level so Datadog can pivot by source JIL job."""
    base = ["migrated:jil2dag", "source:autosys"]
    return base + [f"jil_job:{j}" for j in source_jobs[:20]]


# Airflow 3: SLA is gone. Deadline/alerting lives in Datadog monitors instead.
DEFAULT_ARGS = {
    "owner": "batch-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
    "on_success_callback": on_task_success,
}
