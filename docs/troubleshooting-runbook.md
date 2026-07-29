# Troubleshooting runbook

Structured as: **symptom → likely root cause → the exact command → prevention.** Covers both sides of a migration — the AutoSys job that's misbehaving and the Airflow DAG that replaced it — because during a cutover you own both.

## AutoSys side

### A job won't start even though its predecessor succeeded
**Root cause (most common):** the `condition` is not what you think, or the predecessor's status isn't what you think. AutoSys evaluates against the event server, not the JIL file.
```bash
autorep -j MY_JOB -q          # dump the *live* JIL (compare to your export)
autorep -j MY_JOB             # current status + last start/end
autorep -d -j MY_JOB          # detailed history, including why it didn't run
```
**Fix / prevention:** never trust the JIL file; trust `autorep -q`. Look-back windows (`s(J,,12.00)`) silently expire — a predecessor that succeeded 13 hours ago no longer satisfies `s(J,,12.00)`.

### A job is stuck in RUNNING but the process is gone
**Root cause:** the agent lost contact with the event server, so the completion event never posted.
```bash
autorep -j MY_JOB              # confirms RUNNING
sendevent -E CHANGE_STATUS -s FAILURE -j MY_JOB    # or SUCCESS, deliberately
```
**Prevention:** monitor agent↔event-server heartbeats; set `term_run_time` so a hung job self-terminates instead of blocking downstream conditions forever.

### Everything stopped at once
**Root cause:** event server or its DB is down, or the scheduler daemon died.
```bash
chk_auto_up                    # is the scheduler / event server up?
autoping -m ALL                # can the scheduler reach every agent machine?
```
**Prevention:** dual event server (shadow) and alerting on `chk_auto_up`.

### A job ran on a holiday
**Root cause:** a `run_calendar` was dropped or never applied.
```bash
autocal_asc -s HOLIDAY_CAL     # inspect the calendar's dates
```
**Prevention:** treat calendars as first-class config; in Airflow, implement them as a custom Timetable (this engine warns whenever a `run_calendar` can't be expressed as cron).

## Airflow side (the migrated DAG)

### DAG doesn't appear in the UI
**Root cause:** an import error, or in Airflow 3 the **DAG processor isn't running** (it's mandatory now).
```bash
airflow dags list-import-errors
airflow dags list | grep eod_
```
**Prevention:** the CI `dagbag-parse` job catches import errors before merge. Confirm the DAG processor component is enabled — a 2→3 upgrade gotcha.

### `<jil_job>` mapped but the task never triggers
**Root cause:** a `trigger_rule` mismatch. A recovery task migrated from `f(J)` uses `all_failed` and will *not* run when the upstream succeeds — that's correct, but surprises people who expected it to always run.
```bash
airflow tasks test <dag_id> <task_id> 2025-01-01
```
**Prevention:** read the fidelity report. Every non-default trigger rule is explained there.

### Cross-DAG sensor hangs forever  {#deadline}
**Root cause:** `ExternalTaskSensor` is waiting on a logical date that never matches, because the source was an AutoSys look-back window, not a same-day dependency.
```bash
airflow tasks states-for-dag-run <upstream_dag_id> <run_id>
```
**Fix:** set `execution_date_fn` / `allowed_states` on the sensor, or redesign as an Asset/Dataset dependency. The engine flags every cross-DAG condition precisely so you review this.

### DAG fails and no one is paged  {#dag-failure}
**Root cause:** Airflow 3 removed SLAs; if you relied on `max_run_alarm`, there is now nothing native alerting on duration.
**Fix:** the Datadog monitors in `observability/monitors/` cover DAG failure and duration deadlines. Import them:
```bash
# via terraform datadog provider, or the API:
curl -X POST "https://api.datadoghq.com/api/v1/monitor" \
  -H "DD-API-KEY: $DD_API_KEY" -H "DD-APPLICATION-KEY: $DD_APP_KEY" \
  -d @observability/monitors/eod_settlement_monitors.json
```
**Prevention:** ship monitors-as-code alongside the DAGs so alerting migrates with the workflow instead of being an afterthought.

## The cutover-week checklist
Run old and new in parallel; compare `jil2dag.task.succeeded` counts against AutoSys `autorep` history for the same window; only decommission an AutoSys box after N clean parallel runs. Validation is a metric comparison, not a vibe.
