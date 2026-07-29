# autosys-airflow-migrator

A deterministic, observable engine that converts **CA AutoSys JIL** into **Apache Airflow 3.2** DAGs — and, unlike a black-box migration, tells you exactly what it *couldn't* translate cleanly and why.

```bash
pip install -e ".[dev]"
jil2dag migrate --input examples/jil --output out/dags   # the one click
```

That command parses a JIL inventory, emits idiomatic Airflow 3 DAGs, and prints a **fidelity report** of every lossy mapping. Same JIL in → byte-identical DAGs out, so the output can be diffed against a golden set in CI.

## Why this exists

Two things happened in 2026 that make an open, rule-based AutoSys→Airflow path worth building:

- **Airflow 2 reached end of life in April 2026.** New work targets Airflow 3, where `schedule_interval`, SLAs, and the `airflow.operators.*` import paths are gone.
- **Astronomer archived [Orbiter](https://github.com/astronomer/orbiter) on June 10, 2026** — the open-source, rule-based translator that supported AutoSys — and moved migration into **Otto**, an AI agent that is commercial and early-access.

So the transparent, auditable, self-hostable path for JIL→DAG is now unmaintained, precisely when every AutoSys shop is being pushed to move. This project fills that gap deliberately: rule-based (you can read and diff every decision), current for Airflow 3.2, and **instrumented for Datadog** — the observability Orbiter never had and an AI agent won't hand you as code.

This is a reference implementation and interview artifact, not a support contract. For a large regulated migration you would still evaluate Astronomer's paid tooling; see `docs/orbiter-vs-otto-vs-this.md` for an honest comparison.

## What it maps

| AutoSys / JIL | Airflow 3 | Notes |
|---|---|---|
| `job_type: c` (command) | `BashOperator` / `SSHOperator` | `SSHOperator` when `machine:` is set |
| `job_type: b` (box) | `TaskGroup` (one top-level box → one DAG) | boxes are containers, not tasks |
| `job_type: f` (filewatcher) | `FileSensor` (`mode="reschedule"`) | |
| `condition: s(J)` | edge `J → this` | default `all_success` |
| `condition: d(J)` | edge, `trigger_rule="all_done"` | |
| `condition: f(J)` | edge, `trigger_rule="all_failed"` | recovery paths |
| `start_times` / `days_of_week` | `schedule=` cron | |
| `term_run_time` | `execution_timeout` | hard kill |
| `alarm_if_fail` | `on_failure_callback` → Datadog | |

## What it refuses to fake (fidelity report)

These have **no clean Airflow analog**, so they surface as warnings rather than silently-wrong DAGs:

- `notrunning(J)` — mutual exclusion, not a dependency → suggests a 1-slot Airflow pool.
- `OR` / `NOT` conditions — no edge form → flagged for manual branch/sensor design.
- `run_calendar` — named AutoSys calendars → need a custom Airflow Timetable.
- `max_run_alarm` — was an alarm, not a kill; Airflow 3 removed SLAs → wired as a Datadog monitor.
- Cross-box conditions → `ExternalTaskSensor`, with a warning that AutoSys look-back windows ≠ Airflow logical dates.

Real output on the bundled example: **5 warnings, 1 info** across 10 jobs → 2 DAGs. Run `make report` to see it.

## Layout

```
jil2dag/          parser -> IR (model) -> translator -> emitter -> CLI
examples/jil/     a realistic end-of-day settlement batch (the complex use case)
observability/    Datadog callbacks, monitors, SLO, dashboard (all as code)
docker/           local Airflow 3.2 + Datadog agent
docs/             AutoSys internals, troubleshooting, upgrade, comparison, interview playbook
.github/          CI: ruff (+Airflow AIR rules), tests, golden-diff, real DagBag parse
```

## Quickstart

```bash
make install        # engine + dev tools
make test           # 24 unit tests
make lint           # ruff incl. Airflow 3 upgrade rules (AIR301/AIR311)
make migrate        # generate DAGs into out/dags
make report         # fidelity report
DD_API_KEY=... make up   # local Airflow 3.2 + Datadog at localhost:8080
```

## Honest limitations

- Covers the ~30 JIL attributes that shape structure and runtime, not all ~180. Unmapped attributes are preserved on the job, never dropped.
- One box → one DAG is defensible and diffable, but chattier than a hand-authored design. Consolidation is a deliberate follow-up, not an accident.
- Generated `SSHOperator` tasks assume a connection `ssh_<machine>` exists; provisioning connections is out of scope.

MIT licensed. See `docs/` for the deep dives.
