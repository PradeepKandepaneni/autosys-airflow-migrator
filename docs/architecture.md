# Architecture

**Direct answer:** A four-stage compiler with a clean intermediate representation in the middle. JIL text → **parser** → IR (`Workflow`) → **translator** → `DagPlan` + `FidelityReport` → **emitter** → Airflow 3 Python. The IR is the design decision that matters: it decouples the *AutoSys front end* from the *Airflow back end*, so JIL parsing and Airflow codegen evolve independently, and a second back end (Dagster, Temporal) could be added without touching the parser.

```
                          ┌──────────────┐
  examples/*.jil  ───────▶│    parser    │  tokenizes JIL stanzas,
                          │ (parser.py)  │  strips comments, parses conditions
                          └──────┬───────┘
                                 ▼
                          ┌──────────────┐
                          │      IR      │  Workflow{ Job, Box, Condition,
                          │  (model.py)  │  Schedule } — scheduler-agnostic
                          └──────┬───────┘
                                 ▼
                          ┌──────────────┐
                          │  translator  │──▶ FidelityReport  (lossy mappings,
                          │(translator.py)│                    with reasons + fixes)
                          └──────┬───────┘
                                 ▼  DagPlan (tasks, edges, trigger rules,
                                 │           groups, sensors, schedule)
                          ┌──────────────┐
                          │    emitter   │  renders Airflow 3.2 source;
                          │ (emitter.py) │  imports only what each DAG uses
                          └──────┬───────┘
                                 ▼
                        out/dags/*.py  ──▶ Airflow 3.2  ──▶ Datadog
                                          (TaskGroups,       (callbacks →
                                           sensors,           DogStatsD →
                                           cron schedule)     monitors/SLO/dash)
```

## Why an IR at all

Without an IR, JIL-isms leak into codegen and every new source format or target requires rewriting both halves. With it:

- **Parser** only knows JIL. It never imports Airflow.
- **Translator** only knows the IR and Airflow's dependency model. It owns every lossy decision and records it in the `FidelityReport`, so "what couldn't we translate?" is answerable programmatically.
- **Emitter** only knows how to render a `DagPlan` as Airflow-3 Python. Retargeting Airflow 4 later means editing one file.

## Determinism, and why it's load-bearing

`translate()` and `emit()` are pure functions of the IR. Sorted iteration order, no timestamps in the output body, no randomness. That makes generated DAGs **diffable against a committed golden set** (`examples/expected_dags/`), which is what turns "trust the migration" into a CI check: if a parser change alters output, the golden diff fails and you review it.

## Observability seam

The emitter injects `from datadog_callbacks import DEFAULT_ARGS, dag_tags` and tags each DAG with its source JIL job names. That single seam is what lets Datadog pivot from an Airflow task all the way back to the AutoSys job it replaced — the pivot you need during parallel-run validation to prove the new system matches the old.

## Extension points

- **New JIL attribute** → add a field in `model.py`, route it in `parser.py`, map it in `translator.py`. Unmapped attributes are already preserved on `Job.unmapped`, so nothing breaks in the meantime.
- **New operator mapping** (e.g. Kubernetes/queue executor instead of SSH) → change `_operator_for()` in `translator.py` and add a render in `emitter.py`.
- **New target orchestrator** → add an emitter; parser and translator are untouched.
