# Interview playbook

How to use this repo in a senior DevOps/SRE/platform interview. Every claim below is backed by code you can open, which is the whole point — you're not reciting, you're demonstrating. Structure for troubleshooting answers: **problem framing → root cause → prevention.** Structure for behavioral: **STAR.**

A discipline note that applies to everything here: only state numbers you can personally defend. This repo lets you say "24 tests, 2 DAGs, 5 fidelity warnings on the example" because those are real and reproducible on the spot (`make test`, `make report`). Don't decorate them with invented production metrics; a senior interviewer will ask about measurement methodology, baseline, and time window, and a placeholder will collapse.

## "Walk me through a migration you designed"

Frame it as compiling a dynamic status-machine into a static graph. Boxes → TaskGroups, conditions → edges with trigger rules, filewatchers → sensors, calendars → timetables. Then pivot to the part that shows seniority: **the things that don't translate.** Point at the fidelity report — `notrunning` is mutual exclusion not a dependency, `OR` has no edge form, look-back windows aren't logical dates. Junior engineers auto-convert everything; senior engineers know where to stop and design.

## "How do you trust an automated migration?"

Three answers, in order: **determinism** (same JIL → identical DAGs, so you can diff against a golden set in CI), **a real DagBag parse** (CI imports the DAGs against actual Airflow 3.2, catching import errors before merge), and **parallel-run validation** (compare `jil2dag.*` Datadog metrics against AutoSys `autorep` history across a full calendar cycle before decommissioning). Trust is a metric comparison, not a leap of faith.

## "AutoSys is throwing intermittent failures — how do you debug?"

Problem framing: distinguish "job failed" from "job never started" from "job stuck RUNNING." Root cause hunt: `autorep -q` for the live definition (not the JIL file), `autorep -d` for history, `chk_auto_up` and `autoping` if it's system-wide, agent↔event-server heartbeat if a job hangs in STARTING. Prevention: `term_run_time` so hung jobs self-terminate instead of blocking downstream conditions; dual event server; alerting on `chk_auto_up`. (Full detail: `troubleshooting-runbook.md`.)

## "What breaks in an Airflow 2→3 upgrade?"

Lead with the operational landmine: the **DAG processor is mandatory** in Airflow 3 — miss it and no DAGs load, with no obvious error. Then the code-level removals: `schedule_interval`, SLAs, SubDags, `airflow.operators.*` paths, XCom pickling, FAB. Close with tooling: ruff `AIR30x`/`AIR31x` rules gate this in CI so a regression can't reintroduce Airflow-2 idioms. You handle the code automatically and the operational parts deliberately.

## "Design observability for these workflows"

Start from the question observability must answer during a migration: *does Airflow behave like AutoSys did?* That demands **business-level** metrics tagged back to the source job, not just infra health — which is why the DAGs emit `jil2dag.task.*` via DogStatsD tagged with `jil_job:`. Then SLOs (99% settlement success over 30 days), monitors that replace the removed SLA/`max_run_alarm` semantics, and a dashboard that pivots by source JIL job. Everything is code (`observability/`), so alerting migrates with the workflow.

## "Where would this fall over at scale?"

Be honest — it earns credibility. One-box-one-DAG is diffable but chatty; a 5,000-job estate wants consolidation and probably Otto + Professional Services for the long tail (`orbiter-vs-otto-vs-this.md`). The parser covers ~30 of ~180 JIL attributes. SSH connections are assumed, not provisioned. Naming those limits unprompted is the difference between a demo and an engineering artifact.

## Behavioral: "a time you handled a risky change" (STAR)

**Situation:** legacy scheduler forced off by end-of-support with a hard date. **Task:** migrate without a silent behavior change in a regulated batch. **Action:** built a deterministic, rule-based converter with a fidelity report so lossy mappings were designed rather than guessed, gated on golden-diff + DagBag parse in CI, and ran old/new in parallel comparing metrics. **Result (defensible framing):** every lossy construct was surfaced and reviewed before cutover; the example converts 10 jobs to 2 DAGs with 5 explicit warnings — state it as *methodology*, and only cite production numbers you actually measured.
