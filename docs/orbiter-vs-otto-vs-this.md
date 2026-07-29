# Orbiter vs. Otto vs. this engine

**Direct answer:** Use **Otto + Astronomer Professional Services** for a large, regulated, time-boxed migration where you want expert-authored DAGs and a support contract. Use **this engine** when you want a transparent, self-hosted, diffable rule-based conversion you fully control and can instrument. **Orbiter** is no longer a live option — it was archived in June 2026 — but its design is the model this project follows.

## Orbiter (Astronomer, open source — archived June 10, 2026)

A framework and CLI (`pip install astronomer-orbiter`) that converted workflow definitions from source systems — Oozie, Control-M, AutoSys, CRON, DAG Factory, Luigi — into Airflow projects. Its core idea was the **TranslationRuleset**: prioritized `@rule` functions written in plain Python that map source constructs to Airflow, extendable per-origin. Community translations lived in a separate `orbiter-community-translations` package.

Strengths: deterministic, inspectable, no vendor lock-in, extensible in Python. Limitation: it was Airflow-2-era, rule-completeness varied by origin, and it carried no observability story. Astronomer archived the repo (now read-only) when it pivoted migration into Otto.

## Otto (Astronomer — current, commercial, early access)

Astronomer's AI data-engineering agent. For migrations it converts Control-M, AutoSys, Automic (UC4), Tidal, and other scheduler definitions into "production-ready" DAGs, mapping every job dependency and carrying your team's conventions, with output that traces back to each source job for validation. It's positioned around the reality that ~83% of scheduler migrations stall on scope, disruption, or skills gaps, and it's paired with Professional Services.

Strengths: encodes years of migration experience, produces idiomatic DAGs a human would recognize, handles the messy long tail. Trade-offs: commercial and early-access; the translation logic is not something you read and diff line-by-line the way you can with a ruleset; you're buying expertise plus a platform, which for many enterprises is exactly right.

## This engine (open, rule-based, observable)

Deliberately Orbiter-shaped and current for Airflow 3.2, with three things it adds:

1. **A fidelity report as a first-class output.** Every lossy mapping (`notrunning`, `OR`/`NOT`, look-back windows, `run_calendar`, cross-DAG conditions) is surfaced with a reason and a suggested fix, rather than converted into a plausible-looking but wrong DAG.
2. **Datadog observability as code.** Callbacks, monitors, an SLO, and a dashboard ship with the DAGs so alerting migrates with the workflow — the gap left when Orbiter was archived.
3. **Determinism you can gate CI on.** Same JIL → identical DAGs, diffed against a golden set, plus a real Airflow 3.2 DagBag parse in CI.

What it is not: a complete JIL implementation, a support contract, or a substitute for Otto on a 5,000-job regulated estate. It covers the ~30 structurally-significant JIL attributes well and is honest about the rest.

## How to choose

- **Regulated, thousands of jobs, hard deadline, want a partner** → Otto + Professional Services.
- **You want to own and audit every translation decision, self-host, and instrument it** → this engine (or fork it).
- **You're evaluating both** → run this engine first on a representative box. Its fidelity report is a fast, free inventory of exactly where your estate will need human design regardless of which tool you ultimately buy.
