# Upgrades: AutoSys, and the Airflow 2→3 jump you inherit

**Direct answer:** Two upgrades sit on top of any migration project, and they share one root risk — *your dependency and timing semantics change underneath jobs that still look identical*. Treat both as behavior-changing, not version-bumping.

## AutoSys upgrade (e.g. 11.3.x → 12.x / R12)

Reasons teams upgrade: end-of-support for the older release, the Broadcom (and later Fortra) ownership changes reshaping licensing, and a push to consolidate agents. The mechanics that bite:

- **Event server / database schema migration.** The upgrade migrates the event-server DB. On large installs (millions of historical events) this is the long pole and needs a maintenance window. Archive old event history first.
- **Agent version skew.** Scheduler and remote agents must be compatible. A partial upgrade where agents lag the scheduler causes jobs to sit in `STARTING`.
- **Dual/shadow event server.** Upgrade the shadow first, verify sync, then fail over — never both live servers at once.
- **JIL compatibility.** Most JIL carries forward, but validate any deprecated attributes and re-`autorep -q` everything post-upgrade to confirm live definitions match your exports.
- **Validation.** Compare `autorep -d` history and run-time distributions before/after; a "successful" upgrade that quietly changed a calendar or timezone is the dangerous kind.

The reason this matters for *your migration to Airflow*: if you're going to upgrade AutoSys anyway, that's the moment to ask whether to migrate off it instead. The forcing function (support/licensing) is the same one that makes the Airflow move a "now" project rather than a "someday" one.

## The Airflow 2→3 upgrade you inherit

Even a greenfield migration lands on Airflow 3, so you own its breaking changes. The ones this engine already accounts for, and the ones you must handle operationally:

**Already handled in generated DAGs**
- `schedule=` replaces the removed `schedule_interval`.
- Operators come from the **standard provider** (`airflow.providers.standard.operators.bash`), not `airflow.operators.*`.
- `DAG` / `TaskGroup` import from the **Task SDK** (`airflow.sdk`).
- **SubDags are gone** → boxes map to TaskGroups.
- **SLAs are gone** → `max_run_alarm`/deadline logic moves to Datadog monitors, not SLA callbacks.

**You must handle operationally**
- The **DAG processor is mandatory** in Airflow 3 — a deployment without it silently loads no DAGs.
- **XCom pickling is disabled** by default; passing non-serializable data between tasks needs a custom XCom backend.
- `execution_date` is replaced by `logical_date` in the context.
- **Flask-AppBuilder was removed** for the new React UI; if you relied on FAB plugins, install the FAB provider or migrate them.
- Astro Runtime 3+ resolves Python packages with **uv, not pip**, by default — pin dependencies to avoid surprise upgrades.

**Verify with the tooling, don't eyeball it.** Ruff ships Airflow upgrade rule sets:
- `AIR30x` — removed parameters/imports that *must* change for Airflow 3.
- `AIR31x` — deprecated-but-still-working items that *should* change before Airflow 4.

This repo's CI runs `AIR301`/`AIR311` against generated DAGs, so a regression that reintroduces Airflow-2 idioms fails the build.

## The shared lesson
Both upgrades are places where a job's *code* is unchanged but its *behavior* isn't — a calendar drops, a timezone shifts, a dependency window changes, a DAG stops loading because a required component is off. The defense is the same in both worlds: compare runtime behavior before and after against a metric baseline (AutoSys `autorep` history; Datadog `jil2dag.*` metrics), and never decommission the old path until the new one has matched it across a full calendar cycle.
