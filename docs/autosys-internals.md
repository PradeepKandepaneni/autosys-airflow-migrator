# AutoSys internals (the parts that decide how a migration goes)

**Direct answer:** AutoSys is an event-driven scheduler, not a time-driven one. Nothing "runs on a schedule" in the way cron does. Instead, a central **event server** holds job state, a **scheduler** evaluates *conditions* against that state, and **agents** on remote machines execute the work. Almost every migration surprise comes from teams assuming AutoSys is cron-with-dependencies when it is really a status state-machine. If you understand the event server's role, the rest follows.

## The four components

**Event server (the database).** The source of truth for every job's current status (`SUCCESS`, `FAILURE`, `RUNNING`, `INACTIVE`, `TERMINATED`, `ON_HOLD`, `ON_ICE`, …) and for the event queue. Everything is an event: a job starting, a file arriving, an operator forcing a status. In HA setups there are two event servers (dual-server / shadow) kept in sync. When people say "AutoSys is down," they almost always mean the event server or its DB connection.

**Scheduler.** Continuously reads new events, re-evaluates the `condition` of every candidate job, and, when a condition becomes true, sends a start event to an agent. It also fires time events from `start_times`/`start_mins`. The scheduler is where dependency logic actually lives — which is exactly what a DAG makes explicit and static.

**Agent (remote executor).** Runs on each target `machine`. Receives start commands, launches the process under the job's `owner`, streams `std_out_file`/`std_err_file`, and reports exit status back as an event. This is the piece a `machine:` attribute selects, and the reason a remote AutoSys job becomes an `SSHOperator` (or a Kubernetes/queue-based executor) rather than a local `BashOperator`.

**JIL (the definition language).** `Job Information Language`. Declarative stanzas (`insert_job` / `update_job` / `delete_job`) describing each job. JIL is loaded via the `jil` command; the loaded definition lives in the event server, not in the JIL file — a critical point, because your JIL export may be stale relative to production. Always diff exported JIL against `autorep -q`.

## Boxes: the concept that breaks naive migrations

A **box** (`job_type: b`) is a container that groups jobs and carries its own condition and schedule. Key semantics people forget:

- A box does not "do" anything; it starts its child jobs when the box's own condition is met.
- A box's status is derived from its children (it's `SUCCESS` when all children succeed, `RUNNING` while any child runs, etc.).
- Boxes can nest.
- Child jobs typically depend on each other via `condition`, not via box order.

The correct Airflow analog is a **TaskGroup**, not a task and not a SubDag (SubDags were removed in Airflow 3). This engine maps one top-level box → one DAG, children → tasks in a TaskGroup.

## Conditions: status predicates, not edges

A `condition:` is a boolean expression over job *statuses*:

| JIL | Meaning | Airflow mapping |
|---|---|---|
| `s(J)` | J succeeded | edge `J→this`, `all_success` |
| `d(J)` | J is done (any terminal status) | edge, `all_done` |
| `f(J)` | J failed | edge, `all_failed` |
| `n(J)` / `notrunning(J)` | J is not currently running | **no edge** — mutual exclusion |
| `s(J,,12.00)` | J succeeded within the last 12h (look-back) | edge, but window semantics differ |

The look-back window (`s(J,,H.MM)`) is the sharpest mismatch. AutoSys asks "did J succeed in the last N hours, wall-clock?" Airflow asks "did the upstream task for *this logical date* succeed?" A cross-DAG `ExternalTaskSensor` defaults to matching logical dates, so a naive conversion of a look-back condition can wait forever or match the wrong run. This engine flags every cross-DAG condition for exactly this reason.

## Scheduling attributes

- `start_times: "06:00, 18:00"` → fixed clock times → cron `0 6,18 * * *`.
- `start_mins: 0,30` → every hour at :00 and :30 → cron `0,30 * * * *`.
- `days_of_week: mo,tu,we,th,fr` → cron DOW field.
- `run_calendar: nyse_trading_days` → a named calendar of specific dates. **No cron equivalent.** In Airflow this is a custom `Timetable`. Ignoring it is the classic "the batch ran on a bank holiday" incident.
- `timezone` → set on the DAG; mind DST, since AutoSys and Airflow handle it differently.

## Runtime and alerting attributes

- `max_run_alarm: N` (minutes) → raises an **alarm** if the job runs longer than N. It does **not** kill the job. In Airflow 3 (no SLAs) this becomes a Datadog monitor on task duration.
- `term_run_time: N` → **kills** the job after N minutes → `execution_timeout=timedelta(minutes=N)`.
- `alarm_if_fail: 1` → send an alarm on failure → `on_failure_callback`.
- `std_out_file` / `std_err_file` → agent-side log redirection → Airflow's task logs plus your log backend.

## The one-paragraph mental model to keep

AutoSys is a distributed status machine: the event server remembers state, the scheduler turns state changes into start decisions via conditions, agents run the work and emit new state. Airflow makes that implicit dependency graph explicit and static in Python. The migration is therefore a *compilation from a dynamic predicate system into a static graph* — and the places where a predicate can't be compiled into an edge (`notrunning`, `OR`, look-back windows, named calendars) are exactly where you must stop and design, not auto-convert.
