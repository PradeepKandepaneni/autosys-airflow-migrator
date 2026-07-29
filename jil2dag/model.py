"""Domain model for AutoSys JIL and the intermediate representation (IR).

The IR is deliberately scheduler-agnostic. The parser produces it from JIL,
and the translator consumes it to emit Airflow. Keeping a clean IR in the
middle is what lets the same engine target Airflow 3 today and, in principle,
Dagster or Temporal later without rewriting the JIL front end.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class JobType(str, Enum):
    """AutoSys job types we support.

    c = command  -> maps to a BashOperator / SSHOperator
    b = box      -> maps to an Airflow TaskGroup (a container, not a task)
    f = filewatcher -> maps to a sensor (FileSensor)
    """

    COMMAND = "c"
    BOX = "b"
    FILEWATCHER = "f"


class DepState(str, Enum):
    """The four AutoSys condition states plus notrunning().

    These are the crux of every real migration. AutoSys conditions are
    *status predicates* evaluated by the event server; Airflow dependencies
    are *edges* with a trigger_rule. The mapping is lossy and must be explicit.
    """

    SUCCESS = "s"           # s(job)  -> upstream success (normal edge)
    DONE = "d"              # d(job)  -> upstream done regardless of status
    FAILURE = "f"           # f(job)  -> upstream failed (trigger_rule=all_failed)
    NOTRUNNING = "n"        # n(job) / notrunning(job) -> mutual exclusion (no clean edge)


@dataclass
class Dependency:
    """A single atom inside a JIL `condition:` expression, e.g. s(JOB_A)."""

    state: DepState
    job: str
    # AutoSys supports look-back like s(job,,12.00) — hours the status stays true.
    lookback_hours: float | None = None


@dataclass
class Condition:
    """A parsed `condition:` expression.

    We keep both the flat atom list (for dependency wiring) and the raw
    boolean structure (for fidelity warnings). AND is the clean case; OR and
    NOT do not translate to Airflow edges and are surfaced as warnings.
    """

    atoms: list[Dependency] = field(default_factory=list)
    has_or: bool = False
    has_not: bool = False
    raw: str = ""


@dataclass
class Schedule:
    """Time-based triggering extracted from start_times/start_mins/calendars."""

    start_times: list[str] = field(default_factory=list)   # ["06:00", "18:00"]
    start_mins: list[int] = field(default_factory=list)     # [0, 30]
    days_of_week: list[str] = field(default_factory=list)   # ["mo","tu",...] or ["all"]
    run_calendar: str | None = None                          # named AutoSys calendar
    timezone: str | None = None


@dataclass
class Job:
    """One JIL stanza, normalized."""

    name: str
    job_type: JobType
    command: str | None = None
    machine: str | None = None
    owner: str | None = None
    box_name: str | None = None            # parent box, if this job lives in one
    condition: Condition | None = None
    schedule: Schedule | None = None
    watch_file: str | None = None          # for filewatcher jobs
    std_out_file: str | None = None
    std_err_file: str | None = None
    alarm_if_fail: bool = False
    max_run_alarm: int | None = None       # minutes; -> Airflow deadline/timeout
    term_run_time: int | None = None       # minutes; hard kill -> execution_timeout
    description: str | None = None
    profile: str | None = None
    # Anything we saw in JIL but did not model. Never silently dropped.
    unmapped: dict[str, str] = field(default_factory=dict)

    @property
    def is_box(self) -> bool:
        return self.job_type is JobType.BOX


@dataclass
class Workflow:
    """The whole parsed JIL inventory."""

    jobs: dict[str, Job] = field(default_factory=dict)

    def add(self, job: Job) -> None:
        if job.name in self.jobs:
            raise ValueError(f"duplicate job definition: {job.name}")
        self.jobs[job.name] = job

    def top_level_jobs(self) -> list[Job]:
        """Jobs not contained in any box."""
        return [j for j in self.jobs.values() if not j.box_name]

    def children_of(self, box_name: str) -> list[Job]:
        return [j for j in self.jobs.values() if j.box_name == box_name]
