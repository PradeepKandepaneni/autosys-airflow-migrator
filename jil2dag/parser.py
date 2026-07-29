"""Parse AutoSys JIL text into the IR (`Workflow`).

Scope note (be honest about this in interviews): JIL has ~180 attributes.
This parser handles the ones that actually shape a workflow's structure and
runtime behavior — the set you meet in 95% of real batch jobs. Attributes we
recognize but don't model are preserved in `Job.unmapped` so nothing is lost
silently; unknown attributes are collected and reported, not swallowed.

JIL grammar we target (the `insert_job` dialect):

    insert_job: JOB_NAME   job_type: c
    command: /opt/app/run.sh
    machine: prod-host-01
    owner: batch@PROD
    box_name: BOX_EOD
    condition: s(EXTRACT) & s(VALIDATE)
    start_times: "06:00, 18:00"
    days_of_week: mo,tu,we,th,fr
    std_out_file: /logs/job.out
    alarm_if_fail: 1
    max_run_alarm: 45
    description: "End of day load"

Stanzas are separated by a blank line or the next `insert_job:`.
"""

from __future__ import annotations

import re

from .model import (
    Condition,
    Dependency,
    DepState,
    Job,
    JobType,
    Schedule,
    Workflow,
)

# Attributes we understand and route to a specific field.
_KNOWN_ATTRS = {
    "insert_job", "update_job", "job_type", "command", "machine", "owner",
    "box_name", "condition", "start_times", "start_mins", "days_of_week",
    "run_calendar", "timezone", "watch_file", "std_out_file", "std_err_file",
    "alarm_if_fail", "max_run_alarm", "term_run_time", "description", "profile",
}

_COND_ATOM = re.compile(
    r"(?P<state>s|d|f|n|notrunning|success|done|failure|terminated)\s*"
    r"\(\s*(?P<job>[A-Za-z0-9_.\-]+)\s*(?:,\s*,?\s*(?P<lb>[\d.]+))?\s*\)",
    re.IGNORECASE,
)

_STATE_ALIASES = {
    "s": DepState.SUCCESS, "success": DepState.SUCCESS,
    "d": DepState.DONE, "done": DepState.DONE,
    "f": DepState.FAILURE, "failure": DepState.FAILURE, "terminated": DepState.FAILURE,
    "n": DepState.NOTRUNNING, "notrunning": DepState.NOTRUNNING,
}


_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)


def _split_stanzas(text: str) -> list[list[str]]:
    """Group raw lines into per-job stanzas keyed off insert_job/update_job."""
    text = _BLOCK_COMMENT.sub("", text)  # drop multi-line /* ... */ comments
    stanzas: list[list[str]] = []
    current: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("/*") or line.startswith("#"):
            continue
        if line.startswith(("insert_job:", "update_job:")):
            if current:
                stanzas.append(current)
            current = [line]
        else:
            current.append(line)
    if current:
        stanzas.append(current)
    return stanzas


def _kv(line: str) -> tuple[str, str] | None:
    if ":" not in line:
        return None
    key, _, val = line.partition(":")
    return key.strip().lower(), val.strip().strip('"')


def parse_condition(expr: str) -> Condition:
    """Parse a JIL condition expression into atoms + fidelity flags."""
    cond = Condition(raw=expr)
    cond.has_or = bool(re.search(r"\bor\b|\|", expr, re.IGNORECASE))
    cond.has_not = "!" in expr
    for m in _COND_ATOM.finditer(expr):
        state = _STATE_ALIASES[m.group("state").lower()]
        lb = float(m.group("lb")) if m.group("lb") else None
        cond.atoms.append(Dependency(state=state, job=m.group("job"), lookback_hours=lb))
    return cond


def _parse_schedule(attrs: dict[str, str]) -> Schedule | None:
    st = attrs.get("start_times")
    sm = attrs.get("start_mins")
    dow = attrs.get("days_of_week")
    cal = attrs.get("run_calendar")
    tz = attrs.get("timezone")
    if not any((st, sm, dow, cal)):
        return None
    sched = Schedule(timezone=tz, run_calendar=cal)
    if st:
        sched.start_times = [t.strip() for t in st.split(",") if t.strip()]
    if sm:
        sched.start_mins = [int(x.strip()) for x in sm.split(",") if x.strip().isdigit()]
    if dow:
        low = dow.strip().lower()
        sched.days_of_week = ["all"] if low == "all" else [
            d.strip() for d in low.split(",") if d.strip()
        ]
    return sched


def parse_jil(text: str) -> Workflow:
    """Parse a full JIL document into a Workflow IR."""
    wf = Workflow()
    for stanza in _split_stanzas(text):
        attrs: dict[str, str] = {}
        name = ""
        for line in stanza:
            kv = _kv(line)
            if not kv:
                continue
            key, val = kv
            if key in ("insert_job", "update_job"):
                # form: "insert_job: NAME   job_type: c"  -> split trailing job_type
                parts = re.split(r"\s+job_type\s*:\s*", val)
                name = parts[0].strip()
                if len(parts) > 1:
                    attrs["job_type"] = parts[1].strip()
            else:
                attrs[key] = val

        if not name:
            continue  # stray lines with no insert_job header — ignore

        jt = JobType(attrs.get("job_type", "c").lower())
        job = Job(
            name=name,
            job_type=jt,
            command=attrs.get("command"),
            machine=attrs.get("machine"),
            owner=attrs.get("owner"),
            box_name=attrs.get("box_name"),
            watch_file=attrs.get("watch_file"),
            std_out_file=attrs.get("std_out_file"),
            std_err_file=attrs.get("std_err_file"),
            alarm_if_fail=attrs.get("alarm_if_fail", "0") not in ("0", "", "n"),
            max_run_alarm=int(attrs["max_run_alarm"]) if attrs.get("max_run_alarm", "").isdigit() else None,
            term_run_time=int(attrs["term_run_time"]) if attrs.get("term_run_time", "").isdigit() else None,
            description=attrs.get("description"),
            profile=attrs.get("profile"),
        )
        if "condition" in attrs:
            job.condition = parse_condition(attrs["condition"])
        job.schedule = _parse_schedule(attrs)
        # Preserve anything we didn't route so migrations are auditable.
        job.unmapped = {
            k: v for k, v in attrs.items() if k not in _KNOWN_ATTRS
        }
        wf.add(job)
    return wf
