from pathlib import Path

from jil2dag.model import DepState, JobType
from jil2dag.parser import parse_condition, parse_jil

FIXTURE = Path(__file__).parent.parent / "examples" / "jil" / "eod_settlement.jil"


def test_parses_all_jobs():
    wf = parse_jil(FIXTURE.read_text())
    assert set(wf.jobs) == {
        "EOD_SETTLEMENT", "EOD_WAIT_FEED", "EOD_EXTRACT", "EOD_VALIDATE",
        "EOD_TRANSFORM", "EOD_REPRICE", "EOD_LOAD", "EOD_RECON_ALERT",
        "EOD_REPORTING", "EOD_REG_REPORT",
    }


def test_job_types():
    wf = parse_jil(FIXTURE.read_text())
    assert wf.jobs["EOD_SETTLEMENT"].job_type is JobType.BOX
    assert wf.jobs["EOD_WAIT_FEED"].job_type is JobType.FILEWATCHER
    assert wf.jobs["EOD_EXTRACT"].job_type is JobType.COMMAND


def test_box_membership():
    wf = parse_jil(FIXTURE.read_text())
    kids = {j.name for j in wf.children_of("EOD_SETTLEMENT")}
    assert "EOD_EXTRACT" in kids and "EOD_LOAD" in kids
    assert "EOD_REG_REPORT" not in kids  # belongs to the other box


def test_schedule_extracted():
    wf = parse_jil(FIXTURE.read_text())
    sched = wf.jobs["EOD_SETTLEMENT"].schedule
    assert sched is not None
    assert sched.start_times == ["18:30"]
    assert sched.days_of_week == ["mo", "tu", "we", "th", "fr"]
    assert sched.run_calendar == "nyse_trading_days"
    assert sched.timezone == "America/New_York"


def test_runtime_attrs():
    wf = parse_jil(FIXTURE.read_text())
    assert wf.jobs["EOD_EXTRACT"].max_run_alarm == 20
    assert wf.jobs["EOD_VALIDATE"].term_run_time == 30
    assert wf.jobs["EOD_VALIDATE"].alarm_if_fail is True


def test_condition_atoms():
    c = parse_condition("s(EOD_VALIDATE) & notrunning(EOD_REPRICE)")
    states = {(a.job, a.state) for a in c.atoms}
    assert ("EOD_VALIDATE", DepState.SUCCESS) in states
    assert ("EOD_REPRICE", DepState.NOTRUNNING) in states
    assert c.has_or is False


def test_condition_or_and_lookback():
    c = parse_condition("s(A,,12.00) | s(B)")
    assert c.has_or is True
    a = next(x for x in c.atoms if x.job == "A")
    assert a.lookback_hours == 12.0


def test_done_and_failure_states():
    c = parse_condition("d(EOD_TRANSFORM) & d(EOD_REPRICE)")
    assert all(a.state is DepState.DONE for a in c.atoms)
    cf = parse_condition("f(EOD_LOAD)")
    assert cf.atoms[0].state is DepState.FAILURE
