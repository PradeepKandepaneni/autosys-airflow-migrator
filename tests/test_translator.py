from pathlib import Path

from jil2dag.parser import parse_jil
from jil2dag.translator import translate

FIXTURE = Path(__file__).parent.parent / "examples" / "jil" / "eod_settlement.jil"


def _translate():
    wf = parse_jil(FIXTURE.read_text())
    return translate(wf)


def test_two_top_level_dags():
    plans, _ = _translate()
    ids = {p.dag_id for p in plans}
    assert ids == {"eod_settlement", "eod_reporting"}


def test_box_becomes_group_with_children_as_tasks():
    plans, _ = _translate()
    settle = next(p for p in plans if p.dag_id == "eod_settlement")
    assert "eod_settlement" in settle.groups
    task_ids = {t.task_id for t in settle.tasks}
    assert {"eod_extract", "eod_validate", "eod_load"} <= task_ids


def test_success_condition_creates_edge():
    plans, _ = _translate()
    settle = next(p for p in plans if p.dag_id == "eod_settlement")
    extract = next(t for t in settle.tasks if t.task_id == "eod_extract")
    assert "eod_wait_feed" in extract.upstreams


def test_done_condition_sets_trigger_rule():
    plans, _ = _translate()
    settle = next(p for p in plans if p.dag_id == "eod_settlement")
    load = next(t for t in settle.tasks if t.task_id == "eod_load")
    assert load.trigger_rule == "all_done"
    assert set(load.upstreams) == {"eod_transform", "eod_reprice"}


def test_failure_condition_sets_all_failed():
    plans, _ = _translate()
    settle = next(p for p in plans if p.dag_id == "eod_settlement")
    recon = next(t for t in settle.tasks if t.task_id == "eod_recon_alert")
    assert recon.trigger_rule == "all_failed"


def test_notrunning_produces_warning_and_no_edge():
    plans, report = _translate()
    settle = next(p for p in plans if p.dag_id == "eod_settlement")
    transform = next(t for t in settle.tasks if t.task_id == "eod_transform")
    # notrunning(EOD_REPRICE) must NOT become an edge
    assert "eod_reprice" not in transform.upstreams
    msgs = " ".join(f.message for f in report.warnings)
    assert "mutual exclusion" in msgs


def test_or_condition_warns():
    _, report = _translate()
    msgs = " ".join(f.message for f in report.warnings)
    assert "OR condition" in msgs


def test_cross_dag_condition_adds_external_sensor():
    plans, report = _translate()
    reporting = next(p for p in plans if p.dag_id == "eod_reporting")
    reg = next(t for t in reporting.tasks if t.task_id == "eod_reg_report")
    waits = {dag for dag, _ in reg.external_waits}
    assert "eod_settlement" in waits
    msgs = " ".join(f.message for f in report.warnings)
    assert "crosses DAG boundary" in msgs


def test_filewatcher_becomes_sensor():
    plans, _ = _translate()
    settle = next(p for p in plans if p.dag_id == "eod_settlement")
    wait = next(t for t in settle.tasks if t.task_id == "eod_wait_feed")
    assert wait.operator == "FileSensor"


def test_remote_machine_uses_ssh():
    plans, _ = _translate()
    settle = next(p for p in plans if p.dag_id == "eod_settlement")
    extract = next(t for t in settle.tasks if t.task_id == "eod_extract")
    assert extract.operator == "SSHOperator"
