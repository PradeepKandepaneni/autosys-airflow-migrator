import ast
from pathlib import Path

from jil2dag.emitter import emit
from jil2dag.parser import parse_jil
from jil2dag.translator import translate

FIXTURE = Path(__file__).parent.parent / "examples" / "jil" / "eod_settlement.jil"


def _plans():
    wf = parse_jil(FIXTURE.read_text())
    plans, _ = translate(wf)
    return plans


def test_emitted_source_is_valid_python():
    for plan in _plans():
        src = emit(plan)
        ast.parse(src)  # raises SyntaxError if the codegen is broken


def test_emitter_is_deterministic():
    for plan in _plans():
        assert emit(plan) == emit(plan)


def test_schedule_becomes_cron():
    settle = next(p for p in _plans() if p.dag_id == "eod_settlement")
    src = emit(settle)
    # 18:30 mon-fri -> "30 18 * * 1,2,3,4,5"
    assert "30 18 * * 1,2,3,4,5" in src


def test_uses_airflow3_imports():
    src = emit(_plans()[0])
    assert "from airflow.sdk import DAG" in src
    assert "schedule=" in src
    assert "schedule_interval" not in src  # removed in Airflow 3


def test_taskgroup_rendered_for_box():
    settle = next(p for p in _plans() if p.dag_id == "eod_settlement")
    src = emit(settle)
    assert "TaskGroup(group_id='eod_settlement')" in src


def test_external_sensor_rendered_cross_dag():
    reporting = next(p for p in _plans() if p.dag_id == "eod_reporting")
    src = emit(reporting)
    assert "ExternalTaskSensor" in src
