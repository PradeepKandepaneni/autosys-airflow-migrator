"""jil2dag CLI — the 'one-click' surface.

    jil2dag migrate --input examples/jil --output out/dags
    jil2dag report  --input examples/jil          # fidelity report only, exit 1 on WARN

The `migrate` command is deterministic: same JIL in, byte-identical DAGs out.
That property is what lets you diff generated DAGs against a golden set in CI,
and it's the honest answer to "how do you trust an automated migration?".
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .emitter import emit
from .parser import parse_jil
from .translator import FidelityReport, translate


def _load_jil(input_path: Path) -> str:
    if input_path.is_dir():
        return "\n".join(
            p.read_text() for p in sorted(input_path.glob("*.jil"))
        )
    return input_path.read_text()


def _print_report(report: FidelityReport) -> int:
    if not report.findings:
        print("fidelity: clean — every job mapped with no downgrades.")
        return 0
    for f in report.findings:
        print(f"[{f.severity}] {f.dag_id} :: {f.job} :: {f.message}")
    n_warn = len(report.warnings)
    print(f"\nfidelity: {n_warn} warning(s), {len(report.findings) - n_warn} info.")
    return 1 if n_warn else 0


def cmd_migrate(args: argparse.Namespace) -> int:
    text = _load_jil(Path(args.input))
    wf = parse_jil(text)
    plans, report = translate(wf)

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    for plan in plans:
        (out / f"{plan.dag_id}.py").write_text(emit(plan))
    print(f"migrated {len(wf.jobs)} job(s) -> {len(plans)} DAG(s) in {out}/")

    code = _print_report(report)
    if args.strict and code != 0:
        return code
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    text = _load_jil(Path(args.input))
    wf = parse_jil(text)
    _, report = translate(wf)
    return _print_report(report)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="jil2dag", description="AutoSys JIL -> Airflow 3 migrator")
    sub = p.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("migrate", help="generate Airflow DAGs from JIL")
    m.add_argument("--input", required=True, help="JIL file or directory of *.jil")
    m.add_argument("--output", required=True, help="output directory for DAGs")
    m.add_argument("--strict", action="store_true", help="exit non-zero on fidelity warnings")
    m.set_defaults(func=cmd_migrate)

    r = sub.add_parser("report", help="print the fidelity report and exit")
    r.add_argument("--input", required=True)
    r.set_defaults(func=cmd_report)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
