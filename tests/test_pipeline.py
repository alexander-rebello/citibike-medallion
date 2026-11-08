import json
import sys

from citibike import pipeline

PY = sys.executable


def step(name, code):
    return name, [PY, "-c", code]


def test_all_steps_run_in_order_and_the_run_is_logged(tmp_path):
    rec = pipeline.run([step("a", "print('A')"), step("b", "print('B')")], str(tmp_path), "test", "schedule")
    assert rec["status"] == "success" and [s["name"] for s in rec["steps"]] == ["a", "b"]
    line = json.loads((tmp_path / "pipeline_runs.jsonl").read_text().splitlines()[-1])
    assert line["run_id"] == rec["run_id"] and line["trigger"] == "schedule" and line["env"] == "test"
    assert (tmp_path / "pipeline_logs" / rec["run_id"] / "a.log").read_text().strip() == "A"


def test_a_failing_step_stops_the_run_and_later_steps_do_not_execute(tmp_path):
    marker = tmp_path / "gold_ran"
    rec = pipeline.run([step("bronze", "import sys; print('unknown header'); sys.exit(1)"),
                        step("gold", f"open({str(marker)!r}, 'w').write('x')")], str(tmp_path), "test")
    assert rec["status"] == "failed" and rec["failed_step"] == "bronze" and rec["skipped"] == ["gold"]
    assert not marker.exists()  # nothing downstream of the failure ran
    assert "unknown header" in (tmp_path / "pipeline_logs" / rec["run_id"] / "bronze.log").read_text()
    assert json.loads((tmp_path / "pipeline_runs.jsonl").read_text())["status"] == "failed"


def test_each_run_appends_one_record(tmp_path):
    pipeline.run([step("a", "pass")], str(tmp_path), "test")
    pipeline.run([step("a", "pass")], str(tmp_path), "test")
    assert len((tmp_path / "pipeline_runs.jsonl").read_text().splitlines()) == 2


def test_build_steps_order_and_skip():
    import importlib.util
    import os
    spec = importlib.util.spec_from_file_location("run_pipeline", os.path.join(os.path.dirname(__file__), "..", "scripts", "run_pipeline.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert [n for n, _ in mod.build_steps("py", [2026], None, False)] == ["download", "select_sources", "bronze", "stations", "silver", "gold", "dashboard"]
    assert [n for n, _ in mod.build_steps("py", [2026], None, False, ("download",))] == ["select_sources", "bronze", "stations", "silver", "gold", "dashboard"]
    bronze = dict(mod.build_steps("py", [2026], ["2026-10"], True))["bronze"]
    assert bronze[-3:] == ["--months", "2026-10", "--force"] or ("--force" in bronze and "2026-10" in bronze)
