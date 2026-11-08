"""Rerun, retry, restart and backfill behaviour."""
import json
from pathlib import Path

import pytest

from skew_pipeline.generate_data import generate
from skew_pipeline.ingest import list_csv_files
from skew_pipeline.ledger import (file_fingerprint, find_successful_load, make_batch_id,
                                  read_batch, read_run_history)
from skew_pipeline.run_pipeline import Config, SimulatedFailure, run

PUB = "published/customer_order_summary"


@pytest.fixture(scope="module")
def data_a(tmp_path_factory):
    root = tmp_path_factory.mktemp("A")
    generate(root, 25, 250, 0.6, 2, seed=11)
    return root


@pytest.fixture(scope="module")
def data_b(tmp_path_factory):
    root = tmp_path_factory.mktemp("B")
    generate(root, 25, 250, 0.6, 2, seed=12)
    return root


def cfg(root, out, run_id, **kw):
    return Config(str(root / "input/customers"), str(root / "input/orders"), str(out),
                  run_id=run_id, shuffle_partitions=2, master="local[2]", **kw)


def batches(out):
    return sorted(p.name for p in (Path(out) / PUB).glob("batch_id=*"))


def no_duplicates(df):
    return df.count() == df.select("customer_id").distinct().count()


def test_same_input_again_is_skipped_and_nothing_changes(spark, data_a, tmp_path):
    out = tmp_path / "out"
    first = run(cfg(data_a, out, "r1"), spark)
    second = run(cfg(data_a, out, "r2"), spark)
    assert first["status"] == "SUCCESS" and first["attempt"] == 1
    assert second["status"] == "SKIPPED" and second["previous_run_id"] == "r1" and second["attempt"] == 2
    assert second["batch_id"] == first["batch_id"] and "already loaded" in second["skipped_reason"]
    assert second["counts"] == first["counts"]
    df = read_batch(spark, out, PUB, first["batch_id"])
    assert df.select("run_id").distinct().first()[0] == "r1"      # untouched
    assert batches(out) == [f"batch_id={first['batch_id']}"] and no_duplicates(df)


def test_force_replaces_the_batch_without_duplicates(spark, data_a, tmp_path):
    out = tmp_path / "out"
    first = run(cfg(data_a, out, "f1"), spark)
    again = run(cfg(data_a, out, "f2", force=True, mode="broadcast"), spark)
    assert again["status"] == "SUCCESS" and again["replaced_existing_load"] is True
    df = read_batch(spark, out, PUB, first["batch_id"])
    assert no_duplicates(df) and df.select("run_id").distinct().first()[0] == "f2"
    assert df.count() == first["counts"]["published_customers"]
    staged = read_batch(spark, out, "staging/orders_valid", first["batch_id"])
    assert staged.count() == first["counts"]["orders_valid"]      # staging also replaced, not appended


def test_skip_is_not_trusted_when_the_loaded_data_is_gone(spark, data_a, tmp_path):
    """Regression: the run log said 'loaded' but the data had been deleted, so nothing ever reloaded it."""
    import shutil
    out = tmp_path / "out"
    first = run(cfg(data_a, out, "g1"), spark)
    shutil.rmtree(out / PUB / f"batch_id={first['batch_id']}")          # published data lost
    again = run(cfg(data_a, out, "g2"), spark)                          # same input, no --force
    assert again["status"] == "SUCCESS" and again["replaced_existing_load"] is False
    df = read_batch(spark, out, PUB, first["batch_id"])
    assert df.count() == first["counts"]["published_customers"] and no_duplicates(df)
    assert df.select("run_id").distinct().first()[0] == "g2"
    third = run(cfg(data_a, out, "g3"), spark)                          # now everything is present again
    assert third["status"] == "SKIPPED"


def test_new_content_is_a_new_batch_and_old_one_is_untouched(spark, data_a, data_b, tmp_path):
    out = tmp_path / "out"
    a = run(cfg(data_a, out, "a1"), spark)
    b = run(cfg(data_b, out, "b1"), spark)
    assert a["batch_id"] != b["batch_id"] and b["status"] == "SUCCESS"
    assert len(batches(out)) == 2
    assert read_batch(spark, out, PUB, a["batch_id"]).select("run_id").distinct().first()[0] == "a1"


def test_backfill_with_explicit_batch_id_replaces_only_that_batch(spark, data_a, data_b, tmp_path):
    out = tmp_path / "out"
    keep = run(cfg(data_a, out, "k1"), spark)
    old = run(cfg(data_a, out, "p1", batch_id="period_1"), spark)
    fixed = run(cfg(data_b, out, "p2", batch_id="period_1"), spark)     # corrected data, same period
    assert old["batch_id"] == fixed["batch_id"] == "period_1"
    assert fixed["status"] == "SUCCESS" and fixed["replaced_existing_load"] is False  # different content
    df = read_batch(spark, out, PUB, "period_1")
    assert df.select("run_id").distinct().first()[0] == "p2" and no_duplicates(df)
    assert read_batch(spark, out, PUB, keep["batch_id"]).select("run_id").distinct().first()[0] == "k1"
    assert len(batches(out)) == 2


def test_failed_run_is_logged_and_retry_completes_it(spark, data_a, tmp_path):
    out = tmp_path / "out"
    with pytest.raises(SimulatedFailure):
        run(cfg(data_a, out, "x1", simulate_failure_at="after_staging"), spark)
    failed = json.loads((out / "run_log/x1.json").read_text())
    assert failed["status"] == "FAILED" and "SimulatedFailure" in failed["failure_reason"]
    assert failed["end_time"] and (out / "run_log/x1.log").exists()
    assert not (out / PUB).exists()                                   # nothing half-published
    retry = run(cfg(data_a, out, "x2"), spark)                        # same command, no --force
    assert retry["status"] == "SUCCESS" and retry["attempt"] == 2     # a FAILED run never blocks a retry
    assert no_duplicates(read_batch(spark, out, PUB, retry["batch_id"]))
    assert [r["status"] for r in read_run_history(out, retry["batch_id"])] == ["FAILED", "SUCCESS"]


def test_crashed_process_leaves_running_record_and_does_not_block_restart(spark, data_a, tmp_path):
    out = tmp_path / "out"
    files = {"customers": list_csv_files(str(data_a / "input/customers")),
             "orders": list_csv_files(str(data_a / "input/orders"))}
    fp = file_fingerprint(files)
    (out / "run_log").mkdir(parents=True)
    (out / "run_log/dead.json").write_text(json.dumps(
        {"run_id": "dead", "batch_id": make_batch_id(fp), "input_fingerprint": fp,
         "status": "RUNNING", "start_time": "2020-01-01T00:00:00Z"}))
    s = run(cfg(data_a, out, "alive"), spark)
    assert s["status"] == "SUCCESS" and s["attempt"] == 2


def test_run_log_has_the_operational_fields(spark, data_a, tmp_path):
    s = run(cfg(data_a, tmp_path / "out", "log1", mode="broadcast"), spark)
    saved = json.loads((tmp_path / "out/run_log/log1.json").read_text())
    for key in ("run_id", "batch_id", "attempt", "input_fingerprint", "mode", "status", "start_time",
                "end_time", "duration_seconds", "failure_reason", "counts", "rejected_by_reason",
                "timings_seconds", "files"):
        assert key in saved, key
    assert saved["mode"] == "broadcast" and saved["failure_reason"] is None
    assert saved["counts"]["orders_input"] == saved["counts"]["orders_valid"] + saved["counts"]["orders_rejected"]
    assert "succeeded" in (tmp_path / "out/run_log/log1.log").read_text() and s["status"] == "SUCCESS"


# ---- ledger helpers (no Spark needed)
def test_fingerprint_depends_on_content_not_location(tmp_path):
    for d in ("x", "y"):
        (tmp_path / d).mkdir()
        (tmp_path / d / "orders.csv").write_text("a,b\n1,2\n")
    fx = file_fingerprint({"orders": [tmp_path / "x/orders.csv"]})
    fy = file_fingerprint({"orders": [tmp_path / "y/orders.csv"]})
    assert fx == fy
    (tmp_path / "y/orders.csv").write_text("a,b\n1,3\n")
    assert file_fingerprint({"orders": [tmp_path / "y/orders.csv"]}) != fx


def test_batch_id_rules():
    fp = "ab" * 32
    assert make_batch_id(fp) == "b_" + fp[:12]
    assert make_batch_id(fp, "2024-03-05") == "2024-03-05"
    for bad in ("a/b", "../x", "has space", ""):
        if bad:
            with pytest.raises(ValueError):
                make_batch_id(fp, bad)


def test_find_successful_load_matches_on_content_fingerprint():
    hist = [{"run_id": "1", "status": "SUCCESS", "input_fingerprint": "AAA"},
            {"run_id": "2", "status": "FAILED", "input_fingerprint": "AAA"}]
    assert find_successful_load(hist, "AAA")["run_id"] == "1"
    assert find_successful_load(hist, "BBB") is None      # corrected data under the same batch label
