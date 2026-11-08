"""End-to-end: generator -> pipeline -> reconciliation against expected labels."""
import json
from decimal import Decimal

import pytest

from skew_pipeline.generate_data import generate
from skew_pipeline.run_pipeline import Config, ReconciliationError, run


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    root = tmp_path_factory.mktemp("gen")
    expected = generate(root, n_customers=40, n_orders=400, skew_ratio=0.7, n_order_files=3, seed=7)
    return root, expected


def _cfg(root, out, run_id):
    return Config(str(root / "input/customers"), str(root / "input/orders"), str(out),
                  run_id=run_id, shuffle_partitions=2, master="local[2]")


def test_generator_is_reproducible(tmp_path):
    a = generate(tmp_path / "a", 30, 300, 0.5, 2, seed=1)
    b = generate(tmp_path / "b", 30, 300, 0.5, 2, seed=1)
    assert a == b
    assert (tmp_path / "a/input/orders/orders_part_000.csv").read_text() == \
           (tmp_path / "b/input/orders/orders_part_000.csv").read_text()
    assert not list((tmp_path / "a/input").rglob("expected*"))  # labels live outside input


def test_pipeline_reconciles_with_expected_labels(spark, generated, tmp_path):
    root, expected = generated
    s = run(_cfg(root, tmp_path / "out", "t_run1"), spark)
    assert s["status"] == "SUCCESS"
    c, o = s["counts"], expected["orders"]
    assert c["customers_input"] == expected["customers"]["input_rows"]
    assert c["customers_valid"] == expected["customers"]["valid_rows"]
    assert c["orders_input"] == o["input_rows"] and c["orders_valid"] == o["valid_rows"]
    assert c["orders_input"] == c["orders_valid"] + c["orders_rejected"]
    assert s["rejected_by_reason"]["orders"] == o["rejected_by_reason"]
    assert s["rejected_by_reason"]["customers"] == expected["customers"]["rejected_by_reason"]
    assert Decimal(c["published_total_amount"]) == Decimal(o["valid_total_amount"])

    hot = spark.read.parquet(str(tmp_path / "out/published/customer_order_summary")) \
        .where(f"customer_id = '{o['hot_customer_id']}'").first()
    assert hot["order_count"] == o["valid_orders_for_hot_customer"]
    assert o["valid_orders_for_hot_customer"] > 0.5 * o["valid_rows"]  # the skew is real


def test_rerun_of_same_input_gives_same_output(spark, generated, tmp_path):
    root, _ = generated
    a = run(_cfg(root, tmp_path / "out", "t_a"), spark)
    cfg_b = _cfg(root, tmp_path / "out", "t_b")
    cfg_b.force = True                       # recompute for real (without it the rerun is SKIPPED)
    b = run(cfg_b, spark)
    assert a["counts"] == b["counts"]
    published = spark.read.parquet(str(tmp_path / "out/published/customer_order_summary"))
    assert published.count() == published.select("customer_id").distinct().count()  # no duplicates


def test_empty_batch_succeeds_with_zero_rows(spark, tmp_path):
    from tests.conftest import write_csv
    from skew_pipeline.schemas import CUSTOMER_COLUMNS, ORDER_COLUMNS
    write_csv(tmp_path / "in/customers/c.csv", CUSTOMER_COLUMNS, [])
    write_csv(tmp_path / "in/orders/o.csv", ORDER_COLUMNS, [])
    s = run(Config(str(tmp_path / "in/customers"), str(tmp_path / "in/orders"), str(tmp_path / "out"),
                   run_id="t_empty", shuffle_partitions=2, master="local[2]"), spark)
    assert s["status"] == "SUCCESS" and s["counts"]["orders_input"] == 0 and s["counts"]["published_customers"] == 0


def test_failure_is_recorded_in_run_log(spark, tmp_path, monkeypatch):
    root = tmp_path / "gen"
    generate(root, 20, 200, 0.5, 2, seed=3)
    import skew_pipeline.run_pipeline as rp
    monkeypatch.setattr(rp, "build_summary",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError, match="boom"):
        run(_cfg(root, tmp_path / "out", "t_fail"), spark)
    log = json.loads((tmp_path / "out/run_log/t_fail.json").read_text())
    assert log["status"] == "FAILED" and "boom" in log["failure_reason"] and log["end_time"]
