"""All execution strategies must return exactly the baseline result."""
import pytest

from skew_pipeline.benchmark import aqe_skew_split_applied, compare_to_baseline, scale_up
from skew_pipeline.generate_data import generate
from skew_pipeline.ledger import read_batch
from skew_pipeline.run_pipeline import Config, run
from skew_pipeline.transform import MODE_CONF, MODES, build_summary, salt_column
from skew_pipeline.spark_utils import temporary_conf


@pytest.fixture(scope="module")
def staged(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("modes")
    generate(root, n_customers=30, n_orders=400, skew_ratio=0.7, n_order_files=2, seed=21)
    out = root / "out"
    s = run(Config(str(root / "input/customers"), str(root / "input/orders"), str(out),
                   run_id="m_base", mode="baseline", shuffle_partitions=2, master="local[2]"), spark)
    b = s["batch_id"]
    return (root, out, b, read_batch(spark, out, "staging/orders_valid", b),
            read_batch(spark, out, "staging/customers_valid", b))


@pytest.mark.parametrize("mode", [m for m in MODES if m != "baseline"])
def test_mode_equals_baseline(spark, staged, mode):
    _, _, _, orders, customers = staged
    r = compare_to_baseline(spark, orders, customers, mode, salt_buckets=4)
    assert r["rows_only_in_mode"] == 0 and r["rows_only_in_baseline"] == 0
    assert r["schema_identical"] and r["sum_order_count_equal"] and r["sum_total_amount_equal"]
    assert r["identical"]


@pytest.mark.parametrize("buckets", [1, 2, 16])
def test_salted_is_correct_for_any_bucket_count(spark, staged, buckets):
    _, _, _, orders, customers = staged
    assert compare_to_baseline(spark, orders, customers, "salted", salt_buckets=buckets)["identical"]


def test_salt_is_deterministic_and_in_range(spark, staged):
    _, _, _, orders, _ = staged
    q = orders.select("order_id", salt_column(8).alias("s"))
    a, b = sorted(q.collect()), sorted(q.collect())
    assert a == b and all(0 <= r["s"] < 8 for r in a)
    assert len({r["s"] for r in a}) > 1


def test_unknown_mode_is_rejected(spark, staged):
    _, _, _, orders, customers = staged
    with pytest.raises(ValueError, match="unknown mode"):
        build_summary(orders, customers, "turbo")
    with pytest.raises(ValueError, match="unknown mode"):
        run(Config("x", "y", "z", mode="turbo"), spark)


def test_pipeline_end_to_end_with_salted_mode_matches_baseline_output(spark, staged):
    root, out, batch, _, _ = staged
    out_salt = out.parent / "out_salt"          # separate output: never invalidate the shared fixtures
    s = run(Config(str(root / "input/customers"), str(root / "input/orders"), str(out_salt), run_id="m_salt",
                   mode="salted", salt_buckets=4, shuffle_partitions=2, master="local[2]"), spark)
    assert s["status"] == "SUCCESS" and s["mode"] == "salted" and s["batch_id"] == batch
    base = read_batch(spark, out, "published/customer_order_summary", batch)
    salted = read_batch(spark, out_salt, "published/customer_order_summary", batch)
    assert salted.drop("run_id").exceptAll(base.drop("run_id")).count() == 0
    assert base.drop("run_id").exceptAll(salted.drop("run_id")).count() == 0
    assert salted.select("run_id").distinct().first()[0] == "m_salt"


def _aqe_plan_is_skewed(spark, orders, customers, conf_override):
    conf = {**MODE_CONF["aqe"], "spark.sql.shuffle.partitions": "8",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
            # test data is a few KB: the target piece size must be tiny or nothing can be cut in two
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1024", **conf_override}
    with temporary_conf(spark, conf):
        df = build_summary(orders, customers, "aqe")
        df.collect()
        plan = df._jdf.queryExecution().executedPlan().toString()
    return any("AQEShuffleRead" in l and "skewed" in l for l in plan.splitlines())


def test_aqe_needs_several_input_splits_and_the_force_flag(spark, staged):
    """Regression for two silent no-ops found while benchmarking."""
    _, _, _, orders, customers = staged
    many = scale_up(orders, 4)                      # 4 scan tasks -> 4 map outputs to split
    assert many.rdd.getNumPartitions() >= 4
    assert _aqe_plan_is_skewed(spark, many, customers, {}) is True
    # same query without the flag: Spark cancels the split (extra shuffle for the groupBy)
    assert _aqe_plan_is_skewed(spark, many, customers,
                               {"spark.sql.adaptive.forceOptimizeSkewedJoin": "false"}) is False
    # a single input split cannot be divided, whatever the thresholds
    if orders.rdd.getNumPartitions() == 1:
        assert _aqe_plan_is_skewed(spark, orders, customers, {}) is False


def test_scaled_input_still_matches_baseline_for_every_mode(spark, staged):
    _, _, _, orders, customers = staged
    many = scale_up(orders, 3)
    for mode in ("broadcast", "salted", "aqe"):
        assert compare_to_baseline(spark, many, customers, mode, salt_buckets=4)["identical"], mode
