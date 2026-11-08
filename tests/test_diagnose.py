"""Skew diagnosis: the numbers must expose a hot key and show salting fixing it."""
from skew_pipeline.diagnose import diagnose, key_distribution, partition_distribution


def _orders(spark, hot_rows, other_keys, rows_each=1):
    rows, i = [], 0
    for _ in range(hot_rows):
        i += 1
        rows.append((f"O{i}", "HOT"))
    for k in range(other_keys):
        for _ in range(rows_each):
            i += 1
            rows.append((f"O{i}", f"C{k}"))
    return spark.createDataFrame(rows, ["order_id", "customer_id"])


def test_key_distribution_finds_the_hot_key(spark):
    d = key_distribution(_orders(spark, 800, 200), top_n=3)
    assert d["total_rows"] == 1000 and d["distinct_keys"] == 201
    assert d["top_keys"][0]["key"] == "HOT" and d["top_keys"][0]["share_of_rows"] == 0.8


def test_partitions_are_imbalanced_for_skewed_keys(spark):
    p = partition_distribution(_orders(spark, 800, 200), 8, ["customer_id"])
    assert sum(p["rows_per_partition"]) == 1000
    assert p["max_rows"] >= 800                      # the hot key cannot be split by hashing
    assert p["max_to_mean_ratio"] >= 5


def test_verdict_flags_skew_and_salting_balances_it(spark):
    rep = diagnose(_orders(spark, 800, 200), n_partitions=8, salt_buckets=16)
    assert rep["verdict"].startswith("SKEWED")
    before, after = rep["partitioning_before_salting"], rep["partitioning_after_salting"]
    assert after["max_to_mean_ratio"] < before["max_to_mean_ratio"] / 2
    assert sum(after["rows_per_partition"]) == sum(before["rows_per_partition"]) == 1000


def test_uniform_data_is_not_flagged(spark):
    rep = diagnose(_orders(spark, 0, 200, rows_each=20), n_partitions=8)
    assert rep["verdict"] == "No significant skew detected."
