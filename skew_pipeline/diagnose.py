"""Skew diagnosis: WHERE is the imbalance, and how big is it?

Two pieces of evidence, both independent of wall-clock noise:
  1. key distribution       - how many rows each customer_id owns
  2. partition distribution - how many rows each shuffle partition receives when
                              Spark hash-partitions by customer_id (this is what
                              a sort-merge join does), before and after salting.
A healthy shuffle has max/mean close to 1. A skewed one has one partition many
times larger than the average; that single task is the bottleneck.

Usage:
  python -m skew_pipeline.diagnose --output output/generated [--batch-id b_xxx]
"""
import argparse
import json
import statistics
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .ledger import latest_successful_batch, read_batch
from .spark_utils import get_spark
from .transform import DEFAULT_SALT_BUCKETS, salt_column


def key_distribution(df: DataFrame, key: str = "customer_id", top_n: int = 5) -> dict:
    total = df.count()
    per_key = df.groupBy(key).count()
    distinct = per_key.count()
    top = per_key.orderBy(F.desc("count"), key).limit(top_n).collect()
    mean = total / distinct if distinct else 0
    return {
        "total_rows": total,
        "distinct_keys": distinct,
        "mean_rows_per_key": round(mean, 2),
        "top_keys": [{"key": r[key], "rows": r["count"],
                      "share_of_rows": round(r["count"] / total, 4) if total else 0} for r in top],
        "top_key_to_mean_ratio": round(top[0]["count"] / mean, 1) if top and mean else None,
    }


def partition_distribution(df: DataFrame, n_partitions: int, cols: list[str]) -> dict:
    """Rows per partition after hash-partitioning by `cols` (what a shuffle join does)."""
    rows = (df.repartition(n_partitions, *cols)
              .groupBy(F.spark_partition_id().alias("p")).count().collect())
    sizes = [0] * n_partitions
    for r in rows:
        sizes[r["p"]] = r["count"]
    total, mean = sum(sizes), (sum(sizes) / n_partitions)
    return {
        "partition_columns": cols,
        "partitions": n_partitions,
        "rows_per_partition": sizes,
        "max_rows": max(sizes),
        "median_rows": statistics.median(sizes),
        "max_to_mean_ratio": round(max(sizes) / mean, 2) if mean else None,
        "largest_partition_share": round(max(sizes) / total, 4) if total else None,
    }


def diagnose(orders: DataFrame, n_partitions: int = 8, salt_buckets: int = DEFAULT_SALT_BUCKETS) -> dict:
    before = partition_distribution(orders, n_partitions, ["customer_id"])
    salted = orders.withColumn("salt", salt_column(salt_buckets))
    after = partition_distribution(salted, n_partitions, ["customer_id", "salt"])
    keys = key_distribution(orders)
    hot = keys["top_keys"][0] if keys["top_keys"] else None
    verdict = (
        f"SKEWED: key {hot['key']} holds {hot['share_of_rows']:.0%} of rows; one of {n_partitions} "
        f"shuffle partitions receives {before['largest_partition_share']:.0%} of all rows "
        f"({before['max_to_mean_ratio']}x the average)."
        if hot and before["max_to_mean_ratio"] and before["max_to_mean_ratio"] >= 2
        else "No significant skew detected.")
    return {"verdict": verdict, "key_distribution": keys,
            "partitioning_before_salting": before,
            "partitioning_after_salting": {**after, "salt_buckets": salt_buckets}}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", required=True, help="pipeline output dir")
    p.add_argument("--batch-id", default=None, help="default: latest successful batch")
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--salt-buckets", type=int, default=DEFAULT_SALT_BUCKETS)
    p.add_argument("--master", default="local[*]")
    a = p.parse_args()
    batch = a.batch_id or latest_successful_batch(a.output)
    if not batch:
        raise SystemExit("no successful batch found; run the pipeline first")
    spark = get_spark("skew_diagnose", a.partitions, a.master)
    report = diagnose(read_batch(spark, a.output, "staging/orders_valid", batch), a.partitions, a.salt_buckets)
    report["batch_id"] = batch
    target = Path(a.output) / "diagnosis"
    target.mkdir(parents=True, exist_ok=True)
    (target / f"diagnosis_{batch}.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
