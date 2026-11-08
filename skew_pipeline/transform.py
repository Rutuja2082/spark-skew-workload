"""Per-customer order summary in four execution strategies.

All four modes produce IDENTICAL results (same rows, same values, same types);
they differ only in how Spark executes the join and aggregation:

  baseline   plain sort-merge join on customer_id. The hot key lands in ONE
             shuffle partition, so one task does most of the work (the problem).
  broadcast  ship the small customers table to every executor and join locally.
             Orders are not shuffled for the join, so the hot key cannot pile up.
             Works only while the dimension fits in memory (here: yes).
  salted     split every key into `salt_buckets` sub-keys: orders get a
             deterministic salt, customers are replicated once per salt value,
             join on (customer_id, salt), then aggregate in two stages.
             Works when BOTH sides are large. Cost: the small side grows
             `salt_buckets` times and there is an extra aggregation stage.
  aqe        Spark's Adaptive Query Execution splits oversized shuffle
             partitions of a sort-merge join at run time (needs no code change).
             Two things were found necessary by experiment, and both are easy
             to miss:
               1. the thresholds must be lowered for laptop-sized data
                  (defaults: 256 MB / factor 5);
               2. forceOptimizeSkewedJoin must be on, because the aggregation
                  after the join groups by the same key: splitting the hot
                  partition would add a shuffle for that aggregation, and
                  Spark silently cancels the optimisation unless forced.
             It also splits by map-task output, so it needs SEVERAL input
             splits: a single small input file (one map task) cannot be split.

Baseline, broadcast and salted run with AQE off and auto-broadcast off, so the
only thing that differs between them is the technique being tested.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MODES = ("baseline", "broadcast", "salted", "aqe")
DEFAULT_SALT_BUCKETS = 16
KEYS = ["customer_id", "country", "segment"]

_FORCE_SHUFFLE_JOIN = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
}
MODE_CONF = {
    "baseline": _FORCE_SHUFFLE_JOIN,
    "broadcast": _FORCE_SHUFFLE_JOIN,  # broadcast is requested explicitly with a hint
    "salted": _FORCE_SHUFFLE_JOIN,
    "aqe": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        # defaults (256 MB / factor 5) never trigger on laptop-sized data
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1048576",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1048576",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
    },
}


def salt_column(salt_buckets: int) -> Column:
    """Deterministic salt in [0, salt_buckets): same order_id -> same salt on every run."""
    return F.pmod(F.hash("order_id"), F.lit(salt_buckets))


def _aggregate(joined: DataFrame) -> DataFrame:
    return joined.groupBy(*KEYS).agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("total_amount"),
        F.min("order_ts").alias("first_order_ts"),
        F.max("order_ts").alias("last_order_ts"),
    )


def _finalize(df: DataFrame) -> DataFrame:
    """Give every mode the identical output schema (types AND nullability)."""
    return (
        # sum(decimal(12,2)) is decimal(22,2); two-stage sums widen it, so pin the type
        df.withColumn("total_amount", F.col("total_amount").cast("decimal(22,2)"))
        # count() is NOT NULL but sum(count) is nullable; an inner-join group always has >= 1 row
        .withColumn("order_count", F.coalesce(F.col("order_count"), F.lit(0).cast("long")))
    )


def build_summary(orders: DataFrame, customers: DataFrame, mode: str = "baseline",
                  salt_buckets: int = DEFAULT_SALT_BUCKETS) -> DataFrame:
    """Return the per-customer summary (without run metadata). Lazy: nothing runs yet."""
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; choose from {MODES}")
    dim = customers.select("customer_id", "country", "segment")

    if mode in ("baseline", "aqe"):
        return _finalize(_aggregate(orders.join(dim, "customer_id", "inner")))

    if mode == "broadcast":
        return _finalize(_aggregate(orders.join(F.broadcast(dim), "customer_id", "inner")))

    # salted: two-stage aggregation
    salted_orders = orders.withColumn("salt", salt_column(salt_buckets))
    salted_dim = dim.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(salt_buckets - 1))))
    joined = salted_orders.join(salted_dim, ["customer_id", "salt"], "inner")
    partial = joined.groupBy(*KEYS, "salt").agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("total_amount"),
        F.min("order_ts").alias("first_order_ts"),
        F.max("order_ts").alias("last_order_ts"),
    )
    final = partial.groupBy(*KEYS).agg(
        F.sum("order_count").alias("order_count"),
        F.sum("total_amount").alias("total_amount"),
        F.min("first_order_ts").alias("first_order_ts"),
        F.max("last_order_ts").alias("last_order_ts"),
    )
    return _finalize(final)
