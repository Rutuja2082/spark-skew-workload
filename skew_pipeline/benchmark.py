"""Benchmark the four execution strategies on already-validated data.

  python -m skew_pipeline.benchmark --output output/generated --repeats 3

For every mode it reports
  * wall time (median / min / max over the measured repeats),
  * the heaviest stage's longest vs median task (max/median = straggler signature),
  * correctness against the baseline result (row counts, key aggregates and a
    full two-way EXCEPT ALL, which must both be empty).

Fairness rules
  * data is read from the pipeline's staging tables; validation is NOT timed
  * results are written with the `noop` sink: the whole plan runs, but disk
    write speed does not blur the comparison
  * warm-up rounds are discarded; measured rounds are interleaved across modes
    (A B C D, A B C D, ...) so machine drift hits every mode equally
  * correctness checks run outside the timed section
"""
import argparse
import json
import os
import statistics
import time
from pathlib import Path

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .diagnose import diagnose
from .ledger import latest_successful_batch, read_batch
from .metrics import stage_task_skew
from .spark_utils import get_spark, temporary_conf
from .transform import DEFAULT_SALT_BUCKETS, MODE_CONF, MODES, build_summary


def _plan(orders: DataFrame, customers: DataFrame, mode: str, salt_buckets: int) -> DataFrame:
    return build_summary(orders, customers, mode, salt_buckets).withColumn("run_id", F.lit("bench"))


def time_mode(spark: SparkSession, orders, customers, mode: str, salt_buckets: int, tag: str) -> dict:
    """Execute one full run of `mode` and return wall time + task evidence."""
    group = f"bench-{tag}-{mode}"
    spark.sparkContext.setJobGroup(group, f"benchmark {mode}")
    with temporary_conf(spark, MODE_CONF[mode]):
        t0 = time.perf_counter()
        _plan(orders, customers, mode, salt_buckets).write.format("noop").mode("overwrite").save()
        wall = time.perf_counter() - t0
    return {"wall_seconds": round(wall, 3), "task_skew": stage_task_skew(spark, group)}


def aqe_skew_split_applied(spark: SparkSession, orders, customers, salt_buckets: int) -> bool:
    """Execute the aqe plan once and check Spark really split a skewed partition.

    A mode that is merely *configured* for skew handling proves nothing; the final
    adaptive plan must contain a skewed shuffle read.
    """
    with temporary_conf(spark, MODE_CONF["aqe"]):
        df = build_summary(orders, customers, "aqe", salt_buckets)
        df.collect()
        plan = df._jdf.queryExecution().executedPlan().toString()
    return any("AQEShuffleRead" in line and "skewed" in line for line in plan.splitlines())


def scale_up(orders: DataFrame, factor: int) -> DataFrame:
    """Repeat the orders `factor` times (for TIMING only: order_ids repeat).

    Gives a bigger workload and several input splits, like a real multi-file table.
    Every mode sees the same scaled input, so the equivalence check stays valid.
    """
    return orders if factor <= 1 else reduce(lambda acc, _: acc.union(orders), range(factor - 1), orders)


def compare_to_baseline(spark: SparkSession, orders, customers, mode: str, salt_buckets: int) -> dict:
    """Prove `mode` returns exactly the baseline result."""
    with temporary_conf(spark, MODE_CONF["baseline"]):
        base = _plan(orders, customers, "baseline", salt_buckets).cache()
        base_n = base.count()
    try:
        with temporary_conf(spark, MODE_CONF[mode]):
            other = _plan(orders, customers, mode, salt_buckets)
            only_in_mode = other.exceptAll(base).count()
            only_in_base = base.exceptAll(other).count()
            other_n = other.count()
            same_schema = other.schema == base.schema
            mode_sum = other.agg(F.sum("order_count"), F.sum("total_amount")).first()
            base_sum = base.agg(F.sum("order_count"), F.sum("total_amount")).first()
    finally:
        base.unpersist()
    return {
        "rows": other_n, "baseline_rows": base_n,
        "rows_only_in_mode": only_in_mode, "rows_only_in_baseline": only_in_base,
        "schema_identical": same_schema,
        "sum_order_count_equal": mode_sum[0] == base_sum[0],
        "sum_total_amount_equal": mode_sum[1] == base_sum[1],
        "identical": (only_in_mode == 0 and only_in_base == 0 and other_n == base_n and same_schema
                      and mode_sum[0] == base_sum[0] and mode_sum[1] == base_sum[1]),
    }


def run_benchmark(spark: SparkSession, orders, customers, modes=MODES, repeats: int = 3, warmup: int = 1,
                  salt_buckets: int = DEFAULT_SALT_BUCKETS, partitions: int = 8, scale: int = 1) -> dict:
    orders = scale_up(orders, scale)           # not cached: every mode pays the same scan cost
    n_orders, n_customers = orders.count(), customers.count()
    diagnosis = diagnose(orders, partitions, salt_buckets)

    samples = {m: [] for m in modes}
    for rnd in range(warmup + repeats):
        for m in modes:  # interleaved
            res = time_mode(spark, orders, customers, m, salt_buckets, f"r{rnd}")
            if rnd >= warmup:
                samples[m].append(res)

    results = {}
    for m in modes:
        walls = [s["wall_seconds"] for s in samples[m]]
        skews = [s["task_skew"] for s in samples[m] if s["task_skew"]]
        # use the repeat whose wall time is the median one for the task evidence
        pick = sorted((s for s in samples[m] if s["task_skew"]), key=lambda s: s["wall_seconds"])
        task = pick[len(pick) // 2]["task_skew"] if pick else None
        results[m] = {
            "wall_seconds_median": round(statistics.median(walls), 3),
            "wall_seconds_min": min(walls), "wall_seconds_max": max(walls),
            "wall_seconds_all": walls,
            "heaviest_stage": task,
            "correctness_vs_baseline": (None if m == "baseline"
                                        else compare_to_baseline(spark, orders, customers, m, salt_buckets)),
            "aqe_skew_split_applied": (aqe_skew_split_applied(spark, orders, customers, salt_buckets)
                                       if m == "aqe" else None),
        }
    base = results["baseline"]["wall_seconds_median"] if "baseline" in results else None
    for m in modes:
        results[m]["speedup_vs_baseline"] = (round(base / results[m]["wall_seconds_median"], 2)
                                             if base else None)
    return {
        "environment": {"spark_version": spark.version, "master": spark.sparkContext.master,
                        "cpu_count": os.cpu_count(), "shuffle_partitions": partitions,
                        "salt_buckets": salt_buckets},
        "data": {"valid_orders": n_orders, "valid_customers": n_customers, "scale_factor": scale},
        "protocol": {"warmup_rounds": warmup, "measured_rounds": repeats, "sink": "noop", "interleaved": True},
        "diagnosis": diagnosis,
        "results": results,
    }


def to_markdown(rep: dict, batch_id: str | None = None) -> str:
    env, data, d = rep["environment"], rep["data"], rep["diagnosis"]
    lines = [f"# Skew benchmark{f' (batch {batch_id})' if batch_id else ''}", "",
             f"- Data: {data['valid_orders']:,} valid orders"
             f"{' (staged data repeated ' + str(data['scale_factor']) + 'x for timing)' if data['scale_factor'] > 1 else ''}"
             f", {data['valid_customers']:,} customers",
             f"- Environment: Spark {env['spark_version']}, `{env['master']}`, {env['cpu_count']} CPU core(s), "
             f"{env['shuffle_partitions']} shuffle partitions, {env['salt_buckets']} salt buckets",
             f"- Protocol: {rep['protocol']['warmup_rounds']} warm-up + {rep['protocol']['measured_rounds']} "
             f"measured rounds, interleaved, `noop` sink",
             "", "## Diagnosis", "", d["verdict"], ""]
    b, a = d["partitioning_before_salting"], d["partitioning_after_salting"]
    lines += [f"Rows per shuffle partition by `customer_id`: {b['rows_per_partition']} "
              f"(max/mean {b['max_to_mean_ratio']}x).",
              f"After salting (`customer_id`, salt): {a['rows_per_partition']} (max/mean {a['max_to_mean_ratio']}x).",
              "", "## Runtime", "",
              "| mode | wall median (s) | min | max | speedup | longest task (s) | median task (s) | "
              "max/median task | identical to baseline |",
              "|---|---|---|---|---|---|---|---|---|"]
    for m, r in rep["results"].items():
        t = r["heaviest_stage"] or {}
        c = r["correctness_vs_baseline"]
        ident = "n/a (reference)" if c is None else ("yes" if c["identical"] else "**NO**")
        lines.append(
            f"| {m} | {r['wall_seconds_median']} | {r['wall_seconds_min']} | {r['wall_seconds_max']} | "
            f"{r['speedup_vs_baseline']}x | "
            f"{round(t['max_task_ms'] / 1000, 2) if t else 'n/a'} | "
            f"{round(t['median_task_ms'] / 1000, 2) if t else 'n/a'} | "
            f"{t.get('max_to_median_ratio', 'n/a')} | {ident} |")
    lines += ["", "## Correctness detail", "",
              "| mode | rows | rows only in mode | rows only in baseline | schema identical | "
              "sum(order_count) equal | sum(total_amount) equal |", "|---|---|---|---|---|---|---|"]
    for m, r in rep["results"].items():
        c = r["correctness_vs_baseline"]
        if c:
            lines.append(f"| {m} | {c['rows']} | {c['rows_only_in_mode']} | {c['rows_only_in_baseline']} | "
                         f"{c['schema_identical']} | {c['sum_order_count_equal']} | {c['sum_total_amount_equal']} |")
    aqe = rep["results"].get("aqe")
    if aqe:
        lines += ["", f"AQE skew split actually applied in the final plan: "
                      f"**{'yes' if aqe['aqe_skew_split_applied'] else 'NO (mode was configured but Spark did not split)'}**."]
    lines += ["", "## How to read this", "",
              "- **Wall time** is total elapsed time. On a machine with few cores it mostly reflects total work; "
              "the benefit of removing a straggler appears when there are more free cores than shuffle partitions.",
              "- **Longest task vs median task** in the heaviest stage is independent of core count: a stage "
              "cannot finish before its longest task, so that value is a lower bound for the stage time on a "
              "cluster with enough cores.",
              "- Numbers are specific to this machine and data size; re-run to reproduce.", ""]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", required=True, help="pipeline output dir (reads its staging tables)")
    p.add_argument("--batch-id", default=None, help="default: latest successful batch")
    p.add_argument("--modes", nargs="+", default=list(MODES), choices=MODES)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--salt-buckets", type=int, default=DEFAULT_SALT_BUCKETS)
    p.add_argument("--scale", type=int, default=1, help="repeat the staged orders N times (timing only)")
    p.add_argument("--master", default="local[*]")
    a = p.parse_args()
    if "baseline" not in a.modes:
        a.modes.insert(0, "baseline")  # every comparison needs the reference
    batch = a.batch_id or latest_successful_batch(a.output)
    if not batch:
        raise SystemExit("no successful batch found; run the pipeline first")
    spark = get_spark("skew_benchmark", a.partitions, a.master)
    orders = read_batch(spark, a.output, "staging/orders_valid", batch)
    customers = read_batch(spark, a.output, "staging/customers_valid", batch)
    rep = run_benchmark(spark, orders, customers, a.modes, a.repeats, a.warmup, a.salt_buckets, a.partitions, a.scale)
    rep["batch_id"] = batch
    target = Path(a.output) / "benchmark"
    target.mkdir(parents=True, exist_ok=True)
    (target / f"benchmark_{batch}_x{a.scale}.json").write_text(json.dumps(rep, indent=2, default=str))
    md = to_markdown(rep, batch)
    (target / f"benchmark_{batch}_x{a.scale}.md").write_text(md)
    print(md)


if __name__ == "__main__":
    main()
