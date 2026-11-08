"""Pipeline runner: ingest -> validate -> stage -> transform -> publish.

  python -m skew_pipeline.run_pipeline --customers data/sample/input/customers \
      --orders data/sample/input/orders --output output/sample [--mode baseline|broadcast|salted|aqe]

Output layout (every data table is partitioned by batch_id):
  staging/customers_valid, staging/orders_valid   validated, typed data
  rejects/                                        every rejected row, all entities
  published/customer_order_summary                analytical target
  run_log/<run_id>.json|.log                      one record per attempt

Restart / retry / backfill rules
  * Same input again  -> SKIPPED (already loaded), nothing is rewritten.
                         Use --force to reload; the batch partition is replaced, not appended.
  * Crashed / failed  -> the run log says RUNNING or FAILED, never SUCCESS, so the next
                         run of the same input reprocesses it from the start and overwrites
                         any partial files of that batch.
  * Backfill          -> run old files with --batch-id <label>; other batches are untouched.
  * Changed content   -> different fingerprint = different batch_id (unless --batch-id is
                         fixed, in which case the batch is replaced with the corrected data).
"""
import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .ingest import list_csv_files, read_raw
from .ledger import (file_fingerprint, find_successful_load, make_batch_id, read_run_history)
from .spark_utils import get_spark, temporary_conf
from .transform import DEFAULT_SALT_BUCKETS, MODE_CONF, MODES, build_summary
from .validate import validate_customers, validate_orders

log = logging.getLogger("skew_pipeline")


class ReconciliationError(RuntimeError):
    """Source-to-target counts or key aggregates do not reconcile."""


LOAD_TABLES = ("staging/customers_valid", "staging/orders_valid", "rejects", "published/customer_order_summary")


def _load_is_present(out: Path, batch_id: str) -> bool:
    """True only if every table partition of the batch exists on disk."""
    return all((out / t / f"batch_id={batch_id}").is_dir() for t in LOAD_TABLES)


class SimulatedFailure(RuntimeError):
    """Raised on purpose (--simulate-failure) to demonstrate retry behaviour."""


@dataclass
class Config:
    customers_path: str
    orders_path: str
    output_dir: str
    run_id: str | None = None
    batch_id: str | None = None
    mode: str = "broadcast"   # chosen mitigation, see docs/design_tradeoffs.md
    salt_buckets: int = DEFAULT_SALT_BUCKETS
    force: bool = False
    simulate_failure_at: str | None = None  # None | "after_staging"
    shuffle_partitions: int = 8
    master: str | None = "local[*]"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _save_log(out: Path, summary: dict) -> None:
    (out / "run_log").mkdir(parents=True, exist_ok=True)
    (out / "run_log" / f"{summary['run_id']}.json").write_text(json.dumps(summary, indent=2, default=str))


def _persist(spark, df, path: Path, batch_id: str):
    """Write df as partition batch_id (dynamic overwrite) and re-read exactly that partition."""
    tagged = df.withColumn("batch_id", F.lit(batch_id))
    tagged.write.mode("overwrite").partitionBy("batch_id").parquet(str(path))
    return spark.read.schema(tagged.schema).parquet(str(path)).where(F.col("batch_id") == batch_id).drop("batch_id")


def run(cfg: Config, spark: SparkSession | None = None) -> dict:
    if cfg.mode not in MODES:
        raise ValueError(f"unknown mode {cfg.mode!r} (available: {', '.join(MODES)})")
    spark = spark or get_spark(shuffle_partitions=cfg.shuffle_partitions, master=cfg.master)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # replace only touched batches
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    start = _now()
    run_id = cfg.run_id or f"run_{start:%Y%m%dT%H%M%S}_{start.microsecond // 1000:03d}Z"
    out = Path(cfg.output_dir)
    (out / "run_log").mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(out / "run_log" / f"{run_id}.log")
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(file_handler)
    log.setLevel(logging.INFO)

    summary = {
        "run_id": run_id, "batch_id": None, "attempt": None, "input_fingerprint": None,
        "mode": cfg.mode, "status": "RUNNING", "start_time": _iso(start), "end_time": None,
        "duration_seconds": None, "failure_reason": None, "skipped_reason": None,
        "previous_run_id": None, "replaced_existing_load": False,
        "config": asdict(cfg), "spark_version": spark.version,
        "files": {}, "counts": {}, "rejected_by_reason": {}, "timings_seconds": {},
    }
    try:
        files = {"customers": list_csv_files(cfg.customers_path), "orders": list_csv_files(cfg.orders_path)}
        fingerprint = file_fingerprint(files)
        batch_id = make_batch_id(fingerprint, cfg.batch_id)
        history = read_run_history(out, batch_id)  # read BEFORE this run registers itself
        prior = find_successful_load(history, fingerprint)
        summary.update(batch_id=batch_id, input_fingerprint=fingerprint, attempt=len(history) + 1)
        _save_log(out, summary)  # a crash from here on leaves a RUNNING record behind
        log.info("run %s started: batch=%s attempt=%d mode=%s", run_id, batch_id, summary["attempt"], cfg.mode)

        data_present = _load_is_present(out, batch_id)
        if prior and not data_present:
            log.warning("run log says batch %s was loaded by %s but its data is missing; reloading",
                        batch_id, prior["run_id"])
        if prior and data_present and not cfg.force:
            summary.update(status="SKIPPED", previous_run_id=prior["run_id"], counts=prior["counts"],
                           skipped_reason=f"batch {batch_id} with identical input already loaded by "
                                          f"{prior['run_id']}; use --force to reload")
            log.info(summary["skipped_reason"])
            return summary
        summary["replaced_existing_load"] = bool(prior) and data_present

        # 1. ingest + validate (default Spark settings), persist to staging
        t0 = time.perf_counter()
        raw_c, info_c = read_raw(spark, "customers", cfg.customers_path)
        raw_o, info_o = read_raw(spark, "orders", cfg.orders_path)
        summary["files"] = {"customers": info_c, "orders": info_o}
        for kind, info in (("customers", info_c), ("orders", info_o)):
            if not info["files_read"]:
                log.warning("no readable %s files (empty batch)", kind)
            for bad in info["files_rejected"]:
                log.error("quarantined %s file %s: %s", kind, bad["file"], bad["reason"])

        cust_in, ord_in = raw_c.count(), raw_o.count()
        cust_valid, cust_rej = validate_customers(raw_c, run_id)
        staged_customers = _persist(spark, cust_valid, out / "staging/customers_valid", batch_id)
        ord_valid, ord_rej = validate_orders(raw_o, staged_customers, run_id)
        staged_orders = _persist(spark, ord_valid, out / "staging/orders_valid", batch_id)
        rejects = _persist(spark, cust_rej.unionByName(ord_rej), out / "rejects", batch_id)
        summary["timings_seconds"]["validate"] = round(time.perf_counter() - t0, 3)

        by_reason = {(r["entity"], r["reject_reason"]): r["n"]
                     for r in rejects.groupBy("entity", "reject_reason").agg(F.count("*").alias("n")).collect()}
        cust_rej_n = sum(n for (e, _), n in by_reason.items() if e == "customers")
        ord_rej_n = sum(n for (e, _), n in by_reason.items() if e == "orders")
        cust_valid_n, ord_valid_n = staged_customers.count(), staged_orders.count()
        summary["rejected_by_reason"] = {
            e: {r: n for (ee, r), n in sorted(by_reason.items()) if ee == e} for e in ("customers", "orders")}

        # invariant: every input row is either valid or rejected, never lost
        if cust_in != cust_valid_n + cust_rej_n:
            raise ReconciliationError(f"customers: in={cust_in} != valid={cust_valid_n} + rejected={cust_rej_n}")
        if ord_in != ord_valid_n + ord_rej_n:
            raise ReconciliationError(f"orders: in={ord_in} != valid={ord_valid_n} + rejected={ord_rej_n}")

        if cfg.simulate_failure_at == "after_staging":
            raise SimulatedFailure("simulated crash after staging, before publishing")

        # 2. transform + publish with the chosen strategy
        t1 = time.perf_counter()
        with temporary_conf(spark, MODE_CONF[cfg.mode]):
            result = build_summary(staged_orders, staged_customers, cfg.mode, cfg.salt_buckets) \
                .withColumn("run_id", F.lit(run_id))
            published = _persist(spark, result, out / "published/customer_order_summary", batch_id)
            agg = published.agg(F.coalesce(F.sum("order_count"), F.lit(0)).alias("n"),
                                F.sum("total_amount").alias("amt")).first()
            published_customers = published.count()
        summary["timings_seconds"]["transform_and_publish"] = round(time.perf_counter() - t1, 3)

        staged_amt = staged_orders.agg(F.sum("amount")).first()[0]
        if agg["n"] != ord_valid_n:
            raise ReconciliationError(f"published order_count {agg['n']} != valid orders {ord_valid_n}")
        if agg["amt"] != staged_amt:
            raise ReconciliationError(f"published total_amount {agg['amt']} != staged {staged_amt}")

        summary["counts"] = {
            "customers_input": cust_in, "customers_valid": cust_valid_n, "customers_rejected": cust_rej_n,
            "orders_input": ord_in, "orders_valid": ord_valid_n, "orders_rejected": ord_rej_n,
            "published_customers": published_customers,
            "published_total_amount": str(agg["amt"]) if agg["amt"] is not None else "0",
        }
        summary["status"] = "SUCCESS"
        log.info("run %s succeeded: %s", run_id, summary["counts"])
    except Exception as exc:  # recorded in the run log, then re-raised
        summary["status"] = "FAILED"
        summary["failure_reason"] = f"{type(exc).__name__}: {exc}"
        log.exception("run %s failed", run_id)
        raise
    finally:
        end = _now()
        summary["end_time"] = _iso(end)
        summary["duration_seconds"] = round((end - start).total_seconds(), 3)
        _save_log(out, summary)
        log.removeHandler(file_handler)
        file_handler.close()
    return summary


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--customers", required=True, help="customers csv file or directory")
    p.add_argument("--orders", required=True, help="orders csv file or directory")
    p.add_argument("--output", required=True)
    p.add_argument("--run-id", default=None)
    p.add_argument("--batch-id", default=None, help="override the content-derived batch id (backfills)")
    p.add_argument("--mode", default="broadcast", choices=MODES,
                   help="join strategy (default: broadcast; baseline reproduces the skewed run)")
    p.add_argument("--salt-buckets", type=int, default=DEFAULT_SALT_BUCKETS)
    p.add_argument("--force", action="store_true", help="reload even if the same input was already loaded")
    p.add_argument("--simulate-failure", choices=["after_staging"], default=None,
                   help="crash on purpose to demonstrate retry")
    p.add_argument("--shuffle-partitions", type=int, default=8)
    p.add_argument("--master", default="local[*]")
    a = p.parse_args()
    s = run(Config(a.customers, a.orders, a.output, a.run_id, a.batch_id, a.mode, a.salt_buckets,
                   a.force, a.simulate_failure, a.shuffle_partitions, a.master))
    print(json.dumps({k: s[k] for k in ("run_id", "batch_id", "attempt", "status", "skipped_reason",
                                        "counts", "timings_seconds")}, indent=2))


if __name__ == "__main__":
    main()
