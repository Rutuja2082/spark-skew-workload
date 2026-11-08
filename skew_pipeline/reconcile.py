"""Independent reconciliation of one loaded batch.

  python -m skew_pipeline.reconcile --output output/generated [--batch-id b_xxx]

The pipeline asserts a few invariants while it runs. This module re-checks the
RESULT from the outside, using different code paths on purpose:
  * raw input rows are counted straight from the CSV files (plain Python, not Spark's CSV reader)
  * the published summary is recomputed with plain SQL and compared row by row
  * expected-answer labels (kept apart from the input data) are compared when found

Every check reports PASS / FAIL / SKIP with expected and actual values. A FAIL
carries the numbers that explain the difference. Exit code 1 if any check fails.
Writes <output>/reconciliation/reconciliation_<batch>.json and .md.
"""
import argparse
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .ledger import file_fingerprint, latest_successful_batch, read_batch, read_run_history
from .schemas import CUSTOMER_COLUMNS, ORDER_COLUMNS, REJECT_REASONS, REJECT_STAGES
from .spark_utils import get_spark

ENTITY_KEYS = {"customers": "customer_id", "orders": "order_id"}
ENTITY_COLUMNS = {"customers": CUSTOMER_COLUMNS, "orders": ORDER_COLUMNS}


class Report:
    def __init__(self):
        self.checks: list[dict] = []

    def add(self, cid, name, ok, expected=None, actual=None, detail=""):
        status = "SKIP" if ok is None else ("PASS" if ok else "FAIL")
        self.checks.append({"id": cid, "check": name, "status": status,
                            "expected": None if expected is None else str(expected),
                            "actual": None if actual is None else str(actual), "detail": detail})

    @property
    def failed(self):
        return [c for c in self.checks if c["status"] == "FAIL"]


def count_data_rows(files: list[str]) -> int:
    """Non-blank lines minus one header per file; independent of Spark's CSV parser."""
    total = 0
    for f in files:
        with open(f, encoding="utf-8-sig") as fh:
            total += max(sum(1 for line in fh if line.strip()) - 1, 0)
    return total


def _names(paths) -> set[str]:
    return {Path(str(p).replace("file:", "", 1)).name for p in paths}


def _discover_expected(orders_path: str | None) -> Path | None:
    if not orders_path:
        return None
    for parent in Path(orders_path).resolve().parents[:3]:
        cand = parent / "expected" / "expected_results.json"
        if cand.exists():
            return cand
    return None


def _recompute_summary(spark, customers: DataFrame, orders: DataFrame) -> DataFrame:
    """The published table, recomputed with plain SQL (not the transform module)."""
    customers.createOrReplaceTempView("rc_customers")
    orders.createOrReplaceTempView("rc_orders")
    return spark.sql("""
        SELECT o.customer_id, c.country, c.segment,
               COUNT(*) AS order_count,
               CAST(SUM(o.amount) AS DECIMAL(22,2)) AS total_amount,
               MIN(o.order_ts) AS first_order_ts, MAX(o.order_ts) AS last_order_ts
        FROM rc_orders o JOIN rc_customers c ON o.customer_id = c.customer_id
        GROUP BY o.customer_id, c.country, c.segment""")


def reconcile(spark: SparkSession, out_dir: str, batch_id: str | None = None,
              customers_path: str | None = None, orders_path: str | None = None,
              expected_path: str | None = None) -> dict:
    out = Path(out_dir)
    batch = batch_id or latest_successful_batch(out)
    if not batch:
        raise SystemExit("no successful batch found; run the pipeline first")
    history = [r for r in read_run_history(out, batch) if r.get("status") == "SUCCESS"]
    if not history:
        raise SystemExit(f"batch {batch} has no SUCCESS run to reconcile")
    log = history[-1]
    rep = Report()
    cfg = log.get("config", {})
    customers_path = customers_path or cfg.get("customers_path")
    orders_path = orders_path or cfg.get("orders_path")

    cust = read_batch(spark, out, "staging/customers_valid", batch)
    ords = read_batch(spark, out, "staging/orders_valid", batch)
    rej = read_batch(spark, out, "rejects", batch)
    pub = read_batch(spark, out, "published/customer_order_summary", batch)
    valid = {"customers": cust, "orders": ords}

    # ---- R0 / R1: input rows vs valid + rejected (counted independently)
    input_rows, flow = {}, {}
    sources_ok = all(Path(f).exists() for e in ENTITY_KEYS for f in log["files"][e]["files_read"])
    for entity in ENTITY_KEYS:
        files = log["files"][entity]["files_read"]
        n_valid = valid[entity].count()
        by_stage = {r["reject_stage"]: r["n"] for r in rej.where(F.col("entity") == entity)
                    .groupBy("reject_stage").agg(F.count("*").alias("n")).collect()}
        n_rej = sum(by_stage.values())
        flow[entity] = {"valid": n_valid, "rejected": n_rej,
                        **{f"rejected_{s}": by_stage.get(s, 0) for s in sorted(REJECT_STAGES)}}
        if sources_ok:
            input_rows[entity] = count_data_rows(files)
            flow[entity]["input"] = input_rows[entity]
            rep.add(f"R1-{entity}", f"{entity}: input rows = valid + rejected", input_rows[entity] == n_valid + n_rej,
                    input_rows[entity], n_valid + n_rej,
                    f"valid={n_valid}, rejected={n_rej} (" + ", ".join(
                        f"{s}={by_stage.get(s, 0)}" for s in sorted(REJECT_STAGES)) + ")")
        else:
            rep.add(f"R1-{entity}", f"{entity}: input rows = valid + rejected", None,
                    detail="input files are no longer available at the logged path; pass --customers/--orders")
        quarantined = log["files"][entity]["files_rejected"]
        if quarantined:
            rep.add(f"R1q-{entity}", f"{entity}: quarantined files (explained, not counted)", True,
                    detail="; ".join(f"{Path(q['file']).name}: {q['reason']}" for q in quarantined))

    # ---- R0: input unchanged since the load
    if sources_ok and log.get("input_fingerprint"):
        files = {"customers": [Path(f) for f in log["files"]["customers"]["files_read"]],
                 "orders": [Path(f) for f in log["files"]["orders"]["files_read"]]}
        # the batch fingerprint also covers quarantined files, so include them
        for e in files:
            files[e] += [Path(q["file"]) for q in log["files"][e]["files_rejected"]]
        now = file_fingerprint(files)
        rep.add("R0", "input files unchanged since the load", now == log["input_fingerprint"],
                log["input_fingerprint"][:12], now[:12])

    # ---- keys, nulls, referential integrity
    for entity, key in ENTITY_KEYS.items():
        df = valid[entity]
        n, d = df.count(), df.select(key).distinct().count()
        rep.add(f"K-{entity}", f"{entity}: primary key {key} unique", n == d, n, d, f"{n - d} duplicate key rows")
        nulls = df.where(" OR ".join(f"{c} IS NULL" for c in ENTITY_COLUMNS[entity])).count()
        rep.add(f"N-{entity}", f"{entity}: no NULL in required columns", nulls == 0, 0, nulls)
    orphans = ords.join(cust.select("customer_id"), "customer_id", "left_anti").count()
    rep.add("FK", "orders.customer_id exists in valid customers", orphans == 0, 0, orphans)

    # ---- published = independent recomputation
    rec = _recompute_summary(spark, cust, ords)
    cols = ["customer_id", "country", "segment", "order_count", "total_amount", "first_order_ts", "last_order_ts"]
    only_pub = pub.select(*cols).exceptAll(rec.select(*cols)).count()
    only_rec = rec.select(*cols).exceptAll(pub.select(*cols)).count()
    rep.add("P1", "published summary equals independent SQL recomputation (two-way EXCEPT ALL)",
            only_pub == 0 and only_rec == 0, "0 / 0", f"{only_pub} / {only_rec}",
            "rows only in published / rows only in recomputation")
    agg = pub.agg(F.sum("order_count").alias("n"), F.sum("total_amount").alias("amt")).first()
    stg = ords.agg(F.count("*").alias("n"), F.sum("amount").alias("amt")).first()
    pn, sn = agg["n"] or 0, stg["n"] or 0
    rep.add("P2", "sum(order_count) = valid orders", pn == sn, sn, pn)
    pa, sa = agg["amt"] or Decimal(0), stg["amt"] or Decimal(0)
    rep.add("P3", "sum(total_amount) = sum(valid order amount) (exact decimal)", pa == sa, sa, pa)

    # ---- rejects: traceable, known vocabulary, winners exist for duplicates
    untraceable = rej.where(F.col("source_file").isNull() | (F.trim("source_file") == "") |
                            F.col("raw_record").isNull() | F.col("reject_reason").isNull() |
                            (F.col("reject_reason") == "")).count()
    rep.add("T1", "every reject has source_file, raw_record and reason", untraceable == 0, 0, untraceable)
    stages_bad = rej.where(~F.col("reject_stage").isin(*REJECT_STAGES)).count()
    reasons = {t for (r,) in rej.select("reject_reason").distinct().collect() for t in r.split("|")}
    unknown = sorted(reasons - REJECT_REASONS)
    rep.add("T2", "reject stages and reasons are in the documented vocabulary",
            stages_bad == 0 and not unknown, "none unknown", f"stages_bad={stages_bad}, unknown_reasons={unknown}")
    read_names = _names(log["files"]["customers"]["files_read"] + log["files"]["orders"]["files_read"])
    reject_files = {r["source_file"] for r in rej.select("source_file").distinct().collect()
                    if r["source_file"]}   # NULL / blank sources are reported by T1
    stray = sorted(_names(reject_files) - read_names)
    rep.add("T3", "every reject points at a file that was read (compared by file name)",
            not stray, "none", stray or "none")
    for entity, key in ENTITY_KEYS.items():
        raw_key = F.trim(F.get_json_object("raw_record", f"$.{key}")).alias(key)
        dups = rej.where((F.col("entity") == entity) & (F.col("reject_stage") == "dedup")).select(raw_key).distinct()
        no_winner = dups.join(valid[entity].select(key), key, "left_anti").count()
        rep.add(f"T4-{entity}", f"{entity}: every duplicate-rejected key has a surviving valid row",
                no_winner == 0, 0, no_winner)

    # ---- run log agrees with the tables
    c = log.get("counts", {})
    got = {"customers_valid": flow["customers"]["valid"], "customers_rejected": flow["customers"]["rejected"],
           "orders_valid": flow["orders"]["valid"], "orders_rejected": flow["orders"]["rejected"],
           "published_customers": pub.count(), "published_total_amount": str(pa)}
    diffs = {k: (c.get(k), v) for k, v in got.items() if str(c.get(k)) != str(v)}
    rep.add("L1", "run log counts equal the stored tables", not diffs, "log == tables", diffs or "equal",
            "differences shown as (log, table)")
    by_reason = {e: {r["reject_reason"]: r["n"] for r in rej.where(F.col("entity") == e)
                 .groupBy("reject_reason").agg(F.count("*").alias("n")).collect()} for e in ENTITY_KEYS}
    table_reasons = {e: dict(sorted(v.items())) for e, v in by_reason.items()}
    log_reasons = {e: dict(sorted(v.items())) for e, v in (log.get("rejected_by_reason") or {}).items()}
    rep.add("L2", "run log reject breakdown equals the rejects table",
            log_reasons == table_reasons, log_reasons, table_reasons)

    # ---- expected-answer labels (never read by the pipeline)
    exp_file = Path(expected_path) if expected_path else _discover_expected(orders_path)
    expected_loaded = None
    if exp_file and exp_file.exists():
        exp = json.loads(exp_file.read_text())
        try:
            expected_loaded = os.path.relpath(exp_file)      # shorter and machine independent
        except ValueError:                                   # e.g. different drive on Windows
            expected_loaded = str(exp_file)
        for entity in ENTITY_KEYS:
            e = exp[entity]
            rep.add(f"E1-{entity}", f"{entity}: valid rows equal expected label", flow[entity]["valid"] == e["valid_rows"],
                    e["valid_rows"], flow[entity]["valid"])
            if "input" in flow[entity]:
                rep.add(f"E2-{entity}", f"{entity}: input rows equal expected label",
                        flow[entity]["input"] == e["input_rows"], e["input_rows"], flow[entity]["input"])
            rep.add(f"E3-{entity}", f"{entity}: reject reasons equal expected label",
                    dict(sorted(by_reason[entity].items())) == dict(sorted(e["rejected_by_reason"].items())),
                    dict(sorted(e["rejected_by_reason"].items())), dict(sorted(by_reason[entity].items())))
        eo = exp["orders"]
        rep.add("E4", "valid total amount equals expected label", str(sa) == eo["valid_total_amount"],
                eo["valid_total_amount"], sa)
        hot = pub.where(F.col("customer_id") == eo["hot_customer_id"]).select("order_count").first()
        rep.add("E5", f"hot customer {eo['hot_customer_id']} order count equals expected label",
                (hot[0] if hot else 0) == eo["valid_orders_for_hot_customer"],
                eo["valid_orders_for_hot_customer"], hot[0] if hot else 0)
    else:
        rep.add("E", "expected-answer labels", None, detail="no expected_results.json found next to the input data")

    n_pass = sum(c["status"] == "PASS" for c in rep.checks)
    return {"batch_id": batch, "run_id": log["run_id"], "mode": log.get("mode"),
            "expected_labels": expected_loaded, "row_flow": flow,
            "summary": {"passed": n_pass, "failed": len(rep.failed),
                        "skipped": sum(c["status"] == "SKIP" for c in rep.checks)},
            "result": "PASS" if not rep.failed else "FAIL", "checks": rep.checks}


def to_markdown(r: dict) -> str:
    s = r["summary"]
    lines = [f"# Reconciliation report: batch `{r['batch_id']}`", "",
             f"**Result: {r['result']}** ({s['passed']} passed, {s['failed']} failed, {s['skipped']} skipped)  ",
             f"Loaded by run `{r['run_id']}` (mode `{r['mode']}`). Expected labels: "
             f"{r['expected_labels'] or 'not used'}", "", "## Row flow (source to target)", "",
             "| entity | input | rejected: parse | dedup | referential | rejected total | valid |",
             "|---|---|---|---|---|---|---|"]
    for e, f in r["row_flow"].items():
        lines.append(f"| {e} | {f.get('input', 'n/a')} | {f['rejected_parse']} | {f['rejected_dedup']} | "
                     f"{f['rejected_referential']} | {f['rejected']} | {f['valid']} |")
    lines += ["", "Rejected rows are not lost: they are in the `rejects` table with source file and raw record.",
              "", "## Checks", "", "| id | check | status | expected | actual |", "|---|---|---|---|---|"]
    for c in r["checks"]:
        exp = (c["expected"] or "").replace("|", "/")[:70]
        act = (c["actual"] or "").replace("|", "/")[:70]
        lines.append(f"| {c['id']} | {c['check']} | **{c['status']}** | {exp} | {act} |")
    notes = [c for c in r["checks"] if c["detail"] and (c["status"] != "PASS" or c["id"].startswith("R1"))]
    if notes:
        lines += ["", "## Notes and explanations", ""]
        lines += [f"- **{c['id']}** ({c['status']}): {c['detail']}" for c in notes]
    return "\n".join(lines) + "\n"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", required=True, help="pipeline output dir")
    p.add_argument("--batch-id", default=None, help="default: latest successful batch")
    p.add_argument("--customers", default=None, help="customers input (default: path from the run log)")
    p.add_argument("--orders", default=None, help="orders input (default: path from the run log)")
    p.add_argument("--expected", default=None, help="expected_results.json (default: auto-discover)")
    p.add_argument("--master", default="local[*]")
    a = p.parse_args()
    spark = get_spark("skew_reconcile", 4, a.master)
    r = reconcile(spark, a.output, a.batch_id, a.customers, a.orders, a.expected)
    target = Path(a.output) / "reconciliation"
    target.mkdir(parents=True, exist_ok=True)
    (target / f"reconciliation_{r['batch_id']}.json").write_text(json.dumps(r, indent=2, default=str))
    md = to_markdown(r)
    (target / f"reconciliation_{r['batch_id']}.md").write_text(md)
    print(md)
    sys.exit(0 if r["result"] == "PASS" else 1)


if __name__ == "__main__":
    main()
