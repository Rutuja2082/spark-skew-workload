"""Sample analytical queries over the loaded batch (what the target consumer would run).

  python -m skew_pipeline.sample_queries --output output/generated

Writes <output>/reports/sample_queries_<batch>.md with each query and its result.
Note: the generator deliberately includes boundary amounts (0.00 and 9999999999.99),
so a few revenue figures look extreme on purpose.
"""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from .ledger import latest_successful_batch, read_batch
from .spark_utils import get_spark

QUERIES = {
    "Top 5 customers by order count (the hot key is obvious)": """
        SELECT customer_id, country, segment, order_count, total_amount,
               ROUND(100.0 * order_count / SUM(order_count) OVER (), 1) AS pct_of_all_orders
        FROM published ORDER BY order_count DESC, customer_id LIMIT 5""",
    "Orders and revenue by country and segment (top 8 by revenue)": """
        SELECT country, segment, COUNT(*) AS customers, SUM(order_count) AS orders, SUM(total_amount) AS revenue
        FROM published GROUP BY country, segment ORDER BY revenue DESC, country, segment LIMIT 8""",
    "Valid orders by status": """
        SELECT status, COUNT(*) AS orders, SUM(amount) AS amount
        FROM orders GROUP BY status ORDER BY orders DESC, status""",
    "Orders per month (late-arriving rows from the previous year are accepted)": """
        SELECT date_format(order_ts, 'yyyy-MM') AS month, COUNT(*) AS orders
        FROM orders GROUP BY 1 ORDER BY 1""",
    "Rejected rows by entity and reason (nothing is silently dropped)": """
        SELECT entity, reject_stage, reject_reason, COUNT(*) AS rows
        FROM rejects GROUP BY entity, reject_stage, reject_reason ORDER BY entity, rows DESC, reject_reason""",
    "Reconciliation sanity: published orders vs valid orders": """
        SELECT (SELECT SUM(order_count) FROM published) AS published_orders,
               (SELECT COUNT(*) FROM orders) AS valid_orders,
               (SELECT SUM(total_amount) FROM published) AS published_amount,
               (SELECT SUM(amount) FROM orders) AS valid_amount""",
}


def _table(rows, cols) -> str:
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(out)


def run_queries(spark: SparkSession, out_dir: str, batch_id: str | None = None) -> tuple[str, dict]:
    batch = batch_id or latest_successful_batch(out_dir)
    if not batch:
        raise SystemExit("no successful batch found; run the pipeline first")
    read_batch(spark, out_dir, "published/customer_order_summary", batch).createOrReplaceTempView("published")
    read_batch(spark, out_dir, "staging/orders_valid", batch).createOrReplaceTempView("orders")
    read_batch(spark, out_dir, "rejects", batch).createOrReplaceTempView("rejects")
    results, md = {}, [f"# Sample queries: batch `{batch}`", ""]
    for title, sql in QUERIES.items():
        df = spark.sql(sql)
        rows = df.collect()
        results[title] = [r.asDict() for r in rows]
        md += [f"## {title}", "", "```sql", " ".join(sql.split()), "```", "", _table(rows, df.columns), ""]
    return "\n".join(md), results


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", required=True)
    p.add_argument("--batch-id", default=None)
    p.add_argument("--master", default="local[*]")
    a = p.parse_args()
    spark = get_spark("skew_queries", 4, a.master)
    md, _ = run_queries(spark, a.output, a.batch_id)
    batch = a.batch_id or latest_successful_batch(a.output)
    target = Path(a.output) / "reports"
    target.mkdir(parents=True, exist_ok=True)
    (target / f"sample_queries_{batch}.md").write_text(md)
    print(md)


if __name__ == "__main__":
    main()
