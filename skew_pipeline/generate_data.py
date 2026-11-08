"""Seeded synthetic data generator with a deliberately skewed customer key.

Writes three things under --out:
  input/customers/customers.csv        source customers
  input/orders/orders_part_NNN.csv     source orders (split over several files)
  expected/expected_results.json       answer labels, kept OUT of the input data

A fraction (--skew) of the clean orders belongs to one hot customer, which is
what creates the skewed join/aggregation key. Every injected defect is assigned
to exactly one row and exactly one reason, so expected counts are exact.

Usage:
  python -m skew_pipeline.generate_data --out data/sample --customers 50 --orders 500
  python -m skew_pipeline.generate_data --out data/generated --customers 5000 --orders 1000000
"""
import argparse
import csv
import json
import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from .schemas import CUSTOMER_COLUMNS, ORDER_COLUMNS, SEGMENTS, STATUSES

COUNTRIES = ["IN", "US", "GB", "DE", "FR", "SG", "AU", "BR"]
HOT_CUSTOMER_ID = "C0000001"
BASE = datetime(2024, 1, 1)
MAX_AMOUNT_CENTS = 999_999_999_999  # 9999999999.99, upper bound of decimal(12,2)

# injected order defects: reason -> share of n_orders (at least 1 row each)
ORDER_DEFECT_RATES = {
    "duplicate_order_id": 0.01,
    "missing_order_id": 0.003,
    "missing_customer_id": 0.003,
    "missing_amount": 0.003,
    "malformed_amount": 0.003,
    "negative_amount": 0.003,
    "malformed_order_ts": 0.003,
    "invalid_status": 0.003,
    "invalid_customer_fk": 0.005,
    "malformed_record": 0.003,
}
LATE_ARRIVING_RATE = 0.01  # valid rows whose order_ts is far older than the batch


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def cents_str(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def _gen_customers(rng, n_customers):
    rows, reasons = [], {}
    for i in range(1, n_customers + 1):
        updated = BASE - timedelta(days=rng.randint(30, 400))
        rows.append([f"C{i:07d}", rng.choice(COUNTRIES), rng.choice(SEGMENTS), iso(updated)])
    clean = [r[:] for r in rows]

    # newer version of existing customers (latest updated_at must win)
    n_dup = min(n_customers, max(1, round(n_customers * 0.02)))
    for orig in rng.sample(clean, n_dup):
        newer = datetime.strptime(orig[3], "%Y-%m-%dT%H:%M:%SZ") + timedelta(days=rng.randint(1, 20))
        seg = rng.choice([s for s in SEGMENTS if s != orig[2]])
        rows.append([orig[0], orig[1], seg, iso(newer)])
    reasons["duplicate_customer_id"] = n_dup

    # invalid customers use ids no order references (C9xxxxxx), so FK math stays exact
    k = max(1, round(n_customers * 0.01))
    nxt = 9_000_000
    for reason in ["missing_customer_id", "invalid_country", "invalid_segment", "malformed_updated_at"]:
        for _ in range(k):
            nxt += 1
            row = [f"C{nxt}", rng.choice(COUNTRIES), rng.choice(SEGMENTS), iso(BASE - timedelta(days=5))]
            if reason == "missing_customer_id":
                row[0] = ""
            elif reason == "invalid_country":
                row[1] = rng.choice(["XX1", "in", "India"])
            elif reason == "invalid_segment":
                row[2] = rng.choice(["VIP", "unknown"])
            else:
                row[3] = rng.choice(["2023-13-45T99:00:00Z", "yesterday", "2023/05/01 10:00:00"])
            rows.append(row)
        reasons[reason] = k
    return rows, reasons, n_customers


def generate(out_dir, n_customers=1000, n_orders=100_000, skew_ratio=0.6,
             n_order_files=4, seed=42) -> dict:
    if n_orders < 100:
        raise ValueError("n_orders must be >= 100 so every defect type can be injected")
    if not 0 <= skew_ratio < 1:
        raise ValueError("skew_ratio must be in [0, 1)")
    rng = random.Random(seed)
    out = Path(out_dir)
    for sub in ("input", "expected"):
        shutil.rmtree(out / sub, ignore_errors=True)

    # ---- customers
    cust_rows, cust_rejects, n_valid_customers = _gen_customers(rng, n_customers)
    rng.shuffle(cust_rows)
    _write_csv(out / "input/customers/customers.csv", CUSTOMER_COLUMNS, cust_rows)

    # ---- orders
    defect_counts = {k: max(1, round(n_orders * r)) for k, r in ORDER_DEFECT_RATES.items()}
    n_clean = n_orders - sum(defect_counts.values())
    next_id = [0]

    def new_id():
        next_id[0] += 1
        return f"O{next_id[0]:09d}"

    def pick_customer():
        if rng.random() < skew_ratio:
            return HOT_CUSTOMER_ID
        return f"C{rng.randint(1, n_customers):07d}"

    def rand_ts(late=False):
        if late:  # late-arriving: belongs to the previous year
            return BASE - timedelta(days=rng.randint(1, 300), seconds=rng.randint(0, 86399))
        return BASE + timedelta(seconds=rng.randint(0, 365 * 86400 - 1))

    n_boundary = min(5, n_clean // 10)
    clean_rows, total_cents, hot_orders, late_valid = [], 0, 0, 0
    for j in range(n_clean):
        cents = 0 if j < n_boundary else MAX_AMOUNT_CENTS if j < 2 * n_boundary else rng.randint(100, 50_000)
        late = rng.random() < LATE_ARRIVING_RATE
        row = [new_id(), pick_customer(), cents_str(cents), rng.choice(STATUSES), iso(rand_ts(late))]
        clean_rows.append(row)
        total_cents += cents
        hot_orders += row[1] == HOT_CUSTOMER_ID
        late_valid += late

    def base_row():
        return [new_id(), pick_customer(), cents_str(rng.randint(100, 50_000)),
                rng.choice(STATUSES), iso(rand_ts())]

    bad_rows = []
    for reason, n in defect_counts.items():
        if reason == "duplicate_order_id":
            bad_rows += [r[:] for r in rng.sample(clean_rows, n)]  # exact re-delivery
            continue
        for k in range(n):
            r = base_row()
            if reason == "missing_order_id":
                r[0] = ""
            elif reason == "missing_customer_id":
                r[1] = ""
            elif reason == "missing_amount":
                r[2] = ""
            elif reason == "malformed_amount":
                r[2] = rng.choice(["abc", "12,50", "$9.99", "1.234", "10000000000.00"])
            elif reason == "negative_amount":
                r[2] = "-" + r[2]
            elif reason == "malformed_order_ts":
                r[4] = rng.choice(["2024-13-45T99:00:00Z", "not-a-date", "2024/05/01 10:00:00"])
            elif reason == "invalid_status":
                r[3] = rng.choice(["UNKNOWN", "DONE", "LOST"])
            elif reason == "invalid_customer_fk":
                r[1] = f"C8{rng.randint(0, 999_999):06d}"
            elif reason == "malformed_record":
                r = r[:3] if k % 2 == 0 else r + ["unexpected_extra_field"]
            bad_rows.append(r)

    all_rows = clean_rows + bad_rows
    assert len(all_rows) == n_orders
    rng.shuffle(all_rows)  # out-of-order delivery
    for f in range(n_order_files):
        _write_csv(out / f"input/orders/orders_part_{f:03d}.csv", ORDER_COLUMNS,
                   all_rows[f::n_order_files])

    # ---- expected labels (never read by the pipeline)
    n_cust_input = len(cust_rows)
    expected = {
        "params": {"seed": seed, "customers": n_customers, "orders": n_orders,
                   "skew_ratio": skew_ratio, "order_files": n_order_files},
        "customers": {
            "input_rows": n_cust_input,
            "valid_rows": n_valid_customers,
            "rejected_by_reason": cust_rejects,
        },
        "orders": {
            "input_rows": n_orders,
            "valid_rows": n_clean,
            "rejected_by_reason": defect_counts,
            "valid_total_amount": cents_str(total_cents),
            "hot_customer_id": HOT_CUSTOMER_ID,
            "valid_orders_for_hot_customer": hot_orders,
            "valid_late_arriving_rows": late_valid,
            "valid_boundary_amount_rows": 2 * n_boundary,
        },
    }
    (out / "expected").mkdir(parents=True, exist_ok=True)
    (out / "expected/expected_results.json").write_text(json.dumps(expected, indent=2))
    return expected


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="data/generated")
    p.add_argument("--customers", type=int, default=1000)
    p.add_argument("--orders", type=int, default=100_000)
    p.add_argument("--skew", type=float, default=0.6, help="share of clean orders for the hot customer")
    p.add_argument("--order-files", type=int, default=4)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    exp = generate(a.out, a.customers, a.orders, a.skew, a.order_files, a.seed)
    o = exp["orders"]
    print(f"wrote {a.out}: {o['input_rows']} orders ({o['valid_rows']} valid, "
          f"{o['valid_orders_for_hot_customer']} for hot customer {o['hot_customer_id']})")


if __name__ == "__main__":
    main()
