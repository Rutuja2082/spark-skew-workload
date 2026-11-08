import csv
from pathlib import Path

import pytest

from skew_pipeline.ingest import read_raw
from skew_pipeline.schemas import CUSTOMER_COLUMNS, ORDER_COLUMNS
from skew_pipeline.spark_utils import get_spark
from skew_pipeline.validate import validate_customers, validate_orders


@pytest.fixture(scope="session")
def spark():
    s = get_spark("skew_tests", shuffle_partitions=2, master="local[2]")
    yield s
    s.stop()


def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        if header is not None:
            w.writerow(header)
        w.writerows(rows)


@pytest.fixture
def validate(spark, tmp_path):
    """validate(customers_rows, orders_rows) -> dict of python results."""
    def _run(customers, orders, cust_header=CUSTOMER_COLUMNS, ord_header=ORDER_COLUMNS):
        write_csv(tmp_path / "customers/c.csv", cust_header, customers)
        write_csv(tmp_path / "orders/o.csv", ord_header, orders)
        raw_c, info_c = read_raw(spark, "customers", str(tmp_path / "customers"))
        raw_o, info_o = read_raw(spark, "orders", str(tmp_path / "orders"))
        cv, cr = validate_customers(raw_c, "t1")
        ov, orj = validate_orders(raw_o, cv, "t1")
        reasons = lambda df: sorted(r["reject_reason"] for r in df.collect())
        return {
            "cust_valid": cv.collect(), "cust_rej": reasons(cr), "cust_rej_df": cr,
            "ord_valid": ov.collect(), "ord_rej": reasons(orj), "ord_rej_df": orj,
            "info_c": info_c, "info_o": info_o,
        }
    return _run
