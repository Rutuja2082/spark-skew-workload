"""Unit tests for ingestion and validation (happy path, edge and negative cases)."""
from decimal import Decimal

GOOD_C = [["C1", "IN", "RETAIL", "2023-06-01T00:00:00Z"], ["C2", "US", "SMB", "2023-06-02T00:00:00Z"]]


def order(oid="O1", cid="C1", amount="10.50", status="PLACED", ts="2024-01-05T10:00:00Z"):
    return [oid, cid, amount, status, ts]


def test_happy_path(validate):
    r = validate(GOOD_C, [order("O1"), order("O2", "C2", "5.00", "SHIPPED")])
    assert len(r["cust_valid"]) == 2 and len(r["ord_valid"]) == 2
    assert r["cust_rej"] == [] and r["ord_rej"] == []
    assert {x["order_id"]: x["amount"] for x in r["ord_valid"]}["O1"] == Decimal("10.50")


def test_empty_input_header_only(validate):
    r = validate([], [])
    assert r["cust_valid"] == [] and r["ord_valid"] == []
    assert r["cust_rej"] == [] and r["ord_rej"] == []


def test_no_input_files_is_an_empty_batch(spark, tmp_path):
    from skew_pipeline.ingest import read_raw
    df, info = read_raw(spark, "orders", str(tmp_path / "does_not_exist"))
    assert df.count() == 0 and info["files_read"] == [] and info["files_rejected"] == []


def test_malformed_records(validate):
    orders = [
        order("O1"),
        ["O2", "C1", "5.00"],                                   # too few fields
        ["O3", "C1", "5.00", "PLACED", "2024-01-01T00:00:00Z", "x"],  # too many fields
        order("O4", amount="abc"),
        order("O5", ts="2024-13-45T99:00:00Z"),
        order("O6", ts="2024/01/05 10:00:00"),
        order("O7", amount="1.234"),
    ]
    r = validate(GOOD_C, orders)
    assert [x["order_id"] for x in r["ord_valid"]] == ["O1"]
    assert r["ord_rej"] == sorted(["malformed_record"] * 2 + ["malformed_amount"] * 2 +
                                  ["malformed_order_ts"] * 2)


def test_missing_required_fields(validate):
    orders = [order("", "C1"), order("O2", ""), order("O3", amount=""), order("O4", status=""),
              order("O5", ts=""), order("O6", amount="   ")]
    r = validate(GOOD_C, orders)
    assert r["ord_valid"] == []
    assert r["ord_rej"] == sorted(["missing_order_id", "missing_customer_id", "missing_amount",
                                   "missing_status", "missing_order_ts", "missing_amount"])


def test_duplicate_orders_keep_one_and_trace_the_rest(validate):
    r = validate(GOOD_C, [order("O1"), order("O1"), order("O2")])
    assert sorted(x["order_id"] for x in r["ord_valid"]) == ["O1", "O2"]
    assert r["ord_rej"] == ["duplicate_order_id"]
    rej = r["ord_rej_df"].collect()[0]
    assert rej["entity"] == "orders" and rej["reject_stage"] == "dedup"
    assert rej["source_file"].endswith("o.csv") and "O1" in rej["raw_record"]


def test_duplicate_customer_latest_version_wins(validate):
    customers = [["C1", "IN", "RETAIL", "2023-01-01T00:00:00Z"],
                 ["C1", "IN", "ENTERPRISE", "2023-03-01T00:00:00Z"]]
    r = validate(customers, [])
    assert len(r["cust_valid"]) == 1 and r["cust_valid"][0]["segment"] == "ENTERPRISE"
    assert r["cust_rej"] == ["duplicate_customer_id"]


def test_invalid_customer_reference_is_rejected(validate):
    r = validate(GOOD_C, [order("O1", "C1"), order("O2", "C404")])
    assert [x["order_id"] for x in r["ord_valid"]] == ["O1"]
    assert r["ord_rej"] == ["invalid_customer_fk"]


def test_order_of_rejected_customer_is_also_rejected(validate):
    customers = GOOD_C + [["C3", "XX1", "SMB", "2023-01-01T00:00:00Z"]]  # invalid country
    r = validate(customers, [order("O1", "C3")])
    assert r["cust_rej"] == ["invalid_country"] and r["ord_rej"] == ["invalid_customer_fk"]


def test_boundary_amounts(validate):
    orders = [order("O1", amount="0.00"), order("O2", amount="9999999999.99"),
              order("O3", amount="10000000000.00"), order("O4", amount="-0.01")]
    r = validate(GOOD_C, orders)
    assert sorted(x["order_id"] for x in r["ord_valid"]) == ["O1", "O2"]
    assert r["ord_rej"] == ["malformed_amount", "negative_amount"]


def test_invalid_status_and_whitespace_trimming(validate):
    r = validate(GOOD_C, [order("O1", status="UNKNOWN"), order(" O2 ", " C1 ", " 7.25 ", " SHIPPED ")])
    assert [x["order_id"] for x in r["ord_valid"]] == ["O2"]
    assert r["ord_rej"] == ["invalid_status"]


def test_late_and_out_of_order_rows_are_accepted(validate):
    orders = [order("O2", ts="2024-06-01T00:00:00Z"), order("O1", ts="2022-01-01T00:00:00Z")]
    r = validate(GOOD_C, orders)
    assert len(r["ord_valid"]) == 2 and r["ord_rej"] == []


def test_schema_change_quarantines_the_file(validate):
    bad_header = ["order_id", "customer_id", "amount", "status"]  # order_ts dropped
    r = validate(GOOD_C, [order()[:4]], ord_header=bad_header)
    assert r["ord_valid"] == []
    assert len(r["info_o"]["files_rejected"]) == 1
    assert "order_ts" in r["info_o"]["files_rejected"][0]["reason"]


def test_reordered_columns_are_quarantined(validate):
    r = validate(GOOD_C, [["C1", "O1", "1.00", "PLACED", "2024-01-01T00:00:00Z"]],
                 ord_header=["customer_id", "order_id", "amount", "status", "order_ts"])
    assert "reordered" in r["info_o"]["files_rejected"][0]["reason"]
