"""Source schemas, allowed values and reject-reason vocabulary.

Raw files are read with an all-string schema so that every malformed value can
be traced to its source row instead of silently becoming NULL during the read.
Typing happens in validate.py.
"""
from pyspark.sql import types as T

CORRUPT_COLUMN = "_corrupt_record"

CUSTOMER_COLUMNS = ["customer_id", "country", "segment", "updated_at"]
ORDER_COLUMNS = ["order_id", "customer_id", "amount", "status", "order_ts"]

EXPECTED_COLUMNS = {"customers": CUSTOMER_COLUMNS, "orders": ORDER_COLUMNS}

SEGMENTS = ["RETAIL", "SMB", "ENTERPRISE"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
COUNTRY_REGEX = r"^[A-Z]{2}$"
# decimal(12,2): up to 10 integer digits and 2 fraction digits, optional sign
AMOUNT_REGEX = r"^-?\d{1,10}(\.\d{1,2})?$"
TS_REGEX = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
TS_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'"  # ISO-8601, UTC


def raw_schema(columns: list[str]) -> T.StructType:
    """All-string read schema plus the corrupt-record column."""
    fields = [T.StructField(c, T.StringType(), True) for c in columns]
    fields.append(T.StructField(CORRUPT_COLUMN, T.StringType(), True))
    return T.StructType(fields)


REJECT_COLUMNS = ["entity", "reject_stage", "reject_reason", "source_file", "raw_record", "run_id"]

# Closed vocabulary of reject reasons (reconcile.py fails on anything outside it)
REJECT_STAGES = {"parse", "dedup", "referential"}
REJECT_REASONS = {
    "malformed_record",
    "missing_customer_id", "missing_country", "missing_segment", "missing_updated_at",
    "invalid_country", "invalid_segment", "malformed_updated_at", "duplicate_customer_id",
    "missing_order_id", "missing_amount", "malformed_amount", "negative_amount",
    "missing_order_ts", "malformed_order_ts", "missing_status", "invalid_status",
    "duplicate_order_id", "invalid_customer_fk",
}
