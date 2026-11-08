"""Row-level validation: parse -> deduplicate -> referential check.

Every input row ends up in exactly one place: the valid table or the rejects
table (with entity, stage, reason, source_file and the untouched raw record).
That makes input = valid + rejected a hard, checkable invariant.

Stages:   parse (malformed / missing / invalid values)
          dedup (key seen more than once: deterministic winner, rest rejected)
          referential (orders only: customer_id must exist in valid customers)
Whitespace around values is trimmed; blank strings count as missing.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schemas import (AMOUNT_REGEX, COUNTRY_REGEX, CORRUPT_COLUMN, CUSTOMER_COLUMNS,
                      ORDER_COLUMNS, REJECT_COLUMNS, SEGMENTS, STATUSES, TS_PATTERN, TS_REGEX)


def _clean(col: str):
    t = F.trim(F.col(col))
    return F.when(t == "", F.lit(None).cast("string")).otherwise(t)


def _prepare(raw: DataFrame, columns: list[str]) -> DataFrame:
    """Keep the untouched raw record, then trim and null-ify blank strings."""
    raw_record = F.coalesce(F.col(CORRUPT_COLUMN), F.to_json(F.struct(*[F.col(c) for c in columns])))
    return raw.select(
        raw_record.alias("raw_record"),
        F.col(CORRUPT_COLUMN).isNull().alias("_well_formed"),
        F.col("source_file"),
        *[_clean(c).alias(c) for c in columns],
    )


def _parse_ts(col: str):
    return F.expr(f"try_to_timestamp({col}, \"{TS_PATTERN}\")")


def _reasons(conditions: list):
    """conditions: Column expressions that yield a reason string or NULL."""
    return F.concat_ws("|", F.filter(F.array(*conditions), lambda x: x.isNotNull()))


def _reject_rows(df: DataFrame, entity: str, stage: str, reason, run_id: str) -> DataFrame:
    return df.select(
        F.lit(entity).alias("entity"),
        F.lit(stage).alias("reject_stage"),
        reason.alias("reject_reason"),
        "source_file",
        "raw_record",
        F.lit(run_id).alias("run_id"),
    ).select(*REJECT_COLUMNS)


def _dedup(df: DataFrame, key: str, order_cols: list, entity: str, reason: str, run_id: str):
    """Keep one row per key (latest timestamp wins, ties broken deterministically)."""
    w = Window.partitionBy(key).orderBy(*order_cols, F.col("source_file").desc(), F.col("raw_record").desc())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    keep = ranked.where("_rn = 1").drop("_rn")
    dropped = _reject_rows(ranked.where("_rn > 1"), entity, "dedup", F.lit(reason), run_id)
    return keep, dropped


def validate_customers(raw: DataFrame, run_id: str) -> tuple[DataFrame, DataFrame]:
    df = _prepare(raw, CUSTOMER_COLUMNS).withColumn("updated_ts", _parse_ts("updated_at"))
    ok = F.col("_well_formed")
    reason = _reasons([
        F.when(~ok, F.lit("malformed_record")),
        F.when(ok & F.col("customer_id").isNull(), F.lit("missing_customer_id")),
        F.when(ok & F.col("country").isNull(), F.lit("missing_country"))
         .when(ok & ~F.col("country").rlike(COUNTRY_REGEX), F.lit("invalid_country")),
        F.when(ok & F.col("segment").isNull(), F.lit("missing_segment"))
         .when(ok & ~F.col("segment").isin(SEGMENTS), F.lit("invalid_segment")),
        F.when(ok & F.col("updated_at").isNull(), F.lit("missing_updated_at"))
         .when(ok & (~F.col("updated_at").rlike(TS_REGEX) | F.col("updated_ts").isNull()),
               F.lit("malformed_updated_at")),
    ])
    flagged = df.withColumn("_reason", reason)
    parse_rejects = _reject_rows(flagged.where("_reason != ''"), "customers", "parse", F.col("_reason"), run_id)
    good = flagged.where("_reason = ''")

    keep, dup_rejects = _dedup(good, "customer_id", [F.col("updated_ts").desc()],
                               "customers", "duplicate_customer_id", run_id)
    valid = keep.select("customer_id", "country", "segment",
                        F.col("updated_ts").alias("updated_at"),
                        "source_file", F.lit(run_id).alias("run_id"))
    return valid, parse_rejects.unionByName(dup_rejects)


def validate_orders(raw: DataFrame, customers_valid: DataFrame, run_id: str) -> tuple[DataFrame, DataFrame]:
    df = (
        _prepare(raw, ORDER_COLUMNS)
        .withColumn("_amount_ok", F.col("amount").rlike(AMOUNT_REGEX))
        .withColumn("amount_dec", F.when(F.col("_amount_ok"), F.expr("try_cast(amount as decimal(12,2))")))
        .withColumn("order_ts_parsed", _parse_ts("order_ts"))
    )
    ok = F.col("_well_formed")
    reason = _reasons([
        F.when(~ok, F.lit("malformed_record")),
        F.when(ok & F.col("order_id").isNull(), F.lit("missing_order_id")),
        F.when(ok & F.col("customer_id").isNull(), F.lit("missing_customer_id")),
        F.when(ok & F.col("amount").isNull(), F.lit("missing_amount"))
         .when(ok & (~F.col("_amount_ok") | F.col("amount_dec").isNull()), F.lit("malformed_amount"))
         .when(ok & (F.col("amount_dec") < 0), F.lit("negative_amount")),
        F.when(ok & F.col("order_ts").isNull(), F.lit("missing_order_ts"))
         .when(ok & (~F.col("order_ts").rlike(TS_REGEX) | F.col("order_ts_parsed").isNull()),
               F.lit("malformed_order_ts")),
        F.when(ok & F.col("status").isNull(), F.lit("missing_status"))
         .when(ok & ~F.col("status").isin(STATUSES), F.lit("invalid_status")),
    ])
    flagged = df.withColumn("_reason", reason)
    parse_rejects = _reject_rows(flagged.where("_reason != ''"), "orders", "parse", F.col("_reason"), run_id)
    good = flagged.where("_reason = ''")

    keep, dup_rejects = _dedup(good, "order_id", [F.col("order_ts_parsed").desc()],
                               "orders", "duplicate_order_id", run_id)

    # referential integrity: customer must exist among VALID customers
    known = customers_valid.select("customer_id").distinct().withColumn("_known", F.lit(True))
    checked = keep.join(known, "customer_id", "left")
    fk_rejects = _reject_rows(checked.where("_known IS NULL"), "orders", "referential",
                              F.lit("invalid_customer_fk"), run_id)
    valid = checked.where("_known IS NOT NULL").select(
        "order_id", "customer_id", F.col("amount_dec").alias("amount"), "status",
        F.col("order_ts_parsed").alias("order_ts"), "source_file", F.lit(run_id).alias("run_id"))
    return valid, parse_rejects.unionByName(dup_rejects).unionByName(fk_rejects)
