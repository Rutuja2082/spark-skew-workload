"""SparkSession helper and a context manager for temporary SQL confs."""
from contextlib import contextmanager

from pyspark.sql import SparkSession


def get_spark(app_name: str = "skew_pipeline", shuffle_partitions: int = 8,
              master: str | None = "local[*]") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")  # all timestamps are UTC
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if master:  # leave empty on Databricks / when a session already exists
        builder = builder.master(master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@contextmanager
def temporary_conf(spark: SparkSession, confs: dict[str, str]):
    """Set SQL confs for the duration of a block, then restore them."""
    old = {k: spark.conf.get(k, None) for k in confs}
    try:
        for k, v in confs.items():
            spark.conf.set(k, v)
        yield
    finally:
        for k, v in old.items():
            if v is None:
                spark.conf.unset(k)
            else:
                spark.conf.set(k, v)
