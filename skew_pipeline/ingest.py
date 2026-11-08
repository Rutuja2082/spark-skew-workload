"""File discovery, schema-drift check and raw (all-string) reads.

Policy for unexpected schema change: a file whose header is not exactly the
expected column list (same names, same order) is quarantined as a whole and
reported in the run log; the remaining files are still processed. Header
checks use plain file access, so paths must be local or POSIX-mounted
(e.g. a Databricks /Volumes path).
"""
import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import CORRUPT_COLUMN, EXPECTED_COLUMNS, raw_schema


def list_csv_files(path: str) -> list[Path]:
    p = Path(path)
    if p.is_file():
        return [p.resolve()]
    if p.is_dir():
        return sorted(f.resolve() for f in p.rglob("*.csv"))
    return []


def check_header(file: Path, expected: list[str]) -> str | None:
    """Return None if the header matches, else a human-readable problem."""
    with file.open(encoding="utf-8-sig", newline="") as fh:
        first = fh.readline()
    if not first.strip():
        return "empty file: no header row"
    header = [h.strip() for h in next(csv.reader([first]))]
    if header == expected:
        return None
    missing = [c for c in expected if c not in header]
    extra = [c for c in header if c not in expected]
    if not missing and not extra:
        return f"columns reordered: got {header}, expected {expected}"
    return f"schema mismatch: missing={missing} unexpected={extra}"


def read_raw(spark: SparkSession, kind: str, path: str) -> tuple[DataFrame, dict]:
    """Read all accepted files of one entity as strings plus source_file.

    Returns (dataframe, info) where info lists files_read and files_rejected.
    """
    columns = EXPECTED_COLUMNS[kind]
    accepted, rejected = [], []
    for f in list_csv_files(path):
        problem = check_header(f, columns)
        if problem:
            rejected.append({"file": str(f), "reason": problem})
        else:
            accepted.append(str(f))

    schema = raw_schema(columns)
    if accepted:
        df = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
            .schema(schema)
            .csv(accepted)
            .select("*", F.col("_metadata.file_path").alias("source_file"))
        )
    else:  # empty batch: keep downstream code uniform
        df = spark.createDataFrame([], schema).withColumn("source_file", F.lit(None).cast("string"))
    return df, {"files_read": accepted, "files_rejected": rejected}
