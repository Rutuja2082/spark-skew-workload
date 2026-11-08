"""Batch identity, input fingerprints and run history (the load 'ledger').

Idempotency model
  * A *batch* is one set of input files. batch_id is derived from the file
    contents (sha256), so the same input always maps to the same batch_id.
    It can be overridden (e.g. a business date) for backfills.
  * A *run* is one execution attempt with its own run_id. Every run writes a
    JSON log; runs of the same batch form its history.
  * Output tables are partitioned by batch_id and written with dynamic
    partition overwrite: re-loading a batch replaces exactly that batch's
    partition, never another batch's data and never appends duplicates.
"""
import hashlib
import json
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BATCH_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def file_fingerprint(files_by_entity: dict[str, list[Path]]) -> str:
    """sha256 over entity name, file name and file content (path independent)."""
    h = hashlib.sha256()
    for entity in sorted(files_by_entity):
        for f in sorted(files_by_entity[entity], key=lambda p: p.name):
            h.update(f"{entity}|{f.name}|".encode())
            with f.open("rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
    return h.hexdigest()


def make_batch_id(fingerprint: str, override: str | None = None) -> str:
    if override:
        if not BATCH_ID_RE.match(override):
            raise ValueError(f"invalid batch_id {override!r}: use letters, digits, _ . - only")
        return override
    return f"b_{fingerprint[:12]}"


def read_run_history(out_dir: str | Path, batch_id: str | None = None) -> list[dict]:
    """All run logs (oldest first), optionally only those of one batch."""
    logs = []
    for f in sorted((Path(out_dir) / "run_log").glob("*.json")):
        try:
            logs.append(json.loads(f.read_text()))
        except json.JSONDecodeError:  # half-written log from a crashed process
            continue
    logs = [l for l in logs if batch_id is None or l.get("batch_id") == batch_id]
    return sorted(logs, key=lambda l: l.get("start_time") or "")


def find_successful_load(history: list[dict], fingerprint: str) -> dict | None:
    """Most recent SUCCESS run of this batch that loaded the same input content."""
    done = [r for r in history if r.get("status") == "SUCCESS" and r.get("input_fingerprint") == fingerprint]
    return done[-1] if done else None


def latest_successful_batch(out_dir: str | Path) -> str | None:
    ok = [r for r in read_run_history(out_dir) if r.get("status") == "SUCCESS"]
    return ok[-1]["batch_id"] if ok else None


def read_batch(spark: SparkSession, out_dir: str | Path, table: str, batch_id: str) -> DataFrame:
    """Read one batch partition of an output table (without the batch_id column)."""
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    return (spark.read.parquet(str(Path(out_dir) / table))
            .where(F.col("batch_id") == batch_id).drop("batch_id"))
