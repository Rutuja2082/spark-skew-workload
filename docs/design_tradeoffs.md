# Design and trade-off note (Day 2)

## 1. The problem
`orders` joined to `customers` and summarised per customer. In the generated data one customer (`C0000001`) owns 60% of the orders. A shuffle (sort-merge) join hashes rows by `customer_id`, so every row of the hot key lands in **one** partition and one task does most of the work.

## 2. Diagnosis (evidence, not opinion)
Measured on 576,600 valid orders (`docs/benchmark_report.md`):

| evidence | value |
|---|---|
| hot key share of rows | 60% |
| rows in the largest of 8 shuffle partitions | 65% of all rows (5.19x the average) |
| same after salting (`customer_id`, salt) | max 1.6x the average |
| baseline longest task vs median task (heaviest stage) | 2.6 s vs 0.28 s = **9.3x** |

Tools: `python -m skew_pipeline.diagnose --output <dir>` (key and partition distribution) and the task-time lookup in `metrics.py` (Spark UI REST API; optional, returns nothing when the UI is unavailable).

## 3. Options compared
All four modes return **exactly** the baseline result (two-way `EXCEPT ALL` empty, same row count, schema, `sum(order_count)`, `sum(total_amount)`).

| mode | idea | wall median | longest task | notes |
|---|---|---|---|---|
| baseline | plain shuffle join | 7.3 s | 2.6 s | the problem |
| **broadcast** | ship small `customers` to every task | **3.3 s (2.19x)** | 0.45 s | no shuffle of orders; limited by dimension size |
| salted | split each key into 16 sub-keys, replicate customers x16, 2-stage aggregate | 8.8 s (0.83x) | 1.14 s | needs no small side; costs extra work |
| aqe | Spark splits the oversized partition at run time | 6.9 s (1.06x, within noise) | 0.45 s | no code, but see 5 |

(1 CPU core, 8 shuffle partitions, 4.6M rows after 8x repeat, 3 measured rounds after 1 warm-up, interleaved.)

## 4. Decision
**Default: `broadcast`.** `customers` is tiny (thousands of rows), so broadcasting is the cheapest, simplest and fastest option, and it removes the shuffle of the big table entirely. It is the pipeline default.

When it stops being the right choice:
- **dimension too large to broadcast** (roughly beyond tens to a few hundred MB): use `salted`, or `aqe` if you prefer no code;
- **both sides large and skewed**: `salted` or `aqe`.

Honest reading of the numbers: on one core, wall time measures total work, so salting looks slower because it does more work (exploded dimension, second aggregation). Its benefit is the shorter critical path (longest task 1.14 s vs 2.6 s), which only turns into a wall-time gain when there are more free cores than partitions. That is an inference from task times, **not measured on a cluster**. The AQE wall-time gain (1.06x) is inside the run-to-run spread (baseline 6.3 to 9.6 s), so only its task-level result (longest task 0.45 s) is evidence.

## 5. Two traps found while measuring AQE
AQE was configured but did nothing at first. Both causes were silent:
1. It splits a skewed partition **by map-task output**. A single small input file means one map task, which cannot be split.
2. Because the aggregation after the join groups by the same key, splitting adds a shuffle, and Spark cancels the optimisation unless `spark.sql.adaptive.forceOptimizeSkewedJoin=true`.

The benchmark therefore checks the **final plan** for a skewed shuffle read and reports it (`AQE skew split actually applied: yes`), and a regression test locks both behaviours in. Lesson: a mode that is merely configured proves nothing.

## 6. Idempotency, retry, restart, backfill
- **Batch identity:** `batch_id = b_<first 12 hex of sha256 over file names and contents>`, or an explicit `--batch-id` (e.g. a business period). Every output table is partitioned by `batch_id`.
- **Same input again:** the run is `SKIPPED` (nothing rewritten). `--force` recomputes and **replaces** that batch's partition (dynamic partition overwrite), so no duplicates and no other batch is touched.
- **Failure / crash:** every run first writes a `RUNNING` log, ends as `SUCCESS` or `FAILED`. Only `SUCCESS` counts as loaded, so a retry of the same command reprocesses from the start and overwrites partial files of that batch. A hard crash leaves `RUNNING`, which also never blocks a restart.
- **Backfill / correction:** rerun the corrected files with the same `--batch-id`. Different content under the same label is not "already loaded", so it replaces the old partition; other batches stay.
- **Observability:** `run_log/<run_id>.json` (status, attempt, batch, fingerprint, mode, counts, rejects by reason, timings, failure reason) and a `.log` text file per run.

## 7. Reconciliation (independent of the pipeline)
The pipeline asserts a few invariants while running; `reconcile.py` re-checks the stored result from the outside, on purpose through different code paths: raw rows are counted from the CSV files in plain Python, the published summary is recomputed in plain SQL and compared row by row, and the expected-answer labels (kept apart from the input) are compared when present. Differences are explained, not just flagged: the report shows where every input row went (parse, dedup and referential rejects, valid). Tests damage a copy of a good load in specific ways (lost or altered published rows, lost or untraceable rejects, duplicate key, orphan order, input edited after load, wrong labels) and assert exactly which checks fail; a mutation check (disabling P1) confirmed those tests really fail when a check is weakened.

## 8. Trade-offs and limitations
- **Not atomic across tables.** Staging, rejects and published are written one after another; a crash between them leaves a mix for that batch until the retry. Mitigation here is the run log (only `SUCCESS` is trusted). A transactional table format (Delta, Iceberg) would fix it.
- **No snapshot isolation.** A reader that holds a lazy DataFrame over a batch while it is reloaded can hit missing files (seen in testing). Read after the load completes.
- **No locking.** Two simultaneous runs of one batch are not prevented.
- **Fingerprinting reads every input file** (cheap here, costly for very large inputs; use file size and mtime or a manifest at scale). Header checks need local or POSIX paths.
- **Benchmark scope.** Staged data is repeated 8x for timing, so order ids repeat there (fine for timing and equivalence, not for business results). One machine, one data shape, 3 measured rounds.
- Free-edition Databricks is serverless-only, so the AQE and broadcast settings used here may not be adjustable there; the local run is the reproducible one.
