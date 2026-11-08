# Skewed-key Spark workload: diagnose, mitigate, compare

A small, tested, reproducible data-engineering project. Orders are joined to customers and summarised per customer. In the generated data one hot customer owns ~60% of the orders, which makes a plain shuffle join slow because one task gets most of the rows. The project reproduces that, **diagnoses** it with numbers, **mitigates** it, and **proves** the fix returns identical results.

## Headline results
Details: `docs/benchmark_report.md` and `docs/design_tradeoffs.md`. Measured on a 1-CPU-core machine, 576,600 valid orders repeated 8x for timing (your timings will differ; the commands below reproduce them).

| mode | wall (median) | longest task vs median | identical to baseline |
|---|---|---|---|
| baseline (plain shuffle join) | 7.3 s | 2.6 s (9.3x) | reference |
| **broadcast (default)** | **3.3 s (2.19x faster)** | 0.45 s (1.3x) | yes |
| salted | 8.8 s (0.83x) | 1.14 s (1.4x) | yes |
| aqe | 6.9 s (1.06x, within noise) | 0.45 s (2.0x) | yes |

Diagnosis: 65% of all rows land in one of 8 shuffle partitions (5.19x the average). Salting and AQE improve the longest task, but on one core that does not turn into a wall-time gain; that is an inference from task times, not a cluster measurement.

## Prerequisites
Python 3.10+ and **Java 11, 17 or 21** (Spark needs Java). On native Windows also install `winutils.exe` and set `HADOOP_HOME` (or use WSL2), otherwise tests that write Parquet fail.

## Quick start
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 1. committed sample (500 orders), then verify it
python -m skew_pipeline.run_pipeline --customers data/sample/input/customers \
    --orders data/sample/input/orders --output output/sample
python -m skew_pipeline.reconcile --output output/sample
python -m skew_pipeline.sample_queries --output output/sample

# 2. larger reproducible skewed run (fixed seed 42); baseline mode reproduces the skew
python -m skew_pipeline.generate_data --out data/generated --customers 2000 --orders 600000 --skew 0.6
python -m skew_pipeline.run_pipeline --customers data/generated/input/customers \
    --orders data/generated/input/orders --output output/generated --mode baseline
python -m skew_pipeline.diagnose  --output output/generated
python -m skew_pipeline.benchmark --output output/generated --repeats 3 --scale 8
python -m skew_pipeline.reconcile --output output/generated

# 3. tests (55 tests, about 4 minutes)
python -m pytest -q
```
On Windows PowerShell put each command on one line (no `\`). `reconcile` exits with code 1 if any check fails. Re-running the same input is skipped; add `--force` to recompute.

## Execution modes (`--mode`)
| mode | what it does | use when |
|---|---|---|
| `baseline` | plain shuffle join (reproduces the skew) | measuring |
| `broadcast` (**default**) | broadcast the small customers table | dimension fits in memory |
| `salted` | key + deterministic salt, dimension replicated, two-stage aggregation | both sides large |
| `aqe` | Spark splits oversized partitions at run time | no-code option; needs several input splits and `forceOptimizeSkewedJoin` |

## Flow
```
CSV -> header check -> raw read (strings, source_file) -> validate: parse -> dedup -> FK check
        |--> rejects/  (entity, stage, reason, source_file, raw_record, run_id)
        v
   staging/ -> transform (chosen mode) -> published/     every table partitioned by batch_id
                                              \-> run_log/<run_id>.json + .log
   afterwards, independent of the pipeline:  reconcile -> reconciliation/   sample_queries -> reports/
```

## Restart, retry, backfill
| situation | behaviour |
|---|---|
| same input again | `SKIPPED`, nothing rewritten |
| `--force` | recompute; that batch partition is **replaced**, never appended |
| run failed or process crashed | only `SUCCESS` counts as loaded, so the same command reprocesses from scratch |
| corrected data for a period | rerun with the same `--batch-id`; other batches are untouched |
| demo a crash | `--simulate-failure after_staging`, then rerun without it |

## Example output
`docs/sample_run/` holds the results of the sample run above (reconciliation report, sample queries, run log, readable CSV exports), so you can review them without running anything. `output/` starts empty and is created by the commands.

## Naming and conventions
snake_case tables and columns; deterministic ids (`C0000001`, `O000000001`, `batch_id = b_<12 hex of sha256 over input names and contents>` or a user label); ISO-8601 UTC timestamps (`2024-03-05T14:23:11Z`); `run_id` (`run_YYYYMMDDTHHMMSS_mmmZ`) and `source_file` on output rows; reject reasons are a closed vocabulary (`schemas.py`, `docs/data_dictionary.md`).

## Assumptions
- Header must match exactly (names and order); otherwise the file is quarantined and logged.
- Whitespace is trimmed, blank means missing, status values are exact upper-case matches.
- A duplicate key keeps the row with the latest timestamp (ties: source file, then raw record); losers go to `rejects`.
- Late-arriving and out-of-order orders are valid; an order whose customer was itself rejected is rejected as `invalid_customer_fk`.
- The generator includes deliberate boundary amounts (`0.00`, `9999999999.99`), so some revenue figures in the sample queries look extreme on purpose.

## Requirement to evidence
| required scenario | where it is covered |
|---|---|
| malformed input | `test_malformed_records` |
| missing required field | `test_missing_required_fields` |
| duplicate key / event | `test_duplicate_orders_keep_one_and_trace_the_rest`, `test_duplicate_customer_latest_version_wins` |
| unexpected schema change | `test_schema_change_quarantines_the_file`, `test_reordered_columns_are_quarantined` |
| empty batch | `test_empty_input_header_only`, `test_no_input_files_is_an_empty_batch`, `test_empty_batch_succeeds_with_zero_rows` |
| partial failure and retry | `test_failed_run_is_logged_and_retry_completes_it`, `test_crashed_process_leaves_running_record...` |
| late or out-of-order data | `test_late_and_out_of_order_rows_are_accepted` |
| invalid reference key | `test_invalid_customer_reference_is_rejected` |
| rerun of the same input | `test_same_input_again_is_skipped...`, `test_force_replaces_the_batch_without_duplicates` |
| domain edge cases | boundary amounts, status vocabulary, whitespace, order of a rejected customer |

| acceptance criterion | evidence |
|---|---|
| primary keys and referential integrity checked | validation rules + reconcile `K`, `FK` |
| deterministic transformations | deterministic dedup tie-break, deterministic salt, all modes equal baseline (`test_modes.py`) |
| source-to-target counts and aggregates reconcile or are explained | reconcile `R1`, `P1-P3` and the row-flow table |
| invalid records traceable | `rejects` table (source file + raw record), reconcile `T1-T4` |
| reruns create no duplicates | `test_idempotency.py` |
| tests pass | `docs/test_evidence.txt` |
| setup works from a clean environment | verified: fresh unzip, new virtualenv, `pip install -r requirements.txt`, quick-start steps 1 and 2 (step 2 scaled down to 30,000 orders) and the full 55-test suite, on Linux with Python 3.12 and Java 21. Not run on native Windows. |

## Design decisions
- Raw columns are read as strings; typing happens in validation, so every bad value becomes a traceable reject instead of a silent NULL.
- One reject table; `input = valid + rejected` is asserted by the pipeline and re-checked independently by `reconcile`.
- Every mode must equal the baseline exactly (two-way `EXCEPT ALL`, schema including nullability).
- A mode that is only configured proves nothing: the benchmark inspects the final AQE plan for a skewed read.
- Reconciliation tests damage a copy of a good load and assert which checks fail; a mutation check confirmed they fail when a check is weakened.

## Known limitations
- Publishing is not atomic across tables, readers have no snapshot isolation during a reload, and concurrent runs of one batch are not prevented.
- Benchmark numbers: one machine, 1 core, 3 measured rounds, data repeated 8x (order ids repeat there, which is fine for timing and equivalence only).
- Header check and fingerprinting need local or POSIX paths; fingerprinting reads every input file.
- Databricks Free Edition (serverless only) may not allow the Spark settings used for baseline and AQE; the local run is the reproducible one.

- AQE skew splitting needs enough data: at a few tens of thousands of rows the hot partition is below the thresholds and the benchmark report says `AQE skew split actually applied: NO`. Use the 600k-row run (`--scale 8`) for the AQE comparison.
- A run is skipped only if its run log says loaded **and** the batch's table partitions exist; a log without data triggers a reload.

## Possible next steps
Delta or Iceberg for atomic multi-table publishing and MERGE-based upserts; a manifest instead of full-file hashing; an orchestrator (retries, alerts) around the run log; automatic mode choice from the diagnosis (broadcast if the dimension is small, otherwise salted); a multi-node benchmark to measure the wall-time effect of salting and AQE.

## Layout
`skew_pipeline/` (schemas, ingest, validate, transform, ledger, metrics, diagnose, benchmark, reconcile, sample_queries, run_pipeline, generate_data, spark_utils) - `tests/` - `docs/` (data dictionary, design note, benchmark report, test evidence) - `data/sample/` (input + expected labels) - `docs/sample_run/` (a finished sample run: run log, reconciliation report, sample queries, rejects.csv, customer_order_summary.csv)
