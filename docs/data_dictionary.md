# Data dictionary

Naming: snake_case tables/columns, ISO-8601 UTC timestamps (`yyyy-MM-ddTHH:mm:ssZ`),
deterministic business ids (`C0000001`, `O000000001`), `run_id` and `source_file` on every output row.

## Source: `customers.csv`  (grain: one row per customer version)
| column | type (after validation) | null? | rule |
|---|---|---|---|
| customer_id | string | no | key; if repeated, the row with latest `updated_at` wins (ties: `source_file`, then raw record, both descending) |
| country | string | no | two uppercase letters |
| segment | string | no | `RETAIL`, `SMB`, `ENTERPRISE` |
| updated_at | timestamp | no | ISO-8601 UTC |

## Source: `orders_part_NNN.csv`  (grain: one row per order event)
| column | type | null? | rule |
|---|---|---|---|
| order_id | string | no | key; repeats are duplicates (winner: latest `order_ts`, same tie-breaks) |
| customer_id | string | no | must exist in **valid** customers (referential integrity) |
| amount | decimal(12,2) | no | `0.00` to `9999999999.99`, max 2 decimals, `.` separator; negative rejected |
| status | string | no | `PLACED`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `RETURNED` (exact match) |
| order_ts | timestamp | no | ISO-8601 UTC. Late-arriving (old) and out-of-order rows are valid |

Whitespace is trimmed; blank = missing. Header must equal the expected column list exactly (name and order).

## Relationships
`orders.customer_id` -> `customers.customer_id` (many-to-one). Orders pointing to a rejected or unknown customer are rejected as `invalid_customer_fk`.

## Output tables (parquet, partitioned by `batch_id`)
Every table lives at `<output>/<table>/batch_id=<batch_id>/part-*.parquet`. `batch_id` is the partition column (it is not stored inside the files), so reading a table root returns it automatically.

| table | grain | columns |
|---|---|---|
| `staging/customers_valid` | customer | customer_id, country, segment, updated_at, source_file, run_id |
| `staging/orders_valid` | order | order_id, customer_id, amount, status, order_ts, source_file, run_id |
| `rejects` | rejected row | entity, reject_stage, reject_reason, source_file, raw_record, run_id |
| `published/customer_order_summary` | customer | customer_id, country, segment, order_count (bigint, not null), total_amount decimal(22,2), first_order_ts, last_order_ts, run_id |

`batch_id`: `b_<12 hex>` derived from file names and contents, or a user label (`[A-Za-z0-9_.-]+`). `run_id`: one per execution attempt (`run_YYYYMMDDTHHMMSS_mmmZ`). A batch can have several runs; the published rows carry the `run_id` that wrote them.

All four execution modes (`baseline`, `broadcast`, `salted`, `aqe`) produce the identical published table (same rows, values, types and nullability).

Invariants enforced every run: `input rows = valid rows + rejected rows` per entity; `sum(order_count) = valid orders`; `sum(total_amount) = sum(valid amount)`.

## Run log (`run_log/<run_id>.json`)
| field | meaning |
|---|---|
| run_id, batch_id, attempt | this execution, the batch it loaded, n-th run of that batch |
| input_fingerprint | sha256 of input file names and contents |
| mode | execution strategy used |
| status | `RUNNING` (in progress or crashed), `SUCCESS`, `FAILED`, `SKIPPED` (identical input already loaded) |
| start_time, end_time, duration_seconds | ISO-8601 UTC, seconds |
| failure_reason, skipped_reason, previous_run_id | why a run failed or was skipped |
| replaced_existing_load | true when `--force` replaced a loaded batch |
| files | per entity: files read, files quarantined (with reason) |
| counts, rejected_by_reason | input / valid / rejected rows, published customers and total amount |
| timings_seconds | validate, transform_and_publish |

Run history of a batch = its run logs ordered by `start_time`; there is no separate ledger file.

## Reports
| file | produced by | content |
|---|---|---|
| `reconciliation/reconciliation_<batch>.md/.json` | `python -m skew_pipeline.reconcile` | row flow (input to valid/rejected), 26 PASS/FAIL/SKIP checks, explanations; exit code 1 on any FAIL |
| `reports/sample_queries_<batch>.md` | `python -m skew_pipeline.sample_queries` | the analytical queries a consumer would run, with results |
| `benchmark/benchmark_<batch>_x<scale>.md/.json` | `python -m skew_pipeline.benchmark` | runtime, task skew and correctness per execution mode |

Reconciliation check ids: `R0` input unchanged since load, `R1` input rows = valid + rejected (counted from the CSV files, not by Spark), `K` primary keys unique, `N` no NULLs in required columns, `FK` referential integrity, `P1` published = independent SQL recomputation (two-way EXCEPT ALL), `P2/P3` count and amount totals, `T1-T4` reject traceability, vocabulary, source files and surviving duplicate winners, `L1/L2` run log agrees with the tables, `E*` equals the expected-answer labels. A check whose inputs are unavailable is `SKIP`, never `PASS`.

## Reject reasons
| stage | reasons |
|---|---|
| parse | `malformed_record` (wrong field count / unparsable line), `missing_<field>`, `invalid_country`, `invalid_segment`, `malformed_updated_at`, `malformed_amount`, `negative_amount`, `malformed_order_ts`, `invalid_status` (several reasons can be joined with `|`) |
| dedup | `duplicate_customer_id`, `duplicate_order_id` |
| referential | `invalid_customer_fk` |

File-level problems (schema drift) are not row rejects: the file is quarantined and listed under `files.<entity>.files_rejected` in the run log.

## Expected labels (kept apart from input)
`data/<set>/expected/expected_results.json` holds the injected-defect counts, valid counts, the exact valid total amount and the hot-customer order count. The pipeline never reads it; tests and reconciliation do.
