# Reconciliation report: batch `b_249dd62140cc`

**Result: PASS** (26 passed, 0 failed, 0 skipped)  
Loaded by run `run_sample` (mode `broadcast`). Expected labels: data/sample/expected/expected_results.json

## Row flow (source to target)

| entity | input | rejected: parse | dedup | referential | rejected total | valid |
|---|---|---|---|---|---|---|
| customers | 55 | 4 | 1 | 0 | 5 | 50 |
| orders | 500 | 16 | 5 | 2 | 23 | 477 |

Rejected rows are not lost: they are in the `rejects` table with source file and raw record.

## Checks

| id | check | status | expected | actual |
|---|---|---|---|---|
| R1-customers | customers: input rows = valid + rejected | **PASS** | 55 | 55 |
| R1-orders | orders: input rows = valid + rejected | **PASS** | 500 | 500 |
| R0 | input files unchanged since the load | **PASS** | 249dd62140cc | 249dd62140cc |
| K-customers | customers: primary key customer_id unique | **PASS** | 50 | 50 |
| N-customers | customers: no NULL in required columns | **PASS** | 0 | 0 |
| K-orders | orders: primary key order_id unique | **PASS** | 477 | 477 |
| N-orders | orders: no NULL in required columns | **PASS** | 0 | 0 |
| FK | orders.customer_id exists in valid customers | **PASS** | 0 | 0 |
| P1 | published summary equals independent SQL recomputation (two-way EXCEPT ALL) | **PASS** | 0 / 0 | 0 / 0 |
| P2 | sum(order_count) = valid orders | **PASS** | 477 | 477 |
| P3 | sum(total_amount) = sum(valid order amount) (exact decimal) | **PASS** | 50000118788.35 | 50000118788.35 |
| T1 | every reject has source_file, raw_record and reason | **PASS** | 0 | 0 |
| T2 | reject stages and reasons are in the documented vocabulary | **PASS** | none unknown | stages_bad=0, unknown_reasons=[] |
| T3 | every reject points at a file that was read (compared by file name) | **PASS** | none | none |
| T4-customers | customers: every duplicate-rejected key has a surviving valid row | **PASS** | 0 | 0 |
| T4-orders | orders: every duplicate-rejected key has a surviving valid row | **PASS** | 0 | 0 |
| L1 | run log counts equal the stored tables | **PASS** | log == tables | equal |
| L2 | run log reject breakdown equals the rejects table | **PASS** | {'customers': {'duplicate_customer_id': 1, 'invalid_country': 1, 'inva | {'customers': {'duplicate_customer_id': 1, 'invalid_country': 1, 'inva |
| E1-customers | customers: valid rows equal expected label | **PASS** | 50 | 50 |
| E2-customers | customers: input rows equal expected label | **PASS** | 55 | 55 |
| E3-customers | customers: reject reasons equal expected label | **PASS** | {'duplicate_customer_id': 1, 'invalid_country': 1, 'invalid_segment':  | {'duplicate_customer_id': 1, 'invalid_country': 1, 'invalid_segment':  |
| E1-orders | orders: valid rows equal expected label | **PASS** | 477 | 477 |
| E2-orders | orders: input rows equal expected label | **PASS** | 500 | 500 |
| E3-orders | orders: reject reasons equal expected label | **PASS** | {'duplicate_order_id': 5, 'invalid_customer_fk': 2, 'invalid_status':  | {'duplicate_order_id': 5, 'invalid_customer_fk': 2, 'invalid_status':  |
| E4 | valid total amount equals expected label | **PASS** | 50000118788.35 | 50000118788.35 |
| E5 | hot customer C0000001 order count equals expected label | **PASS** | 287 | 287 |

## Notes and explanations

- **R1-customers** (PASS): valid=50, rejected=5 (dedup=1, parse=4, referential=0)
- **R1-orders** (PASS): valid=477, rejected=23 (dedup=5, parse=16, referential=2)
