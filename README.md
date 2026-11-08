# Skewed-Key Spark Workload: Diagnose, Mitigate, Compare

A reproducible PySpark project that creates a workload with a highly skewed customer key distribution, diagnoses the performance bottleneck, applies multiple mitigation strategies, and compares runtime, task balance, and correctness.

---

## 1. Client Assignment

> Create a reproducible Spark workload with a skewed key distribution. Diagnose the bottleneck, implement a suitable mitigation, and report runtime and correctness comparisons.

This project implements the assignment using a synthetic customer-and-orders workload.

---

## 2. Project Objective

The project demonstrates an end-to-end Spark workload that:

- Generates reproducible synthetic data.
- Creates a highly skewed customer key distribution.
- Validates input data.
- Handles duplicate, missing, invalid, and late records.
- Separates valid records from rejected records.
- Preserves reject traceability.
- Performs customer/order transformations.
- Publishes an aggregated customer-level result.
- Detects and measures Spark data skew.
- Compares baseline and optimized execution strategies.
- Verifies correctness across execution modes.
- Supports idempotent processing and controlled reruns.
- Provides reconciliation and automated tests.
- Documents the complete setup and execution process.

---

## 3. Environment

The project was developed and tested on native Windows.

| Component | Tested Environment |
|---|---|
| Operating System | Windows |
| Python | 3.10+ |
| PySpark | 3.5.9 |
| Apache Spark | 3.5.9 |
| Java | 17 |
| Hadoop | 3.3.4 native Windows support |
| Execution Mode | `local[*]` |
| CPU | 8 cores |
| Shuffle Partitions | 8 |
| Salt Buckets | 16 |
| Data Scale | 600,000 input orders |
| Timing Scale | 8x staged workload |

---

# 4. Headline Results

The benchmark used the same workload and execution protocol for all strategies.

The staged benchmark contained **4,612,800 valid orders**, representing the 576,600 valid orders repeated 8 times for timing.

| Mode | Median Runtime | Longest Task | Max/Median Task | Speedup | Correctness |
|---|---:|---:|---:|---:|---|
| Baseline | 7.937 s | 3.84 s | 3.7x | 1.00x | Reference |
| Broadcast | **3.314 s** | **0.55 s** | 1.7x | **2.39x** | Identical |
| Salted | 8.824 s | 4.48 s | **1.3x** | 0.90x | Identical |
| AQE | 6.443 s | 2.60 s | 2.4x | 1.23x | Identical |

### Selected mitigation

**Broadcast join was selected as the best mitigation for this workload.**

It reduced median runtime from **7.937 seconds to 3.314 seconds**, providing a **2.39x speedup** while preserving identical results.

Salting significantly improved partition balance but introduced additional computation and was slower overall for this workload.

AQE provided a moderate improvement and confirmed that Spark's skew-handling mechanism was applied.

---

# 5. Skew Diagnosis

The generated workload contains:

- 576,600 valid orders.
- 2,000 distinct customers.
- Hot customer: `C0000001`.
- Hot customer orders: 345,455.
- Hot customer share: 59.91%.
- Mean orders per customer: 288.3.
- Top-key-to-mean ratio: 1,198.2x.

The skewed customer key causes one shuffle partition to receive substantially more records than the others.

## Before salting

Partition distribution:

```text
[29488, 28111, 26076, 373859, 31682, 26868, 29426, 31090]
```

Key observations:

- Largest partition: 373,859 rows.
- Median partition: 29,457 rows.
- Largest partition / mean partition: 5.19x.
- Largest partition share: 64.84%.

### Diagnosis

```text
SKEWED: key C0000001 holds 60% of rows;
one of 8 shuffle partitions receives 65% of all rows (5.19x the average).
```

This demonstrates the bottleneck caused by the highly concentrated customer key.

---

# 6. Partition Balance After Salting

With 16 salt buckets, the partition distribution became:

```text
[50020, 72859, 94009, 92665, 28852, 115233, 72197, 50765]
```

Key observations:

- Largest partition: 115,233 rows.
- Median partition: 72,528 rows.
- Largest partition / mean partition: 1.60x.
- Largest partition share: 19.98%.

Therefore, salting substantially improved partition balance:

```text
Before salting: 5.19x max/mean
After salting:  1.60x max/mean
```

However, better partition balance did not translate into lower wall-clock runtime for this workload because the salting approach introduced additional processing overhead.

---

# 7. Mitigation Strategies

The project compares three approaches.

## 7.1 Baseline

The baseline uses the normal customer/order aggregation and join strategy without skew-specific mitigation.

Purpose:

- Establish a reference runtime.
- Establish the reference result.
- Measure the effect of the skew.

---

## 7.2 Broadcast Join

The customer dataset is small enough to broadcast.

Broadcasting avoids a large shuffle for the customer side and significantly reduces the impact of the skewed join.

Result:

```text
Median runtime: 3.314 s
Speedup:        2.39x
Correctness:    Identical
```

This was the best-performing strategy for the tested workload.

---

## 7.3 Salting

Salting adds a salt value to distribute records belonging to the hot key across multiple shuffle partitions.

Configuration:

```text
Salt buckets: 16
```

Result:

```text
Partition max/mean before: 5.19x
Partition max/mean after:  1.60x

Median runtime: 8.824 s
Speedup:        0.90x
Correctness:    Identical
```

Salting improved distribution substantially, but the additional work made it slower than the baseline in this local benchmark.

---

## 7.4 Adaptive Query Execution (AQE)

AQE was tested as another mitigation.

Result:

```text
Median runtime: 6.443 s
Speedup:        1.23x
Correctness:    Identical
```

AQE skew splitting was confirmed in the final execution plan.

---

# 8. Benchmark Methodology

The benchmark used:

- The same input workload for every strategy.
- One warm-up execution.
- Three measured rounds.
- Interleaved execution order.
- A `noop` sink to reduce output-writing effects.
- The same Spark environment.
- The same shuffle partition configuration.
- The same data scale.

The benchmark workload contained:

```text
4,612,800 valid orders
2,000 customers
8x timing scale
8 shuffle partitions
16 salt buckets
```

This makes the comparison reproducible within the tested environment.

---

# 9. Correctness Comparison

Each optimized strategy was compared with the baseline result.

The following checks were performed:

- Output row count.
- Rows only present in optimized result.
- Rows only present in baseline result.
- Schema equality.
- Total order count equality.
- Total order amount equality.

Results:

| Strategy | Rows Only in Mode | Rows Only in Baseline | Schema Same | Order Count Same | Amount Same |
|---|---:|---:|---|---|---|
| Broadcast | 0 | 0 | Yes | Yes | Yes |
| Salted | 0 | 0 | Yes | Yes | Yes |
| AQE | 0 | 0 | Yes | Yes | Yes |

Therefore, all tested mitigation strategies produced results equivalent to the baseline.

---

# 10. Data Quality

The pipeline intentionally includes invalid and duplicate records so that validation and quarantine behavior can be demonstrated.

For the 600,000-order workload:

```text
Orders input:     600,000
Orders valid:     576,600
Orders rejected:   23,400
```

For customers:

```text
Customers input:  2,120
Customers valid:  2,000
Customers rejected: 120
```

## Validation categories

The pipeline handles:

- Invalid records.
- Missing required fields.
- Invalid data types.
- Duplicate records.
- Referential integrity failures.
- Late records.

Rejected records are retained separately rather than silently discarded.

---

# 11. Reject Quarantine and Traceability

Rejected records are stored with information that allows the source record to be traced.

The reject information includes:

- Source file.
- Raw record.
- Processing stage.
- Reject reason.
- Relevant metadata.

This allows data-quality failures to be investigated without losing the original input information.

---

# 12. Reconciliation

The reconciliation process validates the complete data flow.

Command:

```powershell
python -m skew_pipeline.reconcile --output output/generated
```

Result:

```text
PASS (26 passed, 0 failed, 0 skipped)
```

Important reconciliation checks include:

### Customers

```text
Input records:       2,120
Parse rejects:          80
Duplicate rejects:      40
Referential rejects:     0
Total rejects:         120
Valid records:        2,000
```

### Orders

```text
Input records:       600,000
Parse rejects:        14,400
Duplicate rejects:     6,000
Referential rejects:   3,000
Total rejects:        23,400
Valid records:       576,600
```

The reconciliation also verifies:

- Input counts.
- Valid + rejected = input.
- Customer uniqueness.
- Order uniqueness.
- Required fields.
- Foreign-key validity.
- Published result against independent SQL recomputation.
- Total order count.
- Total amount.
- Reject traceability.
- Run-log counts.
- Expected result labels.
- Hot customer count.

Final published total amount:

```text
50,144,247,690.76
```

---

# 13. Idempotency and Recovery

The pipeline records processing information using run and batch identifiers.

Example:

```text
run_id
batch_id
attempt
status
mode
```

The pipeline detects previously processed batches and can skip duplicate processing.

A forced rerun can be performed when intentionally reprocessing a batch:

```powershell
python -m skew_pipeline.run_pipeline `
  --customers data/sample/input/customers `
  --orders data/sample/input/orders `
  --output output/sample `
  --force
```

This supports controlled retry and restart behavior.

Explicit input and output locations also allow the same pipeline pattern to be used for different runs or backfill-style processing.

---

# 14. Observability

Each pipeline execution records operational information including:

- Run ID.
- Batch ID.
- Attempt number.
- Processing mode.
- Status.
- Input counts.
- Valid counts.
- Rejected counts.
- Reject breakdown.
- Published row count.
- Published total amount.
- Stage timings.

Example successful run:

```text
Status: SUCCESS

Customers input:       2,120
Customers valid:       2,000
Customers rejected:      120

Orders input:        600,000
Orders valid:        576,600
Orders rejected:      23,400

Published customers:  2,000
Published amount:     50,144,247,690.76
```

---

# 15. Pipeline Flow

The overall pipeline follows this pattern:

```text
Synthetic Input
      |
      v
+----------------+
| Ingestion      |
+----------------+
      |
      v
+----------------+
| Validation     |
+----------------+
      |
      +----------------------+
      |                      |
      v                      v
 Valid Records        Reject / Quarantine
      |                      |
      v                      v
+----------------+     Traceability
| Transformation |
+----------------+
      |
      v
+----------------+
| Spark Join /   |
| Aggregation    |
+----------------+
      |
      v
+----------------+
| Published      |
| Customer Data  |
+----------------+
      |
      v
Reconciliation
      |
      v
Observability
```

---

# 16. Project Structure

```text
spark-skew-workload/
│
├── .gitignore
├── README.md
├── requirements.txt
├── pytest.ini
├── project_structure.txt
│
├── data/
│   └── sample/
│       ├── input/
│       └── expected/
│
├── docs/
│   └──
│
├── output/
│   └──
│
├── skew_pipeline/
│   ├── __init__.py
│   ├── benchmark.py
│   ├── diagnose.py
│   ├── generate_data.py
│   ├── pipeline.py
│   ├── reconcile.py
│   ├── run_pipeline.py
│   └── ...
│
└── tests/
    ├──
    └── ...
```

Generated runtime data is intentionally excluded from Git where appropriate.

---

# 17. Windows Prerequisites

The project was tested on native Windows.

Install:

- Python 3.10 or newer.
- Java 17.
- PySpark 3.5.9.
- Hadoop native Windows support.

The tested Hadoop native support uses Hadoop 3.3.4 binaries containing:

```text
C:\hadoop\bin\hadoop.dll
C:\hadoop\bin\winutils.exe
```

Set the following user environment variable:

```text
HADOOP_HOME=C:\hadoop
```

Add the following directory to the user PATH:

```text
C:\hadoop\bin
```

After changing environment variables, open a new PowerShell or VS Code terminal.

---

# 18. Installation

Clone the repository:

```powershell
git clone https://github.com/Rutuja2082/spark-skew-workload.git
cd spark-skew-workload
```

Create a virtual environment:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

---

# 19. Run the Sample Workload

The repository includes a small reproducible sample.

Run:

```powershell
python -m skew_pipeline.run_pipeline --customers data/sample/input/customers --orders data/sample/input/orders --output output/sample --force
```

Expected successful processing includes approximately:

```text
Customers input:       55
Customers valid:       50
Customers rejected:     5

Orders input:         500
Orders valid:         477
Orders rejected:       23

Published customers:   49
Published total amount:
50000118788.35
```

The exact run ID and timing values vary between executions.

---

# 20. Generate the Large Workload

Generate the reproducible large dataset:

```powershell
python -m skew_pipeline.generate_data --out data/generated --customers 2000 --orders 600000 --skew 0.6
```

Expected generation summary:

```text
600000 orders
576600 valid
23400 invalid/rejected
345455 hot-customer orders
```

The generator uses a fixed random seed to make the generated workload reproducible.

---

# 21. Run the Baseline

Run the baseline pipeline:

```powershell
python -m skew_pipeline.run_pipeline --customers data/generated/input/customers --orders data/generated/input/orders --output output/generated --mode baseline
```

Expected key results:

```text
Customers input:       2,120
Customers valid:       2,000
Customers rejected:      120

Orders input:        600,000
Orders valid:        576,600
Orders rejected:      23,400

Published customers:  2,000
Published amount:     50,144,247,690.76
```

---

# 22. Diagnose Skew

Run:

```powershell
python -m skew_pipeline.diagnose --output output/generated
```

The diagnosis identifies the hot key and compares partition distributions before and after salting.

Expected conclusion:

```text
SKEWED: key C0000001 holds 60% of rows;
one of 8 shuffle partitions receives 65% of all rows (5.19x the average).
```

---

# 23. Run the Benchmark

Run:

```powershell
python -m skew_pipeline.benchmark --output output/generated --repeats 3 --scale 8
```

The benchmark compares:

```text
baseline
broadcast
salted
aqe
```

It reports:

- Runtime.
- Minimum runtime.
- Maximum runtime.
- Median runtime.
- Speedup.
- Longest task.
- Median task.
- Task imbalance.
- Correctness comparison.

---

# 24. Run Reconciliation

Run:

```powershell
python -m skew_pipeline.reconcile --output output/generated
```

Expected result:

```text
PASS (26 passed, 0 failed, 0 skipped)
```

---

# 25. Run Automated Tests

Run the complete test suite:

```powershell
pytest -q
```

Current result:

```text
55 passed
```

The tests cover areas including:

- Data generation.
- Input validation.
- Invalid records.
- Duplicate handling.
- Referential integrity.
- Reject traceability.
- Transformation logic.
- Aggregation correctness.
- Idempotency.
- Pipeline behavior.
- Reconciliation.
- Skew diagnosis.
- Benchmark correctness.

---

# 26. Testing and Edge Cases

The project intentionally tests multiple data-quality and operational scenarios.

### Normal data

Valid customer and order records are processed successfully.

### Missing data

Required fields are validated and invalid records are rejected.

### Invalid data

Malformed records and invalid values are quarantined.

### Duplicate data

Duplicate records are identified and handled deterministically.

### Referential integrity

Orders referencing unavailable customers are rejected.

### Late data

Records containing late-event information are handled as part of the ingestion and validation process.

### Idempotent rerun

Previously processed batches can be detected and skipped.

### Forced rerun

A controlled forced execution can be used when a batch intentionally needs to be reprocessed.

### Skewed key

A deliberately hot customer key is used to reproduce a Spark skew bottleneck.

---

# 27. Design Decisions

## Why use synthetic data?

Synthetic data makes the project:

- Reproducible.
- Safe to publish.
- Independent of confidential data.
- Easy to scale.
- Easy to test.

## Why use a hot customer key?

A concentrated key distribution provides a deterministic way to reproduce Spark data skew.

## Why test multiple mitigations?

No single skew strategy is optimal for every workload.

The benchmark compares:

- Normal execution.
- Broadcast.
- Salting.
- AQE.

This makes the performance conclusion evidence-based rather than theoretical.

---

# 28. Performance Interpretation

The benchmark demonstrates an important Spark engineering tradeoff.

### Broadcast

Best overall for this workload because the customer table is small.

```text
2.39x faster than baseline
```

### Salting

Best partition-balance improvement:

```text
5.19x -> 1.60x max/mean
```

However, it was slower overall because of additional computation and aggregation overhead.

### AQE

Provided a moderate runtime improvement:

```text
1.23x speedup
```

and skew splitting was confirmed in the final plan.

Therefore:

> The best optimization depends on the workload, data sizes, and cost of the mitigation itself.

---

# 29. Acceptance Criteria

| Requirement | Status |
|---|---|
| Reproducible workload | PASS |
| Skewed key distribution | PASS |
| Small reproducible sample | PASS |
| Scalable 100k+ generator | PASS |
| Fixed random seed | PASS |
| Invalid records | PASS |
| Duplicate records | PASS |
| Missing values | PASS |
| Referential integrity validation | PASS |
| Reject quarantine | PASS |
| Source traceability | PASS |
| Run-level observability | PASS |
| Idempotent processing | PASS |
| Retry/restart support | PASS |
| Backfill-style execution | PASS |
| Automated tests | PASS |
| Skew diagnosis | PASS |
| Skew mitigation | PASS |
| Runtime comparison | PASS |
| Correctness comparison | PASS |
| Reconciliation | PASS |
| Documentation | PASS |
| Reproducibility | PASS |

---

# 30. Known Limitations

This benchmark is designed for reproducibility and demonstration rather than production-scale infrastructure.

Known limitations include:

- Execution uses local Spark rather than a distributed cluster.
- Runtime values depend on the local machine.
- Only one primary skew pattern is benchmarked.
- The customer dimension is small enough for broadcast.
- The benchmark does not represent cloud infrastructure costs.
- Production monitoring and alerting are outside the project scope.
- Synthetic data does not represent every possible real-world data-quality pattern.

---

# 31. Possible Next Steps

Potential production extensions include:

- Test larger datasets.
- Test multiple hot keys.
- Test different skew percentages.
- Test different salt-bucket counts.
- Compare additional Spark configurations.
- Run the workload on a distributed Spark cluster.
- Add persistent metrics and dashboards.
- Add CI/CD execution.
- Add automated performance regression thresholds.
- Add production-grade data-quality monitoring.

---

# 32. Reproducibility Checklist

A reviewer can reproduce the project by following these steps:

```text
1. Clone the GitHub repository.
2. Install Python.
3. Install Java 17.
4. Configure Hadoop native Windows support.
5. Create and activate the Python virtual environment.
6. Install requirements.
7. Run the sample workload.
8. Generate the large workload.
9. Run the baseline.
10. Run skew diagnosis.
11. Run the benchmark.
12. Run reconciliation.
13. Run pytest.
```

---

# 33. Final Result

The project successfully reproduces a Spark data-skew problem and demonstrates multiple mitigation approaches.

The key findings are:

```text
Hot key share:                 59.91%
Baseline partition max/mean:   5.19x
Salted partition max/mean:     1.60x

Baseline median runtime:       7.937 s
Broadcast median runtime:      3.314 s
AQE median runtime:            6.443 s
Salted median runtime:         8.824 s

Broadcast speedup:              2.39x
AQE speedup:                    1.23x
Salted speedup:                 0.90x

Correctness:                   Identical
Reconciliation:                PASS
Automated tests:               55 passed
```

For this workload, **broadcast join is the selected mitigation** because it provides the best measured runtime while maintaining full correctness.

The project demonstrates that Spark skew should be diagnosed using actual partition and task behavior, and that mitigation should be selected based on measured workload characteristics rather than applying a single optimization blindly.