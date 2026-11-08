# Skew benchmark (batch b_ddf21169ef1f)

> **Measured in a 1-CPU-core Linux sandbox** (600,000 generated orders, staged data repeated 8x for timing). Absolute times will differ on your machine; regenerate with
> `python -m skew_pipeline.benchmark --output output/generated --scale 8`.
> On one core, wall time mostly measures *total work*. The longest-task column is the core-count-independent evidence of the straggler.


- Data: 4,612,800 valid orders (staged data repeated 8x for timing), 2,000 customers
- Environment: Spark 3.5.9, `local[*]`, 1 CPU core(s), 8 shuffle partitions, 16 salt buckets
- Protocol: 1 warm-up + 3 measured rounds, interleaved, `noop` sink

## Diagnosis

SKEWED: key C0000001 holds 60% of rows; one of 8 shuffle partitions receives 65% of all rows (5.19x the average).

Rows per shuffle partition by `customer_id`: [235904, 224888, 208608, 2990872, 253456, 214944, 235408, 248720] (max/mean 5.19x).
After salting (`customer_id`, salt): [400160, 582872, 752072, 741320, 230816, 921864, 577576, 406120] (max/mean 1.6x).

## Runtime

| mode | wall median (s) | min | max | speedup | longest task (s) | median task (s) | max/median task | identical to baseline |
|---|---|---|---|---|---|---|---|---|
| baseline | 7.336 | 6.344 | 9.638 | 1.0x | 2.6 | 0.28 | 9.3 | n/a (reference) |
| broadcast | 3.344 | 3.341 | 3.35 | 2.19x | 0.45 | 0.34 | 1.3 | yes |
| salted | 8.805 | 8.071 | 9.095 | 0.83x | 1.14 | 0.82 | 1.4 | yes |
| aqe | 6.949 | 5.584 | 7.516 | 1.06x | 0.45 | 0.22 | 2.0 | yes |

## Correctness detail

| mode | rows | rows only in mode | rows only in baseline | schema identical | sum(order_count) equal | sum(total_amount) equal |
|---|---|---|---|---|---|---|
| broadcast | 2000 | 0 | 0 | True | True | True |
| salted | 2000 | 0 | 0 | True | True | True |
| aqe | 2000 | 0 | 0 | True | True | True |

AQE skew split actually applied in the final plan: **yes**.

## How to read this

- **Wall time** is total elapsed time. On a machine with few cores it mostly reflects total work; the benefit of removing a straggler appears when there are more free cores than shuffle partitions.
- **Longest task vs median task** in the heaviest stage is independent of core count: a stage cannot finish before its longest task, so that value is a lower bound for the stage time on a cluster with enough cores.
- Numbers are specific to this machine and data size; re-run to reproduce.
