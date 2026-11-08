# Sample queries: batch `b_249dd62140cc`

## Top 5 customers by order count (the hot key is obvious)

```sql
SELECT customer_id, country, segment, order_count, total_amount, ROUND(100.0 * order_count / SUM(order_count) OVER (), 1) AS pct_of_all_orders FROM published ORDER BY order_count DESC, customer_id LIMIT 5
```

| customer_id | country | segment | order_count | total_amount | pct_of_all_orders |
|---|---|---|---|---|---|
| C0000001 | US | RETAIL | 287 | 30000070301.57 | 60.2 |
| C0000030 | IN | RETAIL | 7 | 2100.54 | 1.5 |
| C0000050 | FR | ENTERPRISE | 7 | 1215.51 | 1.5 |
| C0000003 | US | ENTERPRISE | 6 | 1537.11 | 1.3 |
| C0000006 | DE | RETAIL | 6 | 10000001004.63 | 1.3 |

## Orders and revenue by country and segment (top 8 by revenue)

```sql
SELECT country, segment, COUNT(*) AS customers, SUM(order_count) AS orders, SUM(total_amount) AS revenue FROM published GROUP BY country, segment ORDER BY revenue DESC, country, segment LIMIT 8
```

| country | segment | customers | orders | revenue |
|---|---|---|---|---|
| US | RETAIL | 5 | 307 | 30000074391.00 |
| DE | RETAIL | 3 | 17 | 10000004307.81 |
| IN | ENTERPRISE | 2 | 8 | 10000001485.97 |
| SG | RETAIL | 4 | 18 | 5206.84 |
| US | ENTERPRISE | 4 | 20 | 4239.06 |
| US | SMB | 4 | 14 | 4188.40 |
| FR | ENTERPRISE | 5 | 20 | 3994.39 |
| IN | RETAIL | 2 | 11 | 3551.79 |

## Valid orders by status

```sql
SELECT status, COUNT(*) AS orders, SUM(amount) AS amount FROM orders GROUP BY status ORDER BY orders DESC, status
```

| status | orders | amount |
|---|---|---|
| DELIVERED | 110 | 27970.72 |
| RETURNED | 105 | 26212.93 |
| CANCELLED | 94 | 10000023507.53 |
| SHIPPED | 88 | 20000021229.73 |
| PLACED | 80 | 20000019867.44 |

## Orders per month (late-arriving rows from the previous year are accepted)

```sql
SELECT date_format(order_ts, 'yyyy-MM') AS month, COUNT(*) AS orders FROM orders GROUP BY 1 ORDER BY 1
```

| month | orders |
|---|---|
| 2023-05 | 1 |
| 2023-08 | 1 |
| 2023-11 | 1 |
| 2024-01 | 52 |
| 2024-02 | 34 |
| 2024-03 | 38 |
| 2024-04 | 46 |
| 2024-05 | 40 |
| 2024-06 | 40 |
| 2024-07 | 32 |
| 2024-08 | 26 |
| 2024-09 | 47 |
| 2024-10 | 44 |
| 2024-11 | 43 |
| 2024-12 | 32 |

## Rejected rows by entity and reason (nothing is silently dropped)

```sql
SELECT entity, reject_stage, reject_reason, COUNT(*) AS rows FROM rejects GROUP BY entity, reject_stage, reject_reason ORDER BY entity, rows DESC, reject_reason
```

| entity | reject_stage | reject_reason | rows |
|---|---|---|---|
| customers | dedup | duplicate_customer_id | 1 |
| customers | parse | invalid_country | 1 |
| customers | parse | invalid_segment | 1 |
| customers | parse | malformed_updated_at | 1 |
| customers | parse | missing_customer_id | 1 |
| orders | dedup | duplicate_order_id | 5 |
| orders | referential | invalid_customer_fk | 2 |
| orders | parse | invalid_status | 2 |
| orders | parse | malformed_amount | 2 |
| orders | parse | malformed_order_ts | 2 |
| orders | parse | malformed_record | 2 |
| orders | parse | missing_amount | 2 |
| orders | parse | missing_customer_id | 2 |
| orders | parse | missing_order_id | 2 |
| orders | parse | negative_amount | 2 |

## Reconciliation sanity: published orders vs valid orders

```sql
SELECT (SELECT SUM(order_count) FROM published) AS published_orders, (SELECT COUNT(*) FROM orders) AS valid_orders, (SELECT SUM(total_amount) FROM published) AS published_amount, (SELECT SUM(amount) FROM orders) AS valid_amount
```

| published_orders | valid_orders | published_amount | valid_amount |
|---|---|---|---|
| 477 | 477 | 50000118788.35 | 50000118788.35 |
