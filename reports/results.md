# Pipeline Execution & Benchmark Report

| Run ID | Timestamp | Engine | File | Size (MB) | Rows Read | Valid | Corrected | Quarantined | Inserted | Updated | Elapsed (s) | Throughput (rows/s) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `475ac0da` | 2026-09-02T21:05:27Z | **PYTHON_BATCH** | `sample_orders_small.csv` | 41.77 | 100,000 | 0 | 95,128 | 4,872 | 87,454 | 7,674 | 83.34s | 1,199.87 |

## Consistency Equation Verification
$$\text{rows\_read} = \text{valid\_count} + \text{corrected\_count} + \text{quarantine\_count}$$

Every single row is ingested into `orders_raw` first, and then accurately classified without loss.
