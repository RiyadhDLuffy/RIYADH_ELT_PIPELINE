# Pipeline Architecture – Hybrid ELT Data Pipeline

## High-Level Data Flow

```
orders_huge_mixed_quality.csv  (or any CSV)
                |
                v
       ┌─────────────────┐
       │   File Router   │  Checks file size vs. threshold (200 MB)
       └────────┬────────┘
                │
        ┌───────┴────────┐
        │                │
        v                v
  Python Batch       PySpark
  (< 200 MB)         (>= 200 MB)
  csv.DictReader     mapPartitions
  chunksize=2000     parallelism=16
        │                │
        └──────┬──────────┘
               │
               v
         ┌───────────┐
         │ orders_raw│  ELT Principle: ALL rows ingested FIRST, unmodified
         │           │  Fields: run_id, source_file, source_row_number,
         │           │          ingested_at, engine_used, raw_record
         └─────┬─────┘
               │
               v
  ┌───────────────────────────┐
  │  Transform & Quality (9+) │  Correction Rules with Audit Trail
  │  Rules Applied In-Process │
  └────────────┬──────────────┘
               │
       ┌───────┴──────────┐
       │                  │
       v                  v
  VALID / CORRECTED    QUARANTINE
  + corrections list   + error_codes
  + quality_status     + raw_record
       │                  │
       v                  v
 orders_validated    orders_quarantine
 Unique Index on     Unique Index on
 order_id (Upsert)   issue_key (Upsert)
       │
       v
  pipeline_audit_logs
  + reports/results.json
  + reports/results.md
```

---

## Engine Selection Logic

```python
if file_size_mb <= SMALL_FILE_THRESHOLD_MB:  # 200 MB
    engine = "python_batch"
    # Uses csv.DictReader in a streaming loop
    # Accumulates rows in batches of BATCH_SIZE (2000)
    # Calls bulk_write() per batch → no full file in memory
else:
    engine = "pyspark"
    # Creates a SparkSession with local[*]
    # Reads CSV with fixed Schema (StringType for all fields)
    # mapPartitions: each Spark Executor writes its own partition
    #   directly to MongoDB via pymongo – no driver collection
```

---

## ELT vs ETL – Key Distinction

This pipeline follows **ELT**:

1. **E – Extract**: CSV is read row-by-row (streaming, no full load)
2. **L – Load RAW**: Every row is written to `orders_raw` *first*, as-is
3. **T – Transform**: Cleaning and classification happens *after* raw storage

This ensures:
- No data loss during ingestion
- Full traceability for every record back to its source
- Raw layer is always complete, even if transformation rules change later

---

## Consistency Equation (Section 6.11)

For each `run_id`:
```
run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
```

This is verified after every run and logged in `pipeline_audit_logs.is_consistent`.

---

## Idempotency Design

- **Stable Business Key**: `order_id`
- **Unique Index**: `orders_validated.order_id` (enforced at DB level)
- **Write Strategy**: `UpdateOne({"order_id": oid}, {"$set": doc, "$setOnInsert": {"created_at": ts}}, upsert=True)`
- **Effect**: Re-running with the same file never creates duplicate documents
- **Tracking**: `inserted_count`, `updated_count`, `unchanged_count` are measured per run

---

## Track B – Incremental Delta Loader

For groups / distinction track:

```
Initial Full Load
    ↓
orders_validated (full state)
    ↓
Delta File (new + modified rows only)
    ↓
incremental_loader.py
  - Reads delta CSV row-by-row
  - Applies quality rules + audit trail
  - Upserts only changed records
  - Reports: inserted / updated / unchanged
    ↓
Re-running same Delta → unchanged_count == delta_rows (idempotent)
```

---

## Quality Rules – Audit Trail Format

Every corrected field produces an audit entry:

```json
{
  "quality_status": "corrected",
  "corrections": [
    {
      "field": "customer_email",
      "original_value": "user@@mail..com",
      "corrected_value": "user@mail.com",
      "rule_code": "EMAIL_REPEATED_SYMBOLS"
    },
    {
      "field": "order_date",
      "original_value": "31-01-2025",
      "corrected_value": "2025-01-31",
      "rule_code": "DATE_NORMALIZED_ISO"
    }
  ]
}
```

---

## Spark Partition Writing (Driver-Safe)

To avoid collecting all data on the Spark driver:

```python
def partition_worker(partition_rows):
    # Each Executor creates its own pymongo client
    client = MongoClient(MONGO_URI)
    # Processes and writes its own partition independently
    for row in partition_rows:
        clean_doc, _, _ = transform_and_classify_row(row.asDict())
        bulk_write(...)
    # Yields only a small summary dict to driver
    yield {"raw_count": n, "valid_count": v, ...}

df.rdd.mapPartitions(partition_worker).collect()
# collect() only receives small summary dicts per partition
```

This approach scales to files of any size without OOM on the driver.
