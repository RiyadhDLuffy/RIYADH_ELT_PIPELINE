# Enterprise Hybrid ELT Data Platform

An end-to-end data engineering and analytics platform implementing hybrid batch ingestion (Python Batch & Apache Spark), automated quality enforcement, MongoDB document store optimization, incremental materialized views, automated background task orchestration, and a unified REST API gateway.

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Directory Layout](#directory-layout)
3. [Environment & Setup](#environment--setup)
4. [Data Ingestion & Routing Engine](#data-ingestion--routing-engine)
5. [Automated Quality Rules & Quarantine](#automated-quality-rules--quarantine)
6. [Database Design & Indexing Strategy](#database-design--indexing-strategy)
7. [Query Performance & Explain Analysis](#query-performance--explain-analysis)
8. [Business Analytics & Aggregation Pipelines](#business-analytics--aggregation-pipelines)
9. [Materialized Views & Incremental Synchronization](#materialized-views--incremental-synchronization)
10. [Automated Task Scheduling & Audit Logs](#automated-task-scheduling--audit-logs)
11. [Unified REST API Reference](#unified-rest-api-reference)
12. [Verification & Benchmark Commands](#verification--benchmark-commands)

---

## Architecture Overview

The platform implements an Extract-Load-Transform (ELT) architecture designed for high-throughput ecommerce order processing and analytics:

```
[Raw CSV Dataset]
       │
       ▼
[File Router] ── (File Size <= 200MB) ──► [Python Batch Loader]
       │
       └───────── (File Size > 200MB)  ──► [Apache Spark Distributed Loader]
                                                      │
       ┌──────────────────────────────────────────────┘
       ▼
[Raw Staging] ──────────────────────────► MongoDB: `orders_raw`
       │
       ▼
[Quality & Cleaning Engine] 
       │
       ├── (Validation Failure) ────────► MongoDB: `orders_quarantine`
       │
       └── (Valid / Auto-Corrected) ────► MongoDB: `orders_validated` (Unique Upsert)
                                                      │
       ┌──────────────────────────────────────────────┴──────────────────────────┐
       ▼                                                                         ▼
[Indexing & Query Engine]                                            [Analytics Aggregations]
  - Compound B-Trees                                                   - Sales by Municipality
  - Execution Plan Benchmarking                                        - Catalog Item Velocity
  - Sub-millisecond Seeks                                              - Customer Lifetime Value
       │                                                                         │
       ▼                                                                         ▼
[REST API Gateway (FastAPI)] ◄────────────────────────────── [Incremental Materialized Views]
  - Interactive OpenAPI / Swagger                                      - `daily_sales_summary`
  - Automated Task Orchestration                                       - `top_products_summary`
  - Operational Query Endpoints                                        - High-Watermark Sync
```

---

## Directory Layout

```
├── config/
│   └── settings.py               # Centralized configuration and environment loader
├── data/
│   └── .gitkeep                  # Staging directory for input data files
├── docs/                         # Architecture documentation and technical specs
├── reports/
│   ├── explain_benchmark_results.json # Execution plan before/after benchmarks
│   ├── results.json              # Ingestion run summary metrics
│   └── results.md                # System validation reports
├── src/
│   ├── aggregations.py           # Analytical aggregation pipelines
│   ├── api.py                    # Unified FastAPI REST application
│   ├── batch_loader.py           # Memory-efficient Python batch processor
│   ├── elt_pipeline.py           # Pipeline execution orchestrator
│   ├── file_router.py            # Size-based engine dispatch router
│   ├── incremental_loader.py     # Delta ingestion processor
│   ├── main.py                   # Main pipeline CLI entrypoint
│   ├── materialized_views.py     # Incremental materialized view synchronizer
│   ├── metrics.py                # Operational metrics recorder
│   ├── mongo_setup.py            # Database client and collection setup
│   ├── quality_rules.py          # Data validation, cleaning, and normalization rules
│   ├── queries_and_indexes.py    # Operational queries and index management
│   ├── run_explain_benchmark.py  # Standalone query plan benchmarking CLI
│   ├── scheduler.py              # Background task scheduling daemon and logger
│   └── spark_loader.py           # PySpark distributed batch processing engine
├── tests/
│   ├── test_cleaning_rules.py    # Unit tests for transformation functions
│   └── test_classification.py    # Quality rule validation tests
├── .env.example                  # Environment configuration template
├── README.md                     # Technical system manual
└── requirements.txt              # Production dependency specifications
```

---

## Environment & Setup

### Prerequisites
- Python 3.10+
- MongoDB 6.0+ (Community or Enterprise Server)
- Java 8/11 (Optional, required only for PySpark distributed execution)

### Installation

1. Clone or unpack the repository into your working directory:
   ```bash
   cd RIYADH_ELT_PIPELINE-main
   ```

2. Configure environment parameters:
   ```bash
   cp .env.example .env
   ```

3. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Verify MongoDB connectivity:
   ```bash
   python -c "from src.mongo_setup import get_database; db = get_database(); db.command('ping'); print('MongoDB Connected successfully.')"
   ```

---

## Data Ingestion & Routing Engine

The platform incorporates an adaptive engine router (`src/file_router.py`):
- **Files <= 200 MB**: Routed to `Python Batch Loader` (`src/batch_loader.py`) for low-overhead, memory-bounded batch chunking.
- **Files > 200 MB**: Routed to `Apache Spark Distributed Loader` (`src/spark_loader.py`) for parallel partition-based ingestion.

### Full Pipeline Execution
```bash
python src/main.py --csv data/orders_sample.csv
```

Optional CLI flags:
- `--engine python_batch` or `--engine pyspark`: Forces specific processing engine.
- `--reset-db`: Clears collections prior to loading.
- `--threshold-mb <value>`: Overrides file routing threshold.

---

## Automated Quality Rules & Quarantine

All raw records pass through deterministic quality validation (`src/quality_rules.py`):
1. **Identity Integrity**: Validates `order_id` and `customer_id` presence.
2. **Arabic Numerals & Words**: Standardizes Arabic digit glyphs and text words into IEEE floating point numbers.
3. **Currency Standardization**: Extracts numeric values and standardizes ISO currency codes.
4. **Status Normalization**: Maps localized synonyms to canonical states (`COMPLETED`, `CANCELLED`, `PENDING`, `CONFIRMED`, `PAID`, `REFUNDED`).
5. **Item Array Parsing**: Parses stringified JSON arrays into structured documents.
6. **Total Reconciliation**: Recomputes basket sum plus logistics fees to detect and correct discrepancies.
7. **Quarantine Routing**: Records with fatal defects (missing IDs, corrupted JSON, unresolvable dates) are routed directly to `orders_quarantine` with error details and issue keys.

---

## Database Design & Indexing Strategy

Target MongoDB Collections:
- **`orders_raw`**: Immutable record of ingested data.
- **`orders_validated`**: Clean and standardized business documents with a unique index on `order_id` for idempotent upsert operations.
- **`orders_quarantine`**: Quarantined records with stable issue keys.
- **`pipeline_audit_logs`**: Execution runtime metrics, row counts, and error tracking.

### Operational Indexes (`src/queries_and_indexes.py`)

| Index Name | Keys | Index Type | Target Workload |
|---|---|---|---|
| `uniq_order_id` | `{"order_id": 1}` | Unique | Primary business key; enables idempotent upserts |
| `idx_status_order_date` | `{"status": 1, "order_date": -1}` | Compound | Status filtering with index-covered date sorting |
| `idx_customer_date` | `{"customer.customer_id": 1, "order_date": -1}` | Compound | Customer profile order timeline lookups |
| `idx_compound_city_amount` | `{"customer.address.city": 1, "total_amount": -1}` | Compound | High-value geographic market filtering and sorting |
| `idx_order_date_range` | `{"order_date": -1}` | Single | Calendar bounded range scans |
| `idx_payment_delivery` | `{"payment.method": 1, "delivery.type": 1, "status": 1}` | Compound | Multi-attribute payment gateway and logistics analysis |

---

## Query Performance & Explain Analysis

Query optimization is verified via MongoDB `explain("executionStats")`. The application records plan metrics before and after index creation:

| Query Identifier | Applied Index | Baseline Stage | Optimized Stage | Scanned Documents Reduction | Optimization Rationale |
|---|---|---|---|---|---|
| `q1_orders_by_status` | `idx_status_order_date` | `COLLSCAN` | `IXSCAN` | > 95% | Eliminates full scan and in-memory SORT stage |
| `q3_city_high_value_orders` | `idx_compound_city_amount` | `COLLSCAN` | `IXSCAN` | > 95% | Directly navigates to city B-Tree branch and evaluates amount range |
| `q5_orders_by_payment_and_delivery` | `idx_payment_delivery` | `COLLSCAN` | `IXSCAN` | > 95% | Satisfies compound equality predicates via single index seek |

### Running Benchmark Utility
```bash
python src/run_explain_benchmark.py
```
Detailed results are exported to `reports/explain_benchmark_results.json`.

---

## Business Analytics & Aggregation Pipelines

The analytics engine (`src/aggregations.py`) provides 5 aggregation pipelines:

1. **`sales_by_city`**: Aggregates gross sales, total order volume, and order size distribution by municipality.
2. **`top_products`**: Evaluates catalog item velocity, total quantities sold, and gross revenue using `$unwind` on line items.
3. **`top_customers`**: Identifies highest-value accounts ranked by cumulative spend and transaction frequency.
4. **`sales_by_period`**: Computes daily sales volume, shipping revenue, and average transaction size over calendar periods.
5. **`orders_by_status_distribution`**: Computes lifecycle state ratios, total revenue, and average order values.

---

## Materialized Views & Incremental Synchronization

To support fast analytical queries without computing aggregations on demand, the system maintains pre-computed collections (`src/materialized_views.py`):
- **`mv_daily_sales_summary`**: Daily order metrics, gross revenues, and fulfillment fee totals.
- **`mv_top_products_summary`**: Catalog product sales counts, units moved, and revenues.

### Incremental Synchronization Mechanism
Rather than rebuilding collections from scratch:
1. The synchronizer tracks an incremental checkpoint (**High-Watermark**) in `mv_refresh_metadata`.
2. Periodic executions query only delta orders where `order_date > watermark`.
3. Delta records are transformed and atomically upserted using `UpdateOne(upsert=True)` bulk writes.
4. Full rebuild remains available via `full_refresh=True`.

---

## Automated Task Scheduling & Audit Logs

The background task engine (`src/scheduler.py`) manages recurring operations:
- **`refresh_materialized_views_job`**: Performs incremental synchronization of materialized views (Interval: 10 minutes).
- **`generate_periodic_report_job`**: Compiles periodic executive analytics snapshots (Interval: 30 minutes).

### Execution Auditing
Every task execution (scheduled or manual) is recorded in the `job_execution_logs` collection:
- `job_name`: Task identifier
- `trigger_type`: `SCHEDULED` or `MANUAL`
- `start_time` / `end_time`: UTC timestamps
- `duration_seconds`: Runtime duration
- `status`: `SUCCESS` or `FAILED`
- `result_summary` / `error`: Execution payload or exception trace

---

## Unified REST API Reference

The platform provides a unified FastAPI service (`src/api.py`) exposing all pipeline capabilities:

```bash
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
```
Interactive Swagger documentation is available at: **`http://localhost:8000/docs`**

### Endpoint Specification

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health status and MongoDB connection verification |
| `POST` | `/ingest` | Triggers pipeline batch ingestion for a target CSV file |
| `POST` | `/indexes` | Builds all defined single and compound operational indexes |
| `GET` | `/indexes/benchmark` | Runs query plan evaluation and returns comparative metrics |
| `GET` | `/queries` | Lists all registered operational queries and parameter schemas |
| `GET` | `/queries/{name}` | Executes a registered query (supports `?explain=true` for plan inspect) |
| `GET` | `/aggregations` | Lists available business aggregation reports |
| `GET` | `/aggregations/{name}` | Computes and returns live aggregation results |
| `POST` | `/refresh-mv` | Triggers incremental synchronization of materialized views |
| `GET` | `/materialized-views/{name}` | Queries pre-computed records from a materialized view |
| `GET` | `/jobs` | Lists background tasks and recent execution audit logs |
| `POST` | `/jobs/{name}/run` | Manually triggers synchronous execution of a registered job |

---

## Verification & Benchmark Commands

### 1. Execute Pipeline Ingestion
```bash
python src/main.py
```

### 2. Verify Database Indexes
```bash
curl -X POST http://localhost:8000/indexes
```

### 3. Evaluate Query Execution Plan
```bash
python src/run_explain_benchmark.py
```

### 4. Query Analytical Aggregation
```bash
curl http://localhost:8000/aggregations/sales_by_city
```

### 5. Synchronize Materialized Views
```bash
curl -X POST http://localhost:8000/refresh-mv
```

### 6. Trigger Scheduled Job Manually & Inspect Audit Logs
```bash
curl -X POST http://localhost:8000/jobs/refresh_materialized_views_job/run
curl http://localhost:8000/jobs
```

### 7. Run Automated Test Suite
```bash
pytest tests/ -v
```
