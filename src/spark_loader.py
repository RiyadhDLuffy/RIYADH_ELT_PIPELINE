"""Distributed PySpark Loader for large CSV files (>= threshold)."""
from __future__ import annotations

import logging
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

# Fix for Windows: Microsoft Store stubs the 'python' command, causing
# PySpark executor workers to fail to connect. Force-set both env vars
# AND pass them as Spark config so the JVM picks them up directly.
_PYTHON_EXEC = sys.executable
os.environ["PYSPARK_PYTHON"] = _PYTHON_EXEC
os.environ["PYSPARK_DRIVER_PYTHON"] = _PYTHON_EXEC

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_URI,
    DB_NAME,
    BATCH_SIZE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    SPARK_DEFAULT_PARTITIONS,
    SPARK_DRIVER_MEMORY,
)
from src.quality_rules import transform_and_classify_row, utc_now

logger = logging.getLogger("spark_loader")


def process_pyspark_batch(
    file_path: Path,
    db: Database,
    run_id: str,
    target_partitions: int = SPARK_DEFAULT_PARTITIONS,
) -> dict[str, Any]:
    """Processes large CSV using PySpark, performing parallel partition writes into MongoDB."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType

    start_time = time.perf_counter()
    now_ts = utc_now()

    # Fixed permissive schema to preserve raw uncleaned strings
    csv_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True),
    ])

    file_size_bytes = file_path.stat().st_size if file_path.exists() else 0
    if target_partitions > 0 and file_size_bytes > 200 * 1024 * 1024:
        calc_partition_bytes = max(1024 * 1024, file_size_bytes // target_partitions)
    else:
        calc_partition_bytes = 128 * 1024 * 1024

    spark = (
        SparkSession.builder
        .appName(f"ELT-PySpark-Hybrid-{run_id}")
        .master("local[*]")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_DRIVER_MEMORY)
        .config("spark.default.parallelism", str(target_partitions))
        .config("spark.pyspark.python", _PYTHON_EXEC)
        .config("spark.pyspark.driver.python", _PYTHON_EXEC)
        .config("spark.sql.shuffle.partitions", str(target_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", str(calc_partition_bytes))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.python.worker.reuse", "true")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"[{now_ts}] [PYSPARK] Spark Engine initialized (Master: {spark.sparkContext.master})")

        df = (
            spark.read
            .schema(csv_schema)
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("escape", '"')       # RFC 4180: "" inside quoted fields
            .csv(str(file_path))
        )

        input_partitions = df.rdd.getNumPartitions()
        if target_partitions > 0 and input_partitions != target_partitions and file_size_bytes > 200 * 1024 * 1024:
            df = df.repartition(target_partitions)
            input_partitions = df.rdd.getNumPartitions()

        file_name = file_path.name

        def partition_worker(partition_rows: Iterable[Any]) -> Iterable[dict[str, Any]]:
            """Worker executed inside Spark Executors: streams partition directly to MongoDB."""
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
            worker_db = client[DB_NAME]
            raw_col = worker_db[RAW_COLLECTION]
            val_col = worker_db[VALIDATED_COLLECTION]
            quar_col = worker_db[QUARANTINE_COLLECTION]

            p_raw_batch: list[dict[str, Any]] = []
            p_val_ops: list[UpdateOne] = []
            p_quar_ops: list[UpdateOne] = []

            p_raw_count = 0
            p_valid_count = 0
            p_corrected_count = 0
            p_quar_count = 0
            p_inserted = 0
            p_updated = 0
            p_unchanged = 0
            p_errors: Counter[str] = Counter()

            def flush_worker_buffers():
                nonlocal p_inserted, p_updated, p_unchanged
                if p_raw_batch:
                    raw_col.insert_many(p_raw_batch, ordered=False)
                    p_raw_batch.clear()
                if p_val_ops:
                    res = val_col.bulk_write(p_val_ops, ordered=False)
                    p_inserted += res.upserted_count
                    p_updated += res.modified_count
                    p_unchanged += (res.matched_count - res.modified_count)
                    p_val_ops.clear()
                if p_quar_ops:
                    quar_col.bulk_write(p_quar_ops, ordered=False)
                    p_quar_ops.clear()

            for idx, spark_row in enumerate(partition_rows, start=1):
                p_raw_count += 1
                raw_dict = spark_row.asDict(recursive=True)

                # 1. ELT Raw Ingestion doc
                raw_doc = {
                    "run_id": run_id,
                    "source_file": file_name,
                    "source_row_number": idx,
                    "ingested_at": now_ts,
                    "engine_used": "pyspark",
                    "raw_record": raw_dict,
                }
                p_raw_batch.append(raw_doc)

                # 2. Transform & Quality Classification
                cat, val_doc, quar_doc = transform_and_classify_row(raw_dict, run_id)
                if cat == "VALID":
                    p_valid_count += 1
                    p_val_ops.append(
                        UpdateOne(
                            {"order_id": val_doc["order_id"]},
                            {"$set": val_doc, "$setOnInsert": {"created_at": now_ts}},
                            upsert=True,
                        )
                    )
                elif cat == "CORRECTED":
                    p_corrected_count += 1
                    p_val_ops.append(
                        UpdateOne(
                            {"order_id": val_doc["order_id"]},
                            {"$set": val_doc, "$setOnInsert": {"created_at": now_ts}},
                            upsert=True,
                        )
                    )
                else:
                    p_quar_count += 1
                    for err in quar_doc.get("error_codes", []):
                        p_errors[err] += 1
                    p_quar_ops.append(
                        UpdateOne(
                            {"issue_key": quar_doc["issue_key"]},
                            {"$set": quar_doc, "$setOnInsert": {"first_seen_at": now_ts}},
                            upsert=True,
                        )
                    )

                if len(p_raw_batch) >= BATCH_SIZE:
                    flush_worker_buffers()

            flush_worker_buffers()
            client.close()

            # Yield partition summary metric to driver
            yield {
                "raw_count": p_raw_count,
                "valid_count": p_valid_count,
                "corrected_count": p_corrected_count,
                "quarantine_count": p_quar_count,
                "inserted_count": p_inserted,
                "updated_count": p_updated,
                "unchanged_count": p_unchanged,
                "error_counts": dict(p_errors),
            }

        # Execute mapPartitions in parallel across executors
        partition_summaries = df.rdd.mapPartitions(partition_worker).collect()

    finally:
        spark.stop()

    # Aggregate summaries on Driver
    total_raw = sum(s["raw_count"] for s in partition_summaries)
    total_valid = sum(s["valid_count"] for s in partition_summaries)
    total_corrected = sum(s["corrected_count"] for s in partition_summaries)
    total_quar = sum(s["quarantine_count"] for s in partition_summaries)
    total_inserted = sum(s["inserted_count"] for s in partition_summaries)
    total_updated = sum(s["updated_count"] for s in partition_summaries)
    total_unchanged = sum(s["unchanged_count"] for s in partition_summaries)

    combined_errors: Counter[str] = Counter()
    for s in partition_summaries:
        combined_errors.update(s["error_counts"])

    total_elapsed = time.perf_counter() - start_time
    total_throughput = total_raw / total_elapsed if total_elapsed > 0 else 0.0

    is_consistent = total_raw == (total_valid + total_corrected + total_quar)

    return {
        "engine_used": "pyspark",
        "rows_read": total_raw,
        "raw_loaded": total_raw,
        "valid_count": total_valid,
        "corrected_count": total_corrected,
        "quarantine_count": total_quar,
        "is_consistent": is_consistent,
        "inserted_count": total_inserted,
        "updated_count": total_updated,
        "unchanged_count": total_unchanged,
        "error_case_counts": dict(combined_errors),
        "partitions": input_partitions,
        "elapsed_seconds": round(total_elapsed, 2),
        "throughput": round(total_throughput, 2),
    }
