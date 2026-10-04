"""Streaming Python Batch Loader for small/medium CSV files (< threshold)."""
from __future__ import annotations

import csv
import logging
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import UpdateOne
from pymongo.database import Database

from config.settings import (
    BATCH_SIZE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)
from src.quality_rules import transform_and_classify_row, utc_now

logger = logging.getLogger("batch_loader")


def process_python_batch(
    file_path: Path,
    db: Database,
    run_id: str,
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    """Streams CSV file in chunks, executes raw load, transformation, validation, and idempotent upserts."""
    raw_col = db[RAW_COLLECTION]
    val_col = db[VALIDATED_COLLECTION]
    quar_col = db[QUARANTINE_COLLECTION]

    import os
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        mem_start_mb = proc.memory_info().rss / (1024 * 1024)
    except Exception:
        proc = None
        mem_start_mb = 0.0

    start_time = time.perf_counter()
    now_ts = utc_now()

    raw_batch: list[dict[str, Any]] = []
    val_ops: list[UpdateOne] = []
    quar_ops: list[UpdateOne] = []

    # Metrics counters
    raw_loaded = 0
    valid_count = 0
    corrected_count = 0
    quarantine_count = 0
    error_counts: Counter[str] = Counter()

    # Track pre-existing IDs for accurate insert vs update detection
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0

    print(f"[{now_ts}] [PYTHON_BATCH] Streaming from: {file_path.name} (Batch size: {batch_size:,})")

    with open(file_path, mode="r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)

        batch_idx = 0
        for row_idx, raw_dict in enumerate(reader, start=1):
            raw_loaded += 1

            # 1. ELT Principle: Raw Ingestion Metadata
            raw_doc = {
                "run_id": run_id,
                "source_file": file_path.name,
                "source_row_number": row_idx,
                "ingested_at": now_ts,
                "engine_used": "python_batch",
                "raw_record": raw_dict,
            }
            raw_batch.append(raw_doc)

            # 2. Transform & Quality Classification
            cat, val_doc, quar_doc = transform_and_classify_row(raw_dict, run_id)

            if cat == "VALID":
                valid_count += 1
                val_ops.append(
                    UpdateOne(
                        {"order_id": val_doc["order_id"]},
                        {"$set": val_doc, "$setOnInsert": {"created_at": now_ts}},
                        upsert=True,
                    )
                )
            elif cat == "CORRECTED":
                corrected_count += 1
                val_ops.append(
                    UpdateOne(
                        {"order_id": val_doc["order_id"]},
                        {"$set": val_doc, "$setOnInsert": {"created_at": now_ts}},
                        upsert=True,
                    )
                )
            else:  # QUARANTINE
                quarantine_count += 1
                for err in quar_doc.get("error_codes", []):
                    error_counts[err] += 1
                quar_ops.append(
                    UpdateOne(
                        {"issue_key": quar_doc["issue_key"]},
                        {"$set": quar_doc, "$setOnInsert": {"first_seen_at": now_ts}},
                        upsert=True,
                    )
                )

            # Flush batch when size threshold reached
            if len(raw_batch) >= batch_size:
                batch_idx += 1
                _raw_to_flush = list(raw_batch)
                _val_to_flush = list(val_ops)
                _quar_to_flush = list(quar_ops)
                raw_batch.clear()
                val_ops.clear()
                quar_ops.clear()

                def _flush_raw(docs=_raw_to_flush):
                    raw_col.insert_many(docs, ordered=False)

                def _flush_val(ops=_val_to_flush):
                    if ops:
                        return val_col.bulk_write(ops, ordered=False)

                def _flush_quar(ops=_quar_to_flush):
                    if ops:
                        quar_col.bulk_write(ops, ordered=False)

                with ThreadPoolExecutor(max_workers=3) as pool:
                    fut_raw = pool.submit(_flush_raw)
                    fut_val = pool.submit(_flush_val)
                    fut_quar = pool.submit(_flush_quar)
                    fut_raw.result()
                    res = fut_val.result()
                    fut_quar.result()

                if res:
                    inserted_count += res.upserted_count
                    updated_count += res.modified_count
                    unchanged_count += (res.matched_count - res.modified_count)

                elapsed_curr = time.perf_counter() - start_time
                throughput_curr = raw_loaded / elapsed_curr if elapsed_curr > 0 else 0
                if batch_idx % 5 == 0 or raw_loaded < 10_000:
                    print(
                        f"  -> Batch #{batch_idx:,} | Ingested: {raw_loaded:,} rows | "
                        f"Rate: {throughput_curr:,.0f} rows/s"
                    )

        # Final remaining flush
        if raw_batch:
            raw_col.insert_many(raw_batch, ordered=False)
            raw_batch.clear()

        if val_ops:
            res = val_col.bulk_write(val_ops, ordered=False)
            inserted_count += res.upserted_count
            updated_count += res.modified_count
            unchanged_count += (res.matched_count - res.modified_count)
            val_ops.clear()

        if quar_ops:
            quar_col.bulk_write(quar_ops, ordered=False)
            quar_ops.clear()

    total_elapsed = time.perf_counter() - start_time
    total_throughput = raw_loaded / total_elapsed if total_elapsed > 0 else 0.0

    if proc is not None:
        try:
            mem_end_mb = proc.memory_info().rss / (1024 * 1024)
        except Exception:
            mem_end_mb = mem_start_mb
    else:
        mem_end_mb = 0.0
    mem_delta_mb = mem_end_mb - mem_start_mb

    # Consistency Check Equation (Section 6.11)
    # run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
    is_consistent = raw_loaded == (valid_count + corrected_count + quarantine_count)

    return {
        "engine_used": "python_batch",
        "rows_read": raw_loaded,
        "raw_loaded": raw_loaded,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "is_consistent": is_consistent,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "error_case_counts": dict(error_counts),
        "batch_size": batch_size,
        "elapsed_seconds": round(total_elapsed, 2),
        "throughput": round(total_throughput, 2),
        "mem_start_mb": round(mem_start_mb, 2),
        "mem_end_mb": round(mem_end_mb, 2),
        "mem_delta_mb": round(mem_delta_mb, 2),
    }
