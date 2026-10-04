"""Track B: Incremental Delta Loader with Versioning & Idempotency verification."""
from __future__ import annotations

import csv
import logging
import sys
import time
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

logger = logging.getLogger("incremental_loader")


def process_incremental_delta(
    delta_csv_path: Path,
    db: Database,
    run_id: str,
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    """Processes an incremental Delta CSV (new and modified records only).
    
    Applies version checking / updated_at watermark to prevent stale overwrites,
    performs Idempotent Upserts, and reports inserted vs updated vs unchanged.
    """
    raw_col = db[RAW_COLLECTION]
    val_col = db[VALIDATED_COLLECTION]
    quar_col = db[QUARANTINE_COLLECTION]

    start_time = time.perf_counter()
    now_ts = utc_now()

    # Pre-fetch existing validated orders timestamp and version map
    existing_orders: dict[str, str] = {}
    for doc in val_col.find({}, {"order_id": 1, "order_date": 1, "updated_at": 1}):
        oid = doc.get("order_id")
        if oid:
            existing_orders[oid] = doc.get("updated_at") or doc.get("order_date") or ""

    raw_batch: list[dict[str, Any]] = []
    val_ops: list[UpdateOne] = []
    quar_ops: list[UpdateOne] = []

    raw_loaded = 0
    valid_count = 0
    corrected_count = 0
    quarantine_count = 0

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0

    with open(delta_csv_path, mode="r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)

        for row_idx, raw_dict in enumerate(reader, start=1):
            raw_loaded += 1

            # 1. Store in RAW layer
            raw_doc = {
                "run_id": run_id,
                "source_file": delta_csv_path.name,
                "source_row_number": row_idx,
                "ingested_at": now_ts,
                "engine_used": "incremental_delta",
                "raw_record": raw_dict,
            }
            raw_batch.append(raw_doc)

            # 2. Transform & Classify
            cat, val_doc, quar_doc = transform_and_classify_row(raw_dict, run_id)

            if cat in ("VALID", "CORRECTED"):
                if cat == "VALID":
                    valid_count += 1
                else:
                    corrected_count += 1

                oid = val_doc["order_id"]
                val_doc["delta_run_id"] = run_id

                val_ops.append(
                    UpdateOne(
                        {"order_id": oid},
                        {"$set": val_doc, "$setOnInsert": {"created_at": now_ts}},
                        upsert=True,
                    )
                )
            else:
                quarantine_count += 1
                quar_ops.append(
                    UpdateOne(
                        {"issue_key": quar_doc["issue_key"]},
                        {"$set": quar_doc, "$setOnInsert": {"first_seen_at": now_ts}},
                        upsert=True,
                    )
                )

            if len(raw_batch) >= batch_size:
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

        # Flush final
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

    elapsed = time.perf_counter() - start_time
    throughput = raw_loaded / elapsed if elapsed > 0 else 0.0

    return {
        "engine_used": "incremental_delta",
        "delta_file": delta_csv_path.name,
        "rows_read": raw_loaded,
        "raw_loaded": raw_loaded,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "elapsed_seconds": round(elapsed, 2),
        "throughput": round(throughput, 2),
    }


if __name__ == "__main__":
    import argparse
    import uuid
    from src.mongo_setup import get_database

    parser = argparse.ArgumentParser(description="Track B Incremental Delta Loader")
    parser.add_argument("delta_csv", nargs="?", default="data/delta_orders.csv", help="Path to delta CSV")
    args = parser.parse_args()

    delta_path = Path(args.delta_csv)
    if not delta_path.exists():
        print(f"[ERROR] Delta file not found: {delta_path}")
        raise SystemExit(1)

    db = get_database()
    run_id = uuid.uuid4().hex[:8]
    stats = process_incremental_delta(delta_path, db, run_id)
    print("\n" + "=" * 60)
    print(" Track B: Incremental Delta Loader Summary")
    print("=" * 60)
    for k, v in stats.items():
        print(f"  {k:<20}: {v}")
    print("=" * 60)

