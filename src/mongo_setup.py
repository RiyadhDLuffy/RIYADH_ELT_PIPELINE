"""MongoDB connection setup, collection management, and indexing."""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any
import pymongo
from pymongo import MongoClient, ASCENDING
from pymongo.database import Database

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_URI,
    DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    AUDIT_COLLECTION,
)

logger = logging.getLogger("mongo_setup")


def get_mongo_client(uri: str = MONGO_URI) -> MongoClient:
    """Creates a MongoClient with connection timeout."""
    return MongoClient(uri, serverSelectionTimeoutMS=10_000)


def get_database(client: MongoClient | None = None, db_name: str = DB_NAME) -> Database:
    """Returns database instance."""
    if client is None:
        client = get_mongo_client()
    return client[db_name]


def setup_collections_and_indexes(db: Database | None = None) -> dict[str, Any]:
    """Ensures collections and required unique/performance indexes exist.
    
    1. orders_raw: No unique constraint to prevent blocking raw ingestion.
    2. orders_validated: Unique index on order_id for Idempotency & Upserts.
    3. orders_quarantine: Index on issue_key and run_id.
    4. pipeline_audit_logs: Index on run_id and timestamp.
    """
    if db is None:
        db = get_database()

    # 1. orders_raw indexes
    raw_col = db[RAW_COLLECTION]
    raw_col.create_index([("run_id", ASCENDING)])
    raw_col.create_index([("ingested_at", ASCENDING)])

    # 2. orders_validated indexes (STABLE BUSINESS KEY)
    val_col = db[VALIDATED_COLLECTION]
    val_col.create_index([("order_id", ASCENDING)], unique=True, name="uniq_order_id")
    val_col.create_index([("customer.customer_id", ASCENDING)])
    val_col.create_index([("order_date", ASCENDING)])
    val_col.create_index([("last_run_id", ASCENDING)])

    # 3. orders_quarantine indexes
    quar_col = db[QUARANTINE_COLLECTION]
    quar_col.create_index([("issue_key", ASCENDING)], unique=True, name="uniq_issue_key")
    quar_col.create_index([("order_id", ASCENDING)])
    quar_col.create_index([("last_run_id", ASCENDING)])
    quar_col.create_index([("history_status", ASCENDING)])

    # 4. pipeline_audit_logs indexes
    audit_col = db[AUDIT_COLLECTION]
    audit_col.create_index([("run_id", ASCENDING)], unique=True)
    audit_col.create_index([("timestamp", ASCENDING)])

    return {
        "database": db.name,
        "collections": [RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION, AUDIT_COLLECTION],
        "status": "ready",
    }


def reset_database(db: Database | None = None) -> None:
    """Drops collections for testing and re-initializes indexes."""
    if db is None:
        db = get_database()
    for col_name in [RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION, AUDIT_COLLECTION]:
        db[col_name].drop()
    setup_collections_and_indexes(db)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    info = setup_collections_and_indexes()
    print("Database & Collections initialized successfully:", info)
