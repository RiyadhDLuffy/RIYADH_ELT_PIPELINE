"""Materialized Views Management and Incremental Synchronization.

Maintains aggregate pre-computed collections:
- daily_sales_summary
- top_products_summary

Supports high-throughput incremental delta refresh using watermarks
and idempotent atomic upserts without full collection rebuilds.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pymongo import UpdateOne
from pymongo.database import Database

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database

logger = logging.getLogger("materialized_views")

MV_DAILY_SALES = "mv_daily_sales_summary"
MV_TOP_PRODUCTS = "mv_top_products_summary"
MV_METADATA_COLLECTION = "mv_refresh_metadata"


def utc_now() -> str:
    """Current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_view_watermark(db: Database, view_name: str) -> Optional[str]:
    """Retrieves the high-watermark timestamp for the target materialized view."""
    meta = db[MV_METADATA_COLLECTION].find_one({"view_name": view_name})
    return meta.get("last_processed_order_date") if meta else None


def update_view_watermark(db: Database, view_name: str, watermark: str, records_updated: int):
    """Persists the watermark checkpoint for incremental synchronization."""
    db[MV_METADATA_COLLECTION].update_one(
        {"view_name": view_name},
        {
            "$set": {
                "view_name": view_name,
                "last_processed_order_date": watermark,
                "last_refreshed_at": utc_now(),
                "last_records_processed": records_updated,
            }
        },
        upsert=True,
    )


def refresh_daily_sales_summary(
    db: Optional[Database] = None,
    full_refresh: bool = False,
) -> Dict[str, Any]:
    """Incrementally synchronizes daily_sales_summary against recent orders."""
    if db is None:
        db = get_database()

    val_col = db[VALIDATED_COLLECTION]
    target_col = db[MV_DAILY_SALES]

    watermark = None if full_refresh else get_view_watermark(db, MV_DAILY_SALES)
    match_stage: Dict[str, Any] = {"status": {"$nin": ["CANCELLED", "REFUNDED"]}}
    if watermark:
        match_stage["order_date"] = {"$gt": watermark}

    pipeline = [
        {"$match": match_stage},
        {
            "$project": {
                "date": {"$substrCP": [{"$ifNull": ["$order_date", "1970-01-01"]}, 0, 10]},
                "total_amount": 1,
                "delivery_cost": {"$ifNull": ["$delivery.cost", 0.0]},
            }
        },
        {
            "$group": {
                "_id": "$date",
                "total_orders": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "avg_order_value": {"$avg": "$total_amount"},
                "total_delivery_fees": {"$sum": "$delivery_cost"},
                "max_order_date": {"$max": "$date"},
            }
        },
    ]

    delta_results = list(val_col.aggregate(pipeline))

    if not delta_results:
        return {
            "view_name": MV_DAILY_SALES,
            "status": "UP_TO_DATE",
            "mode": "FULL" if full_refresh else "INCREMENTAL",
            "records_processed": 0,
            "refreshed_at": utc_now(),
        }

    operations = []
    max_date = watermark or "1970-01-01"
    for row in delta_results:
        date_key = row["_id"]
        if date_key > max_date:
            max_date = date_key

        doc = {
            "date": date_key,
            "total_orders": row["total_orders"],
            "total_sales": round(float(row["total_sales"]), 2),
            "avg_order_value": round(float(row["avg_order_value"]), 2),
            "total_delivery_fees": round(float(row["total_delivery_fees"]), 2),
            "last_updated_at": utc_now(),
        }
        operations.append(
            UpdateOne({"_id": date_key}, {"$set": doc}, upsert=True)
        )

    if operations:
        target_col.bulk_write(operations, ordered=False)

    update_view_watermark(db, MV_DAILY_SALES, max_date, len(operations))

    return {
        "view_name": MV_DAILY_SALES,
        "status": "SUCCESS",
        "mode": "FULL" if full_refresh else "INCREMENTAL",
        "records_processed": len(operations),
        "new_watermark": max_date,
        "refreshed_at": utc_now(),
    }


def refresh_top_products_summary(
    db: Optional[Database] = None,
    full_refresh: bool = False,
) -> Dict[str, Any]:
    """Incrementally synchronizes top_products_summary against recent orders."""
    if db is None:
        db = get_database()

    val_col = db[VALIDATED_COLLECTION]
    target_col = db[MV_TOP_PRODUCTS]

    watermark = None if full_refresh else get_view_watermark(db, MV_TOP_PRODUCTS)
    match_stage: Dict[str, Any] = {"status": {"$nin": ["CANCELLED", "REFUNDED"]}}
    if watermark:
        match_stage["order_date"] = {"$gt": watermark}

    pipeline = [
        {"$match": match_stage},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": {"$ifNull": ["$items.sku", "UNKNOWN_SKU"]},
                "product_name": {"$first": "$items.name"},
                "total_quantity_sold": {"$sum": "$items.qty"},
                "total_revenue": {"$sum": "$items.total"},
                "order_appearances": {"$sum": 1},
                "max_order_date": {"$max": "$order_date"},
            }
        },
    ]

    delta_results = list(val_col.aggregate(pipeline))

    if not delta_results:
        return {
            "view_name": MV_TOP_PRODUCTS,
            "status": "UP_TO_DATE",
            "mode": "FULL" if full_refresh else "INCREMENTAL",
            "records_processed": 0,
            "refreshed_at": utc_now(),
        }

    operations = []
    max_date = watermark or "1970-01-01"
    for row in delta_results:
        sku = row["_id"]
        row_max_date = str(row.get("max_order_date") or "")
        if row_max_date > max_date:
            max_date = row_max_date

        doc = {
            "sku": sku,
            "product_name": row.get("product_name") or "Unnamed Item",
            "total_quantity_sold": row["total_quantity_sold"],
            "total_revenue": round(float(row["total_revenue"]), 2),
            "order_appearances": row["order_appearances"],
            "last_updated_at": utc_now(),
        }
        operations.append(
            UpdateOne({"_id": sku}, {"$set": doc}, upsert=True)
        )

    if operations:
        target_col.bulk_write(operations, ordered=False)

    update_view_watermark(db, MV_TOP_PRODUCTS, max_date, len(operations))

    return {
        "view_name": MV_TOP_PRODUCTS,
        "status": "SUCCESS",
        "mode": "FULL" if full_refresh else "INCREMENTAL",
        "records_processed": len(operations),
        "new_watermark": max_date,
        "refreshed_at": utc_now(),
    }


def refresh_all_materialized_views(
    db: Optional[Database] = None,
    full_refresh: bool = False,
) -> Dict[str, Any]:
    """Triggers refresh for all defined materialized views."""
    if db is None:
        db = get_database()

    res1 = refresh_daily_sales_summary(db, full_refresh=full_refresh)
    res2 = refresh_top_products_summary(db, full_refresh=full_refresh)

    return {
        "status": "COMPLETED",
        "views": {
            "daily_sales_summary": res1,
            "top_products_summary": res2,
        },
        "refreshed_at": utc_now(),
    }


def get_materialized_view_data(
    db: Optional[Database] = None,
    view_name: str = "daily_sales_summary",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieves analytical records from a pre-computed materialized view collection."""
    if db is None:
        db = get_database()

    col_map = {
        "daily_sales_summary": MV_DAILY_SALES,
        "top_products_summary": MV_TOP_PRODUCTS,
    }
    target_col_name = col_map.get(view_name, view_name)
    col = db[target_col_name]
    return list(col.find({}, {"_id": 0}).limit(limit))
