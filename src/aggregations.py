"""Data Aggregation and Analytics Reporting Module.

Provides aggregation pipelines generating business analytics,
customer spend analysis, product velocity, and lifecycle distribution.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from pymongo.database import Database

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database

logger = logging.getLogger("aggregations")


def report_sales_by_city(
    db: Database,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Aggregate total sales, order volume, and transaction averages grouped by municipality."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$match": {"status": {"$nin": ["CANCELLED", "REFUNDED"]}}},
        {
            "$group": {
                "_id": {"$ifNull": ["$customer.address.city", "Unspecified"]},
                "total_sales": {"$sum": "$total_amount"},
                "total_orders": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "min_order_value": {"$min": "$total_amount"},
                "max_order_value": {"$max": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_sales": {"$round": ["$total_sales", 2]},
                "total_orders": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "min_order_value": {"$round": ["$min_order_value", 2]},
                "max_order_value": {"$round": ["$max_order_value", 2]},
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": limit},
    ]
    return list(col.aggregate(pipeline))


def report_top_products(
    db: Database,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """Aggregate catalog product performance by quantity sold and gross revenue."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$match": {"status": {"$nin": ["CANCELLED", "REFUNDED"]}}},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": {
                    "sku": {"$ifNull": ["$items.sku", "UNKNOWN_SKU"]},
                    "name": {"$ifNull": ["$items.name", "Unnamed Item"]},
                },
                "total_quantity_sold": {"$sum": "$items.qty"},
                "total_revenue": {"$sum": "$items.total"},
                "orders_count": {"$sum": 1},
                "avg_unit_price": {"$avg": "$items.unit_price"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "sku": "$_id.sku",
                "product_name": "$_id.name",
                "total_quantity_sold": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": 1,
                "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit},
    ]
    return list(col.aggregate(pipeline))


def report_top_customers(
    db: Database,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Identify top customers ranked by cumulative lifetime purchase volume."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$match": {"status": {"$nin": ["CANCELLED", "REFUNDED"]}}},
        {
            "$group": {
                "_id": "$customer.customer_id",
                "customer_name": {"$first": "$customer.name"},
                "customer_phone": {"$first": "$customer.phone"},
                "city": {"$first": "$customer.address.city"},
                "total_spend": {"$sum": "$total_amount"},
                "orders_placed": {"$sum": 1},
                "avg_spend_per_order": {"$avg": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "customer_name": 1,
                "customer_phone": 1,
                "city": 1,
                "total_spend": {"$round": ["$total_spend", 2]},
                "orders_placed": 1,
                "avg_spend_per_order": {"$round": ["$avg_spend_per_order", 2]},
            }
        },
        {"$sort": {"total_spend": -1}},
        {"$limit": limit},
    ]
    return list(col.aggregate(pipeline))


def report_sales_by_period(
    db: Database,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """Aggregate periodic sales velocity, order count, and fulfillment fees by calendar day."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$match": {"status": {"$nin": ["CANCELLED", "REFUNDED"]}}},
        {
            "$project": {
                "period": {"$substrCP": [{"$ifNull": ["$order_date", "1970-01-01"]}, 0, 10]},
                "total_amount": 1,
                "delivery_cost": {"$ifNull": ["$delivery.cost", 0.0]},
            }
        },
        {
            "$group": {
                "_id": "$period",
                "daily_revenue": {"$sum": "$total_amount"},
                "daily_orders": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "total_delivery_fees": {"$sum": "$delivery_cost"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "daily_revenue": {"$round": ["$daily_revenue", 2]},
                "daily_orders": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "total_delivery_fees": {"$round": ["$total_delivery_fees", 2]},
            }
        },
        {"$sort": {"date": -1}},
        {"$limit": limit},
    ]
    return list(col.aggregate(pipeline))


def report_order_status_distribution(
    db: Database,
) -> List[Dict[str, Any]]:
    """Compute aggregate distribution and monetary volume across order lifecycle states."""
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {
            "$group": {
                "_id": {"$ifNull": ["$status", "UNKNOWN"]},
                "orders_count": {"$sum": 1},
                "total_value": {"$sum": "$total_amount"},
                "avg_value": {"$avg": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "orders_count": 1,
                "total_value": {"$round": ["$total_value", 2]},
                "avg_value": {"$round": ["$avg_value", 2]},
            }
        },
        {"$sort": {"orders_count": -1}},
    ]
    return list(col.aggregate(pipeline))


AGGREGATION_REGISTRY = {
    "sales_by_city": {
        "func": report_sales_by_city,
        "description": "Geographic sales breakdown by customer city with financial averages.",
        "default_params": {"limit": 20},
    },
    "top_products": {
        "func": report_top_products,
        "description": "Catalog item velocity ranking by unit count and total revenue.",
        "default_params": {"limit": 15},
    },
    "top_customers": {
        "func": report_top_customers,
        "description": "Customer lifetime valuation rankings and transaction counts.",
        "default_params": {"limit": 20},
    },
    "sales_by_period": {
        "func": report_sales_by_period,
        "description": "Chronological revenue trends and shipping revenue grouped by day.",
        "default_params": {"limit": 30},
    },
    "orders_by_status_distribution": {
        "func": report_order_status_distribution,
        "description": "Fulfillment status breakdown and portfolio monetary distribution.",
        "default_params": {},
    },
}


def run_aggregation(
    db: Optional[Database] = None,
    name: str = "sales_by_city",
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Executes a registered analytical aggregation report."""
    if db is None:
        db = get_database()
    if name not in AGGREGATION_REGISTRY:
        raise ValueError(f"Unknown aggregation report '{name}'. Available: {list(AGGREGATION_REGISTRY.keys())}")

    entry = AGGREGATION_REGISTRY[name]
    merged_params = dict(entry["default_params"])
    if params:
        merged_params.update(params)

    return entry["func"](db, **merged_params)
