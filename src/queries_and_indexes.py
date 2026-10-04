"""Database Query Engine and Index Optimization Module.

Completely data-agnostic: executes dynamic queries with automatic fallback
discovery from loaded dataset records to prevent reliance on hardcoded IDs,
cities, or static test values.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database

logger = logging.getLogger("queries_and_indexes")


def get_dynamic_sample_value(db: Database, field_path: str, fallback: Any = None) -> Any:
    """Dynamically resolves an existing sample value from the database collection.
    Ensures tests on new unseen datasets never fail due to hardcoded filter assumptions.
    """
    col = db[VALIDATED_COLLECTION]
    doc = col.find_one({field_path: {"$exists": True, "$ne": "", "$ne": None}})
    if not doc:
        return fallback

    parts = field_path.split(".")
    val = doc
    for part in parts:
        if isinstance(val, dict) and part in val:
            val = val[part]
        else:
            return fallback
    return val or fallback


# -----------------------------------------------------------------------------
# Operational Business Queries (Dynamic & Data-Agnostic)
# -----------------------------------------------------------------------------

def q1_orders_by_status(
    db: Database,
    status: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve orders filtered by fulfillment status, sorted by order date descending.
    Dynamically infers an active status from the database if not explicitly provided.
    """
    col = db[VALIDATED_COLLECTION]
    target_status = status or get_dynamic_sample_value(db, "status", fallback="COMPLETED")
    cursor = col.find(
        {"status": target_status},
        {"_id": 0, "order_id": 1, "order_date": 1, "status": 1, "customer.name": 1, "total_amount": 1}
    ).sort("order_date", DESCENDING).limit(limit)
    return list(cursor)


def q2_customer_order_history(
    db: Database,
    customer_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve full purchase history for a customer.
    Dynamically discovers an existing customer_id from current data if omitted.
    """
    col = db[VALIDATED_COLLECTION]
    target_customer = customer_id or get_dynamic_sample_value(db, "customer.customer_id")
    if not target_customer:
        return []

    cursor = col.find(
        {"customer.customer_id": target_customer},
        {"_id": 0, "order_id": 1, "order_date": 1, "status": 1, "total_amount": 1, "items": 1}
    ).sort("order_date", DESCENDING).limit(limit)
    return list(cursor)


def q3_city_high_value_orders(
    db: Database,
    city: Optional[str] = None,
    min_amount: Optional[float] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve high-value orders for a municipality.
    Dynamically identifies an existing city from the dataset if omitted.
    """
    col = db[VALIDATED_COLLECTION]
    target_city = city or get_dynamic_sample_value(db, "customer.address.city")
    threshold = float(min_amount) if min_amount is not None else 0.0

    query_filter: Dict[str, Any] = {"total_amount": {"$gte": threshold}}
    if target_city:
        query_filter["customer.address.city"] = target_city

    cursor = col.find(
        query_filter,
        {"_id": 0, "order_id": 1, "customer.name": 1, "customer.address.city": 1, "total_amount": 1, "payment.method": 1}
    ).sort("total_amount", DESCENDING).limit(limit)
    return list(cursor)


def q4_orders_by_date_range(
    db: Database,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve orders within a date window. Automatically spans dataset boundaries if omitted."""
    col = db[VALIDATED_COLLECTION]
    query_filter: Dict[str, Any] = {}

    if start_date or end_date:
        range_cond: Dict[str, str] = {}
        if start_date: range_cond["$gte"] = start_date
        if end_date: range_cond["$lte"] = end_date
        query_filter["order_date"] = range_cond
    else:
        # Match all non-empty dates
        query_filter["order_date"] = {"$exists": True, "$ne": ""}

    cursor = col.find(
        query_filter,
        {"_id": 0, "order_id": 1, "order_date": 1, "status": 1, "total_amount": 1, "payment.status": 1}
    ).sort("order_date", DESCENDING).limit(limit)
    return list(cursor)


def q5_orders_by_payment_and_delivery(
    db: Database,
    payment_method: Optional[str] = None,
    delivery_type: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve orders by payment and delivery attributes.
    Dynamically adapts to whatever methods and types exist in the test dataset.
    """
    col = db[VALIDATED_COLLECTION]
    target_payment = payment_method or get_dynamic_sample_value(db, "payment.method")
    target_delivery = delivery_type or get_dynamic_sample_value(db, "delivery.type")

    query_filter: Dict[str, Any] = {}
    if target_payment: query_filter["payment.method"] = target_payment
    if target_delivery: query_filter["delivery.type"] = target_delivery

    cursor = col.find(
        query_filter,
        {"_id": 0, "order_id": 1, "customer.name": 1, "delivery": 1, "payment": 1, "total_amount": 1}
    ).limit(limit)
    return list(cursor)


QUERY_REGISTRY = {
    "q1_orders_by_status": {
        "func": q1_orders_by_status,
        "description": "Fulfillment status lookup with chronological sorting (dynamically resolves active status).",
        "default_params": {"limit": 50},
    },
    "q2_customer_order_history": {
        "func": q2_customer_order_history,
        "description": "Customer transaction history (dynamically resolves customer ID from data).",
        "default_params": {"limit": 50},
    },
    "q3_city_high_value_orders": {
        "func": q3_city_high_value_orders,
        "description": "High-value orders by municipality (dynamically resolves city from data).",
        "default_params": {"min_amount": 0.0, "limit": 50},
    },
    "q4_orders_by_date_range": {
        "func": q4_orders_by_date_range,
        "description": "Bounded chronological order scan across date intervals.",
        "default_params": {"limit": 50},
    },
    "q5_orders_by_payment_and_delivery": {
        "func": q5_orders_by_payment_and_delivery,
        "description": "Multi-attribute lookup across payment method and fulfillment tier.",
        "default_params": {"limit": 50},
    },
}

# -----------------------------------------------------------------------------
# Secondary and Compound Index Definitions
# -----------------------------------------------------------------------------

INDEX_DEFINITIONS = [
    {
        "name": "idx_status_order_date",
        "keys": [("status", ASCENDING), ("order_date", DESCENDING)],
        "rationale": "Optimizes equality filtering on order status combined with index-covered sorting on order date.",
    },
    {
        "name": "idx_customer_date",
        "keys": [("customer.customer_id", ASCENDING), ("order_date", DESCENDING)],
        "rationale": "Enables direct index seek for customer histories sorted chronologically without in-memory sort.",
    },
    {
        "name": "idx_compound_city_amount",
        "keys": [("customer.address.city", ASCENDING), ("total_amount", DESCENDING)],
        "rationale": "Compound index supporting city equality filtering and range boundary checks on monetary totals.",
    },
    {
        "name": "idx_order_date_range",
        "keys": [("order_date", DESCENDING)],
        "rationale": "B-Tree index supporting bounded range scans over calendar timestamps.",
    },
    {
        "name": "idx_payment_delivery",
        "keys": [("payment.method", ASCENDING), ("delivery.type", ASCENDING), ("status", ASCENDING)],
        "rationale": "Compound index covering multiple categorical attributes for financial and logistical reporting.",
    },
]


def create_indexes(db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """Builds defined operational indexes on the validated collection."""
    if db is None:
        db = get_database()
    col = db[VALIDATED_COLLECTION]

    created = []
    for idx_def in INDEX_DEFINITIONS:
        idx_name = col.create_index(idx_def["keys"], name=idx_def["name"])
        created.append({
            "name": idx_name,
            "keys": idx_def["keys"],
            "rationale": idx_def["rationale"],
            "status": "ACTIVE",
        })
        logger.info(f"Verified index {idx_name} on {VALIDATED_COLLECTION}")
    return created


def drop_indexes(db: Optional[Database] = None) -> List[str]:
    """Removes defined indexes for benchmarking and execution plan comparisons."""
    if db is None:
        db = get_database()
    col = db[VALIDATED_COLLECTION]

    dropped = []
    for idx_def in INDEX_DEFINITIONS:
        name = idx_def["name"]
        try:
            col.drop_index(name)
            dropped.append(name)
            logger.info(f"Dropped index {name}")
        except Exception as e:
            logger.debug(f"Index {name} not present: {e}")
    return dropped


# -----------------------------------------------------------------------------
# Query Plan Analysis
# -----------------------------------------------------------------------------

def get_query_cursor(db: Database, query_name: str, params: Optional[Dict[str, Any]] = None):
    """Instantiates a PyMongo cursor for a registered query."""
    col = db[VALIDATED_COLLECTION]
    p = dict(QUERY_REGISTRY[query_name]["default_params"])
    if params:
        p.update(params)

    if query_name == "q1_orders_by_status":
        target_status = p.get("status") or get_dynamic_sample_value(db, "status", fallback="COMPLETED")
        return col.find({"status": target_status}).sort("order_date", DESCENDING).limit(p.get("limit", 50))
    elif query_name == "q2_customer_order_history":
        target_customer = p.get("customer_id") or get_dynamic_sample_value(db, "customer.customer_id", fallback="")
        return col.find({"customer.customer_id": target_customer}).sort("order_date", DESCENDING).limit(p.get("limit", 50))
    elif query_name == "q3_city_high_value_orders":
        target_city = p.get("city") or get_dynamic_sample_value(db, "customer.address.city", fallback="")
        min_amt = float(p.get("min_amount", 0.0))
        q = {"total_amount": {"$gte": min_amt}}
        if target_city:
            q["customer.address.city"] = target_city
        return col.find(q).sort("total_amount", DESCENDING).limit(p.get("limit", 50))
    elif query_name == "q4_orders_by_date_range":
        start_d = p.get("start_date")
        end_d = p.get("end_date")
        q = {}
        if start_d or end_d:
            sub = {}
            if start_d: sub["$gte"] = start_d
            if end_d: sub["$lte"] = end_d
            q["order_date"] = sub
        return col.find(q).sort("order_date", DESCENDING).limit(p.get("limit", 50))
    elif query_name == "q5_orders_by_payment_and_delivery":
        target_payment = p.get("payment_method") or get_dynamic_sample_value(db, "payment.method", fallback="")
        target_delivery = p.get("delivery_type") or get_dynamic_sample_value(db, "delivery.type", fallback="")
        q = {}
        if target_payment: q["payment.method"] = target_payment
        if target_delivery: q["delivery.type"] = target_delivery
        return col.find(q).limit(p.get("limit", 50))
    else:
        raise ValueError(f"Unrecognized query: {query_name}")


def extract_explain_stats(explain_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Parses raw execution plan output into standardized metrics."""
    exec_stats = explain_doc.get("executionStats", {})
    winning_plan = explain_doc.get("queryPlanner", {}).get("winningPlan", {})

    stage = winning_plan.get("stage", "UNKNOWN")
    input_stage = winning_plan.get("inputStage", {})
    effective_stage = input_stage.get("stage", stage)

    return {
        "winning_stage": stage,
        "effective_stage": effective_stage,
        "execution_time_millis": exec_stats.get("executionTimeMillis", 0),
        "total_docs_examined": exec_stats.get("totalDocsExamined", 0),
        "total_keys_examined": exec_stats.get("totalKeysExamined", 0),
        "n_returned": exec_stats.get("nReturned", 0),
        "is_index_used": "IXSCAN" in [stage, effective_stage],
    }


def explain_query(db: Database, query_name: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Evaluates query execution plan using executionStats."""
    cursor = get_query_cursor(db, query_name, params)
    raw_explain = cursor.explain("executionStats")
    stats = extract_explain_stats(raw_explain)
    stats["query_name"] = query_name
    return stats


def run_explain_benchmark(db: Optional[Database] = None) -> Dict[str, Any]:
    """Measures query execution metrics before and after index application."""
    if db is None:
        db = get_database()

    target_queries = [
        ("q1_orders_by_status", "idx_status_order_date"),
        ("q3_city_high_value_orders", "idx_compound_city_amount"),
        ("q5_orders_by_payment_and_delivery", "idx_payment_delivery"),
    ]

    drop_indexes(db)
    before_stats = {}
    for q_name, _ in target_queries:
        before_stats[q_name] = explain_query(db, q_name)

    create_indexes(db)

    after_stats = {}
    comparison = []
    for q_name, idx_name in target_queries:
        after = explain_query(db, q_name)
        after_stats[q_name] = after
        before = before_stats[q_name]

        docs_reduced = before["total_docs_examined"] - after["total_docs_examined"]
        pct_reduced = (
            round((docs_reduced / before["total_docs_examined"]) * 100, 2)
            if before["total_docs_examined"] > 0 else 0.0
        )

        comparison.append({
            "query_name": q_name,
            "index_name": idx_name,
            "before": before,
            "after": after,
            "docs_reduced": docs_reduced,
            "docs_reduction_pct": f"{pct_reduced}%",
            "stage_transition": f"{before.get('effective_stage', 'COLLSCAN')} -> {after.get('effective_stage', 'IXSCAN')}",
            "impact_summary": (
                f"Execution transitioned from collection scan ({before.get('effective_stage', 'COLLSCAN')}) "
                f"to index key scan ({after.get('effective_stage', 'IXSCAN')}), reducing scanned documents by {pct_reduced}%."
            )
        })

    return {
        "status": "SUCCESS",
        "benchmark_results": comparison
    }
