"""Unified REST API Service for Data Operations and Pipeline Orchestration.

Exposes RESTful endpoints for:
- Health and database connectivity verification
- Pipeline batch ingestion triggering (supports arbitrary input file paths)
- Database indexing and query plan benchmarking
- Parameterized operational queries (dynamic and data-agnostic)
- Analytical aggregation reports
- Materialized view synchronization
- Background task status and manual invocation
"""
from __future__ import annotations

import os
import sys
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query, Body
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import MONGO_URI, DB_NAME, VALIDATED_COLLECTION
from src.mongo_setup import get_database
from src.elt_pipeline import run_pipeline
from src.queries_and_indexes import (
    QUERY_REGISTRY,
    INDEX_DEFINITIONS,
    create_indexes,
    explain_query,
    run_explain_benchmark,
)
from src.aggregations import AGGREGATION_REGISTRY, run_aggregation
from src.materialized_views import (
    refresh_all_materialized_views,
    get_materialized_view_data,
)
from src.scheduler import (
    SCHEDULED_JOBS,
    run_job_manually,
    get_all_jobs_status,
    scheduler_instance,
)

logger = logging.getLogger("api")

app = FastAPI(
    title="Enterprise ELT Data Platform API",
    description="Unified REST API interface for data ingestion, analytics, materialized views, and scheduled job management.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


@app.on_event("startup")
def startup_event():
    """Initializes background schedulers on service start."""
    try:
        scheduler_instance.start()
    except Exception as e:
        logger.warning(f"Scheduler initialization warning: {e}")


@app.on_event("shutdown")
def shutdown_event():
    """Terminates background worker threads gracefully."""
    scheduler_instance.stop()


# -----------------------------------------------------------------------------
# System & Health Check Endpoints
# -----------------------------------------------------------------------------

@app.get("/health", tags=["System"])
def health_check():
    """Returns database connection status and collection document volumes."""
    db_status = "DISCONNECTED"
    collections_count = 0
    validated_records = 0

    try:
        db = get_database()
        db.command("ping")
        db_status = "CONNECTED"
        collections_count = len(db.list_collection_names())
        validated_records = db[VALIDATED_COLLECTION].count_documents({})
    except Exception as e:
        db_status = f"ERROR: {str(e)}"

    return {
        "status": "UP",
        "service": "Enterprise ELT Data Platform API",
        "database": {
            "name": DB_NAME,
            "status": db_status,
            "collections_count": collections_count,
            "validated_orders_count": validated_records,
        },
    }


# -----------------------------------------------------------------------------
# Data Ingestion Gateway
# -----------------------------------------------------------------------------

class IngestRequest(BaseModel):
    csv_path: Optional[str] = Field(None, description="Path to any source CSV file. Fully dynamic and data-agnostic.")
    engine_override: Optional[str] = Field(None, description="Execution engine override: 'python_batch' or 'pyspark'.")
    reset_db: bool = Field(False, description="Reset target collections prior to execution.")


@app.post("/ingest", tags=["Ingestion"])
def trigger_ingest(
    payload: Optional[IngestRequest] = Body(None),
    csv_path: Optional[str] = Query(None, description="Optional path to target CSV file"),
):
    """Triggers dataset ingestion through automated routing, quality rules, and database upserts.
    Accepts any file path passed via body or query parameter without relying on fixed filenames.
    """
    payload = payload or IngestRequest()
    target_csv = (payload.csv_path if payload and payload.csv_path else None) or csv_path

    if not target_csv:
        # Search for any CSV file present in data/ or root directory
        data_dir = PROJECT_ROOT / "data"
        found = list(data_dir.glob("*.csv")) or list(PROJECT_ROOT.glob("*.csv"))
        if found:
            target_csv = str(found[0])
        else:
            raise HTTPException(status_code=400, detail="No source CSV file provided and none located in data/ directory.")

    try:
        result = run_pipeline(
            csv_path=target_csv,
            engine_override=payload.engine_override,
            reset_db=payload.reset_db,
        )
        return {"status": "SUCCESS", "ingestion_result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


# -----------------------------------------------------------------------------
# Indexing & Query Plan Optimization
# -----------------------------------------------------------------------------

@app.post("/indexes", tags=["Indexing & Optimization"])
def build_indexes():
    """Applies defined operational compound and single-key indexes."""
    try:
        db = get_database()
        created = create_indexes(db)
        return {
            "status": "SUCCESS",
            "message": f"Successfully validated and built {len(created)} indexes.",
            "indexes": created,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/indexes/benchmark", tags=["Indexing & Optimization"])
def benchmark_indexes():
    """Runs executionStats plan comparisons for representative queries."""
    try:
        db = get_database()
        benchmark = run_explain_benchmark(db)
        return benchmark
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Operational Business Queries
# -----------------------------------------------------------------------------

@app.get("/queries", tags=["Queries"])
def list_queries():
    """Lists registered operational queries with parameters and descriptions."""
    res = {}
    for name, item in QUERY_REGISTRY.items():
        res[name] = {
            "description": item["description"],
            "default_params": item["default_params"],
        }
    return {"count": len(res), "queries": res}


@app.get("/queries/{name}", tags=["Queries"])
def execute_query(
    name: str,
    explain: bool = Query(False, description="Return execution plan (executionStats) instead of data rows"),
    limit: int = Query(50, description="Maximum documents to return"),
    status: Optional[str] = Query(None, description="Fulfillment status filter"),
    customer_id: Optional[str] = Query(None, description="Customer identifier filter"),
    city: Optional[str] = Query(None, description="Municipality filter"),
    min_amount: Optional[float] = Query(None, description="Minimum order total filter"),
    start_date: Optional[str] = Query(None, description="Start date filter (inclusive)"),
    end_date: Optional[str] = Query(None, description="End date filter (inclusive)"),
    payment_method: Optional[str] = Query(None, description="Payment method filter"),
    delivery_type: Optional[str] = Query(None, description="Delivery type filter"),
):
    """Executes a registered operational query or evaluates its execution plan."""
    if name not in QUERY_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Query '{name}' not found. Available queries: {list(QUERY_REGISTRY.keys())}")

    db = get_database()
    params: Dict[str, Any] = {"limit": limit}
    if status is not None: params["status"] = status
    if customer_id is not None: params["customer_id"] = customer_id
    if city is not None: params["city"] = city
    if min_amount is not None: params["min_amount"] = min_amount
    if start_date is not None: params["start_date"] = start_date
    if end_date is not None: params["end_date"] = end_date
    if payment_method is not None: params["payment_method"] = payment_method
    if delivery_type is not None: params["delivery_type"] = delivery_type

    try:
        if explain:
            stats = explain_query(db, name, params)
            return {"query": name, "explain_stats": stats}
        else:
            q_func = QUERY_REGISTRY[name]["func"]
            merged = dict(QUERY_REGISTRY[name]["default_params"])
            merged.update(params)
            rows = q_func(db, **merged)
            return {"query": name, "count": len(rows), "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Analytics & Aggregations
# -----------------------------------------------------------------------------

@app.get("/aggregations", tags=["Analytics"])
def list_aggregations():
    """Lists registered business aggregation pipelines."""
    res = {}
    for name, item in AGGREGATION_REGISTRY.items():
        res[name] = {
            "description": item["description"],
            "default_params": item["default_params"],
        }
    return {"count": len(res), "reports": res}


@app.get("/aggregations/{name}", tags=["Analytics"])
def execute_aggregation(
    name: str,
    limit: Optional[int] = Query(None, description="Result limit override"),
):
    """Executes a registered aggregation pipeline and returns computed metrics."""
    if name not in AGGREGATION_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Aggregation report '{name}' not found. Available: {list(AGGREGATION_REGISTRY.keys())}")

    params: Dict[str, Any] = {}
    if limit is not None:
        params["limit"] = limit

    try:
        db = get_database()
        data = run_aggregation(db, name, params)
        return {"report": name, "count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Materialized Views
# -----------------------------------------------------------------------------

@app.post("/refresh-mv", tags=["Materialized Views"])
def refresh_materialized_views_endpoint(full_refresh: bool = Query(False, description="Perform full rebuild instead of incremental delta synchronization")):
    """Synchronizes materialized view collections using high-watermark checkpoints."""
    try:
        db = get_database()
        result = refresh_all_materialized_views(db, full_refresh=full_refresh)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/materialized-views/{name}", tags=["Materialized Views"])
def view_materialized_data(
    name: str,
    limit: int = Query(50, description="Maximum documents to retrieve"),
):
    """Retrieves cached summary records from a materialized view collection."""
    valid_names = ["daily_sales_summary", "top_products_summary"]
    if name not in valid_names:
        raise HTTPException(status_code=404, detail=f"Invalid view '{name}'. Options: {valid_names}")

    try:
        db = get_database()
        data = get_materialized_view_data(db, view_name=name, limit=limit)
        return {"view_name": name, "count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Background Task Scheduling & Monitoring
# -----------------------------------------------------------------------------

@app.get("/jobs", tags=["Scheduled Tasks"])
def get_jobs():
    """Retrieves background task schedules and recent execution audit logs."""
    try:
        db = get_database()
        return get_all_jobs_status(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/jobs/{name}/run", tags=["Scheduled Tasks"])
def trigger_job_manually(name: str):
    """Triggers on-demand execution of a background task and records execution metrics."""
    if name not in SCHEDULED_JOBS:
        raise HTTPException(status_code=404, detail=f"Task '{name}' not found. Available: {list(SCHEDULED_JOBS.keys())}")

    try:
        db = get_database()
        res = run_job_manually(name, db=db)
        return {"status": "EXECUTED", "log": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
