"""Automated Job Scheduler and Audit Logging Service.

Provides background job execution for:
- Periodic materialized view synchronization
- Periodic analytical snapshot generation
- Job lifecycle tracking with execution metrics (duration, status, timestamps)
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pymongo.database import Database

from src.mongo_setup import get_database
from src.materialized_views import refresh_all_materialized_views
from src.aggregations import report_sales_by_city, report_top_products

logger = logging.getLogger("scheduler")

JOB_LOGS_COLLECTION = "job_execution_logs"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# -----------------------------------------------------------------------------
# Background Job Handlers
# -----------------------------------------------------------------------------

def task_refresh_materialized_views(db: Database) -> Dict[str, Any]:
    """Execute scheduled incremental synchronization for all materialized views."""
    return refresh_all_materialized_views(db, full_refresh=False)


def task_generate_periodic_report(db: Database) -> Dict[str, Any]:
    """Compile and archive periodic executive sales snapshot."""
    city_data = report_sales_by_city(db, limit=5)
    top_products = report_top_products(db, limit=5)
    total_revenue_top = sum(p.get("total_revenue", 0.0) for p in top_products)

    snapshot = {
        "report_type": "EXECUTIVE_SALES_SNAPSHOT",
        "top_cities_count": len(city_data),
        "top_products_count": len(top_products),
        "sample_revenue_captured": round(total_revenue_top, 2),
        "generated_at": utc_now(),
    }
    db["periodic_reports_history"].insert_one(dict(snapshot))
    snapshot.pop("_id", None)
    return snapshot


# -----------------------------------------------------------------------------
# Job Registry Definition
# -----------------------------------------------------------------------------

SCHEDULED_JOBS: Dict[str, Dict[str, Any]] = {
    "refresh_materialized_views_job": {
        "func": task_refresh_materialized_views,
        "description": "Incrementally updates materialized view aggregates.",
        "interval_seconds": 600,
        "last_status": "IDLE",
        "last_run": None,
    },
    "generate_periodic_report_job": {
        "func": task_generate_periodic_report,
        "description": "Calculates periodic sales performance metrics.",
        "interval_seconds": 1800,
        "last_status": "IDLE",
        "last_run": None,
    },
}


# -----------------------------------------------------------------------------
# Execution & Audit Logging Engine
# -----------------------------------------------------------------------------

def execute_job_with_logging(
    job_name: str,
    db: Optional[Database] = None,
    trigger_type: str = "MANUAL",
) -> Dict[str, Any]:
    """Runs a task and logs operational metrics to the audit collection."""
    if db is None:
        db = get_database()

    if job_name not in SCHEDULED_JOBS:
        raise ValueError(f"Unknown job '{job_name}'. Registered jobs: {list(SCHEDULED_JOBS.keys())}")

    job_spec = SCHEDULED_JOBS[job_name]
    start_time = utc_now()
    start_timestamp = time.time()
    status = "SUCCESS"
    error_msg = None
    output = None

    try:
        output = job_spec["func"](db)
        job_spec["last_status"] = "SUCCESS"
    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        job_spec["last_status"] = "FAILED"
        logger.error(f"Job execution failed for {job_name}: {e}")

    end_time = utc_now()
    duration_sec = round(time.time() - start_timestamp, 3)
    job_spec["last_run"] = end_time

    log_entry = {
        "job_name": job_name,
        "trigger_type": trigger_type,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration_sec,
        "status": status,
        "error": error_msg,
        "result_summary": output,
    }

    try:
        db[JOB_LOGS_COLLECTION].insert_one(dict(log_entry))
        log_entry.pop("_id", None)
    except Exception as e:
        logger.warning(f"Failed to record job audit log: {e}")

    return log_entry


def run_job_manually(job_name: str, db: Optional[Database] = None) -> Dict[str, Any]:
    """Manually invokes a registered job synchronously."""
    return execute_job_with_logging(job_name, db=db, trigger_type="MANUAL")


def get_all_jobs_status(db: Optional[Database] = None) -> Dict[str, Any]:
    """Retrieves operational status and audit trails for all background tasks."""
    if db is None:
        db = get_database()

    logs_col = db[JOB_LOGS_COLLECTION]
    recent_logs = list(logs_col.find({}, {"_id": 0}).sort("start_time", -1).limit(20))

    jobs_info = []
    for name, spec in SCHEDULED_JOBS.items():
        jobs_info.append({
            "name": name,
            "description": spec["description"],
            "interval_seconds": spec["interval_seconds"],
            "last_status": spec["last_status"],
            "last_run": spec["last_run"],
        })

    return {
        "active_jobs": jobs_info,
        "recent_execution_logs": recent_logs,
        "server_time": utc_now(),
    }


# -----------------------------------------------------------------------------
# Background Scheduler Daemon
# -----------------------------------------------------------------------------

class SimpleBackgroundScheduler:
    """Daemon thread for interval-based background job execution."""

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="JobSchedulerDaemon")
        self._thread.start()
        logger.info("Job scheduler daemon initialized.")

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("Job scheduler daemon stopped.")

    def _run_loop(self):
        db = get_database()
        last_executed: Dict[str, float] = {k: 0.0 for k in SCHEDULED_JOBS}

        while self._running:
            now = time.time()
            for job_name, spec in SCHEDULED_JOBS.items():
                interval = spec["interval_seconds"]
                if (now - last_executed[job_name]) >= interval:
                    last_executed[job_name] = now
                    try:
                        execute_job_with_logging(job_name, db=db, trigger_type="SCHEDULED")
                    except Exception as e:
                        logger.error(f"Error executing scheduled job {job_name}: {e}")
            time.sleep(10)


scheduler_instance = SimpleBackgroundScheduler()
