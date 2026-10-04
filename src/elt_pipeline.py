"""Main ELT Pipeline Orchestrator and Execution Controller."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path
from typing import Any

# Force UTF-8 for Windows console output
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    SMALL_FILE_THRESHOLD_MB,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    AUDIT_COLLECTION,
)
from src.mongo_setup import get_database, setup_collections_and_indexes, reset_database
from src.file_router import route_file
from src.batch_loader import process_python_batch
from src.spark_loader import process_pyspark_batch
from src.metrics import record_pipeline_metrics
from src.quality_rules import utc_now

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
logger = logging.getLogger("elt_pipeline")


def run_pipeline(
    csv_path: str | Path,
    engine_override: str | None = None,
    threshold_mb: float = SMALL_FILE_THRESHOLD_MB,
    reset_db: bool = False,
) -> dict[str, Any]:
    """Orchestrates the end-to-end ELT execution."""
    file_path = Path(csv_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Source CSV file not found: {file_path}")

    # 1. MongoDB Setup
    db = get_database()
    if reset_db:
        print("[SETUP] Resetting MongoDB collections for fresh benchmark...")
        reset_database(db)
    else:
        setup_collections_and_indexes(db)

    # 2. File Routing Decision
    route = route_file(file_path, threshold_mb=threshold_mb, engine_override=engine_override)
    run_id = uuid.uuid4().hex[:8]

    print("\n" + "=" * 70)
    print(f"[RUN] ELT PIPELINE RUN: {run_id}")
    print(f"[FILE] Target File    : {route.file_path.name} ({route.size_mb:.2f} MB)")
    print(f"[ENGINE] Selected Engine: {route.engine.upper()}")
    print(f"[ROUTER] Decision Reason: {route.reason}")
    print("=" * 70 + "\n")

    # 3. Execution based on engine (with automatic fallback)
    if route.engine == "pyspark":
        try:
            run_stats = process_pyspark_batch(route.file_path, db, run_id)
        except Exception as spark_err:
            print(f"\n{'!' * 70}")
            print(f"[FALLBACK] PySpark engine failed: {type(spark_err).__name__}")
            print(f"[FALLBACK] Automatically retrying with PYTHON_BATCH engine...")
            print(f"[FALLBACK] (To avoid this, install Java 11+ or use --engine python_batch)")
            print(f"{'!' * 70}\n")
            logger.warning("PySpark failed (%s), falling back to python_batch", spark_err)
            # Reset DB state since PySpark may have partially written
            reset_database(db)
            run_id = uuid.uuid4().hex[:8]  # fresh run_id for the retry
            route = route._replace(engine="python_batch")
            print(f"[RUN] RETRY ELT PIPELINE RUN: {run_id} (Engine: PYTHON_BATCH)\n")
            run_stats = process_python_batch(route.file_path, db, run_id)
    else:
        run_stats = process_python_batch(route.file_path, db, run_id)

    # 4. Final Aggregated Metrics
    metrics: dict[str, Any] = {
        "run_id": run_id,
        "timestamp": utc_now(),
        "file_name": route.file_path.name,
        "file_path": str(route.file_path.resolve()),
        "file_size_mb": route.size_mb,
        "routing_reason": route.reason,
        **run_stats,
    }

    # 5. Record to MongoDB Audit collection & generate reports/results.json
    record_pipeline_metrics(db, metrics)

    # 6. Stylized Console Summary matching official project output format
    err_counts = metrics.get("error_case_counts", {})
    sorted_errors = sorted(err_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    print("\n" + "=" * 80)
    print(" Pipeline Run Summary | بيانات معالجة خط الأنابيب")
    print("=" * 80)
    print(f" Run ID           : {run_id}")
    print(f" Engine Used      : {metrics['engine_used']}")
    print(f" File Source      : {metrics['file_path']}")
    print(f" Timestamp        : {metrics['timestamp']}")
    print("-" * 80)
    print(f" Total Raw Records : {metrics['raw_loaded']:,}")
    print(f"   - Valid Records : {metrics['valid_count']:,}")
    print(f"   - Corrected     : {metrics['corrected_count']:,}")
    print(f"   - Quarantined   : {metrics['quarantine_count']:,}")
    print(f" Consistency Check : {'PASSED' if metrics['is_consistent'] else 'FAILED'}")
    print("-" * 80)
    print(f" Elapsed Time     : {metrics['elapsed_seconds']:.2f} s")
    print(f" Throughput       : {metrics['throughput']:,.2f} records/s")
    if "mem_start_mb" in metrics and "mem_end_mb" in metrics:
        print(f" Memory Start     : {metrics['mem_start_mb']:.2f} MB")
        print(f" Memory End       : {metrics['mem_end_mb']:.2f} MB")
        delta_sign = "+" if metrics['mem_delta_mb'] >= 0 else ""
        print(f" Memory Delta     : {delta_sign}{metrics['mem_delta_mb']:.2f} MB")
    print("-" * 80)
    print(" Top Quarantine Errors:")
    if sorted_errors:
        for err_code, count in sorted_errors:
            print(f"   * {err_code}: {count:,}")
    else:
        print("   (No quarantine errors encountered)")
    print("=" * 80 + "\n")

    return metrics


def main():
    parser = argparse.ArgumentParser(description="Adaptive ELT Pipeline (Python Batch & PySpark)")
    parser.add_argument("csv_path", nargs="?", default="orders_huge_mixed_quality.csv", help="Input CSV path")
    parser.add_argument("--engine", choices=["auto", "python_batch", "pyspark"], default="auto", help="Engine choice")
    parser.add_argument("--threshold", type=float, default=SMALL_FILE_THRESHOLD_MB, help="Threshold in MB for routing")
    parser.add_argument("--reset-db", action="store_true", help="Drop collections before running")
    parser.add_argument("--visualize", action="store_true", help="Generate charts after pipeline completes")
    args = parser.parse_args()

    engine_override = None if args.engine == "auto" else args.engine
    run_pipeline(args.csv_path, engine_override=engine_override, threshold_mb=args.threshold, reset_db=args.reset_db)

    if args.visualize:
        try:
            from src.visualize_results import main as run_visualizer
            print("\n[VISUALIZE] Generating charts from pipeline results...")
            run_visualizer()
        except ImportError:
            print("\n[VISUALIZE] visualize_results module not installed/available; skipping.")


if __name__ == "__main__":
    main()
