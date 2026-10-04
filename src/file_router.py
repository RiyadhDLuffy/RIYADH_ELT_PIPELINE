"""Automatic File Router for selecting between Python Batch and PySpark engines."""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import NamedTuple

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import SMALL_FILE_THRESHOLD_MB, THRESHOLD_BYTES

logger = logging.getLogger("file_router")


class RouteDecision(NamedTuple):
    file_path: Path
    size_bytes: int
    size_mb: float
    engine: str  # "python_batch" or "pyspark"
    reason: str


def route_file(
    file_path_str: str | Path,
    threshold_mb: float = SMALL_FILE_THRESHOLD_MB,
    engine_override: str | None = None,
) -> RouteDecision:
    """Inspects file size and determines optimal ELT engine with justification."""
    path = Path(file_path_str)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    size_bytes = path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    if engine_override in ("python_batch", "pyspark", "batch", "spark"):
        engine = "pyspark" if "spark" in engine_override else "python_batch"
        reason = f"Manual override specified: {engine.upper()}"
    elif size_mb <= threshold_mb:
        engine = "python_batch"
        reason = (
            f"File size ({size_mb:.2f} MB) is <= threshold ({threshold_mb:.0f} MB). "
            f"Selected PYTHON_BATCH for zero-overhead streaming and low memory footprint."
        )
    else:
        engine = "pyspark"
        reason = (
            f"File size ({size_mb:.2f} MB) exceeds threshold ({threshold_mb:.0f} MB). "
            f"Selected PYSPARK for multi-core distributed partition processing and parallel execution."
        )

    return RouteDecision(
        file_path=path,
        size_bytes=size_bytes,
        size_mb=round(size_mb, 2),
        engine=engine,
        reason=reason,
    )
