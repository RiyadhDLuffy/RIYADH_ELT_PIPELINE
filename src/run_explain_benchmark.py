"""Execution Plan and Index Performance Benchmark Utility.

Executes baseline versus indexed explain("executionStats") comparisons
and outputs benchmark metrics to console and reports directory.
"""
import sys
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mongo_setup import get_database
from src.queries_and_indexes import run_explain_benchmark

def main():
    print("=" * 76)
    print("  DATABASE INDEXING AND EXECUTION PLAN BENCHMARK")
    print("=" * 76)

    try:
        db = get_database()
        benchmark = run_explain_benchmark(db)
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        return

    results = benchmark.get("benchmark_results", [])
    print(f"\nEvaluating {len(results)} representative operational queries:\n")

    for i, res in enumerate(results, 1):
        print(f"[{i}] Query: {res['query_name']}")
        print(f"    Index:              {res['index_name']}")
        print(f"    Stage Transition:   {res['stage_transition']}")
        print(f"    Documents Scanned:  Before = {res['before']['total_docs_examined']} | After = {res['after']['total_docs_examined']}")
        print(f"    Reduction:          {res['docs_reduced']} ({res['docs_reduction_pct']})")
        print(f"    Execution Latency:  Before = {res['before']['execution_time_millis']}ms | After = {res['after']['execution_time_millis']}ms")
        print(f"    Summary:            {res['impact_summary']}\n")

    out_file = PROJECT_ROOT / "reports" / "explain_benchmark_results.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(benchmark, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Benchmark results successfully exported to: {out_file}")

if __name__ == "__main__":
    main()
