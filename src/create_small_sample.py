"""Sample generator for creating a small, reproducible CSV subset for testing."""
from __future__ import annotations
import argparse
import csv
import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DATA_DIR


def extract_sample(
    input_path: str | Path,
    output_path: str | Path,
    max_rows: int = 100_000,
) -> int:
    """Streams input CSV and creates a smaller sample CSV."""
    inp = Path(input_path)
    out = Path(output_path)

    if not inp.exists():
        raise FileNotFoundError(f"Input CSV not found: {inp}")

    out.parent.mkdir(parents=True, exist_ok=True)
    start_time = time.perf_counter()

    count = 0
    with open(inp, mode="r", encoding="utf-8-sig", errors="replace") as f_in:
        reader = csv.reader(f_in)
        header = next(reader, None)
        if header is None:
            print("Empty input CSV.")
            return 0

        with open(out, mode="w", encoding="utf-8", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)

            for row in reader:
                writer.writerow(row)
                count += 1
                if count >= max_rows:
                    break

    elapsed = time.perf_counter() - start_time
    size_mb = out.stat().st_size / (1024 * 1024)
    print(f"[Sample Extracted] {count:,} rows written to {out.name} ({size_mb:.2f} MB) in {elapsed:.2f}s")
    return count


def main():
    parser = argparse.ArgumentParser(description="Create small test sample from large orders CSV")
    parser.add_argument("--input", default="orders_huge_mixed_quality.csv", help="Input CSV path")
    parser.add_argument("--rows", type=int, default=100_000, help="Number of rows to extract")
    parser.add_argument("--output", default=str(DATA_DIR / "sample_orders_small.csv"), help="Output sample CSV path")
    args = parser.parse_args()

    extract_sample(args.input, args.output, args.rows)


if __name__ == "__main__":
    main()
