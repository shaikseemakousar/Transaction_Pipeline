"""Performance measurement.

  python scripts/benchmark.py --rows 100000

1. Generates N synthetic leg rows (fixed seed), runs the full pipeline on a fresh
   warehouse and reports wall time, throughput and per-stage timings (read/validate/load).
2. Measures the optimisation used in the load step: bulk insert through a registered
   DataFrame vs. row-by-row parameterised inserts (executemany), on the same rows.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

from txn_pipeline.config import PipelineConfig  # noqa: E402
from txn_pipeline.generator import generate  # noqa: E402
from txn_pipeline.pipeline import Pipeline  # noqa: E402


def end_to_end(rows: int, workdir: Path) -> None:
    t = time.perf_counter()
    stats = generate(workdir / "data", n_rows=rows, seed=42, n_files=3)
    gen_s = time.perf_counter() - t

    cfg = PipelineConfig(input_dir=workdir / "data" / "incoming", database=workdir / "bench.duckdb",
                         accounts_file=workdir / "data" / "accounts.csv")
    t = time.perf_counter()
    summary = Pipeline(cfg).run()
    run_s = time.perf_counter() - t

    con = duckdb.connect(str(cfg.database), read_only=True)
    stages = con.execute("SELECT SUM(read_ms), SUM(validate_ms), SUM(load_ms) FROM file_loads").fetchall()[0]
    con.close()

    print(f"\n== End-to-end: {stats['rows']:,} leg rows / {stats['transactions']:,} transactions in {stats['files']} files")
    print(f"generate      : {gen_s:6.2f} s")
    print(f"pipeline run  : {run_s:6.2f} s  ({stats['rows'] / run_s:,.0f} rows/s)  status={summary.status}")
    print(f"  read        : {stages[0] / 1000:6.2f} s")
    print(f"  validate    : {stages[1] / 1000:6.2f} s")
    print(f"  load        : {stages[2] / 1000:6.2f} s")
    print(f"curated rows  : {summary.rows_curated:,}   quarantined rows: {summary.rows_quarantined:,}")


def insert_comparison(rows: int) -> None:
    df = pd.DataFrame({
        "event_id": [f"EVT-{i:08d}" for i in range(rows)],
        "account_id": [f"CUS-{i % 500:05d}" for i in range(rows)],
        "amount": [f"{(i % 50000) / 100 + 1:.2f}" for i in range(rows)],
        "event_ts": ["2026-11-01T10:00:00"] * rows,
    })
    ddl = "CREATE TABLE t (event_id VARCHAR PRIMARY KEY, account_id VARCHAR, amount DECIMAL(18,2), event_ts TIMESTAMP)"

    con = duckdb.connect()
    con.execute(ddl)
    t = time.perf_counter()
    con.executemany("INSERT INTO t VALUES (?, ?, CAST(? AS DECIMAL(18,2)), CAST(? AS TIMESTAMP))",
                    df.values.tolist())
    row_s = time.perf_counter() - t
    con.close()

    con = duckdb.connect()
    con.execute(ddl)
    t = time.perf_counter()
    con.register("df", df)
    con.execute("INSERT INTO t SELECT event_id, account_id, CAST(amount AS DECIMAL(18,2)), CAST(event_ts AS TIMESTAMP) FROM df")
    bulk_s = time.perf_counter() - t
    con.close()

    print(f"\n== Load optimisation: inserting {rows:,} rows into DuckDB")
    print(f"row-by-row executemany : {row_s:7.3f} s  ({rows / row_s:,.0f} rows/s)")
    print(f"bulk DataFrame insert  : {bulk_s:7.3f} s  ({rows / bulk_s:,.0f} rows/s)")
    print(f"speed-up               : {row_s / bulk_s:,.0f}x")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--insert-rows", type=int, default=20_000,
                        help="rows for the insert comparison (row-by-row is slow)")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        end_to_end(args.rows, Path(tmp))
    insert_comparison(args.insert_rows)


if __name__ == "__main__":
    main()
