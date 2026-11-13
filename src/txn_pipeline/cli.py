"""Command line interface.

  python -m txn_pipeline run      [--input-dir D] [--accounts F] [--db F] [--force]
  python -m txn_pipeline report   [--db F] [--out F] [--export-dir D]
  python -m txn_pipeline check    [--db F]
  python -m txn_pipeline generate --rows N [--seed S] [--files K] [--out D]
  python -m txn_pipeline reset    [--db F]

Exit codes: 0 success, 1 failure, 2 partial failure / failed checks.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from txn_pipeline.config import DEFAULT_CONFIG_PATH, load_config


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")):
        handler.setFormatter(fmt)
        root.addHandler(handler)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="txn_pipeline", description="Transaction validation pipeline")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="TOML config file")
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="ingest, validate and load all input files")
    run.add_argument("--input-dir")
    run.add_argument("--accounts", dest="accounts_file")
    run.add_argument("--db", dest="database")
    run.add_argument("--force", action="store_true",
                     help="re-process files even if their checksum was already loaded (backfill)")

    rep = sub.add_parser("report", help="write a markdown report of the latest run + reconciliation")
    rep.add_argument("--db", dest="database")
    rep.add_argument("--out", type=Path, help="report path (default: <reports_dir>/pipeline_report.md)")
    rep.add_argument("--export-dir", type=Path, help="also export curated/quarantine outputs as CSV")

    chk = sub.add_parser("check", help="run reconciliation checks only")
    chk.add_argument("--db", dest="database")

    gen = sub.add_parser("generate", help="generate a synthetic dataset with labelled anomalies")
    gen.add_argument("--rows", type=int, default=100_000, help="approximate number of leg rows")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--files", type=int, default=3)
    gen.add_argument("--invalid-rate", type=float, default=0.05)
    gen.add_argument("--out", type=Path, default=Path("data/generated"))

    rst = sub.add_parser("reset", help="delete the warehouse database")
    rst.add_argument("--db", dest="database")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    overrides = {k: getattr(args, k, None) for k in ("input_dir", "accounts_file", "database")}
    cfg = load_config(args.config, **overrides)
    setup_logging(cfg.log_dir)

    if args.command == "run":
        from txn_pipeline.pipeline import Pipeline

        summary = Pipeline(cfg).run(force=args.force)
        print(f"\nrun_id={summary.run_id} status={summary.status} files={len(summary.files)} "
              f"rows_read={summary.rows_read} curated={summary.rows_curated} "
              f"quarantined={summary.rows_quarantined} duration_ms={summary.duration_ms}")
        for f in summary.files:
            print(f"  {f.source_file:<40} {f.status:<8} read={f.rows_read} curated={f.rows_curated} "
                  f"quarantined={f.rows_quarantined} already_loaded={f.rows_already_loaded} "
                  f"late={f.late_arrivals}" + (f"  error={f.error_message}" if f.error_message else ""))
        return {"SUCCESS": 0, "PARTIAL_FAILURE": 2}.get(summary.status, 1)

    if args.command == "report":
        from txn_pipeline.reports import build_report, export_outputs

        text, checks = build_report(cfg.database)
        out = args.out or cfg.reports_dir / "pipeline_report.md"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        print(f"report written to {out}")
        if args.export_dir:
            for path in export_outputs(cfg.database, args.export_dir):
                print(f"exported {path}")
        failed = [c for c in checks if not c.passed]
        print(f"reconciliation: {len(checks) - len(failed)}/{len(checks)} checks passed")
        return 2 if failed else 0

    if args.command == "check":
        import duckdb

        from txn_pipeline.reports import run_checks

        con = duckdb.connect(str(cfg.database), read_only=True)
        checks = run_checks(con)
        con.close()
        for c in checks:
            print(f"{'PASS' if c.passed else 'FAIL'}  {c.name:<34} violations={c.violations}")
        return 0 if all(c.passed for c in checks) else 2

    if args.command == "generate":
        from txn_pipeline.generator import generate

        stats = generate(args.out, n_rows=args.rows, seed=args.seed, n_files=args.files,
                         invalid_rate=args.invalid_rate)
        print(f"generated {stats['rows']} leg rows / {stats['transactions']} transactions "
              f"in {stats['files']} files under {args.out} (labels: {stats['labels_file']})")
        return 0

    if args.command == "reset":
        for path in (cfg.database, Path(str(cfg.database) + ".wal")):
            if path.exists():
                path.unlink()
                print(f"deleted {path}")
        return 0
    return 1
