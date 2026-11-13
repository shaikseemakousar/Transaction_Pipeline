"""Reconciliation checks + human-readable run report + CSV export of outputs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

SQL_PATH = Path(__file__).parent / "sql" / "reconciliation.sql"
_CHECK_RE = re.compile(r"--\s*name:\s*(\w+)\s*\|\s*(.+?)\n(.*?);", re.S)


@dataclass
class CheckResult:
    name: str
    description: str
    violations: int

    @property
    def passed(self) -> bool:
        return self.violations == 0


def load_checks(sql_path: Path = SQL_PATH) -> list[tuple[str, str, str]]:
    return [(m.group(1), m.group(2).strip(), m.group(3).strip()) for m in _CHECK_RE.finditer(sql_path.read_text())]


def run_checks(con: duckdb.DuckDBPyConnection) -> list[CheckResult]:
    return [CheckResult(name, desc, int(con.execute(sql).fetchall()[0][0])) for name, desc, sql in load_checks()]


def _md_table(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> str:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if not rows:
        return "_(no rows)_\n"
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in rows:
        out.append("| " + " | ".join("" if v is None else str(v) for v in row) + " |")
    return "\n".join(out) + "\n"


def build_report(db_path: Path) -> tuple[str, list[CheckResult]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        latest = con.execute("SELECT run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT 1").fetchall()
        if not latest:
            return "# Pipeline report\n\nNo runs recorded yet.\n", []
        run_id = latest[0][0]
        checks = run_checks(con)
        generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        parts = [
            "# Pipeline report\n",
            f"Generated: {generated}  \nDatabase: `{db_path.as_posix()}`  \nLatest run: `{run_id}`\n",
            "## Latest run\n",
            _md_table(con, """SELECT status, files_seen, files_loaded, files_skipped, files_failed,
                                     rows_read, rows_curated, rows_quarantined, duration_ms, error_message
                              FROM pipeline_runs WHERE run_id = ?""", [run_id]),
            "## Files in latest run\n",
            _md_table(con, """SELECT source_file, status, rows_read, rows_curated, rows_quarantined,
                                     rows_already_loaded, transactions_curated, late_arrivals,
                                     read_ms, validate_ms, load_ms, warnings, error_message
                              FROM file_loads WHERE run_id = ? ORDER BY source_file""", [run_id]),
            "## Run history\n",
            _md_table(con, """SELECT run_id, status, started_at, files_loaded, files_skipped, files_failed,
                                     rows_read, rows_curated, rows_quarantined, duration_ms
                              FROM pipeline_runs ORDER BY started_at"""),
            "## Warehouse totals\n",
            _md_table(con, """SELECT (SELECT COUNT(*) FROM curated_transactions) AS curated_transactions,
                                     (SELECT COUNT(*) FROM curated_entries)       AS curated_entries,
                                     (SELECT COUNT(*) FROM curated_entries WHERE is_late_arrival) AS late_arrivals,
                                     (SELECT COUNT(*) FROM quarantine)            AS quarantined_rows,
                                     (SELECT COUNT(*) FROM v_balance_exceptions)  AS balance_exceptions"""),
            "## Quarantine by reason (all runs)\n",
            _md_table(con, """SELECT reason_code, COUNT(*) AS rows, COUNT(DISTINCT transaction_id) AS transactions
                              FROM quarantine GROUP BY reason_code ORDER BY rows DESC, reason_code"""),
            "## Curated volume by currency\n",
            _md_table(con, """SELECT currency, COUNT(DISTINCT transaction_id) AS transactions,
                                     SUM(CASE WHEN direction='DEBIT' THEN amount ELSE 0 END) AS total_debits,
                                     SUM(CASE WHEN direction='CREDIT' THEN amount ELSE 0 END) AS total_credits,
                                     SUM(signed_amount) AS net
                              FROM curated_entries GROUP BY currency ORDER BY currency"""),
            "## Balance exceptions (non-overdraft accounts that went negative)\n",
            _md_table(con, """SELECT account_id, COUNT(*) AS breaches, MIN(running_balance) AS lowest_balance,
                                     MIN(event_ts) AS first_breach_ts
                              FROM v_balance_exceptions GROUP BY account_id
                              ORDER BY lowest_balance LIMIT 20"""),
            "## Reconciliation checks\n",
            "| check | description | violations | result |\n|---|---|---|---|\n"
            + "".join(f"| {c.name} | {c.description} | {c.violations} | {'PASS' if c.passed else 'FAIL'} |\n" for c in checks),
        ]
        return "\n".join(parts), checks
    finally:
        con.close()


def export_outputs(db_path: Path, out_dir: Path) -> list[Path]:
    """Write curated/quarantine/balance outputs as CSV (sample output for reviewers)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    queries = {
        "curated_transactions.csv": "SELECT * EXCLUDE (loaded_at, run_id) FROM curated_transactions ORDER BY transaction_id",
        "curated_entries.csv": "SELECT * EXCLUDE (loaded_at, run_id, payload_hash, file_checksum) FROM curated_entries ORDER BY transaction_id, leg_seq",
        "quarantine.csv": "SELECT source_file, source_line, event_id, transaction_id, reason_code, reason_detail, raw_record FROM quarantine ORDER BY source_file, source_line, reason_code",
        "account_balances.csv": "SELECT * FROM v_account_balances ORDER BY account_id",
        "balance_exceptions.csv": "SELECT * FROM v_balance_exceptions ORDER BY account_id, event_ts",
    }
    written = []
    try:
        for name, sql in queries.items():
            path = out_dir / name
            # COPY keeps DECIMAL values exact (pandas would turn them into floats)
            con.execute(f"COPY ({sql}) TO '{path.as_posix()}' (HEADER, DELIMITER ',')")
            written.append(path)
    finally:
        con.close()
    return written
