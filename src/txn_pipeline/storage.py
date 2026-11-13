"""DuckDB storage layer: DDL, idempotent inserts and lookup queries.

Layers in the warehouse:
  ref_accounts            reference data (upserted each run)
  curated_entries         validated legs, append-only, PK event_id      (curated output)
  curated_transactions    one row per valid transaction, PK transaction_id
  quarantine              every rejected row + reason + raw text + source location
  pipeline_runs           one row per CLI run (status, counts, timings)
  file_loads              one row per file per run (status, counts, checksum)
Balances are *views* derived from the append-only entries, so late-arriving events
are automatically placed in the right order and nothing is ever overwritten.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

DDL = """
CREATE TABLE IF NOT EXISTS ref_accounts (
    account_id       VARCHAR PRIMARY KEY,
    account_name     VARCHAR NOT NULL,
    account_type     VARCHAR NOT NULL,
    currency         VARCHAR(3) NOT NULL,
    opening_balance  DECIMAL(18,2) NOT NULL,
    allow_overdraft  BOOLEAN NOT NULL,
    opened_at        TIMESTAMP NOT NULL,
    source_file      VARCHAR NOT NULL,
    run_id           VARCHAR NOT NULL,
    loaded_at        TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS curated_entries (
    event_id          VARCHAR PRIMARY KEY,
    transaction_id    VARCHAR NOT NULL,
    leg_seq           INTEGER NOT NULL,
    account_id        VARCHAR NOT NULL,
    direction         VARCHAR NOT NULL,
    amount            DECIMAL(18,2) NOT NULL,
    signed_amount     DECIMAL(18,2) NOT NULL,
    currency          VARCHAR(3) NOT NULL,
    transaction_type  VARCHAR NOT NULL,
    event_ts          TIMESTAMP NOT NULL,
    description       VARCHAR,
    payload_hash      VARCHAR NOT NULL,
    is_late_arrival   BOOLEAN NOT NULL,
    source_file       VARCHAR NOT NULL,
    source_line       INTEGER NOT NULL,
    file_checksum     VARCHAR NOT NULL,
    run_id            VARCHAR NOT NULL,
    loaded_at         TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS curated_transactions (
    transaction_id    VARCHAR PRIMARY KEY,
    transaction_type  VARCHAR NOT NULL,
    currency          VARCHAR(3) NOT NULL,
    total_amount      DECIMAL(18,2) NOT NULL,
    leg_count         INTEGER NOT NULL,
    event_ts          TIMESTAMP NOT NULL,
    source_file       VARCHAR NOT NULL,
    run_id            VARCHAR NOT NULL,
    loaded_at         TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id     VARCHAR PRIMARY KEY,
    run_id            VARCHAR NOT NULL,
    source_file       VARCHAR NOT NULL,
    file_checksum     VARCHAR NOT NULL,
    source_line       INTEGER,
    event_id          VARCHAR,
    transaction_id    VARCHAR,
    reason_code       VARCHAR NOT NULL,
    reason_detail     VARCHAR,
    raw_record        VARCHAR,
    quarantined_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR PRIMARY KEY,
    status            VARCHAR NOT NULL,     -- RUNNING | SUCCESS | PARTIAL_FAILURE | FAILED
    started_at        TIMESTAMP NOT NULL,
    finished_at       TIMESTAMP,
    duration_ms       BIGINT,
    files_seen        INTEGER,
    files_loaded      INTEGER,
    files_skipped     INTEGER,
    files_failed      INTEGER,
    rows_read         BIGINT,
    rows_curated      BIGINT,
    rows_quarantined  BIGINT,
    error_message     VARCHAR
);

CREATE TABLE IF NOT EXISTS file_loads (
    run_id               VARCHAR NOT NULL,
    source_file          VARCHAR NOT NULL,
    file_checksum        VARCHAR NOT NULL,
    status               VARCHAR NOT NULL,  -- SUCCESS | EMPTY | SKIPPED | FAILED
    rows_read            BIGINT NOT NULL,
    rows_curated         BIGINT NOT NULL,
    rows_quarantined     BIGINT NOT NULL,
    rows_already_loaded  BIGINT NOT NULL,
    transactions_curated BIGINT NOT NULL,
    late_arrivals        BIGINT NOT NULL,
    started_at           TIMESTAMP NOT NULL,
    finished_at          TIMESTAMP NOT NULL,
    read_ms              BIGINT,
    validate_ms          BIGINT,
    load_ms              BIGINT,
    warnings             VARCHAR,
    error_message        VARCHAR,
    PRIMARY KEY (run_id, source_file)
);

CREATE OR REPLACE VIEW v_account_balances AS
SELECT a.account_id, a.account_type, a.currency, a.opening_balance,
       COALESCE(SUM(e.signed_amount), 0)                     AS net_movement,
       a.opening_balance + COALESCE(SUM(e.signed_amount), 0) AS current_balance,
       COUNT(e.event_id)                                     AS entry_count,
       MAX(e.event_ts)                                       AS last_event_ts
FROM ref_accounts a
LEFT JOIN curated_entries e ON e.account_id = a.account_id
GROUP BY a.account_id, a.account_type, a.currency, a.opening_balance;

-- running balance in *event-time* order (not arrival order), so late data lands correctly
CREATE OR REPLACE VIEW v_running_balances AS
SELECT e.account_id, e.event_id, e.transaction_id, e.event_ts, e.direction, e.amount,
       a.allow_overdraft,
       a.opening_balance + SUM(e.signed_amount) OVER (
           PARTITION BY e.account_id
           ORDER BY e.event_ts, e.transaction_id, e.leg_seq
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM curated_entries e
JOIN ref_accounts a ON a.account_id = e.account_id;

-- business exception: balance went below zero on an account that does not allow overdraft
CREATE OR REPLACE VIEW v_balance_exceptions AS
SELECT * FROM v_running_balances
WHERE NOT allow_overdraft AND running_balance < 0;
"""


class Storage:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        if str(db_path) != ":memory:":
            db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path))

    def close(self) -> None:
        self.con.close()

    def init_schema(self) -> None:
        self.con.execute(DDL)

    # ------------------------------------------------------------ transactions
    def begin(self) -> None:
        self.con.execute("BEGIN TRANSACTION")

    def commit(self) -> None:
        self.con.execute("COMMIT")

    def rollback(self) -> None:
        self.con.execute("ROLLBACK")

    # ------------------------------------------------------------ runs
    def start_run(self, run_id: str, started_at: datetime) -> None:
        self.con.execute(
            "INSERT INTO pipeline_runs (run_id, status, started_at) VALUES (?, 'RUNNING', ?)",
            [run_id, started_at],
        )

    def finish_run(self, run_id: str, **fields) -> None:
        cols = ", ".join(f"{k} = ?" for k in fields)
        self.con.execute(f"UPDATE pipeline_runs SET {cols} WHERE run_id = ?", [*fields.values(), run_id])

    def record_file_load(self, row: dict) -> None:
        cols = ", ".join(row)
        params = ", ".join("?" for _ in row)
        self.con.execute(f"INSERT INTO file_loads ({cols}) VALUES ({params})", list(row.values()))

    def file_already_loaded(self, checksum: str) -> bool:
        # fetchall (not fetchone): DuckDB 2.x keeps a partially fetched result open
        hits = self.con.execute(
            "SELECT 1 FROM file_loads WHERE file_checksum = ? AND status IN ('SUCCESS', 'EMPTY') LIMIT 1",
            [checksum],
        ).fetchall()
        return bool(hits)

    # ------------------------------------------------------------ reference data
    def upsert_accounts(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        self.con.register("df_accounts", df)
        self.con.execute(
            """
            INSERT OR REPLACE INTO ref_accounts
            SELECT account_id, account_name, account_type, currency,
                   CAST(opening_balance AS DECIMAL(18,2)), allow_overdraft,
                   CAST(opened_at AS TIMESTAMP), source_file, run_id, CAST(loaded_at AS TIMESTAMP)
            FROM df_accounts
            """
        )
        self.con.unregister("df_accounts")

    def fetch_accounts(self) -> list[tuple]:
        return self.con.execute(
            "SELECT account_id, currency, opened_at, allow_overdraft FROM ref_accounts"
        ).fetchall()

    # ------------------------------------------------------------ lookups for idempotency
    def existing_event_hashes(self, event_ids: list[str]) -> dict[str, str]:
        if not event_ids:
            return {}
        self.con.register("df_ids", pd.DataFrame({"event_id": event_ids}))
        rows = self.con.execute(
            "SELECT e.event_id, e.payload_hash FROM curated_entries e JOIN df_ids d USING (event_id)"
        ).fetchall()
        self.con.unregister("df_ids")
        return dict(rows)

    def existing_transaction_ids(self, txn_ids: list[str]) -> set[str]:
        if not txn_ids:
            return set()
        self.con.register("df_txn", pd.DataFrame({"transaction_id": txn_ids}))
        rows = self.con.execute(
            "SELECT t.transaction_id FROM curated_transactions t JOIN df_txn d USING (transaction_id)"
        ).fetchall()
        self.con.unregister("df_txn")
        return {r[0] for r in rows}

    def max_event_ts_by_account(self) -> dict[str, datetime]:
        return dict(
            self.con.execute(
                "SELECT account_id, MAX(event_ts) FROM curated_entries GROUP BY account_id"
            ).fetchall()
        )

    # ------------------------------------------------------------ bulk writes (anti-join = idempotent)
    def insert_entries(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self.con.register("df_entries", df)
        self.con.execute(
            """
            INSERT INTO curated_entries
            SELECT event_id, transaction_id, leg_seq, account_id, direction,
                   CAST(amount AS DECIMAL(18,2)), CAST(signed_amount AS DECIMAL(18,2)),
                   currency, transaction_type, CAST(event_ts AS TIMESTAMP), description,
                   payload_hash, is_late_arrival, source_file, source_line, file_checksum,
                   run_id, CAST(loaded_at AS TIMESTAMP)
            FROM df_entries d
            WHERE NOT EXISTS (SELECT 1 FROM curated_entries c WHERE c.event_id = d.event_id)
            """
        )
        self.con.unregister("df_entries")
        return len(df)

    def insert_transactions_for(self, run_id: str, file_checksum: str, loaded_at: datetime) -> int:
        """Derive transaction headers from the legs just loaded for this file."""
        self.con.execute(
            """
            INSERT INTO curated_transactions
            SELECT transaction_id,
                   ANY_VALUE(transaction_type),
                   ANY_VALUE(currency),
                   SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END),
                   COUNT(*),
                   MIN(event_ts),
                   ANY_VALUE(source_file),
                   ?, ?
            FROM curated_entries e
            WHERE run_id = ? AND file_checksum = ?
              AND NOT EXISTS (SELECT 1 FROM curated_transactions t WHERE t.transaction_id = e.transaction_id)
            GROUP BY transaction_id
            """,
            [run_id, loaded_at, run_id, file_checksum],
        )
        return self.con.execute(
            "SELECT COUNT(*) FROM curated_transactions WHERE run_id = ? AND transaction_id IN "
            "(SELECT transaction_id FROM curated_entries WHERE run_id = ? AND file_checksum = ?)",
            [run_id, run_id, file_checksum],
        ).fetchall()[0][0]

    def insert_quarantine(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self.con.register("df_q", df)
        self.con.execute(
            """
            INSERT INTO quarantine
            SELECT quarantine_id, run_id, source_file, file_checksum, source_line, event_id,
                   transaction_id, reason_code, reason_detail, raw_record,
                   CAST(quarantined_at AS TIMESTAMP)
            FROM df_q d
            WHERE NOT EXISTS (SELECT 1 FROM quarantine q WHERE q.quarantine_id = d.quarantine_id)
            """
        )
        self.con.unregister("df_q")
        return len(df)
