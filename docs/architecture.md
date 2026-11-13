# Architecture

## Actors / systems

| Actor | In this project |
|---|---|
| Source system / file producer | CSV files dropped into an input folder (`transactions_*.csv`) plus a reference `accounts.csv` |
| Storage / processing layer | Python package `txn_pipeline` + embedded DuckDB file (`warehouse/pipeline.duckdb`) |
| Target analytical consumer | curated tables, balance views, CSV exports, `reports/pipeline_report.md` |
| Pipeline operator | CLI (`run`, `check`, `report`, `reset`), `pipeline_runs` / `file_loads` tables, `logs/pipeline.log` |
| Data engineer | config in `config/pipeline.toml`, rules in `validators.py`, checks in `sql/reconciliation.sql` |

## Flow

```mermaid
flowchart TD
    A[accounts.csv] -->|validate + upsert| R[(ref_accounts)]
    A -->|bad rows| Q[(quarantine)]

    F[transactions_*.csv<br/>sorted by name] --> RD[reader.py<br/>checksum · header check · malformed rows · line numbers]
    RD -->|checksum already loaded| SK[file_loads: SKIPPED]
    RD -->|missing column / unreadable| FL[file_loads: FAILED<br/>nothing loaded]
    RD -->|header only / 0 bytes| EM[file_loads: EMPTY]
    RD --> V1[row rules<br/>required · types · enums · currency · ISO ts<br/>future ts · account exists · currency = account · after open]
    V1 --> D1[duplicates in file<br/>exact → DUPLICATE_EVENT<br/>different → CONFLICTING_DUPLICATE]
    D1 --> D2[duplicates vs warehouse<br/>same hash → already loaded<br/>different → CONFLICTING_DUPLICATE]
    D2 --> V2[transaction rules<br/>already posted · sibling rejected · ≥2 legs · unique leg_seq<br/>one currency · same ts · same type · debits = credits]
    V2 -->|valid| C[(curated_entries<br/>curated_transactions)]
    V1 & D1 & D2 & V2 -->|reject + reason| Q
    C --> VW[views: v_account_balances · v_running_balances · v_balance_exceptions]
    C & Q --> RC[reconciliation.sql · report · CSV export]

    subgraph TX [one DB transaction per file]
        C
        Q
        OK[file_loads: SUCCESS]
    end
```

## Key design points

1. **Validate in Python, store and reconcile in SQL.** The rules are small pure functions that are easy to unit
   test and give one deterministic reason per row. DuckDB handles bulk storage, the anti-join idempotency and
   all set-based checks (balances, reconciliation).
2. **A transaction is all-or-nothing.** If any leg fails, every leg of that transaction is quarantined, so a
   half transaction can never unbalance the curated ledger.
3. **Atomic per file.** Curated rows, quarantine rows and the SUCCESS registration commit together. A crash
   leaves the file unregistered, so the next run retries it.
4. **Append-only curated layer + derived balances.** Balances are computed in event-time order from the
   entries, so late-arriving data does not need any "update" logic and nothing is overwritten.
5. **Two levels of idempotency.** File checksum skip (cheap), plus natural-key anti-joins on insert (safe
   even under `--force` backfills or overlapping files).
