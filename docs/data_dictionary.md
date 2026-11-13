# Data dictionary

## Source: `transactions_*.csv` (grain: one row = one ledger leg)

| Column | Type (after parsing) | Required | Rules |
|---|---|---|---|
| event_id | string | yes | natural key of the leg; unique across all files |
| transaction_id | string | yes | groups legs; a transaction needs ≥ 2 legs |
| leg_seq | integer ≥ 1 | yes | unique within the transaction |
| account_id | string | yes | must exist in `ref_accounts` (FK) |
| direction | `DEBIT` \| `CREDIT` | yes | case-insensitive on input, stored upper-case |
| amount | decimal(18,2) > 0 | yes | digits with optional ≤ 2 decimals; no sign, no exponent, no thousands separator |
| currency | ISO-4217 code | yes | in `allowed_currencies`; must equal the account currency |
| transaction_type | `PAYMENT` \| `TRANSFER` \| `DEPOSIT` \| `WITHDRAWAL` \| `FEE` | yes | same for all legs of a transaction |
| event_ts | ISO-8601 with timezone | yes | e.g. `2026-11-01T09:00:00Z` or `+01:00`; stored as UTC; same for all legs; not after run start + tolerance; not before account `opened_at` |
| description | string | no | free text, not part of the duplicate hash |

Columns may appear in any order. Unknown extra columns are ignored and logged as `SCHEMA_DRIFT`.
A missing required column fails the whole file (`SCHEMA_MISMATCH`).

## Source: `accounts.csv` (grain: one row = one account)

| Column | Type | Rules |
|---|---|---|
| account_id | string | primary key; duplicates in the file are rejected |
| account_name | string | required |
| account_type | `CUSTOMER` \| `MERCHANT` \| `EXTERNAL` \| `FEE_INCOME` | required |
| currency | ISO-4217 | in `allowed_currencies` |
| opening_balance | decimal(18,2) | may be negative |
| allow_overdraft | `true` \| `false` | if false, a negative running balance is reported as an exception |
| opened_at | ISO-8601 with timezone | events before this time are rejected |

## Target tables (DuckDB)

### `ref_accounts`: reference data, PK `account_id`
Source columns plus `source_file`, `run_id`, `loaded_at`. Upserted on every run (latest wins).

### `curated_entries`: validated legs, PK `event_id`, append-only
| Column | Type | Null | Notes |
|---|---|---|---|
| event_id | VARCHAR | no | PK |
| transaction_id | VARCHAR | no | FK → curated_transactions |
| leg_seq | INTEGER | no | |
| account_id | VARCHAR | no | FK → ref_accounts |
| direction | VARCHAR | no | DEBIT / CREDIT |
| amount | DECIMAL(18,2) | no | always > 0 |
| signed_amount | DECIMAL(18,2) | no | derived: +amount for CREDIT, −amount for DEBIT |
| currency | VARCHAR(3) | no | |
| transaction_type | VARCHAR | no | |
| event_ts | TIMESTAMP | no | UTC |
| description | VARCHAR | yes | |
| payload_hash | VARCHAR | no | derived: hash of business fields; used to tell an exact replay from a conflicting one |
| is_late_arrival | BOOLEAN | no | derived: event_ts earlier than the latest already-curated event of the same account at load time |
| source_file, source_line, file_checksum | VARCHAR / INTEGER / VARCHAR | no | lineage |
| run_id | VARCHAR | no | lineage |
| loaded_at | TIMESTAMP | no | UTC |

### `curated_transactions`: one row per valid transaction, PK `transaction_id`
`transaction_type`, `currency`, `total_amount` (= sum of debits = sum of credits), `leg_count`, `event_ts`,
`source_file`, `run_id`, `loaded_at`. Derived in SQL from the legs loaded in the same DB transaction.

### `quarantine`: every rejected row, PK `quarantine_id`
| Column | Notes |
|---|---|
| quarantine_id | deterministic `q_` + hash(file_checksum, source_line, reason_code, event_id) |
| run_id, source_file, file_checksum, source_line | where the row came from |
| event_id, transaction_id | as given in the source (best effort for malformed rows); nullable |
| reason_code | one of the codes below |
| reason_detail | human-readable explanation, e.g. `debits 50.00 != credits 45.00 (diff 5.00)` |
| raw_record | the original line, for replay / investigation |
| quarantined_at | UTC |

### `pipeline_runs`: one row per CLI run, PK `run_id`
`status` (RUNNING / SUCCESS / PARTIAL_FAILURE / FAILED), `started_at`, `finished_at`, `duration_ms`,
`files_seen/loaded/skipped/failed`, `rows_read/curated/quarantined`, `error_message`.

### `file_loads`: one row per file per run, PK (`run_id`, `source_file`)
`file_checksum`, `status` (SUCCESS / EMPTY / SKIPPED / FAILED), `rows_read`, `rows_curated`, `rows_quarantined`,
`rows_already_loaded`, `transactions_curated`, `late_arrivals`, `read_ms`, `validate_ms`, `load_ms`,
`warnings`, `error_message`. Invariant for SUCCESS: `rows_read = rows_curated + rows_quarantined + rows_already_loaded`.

### Views
| View | Meaning |
|---|---|
| `v_account_balances` | opening balance + net movement = current balance per account |
| `v_running_balances` | running balance per leg in **event-time** order (ties: transaction_id, leg_seq) |
| `v_balance_exceptions` | rows of `v_running_balances` where `allow_overdraft = false` and balance < 0 |

## Reason codes

| Code | Level | Meaning |
|---|---|---|
| MALFORMED_ROW | row | wrong number of fields |
| MISSING_REQUIRED_FIELD | row | a required value is blank |
| INVALID_LEG_SEQ / INVALID_DIRECTION / INVALID_TRANSACTION_TYPE | row | bad enum or integer |
| INVALID_AMOUNT | row | not a positive decimal with ≤ 2 dp |
| INVALID_CURRENCY | row | not an allowed ISO code |
| INVALID_TIMESTAMP | row | not ISO-8601 with timezone, or impossible date |
| FUTURE_TIMESTAMP | row | after run start + tolerance |
| UNKNOWN_ACCOUNT | row | account_id not in reference data |
| CURRENCY_ACCOUNT_MISMATCH | row | leg currency ≠ account currency |
| BEFORE_ACCOUNT_OPEN | row | event before account `opened_at` |
| DUPLICATE_EVENT | row | exact copy of an event earlier in the same file (does not block the transaction) |
| CONFLICTING_DUPLICATE | row | same event_id, different content (in file or vs warehouse) |
| INCOMPLETE_TRANSACTION | txn | another leg of the transaction was rejected |
| SINGLE_LEG | txn | fewer than 2 legs |
| DUPLICATE_LEG_SEQ | txn | leg_seq repeated |
| MIXED_CURRENCY | txn | legs in more than one currency |
| LEG_TIMESTAMP_MISMATCH / LEG_TYPE_MISMATCH | txn | legs disagree on event_ts / transaction_type |
| UNBALANCED | txn | sum(debits) ≠ sum(credits) |
| TRANSACTION_ALREADY_POSTED | txn | new legs for a transaction already curated |
| INVALID_ACCOUNT_RECORD | reference | bad row in accounts.csv |
