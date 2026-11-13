# Pipeline report

Generated: 2026-11-13T20:00:07Z  
Database: `warehouse/pipeline.duckdb`  
Latest run: `run_20261113T200007060178Z`

## Latest run

| status | files_seen | files_loaded | files_skipped | files_failed | rows_read | rows_curated | rows_quarantined | duration_ms | error_message |
|---|---|---|---|---|---|---|---|---|---|
| SUCCESS | 3 | 0 | 3 | 0 | 0 | 0 | 0 | 168 |  |

## Files in latest run

| source_file | status | rows_read | rows_curated | rows_quarantined | rows_already_loaded | transactions_curated | late_arrivals | read_ms | validate_ms | load_ms | warnings | error_message |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| transactions_001.csv | SKIPPED | 39 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | identical file content already loaded (checksum match) |  |
| transactions_002.csv | SKIPPED | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | identical file content already loaded (checksum match) |  |
| transactions_003_empty.csv | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | identical file content already loaded (checksum match) |  |

## Run history

| run_id | status | started_at | files_loaded | files_skipped | files_failed | rows_read | rows_curated | rows_quarantined | duration_ms |
|---|---|---|---|---|---|---|---|---|---|
| run_20261113T200005049692Z | SUCCESS | 2026-11-13 20:00:05.232546 | 3 | 0 | 0 | 49 | 17 | 30 | 456 |
| run_20261113T200007060178Z | SUCCESS | 2026-11-13 20:00:07.141221 | 0 | 3 | 0 | 0 | 0 | 0 | 168 |

## Warehouse totals

| curated_transactions | curated_entries | late_arrivals | quarantined_rows | balance_exceptions |
|---|---|---|---|---|
| 8 | 17 | 2 | 31 | 1 |

## Quarantine by reason (all runs)

| reason_code | rows | transactions |
|---|---|---|
| INCOMPLETE_TRANSACTION | 9 | 9 |
| FUTURE_TIMESTAMP | 2 | 1 |
| LEG_TIMESTAMP_MISMATCH | 2 | 1 |
| MIXED_CURRENCY | 2 | 1 |
| UNBALANCED | 2 | 1 |
| BEFORE_ACCOUNT_OPEN | 1 | 1 |
| CONFLICTING_DUPLICATE | 1 | 1 |
| CURRENCY_ACCOUNT_MISMATCH | 1 | 1 |
| DUPLICATE_EVENT | 1 | 1 |
| INVALID_ACCOUNT_RECORD | 1 | 0 |
| INVALID_AMOUNT | 1 | 1 |
| INVALID_CURRENCY | 1 | 1 |
| INVALID_DIRECTION | 1 | 1 |
| INVALID_TIMESTAMP | 1 | 1 |
| MALFORMED_ROW | 1 | 1 |
| MISSING_REQUIRED_FIELD | 1 | 1 |
| SINGLE_LEG | 1 | 1 |
| TRANSACTION_ALREADY_POSTED | 1 | 1 |
| UNKNOWN_ACCOUNT | 1 | 1 |

## Curated volume by currency

| currency | transactions | total_debits | total_credits | net |
|---|---|---|---|---|
| EUR | 3 | 400.00 | 400.00 | 0.00 |
| USD | 5 | 776.75 | 776.75 | 0.00 |

## Balance exceptions (non-overdraft accounts that went negative)

| account_id | breaches | lowest_balance | first_breach_ts |
|---|---|---|---|
| ACC-1002 | 1 | -65.25 | 2026-11-01 14:00:00 |

## Reconciliation checks

| check | description | violations | result |
|---|---|---|---|
| unbalanced_curated_transactions | every curated transaction has sum(debits) = sum(credits) | 0 | PASS |
| trial_balance_not_zero | per currency, all signed movements net to zero (double-entry holds globally) | 0 | PASS |
| single_leg_curated_transactions | no curated transaction has fewer than 2 legs | 0 | PASS |
| orphan_entries | every curated leg references a known account (referential integrity) | 0 | PASS |
| currency_mismatch | every curated leg uses its account's currency | 0 | PASS |
| header_detail_mismatch | transaction headers agree with their legs (leg count and total) | 0 | PASS |
| entries_without_header | every curated leg has a transaction header | 0 | PASS |
| file_row_accounting | for each loaded file: rows_read = curated + quarantined + already_loaded | 0 | PASS |
| curated_rows_match_file_loads | curated row count equals the sum reported by successful file loads | 0 | PASS |
| non_positive_amounts | no curated leg has amount <= 0 | 0 | PASS |
| quarantine_without_reason | every quarantined row has a reason and a source location | 0 | PASS |
