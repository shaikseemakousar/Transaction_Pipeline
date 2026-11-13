# Pipeline report

Generated: 2026-11-13T19:57:34Z  
Database: `warehouse/generated.duckdb`  
Latest run: `run_20261113T195727270943Z`

## Latest run

| status | files_seen | files_loaded | files_skipped | files_failed | rows_read | rows_curated | rows_quarantined | duration_ms | error_message |
|---|---|---|---|---|---|---|---|---|---|
| SUCCESS | 3 | 3 | 0 | 0 | 100359 | 94683 | 5317 | 5567 |  |

## Files in latest run

| source_file | status | rows_read | rows_curated | rows_quarantined | rows_already_loaded | transactions_curated | late_arrivals | read_ms | validate_ms | load_ms | warnings | error_message |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| transactions_001.csv | SUCCESS | 32281 | 30549 | 1732 | 0 | 14922 | 0 | 225 | 626 | 534 |  |  |
| transactions_002.csv | SUCCESS | 33678 | 31538 | 1781 | 359 | 15394 | 0 | 281 | 654 | 544 |  |  |
| transactions_003.csv | SUCCESS | 34400 | 32596 | 1804 | 0 | 15872 | 991 | 267 | 534 | 771 |  |  |

## Run history

| run_id | status | started_at | files_loaded | files_skipped | files_failed | rows_read | rows_curated | rows_quarantined | duration_ms |
|---|---|---|---|---|---|---|---|---|---|
| run_20261113T195727270943Z | SUCCESS | 2026-11-13 19:57:27.364360 | 3 | 0 | 0 | 100359 | 94683 | 5317 | 5567 |

## Warehouse totals

| curated_transactions | curated_entries | late_arrivals | quarantined_rows | balance_exceptions |
|---|---|---|---|---|
| 46188 | 94683 | 991 | 5317 | 8047 |

## Quarantine by reason (all runs)

| reason_code | rows | transactions |
|---|---|---|
| INCOMPLETE_TRANSACTION | 1721 | 1628 |
| DUPLICATE_EVENT | 507 | 507 |
| UNBALANCED | 439 | 212 |
| LEG_TIMESTAMP_MISMATCH | 431 | 206 |
| FUTURE_TIMESTAMP | 393 | 189 |
| INVALID_AMOUNT | 236 | 236 |
| INVALID_DIRECTION | 218 | 218 |
| MALFORMED_ROW | 211 | 211 |
| CURRENCY_ACCOUNT_MISMATCH | 210 | 210 |
| SINGLE_LEG | 198 | 198 |
| INVALID_TIMESTAMP | 194 | 194 |
| INVALID_CURRENCY | 191 | 191 |
| UNKNOWN_ACCOUNT | 187 | 187 |
| MISSING_REQUIRED_FIELD | 181 | 181 |

## Curated volume by currency

| currency | transactions | total_debits | total_credits | net |
|---|---|---|---|---|
| EUR | 11612 | 2923400.47 | 2923400.47 | 0.00 |
| GBP | 11540 | 2892225.30 | 2892225.30 | 0.00 |
| INR | 11545 | 2874419.31 | 2874419.31 | 0.00 |
| USD | 11491 | 2892822.23 | 2892822.23 | 0.00 |

## Balance exceptions (non-overdraft accounts that went negative)

| account_id | breaches | lowest_balance | first_breach_ts |
|---|---|---|---|
| CUS-00404 | 109 | -7850.34 | 2026-03-19 06:35:30 |
| CUS-00463 | 168 | -6951.00 | 2026-01-01 13:46:27 |
| CUS-00185 | 147 | -6863.91 | 2026-01-03 18:04:53 |
| CUS-00211 | 149 | -6752.33 | 2026-01-17 22:57:00 |
| CUS-00218 | 107 | -6047.80 | 2026-03-13 06:31:58 |
| CUS-00034 | 142 | -5823.50 | 2026-01-30 14:54:03 |
| CUS-00348 | 155 | -5066.39 | 2026-01-01 20:59:06 |
| CUS-00437 | 132 | -4946.38 | 2026-02-03 11:20:31 |
| CUS-00461 | 99 | -4903.84 | 2026-03-31 06:09:26 |
| CUS-00425 | 86 | -4890.06 | 2026-04-13 03:13:17 |
| MER-00430 | 126 | -4800.26 | 2026-01-28 16:10:19 |
| CUS-00493 | 60 | -4519.44 | 2026-01-02 17:12:23 |
| CUS-00448 | 108 | -4512.89 | 2026-02-13 07:41:16 |
| CUS-00478 | 100 | -4510.90 | 2026-04-03 02:15:59 |
| CUS-00326 | 126 | -4497.31 | 2026-01-18 09:59:32 |
| CUS-00254 | 77 | -4486.63 | 2026-03-19 20:23:09 |
| CUS-00203 | 152 | -4434.23 | 2026-01-03 06:44:42 |
| CUS-00331 | 126 | -4415.91 | 2026-02-19 11:44:56 |
| CUS-00049 | 77 | -4410.22 | 2026-03-28 11:46:30 |
| CUS-00181 | 97 | -4323.46 | 2026-03-16 08:08:19 |

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
