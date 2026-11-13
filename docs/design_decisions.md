# Design decisions, trade-offs and limitations

## Assumptions (where the brief left room)

1. **Input format.** Leg-level CSV files land in a folder. CSV was chosen because it is the most common
   producer format and the one where malformed input actually happens (a typed format like Parquet hides that
   whole class of error).
2. **Double-entry model.** A transaction has ≥ 2 legs and is valid only if `sum(debits) = sum(credits)`.
   A "transfer leg" is just a leg of a TRANSFER transaction. A transfer with a fee is a 3-leg transaction.
3. **Sign convention.** CREDIT increases the account's balance and DEBIT decreases it. `EXTERNAL` settlement and
   `FEE_INCOME` accounts mirror the outside world and are allowed to go negative.
4. **Event ordering.** `event_ts` is the business time. All legs of a transaction must share it, it must
   not be in the future (5-minute tolerance for clock skew), and it must not precede the account's `opened_at`.
   Rows inside a file may be in any order. Late events (earlier than data already loaded) are accepted and flagged.
5. **Complete transactions per file.** The producer sends all legs of a transaction together.
6. **No FX.** A transaction uses one currency, and each leg must match its account currency.
7. **Reference data is trusted after validation.** Bad account rows are quarantined. Accounts are upserted.

## Key decisions and trade-offs

| Decision | Why | Trade-off |
|---|---|---|
| **DuckDB** embedded warehouse | real SQL, ACID transactions, primary keys, window functions, zero setup, free, fast on 100k+ rows | single writer; not a shared server DB. Swapping in Postgres would mostly mean changing `storage.py` |
| **Python row validation** (pure functions) | very readable, unit-testable, one deterministic reason per row, exact `Decimal` maths | slower than vectorised SQL (validation is ~35% of runtime); fine at this scale |
| **Bulk DataFrame inserts** | measured ~800× faster than row-by-row inserts | needs pandas as a dependency |
| **Reject the whole transaction if one leg fails** | the curated ledger must always balance; a partial transaction is worse than none | one bad leg holds back the good legs (all are in quarantine with the cause) |
| **Exact duplicate in a file does not block the transaction** | it is a harmless repeated copy; the original copy is valid | if the copy was actually meant as a second, different event, the producer must give it a new event_id |
| **Conflicting duplicate never overwrites** | the brief asks to preserve history instead of silently overwriting | corrections need an explicit mechanism (e.g. reversal transactions; see next steps) |
| **Balance breach = flag, not reject** | the leg itself is valid; whether a balance is negative depends on which other events have arrived yet (late data can change it) | consumers must look at `v_balance_exceptions`; a stricter "reject overdrafts" policy is easy to add but order-dependent |
| **Atomic per file, continue on failure** (`stop_on_error=false`) | one bad file should not block independent good files; balances are event-time views so a later retry still lands correctly | the curated layer can be temporarily "missing" a failed file; the run status `PARTIAL_FAILURE` and non-zero exit code make that visible |
| **Checksum-based skip + anti-join inserts** | cheap skip for the common rerun; anti-join makes `--force`, overlapping files and backfills safe | a re-exported file with different bytes but identical rows is re-read (and then counted as already loaded), costing a little time |
| **Fixed reason-check order** | each row gets exactly one reason, so reporting is stable | a row with several problems only shows the first |

## Retry, restart, backfill

See README §6. In short: rerunning is always safe, failed files are retried automatically, and `--force`
reprocesses everything without creating duplicates.

## How I know it works

* **63 automated tests** cover every edge case required by the brief, plus domain cases (see README §4).
* **Labelled data:** both the hand-made sample and the generated dataset carry expected outcomes in a separate
  file. Tests assert the pipeline's curated/quarantined decision for **every** transaction matches them
  (4,000-row generated set in the test suite; also checked manually at 100k rows).
* **Reconciliation:** 11 SQL checks run after every load (`check` / `report`), all returning 0 violations on
  the sample and on 100k generated rows. They cover balanced transactions, trial balance per currency = 0,
  referential integrity, header/detail agreement, and row accounting per file.
* **Hand-calculated balances** for the sample data match `v_account_balances` (e.g. ACC-1001 = 1068.50,
  ACC-1002 = −65.25 with one balance exception after the late-arriving transfer).

## Known limitations

* Transactions split across files are rejected rather than held until complete.
* No FX, no reversals/corrections workflow. Corrections must come as new balancing transactions.
* Reference-data changes are not versioned (no SCD2 on accounts).
* Single-process. Concurrent runs against the same DuckDB file are not supported (DuckDB locks the file).
* Quarantined rows are not automatically replayed after the data is fixed. The producer resends a corrected file.
* A run killed hard (e.g. power loss) stays `RUNNING` in `pipeline_runs`. Data is still consistent because of the
  per-file transaction.

## Next steps (if this went to production)

1. A "pending legs" area to hold incomplete transactions until they complete or time out.
2. Explicit `REVERSAL` transaction type that references the original, for corrections.
3. SCD2 history for `ref_accounts`.
4. Parallel file validation (multiprocessing) or SQL-based row rules for higher volume.
5. Schedule with an orchestrator (cron/Airflow/Dagster) and alert on `PARTIAL_FAILURE`/`FAILED` or any failed
   reconciliation check.
