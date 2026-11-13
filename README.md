# Transaction Validation Pipeline

A small, local data-engineering pipeline that ingests ledger **legs** (debit/credit entries),
validates them row by row and transaction by transaction, and publishes only trustworthy data
to a **curated** layer. Everything else goes to a **quarantine** table with a reason code and a
pointer back to the exact source file and line. Every run is recorded with counts, timings and
status. Reconciliation queries then prove the curated output is correct.

**Stack:** Python 3.11+, DuckDB (embedded, file-based warehouse), pandas (bulk loading), pytest.
No server, no cloud account, no credentials.

```
CSV files ──► read (checksum, schema check) ──► row rules ──► duplicate rules ──► transaction rules
                                                                                   │
                     ┌─────────────────────────────────────────────────────────────┤
                     ▼                                                             ▼
            quarantine (reason, source_file, line, raw)            curated_entries / curated_transactions
                                                                   └─► views: balances, running balances,
                                                                              balance exceptions
            pipeline_runs + file_loads (status, counts, timings) ◄── one DB transaction per file
```

More detail: [docs/architecture.md](docs/architecture.md) · [docs/data_dictionary.md](docs/data_dictionary.md) ·
[docs/design_decisions.md](docs/design_decisions.md) · [docs/BUILD_STEPS.md](docs/BUILD_STEPS.md)

---

## 1. Setup

```bash
python -m venv .venv
# Windows (PowerShell):   .venv\Scripts\Activate.ps1
# macOS / Linux:          source .venv/bin/activate
pip install -r requirements.txt
```

All commands are run **from the repository root**. The package lives in `src/`, so either set
`PYTHONPATH=src` or use the commands exactly as shown:

```bash
# Windows PowerShell
$env:PYTHONPATH = "src"
# macOS / Linux
export PYTHONPATH=src
```

## 2. Run on the sample data (≈1 minute)

```bash
python -m txn_pipeline reset                     # start from an empty warehouse
python -m txn_pipeline run                       # load data/sample/incoming/*.csv
python -m txn_pipeline run                       # run again -> every file SKIPPED (idempotent)
python -m txn_pipeline report --export-dir data/sample/output
python -m txn_pipeline check                     # reconciliation checks only, exit 0 = all pass
```

Expected result of the first run:

```
status=SUCCESS files=3 rows_read=49 curated=17 quarantined=30
  transactions_001.csv        SUCCESS  read=39 curated=13 quarantined=26 already_loaded=0 late=0
  transactions_002.csv        SUCCESS  read=10 curated=4  quarantined=4  already_loaded=2 late=2
  transactions_003_empty.csv  EMPTY    read=0
```

* `reports/pipeline_report.md`: run summary, quarantine by reason, balance exceptions, reconciliation results
* `data/sample/output/*.csv`: curated transactions/entries, quarantine, balances (committed as sample output)
* `data/sample/expected/expected_outcomes.csv`: the expected decision for every sample transaction,
  kept separate from the input files and checked by `tests/test_pipeline.py::test_sample_dataset_matches_expected_outcomes`

Schema-change demo (file missing the `currency` column, so the whole file is rejected and nothing is loaded):

```bash
python -m txn_pipeline run --input-dir data/sample/bad_schema --db warehouse/demo_bad.duckdb
```

## 3. Run at scale (100k+ rows)

```bash
python -m txn_pipeline generate --rows 100000 --seed 42 --files 3 --out data/generated
python -m txn_pipeline run --input-dir data/generated/incoming --accounts data/generated/accounts.csv --db warehouse/generated.duckdb
python -m txn_pipeline check --db warehouse/generated.duckdb
python scripts/benchmark.py --rows 100000
```

The generator is deterministic (fixed seed). About 5% of transactions carry one labelled anomaly, about 3% arrive
late, about 1% are replayed in a later file, and rows are shuffled inside files (out-of-order input).
Labels are written to `data/generated/expected/expected_outcomes.csv`, never into the input files.

## 4. Tests

```bash
python -m pytest            # 63 tests, ~10 s
```

| Area | Tests |
|---|---|
| Happy path | `test_happy_path_loads_balanced_transactions` |
| Empty input | header-only file, zero-byte file, empty folder |
| Malformed records | wrong field count → `MALFORMED_ROW`, sibling legs blocked |
| Nulls / missing fields | blank amount, blank event_id, blank account |
| Duplicates | exact copy in file, conflicting copy in file, replay in later file, changed event after load |
| Schema change | missing column → file FAILED; extra and re-ordered columns tolerated |
| Partial failure & retry | crash before commit → nothing written → retry succeeds; one bad file → PARTIAL_FAILURE → only it is retried |
| Rerun same input | checksum skip; `--force` backfill creates no duplicates |
| Invalid reference key | `UNKNOWN_ACCOUNT`, invalid reference rows quarantined |
| **Domain-specific** | unbalanced to the cent, mixed currency, late arrival + event-time running balance, extra leg for posted transaction, future / before-account-open events |
| Generator | same seed gives identical bytes; pipeline decisions match the independent labels on 4k rows |
| Unit | every field parser and row/transaction rule in `validators.py` |

## 5. What the pipeline guarantees

| Guarantee | How |
|---|---|
| Invalid data never reaches curated | every leg passes row rules **and** its whole transaction passes transaction rules; one bad leg blocks the transaction (`INCOMPLETE_TRANSACTION`) |
| Every curated transaction balances | `check_transaction` enforces `sum(debits) = sum(credits)` with `Decimal` (no float drift), and reconciliation re-checks it in SQL |
| Safe reruns | file checksum registry (`file_loads`), natural-key anti-joins on insert, deterministic `quarantine_id` |
| No half-loaded files | curated rows, quarantine rows and the `file_loads` SUCCESS record are committed in one DB transaction |
| Traceability | every curated and quarantined row carries `source_file`, `source_line`, `file_checksum`, `run_id`; quarantine keeps the raw line |
| History is not overwritten | curated tables are append-only; a changed copy of a loaded event is quarantined (`CONFLICTING_DUPLICATE`), not applied |
| Late data handled | balances are views computed in **event-time** order, so late events slot into the right place; `is_late_arrival` flags them |
| Measurable | `pipeline_runs` and `file_loads` hold status, row counts, rejects, timings and errors; 11 reconciliation checks |

## 6. Retry, restart and backfill

* **Retry after a failure:** just run again. A failed file was rolled back and never registered, so it is
  reprocessed. Successfully loaded files are skipped by checksum.
* **Restart after a crash mid-run:** same as retry. The interrupted run stays visible as `RUNNING` in
  `pipeline_runs` for the operator.
* **Backfill / reprocess:** drop old files into an input folder and run with `--force`. Already-loaded events are
  counted as `rows_already_loaded` and not duplicated. New events load normally, and late ones are flagged.
* **Full rebuild:** `python -m txn_pipeline reset`, then run. Outputs are fully derived from the input files.

## 7. Performance (measured on a laptop, `scripts/benchmark.py --rows 100000`)

| Measure | Result |
|---|---|
| 100,359 leg rows / 48,621 transactions / 3 files | **5.3 s end-to-end (~19k rows/s)** |
| Stage split | read 0.8 s · validate 1.8 s · load 1.6 s |
| Optimisation applied: bulk DataFrame insert vs row-by-row `executemany` (20k rows) | 0.04 s vs 30.6 s (**~800× faster**) |

Next optimisation (not needed at this scale): validation is the largest stage and is single-threaded Python.
Files are independent until load, so they could be validated in parallel (multiprocessing). Alternatively the
row rules could be moved into vectorised DuckDB SQL.

## 8. Repository layout

```
config/pipeline.toml            default paths & validation settings
src/txn_pipeline/
  schemas.py                    source schemas, allowed values, reason codes
  reader.py                     CSV ingestion: checksum, header check, malformed rows, line numbers
  validators.py                 pure validation functions (row + transaction rules)
  pipeline.py                   orchestration, duplicate handling, atomic per-file load, run metrics
  storage.py                    DuckDB DDL, views, idempotent bulk inserts
  reports.py                    reconciliation runner, markdown report, CSV export
  sql/reconciliation.sql        reconciliation checks (also runnable in the DuckDB CLI)
  generator.py                  reproducible synthetic data + separate labels
  cli.py                        run | report | check | generate | reset
tests/                          unit + end-to-end tests
scripts/benchmark.py            performance measurement
data/sample/                    small hand-made input, expected outcomes, sample output
docs/                           architecture, data dictionary, design decisions, build steps
ai/                             prompt history and AI-usage disclosure
```

## 9. Naming conventions

* `snake_case` for all tables and columns. Layer prefixes are `ref_`, `curated_` and `v_` (views).
* IDs are stable and deterministic. `event_id` and `transaction_id` are source natural keys.
  `quarantine_id` = hash(file checksum, line, reason, event_id), so reprocessing yields the same id.
  `run_id` = `run_<UTC timestamp>`.
* Timestamps are ISO-8601 with a timezone on input, and are stored as UTC `TIMESTAMP`.
* `run_id`, `source_file`, `source_line` and `file_checksum` appear on every curated and quarantined row.

## 10. Assumptions & limitations (summary; full list in [docs/design_decisions.md](docs/design_decisions.md))

* All legs of a transaction arrive in the **same file**. A transaction split across files is quarantined as
  `SINGLE_LEG`/`UNBALANCED` and has to be resent together.
* One currency per transaction (no FX). Amounts are positive with at most 2 decimals.
* CREDIT increases an account's balance and DEBIT decreases it. External and fee accounts may go negative.
* A negative balance on a non-overdraft account is **flagged** (`v_balance_exceptions`), not rejected. It describes
  the account state, and with late data it depends on arrival order.
* Reference accounts are upserted (latest wins). Account history is not versioned.
* Single-process, local DuckDB file. It is not designed for concurrent writers.

## 11. AI usage

This project was built with an AI coding assistant. See [ai/prompt_history.md](ai/prompt_history.md) and
[ai/ai_usage.md](ai/ai_usage.md).
