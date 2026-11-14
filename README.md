# transaction-pipeline



The task was to build a pipeline that takes debit/credit transaction data, checks that it is actually correct
(legs balance, currencies make sense, events are in a sensible order) and makes sure bad records never end up
in the final "curated" output. I kept it small and local on purpose. It runs on a laptop with Python and DuckDB,
with no servers or cloud accounts needed.

## The idea in one paragraph

Every row in the input is one **leg** of a transaction (one debit or one credit on one account). A transaction is
two or more legs with the same `transaction_id`, and it is only valid if total debits = total credits. The
pipeline reads the CSV files, checks every row, then checks every transaction as a whole. Good transactions go
to the curated tables. Anything wrong goes to a `quarantine` table with a reason code, the original line, and the
file and line number it came from, so it can always be traced back. Every run is logged with counts, timings and
a status.

```
input CSVs -> read & check header -> row checks -> duplicate checks -> transaction checks
                                                                          |
                         +------------------------------------------------+
                         v                                                v
              quarantine (with reason)                        curated_entries / curated_transactions
                                                                          |
                                                                          v
                                                       balance views + reconciliation checks
```

## How to run it

You need Python 3.11 or newer.

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1          # on Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
$env:PYTHONPATH = "src"             # on Mac/Linux: export PYTHONPATH=src
```

Run everything from the repo root.

**Sample data** (small files I made by hand, where each row tests one rule):

```bash
python -m txn_pipeline reset
python -m txn_pipeline run
python -m txn_pipeline run          # second run: everything is skipped because it was already loaded
python -m txn_pipeline report --export-dir data/sample/output
python -m txn_pipeline check
```

The first run should show 49 rows read: 17 curated and 30 quarantined. The third file is empty on purpose.
The report goes to `reports/pipeline_report.md` and the output tables are exported to `data/sample/output/`.
What I expect to happen to every sample transaction is written down in
`data/sample/expected/expected_outcomes.csv`, and one of the tests checks the pipeline against that file.

To see what happens when a file has the wrong columns:

```bash
python -m txn_pipeline run --input-dir data/sample/bad_schema --db warehouse/demo_bad.duckdb
```

**Bigger data** (generated, 100k+ rows):

```bash
python -m txn_pipeline generate --rows 100000 --seed 42 --out data/generated
python -m txn_pipeline run --input-dir data/generated/incoming --accounts data/generated/accounts.csv --db warehouse/generated.duckdb
python -m txn_pipeline check --db warehouse/generated.duckdb
```

The generator always produces the same data for the same seed. Around 5% of transactions get one deliberate
problem. Some arrive late, some are sent twice, and the rows inside each file are shuffled. The "right answers"
are saved in a separate labels file, not mixed into the input.

**Tests:**

```bash
python -m pytest
```

There are 63 tests and they take about 10 seconds.

## What gets checked

Row level: required fields, leg_seq, direction, transaction type, amount (positive, max 2 decimals), currency
code, ISO-8601 timestamp with timezone, no future dates, account must exist, currency must match the account,
event not before the account was opened.

Transaction level: at least 2 legs, no repeated leg_seq, one currency, all legs at the same time and of the same
type, and debits = credits.

Duplicates:
- An exact copy of a row in the same file is quarantined, but the transaction still goes through.
- The same `event_id` with different content is treated as a conflict and is never allowed to overwrite what's
  already loaded.
- A replay of something already loaded is just counted and skipped.

If even one leg of a transaction fails, I reject the whole transaction. A half-loaded transaction would make the
ledger unbalanced, which is worse than holding the whole thing back.

All the reason codes and table columns are listed in [docs/data_dictionary.md](docs/data_dictionary.md).

## Reruns, failures and late data

These were the parts I spent the most thought on:

- **Running twice is safe.** Each file's checksum is stored after a successful load, so the same file is skipped
  next time. Inserts also check the natural keys, so even `--force` (reprocess everything) can't create duplicates.
- **A crash can't leave half a file behind.** Curated rows, quarantine rows and the "file loaded" record are
  written in one database transaction. If something fails, it all rolls back, and the next run simply retries
  that file. The tests simulate this with a fake crash.
- **One bad file doesn't stop the others.** The run is marked `PARTIAL_FAILURE` and only the failed file is
  retried next time.
- **Late data.** Balances are calculated from the entries in event-time order, not in the order the files
  arrived. A late event lands in the right place automatically, and it is also flagged with `is_late_arrival`.
- **Negative balances** on accounts that don't allow overdraft are flagged in `v_balance_exceptions` rather than
  rejected. The rows themselves are valid, and whether the balance dips below zero can change when late data
  shows up.

## How I checked it works

- The 63 tests cover the cases the brief asked for: happy path, empty input, malformed rows, duplicates, nulls,
  schema change, partial failure and retry, invalid account reference, and rerunning the same input. They also
  cover a few banking-specific ones (unbalanced by one cent, mixed currency, late arrivals, an extra leg for a
  transaction that was already posted).
- The pipeline's decision for every transaction matches the expected answers, both for the sample files and for
  the generated data.
- `python -m txn_pipeline check` runs 11 SQL reconciliation checks, for example: every curated transaction
  balances, total movements per currency net to zero, every row read is accounted for. All of them pass on the
  sample data and on 100k generated rows.
- I worked out the sample account balances by hand and compared them with the output (for example ACC-1001 ends
  at 1068.50, and ACC-1002 goes to -65.25 and shows up as a balance exception).

## Performance

On my laptop, 100,359 rows (48,621 transactions, 3 files) take about **5.3 seconds** end to end: 0.8 s reading,
1.8 s validating, 1.6 s loading.

The main optimisation is loading rows into DuckDB in bulk from a DataFrame instead of inserting them one at a
time. I measured both with `scripts/benchmark.py`. For 20k rows, row-by-row took about 30 seconds and bulk took
0.04 seconds. If it ever needed to be faster, I would validate files in parallel next, since validation is now
the slowest step.

## Assumptions and limitations

- All legs of a transaction come in the same file. If they are split across files, the transaction is rejected.
- One currency per transaction. There is no FX.
- A credit increases an account's balance and a debit decreases it. External settlement and fee accounts are
  allowed to go negative.
- Account reference data is overwritten with the latest version. I don't keep history for it.
- It is a single-process tool on a local DuckDB file. It isn't built for several runs at the same time.
- There is no corrections/reversals flow yet. A fix has to come in as a new balancing transaction.

The longer version, with the trade-offs I made, is in [docs/design_decisions.md](docs/design_decisions.md).

## Project layout

```
config/pipeline.toml       paths and settings
src/txn_pipeline/
  schemas.py               columns, allowed values, reason codes
  reader.py                reads CSVs, checksum, header check, bad rows
  validators.py            all the row and transaction rules
  pipeline.py              runs everything, duplicates, one DB transaction per file
  storage.py               DuckDB tables and views
  reports.py               reconciliation, report, CSV export
  sql/reconciliation.sql   the reconciliation queries
  generator.py             test data generator
  cli.py                   command line
tests/                     unit and end-to-end tests
scripts/benchmark.py       performance test
data/sample/               hand-made input, expected results, sample output
docs/                      architecture, data dictionary, design decisions, build steps
Aii/                        prompt history and AI usage notes
```



## Use of AI

I used an AI coding assistant during this project, as the challenge allows. My prompts and notes on how I used it
are in [Aii/RROMPT_RESPONSE_HISTORY.md](Aii/PROMPT_RESPONSE_HISTORY.md)

