# Prompt and response history

Ten prompts, in order. Each entry has the prompt verbatim and the reply as shown in chat. Tool activity (writing files, running tests and benchmarks) is left out, because the code is in the deliverable files named under each turn. Automatic system notices are not prompts and are omitted.

---

## Turn 1

**Prompt**

> Help me define source and target schemas, keys, and assumptions for a transaction pipeline (debit/credit legs, balances, currency, event ordering).

**Response**

# Transaction Pipeline: Schema Design

## 1. Source schemas (raw, append-only)

**`src_txn_event`** (one row per event, including legs)
| Column | Type | Notes |
|---|---|---|
| event_id | string | Unique per event; idempotency key |
| txn_id | string | Groups legs of one transaction |
| leg_seq | int | 1..n within txn |
| source_system | string | Part of the natural key |
| account_id | string | |
| direction | enum(`D`,`C`) | Debit/credit |
| amount_minor | bigint | Integer minor units; always ≥ 0 |
| currency | char(3) | ISO 4217 |
| event_type | enum | `POSTED`, `REVERSED`, `ADJUSTED` |
| event_ts | timestamp (UTC) | When it happened |
| ingest_ts | timestamp (UTC) | When we received it |
| account_seq | bigint | Per-account sequence, if source provides it |
| reverses_txn_id | string null | Set on reversals |

**`src_account`**: account_id, account_type (asset/liability/…), base_currency, open_ts, close_ts, status
**`src_fx_rate`**: rate_date, from_ccy, to_ccy, rate (decimal 18,8), rate_source

## 2. Target schemas (star + balance snapshot)

**`fact_ledger_entry`** (grain: one row per leg)
- PK: `(source_system, txn_id, leg_seq)`
- Surrogate: `entry_key`
- FKs: `account_key`, `currency_key`, `posting_date_key`
- Measures: `signed_amount_minor` (+debit/−credit, or per account-type convention), `amount_base_minor`, `fx_rate_applied`
- Lineage: `event_id`, `ingest_ts`, `is_reversal`, `reverses_entry_key`

**`fact_transaction`** (grain: one row per txn)
- PK: `(source_system, txn_id)`
- Columns: leg_count, total_debit_minor, total_credit_minor, is_balanced, status, first_event_ts, last_event_ts

**`fact_account_balance_daily`** (grain: account × currency × date)
- PK: `(account_key, currency_key, balance_date)`
- Columns: opening_minor, debits_minor, credits_minor, closing_minor, entry_count

**Dimensions:** `dim_account` (SCD2: effective_from/to, is_current), `dim_currency` (ISO code, minor_unit_exponent), `dim_date`

## 3. Keys and idempotency
- **Dedup key:** `event_id`. Keep the latest `ingest_ts` on a conflict.
- **Business key:** `(source_system, txn_id, leg_seq)`.
- **Merge strategy:** upsert on the business key, never blind insert.
- **Reversals:** insert a new offsetting txn linked via `reverses_txn_id`. Never update or delete originals.

## 4. Event ordering
- **Order within an account:** `account_seq` if available, else `(event_ts, ingest_ts, event_id)` as a deterministic tie-break.
- **Balance** = cumulative sum of signed amounts in that order, per (account, currency).
- **Late arrivals:** accept events up to N days late (assume N = 3). Recompute balances from the earliest affected date forward.
- **Watermark:** process by `ingest_ts`, and aggregate by `event_ts` / posting date.

## 5. Assumptions to confirm
1. **Double-entry:** Σdebits = Σcredits per txn, per currency.
2. **Money:** stored as integer minor units, with no floats. Exponent comes from `dim_currency` (JPY=0, USD=2, KWD=3).
3. **Currency:** an account holds balances per currency, or has a single base currency. Decide which. Multi-currency txns balance per currency, or via base-currency conversion.
4. **FX:** the rate is applied by `posting_date` (end-of-day) from one named source. Missing rates fall back to the last available, flagged.
5. **Time:** all timestamps are UTC, and posting date is derived in a fixed business timezone.
6. **Immutability:** source events are append-only, and corrections arrive as new events.
7. **Status lifecycle:** `PENDING → POSTED → REVERSED`. Only `POSTED` affects balances.
8. **Sign convention:** debit increases assets and decreases liabilities. Document one rule per account type.

## 6. Data quality checks (track as metrics)
| Check | Target |
|---|---|
| Unbalanced txns (Σd ≠ Σc) | 0 |
| Duplicate `event_id` after dedup | 0 |
| Orphan legs (no header or account match) | 0 |
| Legs per txn < 2 | 0 |
| Late-arriving events (> N days) | < 0.1% |
| Missing FX rate on posting date | 0 |
| Balance recon: closing = opening + Σ(entries) | 100% of accounts |
| Source-to-target row count match | 100% |

## 7. Open questions to settle first
Do you have a source-provided per-account sequence number? That decides whether ordering is deterministic or best-effort. Also, are balances needed intraday or end-of-day only?

If you share a sample payload, I can map it to these tables and write the DDL and merge SQL.

---

## Turn 2

**Prompt**

> Write a seeded synthetic data generator: small sample plus a configurable 100k+ run, with labelled invalid records (duplicates, missing fields, bad currency, out-of-order events).

**Deliverable:** `gen_txn_data.py`

**Response**

I've written `gen_txn_data.py` (stdlib only), and it runs on both presets. Two sample runs with the same seed gave byte-identical files.

**Run it**
```bash
python gen_txn_data.py --preset sample --seed 42                 # 100 txns, 238 rows
python gen_txn_data.py --preset large  --seed 42                 # 50k txns, 117,267 rows
python gen_txn_data.py --n-txns 250000 --invalid-rate 0.08 --format jsonl
```

**Outputs:** `src_account`, `src_fx_rate`, `src_txn_event`, and `manifest.json`. The manifest has the args, label counts and a SHA-256 of the event file for reproducibility checks.

**Valid data follows the schema from before:**
- Each txn has 2–3 legs, and debits equal credits per txn.
- Amounts are integer minor units, so JPY has 0 decimals and KWD has 3.
- About 3% of txns get an offsetting `REVERSED` txn linked by `reverses_txn_id`.
- `account_seq` is assigned in true event order.
- About 5% of valid rows arrive late, but inside the 3-day window.

**Invalid records.** Each carries `dq_label` and `dq_detail` as ground truth. At the default 5% invalid rate on the large run:

| `dq_label` | What's injected | Large run |
|---|---|---|
| `duplicate_event` | Exact copy of a clean row (same `event_id`), arriving later | 1,447 |
| `missing_field` | One of txn_id, account_id, direction, amount_minor, currency or event_ts blanked | 1,158 |
| `bad_currency` | Codes not in the valid list (`US`, `ZZZ`, `123`, …) | 868 |
| `out_of_order` | `event_ts` shifted back 1–10 days, contradicting `account_seq` | 1,158 |
| `late_beyond_window` | Arrives 4–10 days after the event | 579 |
| `unbalanced_txn` | One leg's amount inflated, so Σd ≠ Σc | 579 |

I added the last two, which you didn't list, because they exercise the late-arrival window and balance checks from the earlier design.

**Notes**
- Rows are written in arrival (`ingest_ts`) order, so out-of-order handling is genuinely tested.
- `dq_label` is empty for valid rows. Drop `dq_label` and `dq_detail` before feeding the pipeline, then compare its rejects against them for precision and recall per defect type.
- Each defect hits one row. A txn with a missing `txn_id` will look incomplete or unbalanced via its sibling legs, so the pipeline's reject count can legitimately exceed the labelled row count at txn level.

---

## Turn 3

**Prompt**

> Implement ingestion, validation, and a reject/quarantine path in Python with SQLite or DuckDB.

**Deliverables:** `txn_pipeline.py` (plus a one-line change to `gen_txn_data.py`)

**Response**

I've built the pipeline as `txn_pipeline.py` (SQLite, stdlib only) and run it on the 117k-row dataset. All four reconciliation checks pass, and rerunning the same file is skipped. I also made one small edit to `gen_txn_data.py`: `missing_field` rows now record the original `txn_id` in `dq_detail`, so the evaluation can explain knock-on rejects.

```bash
python gen_txn_data.py --preset large --seed 42 --out-dir out
python txn_pipeline.py --db p.db --events out/src_txn_event.csv \
       --accounts out/src_account.csv --reset --evaluate --export-quarantine q.csv
```

**Flow** (one atomic transaction per file, so a failure leaves nothing behind):
1. **Load:** the file's sha256 is checked against `load_log`, and every source row goes into append-only `raw_txn_event`.
2. **Row checks:** missing fields, bad direction, amount, timestamp or currency, unknown account, currency mismatch, lateness beyond 3 days. All reasons are recorded per row.
3. **Dedup:** first arrival wins. Rejects are classed as `DUPLICATE_EVENT`, `DUPLICATE_CONFLICT` (same id, different payload) or `DUPLICATE_BUSINESS_KEY`.
4. **Ordering:** `event_ts` must not run backwards against `account_seq` within an account. `--ooo-policy accept` turns this check off.
5. **Transaction checks:** at least 2 legs and Σd = Σc per currency, otherwise `TXN_INCOMPLETE` or `UNBALANCED_TXN`. A reversal must point at an accepted txn, otherwise `REVERSAL_ORPHAN`.
6. **Publish:** survivors go to `fact_ledger_entry`. Rejects go to `quarantine_txn_event` with stage, reason code, detail and the full `raw_json`, so they can be replayed after a fix.
7. **Marts:** `fact_transaction` and `fact_account_balance_daily` are rebuilt from scratch, which keeps late arrivals correct.

**Large run:** 117,267 raw rows became 105,897 accepted and 11,370 quarantined.

| Label | Injected | Caught with expected reason |
|---|---|---|
| missing_field | 1,158 | 1,158 |
| bad_currency | 868 | 868 |
| late_beyond_window | 579 | 579 |
| duplicate_event | 1,447 | 1,446 |
| unbalanced_txn | 579 | 562 |
| out_of_order | 1,158 | 1,059 |

- **No unexplained rejects:** none of the 111,478 clean rows was rejected for no reason. 5,674 were quarantined as collateral, because one bad leg breaks its whole txn (incomplete or unbalanced) and orphans any reversal of it. The evaluation traces each to a labelled txn.
- **Out-of-order misses:** about 99 of the 1,158 shifted rows (roughly 8.5%) were not detected. The shift left them in order relative to their neighbours, so there was nothing to detect.
- **Other-reason catches:** 17 `unbalanced_txn` rows and 1 duplicate were caught under a different reason code. The unbalanced ones were already rejected as `TXN_INCOMPLETE` after a sibling leg failed. The duplicate copy of a late-but-valid row was pushed past 3 days by its added delay, so it was rejected as `LATE_BEYOND_WINDOW`.

**Assumptions to keep in mind**
- All legs of a txn arrive in the same file, since transaction checks run per file.
- Each account holds a single currency, with a signed convention of debit + and credit −.
- Posting date is the UTC date.
- `VALID_CCY` is hard-coded and should come from `dim_currency`.
- Duplicate lookup loads all existing `event_id` values into memory. That is fine at this size, but past a few million rows it should become an indexed lookup against `fact_ledger_entry`.

---

## Turn 4

**Prompt**

> Add run metadata: run_id, start/end time, input/output/rejected counts, failure reason.

**Deliverable:** `txn_pipeline.py` (updated)

**Response**

I've added a `pipeline_run` table to `txn_pipeline.py`, with one row written per invocation. I tested the success, skip and two failure paths below.

**Columns**
- `run_id` (timestamp plus a random suffix), `status`, `start_ts`, `end_ts` and `duration_s`.
- `input_file`, `input_sha256` and `load_id`.
- `input_rows`, `output_rows`, `rejected_rows` and `rejected_by_reason` (JSON).
- `failure_stage`, `failure_reason`, `note` and `ooo_policy`.

**How it behaves**
- **Written outside the data transaction.** The `RUNNING` row is committed up front and updated at the end, so a failed file rolls back its data but the failure record survives.
- **Statuses:** `SUCCESS`, `SKIPPED` (same sha256 already loaded), `FAILED`, or `RUNNING` if the run is still in progress. The exit code is 2 on failure, 0 otherwise.
- **Failure detail:** `failure_stage` is one of `load`, `validate`, `publish`, `marts` or `reconcile`. `failure_reason` is the exception type and message, capped at 500 characters.
- **Failed-run counts:** `input_rows` is kept (what was read). `output_rows` and `rejected_rows` are `NULL`, because nothing was committed.
- **Failed reconciliation now rolls back.** Previously a failed check committed the data and only changed the exit code. A failed check now raises an error, rolls the file back and records a `FAILED` run.

**Test results**

| Case | Status | Detail |
|---|---|---|
| Large file | `SUCCESS` | 117,267 in, 105,897 out, 11,370 rejected, 5.7 s |
| Same file again | `SKIPPED` | 0 / 0 / 0 |
| Missing input file | `FAILED` | stage `load`, `FileNotFoundError` |
| Forced reconciliation failure | `FAILED` | stage `reconcile`; 2,000 rows read, nothing written |

After the failures, the database still held only the first load's 105,897 fact rows and 1 `load_log` entry. A failed file can simply be rerun.

**Limit:** if the process is killed outright, the run stays `RUNNING` forever, because nothing gets to update it. I haven't added a cleanup for that. A reaper job or a heartbeat column would be the fix, and either needs a decision on how long a run may take before it counts as dead.

---

## Turn 5

**Prompt**

> Make the load idempotent and safe to rerun, and document retry and backfill.

**Deliverables:** `txn_pipeline.py` (rewritten), `RUNBOOK.md`, `scenario_rerun_safety.py` (named `test_rerun_safety.py` at the time)

**Response**

Wrapping up: I'm presenting the updated pipeline, the runbook and the test suite.

I made the load idempotent in `txn_pipeline.py`, documented retry and backfill in `RUNBOOK.md`, and added `test_rerun_safety.py`. All 8 tests pass, and the main load still gives the same counts as before (105,897 accepted, 11,370 rejected).

**What changed**
- **Event-level idempotency.** Each ledger row now stores a `row_hash` of its business fields (everything except `ingest_ts`).
  - An event already loaded with the same payload is skipped and counted in `skipped_rows`.
  - The same `event_id` with a different payload is quarantined as `DUPLICATE_CONFLICT`.
  - Overlapping extracts and reruns therefore no longer flood the quarantine.
- **Concurrent runs.** The write lock is taken with `BEGIN IMMEDIATE` before the checks. Two simultaneous runs on one file give one `SUCCESS` and one `SKIPPED`.
- **Transactions split across files.** A txn's new legs are now checked together with legs already in the ledger.
- **Quarantine replay.** `--replay-quarantine` re-validates open rejects against current data. It is meant for rejections that resolve once reference data or sibling legs arrive (orphan accounts, reversals, incomplete and unbalanced txns). Old quarantine rows are marked superseded rather than deleted.
- **Crashed runs.** A run stuck at `RUNNING` is marked `ABANDONED` by the next run once it is older than `--stale-after-min` (default 120).
- **Upgrade of old databases.** Databases from the earlier version are migrated in place, including backfilling the new hashes.

**Test results (6,000 txns)**

| Scenario | Result |
|---|---|
| Same file twice | Second run `SKIPPED`, ledger unchanged |
| Overlapping extract | Overlap skipped, ledger equals a one-pass load |
| 3 chunks loaded newest-first, then replayed | Ledger identical to one pass (12,995 entries); a second replay changes nothing |
| Account master arrives late, then replay | All 453 orphan-related rejects recovered |
| Two concurrent runs | One `SUCCESS`, one `SKIPPED` |
| Stale `RUNNING` row | Old one `ABANDONED`, a 1-minute-old one left alone |
| Database from the earlier version | Migrates and converges |

**Decisions for you**
- **Corrections:** I treat the source as append-only, so a corrected event must arrive as a new event. Re-sending an existing `event_id` with changed values is quarantined, never applied. If your source re-sends corrected rows under the same id, this needs to change.
- **Late duplicates:** a duplicate of an already-loaded event arriving in a later file counts as skipped, not as a defect. Duplicates inside one file are still quarantined.
- **Ordering across files:** the out-of-order check only compares rows in the same file, so file boundaries are blind spots in a chunked backfill. The runbook says to use large files or `--ooo-policy accept`.
- **No "unload load N":** if bad data gets accepted, the fix is to rebuild with `--reset` from the retained source files, or restore a backup.
- **Stale threshold:** a legitimate run longer than `--stale-after-min` could be marked `ABANDONED` by a concurrent starter. Raise it above your longest expected run.

Test 7 loads a database with the earlier script, which is kept at `/tmp/txn_pipeline_v3.py` here, so it only runs where that file exists and is skipped otherwise.

---

## Turn 6

**Prompt**

> Write pytest tests for the required edge cases.

**Deliverable:** `test_txn_pipeline.py`

My first reply was cut off before the summary. The user sent "Continue" and the reply below completed the turn.

**Prompt (continuation)**

> Continue

**Response**

I wrote 98 pytest tests in `test_txn_pipeline.py`, and all pass in about 2 seconds (`pytest -q`). Each test builds a tiny hand-written input, so the expected result is exact.

**What's covered**

| Area | Edge cases |
|---|---|
| Debit/credit legs | Balanced 2-leg and split 3-leg txns; unbalanced, single-leg, and cross-currency txns; each currency balancing separately; a late extra leg on a balanced txn |
| Amounts | Zero, negative, decimal, "1e3" and text amounts; JPY and KWD integer minor units; bad direction, `leg_seq` and event type |
| Missing fields | Each of the 11 required fields blank (the reason names the field, and the healthy sibling leg is rejected as incomplete); reversal with no `reverses_txn_id`; raw and quarantine copies kept verbatim |
| Currency and accounts | 6 bad codes, currency vs account mismatch, orphan account |
| Timestamps | Malformed event and ingest times, the 3-day lag boundary (exact vs +1s), ingest before event |
| Duplicates | Same file, first arrival wins by `ingest_ts` rather than file position, same id with a changed payload, same business key with a new id, already loaded in an earlier file, overlapping files |
| Ordering | Backwards `event_ts` against `account_seq`, accept policy, ties, missing `account_seq`, and results not depending on file order |
| Reversals | Nets to zero, unknown original, original quarantined, reversal arriving before or after the original |
| Balances | Daily roll-forward with gaps, zero-sum per currency, backfilled earlier day updating a later opening balance, UTC posting date |
| Reruns and failures | Same file twice, run metadata adding up, missing file, reconciliation failure rolling back and retrying, mid-pipeline exception, stale runs, two concurrent processes, orphan-account replay, split legs joined on replay, replay stability |
| Generator | Seeded determinism, all six defect types present, defect-free txns balanced, pipeline scored against ground truth, and a chunked newest-first backfill plus replay equal to a one-pass load |

**One bug found and fixed:** a non-integer `account_seq` such as `"abc"` passed validation and then crashed the publish step with `ValueError`, failing the whole file. It is now rejected as `BAD_ACCOUNT_SEQ`, and a test covers it.

**Do the tests catch regressions?** I broke the pipeline on purpose in 7 ways, and the suite fails each time. The breaks were: the lag boundary, the ordering tie rule, the debit/credit sign, the balance check, the event-level duplicate check, the business-key duplicate check, and the write lock. The write-lock break first slipped through. When accounts are loaded, SQLite already takes the lock by then, so I added a test for the replay path, where only the explicit lock protects the run.

**Housekeeping:** pytest was also collecting my earlier script-style `test_rerun_safety.py` and exiting early, so I renamed it `scenario_rerun_safety.py` and updated `RUNBOOK.md`. It still passes 8/8 on its own, and one of its checks (migrating a database from the earlier version) has no pytest equivalent.

**Gap:** the out-of-order test on generated data only requires 85% recall, because about 8% of the generator's shifted rows stay in order and can't be detected.

---

## Turn 7

**Prompt**

> Add reconciliation queries and a validation report, plus a simple performance measurement with one optimization.

**Deliverables:** `reconciliation.sql`, `validation_report.py`, `bench_pipeline.py`, `txn_pipeline.py` (faster timestamp parser), `test_txn_pipeline.py` (now 143 tests), `RUNBOOK.md` (new sections 8 and 9)

**Response**

Running the benchmark on 50k txns, baseline vs optimized, to get the real before/after numbers.

I added the reconciliation queries and a validation report, and timed the pipeline before and after one optimization. The pytest suite now has 143 tests (up from 98), all passing, and `scenario_rerun_safety.py` still passes 8/8.

**Reconciliation and report**
- `reconciliation.sql` has 28 named checks, each returning a violation count that should be 0. It also has 9 report tables.
- `validation_report.py --db pipeline.db` runs them read-only and writes `validation_report.md`. It exits 1 if any error check fails, and 2 with `--strict` if only warnings fail.
- On the 117k-row load, 25 of 25 error checks pass and there are no warnings.
- The checks cover:
  - load accounting
  - double entry
  - keys and reference data
  - UTC posting dates and late arrivals
  - marts tying back to the ledger
  - warnings for partial reversals, a failed latest run, and a run stuck in `RUNNING`
- The report also shows run history, open quarantine by reason, top accounts by open rejects, ledger totals, arrival-lag buckets, and detection recall per defect type for generator data.
- Each check has a test that corrupts a healthy database in one specific way and asserts it gets flagged. There are 19 corruptions, so a check that always returns 0 would be caught.

**Performance** (50k txns, 117,267 rows, median of 3 fresh-process runs)

| Stage | Baseline | Optimized |
|---|---|---|
| validate | 3.34 s | 1.96 s |
| total | 6.86 s | 5.35 s |

- **Result:** throughput went from 17.1k to 21.9k rows/s (1.28x). Peak memory stayed around 200 MB.
- **Hotspot:** profiling showed `datetime.strptime` taking about 30% of a run. `parse_ts` now checks a fixed-width regex and calls `fromisoformat`.
- **Output unchanged:** the ledger and quarantine hashes are identical between versions, and `bench_pipeline.py` verifies this on every run.
- **Behaviour change:** un-padded timestamps like `2026-3-10T1:2:3Z` used to be accepted and are now rejected as `BAD_TIMESTAMP`. They broke the string ordering and the posting date taken from `event_ts[:10]`.
- **Absolute times:** they are machine-specific, so compare the ratio rather than the seconds.

**Not done:** `row_hash` is computed twice for accepted rows, which is about 20% of the profile, and is the obvious next target. I left it alone to keep this to one optimization.

I added a "Reconciliation and validation report" section and a "Performance" section to `RUNBOOK.md`.

---

## Turn 8

**Prompt**

> Write the README, data dictionary, and a short design/trade-off note.

**Deliverables:** `README.md`, `DATA_DICTIONARY.md`, `DESIGN.md`

**Response**

Final pass on the docs: fixing one version claim in the README and checking the numbers I quoted.

I wrote `README.md`, `DATA_DICTIONARY.md` and `DESIGN.md`. I checked the quoted counts against the code before finalising: 28 checks, 9 report tables, 19 corruption tests, 143 passing tests. Every reason code the pipeline can raise is in the dictionary.

- **`README.md`:** covers the quick start, a flow diagram, and what each file in the folder does. It also has the common commands, the options and exit codes, a reference run, and the main limits. In the reference run, 117,267 rows give 105,897 accepted and 11,370 quarantined, with detection recall per defect type.
- **`DATA_DICTIONARY.md`:** covers the conventions (minor units, sign, UTC, late window), the source columns with their rules, and every pipeline table with its keys and grain. It also lists all 19 reason codes by stage with whether each can be replayed, the run statuses, and the key table.
- **`DESIGN.md`:** has a table of 13 decisions, each with why it was chosen and what it costs. It also covers the failure model, the one performance optimization, known gaps, and what changes at scale.

Four things the docs say plainly that you may want to act on:
- **Collateral rejects:** a transaction is accepted whole or not at all, so about half of the quarantined rows (5,674 of 11,370) are healthy legs of a bad transaction.
- **FX:** rates are generated but never used, so there is no currency conversion.
- **Late events:** an event that is genuinely more than 3 days late has no override today; you would change `LATE_DAYS` or add an allow-list.
- **No unload:** accepted bad data cannot be removed by load; you rebuild from the source files or restore a backup.

I stated the Python version only as "tested on 3.12 / SQLite 3.45" rather than claiming support for older versions, because I haven't run it on any.

---

## Turn 9

**Prompt**

> now give the prompt and response history for submission

**Response:** this file.

---

## Deliverables at the end of the conversation

| File | Purpose |
|---|---|
| `gen_txn_data.py` | Seeded synthetic data generator with labelled defects |
| `txn_pipeline.py` | Ingestion, validation, quarantine, run metadata, idempotent load, replay |
| `reconciliation.sql` | 28 reconciliation checks and 9 report tables |
| `validation_report.py` | Runs the SQL and writes the validation report |
| `bench_pipeline.py` | Performance measurement, baseline vs optimized |
| `test_txn_pipeline.py` | 143 pytest tests |
| `scenario_rerun_safety.py` | 8 end-to-end rerun and backfill scenarios |
| `README.md`, `DATA_DICTIONARY.md`, `DESIGN.md`, `RUNBOOK.md` | Documentation |
| `PROMPT_RESPONSE_HISTORY.md` | This file |
