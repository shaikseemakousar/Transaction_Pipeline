# Build steps: how this solution maps to the brief

Each step lists what was built, where the code is, how to verify it, and the commit that records it.
Use it as a walkthrough script for the review call.

---

## Step 1: Schemas, keys and assumptions (brief: workflow rule 1, Step 1)

**Built:** source schema (leg grain), allowed values, reason codes, config file, README/data dictionary.

* `src/txn_pipeline/schemas.py`: required/optional columns, enums, every `Reason` code
* `config/pipeline.toml` + `src/txn_pipeline/config.py`: paths and validation settings, overridable from CLI
* `docs/data_dictionary.md`, `docs/design_decisions.md` (assumptions)

Key idea to explain: *one row = one leg*. A transaction = ≥ 2 legs sharing `transaction_id`.
Natural keys: `event_id` (leg) and `transaction_id`.

```bash
git add .gitignore requirements.txt pytest.ini config/ src/txn_pipeline/__init__.py src/txn_pipeline/__main__.py src/txn_pipeline/schemas.py src/txn_pipeline/config.py
git commit -m "Define source schema, reason codes and pipeline config"
```

## Step 2: Ingestion (brief: workflow rule 3, Step 2)

**Built:** `src/txn_pipeline/reader.py`
* SHA-256 checksum of the raw bytes (used later for "already loaded?")
* header normalised and compared with the schema → `missing_columns` (fail file) / `extra_columns` (warn)
* rows with the wrong field count → malformed list, with physical line number and best-effort ids
* blank values → `None`, so "missing" is detected uniformly

## Step 3: Validation rules (brief: Step 3)

**Built:** `src/txn_pipeline/validators.py` (pure functions, no I/O)
* `validate_txn_row`: fixed-order checks: required → leg_seq → direction → type → amount → currency →
  ISO timestamp → not future → account exists → currency matches account → not before account opened
* `check_transaction`: ≥ 2 legs, unique leg_seq, one currency, one timestamp, one type, debits = credits
* `Decimal` everywhere for money (no float rounding)

```bash
git add src/txn_pipeline/reader.py src/txn_pipeline/validators.py
git commit -m "Add CSV reader and row/transaction validation rules"
```

## Step 4: Storage, curated + quarantine layers (brief: workflow rule 3, Step 3)

**Built:** `src/txn_pipeline/storage.py`
* DuckDB tables `ref_accounts`, `curated_entries`, `curated_transactions`, `quarantine`, `pipeline_runs`, `file_loads`
* views `v_account_balances`, `v_running_balances` (event-time order), `v_balance_exceptions`
* bulk inserts with `WHERE NOT EXISTS` anti-joins, so the same row can never be inserted twice

## Step 5: Orchestration, duplicates, idempotency, run metrics (brief: rule 4, Steps 4 & 6)

**Built:** `src/txn_pipeline/pipeline.py`
1. load + validate reference accounts (bad rows → quarantine)
2. per file: checksum skip → schema check → empty check → row rules → in-file duplicates →
   duplicates vs warehouse → transaction rules → late-arrival flag
3. **one DB transaction per file**: curated rows + quarantine rows + `file_loads` SUCCESS commit together
4. `pipeline_runs` / `file_loads` record status, counts (read / curated / quarantined / already loaded / late),
   per-stage timings and the failure reason
5. `fault_hook` checkpoints let tests simulate a crash mid-load

```bash
git add src/txn_pipeline/storage.py src/txn_pipeline/pipeline.py
git commit -m "Add DuckDB storage and atomic, idempotent per-file pipeline with run tracking"
```

## Step 6: Reconciliation, report and CLI (brief: rule 5, Step 8)

**Built:**
* `src/txn_pipeline/sql/reconciliation.sql`: 11 named checks, each returns a violation count (0 = pass)
* `src/txn_pipeline/reports.py`: runs the checks, writes `reports/pipeline_report.md`, exports CSVs
* `src/txn_pipeline/cli.py`: `run`, `report`, `check`, `generate`, `reset`; exit code ≠ 0 on failure

```bash
git add src/txn_pipeline/sql/ src/txn_pipeline/reports.py src/txn_pipeline/cli.py
git commit -m "Add reconciliation checks, markdown report and CLI"
```

## Step 7: Sample data + scalable generator (brief: rule 2, Step 7, volume section)

**Built:**
* `data/sample/`: hand-made files where **each row targets one rule**. Expected outcomes live in
  `data/sample/expected/expected_outcomes.csv` (separate from input). There is also a bad-schema file and an empty file.
* `src/txn_pipeline/generator.py`: fixed seed, 100k+ rows, 4 currencies, 2- and 3-leg transactions,
  12 labelled anomaly types, exact duplicates, out-of-order rows, late arrivals, replays; labels in a separate file.

```bash
git add src/txn_pipeline/generator.py data/sample/accounts.csv data/sample/incoming/ data/sample/bad_schema/ data/sample/expected/
git commit -m "Add hand-made sample dataset with expected outcomes and seeded generator"
```

## Step 8: Automated tests (brief: rule 5, Step 5)

**Built:** `tests/` (63 tests)
* `test_validators.py`: every parser and rule, parametrised
* `test_pipeline.py`: one end-to-end scenario per required edge case (happy path, empty, malformed,
  duplicates, nulls, schema change, partial failure + retry, rerun, invalid reference key) + domain cases
* `test_generator.py`: reproducibility; pipeline decisions = independent labels

```bash
python -m pytest
git add tests/
git commit -m "Add unit and end-to-end tests for all required edge cases"
```

## Step 9: Performance (brief: Step 9)

**Built:** `scripts/benchmark.py`. Measured: 100k rows in ~5.3 s. The bulk insert is ~800× faster than row-by-row.
The next candidate optimisation is parallel validation.

```bash
python scripts/benchmark.py --rows 100000
git add scripts/
git commit -m "Add benchmark script with load optimisation measurement"
```

## Step 10: Outputs, documentation, AI disclosure (brief: Steps 1 & 10, submission guidelines)

```bash
python -m txn_pipeline reset
python -m txn_pipeline run
python -m txn_pipeline run
python -m txn_pipeline report --export-dir data/sample/output
git add README.md docs/ ai/ reports/ data/sample/output/
git commit -m "Add README, design notes, data dictionary, sample outputs and AI usage disclosure"
```
