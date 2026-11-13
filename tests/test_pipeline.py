"""End-to-end tests: real files -> real DuckDB warehouse, one scenario per test."""

from __future__ import annotations

import csv
import shutil
from decimal import Decimal
from pathlib import Path

import duckdb

from conftest import HEADER, leg, transfer
from txn_pipeline.config import PipelineConfig
from txn_pipeline.pipeline import Pipeline
from txn_pipeline.reports import run_checks
from txn_pipeline.schemas import Reason

REPO = Path(__file__).resolve().parents[1]


def all_checks_pass(env) -> bool:
    con = duckdb.connect(str(env.cfg.database), read_only=True)
    try:
        failed = [c for c in run_checks(con) if not c.passed]
    finally:
        con.close()
    assert not failed, failed
    return True


# ------------------------------------------------------------------ happy path
def test_happy_path_loads_balanced_transactions(env):
    env.write("transactions_001.csv", transfer(1) + transfer(2, amount="25.50"))
    summary = env.run()

    assert summary.status == "SUCCESS"
    f = summary.files[0]
    assert (f.rows_read, f.rows_curated, f.rows_quarantined, f.transactions_curated) == (4, 4, 0, 2)
    assert env.curated_txns() == {"T1", "T2"}
    assert env.scalar("SELECT current_balance FROM v_account_balances WHERE account_id='A1'") == Decimal("64.50")
    assert env.scalar("SELECT status FROM pipeline_runs") == "SUCCESS"
    assert all_checks_pass(env)


# ------------------------------------------------------------------ empty input
def test_empty_batch_header_only_and_zero_byte(env):
    env.write("transactions_001.csv", [])
    (env.incoming / "transactions_002.csv").write_bytes(b"")
    summary = env.run()
    assert summary.status == "SUCCESS"
    assert [f.status for f in summary.files] == ["EMPTY", "EMPTY"]
    assert env.scalar("SELECT COUNT(*) FROM curated_entries") == 0


def test_no_input_files_is_a_successful_noop(env):
    summary = env.run()
    assert summary.status == "SUCCESS" and summary.files == []


# ------------------------------------------------------------------ malformed / nulls
def test_malformed_row_quarantined_and_its_transaction_blocked(env):
    lines = transfer(1)
    lines[0] += ",unexpected extra field"
    env.write("transactions_001.csv", lines + transfer(2))
    summary = env.run()

    assert summary.files[0].rows_quarantined == 2
    assert env.reasons("T1") == {Reason.MALFORMED_ROW, Reason.INCOMPLETE_TRANSACTION}
    assert env.curated_txns() == {"T2"}
    line = env.scalar("SELECT source_line FROM quarantine WHERE reason_code='MALFORMED_ROW'")
    assert line == 2  # traceable to the physical line in the source file


def test_missing_required_fields_and_nulls(env):
    env.write("transactions_001.csv", [
        leg("E1-1", "T1", 1, "A1", "DEBIT", ""),           # blank amount
        leg("E1-2", "T1", 2, "A2", "CREDIT", "10.00"),
        leg("", "T2", 1, "A1", "DEBIT", "5.00"),           # blank event_id
        leg("E2-2", "T2", 2, "", "CREDIT", "5.00"),        # blank account
    ])
    env.run()
    assert env.reasons("T1") == {Reason.MISSING_REQUIRED_FIELD, Reason.INCOMPLETE_TRANSACTION}
    assert env.reasons("T2") == {Reason.MISSING_REQUIRED_FIELD}
    assert env.curated_txns() == set()


# ------------------------------------------------------------------ duplicates
def test_exact_duplicate_in_file_is_dropped_but_transaction_kept(env):
    lines = transfer(1)
    env.write("transactions_001.csv", lines + [lines[0]])
    summary = env.run()
    assert env.curated_txns() == {"T1"}
    assert env.reasons("T1") == {Reason.DUPLICATE_EVENT}
    assert summary.files[0].rows_read == 3 and summary.files[0].rows_curated == 2


def test_conflicting_duplicate_in_file_blocks_transaction(env):
    lines = transfer(1)
    conflicting = lines[0].replace("10.00", "99.00")
    env.write("transactions_001.csv", lines + [conflicting])
    env.run()
    assert env.curated_txns() == set()
    assert Reason.CONFLICTING_DUPLICATE in env.reasons("T1")


def test_replay_in_later_file_is_counted_not_duplicated(env):
    env.write("transactions_001.csv", transfer(1))
    env.write("transactions_002.csv", transfer(1) + transfer(2))  # T1 replayed
    summary = env.run()
    second = summary.files[1]
    assert (second.rows_already_loaded, second.rows_curated, second.rows_quarantined) == (2, 2, 0)
    assert env.scalar("SELECT COUNT(*) FROM curated_entries") == 4


def test_changed_event_after_load_does_not_overwrite_history(env):
    env.write("transactions_001.csv", transfer(1))
    env.run()
    env.write("transactions_002.csv", [transfer(1)[0].replace("10.00", "11.00")])
    env.run()
    assert env.scalar("SELECT amount FROM curated_entries WHERE event_id='E1-1'") == Decimal("10.00")
    assert env.reasons("T1") == {Reason.CONFLICTING_DUPLICATE}


# ------------------------------------------------------------------ schema change
def test_missing_column_fails_file_without_loading_anything(env):
    env.write("transactions_001.csv", ["E1-1,T1,1,A1,DEBIT,10.00,TRANSFER,2026-11-01T10:00:00Z"],
              header="event_id,transaction_id,leg_seq,account_id,direction,amount,transaction_type,event_ts")
    summary = env.run()
    assert summary.status == "FAILED"
    assert "SCHEMA_MISMATCH" in summary.files[0].error_message
    assert env.scalar("SELECT COUNT(*) FROM curated_entries") == 0
    assert env.scalar("SELECT status FROM file_loads") == "FAILED"


def test_extra_column_and_reordered_columns_are_tolerated(env):
    header = "transaction_id,event_id,channel,leg_seq,account_id,direction,amount,currency,transaction_type,event_ts,description"
    env.write("transactions_001.csv", [
        "T1,E1-1,web,1,A1,DEBIT,10.00,USD,TRANSFER,2026-11-01T10:00:00Z,x",
        "T1,E1-2,web,2,A2,CREDIT,10.00,USD,TRANSFER,2026-11-01T10:00:00Z,x",
    ], header=header)
    summary = env.run()
    assert summary.status == "SUCCESS" and env.curated_txns() == {"T1"}
    assert "SCHEMA_DRIFT" in summary.files[0].warnings[0]


# ------------------------------------------------------------------ partial failure + retry
def test_crash_mid_load_rolls_back_and_retry_succeeds(env):
    env.write("transactions_001.csv", transfer(1) + [leg("E9-1", "T9", 1, "A1", "DEBIT", "1.00")])

    def crash(checkpoint):
        if checkpoint == "before_commit":
            raise RuntimeError("simulated crash")

    failed = env.run(fault_hook=crash)
    assert failed.status == "FAILED"
    # nothing half-written: no curated rows, no quarantine rows, file not registered as loaded
    assert env.scalar("SELECT COUNT(*) FROM curated_entries") == 0
    assert env.scalar("SELECT COUNT(*) FROM quarantine WHERE source_file='transactions_001.csv'") == 0

    retry = env.run()
    assert retry.status == "SUCCESS"
    assert retry.files[0].rows_curated == 2 and retry.files[0].rows_quarantined == 1
    assert env.query("SELECT status FROM file_loads ORDER BY started_at") == [("FAILED",), ("SUCCESS",)]
    assert all_checks_pass(env)


def test_one_bad_file_gives_partial_failure_and_only_it_is_retried(env):
    env.write("transactions_001.csv", transfer(1))
    env.write("transactions_002.csv", transfer(2))
    calls = {"n": 0}

    def crash_second_file(checkpoint):
        if checkpoint == "after_entries":
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("disk full")

    first = env.run(fault_hook=crash_second_file)
    assert first.status == "PARTIAL_FAILURE"
    assert env.curated_txns() == {"T1"}

    retry = env.run()
    assert [f.status for f in retry.files] == ["SKIPPED", "SUCCESS"]
    assert env.curated_txns() == {"T1", "T2"}


# ------------------------------------------------------------------ rerun / idempotency
def test_rerun_same_input_is_skipped(env):
    env.write("transactions_001.csv", transfer(1))
    env.run()
    again = env.run()
    assert again.files[0].status == "SKIPPED"
    assert env.scalar("SELECT COUNT(*) FROM curated_entries") == 2


def test_forced_rerun_backfill_is_idempotent(env):
    lines = transfer(1) + [leg("E9-1", "T9", 1, "A1", "DEBIT", "1.00")]
    env.write("transactions_001.csv", lines)
    env.run()
    forced = env.run(force=True)
    f = forced.files[0]
    assert f.status == "SUCCESS" and f.rows_curated == 0 and f.rows_already_loaded == 2
    assert env.scalar("SELECT COUNT(*) FROM curated_entries") == 2
    assert env.scalar("SELECT COUNT(*) FROM quarantine") == 1  # same deterministic quarantine_id
    assert all_checks_pass(env)


# ------------------------------------------------------------------ reference key
def test_invalid_reference_key(env):
    env.write("transactions_001.csv", transfer(1, dst="GHOST"))
    env.run()
    assert env.reasons("T1") == {Reason.UNKNOWN_ACCOUNT, Reason.INCOMPLETE_TRANSACTION}


# ------------------------------------------------------------------ domain-specific edge cases
def test_unbalanced_transaction_never_reaches_curated(env):
    env.write("transactions_001.csv", [
        leg("E1-1", "T1", 1, "A1", "DEBIT", "10.00"),
        leg("E1-2", "T1", 2, "A2", "CREDIT", "9.99"),
    ])
    env.run()
    assert env.curated_txns() == set()
    assert env.reasons("T1") == {Reason.UNBALANCED}


def test_mixed_currency_transaction_rejected(env):
    env.write("transactions_001.csv", [
        leg("E1-1", "T1", 1, "A1", "DEBIT", "10.00", currency="USD"),
        leg("E1-2", "T1", 2, "E1", "CREDIT", "10.00", currency="EUR"),
    ])
    env.run()
    assert env.reasons("T1") == {Reason.MIXED_CURRENCY}


def test_late_arrival_flagged_and_balances_use_event_time_order(env):
    # file 1: A2 receives 10 at 10:00 then pays 5 at 11:00 (never negative in arrival order)
    env.write("transactions_001.csv", transfer(1, ts="2026-11-01T10:00:00Z")
              + transfer(2, src="A2", dst="A1", amount="5.00", ts="2026-11-01T11:00:00Z"))
    env.run()
    # file 2 arrives later but happened EARLIER: A2 pays 5 at 09:00 -> balance -5 at that moment
    env.write("transactions_002.csv", transfer(3, src="A2", dst="A1", amount="5.00", ts="2026-11-01T09:00:00Z"))
    summary = env.run()

    assert summary.files[1].late_arrivals == 2
    assert env.scalar("SELECT COUNT(*) FROM curated_entries WHERE is_late_arrival") == 2
    breach = env.query("SELECT event_id, running_balance FROM v_balance_exceptions WHERE account_id='A2'")
    assert breach == [("E3-1", Decimal("-5.00"))]


def test_extra_leg_for_already_posted_transaction_is_rejected(env):
    env.write("transactions_001.csv", transfer(1))
    env.run()
    env.write("transactions_002.csv", [leg("E1-3", "T1", 3, "A2", "CREDIT", "1.00")])
    env.run()
    assert env.reasons("T1") == {Reason.TRANSACTION_ALREADY_POSTED}
    assert env.scalar("SELECT leg_count FROM curated_transactions WHERE transaction_id='T1'") == 2


def test_future_and_before_open_events_rejected(env):
    env.write("transactions_001.csv",
              transfer(1, ts="2027-01-01T00:00:00Z")
              + transfer(2, src="EXT-USD", dst="NEW1", ts="2026-10-01T00:00:00Z"))
    env.run()
    assert env.reasons("T1") == {Reason.FUTURE_TIMESTAMP}
    assert env.reasons("T2") == {Reason.BEFORE_ACCOUNT_OPEN, Reason.INCOMPLETE_TRANSACTION}


def test_invalid_account_reference_rows_are_quarantined(env):
    with env.cfg.accounts_file.open("a", encoding="utf-8") as fh:
        fh.write("BAD,Bad,CUSTOMER,USD,abc,false,2025-01-01T00:00:00Z\n")
    env.write("transactions_001.csv", transfer(1, dst="BAD"))
    env.run()
    assert env.scalar("SELECT COUNT(*) FROM quarantine WHERE reason_code='INVALID_ACCOUNT_RECORD'") == 1
    assert Reason.UNKNOWN_ACCOUNT in env.reasons("T1")


# ------------------------------------------------------------------ the committed sample dataset
def test_sample_dataset_matches_expected_outcomes(tmp_path):
    sample = REPO / "data" / "sample"
    cfg = PipelineConfig(input_dir=sample / "incoming", database=tmp_path / "w.duckdb",
                         accounts_file=sample / "accounts.csv")
    summary = Pipeline(cfg).run()
    assert summary.status == "SUCCESS"

    con = duckdb.connect(str(cfg.database), read_only=True)
    curated = {r[0] for r in con.execute("SELECT transaction_id FROM curated_transactions").fetchall()}
    reasons: dict[str, set] = {}
    for txn, reason in con.execute("SELECT transaction_id, reason_code FROM quarantine").fetchall():
        reasons.setdefault(txn, set()).add(reason)
    assert all(c.passed for c in run_checks(con))
    con.close()

    with (sample / "expected" / "expected_outcomes.csv").open(encoding="utf-8") as fh:
        for label in csv.DictReader(fh):
            txn = label["transaction_id"]
            if label["expected_status"] == "CURATED":
                assert txn in curated, txn
            else:
                assert txn not in curated, txn
                assert label["expected_reason"] in reasons.get(txn, set()), (txn, reasons.get(txn))
