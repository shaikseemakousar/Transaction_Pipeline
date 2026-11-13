"""Pipeline orchestration: discover files -> read -> validate -> load atomically -> record run.

Guarantees
  * Atomic per file: curated rows, quarantine rows and the file_loads SUCCESS record are
    written in ONE database transaction. A crash mid-file leaves nothing behind, so a
    retry simply reprocesses that file.
  * Idempotent: a file whose checksum was already loaded is skipped; and even when forced
    (--force / backfill), inserts are anti-joined on natural keys so nothing duplicates.
  * Every input row ends up in exactly one bucket: curated, quarantined or already-loaded
    (rows_read = rows_curated + rows_quarantined + rows_already_loaded).
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd

from txn_pipeline.config import PipelineConfig
from txn_pipeline.reader import FileReadResult, read_csv_file
from txn_pipeline.schemas import (
    NON_POISONING_REASONS,
    OPTIONAL_ACCOUNT_COLUMNS,
    OPTIONAL_TXN_COLUMNS,
    REQUIRED_ACCOUNT_COLUMNS,
    REQUIRED_TXN_COLUMNS,
    Reason,
)
from txn_pipeline.storage import Storage
from txn_pipeline.validators import (
    Account,
    ValidationError,
    check_transaction,
    validate_account_row,
    validate_txn_row,
)

logger = logging.getLogger("txn_pipeline")


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def make_run_id(ts: datetime) -> str:
    return "run_" + ts.strftime("%Y%m%dT%H%M%S%fZ")


def make_quarantine_id(checksum: str, line_no: int | None, reason: str, event_id: str | None) -> str:
    """Deterministic: re-processing the same file produces the same quarantine ids."""
    key = f"{checksum}|{line_no}|{reason}|{event_id}"
    return "q_" + hashlib.sha256(key.encode()).hexdigest()[:24]


def _ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


class FileRejected(Exception):
    """The whole file cannot be processed (unreadable, schema mismatch)."""


@dataclass
class Reject:
    line_no: int | None
    raw: str | None
    event_id: str | None
    transaction_id: str | None
    reason: str
    detail: str


@dataclass
class FileOutcome:
    source_file: str
    started_at: datetime
    file_checksum: str = ""
    status: str = "FAILED"
    rows_read: int = 0
    rows_curated: int = 0
    rows_quarantined: int = 0
    rows_already_loaded: int = 0
    transactions_curated: int = 0
    late_arrivals: int = 0
    read_ms: int = 0
    validate_ms: int = 0
    load_ms: int = 0
    warnings: list[str] = field(default_factory=list)
    error_message: str | None = None
    reject_counts: dict[str, int] = field(default_factory=dict)

    def as_row(self, run_id: str, finished_at: datetime) -> dict:
        return {
            "run_id": run_id,
            "source_file": self.source_file,
            "file_checksum": self.file_checksum,
            "status": self.status,
            "rows_read": self.rows_read,
            "rows_curated": self.rows_curated,
            "rows_quarantined": self.rows_quarantined,
            "rows_already_loaded": self.rows_already_loaded,
            "transactions_curated": self.transactions_curated,
            "late_arrivals": self.late_arrivals,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "read_ms": self.read_ms,
            "validate_ms": self.validate_ms,
            "load_ms": self.load_ms,
            "warnings": "; ".join(self.warnings) or None,
            "error_message": self.error_message,
        }


@dataclass
class RunSummary:
    run_id: str
    status: str
    files: list[FileOutcome]
    duration_ms: int
    error_message: str | None = None

    @property
    def rows_read(self) -> int:
        return sum(f.rows_read for f in self.files if f.status != "SKIPPED")

    @property
    def rows_curated(self) -> int:
        return sum(f.rows_curated for f in self.files)

    @property
    def rows_quarantined(self) -> int:
        return sum(f.rows_quarantined for f in self.files)


class Pipeline:
    def __init__(
        self,
        config: PipelineConfig,
        *,
        now: datetime | None = None,
        fault_hook: Callable[[str], None] | None = None,
    ):
        """
        now        -- override "current time" (tests; future-timestamp rule uses it)
        fault_hook -- called at named checkpoints; tests raise from it to simulate crashes
        """
        self.cfg = config
        self.now = now
        self.fault_hook = fault_hook or (lambda checkpoint: None)

    # ================================================================ run
    def run(self, *, force: bool = False) -> RunSummary:
        run_started = self.now or utcnow()
        run_id = make_run_id(utcnow())
        t0 = time.perf_counter()
        storage = Storage(self.cfg.database)
        storage.init_schema()
        storage.start_run(run_id, utcnow())
        logger.info("run_id=%s started input_dir=%s force=%s", run_id, self.cfg.input_dir, force)

        outcomes: list[FileOutcome] = []
        error: str | None = None
        try:
            if self.cfg.accounts_file:
                self._load_accounts(storage, run_id, self.cfg.accounts_file)
            accounts = {
                acc_id: Account(acc_id, cur, opened, overdraft)
                for acc_id, cur, opened, overdraft in storage.fetch_accounts()
            }
            if not accounts:
                logger.warning("run_id=%s no reference accounts loaded; every leg will be UNKNOWN_ACCOUNT", run_id)
            if not self.cfg.input_dir.is_dir():
                raise FileNotFoundError(f"input_dir not found: {self.cfg.input_dir}")

            files = sorted(p for p in self.cfg.input_dir.glob(self.cfg.file_pattern) if p.is_file())
            if not files:
                logger.warning("run_id=%s no files matched %s", run_id, self.cfg.file_pattern)
            for path in files:
                outcome = self._process_file(storage, run_id, path, accounts, run_started, force)
                outcomes.append(outcome)
                if outcome.status == "FAILED" and self.cfg.stop_on_error:
                    logger.error("run_id=%s stop_on_error=true, aborting after %s", run_id, path.name)
                    break
        except Exception as exc:  # fatal, run-level failure
            logger.exception("run_id=%s fatal error", run_id)
            error = f"{type(exc).__name__}: {exc}"

        failed = [o for o in outcomes if o.status == "FAILED"]
        if error or (failed and len(failed) == len(outcomes)):
            status = "FAILED"
        elif failed:
            status = "PARTIAL_FAILURE"
        else:
            status = "SUCCESS"

        summary = RunSummary(run_id, status, outcomes, _ms(t0), error)
        storage.finish_run(
            run_id,
            status=status,
            finished_at=utcnow(),
            duration_ms=summary.duration_ms,
            files_seen=len(outcomes),
            files_loaded=sum(o.status in ("SUCCESS", "EMPTY") for o in outcomes),
            files_skipped=sum(o.status == "SKIPPED" for o in outcomes),
            files_failed=len(failed),
            rows_read=summary.rows_read,
            rows_curated=summary.rows_curated,
            rows_quarantined=summary.rows_quarantined,
            error_message=error or ("; ".join(f"{o.source_file}: {o.error_message}" for o in failed) or None),
        )
        storage.close()
        logger.info(
            "run_id=%s finished status=%s files=%d read=%d curated=%d quarantined=%d duration_ms=%d",
            run_id, status, len(outcomes), summary.rows_read, summary.rows_curated,
            summary.rows_quarantined, summary.duration_ms,
        )
        return summary

    # ================================================================ reference data
    def _load_accounts(self, storage: Storage, run_id: str, path: Path) -> None:
        read = read_csv_file(path, REQUIRED_ACCOUNT_COLUMNS, OPTIONAL_ACCOUNT_COLUMNS)
        if read.error or read.missing_columns:
            raise FileRejected(f"accounts file {path.name}: {read.error or f'missing columns {read.missing_columns}'}")
        now = utcnow()
        valid, rejects, seen = [], [], set()
        for m in read.malformed:
            rejects.append(Reject(m.line_no, m.raw, None, None, Reason.MALFORMED_ROW, m.detail))
        for row in read.rows:
            try:
                acc = validate_account_row(row.values, self.cfg.allowed_currencies)
            except ValidationError as exc:
                rejects.append(Reject(row.line_no, row.raw, None, None, exc.reason, f"{row.values.get('account_id')}: {exc.detail}"))
                continue
            if acc["account_id"] in seen:
                rejects.append(Reject(row.line_no, row.raw, None, None, Reason.INVALID_ACCOUNT_RECORD, f"duplicate account_id {acc['account_id']}"))
                continue
            seen.add(acc["account_id"])
            valid.append({
                **acc,
                "opening_balance": str(acc["opening_balance"]),
                "opened_at": acc["opened_at"].isoformat(),
                "source_file": path.name,
                "run_id": run_id,
                "loaded_at": now.isoformat(),
            })
        storage.begin()
        try:
            storage.upsert_accounts(pd.DataFrame(valid))
            storage.insert_quarantine(self._quarantine_frame(rejects, run_id, path.name, read.checksum))
            storage.commit()
        except Exception:
            storage.rollback()
            raise
        logger.info("run_id=%s accounts loaded=%d rejected=%d from %s", run_id, len(valid), len(rejects), path.name)

    # ================================================================ one file
    def _process_file(
        self,
        storage: Storage,
        run_id: str,
        path: Path,
        accounts: dict[str, Account],
        run_started: datetime,
        force: bool,
    ) -> FileOutcome:
        out = FileOutcome(source_file=path.name, started_at=utcnow())
        try:
            t = time.perf_counter()
            read = read_csv_file(path, REQUIRED_TXN_COLUMNS, OPTIONAL_TXN_COLUMNS)
            out.read_ms = _ms(t)
            out.file_checksum = read.checksum
            out.rows_read = read.rows_read

            if not force and storage.file_already_loaded(read.checksum):
                out.status = "SKIPPED"
                out.warnings.append("identical file content already loaded (checksum match)")
                storage.record_file_load(out.as_row(run_id, utcnow()))
                logger.info("run_id=%s file=%s SKIPPED (already loaded)", run_id, path.name)
                return out
            if read.error:
                raise FileRejected(read.error)
            if read.missing_columns:
                raise FileRejected(f"SCHEMA_MISMATCH: missing required columns {read.missing_columns}")
            if read.extra_columns:
                out.warnings.append(f"SCHEMA_DRIFT: unexpected columns ignored {read.extra_columns}")
                logger.warning("run_id=%s file=%s unexpected columns %s", run_id, path.name, read.extra_columns)
            if read.is_empty:
                out.status = "EMPTY"
                storage.record_file_load(out.as_row(run_id, utcnow()))
                logger.info("run_id=%s file=%s EMPTY batch", run_id, path.name)
                return out

            t = time.perf_counter()
            curated, rejects, already = self._validate(storage, read, accounts, run_started)
            out.validate_ms = _ms(t)

            t = time.perf_counter()
            loaded_at = utcnow()
            entries_df = self._entries_frame(curated, run_id, path.name, read.checksum, loaded_at)
            quarantine_df = self._quarantine_frame(rejects, run_id, path.name, read.checksum)
            storage.begin()
            try:
                storage.insert_entries(entries_df)
                self.fault_hook("after_entries")
                out.transactions_curated = storage.insert_transactions_for(run_id, read.checksum, loaded_at)
                storage.insert_quarantine(quarantine_df)
                self.fault_hook("before_commit")
                out.rows_curated = len(curated)
                out.rows_quarantined = len(rejects)
                out.rows_already_loaded = already
                out.late_arrivals = sum(1 for r in curated if r["is_late_arrival"])
                out.status = "SUCCESS"
                out.load_ms = _ms(t)
                storage.record_file_load(out.as_row(run_id, utcnow()))
                storage.commit()
            except Exception:
                storage.rollback()
                raise
            for r in rejects:
                out.reject_counts[r.reason] = out.reject_counts.get(r.reason, 0) + 1
            logger.info(
                "run_id=%s file=%s SUCCESS read=%d curated=%d quarantined=%d already_loaded=%d late=%d",
                run_id, path.name, out.rows_read, out.rows_curated, out.rows_quarantined,
                out.rows_already_loaded, out.late_arrivals,
            )
        except Exception as exc:
            out.status = "FAILED"
            out.error_message = f"{type(exc).__name__}: {exc}"
            out.rows_curated = out.rows_quarantined = out.rows_already_loaded = 0
            out.transactions_curated = out.late_arrivals = 0
            logger.error("run_id=%s file=%s FAILED %s", run_id, path.name, out.error_message)
            storage.record_file_load(out.as_row(run_id, utcnow()))
        return out

    # ================================================================ validation
    def _validate(
        self,
        storage: Storage,
        read: FileReadResult,
        accounts: dict[str, Account],
        run_started: datetime,
    ) -> tuple[list[dict], list[Reject], int]:
        rejects: list[Reject] = [
            Reject(m.line_no, m.raw, m.event_id_hint, m.transaction_id_hint, Reason.MALFORMED_ROW, m.detail)
            for m in read.malformed
        ]

        # ---- 1. row-level validation
        valid: list[dict] = []
        for row in read.rows:
            try:
                rec = validate_txn_row(
                    row.values, accounts, self.cfg.allowed_currencies,
                    run_started, self.cfg.future_tolerance_minutes,
                )
            except ValidationError as exc:
                rejects.append(Reject(row.line_no, row.raw, row.values.get("event_id"),
                                      row.values.get("transaction_id"), exc.reason, exc.detail))
                continue
            rec["source_line"] = row.line_no
            rec["raw"] = row.raw
            valid.append(rec)

        # ---- 2. duplicates inside this file (keep first occurrence)
        first_seen: dict[str, dict] = {}
        unique: list[dict] = []
        for rec in valid:
            first = first_seen.get(rec["event_id"])
            if first is None:
                first_seen[rec["event_id"]] = rec
                unique.append(rec)
            elif first["payload_hash"] == rec["payload_hash"]:
                rejects.append(self._reject(rec, Reason.DUPLICATE_EVENT,
                                            f"exact duplicate of line {first['source_line']}"))
            else:
                rejects.append(self._reject(rec, Reason.CONFLICTING_DUPLICATE,
                                            f"event_id also on line {first['source_line']} with different content"))

        # ---- 3. duplicates against data already loaded by earlier runs
        existing = storage.existing_event_hashes([r["event_id"] for r in unique])
        new: list[dict] = []
        already_loaded = 0
        for rec in unique:
            prior_hash = existing.get(rec["event_id"])
            if prior_hash is None:
                new.append(rec)
            elif prior_hash == rec["payload_hash"]:
                already_loaded += 1  # harmless replay: skip silently, but count it
            else:
                rejects.append(self._reject(rec, Reason.CONFLICTING_DUPLICATE,
                                            "event_id already loaded with different content; curated history kept"))

        # ---- 4. transaction-level rules (a transaction is accepted or rejected as a whole)
        poisoned: dict[str, str] = {}
        for r in rejects:
            if r.transaction_id and r.reason not in NON_POISONING_REASONS:
                poisoned.setdefault(r.transaction_id, r.reason)

        by_txn: dict[str, list[dict]] = defaultdict(list)
        for rec in new:
            by_txn[rec["transaction_id"]].append(rec)
        already_posted = storage.existing_transaction_ids(list(by_txn))

        max_ts = storage.max_event_ts_by_account()
        curated: list[dict] = []
        for txn_id, legs in by_txn.items():
            if txn_id in already_posted:
                failure = (Reason.TRANSACTION_ALREADY_POSTED,
                           "transaction_id already curated by an earlier load; new legs not applied")
            elif txn_id in poisoned:
                failure = (Reason.INCOMPLETE_TRANSACTION,
                           f"another leg of this transaction was rejected ({poisoned[txn_id]})")
            else:
                failure = check_transaction(legs)
            if failure:
                rejects.extend(self._reject(leg, *failure) for leg in legs)
                continue
            for leg in legs:
                prior = max_ts.get(leg["account_id"])
                leg["is_late_arrival"] = prior is not None and leg["event_ts"] < prior
            curated.extend(legs)
        return curated, rejects, already_loaded

    @staticmethod
    def _reject(rec: dict, reason: str, detail: str) -> Reject:
        return Reject(rec["source_line"], rec["raw"], rec["event_id"], rec["transaction_id"], reason, detail)

    # ================================================================ frames
    @staticmethod
    def _entries_frame(curated: list[dict], run_id: str, source_file: str, checksum: str, loaded_at: datetime) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "event_id": r["event_id"],
                    "transaction_id": r["transaction_id"],
                    "leg_seq": r["leg_seq"],
                    "account_id": r["account_id"],
                    "direction": r["direction"],
                    "amount": str(r["amount"]),
                    "signed_amount": str(r["signed_amount"]),
                    "currency": r["currency"],
                    "transaction_type": r["transaction_type"],
                    "event_ts": r["event_ts"].isoformat(),
                    "description": r["description"],
                    "payload_hash": r["payload_hash"],
                    "is_late_arrival": r["is_late_arrival"],
                    "source_file": source_file,
                    "source_line": r["source_line"],
                    "file_checksum": checksum,
                    "run_id": run_id,
                    "loaded_at": loaded_at.isoformat(),
                }
                for r in curated
            ]
        )

    @staticmethod
    def _quarantine_frame(rejects: list[Reject], run_id: str, source_file: str, checksum: str) -> pd.DataFrame:
        now = utcnow().isoformat()
        return pd.DataFrame(
            [
                {
                    "quarantine_id": make_quarantine_id(checksum, r.line_no, r.reason, r.event_id),
                    "run_id": run_id,
                    "source_file": source_file,
                    "file_checksum": checksum,
                    "source_line": r.line_no,
                    "event_id": r.event_id,
                    "transaction_id": r.transaction_id,
                    "reason_code": r.reason,
                    "reason_detail": r.detail,
                    "raw_record": r.raw,
                    "quarantined_at": now,
                }
                for r in rejects
            ]
        )
