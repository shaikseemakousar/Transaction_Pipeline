"""Validation rules as small pure functions (easy to unit test).

Two layers:
  1. Row-level  : schema/type/reference checks on a single leg.
  2. Transaction: rules that need all legs of a transaction (balance, currency, ...).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from txn_pipeline.schemas import (
    REQUIRED_ACCOUNT_COLUMNS,
    REQUIRED_TXN_COLUMNS,
    VALID_ACCOUNT_TYPES,
    VALID_DIRECTIONS,
    VALID_TXN_TYPES,
    Reason,
)

AMOUNT_RE = re.compile(r"^\d{1,16}(\.\d{1,2})?$")             # positive, max 2 dp
SIGNED_AMOUNT_RE = re.compile(r"^-?\d{1,16}(\.\d{1,2})?$")
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
ISO_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$"
)
CENT = Decimal("0.01")


class ValidationError(Exception):
    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


# ------------------------------------------------------------------ field parsers
def parse_amount(value: str, *, allow_negative: bool = False, field: str = "amount") -> Decimal:
    pattern = SIGNED_AMOUNT_RE if allow_negative else AMOUNT_RE
    if not pattern.match(value):
        raise ValidationError(Reason.INVALID_AMOUNT, f"{field}={value!r} is not a valid decimal (max 2 dp)")
    amount = Decimal(value).quantize(CENT)
    if not allow_negative and amount <= 0:
        raise ValidationError(Reason.INVALID_AMOUNT, f"{field}={value!r} must be > 0")
    return amount


def parse_timestamp(value: str, *, field: str = "event_ts") -> datetime:
    """Parse a strict ISO-8601 timestamp WITH timezone; return naive UTC datetime."""
    if not ISO_TS_RE.match(value):
        raise ValidationError(
            Reason.INVALID_TIMESTAMP, f"{field}={value!r} is not ISO-8601 with timezone"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError(Reason.INVALID_TIMESTAMP, f"{field}={value!r}: {exc}") from exc
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def parse_currency(value: str, allowed: frozenset[str]) -> str:
    code = value.upper()
    if not CURRENCY_RE.match(code) or code not in allowed:
        raise ValidationError(Reason.INVALID_CURRENCY, f"currency={value!r} not in {sorted(allowed)}")
    return code


def payload_hash(record: dict) -> str:
    """Hash of the business content of a leg (used to tell exact vs conflicting duplicates)."""
    parts = [
        record["event_id"], record["transaction_id"], str(record["leg_seq"]),
        record["account_id"], record["direction"], str(record["amount"]),
        record["currency"], record["transaction_type"], record["event_ts"].isoformat(),
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:32]


# ------------------------------------------------------------------ accounts
@dataclass(frozen=True)
class Account:
    account_id: str
    currency: str
    opened_at: datetime
    allow_overdraft: bool


def validate_account_row(values: dict, allowed_currencies: frozenset[str]) -> dict:
    missing = [c for c in REQUIRED_ACCOUNT_COLUMNS if values.get(c) is None]
    if missing:
        raise ValidationError(Reason.INVALID_ACCOUNT_RECORD, f"missing {missing}")
    account_type = values["account_type"].upper()
    if account_type not in VALID_ACCOUNT_TYPES:
        raise ValidationError(Reason.INVALID_ACCOUNT_RECORD, f"account_type={values['account_type']!r}")
    overdraft = values["allow_overdraft"].lower()
    if overdraft not in ("true", "false"):
        raise ValidationError(Reason.INVALID_ACCOUNT_RECORD, f"allow_overdraft={values['allow_overdraft']!r}")
    try:
        currency = parse_currency(values["currency"], allowed_currencies)
        opening = parse_amount(values["opening_balance"], allow_negative=True, field="opening_balance")
        opened_at = parse_timestamp(values["opened_at"], field="opened_at")
    except ValidationError as exc:
        raise ValidationError(Reason.INVALID_ACCOUNT_RECORD, exc.detail) from exc
    return {
        "account_id": values["account_id"],
        "account_name": values["account_name"],
        "account_type": account_type,
        "currency": currency,
        "opening_balance": opening,
        "allow_overdraft": overdraft == "true",
        "opened_at": opened_at,
    }


# ------------------------------------------------------------------ row level
def validate_txn_row(
    values: dict,
    accounts: dict[str, Account],
    allowed_currencies: frozenset[str],
    run_started_at: datetime,
    future_tolerance_minutes: int,
) -> dict:
    """Validate one leg. Returns a typed, normalised record or raises ValidationError.

    Checks run in a fixed order so each bad row gets one deterministic reason.
    """
    missing = [c for c in REQUIRED_TXN_COLUMNS if values.get(c) is None]
    if missing:
        raise ValidationError(Reason.MISSING_REQUIRED_FIELD, f"missing/blank: {', '.join(missing)}")

    try:
        leg_seq = int(values["leg_seq"])
        if leg_seq < 1:
            raise ValueError
    except ValueError:
        raise ValidationError(Reason.INVALID_LEG_SEQ, f"leg_seq={values['leg_seq']!r} must be an integer >= 1")

    direction = values["direction"].upper()
    if direction not in VALID_DIRECTIONS:
        raise ValidationError(Reason.INVALID_DIRECTION, f"direction={values['direction']!r}")

    txn_type = values["transaction_type"].upper()
    if txn_type not in VALID_TXN_TYPES:
        raise ValidationError(Reason.INVALID_TRANSACTION_TYPE, f"transaction_type={values['transaction_type']!r}")

    amount = parse_amount(values["amount"])
    currency = parse_currency(values["currency"], allowed_currencies)
    event_ts = parse_timestamp(values["event_ts"])

    latest_allowed = run_started_at + timedelta(minutes=future_tolerance_minutes)
    if event_ts > latest_allowed:
        raise ValidationError(Reason.FUTURE_TIMESTAMP, f"event_ts={values['event_ts']} is after run start")

    account = accounts.get(values["account_id"])
    if account is None:
        raise ValidationError(Reason.UNKNOWN_ACCOUNT, f"account_id={values['account_id']!r} not in reference data")
    if account.currency != currency:
        raise ValidationError(
            Reason.CURRENCY_ACCOUNT_MISMATCH,
            f"leg currency {currency} != account {account.account_id} currency {account.currency}",
        )
    if event_ts < account.opened_at:
        raise ValidationError(
            Reason.BEFORE_ACCOUNT_OPEN,
            f"event_ts {event_ts.isoformat()} before account opened_at {account.opened_at.isoformat()}",
        )

    record = {
        "event_id": values["event_id"],
        "transaction_id": values["transaction_id"],
        "leg_seq": leg_seq,
        "account_id": values["account_id"],
        "direction": direction,
        "amount": amount,
        # sign convention: CREDIT increases the account balance, DEBIT decreases it
        "signed_amount": amount if direction == "CREDIT" else -amount,
        "currency": currency,
        "transaction_type": txn_type,
        "event_ts": event_ts,
        "description": values.get("description"),
    }
    record["payload_hash"] = payload_hash(record)
    return record


# ------------------------------------------------------------------ transaction level
def check_transaction(legs: list[dict]) -> tuple[str, str] | None:
    """Return (reason, detail) for the first failing transaction rule, or None if valid."""
    if len(legs) < 2:
        return Reason.SINGLE_LEG, f"transaction has {len(legs)} leg(s); at least 2 required"

    seqs = [leg["leg_seq"] for leg in legs]
    if len(seqs) != len(set(seqs)):
        return Reason.DUPLICATE_LEG_SEQ, f"leg_seq values {sorted(seqs)} are not unique"

    currencies = {leg["currency"] for leg in legs}
    if len(currencies) > 1:
        return Reason.MIXED_CURRENCY, f"legs use {sorted(currencies)}; FX is out of scope"

    timestamps = {leg["event_ts"] for leg in legs}
    if len(timestamps) > 1:
        return Reason.LEG_TIMESTAMP_MISMATCH, f"legs have {len(timestamps)} different event_ts values"

    types = {leg["transaction_type"] for leg in legs}
    if len(types) > 1:
        return Reason.LEG_TYPE_MISMATCH, f"legs have transaction_type {sorted(types)}"

    debits = sum((leg["amount"] for leg in legs if leg["direction"] == "DEBIT"), Decimal("0"))
    credits = sum((leg["amount"] for leg in legs if leg["direction"] == "CREDIT"), Decimal("0"))
    if debits != credits:
        return Reason.UNBALANCED, f"debits {debits} != credits {credits} (diff {debits - credits})"
    return None
