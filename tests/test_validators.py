"""Unit tests for the pure validation functions."""

from datetime import datetime
from decimal import Decimal

import pytest

from txn_pipeline.schemas import Reason
from txn_pipeline.validators import (
    Account,
    ValidationError,
    check_transaction,
    parse_amount,
    parse_timestamp,
    validate_txn_row,
)

NOW = datetime(2026, 11, 14, 12, 0, 0)
ACCOUNTS = {
    "A1": Account("A1", "USD", datetime(2025, 1, 1), False),
    "A2": Account("A2", "USD", datetime(2025, 1, 1), False),
    "NEW": Account("NEW", "USD", datetime(2026, 11, 1), False),
}
CURRENCIES = frozenset({"USD", "EUR"})


def row(**overrides):
    base = dict(event_id="E1", transaction_id="T1", leg_seq="1", account_id="A1", direction="DEBIT",
                amount="10.00", currency="USD", transaction_type="TRANSFER",
                event_ts="2026-11-01T10:00:00Z", description=None)
    base.update(overrides)
    return base


def validate(**overrides):
    return validate_txn_row(row(**overrides), ACCOUNTS, CURRENCIES, NOW, 5)


# --------------------------------------------------------------------- field parsers
@pytest.mark.parametrize("value,expected", [("10", "10.00"), ("10.5", "10.50"), ("0.01", "0.01"), ("123456.78", "123456.78")])
def test_parse_amount_valid(value, expected):
    assert parse_amount(value) == Decimal(expected)


@pytest.mark.parametrize("value", ["0", "0.00", "-1.00", "10.555", "abc", "1e5", "1,000.00", " "])
def test_parse_amount_invalid(value):
    with pytest.raises(ValidationError) as exc:
        parse_amount(value)
    assert exc.value.reason == Reason.INVALID_AMOUNT


def test_parse_timestamp_normalises_offsets_to_utc():
    assert parse_timestamp("2026-11-01T10:15:00+01:00") == datetime(2026, 11, 1, 9, 15)
    assert parse_timestamp("2026-11-01T09:15:00Z") == datetime(2026, 11, 1, 9, 15)
    assert parse_timestamp("2026-11-01T09:15:00.123Z") == datetime(2026, 11, 1, 9, 15, 0, 123000)


@pytest.mark.parametrize("value", ["2026-11-01T10:00:00", "2026-11-01", "11/01/2026 10:00",
                                   "2026-02-30T10:00:00Z", "2026-11-01 10:00:00Z"])
def test_parse_timestamp_rejects_non_iso_or_missing_timezone(value):
    with pytest.raises(ValidationError) as exc:
        parse_timestamp(value)
    assert exc.value.reason == Reason.INVALID_TIMESTAMP


# --------------------------------------------------------------------- row validation
def test_valid_row_is_typed_and_normalised():
    rec = validate(direction="debit", currency="usd", transaction_type="transfer")
    assert rec["direction"] == "DEBIT" and rec["currency"] == "USD"
    assert rec["amount"] == Decimal("10.00") and rec["signed_amount"] == Decimal("-10.00")
    assert rec["leg_seq"] == 1 and rec["event_ts"] == datetime(2026, 11, 1, 10)


@pytest.mark.parametrize("overrides,reason", [
    ({"amount": None}, Reason.MISSING_REQUIRED_FIELD),
    ({"account_id": None}, Reason.MISSING_REQUIRED_FIELD),
    ({"leg_seq": "0"}, Reason.INVALID_LEG_SEQ),
    ({"leg_seq": "one"}, Reason.INVALID_LEG_SEQ),
    ({"direction": "SIDEWAYS"}, Reason.INVALID_DIRECTION),
    ({"transaction_type": "GIFT"}, Reason.INVALID_TRANSACTION_TYPE),
    ({"amount": "-5"}, Reason.INVALID_AMOUNT),
    ({"currency": "XYZ"}, Reason.INVALID_CURRENCY),
    ({"event_ts": "yesterday"}, Reason.INVALID_TIMESTAMP),
    ({"event_ts": "2030-01-01T00:00:00Z"}, Reason.FUTURE_TIMESTAMP),
    ({"account_id": "NOPE"}, Reason.UNKNOWN_ACCOUNT),
    ({"currency": "EUR"}, Reason.CURRENCY_ACCOUNT_MISMATCH),
    ({"account_id": "NEW", "event_ts": "2026-10-01T00:00:00Z"}, Reason.BEFORE_ACCOUNT_OPEN),
])
def test_row_rules(overrides, reason):
    with pytest.raises(ValidationError) as exc:
        validate(**overrides)
    assert exc.value.reason == reason


def test_future_tolerance_allows_small_clock_skew():
    validate(event_ts="2026-11-14T12:04:00Z")  # 4 minutes ahead, tolerance is 5
    with pytest.raises(ValidationError):
        validate(event_ts="2026-11-14T12:06:00Z")


def test_payload_hash_ignores_description_but_not_amount():
    a = validate(description="x")
    b = validate(description="y")
    c = validate(amount="10.01")
    assert a["payload_hash"] == b["payload_hash"] != c["payload_hash"]


# --------------------------------------------------------------------- transaction rules
def legs(*specs, ts="2026-11-01T10:00:00Z", ttype="TRANSFER"):
    out = []
    for i, (acct, direction, amount, *cur) in enumerate(specs, start=1):
        out.append(validate(event_id=f"E{i}", leg_seq=str(i), account_id=acct, direction=direction,
                            amount=amount, event_ts=ts, transaction_type=ttype))
        if cur:
            out[-1]["currency"] = cur[0]
    return out


def test_balanced_two_and_three_leg_transactions_pass():
    assert check_transaction(legs(("A1", "DEBIT", "10.00"), ("A2", "CREDIT", "10.00"))) is None
    assert check_transaction(legs(("A1", "DEBIT", "10.50"), ("A2", "CREDIT", "10.00"), ("A2", "CREDIT", "0.50"))) is None


def test_unbalanced_detected_to_the_cent():
    reason, detail = check_transaction(legs(("A1", "DEBIT", "10.00"), ("A2", "CREDIT", "9.99")))
    assert reason == Reason.UNBALANCED and "0.01" in detail


def test_single_leg_and_all_same_direction():
    assert check_transaction(legs(("A1", "DEBIT", "10.00")))[0] == Reason.SINGLE_LEG
    assert check_transaction(legs(("A1", "DEBIT", "5.00"), ("A2", "DEBIT", "5.00")))[0] == Reason.UNBALANCED


def test_mixed_currency_and_leg_mismatches():
    mixed = legs(("A1", "DEBIT", "10.00"), ("A2", "CREDIT", "10.00", "EUR"))
    assert check_transaction(mixed)[0] == Reason.MIXED_CURRENCY

    ts_mismatch = legs(("A1", "DEBIT", "10.00"), ("A2", "CREDIT", "10.00"))
    ts_mismatch[1]["event_ts"] = datetime(2026, 11, 1, 11)
    assert check_transaction(ts_mismatch)[0] == Reason.LEG_TIMESTAMP_MISMATCH

    type_mismatch = legs(("A1", "DEBIT", "10.00"), ("A2", "CREDIT", "10.00"))
    type_mismatch[1]["transaction_type"] = "PAYMENT"
    assert check_transaction(type_mismatch)[0] == Reason.LEG_TYPE_MISMATCH

    dup_seq = legs(("A1", "DEBIT", "10.00"), ("A2", "CREDIT", "10.00"))
    dup_seq[1]["leg_seq"] = 1
    assert check_transaction(dup_seq)[0] == Reason.DUPLICATE_LEG_SEQ
