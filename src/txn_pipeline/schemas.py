"""Single source of truth for source schemas, allowed values and reject reason codes.

Source grain: one CSV row = one ledger *leg* (a debit or credit against one account).
A transaction is made of >= 2 legs that share a transaction_id and must balance.
"""

# ---------------------------------------------------------------- source: transactions
REQUIRED_TXN_COLUMNS = (
    "event_id",          # natural key of the leg event, unique across all files
    "transaction_id",    # groups legs into one business transaction
    "leg_seq",           # 1..n position of the leg inside the transaction
    "account_id",        # FK -> accounts.account_id
    "direction",         # DEBIT | CREDIT
    "amount",            # positive decimal, max 2 dp
    "currency",          # ISO-4217 code, must match the account currency
    "transaction_type",  # PAYMENT | TRANSFER | DEPOSIT | WITHDRAWAL | FEE
    "event_ts",          # ISO-8601 with timezone, e.g. 2026-11-01T09:00:00Z
)
OPTIONAL_TXN_COLUMNS = ("description",)

# ---------------------------------------------------------------- source: accounts
REQUIRED_ACCOUNT_COLUMNS = (
    "account_id",
    "account_name",
    "account_type",
    "currency",
    "opening_balance",
    "allow_overdraft",
    "opened_at",
)
OPTIONAL_ACCOUNT_COLUMNS = ()

VALID_DIRECTIONS = frozenset({"DEBIT", "CREDIT"})
VALID_TXN_TYPES = frozenset({"PAYMENT", "TRANSFER", "DEPOSIT", "WITHDRAWAL", "FEE"})
VALID_ACCOUNT_TYPES = frozenset({"CUSTOMER", "MERCHANT", "EXTERNAL", "FEE_INCOME"})


class Reason:
    """Reject / quarantine reason codes. Every quarantined row carries exactly one."""

    # file / row structure
    MALFORMED_ROW = "MALFORMED_ROW"                    # wrong number of fields
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"  # null/blank required value
    # field validation
    INVALID_LEG_SEQ = "INVALID_LEG_SEQ"
    INVALID_DIRECTION = "INVALID_DIRECTION"
    INVALID_TRANSACTION_TYPE = "INVALID_TRANSACTION_TYPE"
    INVALID_AMOUNT = "INVALID_AMOUNT"
    INVALID_CURRENCY = "INVALID_CURRENCY"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    # reference / ordering rules
    UNKNOWN_ACCOUNT = "UNKNOWN_ACCOUNT"                # invalid reference key
    CURRENCY_ACCOUNT_MISMATCH = "CURRENCY_ACCOUNT_MISMATCH"
    FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
    BEFORE_ACCOUNT_OPEN = "BEFORE_ACCOUNT_OPEN"
    # duplicates
    DUPLICATE_EVENT = "DUPLICATE_EVENT"                # exact copy of an event in the same file
    CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"    # same event_id, different content
    # transaction level
    INCOMPLETE_TRANSACTION = "INCOMPLETE_TRANSACTION"  # a sibling leg was rejected
    SINGLE_LEG = "SINGLE_LEG"
    DUPLICATE_LEG_SEQ = "DUPLICATE_LEG_SEQ"
    MIXED_CURRENCY = "MIXED_CURRENCY"
    LEG_TIMESTAMP_MISMATCH = "LEG_TIMESTAMP_MISMATCH"
    LEG_TYPE_MISMATCH = "LEG_TYPE_MISMATCH"
    UNBALANCED = "UNBALANCED"                          # sum(debits) != sum(credits)
    TRANSACTION_ALREADY_POSTED = "TRANSACTION_ALREADY_POSTED"
    # reference data
    INVALID_ACCOUNT_RECORD = "INVALID_ACCOUNT_RECORD"


# Row-level rejects that do NOT invalidate the rest of the transaction:
# an exact duplicate is just a repeated copy; the original copy is still processed.
NON_POISONING_REASONS = frozenset({Reason.DUPLICATE_EVENT})
