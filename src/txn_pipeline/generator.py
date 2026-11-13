"""Synthetic, reproducible dataset generator (fixed seed -> identical output).

Produces under <out_dir>:
  accounts.csv                      reference data
  incoming/transactions_NNN.csv     leg-level input files (rows shuffled = out of order)
  expected/expected_outcomes.csv    labels, kept SEPARATE from the input data:
                                    transaction_id, expected_status, expected_reason, injected_anomaly

Realism built in:
  * 2-leg and 3-leg (transfer + fee) transactions across 4 currencies
  * files split by event time, then ~3% of early transactions moved to the last file
    (late-arriving data) and ~1% of clean transactions replayed in a later file
  * a configurable share of transactions gets exactly one labelled anomaly
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from txn_pipeline.schemas import OPTIONAL_TXN_COLUMNS, REQUIRED_TXN_COLUMNS, Reason

HEADER = list(REQUIRED_TXN_COLUMNS) + list(OPTIONAL_TXN_COLUMNS)
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
START = datetime(2026, 1, 1)
WINDOW_SECONDS = 270 * 24 * 3600  # events spread over ~9 months

# anomaly -> expected quarantine reason recorded for the transaction
ANOMALIES = {
    "unbalanced": Reason.UNBALANCED,
    "single_leg": Reason.SINGLE_LEG,
    "unknown_account": Reason.UNKNOWN_ACCOUNT,
    "invalid_currency": Reason.INVALID_CURRENCY,
    "currency_mismatch": Reason.CURRENCY_ACCOUNT_MISMATCH,
    "missing_amount": Reason.MISSING_REQUIRED_FIELD,
    "negative_amount": Reason.INVALID_AMOUNT,
    "bad_timestamp": Reason.INVALID_TIMESTAMP,
    "future_timestamp": Reason.FUTURE_TIMESTAMP,
    "leg_ts_mismatch": Reason.LEG_TIMESTAMP_MISMATCH,
    "malformed_row": Reason.MALFORMED_ROW,
    "invalid_direction": Reason.INVALID_DIRECTION,
}


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_accounts(rng: random.Random, n_customers: int) -> list[dict]:
    accounts = []
    for cur in CURRENCIES:
        accounts.append(dict(account_id=f"EXT-{cur}", account_name=f"External settlement {cur}",
                             account_type="EXTERNAL", currency=cur, opening_balance="0.00",
                             allow_overdraft="true", opened_at="2025-01-01T00:00:00Z"))
        accounts.append(dict(account_id=f"FEE-{cur}", account_name=f"Fee income {cur}",
                             account_type="FEE_INCOME", currency=cur, opening_balance="0.00",
                             allow_overdraft="true", opened_at="2025-01-01T00:00:00Z"))
    for i in range(1, n_customers + 1):
        is_merchant = i % 10 == 0
        opened = datetime(2025, 1, 1) + timedelta(days=rng.randint(0, 300))
        accounts.append(dict(
            account_id=f"{'MER' if is_merchant else 'CUS'}-{i:05d}",
            account_name=f"{'Merchant' if is_merchant else 'Customer'} {i:05d}",
            account_type="MERCHANT" if is_merchant else "CUSTOMER",
            currency=rng.choice(CURRENCIES),
            opening_balance=f"{rng.randint(0, 500000) / 100:.2f}",
            allow_overdraft="true" if rng.random() < 0.2 else "false",
            opened_at=_iso(opened),
        ))
    return accounts


def _legs_for(rng: random.Random, txn_no: int, by_cur: dict[str, list[str]], ts: datetime) -> list[dict]:
    cur = rng.choice(CURRENCIES)
    pool = by_cur[cur]
    amount = Decimal(rng.randint(100, 50000)) / 100
    kind = rng.choices(["PAYMENT", "TRANSFER", "DEPOSIT", "WITHDRAWAL", "TRANSFER_FEE"],
                       weights=[35, 25, 20, 15, 5])[0]
    a, b = rng.sample(pool, 2)
    if kind == "DEPOSIT":
        pairs = [(f"EXT-{cur}", "DEBIT", amount), (a, "CREDIT", amount)]
    elif kind == "WITHDRAWAL":
        pairs = [(a, "DEBIT", amount), (f"EXT-{cur}", "CREDIT", amount)]
    elif kind == "TRANSFER_FEE":
        fee = (amount * Decimal("0.01")).quantize(Decimal("0.01")) or Decimal("0.01")
        pairs = [(a, "DEBIT", amount + fee), (b, "CREDIT", amount), (f"FEE-{cur}", "CREDIT", fee)]
    else:
        pairs = [(a, "DEBIT", amount), (b, "CREDIT", amount)]
    txn_type = "TRANSFER" if kind == "TRANSFER_FEE" else kind
    txn_id = f"TXN-{txn_no:08d}"
    return [
        dict(event_id=f"EVT-{txn_no:08d}-{seq}", transaction_id=txn_id, leg_seq=str(seq), account_id=acc,
             direction=direction, amount=f"{amt:.2f}", currency=cur, transaction_type=txn_type,
             event_ts=_iso(ts), description=f"{txn_type.lower()} {txn_id}")
        for seq, (acc, direction, amt) in enumerate(pairs, start=1)
    ]


def _inject(rng: random.Random, anomaly: str, legs: list[dict], account_currency: dict[str, str]) -> list:
    """Mutate legs in place to carry exactly one anomaly. Returns rows (dicts or raw lists)."""
    target = rng.randrange(len(legs))
    leg = legs[target]
    if anomaly == "unbalanced":
        credit = next(lg for lg in legs if lg["direction"] == "CREDIT")
        credit["amount"] = f"{Decimal(credit['amount']) + Decimal('0.01'):.2f}"
    elif anomaly == "single_leg":
        del legs[1:]
    elif anomaly == "unknown_account":
        leg["account_id"] = "CUS-99999"
    elif anomaly == "invalid_currency":
        leg["currency"] = "XXX"
    elif anomaly == "currency_mismatch":
        leg["currency"] = next(c for c in CURRENCIES if c != account_currency[leg["account_id"]])
    elif anomaly == "missing_amount":
        leg["amount"] = ""
    elif anomaly == "negative_amount":
        leg["amount"] = f"-{leg['amount']}"
    elif anomaly == "bad_timestamp":
        leg["event_ts"] = leg["event_ts"].replace("T", " ").replace("Z", "")
    elif anomaly == "future_timestamp":
        for lg in legs:
            lg["event_ts"] = "2031-01-01T00:00:00Z"
    elif anomaly == "leg_ts_mismatch":
        legs[-1]["event_ts"] = _iso(datetime.strptime(legs[-1]["event_ts"], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=1))
    elif anomaly == "invalid_direction":
        leg["direction"] = "SIDEWAYS"
    rows: list = [dict(lg) for lg in legs]
    if anomaly == "malformed_row":
        rows[target] = [rows[target][c] for c in HEADER] + ["unexpected extra field"]
    return rows


def generate(out_dir: Path, *, n_rows: int = 100_000, seed: int = 42, n_files: int = 3,
             invalid_rate: float = 0.05, n_customers: int | None = None) -> dict:
    rng = random.Random(seed)
    out_dir = Path(out_dir)
    (out_dir / "incoming").mkdir(parents=True, exist_ok=True)
    (out_dir / "expected").mkdir(parents=True, exist_ok=True)
    for old in (out_dir / "incoming").glob("transactions_*.csv"):
        old.unlink()

    accounts = _make_accounts(rng, n_customers or max(20, n_rows // 200))
    account_currency = {a["account_id"]: a["currency"] for a in accounts}
    latest_open = max(datetime.strptime(a["opened_at"], "%Y-%m-%dT%H:%M:%SZ") for a in accounts)
    by_cur: dict[str, list[str]] = {c: [] for c in CURRENCIES}
    for a in accounts:
        if a["account_type"] in ("CUSTOMER", "MERCHANT"):
            by_cur[a["currency"]].append(a["account_id"])
    for cur, pool in by_cur.items():  # guarantee >= 2 accounts per currency
        while len(pool) < 2:
            acc_id = f"CUS-X{cur}{len(pool)}"
            accounts.append(dict(account_id=acc_id, account_name=f"Customer {acc_id}", account_type="CUSTOMER",
                                 currency=cur, opening_balance="100.00", allow_overdraft="false",
                                 opened_at="2025-01-01T00:00:00Z"))
            account_currency[acc_id] = cur
            pool.append(acc_id)

    # events start after every account is open, so BEFORE_ACCOUNT_OPEN only appears when injected
    start = max(START, latest_open + timedelta(days=1))
    txns: list[tuple[datetime, str, list, str, str, str]] = []  # (ts, txn_id, rows, status, reason, anomaly)
    leg_count, txn_no = 0, 0
    anomaly_names = list(ANOMALIES)
    while leg_count < n_rows:
        txn_no += 1
        ts = start + timedelta(seconds=rng.randrange(WINDOW_SECONDS))
        legs = _legs_for(rng, txn_no, by_cur, ts)
        if rng.random() < invalid_rate:
            anomaly = rng.choice(anomaly_names)
            rows = _inject(rng, anomaly, legs, account_currency)
            txns.append((ts, legs[0]["transaction_id"], rows, "QUARANTINED", ANOMALIES[anomaly], anomaly))
        else:
            rows = [dict(lg) for lg in legs]
            if rng.random() < 0.01:  # exact duplicate row inside the same file
                rows.append(dict(rng.choice(legs)))
                txns.append((ts, legs[0]["transaction_id"], rows, "CURATED", "", "duplicate_event"))
            else:
                txns.append((ts, legs[0]["transaction_id"], rows, "CURATED", "", ""))
        leg_count += len(rows)

    # split into files by event time, then create late arrivals + replays
    txns.sort(key=lambda t: t[0])
    chunk = -(-len(txns) // n_files)
    files: list[list] = [txns[i * chunk:(i + 1) * chunk] for i in range(n_files)]
    if n_files > 1:
        first = files[0]
        late = [t for t in first if rng.random() < 0.03]
        late_ids = {t[1] for t in late}
        files[0] = [t for t in first if t[1] not in late_ids]
        files[-1].extend(late)
        replays = [t for t in files[0] if t[3] == "CURATED" and t[5] == "" and rng.random() < 0.01]
        files[1].extend((t[0], t[1], [dict(r) for r in t[2]], t[3], t[4], "replayed_in_later_file") for t in replays)

    total_rows = 0
    for i, file_txns in enumerate(files, start=1):
        rows = [r for t in file_txns for r in t[2]]
        rng.shuffle(rows)  # out-of-order input
        total_rows += len(rows)
        with (out_dir / "incoming" / f"transactions_{i:03d}.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(HEADER)
            for r in rows:
                w.writerow(r if isinstance(r, list) else [r[c] for c in HEADER])

    with (out_dir / "accounts.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(accounts[0]))
        w.writeheader()
        w.writerows(accounts)

    labels_path = out_dir / "expected" / "expected_outcomes.csv"
    seen: set[str] = set()
    with labels_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["transaction_id", "expected_status", "expected_reason", "injected_anomaly"])
        for ts, txn_id, _rows, status, reason, anomaly in sorted(txns, key=lambda t: t[1]):
            if txn_id not in seen:
                seen.add(txn_id)
                w.writerow([txn_id, status, reason, anomaly])

    return {"rows": total_rows, "transactions": len(txns), "files": n_files,
            "labels_file": str(labels_path), "seed": seed}
