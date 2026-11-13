"""Shared test helpers: an isolated pipeline environment in a temp folder."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from txn_pipeline.config import PipelineConfig
from txn_pipeline.pipeline import Pipeline

HEADER = "event_id,transaction_id,leg_seq,account_id,direction,amount,currency,transaction_type,event_ts,description"

ACCOUNTS_CSV = """account_id,account_name,account_type,currency,opening_balance,allow_overdraft,opened_at
A1,Account One,CUSTOMER,USD,100.00,false,2025-01-01T00:00:00Z
A2,Account Two,CUSTOMER,USD,0.00,false,2025-01-01T00:00:00Z
E1,Euro Account,CUSTOMER,EUR,0.00,true,2025-01-01T00:00:00Z
EXT-USD,External USD,EXTERNAL,USD,0.00,true,2025-01-01T00:00:00Z
NEW1,Opened Late,CUSTOMER,USD,0.00,false,2026-11-01T00:00:00Z
"""

# fixed "now" so the future-timestamp rule is deterministic
NOW = datetime(2026, 11, 14, 12, 0, 0)


def leg(event_id, txn, seq, account, direction, amount, currency="USD",
        ttype="TRANSFER", ts="2026-11-01T10:00:00Z", desc="test"):
    return f"{event_id},{txn},{seq},{account},{direction},{amount},{currency},{ttype},{ts},{desc}"


def transfer(n: int, src="A1", dst="A2", amount="10.00", ts="2026-11-01T10:00:00Z") -> list[str]:
    """A valid, balanced 2-leg transfer with ids E<n>-1/E<n>-2 and T<n>."""
    return [
        leg(f"E{n}-1", f"T{n}", 1, src, "DEBIT", amount, ts=ts),
        leg(f"E{n}-2", f"T{n}", 2, dst, "CREDIT", amount, ts=ts),
    ]


class Env:
    def __init__(self, tmp_path: Path):
        self.root = tmp_path
        self.incoming = tmp_path / "incoming"
        self.incoming.mkdir()
        accounts = tmp_path / "accounts.csv"
        accounts.write_text(ACCOUNTS_CSV, encoding="utf-8")
        self.cfg = PipelineConfig(
            input_dir=self.incoming,
            database=tmp_path / "warehouse.duckdb",
            accounts_file=accounts,
            reports_dir=tmp_path / "reports",
            log_dir=tmp_path / "logs",
        )

    def write(self, name: str, lines: list[str], header: str = HEADER) -> Path:
        path = self.incoming / name
        path.write_text(header + "\n" + "".join(line + "\n" for line in lines), encoding="utf-8")
        return path

    def run(self, *, force: bool = False, fault_hook=None, now=NOW):
        return Pipeline(self.cfg, now=now, fault_hook=fault_hook).run(force=force)

    def query(self, sql: str, params=None) -> list[tuple]:
        con = duckdb.connect(str(self.cfg.database), read_only=True)
        try:
            return con.execute(sql, params or []).fetchall()
        finally:
            con.close()

    def scalar(self, sql: str, params=None):
        return self.query(sql, params)[0][0]

    def curated_txns(self) -> set[str]:
        return {r[0] for r in self.query("SELECT transaction_id FROM curated_transactions")}

    def reasons(self, txn_id: str) -> set[str]:
        return {r[0] for r in self.query(
            "SELECT reason_code FROM quarantine WHERE transaction_id = ?", [txn_id])}


@pytest.fixture
def env(tmp_path) -> Env:
    return Env(tmp_path)
