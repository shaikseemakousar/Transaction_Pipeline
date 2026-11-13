"""The generator is reproducible, and the pipeline's decisions match its independent labels."""

import csv

import duckdb

from txn_pipeline.config import PipelineConfig
from txn_pipeline.generator import generate
from txn_pipeline.pipeline import Pipeline
from txn_pipeline.reports import run_checks


def _read_all(folder):
    return {p.relative_to(folder).as_posix(): p.read_bytes() for p in sorted(folder.rglob("*.csv"))}


def test_same_seed_gives_identical_files(tmp_path):
    generate(tmp_path / "a", n_rows=500, seed=7)
    generate(tmp_path / "b", n_rows=500, seed=7)
    generate(tmp_path / "c", n_rows=500, seed=8)
    assert _read_all(tmp_path / "a") == _read_all(tmp_path / "b")
    assert _read_all(tmp_path / "a") != _read_all(tmp_path / "c")


def test_pipeline_outcomes_match_generator_labels(tmp_path):
    data = tmp_path / "gen"
    stats = generate(data, n_rows=4000, seed=42, n_files=3, invalid_rate=0.10)
    assert stats["rows"] >= 4000

    cfg = PipelineConfig(input_dir=data / "incoming", database=tmp_path / "w.duckdb",
                         accounts_file=data / "accounts.csv")
    summary = Pipeline(cfg).run()
    assert summary.status == "SUCCESS"

    con = duckdb.connect(str(cfg.database), read_only=True)
    curated = {r[0] for r in con.execute("SELECT transaction_id FROM curated_transactions").fetchall()}
    reasons: dict[str, set] = {}
    for txn, reason in con.execute("SELECT transaction_id, reason_code FROM quarantine").fetchall():
        reasons.setdefault(txn, set()).add(reason)
    late = con.execute("SELECT COUNT(*) FROM curated_entries WHERE is_late_arrival").fetchall()[0][0]
    checks = run_checks(con)
    con.close()

    with (data / "expected" / "expected_outcomes.csv").open(encoding="utf-8") as fh:
        labels = list(csv.DictReader(fh))
    expected_curated = {r["transaction_id"] for r in labels if r["expected_status"] == "CURATED"}
    assert curated == expected_curated
    for r in labels:
        if r["expected_status"] == "QUARANTINED":
            assert r["expected_reason"] in reasons.get(r["transaction_id"], set()), r

    assert late > 0                       # late-arriving data was produced and flagged
    assert sum(f.rows_already_loaded for f in summary.files) > 0  # replays were absorbed
    assert all(c.passed for c in checks), [c for c in checks if not c.passed]
