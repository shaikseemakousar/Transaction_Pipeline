"""Pipeline configuration: TOML file + CLI overrides -> immutable dataclass."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path

DEFAULT_CONFIG_PATH = Path("config/pipeline.toml")


@dataclass(frozen=True)
class PipelineConfig:
    input_dir: Path
    database: Path
    accounts_file: Path | None = None
    reports_dir: Path = Path("reports")
    log_dir: Path = Path("logs")
    file_pattern: str = "transactions_*.csv"
    stop_on_error: bool = False
    allowed_currencies: frozenset[str] = field(
        default_factory=lambda: frozenset({"USD", "EUR", "GBP", "INR"})
    )
    future_tolerance_minutes: int = 5


def load_config(path: Path | None = None, **overrides) -> PipelineConfig:
    """Load the TOML config (if it exists) and apply non-None keyword overrides."""
    path = path or DEFAULT_CONFIG_PATH
    raw: dict = {}
    if path.exists():
        with path.open("rb") as fh:
            raw = tomllib.load(fh)

    paths = raw.get("paths", {})
    ingest = raw.get("ingest", {})
    validation = raw.get("validation", {})

    accounts = paths.get("accounts_file")
    cfg = PipelineConfig(
        input_dir=Path(paths.get("input_dir", "data/sample/incoming")),
        database=Path(paths.get("database", "warehouse/pipeline.duckdb")),
        accounts_file=Path(accounts) if accounts else None,
        reports_dir=Path(paths.get("reports_dir", "reports")),
        log_dir=Path(paths.get("log_dir", "logs")),
        file_pattern=ingest.get("file_pattern", "transactions_*.csv"),
        stop_on_error=bool(ingest.get("stop_on_error", False)),
        allowed_currencies=frozenset(
            c.upper() for c in validation.get("allowed_currencies", ["USD", "EUR", "GBP", "INR"])
        ),
        future_tolerance_minutes=int(validation.get("future_tolerance_minutes", 5)),
    )

    clean = {k: v for k, v in overrides.items() if v is not None}
    for key in ("input_dir", "database", "accounts_file", "reports_dir", "log_dir"):
        if key in clean:
            clean[key] = Path(clean[key])
    return replace(cfg, **clean)
