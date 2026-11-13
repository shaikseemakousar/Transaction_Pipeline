"""Ingestion: read a CSV file defensively.

Nothing here decides business validity. The reader only:
  * computes a content checksum (used for idempotent re-runs),
  * checks the header against the expected schema (schema-drift detection),
  * splits rows into well-formed rows and malformed rows (wrong field count),
  * keeps the physical line number of every row so rejects stay traceable.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class RawRow:
    line_no: int
    raw: str
    values: dict[str, str | None] | None = None  # None for malformed rows
    detail: str | None = None                      # why the row is malformed
    event_id_hint: str | None = None               # best-effort ids of a malformed row,
    transaction_id_hint: str | None = None         # used only for traceability/linking


@dataclass
class FileReadResult:
    path: Path
    checksum: str = ""
    header: list[str] = field(default_factory=list)
    rows: list[RawRow] = field(default_factory=list)
    malformed: list[RawRow] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def rows_read(self) -> int:
        return len(self.rows) + len(self.malformed)

    @property
    def is_empty(self) -> bool:
        return self.rows_read == 0


def file_checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _clean(value: str) -> str | None:
    value = value.strip()
    return value if value else None


def read_csv_file(
    path: Path, required: tuple[str, ...], optional: tuple[str, ...] = ()
) -> FileReadResult:
    result = FileReadResult(path=path)
    data = path.read_bytes()
    result.checksum = file_checksum(data)

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        result.error = f"FILE_UNREADABLE: not valid UTF-8 ({exc})"
        return result

    if not text.strip():  # zero-byte or whitespace-only file = empty batch
        return result

    reader = csv.reader(io.StringIO(text, newline=""))
    header_fields = next(reader, None)
    if header_fields is None:
        return result
    header = [h.strip().lower() for h in header_fields]
    result.header = header
    result.missing_columns = [c for c in required if c not in header]
    known = set(required) | set(optional)
    result.extra_columns = [c for c in header if c not in known]
    if result.missing_columns:
        return result  # schema mismatch: the caller fails the whole file

    idx_event = header.index("event_id") if "event_id" in header else None
    idx_txn = header.index("transaction_id") if "transaction_id" in header else None

    for fields in reader:
        line_no = reader.line_num
        if not fields or all(not f.strip() for f in fields):
            continue  # blank line
        raw = ",".join(fields)
        if len(fields) != len(header):
            result.malformed.append(
                RawRow(
                    line_no=line_no,
                    raw=raw,
                    detail=f"expected {len(header)} fields, got {len(fields)}",
                    event_id_hint=_clean(fields[idx_event]) if idx_event is not None and idx_event < len(fields) else None,
                    transaction_id_hint=_clean(fields[idx_txn]) if idx_txn is not None and idx_txn < len(fields) else None,
                )
            )
            continue
        values = {col: _clean(val) for col, val in zip(header, fields) if col in known}
        result.rows.append(RawRow(line_no=line_no, raw=raw, values=values))
    return result
