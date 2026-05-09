"""Forward-only file-based migrations.

A migration is a `.sql` file in a directory of your choosing; the
runner applies pending files in filename order, tracking applied
ones in `_etchdb_migrations(filename, checksum, applied_at)` and
refusing to operate on drift or disappearance.

Public surface lives on DB: `db.migrate(directory)` and
`db.migration_status(directory)`. Conventions (filename ordering,
transaction model, the `-- etchdb:no-transaction` opt-out, SQLite's
`executescript` caveat, recovery from inconsistent state) are in the
README and the per-method docstrings.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from etchdb.db import DB


_TRACKING_TABLE = "_etchdb_migrations"
_TRACKING_TABLE_DDL = (
    f"CREATE TABLE IF NOT EXISTS {_TRACKING_TABLE} ("
    "filename TEXT PRIMARY KEY, "
    "checksum TEXT NOT NULL, "
    "applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
    ")"
)

_NO_TRANSACTION_MARKER = "-- etchdb:no-transaction"

# Strip dollar-quoted blocks, string literals, and line comments
# before the transaction-keyword check, so a `BEGIN` inside PL/pgSQL
# or a `COMMIT` inside a string literal doesn't trip the regex.
_DOLLAR_QUOTED_RE = re.compile(r"\$([A-Za-z_]*)\$.*?\$\1\$", re.DOTALL)
_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_TX_KEYWORD_RE = re.compile(
    r"\b(BEGIN|COMMIT|ROLLBACK|START\s+TRANSACTION|END\s+TRANSACTION)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class MigrationStatus:
    """Snapshot of migration state.

    `pending` and `applied` are filename lists in apply order. `drifted`
    and `missing` indicate inconsistent state and must be empty before
    `db.migrate(...)` will run; `is_consistent` is the convenience
    check.
    """

    pending: list[str] = field(default_factory=list)
    applied: list[str] = field(default_factory=list)
    drifted: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)

    @property
    def is_consistent(self) -> bool:
        return not self.drifted and not self.missing


def _file_checksum(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _strip_for_tx_check(sql: str) -> str:
    s = _DOLLAR_QUOTED_RE.sub("", sql)
    s = _STRING_LITERAL_RE.sub("", s)
    s = _LINE_COMMENT_RE.sub("", s)
    return s


def _check_no_explicit_transaction(filename: str, content: str) -> None:
    stripped = _strip_for_tx_check(content)
    m = _TX_KEYWORD_RE.search(stripped)
    if m:
        raise ValueError(
            f"Migration {filename!r} contains explicit transaction control "
            f"({m.group(1).upper()!r}). etchdb wraps each migration in its own "
            f"transaction; remove BEGIN / COMMIT / ROLLBACK from the file. For "
            f"non-transactional DDL (e.g. CREATE INDEX CONCURRENTLY), put "
            f"`{_NO_TRANSACTION_MARKER}` on the first non-blank line."
        )


def _has_no_transaction_marker(content: str) -> bool:
    for line in content.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped == _NO_TRANSACTION_MARKER
    return False


def _load_migration_files(directory: str | Path) -> list[tuple[str, str, str]]:
    """Return [(filename, content, checksum), ...] sorted by filename."""
    path = Path(directory)
    if not path.is_dir():
        raise ValueError(f"Migrations directory not found: {str(path)!r}")
    files = sorted(p for p in path.iterdir() if p.is_file() and p.suffix == ".sql")
    out: list[tuple[str, str, str]] = []
    for p in files:
        content = p.read_text(encoding="utf-8")
        out.append((p.name, content, _file_checksum(content)))
    return out


async def _ensure_tracking_table(db: DB) -> None:
    await db.execute(_TRACKING_TABLE_DDL)


async def status(db: DB, directory: str | Path) -> MigrationStatus:
    """Compare the migrations directory against the tracking table.

    Creates the tracking table on first call (idempotent).
    """
    files = _load_migration_files(directory)
    file_index = {fn: cs for fn, _, cs in files}

    await _ensure_tracking_table(db)
    applied_rows = await db.fetch(f"SELECT filename, checksum FROM {_TRACKING_TABLE}")
    applied_index = {r["filename"]: r["checksum"] for r in applied_rows}

    pending: list[str] = []
    applied: list[str] = []
    drifted: list[str] = []

    for fn in file_index:
        if fn in applied_index:
            if applied_index[fn] != file_index[fn]:
                drifted.append(fn)
            else:
                applied.append(fn)
        else:
            pending.append(fn)

    missing = sorted(fn for fn in applied_index if fn not in file_index)

    return MigrationStatus(
        pending=pending,
        applied=applied,
        drifted=sorted(drifted),
        missing=missing,
    )


async def migrate(db: DB, directory: str | Path) -> int:
    """Apply every pending migration. Returns the count applied.

    Raises `RuntimeError` when state is inconsistent (drift or missing
    files), naming the offending filenames and the recovery command.
    """
    s = await status(db, directory)
    if not s.is_consistent:
        parts: list[str] = []
        if s.drifted:
            parts.append(
                f"file content changed since apply: {s.drifted}. "
                f"To re-apply, delete the row first: "
                f"DELETE FROM {_TRACKING_TABLE} WHERE filename = '<name>'."
            )
        if s.missing:
            parts.append(
                f"applied migrations no longer in directory: {s.missing}. "
                f"Restore the file(s), or DELETE FROM {_TRACKING_TABLE} "
                f"WHERE filename IN (...) if intentional."
            )
        raise RuntimeError("Migration state is inconsistent; refusing to apply. " + " ".join(parts))

    if not s.pending:
        return 0

    files_by_name = {fn: (content, cs) for fn, content, cs in _load_migration_files(directory)}
    ph = db._adapter.placeholder
    insert_sql = f"INSERT INTO {_TRACKING_TABLE} (filename, checksum) VALUES ({ph(0)}, {ph(1)})"

    applied = 0
    for fn in s.pending:
        content, checksum = files_by_name[fn]
        _check_no_explicit_transaction(fn, content)
        no_tx = _has_no_transaction_marker(content)

        if no_tx:
            await db._adapter.execute_script(content)
            await db.execute(insert_sql, fn, checksum)
        else:
            async with db.transaction() as tx:
                await tx._adapter.execute_script(content)
                await tx.execute(insert_sql, fn, checksum)
        applied += 1

    return applied
