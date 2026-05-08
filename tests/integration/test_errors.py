"""Integration tests for the typed exception family.

Drives a constraint violation and a missing-table query on every
parametrized backend to confirm both Postgres adapters and aiosqlite
re-raise as the same etchdb.* family. The original driver exception
must remain reachable as `__cause__` so debuggers still see the
underlying error.
"""

import pytest

from etchdb import DB, IntegrityError, UndefinedTableError
from tests._models import User


async def test_unique_violation_raises_integrity_error(db: DB):
    await db.insert(User(id=1, name="Alice"))

    with pytest.raises(IntegrityError) as exc_info:
        await db.insert(User(id=1, name="Duplicate"))

    # The original driver exception is preserved.
    assert exc_info.value.__cause__ is not None


async def test_missing_table_raises_undefined_table_error(db: DB):
    with pytest.raises(UndefinedTableError):
        await db.fetch("SELECT * FROM nonexistent_etchdb_table_xxxxx")


async def test_typed_errors_inherit_etchdb_error(db: DB):
    """A single `except EtchdbError` catches every member of the family."""
    from etchdb import EtchdbError

    await db.insert(User(id=1, name="Alice"))

    with pytest.raises(EtchdbError):
        await db.insert(User(id=1, name="Duplicate"))

    with pytest.raises(EtchdbError):
        await db.fetch("SELECT * FROM nonexistent_etchdb_table_xxxxx")
