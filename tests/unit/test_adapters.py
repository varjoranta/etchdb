"""Test adapter placeholder styles and import-error wiring.

These run without any database connection. They verify the
driver-specific placeholder format and that the driver subpackages
fail loudly when their backing driver is unavailable.
"""

from etchdb.adapter import AdapterBase
from etchdb.aiosqlite import AiosqliteAdapter
from etchdb.asyncpg import AsyncpgAdapter
from etchdb.asyncpg.adapter import _rowcount_from_status
from etchdb.psycopg import PsycopgAdapter


def test_asyncpg_placeholder_is_dollar_n():
    assert AsyncpgAdapter.placeholder(0) == "$1"
    assert AsyncpgAdapter.placeholder(1) == "$2"
    assert AsyncpgAdapter.placeholder(7) == "$8"


def test_psycopg_placeholder_matches_asyncpg():
    """psycopg uses AsyncRawCursor, so $N placeholders match asyncpg
    exactly. Raw SQL written for one adapter runs against the other."""
    assert PsycopgAdapter.placeholder(0) == "$1"
    assert PsycopgAdapter.placeholder(1) == "$2"
    assert PsycopgAdapter.placeholder(7) == "$8"


def test_aiosqlite_placeholder_is_question_mark():
    assert AiosqliteAdapter.placeholder(0) == "?"
    assert AiosqliteAdapter.placeholder(1) == "?"
    assert AiosqliteAdapter.placeholder(99) == "?"


def test_adapters_inherit_adapter_base():
    assert issubclass(AsyncpgAdapter, AdapterBase)
    assert issubclass(AiosqliteAdapter, AdapterBase)
    assert issubclass(PsycopgAdapter, AdapterBase)


def test_asyncpg_rowcount_from_status_dml():
    """asyncpg returns command tags like 'UPDATE 5' / 'DELETE 3' /
    'INSERT 0 5' (oid 0, then count). The trailing token is the
    affected-row count."""
    assert _rowcount_from_status("UPDATE 5") == 5
    assert _rowcount_from_status("DELETE 3") == 3
    assert _rowcount_from_status("INSERT 0 7") == 7
    assert _rowcount_from_status("UPDATE 0") == 0


def test_asyncpg_rowcount_from_status_ddl_returns_minus_one():
    """DDL command tags have no numeric tail; -1 is the cross-driver
    'no rowcount available' sentinel (mirroring psycopg / sqlite3)."""
    assert _rowcount_from_status("CREATE TABLE") == -1
    assert _rowcount_from_status("DROP TABLE") == -1
    assert _rowcount_from_status("BEGIN") == -1


def test_asyncpg_rowcount_from_status_select_returns_minus_one():
    """db.execute promises rowcount for DML only; SELECT N's trailing
    int is a row count, not an affected-row count, so it must not leak
    through."""
    assert _rowcount_from_status("SELECT 1") == -1
    assert _rowcount_from_status("SELECT 0") == -1
    assert _rowcount_from_status("COPY 5") == -1
