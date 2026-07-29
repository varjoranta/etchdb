"""Test adapter placeholder styles and import-error wiring.

These run without any database connection. They verify the
driver-specific placeholder format and that the driver subpackages
fail loudly when their backing driver is unavailable.
"""

import inspect

import asyncpg

from etchdb import errors
from etchdb.adapter import AdapterBase
from etchdb.aiosqlite import AiosqliteAdapter
from etchdb.asyncpg import AsyncpgAdapter
from etchdb.asyncpg.adapter import _POOL_STATE_MSGS, _map_exception, _rowcount_from_status
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


def test_asyncpg_closed_pool_maps_to_operational_error_with_guidance():
    """A stale adapter is the likeliest cause of a closed-pool error, and
    the raw asyncpg traceback names neither etchdb nor that cause.

    from_pool() captures the pool object, so an adapter cached across a
    close/recreate cycle keeps serving the dead pool. Callers hit this in
    test teardown and on reconnect, and the bare InterfaceError sends them
    reading asyncpg internals instead of their own caching.
    """
    mapped = _map_exception(asyncpg.exceptions.InterfaceError("pool is closed"))
    assert isinstance(mapped, errors.OperationalError)
    assert "pool is closed" in str(mapped)
    assert "from_pool()" in str(mapped)

    mapped = _map_exception(asyncpg.exceptions.InterfaceError("pool is not initialized"))
    assert isinstance(mapped, errors.OperationalError)


def test_asyncpg_other_interface_errors_still_propagate():
    """Only the two pool-state messages are remapped. InterfaceError also
    covers unrelated misuse (released connections, prepared-statement
    errors) that must not be relabelled as a stale-adapter problem."""
    assert _map_exception(asyncpg.exceptions.InterfaceError("cannot perform operation")) is None


def test_asyncpg_pool_state_messages_still_match_asyncpg():
    """Pins the literal strings this mapping depends on.

    _POOL_STATE_MSGS matches Pool._check_init's messages exactly. If an
    asyncpg upgrade rewords them, the mapping would silently stop firing
    and the unhelpful traceback would come back. Fail here instead.
    """
    src = inspect.getsource(asyncpg.pool.Pool._check_init)
    for msg in _POOL_STATE_MSGS:
        assert msg in src, f"asyncpg no longer raises {msg!r} — update _POOL_STATE_MSGS"
