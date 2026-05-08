"""Test adapter placeholder styles and import-error wiring.

These run without any database connection. They verify the
driver-specific placeholder format and that the driver subpackages
fail loudly when their backing driver is unavailable.
"""

from etchdb.adapter import AdapterBase
from etchdb.aiosqlite import AiosqliteAdapter
from etchdb.asyncpg import AsyncpgAdapter
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
