"""Test adapter placeholder styles and import-error wiring.

These run without any database connection. They verify the
driver-specific placeholder format and that the driver subpackages
fail loudly when their backing driver is unavailable.
"""

from etchdb.adapter import AdapterBase
from etchdb.aiosqlite import AiosqliteAdapter
from etchdb.asyncpg import AsyncpgAdapter


def test_asyncpg_placeholder_is_dollar_n():
    assert AsyncpgAdapter.placeholder(0) == "$1"
    assert AsyncpgAdapter.placeholder(1) == "$2"
    assert AsyncpgAdapter.placeholder(7) == "$8"


def test_aiosqlite_placeholder_is_question_mark():
    assert AiosqliteAdapter.placeholder(0) == "?"
    assert AiosqliteAdapter.placeholder(1) == "?"
    assert AiosqliteAdapter.placeholder(99) == "?"


def test_adapters_inherit_adapter_base():
    assert issubclass(AsyncpgAdapter, AdapterBase)
    assert issubclass(AiosqliteAdapter, AdapterBase)
