"""Test URL-scheme dispatch and lazy driver-import behavior on DB.from_url.

These tests don't open a real connection; they exercise the dispatch
logic and error messages.
"""

import pytest

from etchdb import DB


async def test_from_url_unsupported_scheme_raises():
    with pytest.raises(ValueError, match="Unsupported URL scheme"):
        await DB.from_url("mysql://x@y/z")


async def test_from_url_psycopg_scheme_not_yet_implemented():
    with pytest.raises(NotImplementedError, match="psycopg"):
        await DB.from_url("postgresql+psycopg://x@y/z")


@pytest.mark.parametrize("url", ["sqlite:///:memory:", "sqlite+aiosqlite:///:memory:"])
async def test_from_url_sqlite_schemes_work(url):
    db = await DB.from_url(url)
    try:
        val = await db.fetchval("SELECT 1")
        assert val == 1
    finally:
        await db.close()
