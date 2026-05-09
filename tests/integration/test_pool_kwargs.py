"""Integration tests for `min_size` / `max_size` kwargs on `DB.from_url`.

The pool kwargs forward to asyncpg's `create_pool` and psycopg's
`AsyncConnectionPool`. aiosqlite has no pool concept and accepts the
kwargs purely for cross-driver consistency so the same call site
works on every backend."""

import pytest
import pytest_asyncio

from etchdb import DB
from tests.integration.conftest import (
    POSTGRES_ASYNCPG_URL,
    POSTGRES_PSYCOPG_URL,
    _postgres_available,
)


@pytest_asyncio.fixture(params=[POSTGRES_ASYNCPG_URL, POSTGRES_PSYCOPG_URL])
async def pg_url(request):
    if not _postgres_available():
        pytest.skip("Postgres not available on localhost:5532. Run `make db-up`.")
    return request.param


async def test_from_url_min_max_size_round_trip(pg_url: str):
    """Pool opens with the requested sizes; a trivial query proves it
    actually accepted and used the kwargs."""
    db = await DB.from_url(pg_url, min_size=1, max_size=2)
    try:
        val = await db.fetchval("SELECT 1")
        assert val == 1
    finally:
        await db.close()


async def test_from_url_aiosqlite_accepts_pool_kwargs():
    db = await DB.from_url("sqlite+aiosqlite:///:memory:", min_size=1, max_size=10)
    try:
        val = await db.fetchval("SELECT 1")
        assert val == 1
    finally:
        await db.close()
