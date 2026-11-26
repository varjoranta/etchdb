"""Integration tests for `min_size` / `max_size` kwargs on `DB.from_url`.

The pool kwargs forward to asyncpg's `create_pool` and psycopg's
`AsyncConnectionPool`. aiosqlite has no pool concept and accepts the
kwargs purely for cross-driver consistency so the same call site
works on every backend."""

import pytest

from etchdb import DB, OperationalError


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


async def test_configure_survives_reuse_of_the_connection(pg_url: str):
    """One connection, used three times. What `configure` set up must
    still be there after the connection has been back to the pool:
    asyncpg resets session state on release, so its callback registers
    a codec, which is the documented use. psycopg keeps a committed
    `SET`."""
    import psycopg

    async def configure(conn):
        if isinstance(conn, psycopg.AsyncConnection):
            await conn.execute("SET application_name = 'etchdb_hook_test'")
            await conn.commit()
        else:
            await conn.set_type_codec(
                "numeric", encoder=str, decoder=lambda v: f"n:{v}", schema="pg_catalog"
            )

    db = await DB.from_url(pg_url, min_size=1, max_size=1, configure=configure)
    try:
        for _ in range(3):
            if pg_url.startswith("postgresql+psycopg"):
                assert await db.fetchval("SHOW application_name") == "etchdb_hook_test"
            else:
                assert await db.fetchval("SELECT 1.5::numeric") == "n:1.5"
            # etchdb's own codec is still in place next to the callback's.
            assert await db.fetchval("SELECT $1::jsonb", {"nested": [1, 2]}) == {"nested": [1, 2]}
    finally:
        await db.close()


async def test_query_after_close_raises_operational_error(pg_url: str):
    """The real drivers, not a constructed exception: a DB used after
    its own close() raises the etchdb error on both."""
    db = await DB.from_url(pg_url, min_size=1, max_size=1)
    await db.close()
    with pytest.raises(OperationalError):
        await db.fetchval("SELECT 1")
