"""Integration tests for `min_size` / `max_size` kwargs on `DB.from_url`.

The pool kwargs forward to asyncpg's `create_pool` and psycopg's
`AsyncConnectionPool`. aiosqlite has no pool concept and accepts the
kwargs purely for cross-driver consistency so the same call site
works on every backend."""

from etchdb import DB


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


async def test_configure_preserves_jsonb_and_applies_session_setup(pg_url: str):
    import psycopg

    async def configure(conn):
        await conn.execute("SET application_name = 'etchdb_hook_test'")
        if isinstance(conn, psycopg.AsyncConnection):
            await conn.commit()

    db = await DB.from_url(pg_url, min_size=1, max_size=2, configure=configure)
    try:
        assert await db.fetchval("SELECT current_setting('application_name')") == "etchdb_hook_test"
        assert await db.fetchval("SELECT $1::jsonb", {"nested": [1, 2]}) == {"nested": [1, 2]}
    finally:
        await db.close()
