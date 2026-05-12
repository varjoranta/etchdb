"""Integration tests for db.ping() and the `async with db:` context
manager. Both are small but exercise the cross-driver path."""

import pytest

from etchdb import DB, OperationalError


async def test_ping_returns_true_on_healthy_db(db: DB):
    assert await db.ping() is True


async def test_ping_raises_on_closed_db():
    """A closed pool / connection surfaces an exception rather than
    silently returning. Exact type varies by driver -- sqlite3 raises
    ProgrammingError on a closed connection, asyncpg / psycopg raise
    their own InterfaceError / PoolClosed -- so the assertion is
    "any failure" rather than a specific type."""
    db = await DB.from_url("sqlite+aiosqlite:///:memory:")
    await db.close()

    with pytest.raises(Exception):  # noqa: B017  driver-specific shutdown error
        await db.ping()


async def test_async_with_closes_on_exit():
    """`async with db:` calls close() on exit; subsequent calls fail."""
    db = await DB.from_url("sqlite+aiosqlite:///:memory:")
    async with db:
        assert await db.ping() is True
    # After exit, the connection is closed; same any-failure assertion
    # as the closed-db test above.
    with pytest.raises(Exception):  # noqa: B017
        await db.ping()


async def test_async_with_closes_on_exception():
    """Exit-on-exception still closes the DB; verifying via a follow-up
    ping that fails."""
    db = await DB.from_url("sqlite+aiosqlite:///:memory:")

    class _SentinelError(Exception):
        pass

    with pytest.raises(_SentinelError):
        async with db:
            raise _SentinelError

    with pytest.raises(Exception):  # noqa: B017
        await db.ping()


def test_operational_error_is_importable():
    """Sanity: the exception type referenced in ping's docstring is in
    the public surface. Future-proofs against import-path drift."""
    assert OperationalError is not None
