"""Integration tests for db.ping() and the `async with db:` context
manager. Both are small but exercise the cross-driver path."""

import pytest

from etchdb import DB


async def test_ping_returns_true_on_healthy_db(db: DB):
    assert await db.ping() is True


async def test_ping_returns_false_on_closed_db():
    """A closed pool / connection makes ping return False rather than
    raising. Callers can do `if await db.ping(): ...` in healthchecks
    without wrapping in try/except."""
    db = await DB.from_url("sqlite+aiosqlite:///:memory:")
    await db.close()
    assert await db.ping() is False


async def test_async_with_closes_on_exit():
    """`async with db:` calls close() on exit; subsequent ping is False."""
    db = await DB.from_url("sqlite+aiosqlite:///:memory:")
    async with db:
        assert await db.ping() is True
    assert await db.ping() is False


async def test_async_with_closes_on_exception():
    """Exit-on-exception still closes the DB; the follow-up ping
    reflects that with False."""
    db = await DB.from_url("sqlite+aiosqlite:///:memory:")

    class _SentinelError(Exception):
        pass

    with pytest.raises(_SentinelError):
        async with db:
            raise _SentinelError

    assert await db.ping() is False
