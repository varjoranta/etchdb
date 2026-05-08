"""asyncpg adapter implementation."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from etchdb import errors
from etchdb.adapter import AdapterBase


def _map_exception(exc: BaseException) -> errors.EtchdbError | None:
    """Translate an asyncpg exception to its etchdb equivalent, or
    None if the exception should propagate unchanged."""
    if isinstance(exc, asyncpg.exceptions.IntegrityConstraintViolationError):
        return errors.IntegrityError(str(exc))
    if isinstance(exc, asyncpg.exceptions.UndefinedTableError):
        return errors.UndefinedTableError(str(exc))
    if isinstance(
        exc,
        asyncpg.exceptions.InvalidPasswordError
        | asyncpg.exceptions.PostgresConnectionError
        | asyncpg.exceptions.ConnectionDoesNotExistError
        | ConnectionError,
    ):
        return errors.OperationalError(str(exc))
    return None


_wrap_errors = errors.wrap(_map_exception)


class AsyncpgAdapter(AdapterBase):
    """AdapterBase implementation backed by an asyncpg pool.

    Construct via `from_pool(pool)` to wrap an externally-managed pool
    (etchdb will not close it), or `await from_url(url)` to let etchdb
    create and own the pool. The `owns_pool` flag tracks ownership.

    For custom pool settings (init=, min_size=, max_size=, codecs),
    create the pool yourself with `asyncpg.create_pool(...)` and pass
    it to `from_pool`. `from_url` is intentionally minimal.
    """

    def __init__(self, pool: asyncpg.Pool, *, owns_pool: bool = False):
        self._pool = pool
        self._owns_pool = owns_pool

    @staticmethod
    def placeholder(i: int) -> str:
        return f"${i + 1}"

    @classmethod
    def from_pool(cls, pool: asyncpg.Pool) -> AsyncpgAdapter:
        """Wrap an externally-managed asyncpg pool. The caller closes it."""
        return cls(pool, owns_pool=False)

    @classmethod
    async def from_url(cls, url: str) -> AsyncpgAdapter:
        """Create an asyncpg pool from `url` and wrap it.

        etchdb owns the pool; `close()` will close it.
        """
        pool = await asyncpg.create_pool(url)
        return cls(pool, owns_pool=True)

    async def execute(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors(), self._pool.acquire() as conn:
            return await conn.execute(sql, *params)

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        async with _wrap_errors(), self._pool.acquire() as conn:
            records = await conn.fetch(sql, *params)
        return [dict(r) for r in records]

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        async with _wrap_errors(), self._pool.acquire() as conn:
            record = await conn.fetchrow(sql, *params)
        return dict(record) if record is not None else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors(), self._pool.acquire() as conn:
            return await conn.fetchval(sql, *params)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AdapterBase]:
        async with _wrap_errors(), self._pool.acquire() as conn, conn.transaction():
            yield _AsyncpgConnAdapter(conn)

    async def close(self) -> None:
        if self._owns_pool:
            await self._pool.close()


class _AsyncpgConnAdapter(AdapterBase):
    """Single-connection adapter, used inside a transaction."""

    def __init__(self, conn: asyncpg.Connection):
        self._conn = conn

    @staticmethod
    def placeholder(i: int) -> str:
        return f"${i + 1}"

    async def execute(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors():
            return await self._conn.execute(sql, *params)

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        async with _wrap_errors():
            records = await self._conn.fetch(sql, *params)
        return [dict(r) for r in records]

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        async with _wrap_errors():
            record = await self._conn.fetchrow(sql, *params)
        return dict(record) if record is not None else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors():
            return await self._conn.fetchval(sql, *params)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AdapterBase]:
        async with _wrap_errors(), self._conn.transaction():
            yield self

    async def close(self) -> None:
        return None
