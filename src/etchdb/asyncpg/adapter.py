"""asyncpg adapter implementation."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from etchdb import errors
from etchdb.adapter import AdapterBase
from etchdb.codecs import json_dumps


async def _init_codecs(conn: asyncpg.Connection) -> None:
    """Pool `init=` callback: register a JSONB codec on every new
    connection so `dict | list` round-trips with UUID, datetime, Enum,
    and Pydantic BaseModel encoded transparently. asyncpg returns JSONB
    as `str` without this; with it, JSONB columns come back as Python
    objects directly."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json_dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


def _map_exception(exc: BaseException) -> errors.EtchdbError | None:
    """Translate an asyncpg exception to its etchdb equivalent, or
    None if the exception should propagate unchanged."""
    if isinstance(exc, asyncpg.exceptions.IntegrityConstraintViolationError):
        return errors.IntegrityError(str(exc))
    if isinstance(exc, asyncpg.exceptions.UndefinedTableError):
        return errors.UndefinedTableError(str(exc))
    if isinstance(exc, asyncpg.exceptions.UndefinedColumnError):
        return errors.UndefinedColumnError(str(exc))
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


_DML_TAGS = ("INSERT", "UPDATE", "DELETE")


def _rowcount_from_status(status: str) -> int:
    """Extract the affected-row count from an asyncpg command tag.

    DML tags ('UPDATE 5', 'DELETE 3', 'INSERT 0 5') end in the
    count and parse to a non-negative int. Anything else (DDL,
    `BEGIN`, `SELECT`, `COPY`) returns -1, matching the psycopg /
    sqlite3 'no rowcount available' sentinel. SELECT in particular
    would otherwise parse to its row count, which `db.execute`
    explicitly does not promise across drivers.
    """
    verb, _, _ = status.partition(" ")
    if verb not in _DML_TAGS:
        return -1
    tail = status.rsplit(" ", 1)[-1]
    try:
        return int(tail)
    except ValueError:
        return -1


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

        etchdb owns the pool; `close()` will close it. The pool is
        initialised with a JSONB codec that handles UUID, datetime,
        Enum, and Pydantic BaseModel transparently. Users wanting a
        pristine pool with no codec setup should construct the pool
        themselves and use `from_pool`.
        """
        pool = await asyncpg.create_pool(url, init=_init_codecs)
        return cls(pool, owns_pool=True)

    async def execute(self, sql: str, *params: Any) -> int:
        async with _wrap_errors(), self._pool.acquire() as conn:
            return _rowcount_from_status(await conn.execute(sql, *params))

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

    async def execute(self, sql: str, *params: Any) -> int:
        async with _wrap_errors():
            return _rowcount_from_status(await self._conn.execute(sql, *params))

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
