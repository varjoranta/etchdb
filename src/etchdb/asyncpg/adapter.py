"""asyncpg adapter implementation."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from etchdb import errors
from etchdb.adapter import AdapterBase, rowcount_from_status
from etchdb.codecs import _with_configure, json_dumps


async def configure_codecs(conn: asyncpg.Connection) -> None:
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


# Compatibility for applications that imported the old private callback.
_init_codecs = configure_codecs

# Exact strings from asyncpg's Pool._check_init, each with the advice
# that fits it. Matched literally rather than by substring so an asyncpg
# reword fails the pinning test loudly instead of silently reverting to an
# unhelpful bare traceback. The third _check_init message ("pool is being
# initialized, but not yet ready") is deliberately absent: that one is a
# startup race and wants different advice.
_POOL_STATE_MSGS = {
    "pool is closed": (
        "the pool behind this DB has been closed. If it was closed and "
        "recreated (app restart, test teardown, reconnect), build a new DB "
        "or adapter from the new pool: from_pool() keeps the pool object it "
        "was given, and a DB from from_url() cannot be used after close()."
    ),
    "pool is not initialized": (
        "the pool passed to from_pool() was never opened. Await "
        "asyncpg.create_pool(...) before wrapping it."
    ),
}


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
    if isinstance(exc, asyncpg.exceptions.InterfaceError) and str(exc) in _POOL_STATE_MSGS:
        # Pool._check_init raises these when the wrapped pool is closed or
        # was never opened. Unmapped, it surfaces as a bare asyncpg traceback
        # ending inside pool.py that names neither etchdb nor a cause.
        return errors.OperationalError(f"{exc}: {_POOL_STATE_MSGS[str(exc)]}")
    return None


_wrap_errors = errors.wrap(_map_exception)


class AsyncpgAdapter(AdapterBase):
    """AdapterBase implementation backed by an asyncpg pool.

    Construct via `from_pool(pool)` to wrap an externally-managed pool
    (etchdb will not close it), or `await from_url(url)` to let etchdb
    create and own the pool. The `owns_pool` flag tracks ownership.

    `from_url` accepts `min_size` / `max_size`, `command_timeout` and
    a `configure` callback for per-connection setup such as ENUM
    codecs. For anything else (`statement_cache_size`, a `setup=`
    callback), create the pool yourself with
    `asyncpg.create_pool(...)`, call `configure_codecs` from its
    `init=` to keep etchdb's JSONB behaviour, and pass it to
    `from_pool`.
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
    async def from_url(
        cls,
        url: str,
        *,
        min_size: int | None = None,
        max_size: int | None = None,
        configure: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
        command_timeout: float | None = None,
    ) -> AsyncpgAdapter:
        """Create an asyncpg pool from `url` and wrap it.

        etchdb owns the pool; `close()` will close it. The pool is
        initialised with a JSONB codec that handles UUID, datetime,
        Enum, and Pydantic BaseModel transparently.

        `configure` runs after the built-in JSONB codec on each new
        connection, using asyncpg's pool `init` callback. It may add or
        override codecs. It is not the place for session settings:
        asyncpg runs `RESET ALL` when a connection goes back to the
        pool, so a `SET` made here is gone after the first query. Put
        those in the URL query (`?search_path=app`), which asyncpg
        sends as server settings on every connection.

        `command_timeout` sets asyncpg's default command timeout in
        seconds; a statement that exceeds it raises the built-in
        `TimeoutError`, not an etchdb error. Pool sizes and timeout
        retain driver defaults when omitted. For other options, use
        `from_pool`.
        """
        pool_kwargs: dict[str, Any] = {"init": _with_configure(configure_codecs, configure)}
        if min_size is not None:
            pool_kwargs["min_size"] = min_size
        if max_size is not None:
            pool_kwargs["max_size"] = max_size
        if command_timeout is not None:
            pool_kwargs["command_timeout"] = command_timeout
        pool = await asyncpg.create_pool(url, **pool_kwargs)
        return cls(pool, owns_pool=True)

    async def execute(self, sql: str, *params: Any) -> int:
        async with _wrap_errors(), self._pool.acquire() as conn:
            return rowcount_from_status(await conn.execute(sql, *params))

    async def execute_script(self, sql: str) -> None:
        async with _wrap_errors(), self._pool.acquire() as conn:
            await conn.execute(sql)

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
            return rowcount_from_status(await self._conn.execute(sql, *params))

    async def execute_script(self, sql: str) -> None:
        async with _wrap_errors():
            await self._conn.execute(sql)

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
