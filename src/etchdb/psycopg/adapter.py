"""psycopg adapter implementation.

Uses `psycopg.AsyncRawCursor` so raw SQL keeps libpq-native `$1, $2, ...`
placeholders: the same form the asyncpg adapter accepts. Callers can
swap between the two Postgres adapters without rewriting their queries.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import psycopg
import psycopg.types.json
from psycopg.rows import AsyncRowFactory, dict_row, tuple_row
from psycopg.types.json import JsonbDumper
from psycopg_pool import AsyncConnectionPool

from etchdb import errors
from etchdb.adapter import AdapterBase
from etchdb.codecs import json_dumps


class _DictAsJsonbDumper(JsonbDumper):
    """Make raw `dict` and `list` round-trip as JSONB.

    psycopg auto-decodes JSONB to dict/list natively, but with our
    libpq-native `$N` placeholders it has no way to know a Python dict
    should become jsonb without an explicit dumper. JsonbDumper handles
    raw objects via its `_dumps` attribute; we point it at `json_dumps`
    so the same encoder handles both `Jsonb()` wrappers and raw dicts.
    """

    _dumps = staticmethod(json_dumps)


async def _init_codecs(conn: psycopg.AsyncConnection) -> None:
    """Pool `configure=` callback: encode UUID, datetime, Enum,
    Pydantic BaseModel and friends in JSONB, and auto-adapt raw
    `dict` / `list` parameters as jsonb so application code doesn't
    need explicit `Jsonb()` wrapping."""
    psycopg.types.json.set_json_dumps(json_dumps, conn)
    conn.adapters.register_dumper(dict, _DictAsJsonbDumper)
    conn.adapters.register_dumper(list, _DictAsJsonbDumper)


def _map_exception(exc: BaseException) -> errors.EtchdbError | None:
    """Translate a psycopg exception to its etchdb equivalent, or None
    if the exception should propagate unchanged."""
    if isinstance(exc, psycopg.errors.IntegrityError):
        return errors.IntegrityError(str(exc))
    if isinstance(exc, psycopg.errors.UndefinedTable):
        return errors.UndefinedTableError(str(exc))
    if isinstance(exc, psycopg.errors.UndefinedColumn):
        return errors.UndefinedColumnError(str(exc))
    if isinstance(exc, psycopg.errors.OperationalError | psycopg.errors.InterfaceError):
        return errors.OperationalError(str(exc))
    return None


_wrap_errors = errors.wrap(_map_exception)


class PsycopgAdapter(AdapterBase):
    """AdapterBase implementation backed by a psycopg AsyncConnectionPool.

    Construct via `from_pool(pool)` to wrap an externally-managed pool
    (etchdb will not close it), or `await from_url(url)` to let etchdb
    create and own the pool. The `owns_pool` flag tracks ownership.

    Raw-SQL methods use `$1, $2, ...` placeholders (libpq native, same
    as asyncpg). psycopg's default `%s` form is not used here, so SQL
    written for the asyncpg adapter runs against this one unchanged.
    """

    def __init__(self, pool: AsyncConnectionPool, *, owns_pool: bool = False):
        self._pool = pool
        self._owns_pool = owns_pool

    @staticmethod
    def placeholder(i: int) -> str:
        return f"${i + 1}"

    @classmethod
    def from_pool(cls, pool: AsyncConnectionPool) -> PsycopgAdapter:
        """Wrap an externally-managed psycopg AsyncConnectionPool."""
        return cls(pool, owns_pool=False)

    @classmethod
    async def from_url(
        cls,
        url: str,
        *,
        min_size: int | None = None,
        max_size: int | None = None,
    ) -> PsycopgAdapter:
        """Open a psycopg AsyncConnectionPool against `url` and wrap it.

        etchdb owns the pool; `close()` will close it. The pool is
        configured with a JSON encoder that handles UUID, datetime,
        Enum, and Pydantic BaseModel transparently.

        `min_size` / `max_size` are forwarded to `AsyncConnectionPool`
        if set. For pool concerns beyond size (psycopg's
        `prepare_threshold`, custom dumpers, etc.), construct the
        pool yourself and use `from_pool`.
        """
        pool_kwargs: dict[str, Any] = {"open": False, "configure": _init_codecs}
        if min_size is not None:
            pool_kwargs["min_size"] = min_size
        if max_size is not None:
            pool_kwargs["max_size"] = max_size
        pool = AsyncConnectionPool(url, **pool_kwargs)
        await pool.open()
        return cls(pool, owns_pool=True)

    @asynccontextmanager
    async def _cursor(
        self, *, row_factory: AsyncRowFactory[Any] = dict_row
    ) -> AsyncIterator[psycopg.AsyncRawCursor[Any]]:
        async with (
            self._pool.connection() as conn,
            psycopg.AsyncRawCursor(conn, row_factory=row_factory) as cur,
        ):
            yield cur

    # psycopg types `cur.execute`'s query arg as LiteralString to
    # discourage SQL injection at the type level; etchdb passes runtime
    # strings here (the SqlQuery / raw-SQL contract). The injection
    # guard is the `*params` substitution, not the query type.
    async def execute(self, sql: str, *params: Any) -> int:
        async with _wrap_errors(), self._cursor() as cur:
            await cur.execute(cast(Any, sql), params)
            if cur.description is not None:
                return -1
            return cur.rowcount

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        async with _wrap_errors(), self._cursor() as cur:
            await cur.execute(cast(Any, sql), params)
            return await cur.fetchall()

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        async with _wrap_errors(), self._cursor() as cur:
            await cur.execute(cast(Any, sql), params)
            return await cur.fetchone()

    async def fetchval(self, sql: str, *params: Any) -> Any:
        # tuple_row skips the dict allocation we'd just throw away.
        async with _wrap_errors(), self._cursor(row_factory=tuple_row) as cur:
            await cur.execute(cast(Any, sql), params)
            row = await cur.fetchone()
            return row[0] if row is not None else None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AdapterBase]:
        async with _wrap_errors(), self._pool.connection() as conn, conn.transaction():
            yield _PsycopgConnAdapter(conn)

    async def close(self) -> None:
        if self._owns_pool:
            await self._pool.close()


class _PsycopgConnAdapter(AdapterBase):
    """Single-connection adapter, used inside a transaction."""

    def __init__(self, conn: psycopg.AsyncConnection):
        self._conn = conn

    @staticmethod
    def placeholder(i: int) -> str:
        return f"${i + 1}"

    @asynccontextmanager
    async def _cursor(
        self, *, row_factory: AsyncRowFactory[Any] = dict_row
    ) -> AsyncIterator[psycopg.AsyncRawCursor[Any]]:
        async with psycopg.AsyncRawCursor(self._conn, row_factory=row_factory) as cur:
            yield cur

    async def execute(self, sql: str, *params: Any) -> int:
        async with _wrap_errors(), self._cursor() as cur:
            await cur.execute(cast(Any, sql), params)
            if cur.description is not None:
                return -1
            return cur.rowcount

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        async with _wrap_errors(), self._cursor() as cur:
            await cur.execute(cast(Any, sql), params)
            return await cur.fetchall()

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        async with _wrap_errors(), self._cursor() as cur:
            await cur.execute(cast(Any, sql), params)
            return await cur.fetchone()

    async def fetchval(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors(), self._cursor(row_factory=tuple_row) as cur:
            await cur.execute(cast(Any, sql), params)
            row = await cur.fetchone()
            return row[0] if row is not None else None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AdapterBase]:
        async with _wrap_errors(), self._conn.transaction():
            yield self

    async def close(self) -> None:
        return None
