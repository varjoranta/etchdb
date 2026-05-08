"""aiosqlite adapter implementation.

aiosqlite has no pool concept; it wraps a single sqlite3 connection
that runs on its own background thread. The adapter therefore holds
one connection rather than a pool. Concurrent calls serialise through
aiosqlite's internal queue, which is the correct sqlite3 model.
"""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlparse

import aiosqlite

from etchdb import errors
from etchdb.adapter import AdapterBase


def _map_exception(exc: BaseException) -> errors.EtchdbError | None:
    """Translate a sqlite3 exception (raised through aiosqlite) to its
    etchdb equivalent, or None if the exception should propagate as-is.
    """
    if isinstance(exc, sqlite3.IntegrityError):
        return errors.IntegrityError(str(exc))
    if isinstance(exc, sqlite3.OperationalError):
        # sqlite3.OperationalError covers both "no such table" and
        # connection-level failures; sqlite3 has no richer subclass,
        # so disambiguate by message text.
        msg = str(exc).lower()
        if "no such table" in msg or "no such column" in msg:
            return errors.UndefinedTableError(str(exc))
        return errors.OperationalError(str(exc))
    return None


_wrap_errors = errors.wrap(_map_exception)


def _path_from_url(url: str) -> str:
    """Extract the SQLite database path from a URL.

    Supported forms:
      sqlite:///:memory:                    -> ":memory:"
      sqlite+aiosqlite:///:memory:          -> ":memory:"
      sqlite:///relative.db                 -> "relative.db"
      sqlite:////absolute/path.db           -> "/absolute/path.db"
    """
    parsed = urlparse(url)
    path = parsed.path
    if path.startswith("/"):
        path = path[1:]
    return path or ":memory:"


class AiosqliteAdapter(AdapterBase):
    """AdapterBase implementation backed by a single aiosqlite connection.

    Construct via `from_connection(conn)` to wrap an externally-managed
    connection (etchdb will not close it), or `await from_url(url)` to
    let etchdb create and own the connection.
    """

    def __init__(self, conn: aiosqlite.Connection, *, owns_conn: bool = False):
        self._conn = conn
        self._owns_conn = owns_conn

    @staticmethod
    def placeholder(i: int) -> str:
        return "?"

    @classmethod
    def from_connection(cls, conn: aiosqlite.Connection) -> AiosqliteAdapter:
        """Wrap an externally-managed aiosqlite connection. The caller closes it."""
        return cls(conn, owns_conn=False)

    @classmethod
    async def from_url(cls, url: str) -> AiosqliteAdapter:
        """Open an aiosqlite connection from `url` and wrap it.

        etchdb owns the connection; `close()` will close it.
        """
        path = _path_from_url(url)
        conn = await aiosqlite.connect(path)
        conn.row_factory = aiosqlite.Row
        return cls(conn, owns_conn=True)

    async def execute(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors():
            await self._conn.execute(sql, params)
            await self._conn.commit()

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        async with _wrap_errors(), self._conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        async with _wrap_errors(), self._conn.execute(sql, params) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row is not None else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors(), self._conn.execute(sql, params) as cursor:
            row = await cursor.fetchone()
        return row[0] if row is not None else None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AdapterBase]:
        # sqlite3's default isolation_level auto-injects BEGIN before DML,
        # so we don't emit one explicitly; we just commit on clean exit
        # and rollback on any exception. Rollback must run before the
        # mapped exception escapes, so we can't reuse `_wrap_errors`.
        try:
            yield _AiosqliteTxAdapter(self._conn)
        except BaseException as e:
            await self._conn.rollback()
            if isinstance(e, Exception):
                mapped = _map_exception(e)
                if mapped is not None:
                    raise mapped from e
            raise
        else:
            await self._conn.commit()

    async def close(self) -> None:
        if self._owns_conn:
            await self._conn.close()


class _AiosqliteTxAdapter(AdapterBase):
    """Tx-scoped aiosqlite adapter; does not commit on each statement."""

    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn

    @staticmethod
    def placeholder(i: int) -> str:
        return "?"

    async def execute(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors():
            await self._conn.execute(sql, params)

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        async with _wrap_errors(), self._conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        async with _wrap_errors(), self._conn.execute(sql, params) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row is not None else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        async with _wrap_errors(), self._conn.execute(sql, params) as cursor:
            row = await cursor.fetchone()
        return row[0] if row is not None else None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AdapterBase]:
        sp = "etchdb_sp"
        await self._conn.execute(f"SAVEPOINT {sp}")
        try:
            yield self
        except BaseException:
            await self._conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            await self._conn.execute(f"RELEASE SAVEPOINT {sp}")
            raise
        else:
            await self._conn.execute(f"RELEASE SAVEPOINT {sp}")

    async def close(self) -> None:
        return None
