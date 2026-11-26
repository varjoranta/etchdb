"""aiosqlite adapter implementation.

aiosqlite has no pool concept; it wraps a single sqlite3 connection
that runs on its own background thread. The adapter therefore holds
one connection rather than a pool, and every `DB` call on it shares
that connection.

Transaction control is explicit. A statement outside `transaction()`
is committed as soon as it finishes; `transaction()` issues its own
`BEGIN` and `COMMIT` / `ROLLBACK`. Nothing relies on the sqlite3
module opening a transaction implicitly, so the adapter behaves the
same on a connection in sqlite3's legacy mode and on one opened with
`autocommit=True` (which is how `from_url` opens it).

One connection means one transaction at a time: a statement issued
while another task's `transaction()` block is open joins that
transaction. Give each concurrent unit of work its own `DB` when it
needs isolation.
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
        # sqlite3.OperationalError covers schema lookup failures and
        # connection-level failures alike; sqlite3 has no richer
        # subclass, so disambiguate by message text.
        msg = str(exc).lower()
        if "no such table" in msg:
            return errors.UndefinedTableError(str(exc))
        # SELECT / UPDATE say "no such column"; INSERT says
        # "table t has no column named c".
        if "no such column" in msg or "has no column named" in msg:
            return errors.UndefinedColumnError(str(exc))
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

    `sqlite://relative.db` (two slashes) puts the file name where a
    URL keeps its host, leaving the path empty. It is rejected rather
    than silently opening an in-memory database.
    """
    parsed = urlparse(url)
    if parsed.netloc:
        raise ValueError(
            f"SQLite URL {url!r} has {parsed.netloc!r} in the host position. "
            f"Use three slashes for a relative path (sqlite:///{parsed.netloc}{parsed.path}) "
            f"or four for an absolute one."
        )
    path = parsed.path
    if path.startswith("/"):
        path = path[1:]
    return path or ":memory:"


async def _control(conn: aiosqlite.Connection, sql: str) -> None:
    """Run a transaction-control statement (BEGIN, COMMIT, SAVEPOINT...)."""
    cursor = await conn.execute(sql)
    await cursor.close()


def _row_dict(cursor: aiosqlite.Cursor, row: Any) -> dict[str, Any]:
    """Build a dict from the cursor's column names, so the result does
    not depend on the connection's `row_factory`. An externally owned
    connection keeps whatever factory its owner set."""
    return dict(zip((d[0] for d in cursor.description), row, strict=True))


class _Statements(AdapterBase):
    """The four statement methods, shared by the connection-level
    adapter and the transaction-scoped one.

    `_standalone` says whether a statement is its own unit of work. A
    standalone statement that opened a transaction (sqlite3's legacy
    mode does this before DML) commits it on success and rolls it back
    on failure. A statement that found a transaction already open
    leaves it to whoever opened it.
    """

    _conn: aiosqlite.Connection
    _standalone: bool

    @staticmethod
    def placeholder(i: int) -> str:
        return "?"

    @asynccontextmanager
    async def _statement(
        self, sql: str, params: tuple[Any, ...]
    ) -> AsyncIterator[aiosqlite.Cursor]:
        conn = self._conn
        owns = self._standalone and not conn.in_transaction
        async with _wrap_errors():
            try:
                async with conn.execute(sql, params) as cursor:
                    yield cursor
                if owns and conn.in_transaction:
                    await _control(conn, "COMMIT")
            except BaseException:
                if owns and conn.in_transaction:
                    await _control(conn, "ROLLBACK")
                raise

    async def execute(self, sql: str, *params: Any) -> int:
        async with self._statement(sql, params) as cursor:
            # DML with RETURNING reports its rowcount, and lets the
            # transaction end, only once its rows have been read.
            if cursor.description is not None:
                await cursor.fetchall()
            return cursor.rowcount

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        async with self._statement(sql, params) as cursor:
            return [_row_dict(cursor, r) for r in await cursor.fetchall()]

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        async with self._statement(sql, params) as cursor:
            row = await cursor.fetchone()
            return _row_dict(cursor, row) if row is not None else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        async with self._statement(sql, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row is not None else None


@asynccontextmanager
async def _savepoint(conn: aiosqlite.Connection) -> AsyncIterator[None]:
    sp = "etchdb_sp"
    await _control(conn, f"SAVEPOINT {sp}")
    try:
        yield
    except BaseException:
        await _control(conn, f"ROLLBACK TO SAVEPOINT {sp}")
        await _control(conn, f"RELEASE SAVEPOINT {sp}")
        raise
    else:
        await _control(conn, f"RELEASE SAVEPOINT {sp}")


class AiosqliteAdapter(_Statements):
    """AdapterBase implementation backed by a single aiosqlite connection.

    Construct via `from_connection(conn)` to wrap an externally-managed
    connection (etchdb will not close it), or `await from_url(url)` to
    let etchdb create and own the connection.
    """

    _standalone = True

    def __init__(self, conn: aiosqlite.Connection, *, owns_conn: bool = False):
        self._conn = conn
        self._owns_conn = owns_conn

    @classmethod
    def from_connection(cls, conn: aiosqlite.Connection) -> AiosqliteAdapter:
        """Wrap an externally-managed aiosqlite connection. The caller
        closes it. Its `row_factory` and transaction mode are left as
        the caller set them."""
        return cls(conn, owns_conn=False)

    @classmethod
    async def from_url(
        cls,
        url: str,
        *,
        min_size: int | None = None,
        max_size: int | None = None,
    ) -> AiosqliteAdapter:
        """Open an aiosqlite connection from `url` and wrap it.

        etchdb owns the connection; `close()` will close it.
        `min_size` / `max_size` are accepted for cross-driver
        consistency with the Postgres adapters but ignored here --
        aiosqlite wraps a single sqlite3 connection, not a pool.

        The connection is opened with `autocommit=True`, so sqlite3
        never opens or commits a transaction on its own and
        `transaction()` is the only thing that does.
        """
        path = _path_from_url(url)
        conn = await aiosqlite.connect(path, autocommit=True)
        return cls(conn, owns_conn=True)

    async def execute_script(self, sql: str) -> None:
        async with _wrap_errors():
            await self._conn.executescript(sql)
            if self._conn.in_transaction:
                await _control(self._conn, "COMMIT")

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AdapterBase]:
        conn = self._conn
        tx = _AiosqliteTxAdapter(conn)
        if conn.in_transaction:
            # Someone else's transaction is open on this connection (the
            # owner of an external connection, or another task). Nest
            # inside it instead of committing it from here.
            async with _savepoint(conn):
                yield tx
            return

        # An explicit BEGIN, so the block is a transaction from its first
        # statement: sqlite3's implicit one starts only at the first DML,
        # which left DDL and a leading savepoint outside it.
        async with _wrap_errors():
            await _control(conn, "BEGIN")
        try:
            yield tx
            # A COMMIT can fail (a deferred constraint, SQLITE_BUSY) and
            # then leaves the transaction open, so it shares the rollback.
            if conn.in_transaction:
                await _control(conn, "COMMIT")
        except BaseException as e:
            if conn.in_transaction:
                await _control(conn, "ROLLBACK")
            # Rollback must run before the mapped exception escapes, so
            # this cannot reuse `_wrap_errors`.
            if isinstance(e, Exception):
                mapped = _map_exception(e)
                if mapped is not None:
                    raise mapped from e
            raise

    async def close(self) -> None:
        if self._owns_conn:
            await self._conn.close()


class _AiosqliteTxAdapter(_Statements):
    """Tx-scoped aiosqlite adapter; does not commit on each statement."""

    _standalone = False

    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn

    async def execute_script(self, sql: str) -> None:
        # On a connection in sqlite3's legacy mode, executescript commits
        # the open transaction before it runs; with `autocommit=True`
        # (every `from_url` connection) the script stays inside it.
        async with _wrap_errors():
            await self._conn.executescript(sql)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AdapterBase]:
        async with _savepoint(self._conn):
            yield self

    async def close(self) -> None:
        return None
