"""Abstract DB adapter; the boundary DB sits on top of."""

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from typing import Any


class AdapterBase(ABC):
    """Abstract DB adapter implemented by each driver.

    The four raw-SQL methods mirror asyncpg's vocabulary:
    `execute / fetch / fetchrow / fetchval`. All take positional
    `*params` bound through the driver's parameterised query API.

    `placeholder(i)` converts a 0-indexed parameter position to the
    driver's placeholder syntax. Postgres returns `$1, $2, ...`; SQLite
    returns `?`.
    """

    @staticmethod
    @abstractmethod
    def placeholder(i: int) -> str:
        """Return the placeholder for the i-th parameter (0-indexed)."""
        ...

    @abstractmethod
    async def execute(self, sql: str, *params: Any) -> int:
        """Execute a statement and return the affected-row count.

        Returns the affected-row count for DML (INSERT / UPDATE /
        DELETE), with or without RETURNING, or `-1` for everything
        else (DDL, BEGIN, SELECT, COPY, ...). The `-1` sentinel
        mirrors psycopg's and sqlite3's native convention. SELECT
        through `execute` is explicitly not a count contract; use
        `fetch` / `fetchrow` / `fetchval` for SELECT and read the row
        count off the result.
        """
        ...

    @abstractmethod
    async def execute_script(self, sql: str) -> None:
        """Execute one or more SQL statements with no parameters.

        Used by the migration runner for files that may contain
        multiple statements separated by `;`. asyncpg / psycopg accept
        multi-statement SQL through the regular execute path;
        aiosqlite wraps `sqlite3.executescript`, which stays inside
        the surrounding transaction on a connection opened with
        `autocommit=True` (every `from_url` connection) and commits
        first on one in sqlite3's legacy transaction mode.
        """
        ...

    @abstractmethod
    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None: ...

    @abstractmethod
    async def fetchval(self, sql: str, *params: Any) -> Any: ...

    @abstractmethod
    def transaction(self) -> AbstractAsyncContextManager["AdapterBase"]:
        """Return an async context manager yielding a transaction-scoped adapter.

        Inside the `async with` block, all calls on the yielded adapter run
        on the same connection within a single transaction. The transaction
        commits on a clean exit and rolls back on any exception.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release resources owned by this adapter.

        For pool-owning adapters created via `from_url`, this closes the
        pool. For adapters wrapping an externally-managed pool (`from_pool`),
        this is a no-op; the caller owns the pool's lifecycle.
        """
        ...


_DML_TAGS = ("INSERT", "UPDATE", "DELETE")


def rowcount_from_status(status: str) -> int:
    """Extract the affected-row count from a Postgres command tag.

    DML tags ('UPDATE 5', 'DELETE 3', 'INSERT 0 5') end in the
    count and parse to a non-negative int, with or without a
    RETURNING clause. Anything else (DDL, `BEGIN`, `SELECT`, `COPY`)
    returns -1, the sqlite3 'no rowcount available' sentinel. SELECT
    in particular would otherwise parse to its row count, which
    `db.execute` explicitly does not promise across drivers. Both
    Postgres adapters read the tag, so they agree.
    """
    verb, _, _ = status.partition(" ")
    if verb not in _DML_TAGS:
        return -1
    tail = status.rsplit(" ", 1)[-1]
    try:
        return int(tail)
    except ValueError:
        return -1
