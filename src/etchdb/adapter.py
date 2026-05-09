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
        DELETE), or `-1` for everything else (DDL, BEGIN, SELECT,
        COPY, ...). The `-1` sentinel mirrors psycopg's and sqlite3's
        native convention. SELECT through `execute` is explicitly not
        a count contract; use `fetch` / `fetchrow` / `fetchval` for
        SELECT and read the row count off the result.
        """
        ...

    @abstractmethod
    async def execute_script(self, sql: str) -> None:
        """Execute one or more SQL statements with no parameters.

        Used by the migration runner for files that may contain
        multiple statements separated by `;`. asyncpg / psycopg accept
        multi-statement SQL through the regular execute path;
        aiosqlite wraps `sqlite3.executescript`, which auto-commits
        any pending transaction (a sqlite3 stdlib behavior).
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
