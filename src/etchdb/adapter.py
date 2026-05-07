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
    async def execute(self, sql: str, *params: Any) -> Any: ...

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
