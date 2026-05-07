"""DB facade: the user-facing entry point.

DB sits on top of an AdapterBase and exposes the user-facing API:
raw SQL passthrough mirroring asyncpg's vocabulary, typed-result
helpers `fetch_models / fetch_model`, and a transaction context
manager. Construct directly with an adapter, or via the URL-scheme
dispatcher `DB.from_url`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from etchdb.adapter import AdapterBase
    from etchdb.row import Row


def _hydrate(model_or_row: type[Row] | Row, row: dict[str, Any] | None) -> Row | None:
    """Build a Row from a fetchrow result, or return None when there is no row.

    Centralises the dict-to-Row construction and the None-handling that
    fetchrow-based methods (`fetch_model`, later `insert` and `update`)
    all share. `model_or_row` may be either a Row class (for `fetch_model`)
    or a Row instance whose class should be used (for `insert`/`update`).
    """
    if row is None:
        return None
    cls = model_or_row if isinstance(model_or_row, type) else type(model_or_row)
    return cls(**row)


class DB:
    """User-facing DB facade."""

    def __init__(self, adapter: AdapterBase):
        self._adapter = adapter

    @classmethod
    async def from_url(cls, url: str) -> DB:
        """Open a DB from a URL, dispatching on the URL scheme.

        Supported schemes:
          postgresql://, postgres://, postgresql+asyncpg://  -> asyncpg
          sqlite:///, sqlite+aiosqlite:///                   -> aiosqlite
          postgresql+psycopg://                              -> NotImplementedError

        Driver subpackages are imported lazily so users only need the
        driver they actually use installed.
        """
        scheme = urlparse(url).scheme.lower()

        if scheme in {"postgresql", "postgres", "postgresql+asyncpg"}:
            from etchdb.asyncpg import AsyncpgAdapter

            # asyncpg only accepts the bare postgresql:// scheme.
            if scheme == "postgresql+asyncpg":
                url = "postgresql://" + url.split("://", 1)[1]
            adapter: AdapterBase = await AsyncpgAdapter.from_url(url)
        elif scheme in {"sqlite", "sqlite+aiosqlite"}:
            from etchdb.aiosqlite import AiosqliteAdapter

            adapter = await AiosqliteAdapter.from_url(url)
        elif scheme == "postgresql+psycopg":
            raise NotImplementedError("psycopg adapter not yet shipped")
        else:
            raise ValueError(f"Unsupported URL scheme: {scheme!r}")

        return cls(adapter)

    async def execute(self, sql: str, *params: Any) -> Any:
        return await self._adapter.execute(sql, *params)

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        return await self._adapter.fetch(sql, *params)

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        return await self._adapter.fetchrow(sql, *params)

    async def fetchval(self, sql: str, *params: Any) -> Any:
        return await self._adapter.fetchval(sql, *params)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[DB]:
        """Open a transaction. Commits on clean exit, rolls back on exception.

        Yields a DB bound to the transaction's connection so that calls
        on `tx` use the same connection as the surrounding block.
        """
        async with self._adapter.transaction() as tx_adapter:
            yield DB(tx_adapter)

    async def close(self) -> None:
        await self._adapter.close()

    async def fetch_models(self, model: type[Row], sql: str, *params: Any) -> list[Row]:
        rows = await self._adapter.fetch(sql, *params)
        return [model(**r) for r in rows]

    async def fetch_model(self, model: type[Row], sql: str, *params: Any) -> Row | None:
        row = await self._adapter.fetchrow(sql, *params)
        return _hydrate(model, row)
