"""DB facade: the user-facing entry point.

DB sits on top of an AdapterBase and exposes the user-facing API:
typed CRUD over Row, raw SQL passthrough mirroring asyncpg's
vocabulary, typed-result helpers `fetch_models / fetch_model`, a
transaction context manager, and a `compose` inspector for previewing
the SQL of a typed op without executing it. Construct directly with
an adapter, or via the URL-scheme dispatcher `DB.from_url`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

from etchdb import sql

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from etchdb.adapter import AdapterBase
    from etchdb.query import SqlQuery
    from etchdb.row import Row


def _hydrate(model_or_row: type[Row] | Row, row: dict[str, Any] | None) -> Row | None:
    """Build a Row from a fetchrow result, or return None when there is no row.

    Centralises the dict-to-Row construction and the None-handling that
    fetchrow-based methods (`fetch_model`, `insert`, `update`) all share.
    `model_or_row` may be either a Row class (for `fetch_model`) or a Row
    instance whose class should be used (for `insert` and `update`).
    """
    if row is None:
        return None
    cls = model_or_row if isinstance(model_or_row, type) else type(model_or_row)
    return cls(**row)


_COMPOSE_OPS = {
    "get": sql.select_one,
    "query": sql.select_many,
    "insert": sql.insert,
    "update": sql.update,
    "delete": sql.delete,
}


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

    async def get(self, model: type[Row], **filters: Any) -> Row | None:
        q = sql.select_one(model, placeholder=self._adapter.placeholder, **filters)
        return await self.fetch_model(model, q.sql, *q.params)

    async def query(
        self,
        model: type[Row],
        *,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | list[str] | None = None,
        **filters: Any,
    ) -> list[Row]:
        q = sql.select_many(
            model,
            placeholder=self._adapter.placeholder,
            limit=limit,
            offset=offset,
            order_by=order_by,
            **filters,
        )
        return await self.fetch_models(model, q.sql, *q.params)

    async def insert(self, row: Row) -> Row:
        q = sql.insert(row, placeholder=self._adapter.placeholder)
        result = await self._adapter.fetchrow(q.sql, *q.params)
        return _hydrate(row, result) or row

    async def update(self, row: Row) -> Row | None:
        """Update `row` keyed by its primary key. Returns the updated row,
        or None if no row matched."""
        q = sql.update(row, placeholder=self._adapter.placeholder, returning="*")
        result = await self._adapter.fetchrow(q.sql, *q.params)
        return _hydrate(row, result)

    async def delete(self, row: Row) -> None:
        q = sql.delete(row, placeholder=self._adapter.placeholder)
        await self._adapter.execute(q.sql, *q.params)

    def compose(
        self,
        op: Literal["get", "query", "insert", "update", "delete"],
        *args: Any,
        **kwargs: Any,
    ) -> SqlQuery:
        """Return the SqlQuery a typed op would produce, without executing it.

        Lets callers inspect or test SQL before it touches the DB. The
        placeholder style follows the underlying adapter ($N for asyncpg,
        ? for aiosqlite).

            q = db.compose("get", User, id=1)
            print(q.sql, q.params)
        """
        try:
            fn = _COMPOSE_OPS[op]
        except KeyError as e:
            raise ValueError(f"Unknown op {op!r}. Expected one of: {sorted(_COMPOSE_OPS)}") from e
        return fn(*args, placeholder=self._adapter.placeholder, **kwargs)
