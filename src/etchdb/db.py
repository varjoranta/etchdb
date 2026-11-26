"""DB facade: the user-facing entry point.

DB sits on top of an AdapterBase and exposes the user-facing API:
typed CRUD over Row, raw SQL passthrough mirroring asyncpg's
vocabulary, typed-result helpers `fetch_models / fetch_model`, a
transaction context manager, and a `compose` inspector for previewing
the SQL of a typed op without executing it. Construct directly with
an adapter, or via the URL-scheme dispatcher `DB.from_url`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from etchdb import sql
from etchdb.adapter import AdapterBase
from etchdb.migrations import MigrationStatus
from etchdb.query import SqlQuery
from etchdb.row import Row

# Postgres caps query parameters at 32767; SQLite is at least 32766 since
# 3.32. Pick the lower bound so a chunk fits in any backend.
_PARAM_LIMIT = 32766


def _has_empty_collection_filter(filters: Mapping[str, Any] | None) -> bool:
    """An empty list / tuple filter would emit `IN ()` (invalid on
    Postgres); short-circuit to "no rows" without round-tripping."""
    if not filters:
        return False
    return any(isinstance(v, list | tuple) and not v for v in filters.values())


def _hydrate[R: Row](model_or_row: type[R] | R, row: dict[str, Any] | None) -> R | None:
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


class DB:
    """User-facing DB facade."""

    def __init__(self, adapter: AdapterBase):
        self._adapter = adapter

    async def __aenter__(self) -> DB:
        """Enable `async with db:` on an already-open DB; closes on exit.

        `async with DB.from_url(...) as db:` does not work -- `from_url`
        is a coroutine. Open first, then enter the block.
        """
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.close()

    @classmethod
    async def from_url(
        cls,
        url: str,
        *,
        min_size: int | None = None,
        max_size: int | None = None,
        configure: Callable[[Any], Awaitable[None]] | None = None,
    ) -> DB:
        """Open a DB from a URL, dispatching on the URL scheme.

        Supported schemes:
          postgresql://, postgres://, postgresql+asyncpg://  -> asyncpg
          postgresql+psycopg://                              -> psycopg
          sqlite:///, sqlite+aiosqlite:///                   -> aiosqlite

        Driver subpackages are imported lazily so users only need the
        driver they actually use installed.

        `min_size` / `max_size` forward to the underlying Postgres pool
        and are ignored on aiosqlite (no pool concept).
        `configure` runs on every new Postgres connection after etchdb's
        built-in codecs. Its connection type depends on the selected driver.
        With psycopg, setup SQL must leave the connection idle. With
        asyncpg, use it for codecs and not for `SET`: asyncpg resets
        session state when a connection returns to the pool. SQLite
        rejects this Postgres-only hook. For driver-specific options, use
        the adapter's `from_url` or wrap your own pool with `from_pool`.
        """
        scheme = urlparse(url).scheme.lower()
        pool_kwargs: dict[str, Any] = {"min_size": min_size, "max_size": max_size}
        if configure is not None:
            pool_kwargs["configure"] = configure

        if scheme in {"postgresql", "postgres", "postgresql+asyncpg"}:
            from etchdb.asyncpg import AsyncpgAdapter

            # asyncpg only accepts the bare postgresql:// scheme.
            if scheme == "postgresql+asyncpg":
                url = "postgresql://" + url.split("://", 1)[1]
            adapter: AdapterBase = await AsyncpgAdapter.from_url(url, **pool_kwargs)
        elif scheme == "postgresql+psycopg":
            from etchdb.psycopg import PsycopgAdapter

            url = "postgresql://" + url.split("://", 1)[1]
            adapter = await PsycopgAdapter.from_url(url, **pool_kwargs)
        elif scheme in {"sqlite", "sqlite+aiosqlite"}:
            if configure is not None:
                raise ValueError("configure is only supported by the Postgres adapters")
            from etchdb.aiosqlite import AiosqliteAdapter

            adapter = await AiosqliteAdapter.from_url(url, min_size=min_size, max_size=max_size)
        else:
            raise ValueError(f"Unsupported URL scheme: {scheme!r}")

        return cls(adapter)

    async def execute(self, sql: str, *params: Any) -> int:
        """Execute a statement and return the affected-row count.

        Returns the rowcount for DML (INSERT / UPDATE / DELETE);
        returns `-1` for everything else (DDL, BEGIN, SELECT, COPY,
        ...). Normalised across asyncpg, psycopg, and aiosqlite so
        the same call site works on every backend. SELECT through
        `execute` is explicitly not a count contract: use
        `fetch` / `fetchrow` / `fetchval` and read the count off the
        result.
        """
        return await self._adapter.execute(sql, *params)

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        return await self._adapter.fetch(sql, *params)

    async def fetchrow(self, sql: str, *params: Any) -> dict[str, Any] | None:
        return await self._adapter.fetchrow(sql, *params)

    async def fetchval(self, sql: str, *params: Any) -> Any:
        return await self._adapter.fetchval(sql, *params)

    async def ping(self) -> bool:
        """Round-trip a `SELECT 1` to verify the connection is alive.

        Returns `True` on success, `False` on any failure (closed pool,
        network error, etc.). Useful for liveness probes and health
        endpoints. If you want the underlying exception (e.g. for
        diagnostics), call `fetchval('SELECT 1')` directly.
        """
        try:
            await self._adapter.fetchval("SELECT 1")
        except Exception:  # noqa: BLE001  healthcheck: any failure -> False
            return False
        return True

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

    async def fetch_models[R: Row](self, model: type[R], sql: str, *params: Any) -> list[R]:
        rows = await self._adapter.fetch(sql, *params)
        return [model(**r) for r in rows]

    async def fetch_model[R: Row](self, model: type[R], sql: str, *params: Any) -> R | None:
        row = await self._adapter.fetchrow(sql, *params)
        return _hydrate(model, row)

    async def get[R: Row](self, model: type[R], **filters: Any) -> R | None:
        if _has_empty_collection_filter(filters):
            return None
        q = sql.select_one(model, placeholder=self._adapter.placeholder, **filters)
        return await self.fetch_model(model, q.sql, *q.params)

    async def query[R: Row](
        self,
        model: type[R],
        *,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | list[str] | None = None,
        **filters: Any,
    ) -> list[R]:
        if _has_empty_collection_filter(filters):
            return []
        q = sql.select_many(
            model,
            placeholder=self._adapter.placeholder,
            limit=limit,
            offset=offset,
            order_by=order_by,
            **filters,
        )
        return await self.fetch_models(model, q.sql, *q.params)

    async def iter_rows[R: Row](
        self,
        model: type[R],
        *,
        batch_size: int = 500,
        order_by: str | list[str] | None = None,
        **filters: Any,
    ) -> AsyncIterator[R]:
        """Stream every matching row, paged by `batch_size`. Default
        `order_by` is `__pk__` so pagination stays stable across pages.

        Uses offset pagination, which is O(N^2) over huge tables. For
        full scans of large tables, prefer a raw keyset loop instead.
        """
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if _has_empty_collection_filter(filters):
            return
        if order_by is None:
            order_by = list(model.__pk__)
        offset = 0
        while True:
            page = await self.query(
                model,
                limit=batch_size,
                offset=offset,
                order_by=order_by,
                **filters,
            )
            for row in page:
                yield row
            # Short page means we're past the end; skip the empty
            # round-trip the next iteration would do.
            if len(page) < batch_size:
                return
            offset += batch_size

    async def iter_rows_keyset[R: Row](
        self,
        model: type[R],
        *,
        by: str | None = None,
        batch_size: int = 500,
        **filters: Any,
    ) -> AsyncIterator[R]:
        """Stream every matching row via keyset pagination.

        Unlike `iter_rows`, per-page cost stays constant: each page
        reads `batch_size` rows via `WHERE (by, *pk_tail) > (...)`
        rather than `OFFSET N`, so total cost is O(N) instead of
        O(N^2). `by` must be a single DB-orderable column. PK columns
        break ties on `by` and let the cursor paginate through NULL
        values; the cursor is `[by] + [p for p in __pk__ if p != by]`.

        `by` defaults to `model.__pk__[0]` (composite PK requires
        explicit `by=`). Filters are AND'd with the cursor. Ordering
        is ascending with `NULLS FIRST` -- NULL rows iterate first
        on every backend. Descending is not supported. SQLite 3.30+
        is required (for the `NULLS FIRST` keyword).
        """
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if _has_empty_collection_filter(filters):
            return
        if by is None:
            if len(model.__pk__) > 1:
                raise ValueError(
                    f"{model.__name__} has composite PK {model.__pk__}; "
                    f"pass `by=<column>` explicitly."
                )
            by = model.__pk__[0]

        order_cols = [by] + [p for p in model.__pk__ if p != by]
        last_seen: list[Any] | None = None
        while True:
            q = sql.select_keyset(
                model,
                placeholder=self._adapter.placeholder,
                by=by,
                batch_size=batch_size,
                last_seen=last_seen,
                **filters,
            )
            rows = await self._adapter.fetch(q.sql, *q.params)
            if not rows:
                return
            for row in rows:
                yield model(**row)
            if len(rows) < batch_size:
                return
            last_seen = [rows[-1][c] for c in order_cols]

    async def insert[R: Row](self, row: R, *, on_conflict: sql._OnConflict = None) -> R:
        """Insert `row` and return the DB's view (RETURNING *).

        `on_conflict="ignore"` appends `ON CONFLICT DO NOTHING`; if a
        conflict happens, RETURNING is empty and the input `row` is
        returned unchanged (so server-defaults are NOT populated).
        `on_conflict="upsert"` appends `ON CONFLICT (<pk>) DO UPDATE
        SET <non-pk> = excluded.<non-pk>`, so the returned row always
        reflects the DB's view. Inc / Now sentinels do not compose
        with upsert; for create-or-increment, drop to raw SQL.
        """
        q = sql.insert(row, placeholder=self._adapter.placeholder, on_conflict=on_conflict)
        result = await self._adapter.fetchrow(q.sql, *q.params)
        return _hydrate(row, result) or row

    async def insert_many(
        self,
        rows: Sequence[Row],
        *,
        on_conflict: sql._OnConflict = None,
    ) -> None:
        """Insert many rows in one or more multi-VALUES statements.

        All rows must be the same Row subclass and share
        `model_fields_set`; the whole batch is checked before anything
        is sent. Long batches are chunked at the driver's parameter
        limit so a single call can cover thousands of rows; a batch
        that needs several statements runs them in one transaction. `on_conflict="ignore"` appends
        `ON CONFLICT DO NOTHING`; `on_conflict="upsert"` appends
        `ON CONFLICT (<pk>) DO UPDATE SET <non-pk> = excluded.<non-pk>`.
        Inc / Now sentinels do not compose with upsert; for
        create-or-increment, drop to raw SQL. Empty `rows` is a
        no-op."""
        if not rows:
            return
        chunk_size = _PARAM_LIMIT // len(sql.insert_many_fields(rows))
        async with self._chunked(len(rows), chunk_size) as adapter:
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                q = sql.insert_many(chunk, placeholder=adapter.placeholder, on_conflict=on_conflict)
                await adapter.execute(q.sql, *q.params)

    @asynccontextmanager
    async def _chunked(self, total: int, chunk_size: int) -> AsyncIterator[AdapterBase]:
        """Yield the adapter a chunked batch should run on. A batch
        that needs more than one statement gets a transaction (a
        savepoint when already inside one), so a failure in a later
        chunk does not leave the earlier ones applied."""
        if total <= chunk_size:
            yield self._adapter
            return
        async with self._adapter.transaction() as tx_adapter:
            yield tx_adapter

    async def delete_many(
        self,
        model: type[Row],
        pk_values: Sequence[Any],
    ) -> None:
        """Delete many rows by primary key.

        For single-column PK, pass scalar values; for composite PK,
        pass a list of mappings. Long batches are chunked at the
        driver's parameter limit and run in one transaction. Empty
        `pk_values` is a no-op."""
        if not pk_values:
            return
        chunk_size = _PARAM_LIMIT // len(model.__pk__)
        async with self._chunked(len(pk_values), chunk_size) as adapter:
            for i in range(0, len(pk_values), chunk_size):
                chunk = pk_values[i : i + chunk_size]
                q = sql.delete_many(model, chunk, placeholder=adapter.placeholder)
                await adapter.execute(q.sql, *q.params)

    async def update[R: Row](self, row: R, *, where: Mapping[str, Any] | None = None) -> R | None:
        """Update `row` by PK, AND'd with `where=`. Returns the updated
        row, or None if PK + `where=` matched no row."""
        if _has_empty_collection_filter(where):
            return None
        q = sql.update(row, placeholder=self._adapter.placeholder, returning="*", where=where)
        result = await self._adapter.fetchrow(q.sql, *q.params)
        return _hydrate(row, result)

    async def update_where(self, row: Row, *, where: Mapping[str, Any]) -> int:
        """Bulk-update rows scoped entirely by `where=`. Returns the
        affected-row count.

        The row's PK is not part of the WHERE clause; it supplies only
        the SET fields. Common shape:

            n = await db.update_where(
                User.patch(status="archived"),
                where={"id": [1, 2, 3]},
            )

        `where=` must be non-empty; for "update every row" use
        `db.execute` with raw SQL. For single-row updates by PK, use
        `db.update`."""
        if _has_empty_collection_filter(where):
            return 0
        q = sql.update_where(row, placeholder=self._adapter.placeholder, where=where)
        return await self._adapter.execute(q.sql, *q.params)

    async def delete(self, row: Row, *, where: Mapping[str, Any] | None = None) -> None:
        """Delete `row` by PK, AND'd with `where=`."""
        if _has_empty_collection_filter(where):
            return
        q = sql.delete(row, placeholder=self._adapter.placeholder, where=where)
        await self._adapter.execute(q.sql, *q.params)

    def compose(
        self,
        op: sql.Op,
        *args: Any,
        **kwargs: Any,
    ) -> SqlQuery:
        """Return the SqlQuery a typed op would produce, without executing.

        Thin wrapper over `etchdb.sql.compose` that fills in `placeholder`
        from the live adapter. Use `sql.compose(...)` directly when you
        don't have a DB instance.
        """
        return sql.compose(op, *args, placeholder=self._adapter.placeholder, **kwargs)

    async def migrate(self, directory: str | Path) -> int:
        """Apply every pending `.sql` file in `directory`. Returns the
        count applied. See `etchdb.migrations` for the conventions
        (filename ordering, transaction wrapping, drift / disappearance
        rejection).
        """
        from etchdb.migrations import migrate as _migrate

        return await _migrate(self, directory)

    async def migration_status(self, directory: str | Path) -> MigrationStatus:
        """Compare the migrations directory against the tracking table.

        Creates the tracking table on first call (idempotent). Returns
        a `MigrationStatus` listing pending / applied / drifted /
        missing filenames; check `is_consistent` before relying on
        the state.
        """
        from etchdb.migrations import status as _status

        return await _status(self, directory)
