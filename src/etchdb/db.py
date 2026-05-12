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
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from etchdb import sql

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping, Sequence
    from pathlib import Path

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


class DB:
    """User-facing DB facade."""

    def __init__(self, adapter: AdapterBase):
        self._adapter = adapter

    async def __aenter__(self) -> DB:
        """Allow `async with db:` on an already-open DB instance.

        Note: `async with DB.from_url(...) as db:` does NOT work --
        `from_url` is a coroutine; you can't combine it with `async
        with` in one expression. Open, then enter:

            db = await DB.from_url(url)
            async with db:
                ...
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
    ) -> DB:
        """Open a DB from a URL, dispatching on the URL scheme.

        Supported schemes:
          postgresql://, postgres://, postgresql+asyncpg://  -> asyncpg
          postgresql+psycopg://                              -> psycopg
          sqlite:///, sqlite+aiosqlite:///                   -> aiosqlite

        Driver subpackages are imported lazily so users only need the
        driver they actually use installed.

        `min_size` / `max_size` forward to the underlying Postgres pool
        and are ignored on aiosqlite (no pool concept). For knobs
        beyond size, construct the pool yourself and use the adapter's
        `from_pool` directly.
        """
        scheme = urlparse(url).scheme.lower()
        pool_kwargs = {"min_size": min_size, "max_size": max_size}

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
            from etchdb.aiosqlite import AiosqliteAdapter

            adapter = await AiosqliteAdapter.from_url(url, **pool_kwargs)
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

        Returns `True` on success. Raises the usual etchdb exception
        family (`OperationalError`, etc.) on failure -- the same
        signal any other query would surface. Useful for liveness
        probes in containers and health endpoints in web apps.
        """
        await self._adapter.fetchval("SELECT 1")
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

    async def fetch_models(self, model: type[Row], sql: str, *params: Any) -> list[Row]:
        rows = await self._adapter.fetch(sql, *params)
        return [model(**r) for r in rows]

    async def fetch_model(self, model: type[Row], sql: str, *params: Any) -> Row | None:
        row = await self._adapter.fetchrow(sql, *params)
        return _hydrate(model, row)

    async def get(self, model: type[Row], **filters: Any) -> Row | None:
        if _has_empty_collection_filter(filters):
            return None
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

    async def iter_rows(
        self,
        model: type[Row],
        *,
        batch_size: int = 500,
        order_by: str | list[str] | None = None,
        **filters: Any,
    ) -> AsyncIterator[Row]:
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

    async def iter_rows_keyset(
        self,
        model: type[Row],
        *,
        by: str | None = None,
        batch_size: int = 500,
        **filters: Any,
    ) -> AsyncIterator[Row]:
        """Stream every matching row via keyset pagination.

        Unlike `iter_rows`, the per-page cost stays constant -- each
        page reads `batch_size` rows via `WHERE <by> > <last_seen>`
        rather than `OFFSET N`, so total cost is O(N) instead of
        O(N^2). The trade-off is the constraint on `by`:

        - It must be a single column (composite-PK keyset uses
          `(a, b) > (last_a, last_b)` and isn't supported here).
        - It must be monotonic-ordered and unique enough that no two
          rows tie. Primary keys usually qualify; created_at columns
          can if the resolution is high enough.

        Defaults to `model.__pk__[0]`. Filters are AND'd with the
        cursor. Ordering is ascending; descending is not supported.
        """
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if _has_empty_collection_filter(filters):
            return
        if by is None:
            if len(model.__pk__) > 1:
                raise ValueError(
                    f"{model.__name__} has a composite primary key "
                    f"{model.__pk__}; pass `by=<column>` explicitly. "
                    f"Single-column keyset on a tied prefix can silently "
                    f"duplicate or skip rows."
                )
            by = model.__pk__[0]
        if by not in sql._db_fields(model):
            raise ValueError(
                f"keyset column {by!r} is not a DB column of "
                f"{model.__name__} (or is marked __fields_not_in_db__); "
                f"keyset pagination needs to read it back from each row."
            )
        if by in filters:
            raise ValueError(
                f"keyset column {by!r} cannot also appear in filters; "
                f"the cursor already constrains that column."
            )

        ph = self._adapter.placeholder
        columns = ", ".join(sql._db_fields(model))
        table = model.__table__
        last_seen: Any = None

        while True:
            params: list[Any] = []
            where_parts: list[str] = []
            if filters:
                filter_sql, filter_values = sql._where_clauses(filters, placeholder=ph)
                if filter_sql:
                    where_parts.append(filter_sql)
                params.extend(filter_values)
            if last_seen is not None:
                where_parts.append(f"{by} > {ph(len(params))}")
                params.append(last_seen)

            sql_str = f"SELECT {columns} FROM {table}"
            if where_parts:
                sql_str += f" WHERE {' AND '.join(where_parts)}"
            sql_str += f" ORDER BY {by}"
            params.append(batch_size)
            sql_str += f" LIMIT {ph(len(params) - 1)}"

            rows = await self._adapter.fetch(sql_str, *params)
            if not rows:
                return
            for row in rows:
                yield model(**row)
            if len(rows) < batch_size:
                return
            last_seen = rows[-1][by]

    async def insert(self, row: Row, *, on_conflict: sql._OnConflict = None) -> Row:
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

        All rows must share `model_fields_set`. Long batches are
        chunked at the driver's parameter limit so a single call can
        cover thousands of rows. `on_conflict="ignore"` appends
        `ON CONFLICT DO NOTHING`; `on_conflict="upsert"` appends
        `ON CONFLICT (<pk>) DO UPDATE SET <non-pk> = excluded.<non-pk>`.
        Inc / Now sentinels do not compose with upsert; for
        create-or-increment, drop to raw SQL. Empty `rows` is a
        no-op."""
        if not rows:
            return
        cols_per_row = sum(1 for f in type(rows[0]).model_fields if f in rows[0].model_fields_set)
        if cols_per_row == 0:
            raise ValueError("insert_many requires at least one field set on the first row")
        chunk_size = _PARAM_LIMIT // cols_per_row
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            q = sql.insert_many(
                chunk, placeholder=self._adapter.placeholder, on_conflict=on_conflict
            )
            await self._adapter.execute(q.sql, *q.params)

    async def delete_many(
        self,
        model: type[Row],
        pk_values: Sequence[Any],
    ) -> None:
        """Delete many rows by primary key.

        For single-column PK, pass scalar values; for composite PK,
        pass a list of mappings. Long batches are chunked at the
        driver's parameter limit. Empty `pk_values` is a no-op."""
        if not pk_values:
            return
        chunk_size = _PARAM_LIMIT // len(model.__pk__)
        for i in range(0, len(pk_values), chunk_size):
            chunk = pk_values[i : i + chunk_size]
            q = sql.delete_many(model, chunk, placeholder=self._adapter.placeholder)
            await self._adapter.execute(q.sql, *q.params)

    async def update(self, row: Row, *, where: Mapping[str, Any] | None = None) -> Row | None:
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
