"""Dialect-neutral SQL emitter.

Generates SqlQuery values for typed Row operations. Driver-free: knows
nothing about asyncpg, psycopg, or aiosqlite. Each adapter passes its
own `placeholder` callable so the same emitter works for both Postgres
($1, $2, ...) and SQLite (?, ?, ...).
"""

from collections.abc import Callable, Mapping
from typing import Any, Literal

from etchdb.query import SqlQuery
from etchdb.row import Row


def insert(
    row: Row,
    *,
    placeholder: Callable[[int], str],
    returning: str | list[str] | None = "*",
) -> SqlQuery:
    """Build an INSERT for `row`, emitting only fields in `model_fields_set`.

    Defaulted fields are omitted so the database applies its own DEFAULT
    (or sequence). An explicit `None` is treated as set. With nothing
    set, emits `INSERT INTO ... DEFAULT VALUES`. `returning="*"` by
    default; pass `None` or a column list to override.
    """
    table = _table_name(row)
    fields = [f for f in type(row).model_fields if f in row.model_fields_set]

    if not fields:
        sql = f"INSERT INTO {table} DEFAULT VALUES"
        if returning is not None:
            sql += f" RETURNING {_format_columns(returning)}"
        return SqlQuery(sql=sql, params=[])

    values = [getattr(row, f) for f in fields]
    columns = ", ".join(fields)
    placeholders = ", ".join(placeholder(i) for i in range(len(fields)))

    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    if returning is not None:
        sql += f" RETURNING {_format_columns(returning)}"

    return SqlQuery(sql=sql, params=values)


def select_one(
    row_class: type[Row],
    *,
    placeholder: Callable[[int], str],
    **filters: Any,
) -> SqlQuery:
    """Build a SELECT for at most one row matching `filters`.

    Filters are joined with AND. Pass no filters to fetch the first row
    in the table (mostly useful for tests / single-row tables).
    """
    table = _table_name(row_class)
    columns = ", ".join(row_class.model_fields)

    where_sql = _eq_clauses(list(filters), placeholder=placeholder)
    sql = f"SELECT {columns} FROM {table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    sql += " LIMIT 1"

    return SqlQuery(sql=sql, params=list(filters.values()))


def select_many(
    row_class: type[Row],
    *,
    placeholder: Callable[[int], str],
    limit: int | None = None,
    offset: int | None = None,
    order_by: str | list[str] | None = None,
    **filters: Any,
) -> SqlQuery:
    """Build a SELECT for multiple rows.

    Filters (keyword arguments) are joined with AND. `limit`, `offset`,
    and `order_by` are keyword-only. `limit` and `offset` are bound as
    parameters; `order_by` is interpolated as a raw SQL fragment, so do
    not pass user-controlled values to it.
    """
    table = _table_name(row_class)
    columns = ", ".join(row_class.model_fields)

    where_sql = _eq_clauses(list(filters), placeholder=placeholder)
    params: list[Any] = list(filters.values())

    sql = f"SELECT {columns} FROM {table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    if order_by:
        sql += f" ORDER BY {_format_columns(order_by)}"
    if limit is not None:
        params.append(int(limit))
        sql += f" LIMIT {placeholder(len(params) - 1)}"
    if offset is not None:
        params.append(int(offset))
        sql += f" OFFSET {placeholder(len(params) - 1)}"

    return SqlQuery(sql=sql, params=params)


def update(
    row: Row,
    *,
    placeholder: Callable[[int], str],
    returning: str | list[str] | None = None,
    where: Mapping[str, Any] | None = None,
) -> SqlQuery:
    """Build an UPDATE for `row` keyed by its primary key.

    Only fields in `model_fields_set` go into the SET clause, giving
    partial-update semantics: columns the caller didn't touch are
    preserved. WHERE uses every field in `__pk__`, AND'd with any
    extra equality filters from `where=`. The common multi-tenant
    pattern is `update(row, where={"user_id": current_user_id})` so
    the update is atomic with the ownership check.

    Raises ValueError if any PK field is unset, if no non-PK field is
    set, or if `where=` keys overlap with `__pk__`. Pass `returning="*"`
    to add RETURNING.
    """
    table = _table_name(row)
    pk_set = set(row.__pk__)
    where_fields, where_values = _pk_where(row, "update", where)

    set_fields = [
        f for f in type(row).model_fields if f in row.model_fields_set and f not in pk_set
    ]
    if not set_fields:
        raise ValueError(f"{type(row).__name__} has no non-PK fields to update")

    set_sql = _eq_clauses(set_fields, placeholder=placeholder, sep=", ")
    where_sql = _eq_clauses(where_fields, placeholder=placeholder, start=len(set_fields))

    set_values = [getattr(row, f) for f in set_fields]

    sql = f"UPDATE {table} SET {set_sql} WHERE {where_sql}"
    if returning is not None:
        sql += f" RETURNING {_format_columns(returning)}"

    return SqlQuery(sql=sql, params=set_values + where_values)


def delete(
    row: Row,
    *,
    placeholder: Callable[[int], str],
    where: Mapping[str, Any] | None = None,
) -> SqlQuery:
    """Build a DELETE for `row` keyed by its primary key, optionally
    AND'd with extra equality filters from `where=`.

    Raises ValueError if any PK field is unset (no row to identify),
    or if `where=` keys overlap with `__pk__`.
    """
    table = _table_name(row)
    where_fields, where_values = _pk_where(row, "delete", where)

    where_sql = _eq_clauses(where_fields, placeholder=placeholder)
    sql = f"DELETE FROM {table} WHERE {where_sql}"
    return SqlQuery(sql=sql, params=where_values)


_OPS = {
    "get": select_one,
    "query": select_many,
    "insert": insert,
    "update": update,
    "delete": delete,
}


def compose(
    op: Literal["get", "query", "insert", "update", "delete"],
    *args: Any,
    placeholder: Callable[[int], str],
    **kwargs: Any,
) -> SqlQuery:
    """Build the SqlQuery for a typed op without a live adapter.

    Useful in tests, scripts, and any code that wants to inspect SQL
    without opening a connection. The DB facade's `compose` method is
    a thin wrapper that fills in `placeholder` from the live adapter
    so callers don't have to.

        from etchdb import sql

        pg = lambda i: f"${i + 1}"
        q = sql.compose("get", User, id=1, placeholder=pg)
        assert q.sql == "SELECT id, name FROM users WHERE id = $1 LIMIT 1"
    """
    try:
        fn = _OPS[op]
    except KeyError as e:
        raise ValueError(f"Unknown op {op!r}. Expected one of: {sorted(_OPS)}") from e
    return fn(*args, placeholder=placeholder, **kwargs)


# --- helpers ----------------------------------------------------------


def _pk_where(row: Row, op: str, where: Mapping[str, Any] | None) -> tuple[list[str], list[Any]]:
    """Validate and build the WHERE field+value lists for `update`/`delete`.

    Asserts every PK field is set on `row` (else `WHERE id = NULL`
    silently matches nothing) and that `where=` does not re-specify a
    PK field (else `id = $1 AND id = $2` would either confuse the
    reader or match nothing). Returns ordered (field_names, values),
    PK first, `where=` second.
    """
    unset_pk = [f for f in row.__pk__ if f not in row.model_fields_set]
    if unset_pk:
        raise ValueError(
            f"Cannot {op}: primary key field(s) {unset_pk} not set on this "
            f"{type(row).__name__}. Set them so the row can be identified."
        )
    if where:
        overlap = set(row.__pk__) & where.keys()
        if overlap:
            raise ValueError(
                f"where= keys overlap with __pk__: {sorted(overlap)}. "
                f"PK fields are already in WHERE; remove them from where=."
            )

    fields = list(row.__pk__)
    values: list[Any] = [getattr(row, f) for f in row.__pk__]
    if where:
        fields.extend(where)
        values.extend(where.values())
    return fields, values


def _table_name(row_or_class: Row | type[Row]) -> str:
    cls = row_or_class if isinstance(row_or_class, type) else type(row_or_class)
    table = getattr(cls, "__table__", None)
    if not table:
        raise ValueError(
            f"{cls.__name__} has no __table__ attribute. "
            "Set `__table__ = 'your_table_name'` on the Row subclass."
        )
    return table


def _eq_clauses(
    fields: list[str],
    *,
    placeholder: Callable[[int], str],
    start: int = 0,
    sep: str = " AND ",
) -> str:
    """Build `field1 = ? AND field2 = ? ...` clauses with the given placeholder style.

    `start` is the 0-indexed position in the surrounding parameter list at
    which this clause's parameters begin (matters for Postgres `$N` numbering).
    """
    if not fields:
        return ""
    return sep.join(f"{f} = {placeholder(start + i)}" for i, f in enumerate(fields))


def _format_columns(cols: str | list[str]) -> str:
    if isinstance(cols, str):
        return cols
    return ", ".join(cols)
