"""Postgres dialect SQL emitter.

Generates SqlQuery values for typed Row operations. This module is
driver-free: it knows nothing about asyncpg or psycopg.

Placeholder style: `$1, $2, ...` (Postgres native).
"""

from typing import Any

from etchdb.query import SqlQuery
from etchdb.row import Row


def insert(row: Row, *, returning: str | list[str] | None = "*") -> SqlQuery:
    """Build an INSERT statement for `row`.

    By default the query ends with `RETURNING *` so the caller can pick up
    server-generated values (serial PKs, default timestamps, etc). Pass
    `returning=None` to omit the clause, or a list of column names to
    select specific columns.
    """
    table = _table_name(row)
    fields = list(type(row).model_fields)
    values = [getattr(row, f) for f in fields]

    columns = ", ".join(fields)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(fields)))

    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    if returning is not None:
        sql += f" RETURNING {_format_columns(returning)}"

    return SqlQuery(sql=sql, params=values)


def select_one(row_class: type[Row], **filters: Any) -> SqlQuery:
    """Build a SELECT for at most one row matching `filters`.

    Filters are joined with AND. Pass no filters to fetch the first row
    in the table (mostly useful for tests / single-row tables).
    """
    table = _table_name(row_class)
    columns = ", ".join(row_class.model_fields)

    where_sql = _eq_clauses(list(filters))
    sql = f"SELECT {columns} FROM {table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    sql += " LIMIT 1"

    return SqlQuery(sql=sql, params=list(filters.values()))


def select_many(
    row_class: type[Row],
    *,
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

    where_sql = _eq_clauses(list(filters))
    params: list[Any] = list(filters.values())

    sql = f"SELECT {columns} FROM {table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    if order_by:
        sql += f" ORDER BY {_format_columns(order_by)}"
    if limit is not None:
        params.append(int(limit))
        sql += f" LIMIT ${len(params)}"
    if offset is not None:
        params.append(int(offset))
        sql += f" OFFSET ${len(params)}"

    return SqlQuery(sql=sql, params=params)


def update(row: Row, *, returning: str | list[str] | None = None) -> SqlQuery:
    """Build an UPDATE for `row` keyed by its primary key.

    All non-PK fields go into the SET clause. Pass `returning="*"` (or a
    list of column names) to add a RETURNING clause.
    """
    table = _table_name(row)
    pk_set = set(row.__pk__)
    set_fields = [f for f in type(row).model_fields if f not in pk_set]

    if not set_fields:
        raise ValueError(f"{type(row).__name__} has no non-PK fields to update")

    set_sql = _eq_clauses(set_fields, sep=", ")
    pk_sql = _eq_clauses(list(row.__pk__), start=len(set_fields) + 1)

    set_values = [getattr(row, f) for f in set_fields]
    pk_values = [getattr(row, f) for f in row.__pk__]

    sql = f"UPDATE {table} SET {set_sql} WHERE {pk_sql}"
    if returning is not None:
        sql += f" RETURNING {_format_columns(returning)}"

    return SqlQuery(sql=sql, params=set_values + pk_values)


def delete(row: Row) -> SqlQuery:
    """Build a DELETE for `row` keyed by its primary key."""
    table = _table_name(row)
    pk_sql = _eq_clauses(list(row.__pk__))
    pk_values = [getattr(row, f) for f in row.__pk__]

    sql = f"DELETE FROM {table} WHERE {pk_sql}"
    return SqlQuery(sql=sql, params=pk_values)


# --- helpers ----------------------------------------------------------


def _table_name(row_or_class: Row | type[Row]) -> str:
    cls = row_or_class if isinstance(row_or_class, type) else type(row_or_class)
    table = getattr(cls, "__table__", None)
    if not table:
        raise ValueError(
            f"{cls.__name__} has no __table__ attribute. "
            "Set `__table__ = 'your_table_name'` on the Row subclass."
        )
    return table


def _eq_clauses(fields: list[str], *, start: int = 1, sep: str = " AND ") -> str:
    """Build `field1 = $START AND field2 = $START+1 ...` clauses."""
    if not fields:
        return ""
    return sep.join(f"{f} = ${start + i}" for i, f in enumerate(fields))


def _format_columns(cols: str | list[str]) -> str:
    if isinstance(cols, str):
        return cols
    return ", ".join(cols)
