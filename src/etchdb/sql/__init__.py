"""Dialect-neutral SQL emitter.

Generates SqlQuery values for typed Row operations. Driver-free: knows
nothing about asyncpg, psycopg, or aiosqlite. Each adapter passes its
own `placeholder` callable so the same emitter works for both Postgres
($1, $2, ...) and SQLite (?, ?, ...).
"""

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

from etchdb.expr import _Expr
from etchdb.query import SqlQuery
from etchdb.row import Row

_OnConflict = Literal["ignore", "upsert"] | None


def insert(
    row: Row,
    *,
    placeholder: Callable[[int], str],
    returning: str | list[str] | None = "*",
    on_conflict: _OnConflict = None,
) -> SqlQuery:
    """Build an INSERT for `row`, emitting only fields in `model_fields_set`.

    Defaulted fields are omitted so the database applies its own DEFAULT
    (or sequence). An explicit `None` is treated as set. With nothing
    set, emits `INSERT INTO ... DEFAULT VALUES`. `returning="*"` by
    default; pass `None` or a column list to override.

    `on_conflict="ignore"` appends `ON CONFLICT DO NOTHING`.
    `on_conflict="upsert"` appends `ON CONFLICT (<pk>) DO UPDATE SET
    <non-pk> = excluded.<non-pk>`, so the row is inserted or updated
    in place. Both work on Postgres and SQLite (3.24+); upsert with
    RETURNING also requires SQLite 3.35+.
    """
    cls = type(row)
    table = _table_name(row)
    fields = [f for f in cls.model_fields if f in row.model_fields_set]

    if not fields:
        sql = f"INSERT INTO {table} DEFAULT VALUES"
        sql += _on_conflict_clause(cls, fields, on_conflict)
        if returning is not None:
            sql += f" RETURNING {_format_columns(returning)}"
        return SqlQuery(sql=sql, params=[])

    values = [getattr(row, f) for f in fields]
    _reject_exprs("insert", fields, values)
    columns = ", ".join(fields)
    placeholders = ", ".join(placeholder(i) for i in range(len(fields)))

    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    sql += _on_conflict_clause(cls, fields, on_conflict)
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

    Filters are joined with AND. A `None` value emits `IS NULL`.
    `select_one` is the single-row verb, so list / tuple filters are
    rejected (they would produce `IN (...) LIMIT 1` and silently
    return "the first match", which is rarely the intent); use
    `select_many` for those instead. Pass no filters to fetch the
    first row in the table (mostly useful for tests / single-row
    tables).
    """
    list_fields = [f for f, v in filters.items() if isinstance(v, list | tuple)]
    if list_fields:
        raise ValueError(
            f"select_one does not accept list / tuple filters: {list_fields}. "
            f"Use select_many (or db.query) for IN-style filters; "
            f"select_one is the single-row verb."
        )

    table = _table_name(row_class)
    columns = ", ".join(row_class.model_fields)

    where_sql, params = _where_clauses(filters, placeholder=placeholder)
    sql = f"SELECT {columns} FROM {table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    sql += " LIMIT 1"

    return SqlQuery(sql=sql, params=params)


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

    Filters (keyword arguments) are joined with AND. `None` emits
    `IS NULL`; list / tuple emits `IN (...)`; anything else binds as
    `= $N`. `limit`, `offset`, and `order_by` are keyword-only.
    `limit` and `offset` are bound as parameters; `order_by` is
    interpolated as a raw SQL fragment, so do not pass user-controlled
    values to it.
    """
    table = _table_name(row_class)
    columns = ", ".join(row_class.model_fields)

    where_sql, params = _where_clauses(filters, placeholder=placeholder)

    sql = f"SELECT {columns} FROM {table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    if order_by:
        sql += f" ORDER BY {_format_columns(order_by)}"
    if limit is not None:
        n = int(limit)
        if n < 0:
            raise ValueError(f"limit must be >= 0, got {n}")
        params.append(n)
        sql += f" LIMIT {placeholder(len(params) - 1)}"
    if offset is not None:
        n = int(offset)
        if n < 0:
            raise ValueError(f"offset must be >= 0, got {n}")
        params.append(n)
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
    extra equality filters from `where=`; a `None` in `where=` emits
    `IS NULL`. The common multi-tenant pattern is
    `update(row, where={"user_id": current_user_id})` so the update is
    atomic with the ownership check.

    Raises ValueError if any PK field is unset, if no non-PK field is
    set, or if `where=` keys overlap with `__pk__`. Pass `returning="*"`
    to add RETURNING.
    """
    table = _table_name(row)
    pk_set = set(row.__pk__)
    where_items = _pk_where_items(row, "update", where)

    set_fields = [
        f for f in type(row).model_fields if f in row.model_fields_set and f not in pk_set
    ]
    if not set_fields:
        raise ValueError(f"{type(row).__name__} has no non-PK fields to update")

    set_items = {f: getattr(row, f) for f in set_fields}
    set_sql, set_values = _set_clauses(set_items, placeholder=placeholder)
    where_sql, where_values = _where_clauses(
        where_items, placeholder=placeholder, start=len(set_values)
    )

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
    AND'd with extra equality filters from `where=`. A `None` in
    `where=` emits `IS NULL`.

    Raises ValueError if any PK field is unset (no row to identify),
    or if `where=` keys overlap with `__pk__`.
    """
    table = _table_name(row)
    where_items = _pk_where_items(row, "delete", where)

    where_sql, where_values = _where_clauses(where_items, placeholder=placeholder)
    sql = f"DELETE FROM {table} WHERE {where_sql}"
    return SqlQuery(sql=sql, params=where_values)


def insert_many(
    rows: Sequence[Row],
    *,
    placeholder: Callable[[int], str],
    on_conflict: _OnConflict = None,
) -> SqlQuery:
    """Build a multi-VALUES INSERT for `rows`.

    All rows must have identical `model_fields_set`; mixed shapes
    raise. `on_conflict="ignore"` appends `ON CONFLICT DO NOTHING`;
    `on_conflict="upsert"` appends `ON CONFLICT (<pk>) DO UPDATE SET
    <non-pk> = excluded.<non-pk>`. Both work on Postgres and SQLite
    (3.24+). For richer conflict handling, drop to raw SQL.

    The DB facade chunks long batches at the driver's parameter limit
    before calling this. This emitter builds one query per chunk.
    """
    if not rows:
        raise ValueError("insert_many requires at least one row")

    first = rows[0]
    first_cls = type(first)
    table = _table_name(first)
    fields = [f for f in first_cls.model_fields if f in first.model_fields_set]
    if not fields:
        raise ValueError("insert_many requires at least one field set on the first row")

    expected = first.model_fields_set
    for i, row in enumerate(rows):
        if type(row) is not first_cls:
            raise ValueError(
                f"insert_many: all rows must be the same Row subclass; "
                f"row[0] is {first_cls.__name__}, row[{i}] is {type(row).__name__}."
            )
        if row.model_fields_set != expected:
            raise ValueError(
                f"insert_many: all rows must share model_fields_set; "
                f"row[0] has {sorted(expected)}, row[{i}] has {sorted(row.model_fields_set)}."
            )

    columns = ", ".join(fields)
    n = len(fields)

    value_groups: list[str] = []
    params: list[Any] = []
    for row in rows:
        group = ", ".join(placeholder(len(params) + i) for i in range(n))
        value_groups.append(f"({group})")
        row_values = [getattr(row, f) for f in fields]
        _reject_exprs("insert_many", fields, row_values)
        params.extend(row_values)

    sql = f"INSERT INTO {table} ({columns}) VALUES {', '.join(value_groups)}"
    sql += _on_conflict_clause(first_cls, fields, on_conflict)

    return SqlQuery(sql=sql, params=params)


def delete_many(
    model: type[Row],
    pk_values: Sequence[Any],
    *,
    placeholder: Callable[[int], str],
) -> SqlQuery:
    """Build a DELETE for many rows by primary key.

    For single-column PK, pass a list of scalar values:
        delete_many(User, [1, 2, 3], placeholder=...)

    For composite PK, pass a list of mappings:
        delete_many(UserRole, [{"user_id": 1, "role_id": 2}, ...], placeholder=...)

    The DB facade chunks long batches before calling this.
    """
    if not pk_values:
        raise ValueError("delete_many requires at least one PK value")

    table = _table_name(model)
    pk_fields = list(model.__pk__)

    if len(pk_fields) == 1:
        col = pk_fields[0]
        placeholders = ", ".join(placeholder(i) for i in range(len(pk_values)))
        sql = f"DELETE FROM {table} WHERE {col} IN ({placeholders})"
        return SqlQuery(sql=sql, params=list(pk_values))

    pk_cols = ", ".join(pk_fields)
    groups: list[str] = []
    params: list[Any] = []
    for pk_value in pk_values:
        if not isinstance(pk_value, Mapping):
            raise ValueError(
                f"Composite PK requires a mapping per row; got {type(pk_value).__name__}"
            )
        missing = set(pk_fields) - set(pk_value.keys())
        if missing:
            raise ValueError(f"Missing PK fields in row: {sorted(missing)}")
        inner = ", ".join(placeholder(len(params) + i) for i in range(len(pk_fields)))
        groups.append(f"({inner})")
        params.extend(pk_value[f] for f in pk_fields)

    sql = f"DELETE FROM {table} WHERE ({pk_cols}) IN ({', '.join(groups)})"
    return SqlQuery(sql=sql, params=params)


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


def _reject_exprs(op: str, fields: Sequence[str], values: Sequence[Any]) -> None:
    """Raise if any value is a column-expression sentinel.

    `Inc` / `Now` only make sense in UPDATE SET clauses (they
    reference the column itself or an SQL function). In INSERT they
    would either be undefined (`Inc` on a row that doesn't exist
    yet) or a footgun, so reject them at the call site.
    """
    bad = [f for f, v in zip(fields, values, strict=True) if isinstance(v, _Expr)]
    if bad:
        raise ValueError(
            f"Cannot {op} with column-expression sentinels (Inc / Now) on "
            f"{bad}. Use them with db.update on an existing row instead."
        )


def _on_conflict_clause(
    model: type[Row],
    set_fields: list[str],
    mode: _OnConflict,
) -> str:
    """Render the ON CONFLICT tail for `insert` / `insert_many`.

    `set_fields` is the column list emitted by the surrounding INSERT
    in declaration order. For `mode="upsert"`, every non-PK column
    from that list is written via `excluded.col`; PK columns supply
    the conflict target.
    """
    if mode is None:
        return ""
    if mode == "ignore":
        return " ON CONFLICT DO NOTHING"
    if mode == "upsert":
        pk = list(model.__pk__)
        non_pk = [f for f in set_fields if f not in pk]
        if not non_pk:
            raise ValueError(
                "on_conflict='upsert' needs at least one non-PK field set; "
                "this row has only PK columns. Use on_conflict='ignore' instead."
            )
        target = ", ".join(pk)
        updates = ", ".join(f"{f} = excluded.{f}" for f in non_pk)
        return f" ON CONFLICT ({target}) DO UPDATE SET {updates}"
    raise ValueError(f"on_conflict={mode!r} is not supported. Pass None, 'ignore', or 'upsert'.")


def _pk_where_items(row: Row, op: str, where: Mapping[str, Any] | None) -> dict[str, Any]:
    """Validate PK and merge it with `where=` into the WHERE filter dict.

    Asserts every PK field is set on `row` and that `where=` does not
    re-specify a PK field. Returns `{**pk_values, **where}` in PK-first
    order so placeholder numbering is stable.
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
    items: dict[str, Any] = {f: getattr(row, f) for f in row.__pk__}
    if where:
        items.update(where)
    return items


def _table_name(row_or_class: Row | type[Row]) -> str:
    cls = row_or_class if isinstance(row_or_class, type) else type(row_or_class)
    table = getattr(cls, "__table__", None)
    if not table:
        raise ValueError(
            f"{cls.__name__} has no __table__ attribute. "
            "Set `__table__ = 'your_table_name'` on the Row subclass."
        )
    return table


def _set_clauses(
    items: Mapping[str, Any],
    *,
    placeholder: Callable[[int], str],
    start: int = 0,
) -> tuple[str, list[Any]]:
    """Build a SET clause body. Plain values bind as `field = $N`;
    `_Expr` values render via their own SQL fragment and may consume
    zero or more placeholders.

    Returns `(sql_body, values)` in placeholder order. Values may be
    shorter than `items` when an expression renders without bound
    parameters (e.g. `Now()` -> `CURRENT_TIMESTAMP`).
    """
    parts: list[str] = []
    values: list[Any] = []
    for field, value in items.items():
        if isinstance(value, _Expr):
            rhs, expr_values = value.render(field, placeholder, start + len(values))
            parts.append(f"{field} = {rhs}")
            values.extend(expr_values)
        else:
            parts.append(f"{field} = {placeholder(start + len(values))}")
            values.append(value)
    return ", ".join(parts), values


def _where_clauses(
    items: Mapping[str, Any],
    *,
    placeholder: Callable[[int], str],
    start: int = 0,
) -> tuple[str, list[Any]]:
    """Build a WHERE-clause body. None becomes `IS NULL`; list/tuple
    becomes `IN (...)`; everything else becomes `field = $N`.

    Returns `(sql_body, values)`. `IS NULL` keys consume no
    placeholder, so `values` may be shorter than `items`. `start` is
    the surrounding parameter-list index at which this clause's first
    bound parameter sits, for Postgres `$N` numbering when SET
    precedes WHERE.
    """
    parts: list[str] = []
    values: list[Any] = []
    for field, value in items.items():
        if value is None:
            parts.append(f"{field} IS NULL")
        elif isinstance(value, list | tuple):
            if not value:
                raise ValueError(
                    f"empty list filter for {field!r}; an empty IN clause matches "
                    f"nothing. Drop the filter, or branch on the empty case."
                )
            if any(v is None for v in value):
                raise ValueError(
                    f"None inside a list filter for {field!r} is ambiguous; SQL "
                    f"IN does not match NULL. Use raw SQL for "
                    f"`field IN (...) OR field IS NULL`."
                )
            ph_list = ", ".join(placeholder(start + len(values) + i) for i in range(len(value)))
            parts.append(f"{field} IN ({ph_list})")
            values.extend(value)
        else:
            parts.append(f"{field} = {placeholder(start + len(values))}")
            values.append(value)
    return " AND ".join(parts), values


def _format_columns(cols: str | list[str]) -> str:
    if isinstance(cols, str):
        return cols
    return ", ".join(cols)
