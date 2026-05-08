"""Column expression sentinels for UPDATE SET clauses.

Plain values bind as `column = $N`. Expression sentinels render as
raw SQL fragments instead, which is the right shape for atomic
increments and DB-side timestamps that need to evaluate inside the
single statement rather than at the Python call site.

Use these via `Row.patch(...)`: a sentinel does not satisfy the
field's declared type (`int`, `datetime`), so plain Pydantic
construction would raise. `patch` skips validation and lets the
sentinel flow through to the SET emitter.

    from etchdb import Inc, Now, Row

    # SET view_count = view_count + $1 WHERE id = $2
    await db.update(User.patch(id=1, view_count=Inc()))

    # SET updated_at = CURRENT_TIMESTAMP WHERE id = $1
    await db.update(Article.patch(id=1, updated_at=Now()))
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class _Expr:
    """Marker for column expressions in SET clauses.

    Subclasses implement `render(column, placeholder, start)` returning
    `(sql_fragment, params)` where `sql_fragment` is the right-hand
    side of `column = ...` (no leading `column =`) and `params` is the
    list of values bound to placeholders inside that fragment.
    `start` is the surrounding parameter-list index where this
    expression's first bound parameter sits.
    """

    __slots__ = ()

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        raise NotImplementedError


class Inc(_Expr):
    """Atomic increment: `column = column + by`.

    `Inc()` adds 1; `Inc(by=5)` adds 5; `Inc(by=-1)` decrements.
    Nothing forbids a non-int `by`, so the same shape works for any
    column whose type supports `+` (interval columns, decimal
    accumulators).

    Two restrictions:
    - Construct rows that hold an `Inc` via `Row.patch(...)`. A
      sentinel does not pass Pydantic validation for the field's
      declared type, so plain `Row(...)` would raise.
    - `Inc` is accepted by `db.update` only. `db.insert` /
      `db.insert_many` reject it (incrementing a row that does not
      exist yet is undefined). The rejection covers `on_conflict=
      "upsert"` too: the INSERT branch still needs a literal initial
      value. For create-or-increment, drop to raw SQL.
    """

    __slots__ = ("by",)

    def __init__(self, by: Any = 1):
        self.by = by

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        return f"{column} + {placeholder(start)}", [self.by]


class Now(_Expr):
    """DB-side current timestamp: `column = CURRENT_TIMESTAMP`.

    `CURRENT_TIMESTAMP` is SQL-standard and works on both Postgres
    (returns timestamptz) and SQLite (returns ISO-8601 text). Use a
    column type that matches your driver's expectation.

    Two restrictions:
    - Construct rows that hold a `Now` via `Row.patch(...)`. A
      sentinel does not pass Pydantic validation for the field's
      declared type, so plain `Row(...)` would raise.
    - `Now` is accepted by `db.update` only. `db.insert` /
      `db.insert_many` reject it, including under `on_conflict=
      "upsert"`; the INSERT branch still needs a literal initial
      value. For a server-default insert timestamp, prefer a column
      `DEFAULT CURRENT_TIMESTAMP` in the schema.
    """

    __slots__ = ()

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        return "CURRENT_TIMESTAMP", []
