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
    Lets-or-truth-be-told: nothing forbids a non-int `by`, so the
    same shape works for any column whose type supports `+` (e.g.
    interval columns, decimal accumulators).
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
    """

    __slots__ = ()

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        return "CURRENT_TIMESTAMP", []
