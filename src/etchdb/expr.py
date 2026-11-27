"""Column sentinels: expressions for UPDATE SET clauses and operators
for filters.

In a SET clause, plain values bind as `column = $N`. Expression
sentinels (`Inc`, `Now`) render as raw SQL fragments instead, which is
the right shape for atomic increments and DB-side timestamps that need
to evaluate inside the single statement rather than at the Python call
site.

In a filter or `where=` entry, a plain value means equality, `None`
means `IS NULL` and a list means `IN`. Filter operators (`Gt`, `Ge`,
`Lt`, `Le`, `Range`, `Not`, `Like`, `ILike`, `StartsWith`, `Contains`)
cover the common conditions between that and raw SQL:

    from etchdb import Ge, Not, Range

    # WHERE age >= $1 AND email IS NOT NULL
    await db.query(User, age=Ge(18), email=Not(None))

    # WHERE created_at >= $1 AND created_at < $2
    await db.query(Event, created_at=Range(ge=start, lt=end))

Use the SET sentinels via `Row.patch(...)`: a sentinel does not satisfy the
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
    - `Inc` is accepted by `db.update` and `db.update_where` only.
      `db.insert` /
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
    - `Now` is accepted by `db.update` and `db.update_where` only.
      `db.insert` /
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


# --- filter operators -------------------------------------------------


def _is_sqlite(placeholder: Callable[[int], str]) -> bool:
    """The emitter is driver-free; the `?` placeholder is how it tells
    SQLite from Postgres where the two need different SQL."""
    return placeholder(0) == "?"


def _escape_like(text: str) -> str:
    """Escape LIKE wildcards so `text` matches literally under
    `ESCAPE '\\'`."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _escape_glob(text: str) -> str:
    """Escape SQLite GLOB wildcards by wrapping each in a bracket set."""
    return "".join(f"[{c}]" if c in "*?[" else c for c in text)


def _require_str(op: str, value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{op} takes a str, got {type(value).__name__}")
    return value


class _Predicate:
    """Marker for filter operators: the value side of a filter or
    `where=` entry that is not plain equality.

    Subclasses implement `render(column, placeholder, start)` returning
    `(condition_sql, params)`: the whole condition for `column` and the
    values bound inside it. `start` is the surrounding parameter-list
    index of this condition's first bound parameter.

    Operators compare by type and arguments, so a test can assert
    `mock.assert_called_with(age=Ge(18))`.
    """

    __slots__ = ()

    def _key(self) -> tuple[Any, ...]:
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return self._key() == other._key()

    def __hash__(self) -> int:
        # The arguments may be unhashable (a list in `Not`), so only
        # the type takes part.
        return hash(type(self))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(repr(v) for v in self._key())})"

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        raise NotImplementedError


def _operand(
    op: str, value: Any, placeholder: Callable[[int], str], index: int
) -> tuple[str, list[Any]]:
    """Right-hand side of a comparison: a bound value, or the SQL of a
    `Now()` sentinel."""
    if value is None:
        raise ValueError(
            f"{op}(None) never matches: a comparison with NULL is not true. "
            f"Filter with `None` for IS NULL or `Not(None)` for IS NOT NULL."
        )
    if isinstance(value, Now):
        return value.render("", placeholder, index)
    if isinstance(value, _Expr | _Predicate | list | tuple | set | frozenset):
        raise ValueError(f"{op} takes a single value or Now(), got {type(value).__name__}")
    return placeholder(index), [value]


class _Compare(_Predicate):
    __slots__ = ("value",)
    _symbol = ""

    def __init__(self, value: Any):
        self.value = value

    def _key(self) -> tuple[Any, ...]:
        return (self.value,)

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        rhs, params = _operand(type(self).__name__, self.value, placeholder, start)
        return f"{column} {self._symbol} {rhs}", params


class Gt(_Compare):
    """`column > value`. `Gt(Now())` compares with `CURRENT_TIMESTAMP`."""

    __slots__ = ()
    _symbol = ">"


class Ge(_Compare):
    """`column >= value`."""

    __slots__ = ()
    _symbol = ">="


class Lt(_Compare):
    """`column < value`."""

    __slots__ = ()
    _symbol = "<"


class Le(_Compare):
    """`column <= value`."""

    __slots__ = ()
    _symbol = "<="


class Range(_Predicate):
    """Two bounds on one column: `Range(ge=start, lt=end)` renders
    `column >= $1 AND column < $2`.

    Bounds are keyword-only. Give at most one lower bound (`gt` or
    `ge`) and one upper bound (`lt` or `le`), and at least one bound in
    total. A bound left as `None` is absent, so one optional end of a
    window can be passed straight through; with both ends absent there
    is nothing to filter on and `Range` raises.
    """

    __slots__ = ("_bounds",)
    _SYMBOLS = (("gt", ">"), ("ge", ">="), ("lt", "<"), ("le", "<="))

    def __init__(self, *, gt: Any = None, ge: Any = None, lt: Any = None, le: Any = None):
        if gt is not None and ge is not None:
            raise ValueError("Range takes one lower bound: gt or ge, not both")
        if lt is not None and le is not None:
            raise ValueError("Range takes one upper bound: lt or le, not both")
        given = {"gt": gt, "ge": ge, "lt": lt, "le": le}
        self._bounds = tuple((n, s, given[n]) for n, s in self._SYMBOLS if given[n] is not None)
        if not self._bounds:
            raise ValueError("Range needs at least one bound")

    def _key(self) -> tuple[Any, ...]:
        return self._bounds

    def __repr__(self) -> str:
        return f"Range({', '.join(f'{name}={value!r}' for name, _, value in self._bounds)})"

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        parts: list[str] = []
        params: list[Any] = []
        for _, symbol, value in self._bounds:
            rhs, bound = _operand("Range", value, placeholder, start + len(params))
            parts.append(f"{column} {symbol} {rhs}")
            params.extend(bound)
        return " AND ".join(parts), params


class Not(_Predicate):
    """Negation of the three plain filter forms.

    `Not(value)` renders `column <> value`, `Not(None)` renders
    `column IS NOT NULL`, and `Not([a, b])` renders
    `column NOT IN (a, b)`.

    SQL semantics apply: `<>` and `NOT IN` are not true for a row whose
    column is NULL, so those rows are left out. Add them back with raw
    SQL (`IS DISTINCT FROM` on Postgres, `IS NOT` on SQLite).
    """

    __slots__ = ("value",)

    def __init__(self, value: Any):
        if isinstance(value, _Expr | _Predicate):
            raise ValueError(
                f"Not takes a value, None or a list, got {type(value).__name__}. "
                f"Use raw SQL to negate another operator."
            )
        self.value = value

    def _key(self) -> tuple[Any, ...]:
        return (self.value,)

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        value = self.value
        if value is None:
            return f"{column} IS NOT NULL", []
        if isinstance(value, list | tuple):
            if not value:
                raise ValueError(
                    f"empty list in Not() for {column!r}; an empty NOT IN "
                    f"excludes nothing. Drop the filter, or branch on the empty case."
                )
            if any(v is None for v in value):
                raise ValueError(
                    f"None inside a Not() list for {column!r}: NOT IN with a NULL "
                    f"matches no row. Use raw SQL for the NULL case."
                )
            ph_list = ", ".join(placeholder(start + i) for i in range(len(value)))
            return f"{column} NOT IN ({ph_list})", list(value)
        return f"{column} <> {placeholder(start)}", [value]


class _Pattern(_Predicate):
    __slots__ = ("pattern",)
    _ignore_case = False

    def __init__(self, pattern: str):
        name = type(self).__name__
        self.pattern = _require_str(name, pattern)
        trailing = len(pattern) - len(pattern.rstrip("\\"))
        if trailing % 2:
            raise ValueError(
                f"{name} pattern ends with a lone escape character: {pattern!r}. "
                f"Write `\\\\` to match a backslash."
            )

    def _key(self) -> tuple[Any, ...]:
        return (self.pattern,)

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        return _like_sql(column, placeholder, start, self._ignore_case), [self.pattern]


def _like_sql(column: str, placeholder: Callable[[int], str], index: int, ignore_case: bool) -> str:
    """`column LIKE $N` with backslash as the escape character.

    Postgres escapes with backslash by default. Spelling the clause out
    there would put a backslash in a string literal, which fails to
    parse when `standard_conforming_strings` is off. SQLite has no
    default escape character, so it needs the clause, and has no ILIKE:
    its LIKE already folds ASCII case.
    """
    if _is_sqlite(placeholder):
        return f"{column} LIKE {placeholder(index)} ESCAPE '\\'"
    return f"{column} {'ILIKE' if ignore_case else 'LIKE'} {placeholder(index)}"


class Like(_Pattern):
    """`column LIKE pattern`, with `%` and `_` as wildcards and `\\` as
    the escape character.

    The pattern is bound as a parameter, but its wildcards are live:
    for text that comes from a user, use `StartsWith` or `Contains`,
    which escape them. Postgres matches case-sensitively. SQLite's
    `LIKE` ignores case for ASCII letters unless
    `PRAGMA case_sensitive_like` is on, so a case-sensitive `Like` is
    not portable to SQLite.
    """

    __slots__ = ()


class ILike(_Pattern):
    """Case-insensitive `Like`: `ILIKE` on Postgres, `LIKE` on SQLite.

    SQLite folds case for ASCII letters only, and not at all when
    `PRAGMA case_sensitive_like` is on; Postgres follows the
    database's locale.
    """

    __slots__ = ()
    _ignore_case = True


class _Literal(_Predicate):
    """Match literal text at a fixed position. Wildcards in the text
    are escaped, so user input is safe to pass."""

    __slots__ = ("ignore_case", "text")
    _anywhere = False

    def __init__(self, text: str, *, ignore_case: bool = False):
        self.text = _require_str(type(self).__name__, text)
        self.ignore_case = ignore_case

    def _key(self) -> tuple[Any, ...]:
        return (self.text, self.ignore_case)

    def __repr__(self) -> str:
        flag = ", ignore_case=True" if self.ignore_case else ""
        return f"{type(self).__name__}({self.text!r}{flag})"

    def render(
        self, column: str, placeholder: Callable[[int], str], start: int
    ) -> tuple[str, list[Any]]:
        if _is_sqlite(placeholder) and not self.ignore_case:
            # SQLite's LIKE ignores ASCII case; GLOB is its
            # case-sensitive match and can still use an index for a
            # prefix.
            pattern = f"{'*' if self._anywhere else ''}{_escape_glob(self.text)}*"
            return f"{column} GLOB {placeholder(start)}", [pattern]
        pattern = f"{'%' if self._anywhere else ''}{_escape_like(self.text)}%"
        return _like_sql(column, placeholder, start, self.ignore_case), [pattern]


class StartsWith(_Literal):
    """`column` begins with `text`, matched literally.

    Case-sensitive on every backend by default; `ignore_case=True`
    folds case the way `ILike` does.
    """

    __slots__ = ()


class Contains(_Literal):
    """`column` contains `text`, matched literally.

    Case-sensitive on every backend by default; `ignore_case=True`
    folds case the way `ILike` does.
    """

    __slots__ = ()
    _anywhere = True
