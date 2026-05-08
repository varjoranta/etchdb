"""Typed exception family for etchdb.

Wraps driver-specific exceptions so application code can catch errors
in one place regardless of the underlying driver. `except
etchdb.IntegrityError` works the same against asyncpg, psycopg, and
aiosqlite. The original driver exception is always preserved as
`__cause__`, so a debugger or `traceback.print_exception` still shows
the full source.

These names are deliberately distinct from the driver-level exceptions
(`sqlite3.IntegrityError`, `psycopg.errors.IntegrityError`, etc.) which
remain reachable via `__cause__` on the etchdb exception.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager


class EtchdbError(Exception):
    """Base for any error raised by etchdb. Catch this to handle every
    DB error regardless of the underlying driver."""


class IntegrityError(EtchdbError):
    """Raised on unique-violation, foreign-key violation, NOT NULL
    violation, or check-constraint failure."""


class UndefinedTableError(EtchdbError):
    """Raised when a query references a table that does not exist."""


class UndefinedColumnError(EtchdbError):
    """Raised when a query references a column that does not exist."""


class OperationalError(EtchdbError):
    """Raised on connection-level or operational failures (server
    unreachable, authentication failure, lost connection, etc.)."""


def wrap(map_fn: Callable[[BaseException], EtchdbError | None]):
    """Build a no-arg async context manager that runs `map_fn` over any
    `Exception` raised inside the block. If `map_fn` returns an
    `EtchdbError`, that error is raised in place (with `from e` to
    preserve the original); if it returns `None`, the original
    exception propagates unchanged. `BaseException` (e.g. `CancelledError`,
    `KeyboardInterrupt`) is never mapped.

    Each driver adapter binds this with its own `_map_exception` so the
    public methods can wrap their bodies with a single
    `async with _wrap_errors(): ...`.
    """

    @asynccontextmanager
    async def _ctx() -> AsyncIterator[None]:
        try:
            yield
        except Exception as e:
            mapped = map_fn(e)
            if mapped is not None:
                raise mapped from e
            raise

    return _ctx
