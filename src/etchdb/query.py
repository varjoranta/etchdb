"""Inspectable SQL value type."""

from typing import Any, NamedTuple


class SqlQuery(NamedTuple):
    """A SQL string and its bound parameters.

    Returned by every typed operation in etchdb. Useful for testing,
    debugging, and copy-pasting into psql.

        q = pg.insert(user)
        print(q.sql)     # INSERT INTO users (id, name) VALUES ($1, $2) RETURNING *
        print(q.params)  # [1, "Alice"]
    """

    sql: str
    params: list[Any]
