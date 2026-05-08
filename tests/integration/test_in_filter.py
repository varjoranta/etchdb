"""Integration tests for list/tuple filter values producing IN clauses.

`db.query(User, id=[1, 2, 3])` round-trips correctly across all
three backends. Empty list filters short-circuit to "no rows"
(no SQL round-trip); None inside a list still raises (ambiguous)."""

import pytest

from etchdb import DB
from tests._models import User


async def test_query_list_filter_returns_matching_rows(db: DB):
    for i in range(1, 6):
        await db.insert(User(id=i, name=f"u{i}"))

    rows = await db.query(User, id=[1, 3, 5], order_by="id")

    assert [u.id for u in rows] == [1, 3, 5]


async def test_query_tuple_filter_works_like_list(db: DB):
    for i in range(1, 4):
        await db.insert(User(id=i, name=f"u{i}"))

    rows = await db.query(User, id=(1, 3), order_by="id")

    assert [u.id for u in rows] == [1, 3]


async def test_get_rejects_list_filter(db: DB):
    """get is the single-row verb; a list filter would be `IN (...)
    LIMIT 1`, which silently returns "first match" — rarely the
    intent. Caller is pointed at db.query instead."""
    with pytest.raises(ValueError, match="select_one does not accept list"):
        await db.get(User, id=[1, 2, 3])


async def test_query_empty_list_filter_short_circuits(db: DB):
    """An empty IN list matches no rows; rather than emit invalid SQL
    or force every caller to guard, the facade returns [] without
    round-tripping."""
    await db.insert(User(id=1, name="alice"))
    assert await db.query(User, id=[]) == []


async def test_get_empty_list_filter_returns_none(db: DB):
    await db.insert(User(id=1, name="alice"))
    assert await db.get(User, id=[]) is None


async def test_iter_rows_empty_list_filter_yields_nothing(db: DB):
    await db.insert(User(id=1, name="alice"))
    rows = [u async for u in db.iter_rows(User, id=[])]
    assert rows == []


async def test_query_none_in_list_filter_raises(db: DB):
    with pytest.raises(ValueError, match="None inside a list filter"):
        await db.query(User, email=["a@x", None])
