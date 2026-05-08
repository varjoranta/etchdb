"""Integration tests for list/tuple filter values producing IN clauses.

`db.query(User, id=[1, 2, 3])` round-trips correctly across all
three backends; empty lists and None-inside-list values raise."""

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


async def test_get_list_filter_returns_one_match(db: DB):
    """select_one with an IN clause: any row that matches stops the
    scan via LIMIT 1."""
    for i in range(1, 4):
        await db.insert(User(id=i, name=f"u{i}"))

    found = await db.get(User, id=[2, 99], name="u2")

    assert found is not None
    assert found.id == 2


async def test_query_empty_list_filter_raises(db: DB):
    with pytest.raises(ValueError, match="empty list filter"):
        await db.query(User, id=[])


async def test_query_none_in_list_filter_raises(db: DB):
    with pytest.raises(ValueError, match="None inside a list filter"):
        await db.query(User, email=["a@x", None])
