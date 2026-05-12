"""Integration tests for db.update_where: bulk update scoped by where=.

Pin the behaviour across all three backends: the patch (set fields)
applies, the WHERE scopes correctly, the return value is the
affected-row count, and the empty-list short-circuit works."""

import pytest

from etchdb import DB
from tests._models import User


async def test_update_where_bulk_patch_applies_to_matching_rows(db: DB):
    """One patch is applied to every row that matches the IN filter;
    rows outside the filter stay unchanged."""
    for i in range(1, 5):
        await db.insert(User(id=i, name=f"u{i}"))

    n = await db.update_where(
        User.patch(name="archived"),
        where={"id": [1, 3]},
    )

    assert n == 2
    rows = await db.query(User, order_by="id")
    assert [u.name for u in rows] == ["archived", "u2", "archived", "u4"]


async def test_update_where_no_match_returns_zero(db: DB):
    await db.insert(User(id=1, name="alice"))

    n = await db.update_where(
        User.patch(name="x"),
        where={"id": [999]},
    )

    assert n == 0


async def test_update_where_empty_list_short_circuits(db: DB):
    """Empty-list filter follows the same short-circuit convention as
    query / get / update: returns 0 without round-tripping. No SQL is
    emitted, so the emitter's non-empty-where check isn't reached."""
    await db.insert(User(id=1, name="alice"))

    n = await db.update_where(
        User.patch(name="x"),
        where={"id": []},
    )

    assert n == 0
    fetched = await db.get(User, id=1)
    assert fetched is not None and fetched.name == "alice"


async def test_update_where_is_null_filter(db: DB):
    """None in `where=` emits IS NULL via _where_clauses; rows with
    NULL in the targeted column match, rows with a value don't."""
    await db.insert(User(id=1, name="a", email=None))
    await db.insert(User(id=2, name="b", email="b@x"))
    await db.insert(User(id=3, name="c", email=None))

    n = await db.update_where(
        User.patch(name="cleaned"),
        where={"email": None},
    )

    assert n == 2
    rows = await db.query(User, order_by="id")
    assert [u.name for u in rows] == ["cleaned", "b", "cleaned"]


async def test_update_where_empty_dict_raises(db: DB):
    """Distinct from the empty-list short-circuit: an empty dict
    means 'no filter at all', which would update every row -- almost
    always a bug -- so the emitter raises."""
    await db.insert(User(id=1, name="alice"))
    with pytest.raises(ValueError, match="non-empty"):
        await db.update_where(User.patch(name="x"), where={})
