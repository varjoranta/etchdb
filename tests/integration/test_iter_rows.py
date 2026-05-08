"""Integration tests for db.iter_rows: paginated async iteration."""

from etchdb import DB
from tests._models import User


async def test_iter_rows_empty_table_yields_nothing(db: DB):
    rows = [u async for u in db.iter_rows(User)]
    assert rows == []


async def test_iter_rows_single_page(db: DB):
    """When batch_size > row count, everything fits in one DB call."""
    for i in range(1, 4):
        await db.insert(User(id=i, name=f"u{i}"))

    rows = [u async for u in db.iter_rows(User, batch_size=10)]

    assert [u.name for u in rows] == ["u1", "u2", "u3"]


async def test_iter_rows_multiple_pages_yields_all_in_order(db: DB):
    """Smaller batch_size triggers pagination; PK-ordered output stays stable."""
    for i in range(1, 8):
        await db.insert(User(id=i, name=f"u{i}"))

    rows = [u async for u in db.iter_rows(User, batch_size=3)]

    assert [u.id for u in rows] == [1, 2, 3, 4, 5, 6, 7]


async def test_iter_rows_filter_propagates(db: DB):
    await db.insert(User(id=1, name="alice"))
    await db.insert(User(id=2, name="bob"))
    await db.insert(User(id=3, name="alice"))

    rows = [u async for u in db.iter_rows(User, name="alice")]

    assert {u.id for u in rows} == {1, 3}


async def test_iter_rows_custom_order_by_overrides_default(db: DB):
    await db.insert(User(id=1, name="charlie"))
    await db.insert(User(id=2, name="alice"))
    await db.insert(User(id=3, name="bob"))

    rows = [u async for u in db.iter_rows(User, order_by="name", batch_size=2)]

    assert [u.name for u in rows] == ["alice", "bob", "charlie"]
