"""Integration tests for IS NULL filter semantics.

`db.get(User, email=None)` and friends must emit `WHERE email IS NULL`
rather than `= NULL` so they actually return rows whose column is NULL.
Without the IS NULL handling, every test in this file would silently
return zero rows."""

from etchdb import DB
from tests._models import User

# --- get / query -----------------------------------------------------


async def test_get_with_none_filter_finds_null_row(db: DB):
    await db.insert(User(id=1, name="alice", email=None))
    await db.insert(User(id=2, name="bob", email="bob@x"))

    found = await db.get(User, email=None)

    assert found is not None
    assert found.id == 1
    assert found.email is None


async def test_query_with_none_filter_returns_only_null_rows(db: DB):
    await db.insert(User(id=1, name="a", email=None))
    await db.insert(User(id=2, name="b", email="b@x"))
    await db.insert(User(id=3, name="c", email=None))

    rows = await db.query(User, email=None)

    assert {u.id for u in rows} == {1, 3}


async def test_query_mixed_none_and_value_filter(db: DB):
    """Combining a bound value and an IS NULL must AND them; only rows
    matching both predicates come back."""
    await db.insert(User(id=1, name="alice", email=None))
    await db.insert(User(id=2, name="alice", email="alice@x"))
    await db.insert(User(id=3, name="bob", email=None))

    rows = await db.query(User, name="alice", email=None)

    assert [u.id for u in rows] == [1]


# --- update / delete with where=None ---------------------------------


async def test_update_where_none_matches_null_row(db: DB):
    """The atomic 'only update if column is still NULL' guard works
    once None becomes IS NULL (vs the previous silent no-op)."""
    await db.insert(User(id=1, name="alice", email=None))

    updated = await db.update(
        User(id=1, name="alice b"),
        where={"email": None},
    )

    assert updated is not None
    assert updated.name == "alice b"


async def test_update_where_none_misses_non_null_row(db: DB):
    """Same guard, opposite outcome: a row with a non-NULL email must
    not be updated by an `email IS NULL` guard."""
    await db.insert(User(id=1, name="alice", email="alice@x"))

    updated = await db.update(
        User(id=1, name="alice b"),
        where={"email": None},
    )

    assert updated is None
    fetched = await db.get(User, id=1)
    assert fetched is not None
    assert fetched.name == "alice"


async def test_delete_where_none_matches_null_row(db: DB):
    await db.insert(User(id=1, name="alice", email=None))
    await db.insert(User(id=2, name="bob", email="bob@x"))

    await db.delete(User(id=1, name="alice"), where={"email": None})

    remaining = await db.query(User, order_by="id")
    assert [u.id for u in remaining] == [2]
