"""Integration tests for db.insert_many / db.delete_many.

Covers the round-trip across all three backends plus a chunking case
that monkeypatches the parameter limit so we can verify large batches
issue multiple statements without changing the public surface."""

import pytest

from etchdb import DB, IntegrityError
from etchdb import db as db_module
from tests._models import User, UserRole

# --- insert_many -----------------------------------------------------


async def test_insert_many_round_trip(db: DB):
    rows = [User(id=i, name=f"u{i}") for i in range(1, 6)]
    await db.insert_many(rows)

    fetched = await db.query(User, order_by="id")
    assert [u.id for u in fetched] == [1, 2, 3, 4, 5]
    assert [u.name for u in fetched] == ["u1", "u2", "u3", "u4", "u5"]


async def test_insert_many_empty_is_noop(db: DB):
    await db.insert_many([])
    assert await db.query(User) == []


async def test_insert_many_on_conflict_ignore_skips_duplicates(db: DB):
    await db.insert(User(id=1, name="original"))

    rows = [User(id=1, name="duplicate"), User(id=2, name="fresh")]
    await db.insert_many(rows, on_conflict="ignore")

    fetched = await db.query(User, order_by="id")
    assert {u.id for u in fetched} == {1, 2}
    # The original row was not overwritten
    one = await db.get(User, id=1)
    assert one is not None and one.name == "original"


async def test_insert_many_without_on_conflict_raises_on_duplicate(db: DB):
    await db.insert(User(id=1, name="original"))

    rows = [User(id=1, name="duplicate")]
    with pytest.raises(IntegrityError):
        await db.insert_many(rows)


async def test_insert_many_on_conflict_upsert_overwrites(db: DB):
    """Upsert: existing rows take the new values for every non-PK
    column; fresh rows are inserted as usual."""
    await db.insert(User(id=1, name="original", email="orig@x"))
    await db.insert(User(id=2, name="alice"))

    rows = [
        User(id=1, name="updated", email="new@x"),
        User(id=3, name="fresh", email="fresh@x"),
    ]
    await db.insert_many(rows, on_conflict="upsert")

    fetched = await db.query(User, order_by="id")
    assert [(u.id, u.name, u.email) for u in fetched] == [
        (1, "updated", "new@x"),
        (2, "alice", None),
        (3, "fresh", "fresh@x"),
    ]


async def test_insert_with_on_conflict_upsert_returns_db_view(db: DB):
    """Single-row upsert via db.insert; the returned Row reflects the
    DB's view (RETURNING * fires whether the row was inserted or
    updated)."""
    await db.insert(User(id=1, name="alice", email="alice@x"))

    returned = await db.insert(
        User(id=1, name="alice b", email="newer@x"),
        on_conflict="upsert",
    )

    assert returned.id == 1
    assert returned.name == "alice b"
    assert returned.email == "newer@x"


async def test_insert_many_chunks_large_batches(db: DB, monkeypatch):
    """Monkeypatch the parameter limit so we can prove chunking happens
    without writing 32k+ rows to the DB. With limit=4 and 2 cols/row,
    chunk_size = 2; 5 rows must split into 3 statements but all rows
    must end up in the table."""
    monkeypatch.setattr(db_module, "_PARAM_LIMIT", 4)

    rows = [User(id=i, name=f"u{i}") for i in range(1, 6)]
    await db.insert_many(rows)

    fetched = await db.query(User, order_by="id")
    assert [u.id for u in fetched] == [1, 2, 3, 4, 5]


# --- delete_many -----------------------------------------------------


async def test_delete_many_single_pk(db: DB):
    for i in range(1, 6):
        await db.insert(User(id=i, name=f"u{i}"))

    await db.delete_many(User, [1, 3, 5])

    remaining = await db.query(User, order_by="id")
    assert [u.id for u in remaining] == [2, 4]


async def test_delete_many_composite_pk(db: DB):
    await db.insert(UserRole(user_id=1, role_id=10))
    await db.insert(UserRole(user_id=1, role_id=20))
    await db.insert(UserRole(user_id=2, role_id=10))

    await db.delete_many(
        UserRole,
        [{"user_id": 1, "role_id": 10}, {"user_id": 2, "role_id": 10}],
    )

    remaining = await db.query(UserRole, order_by=["user_id", "role_id"])
    assert [(r.user_id, r.role_id) for r in remaining] == [(1, 20)]


async def test_delete_many_empty_is_noop(db: DB):
    await db.insert(User(id=1, name="alice"))
    await db.delete_many(User, [])
    assert await db.get(User, id=1) is not None


async def test_delete_many_chunks_large_batches(db: DB, monkeypatch):
    """Same chunking proof as for insert_many: monkeypatched limit
    forces multiple DELETE statements; the row set must still be empty
    at the end."""
    monkeypatch.setattr(db_module, "_PARAM_LIMIT", 2)

    for i in range(1, 6):
        await db.insert(User(id=i, name=f"u{i}"))

    await db.delete_many(User, [1, 2, 3, 4, 5])

    assert await db.query(User) == []


# --- a multi-chunk batch is one unit ----------------------------------


async def test_insert_many_rolls_back_earlier_chunks_on_failure(db: DB, monkeypatch):
    """Two columns per row and a limit of 4 gives chunks of 2. The
    duplicate sits in the second chunk; the first must not survive it."""
    monkeypatch.setattr(db_module, "_PARAM_LIMIT", 4)
    rows = [User(id=1, name="a"), User(id=2, name="b"), User(id=3, name="c"), User(id=3, name="d")]
    with pytest.raises(IntegrityError):
        await db.insert_many(rows)
    assert await db.query(User) == []


async def test_insert_many_rejects_a_foreign_row_before_writing(db: DB, monkeypatch):
    """A row of another model at the start of a later chunk used to
    pass that chunk's own check and be written to its own table."""
    monkeypatch.setattr(db_module, "_PARAM_LIMIT", 4)
    rows = [User(id=1, name="a"), User(id=2, name="b"), UserRole(user_id=1, role_id=1)]
    with pytest.raises(ValueError, match="same Row subclass"):
        await db.insert_many(rows)
    assert await db.query(User) == []
    assert await db.query(UserRole) == []


async def test_delete_many_rolls_back_earlier_chunks_on_failure(db: DB, monkeypatch):
    monkeypatch.setattr(db_module, "_PARAM_LIMIT", 2)
    await db.insert_many([User(id=i, name=f"u{i}") for i in range(1, 5)])
    with pytest.raises(Exception):  # noqa: B017  driver-specific bind error
        await db.delete_many(User, [1, 2, 3, object()])
    assert len(await db.query(User)) == 4


async def test_query_offset_without_limit(db: DB):
    await db.insert_many([User(id=i, name=f"u{i}") for i in range(1, 4)])
    rows = await db.query(User, offset=1, order_by="id")
    assert [u.id for u in rows] == [2, 3]
