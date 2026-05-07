"""Test typed CRUD on the DB facade across SQLite and Postgres."""

from etchdb import DB
from tests._models import User, UserRole

# --- insert --------------------------------------------------------


async def test_insert_returns_typed_row(db: DB):
    inserted = await db.insert(User(id=1, name="Alice", email="a@x"))
    assert isinstance(inserted, User)
    assert inserted.id == 1
    assert inserted.name == "Alice"
    assert inserted.email == "a@x"


async def test_insert_persists(db: DB):
    await db.insert(User(id=2, name="Bob"))
    val = await db.fetchval("SELECT name FROM users WHERE id = 2")
    assert val == "Bob"


async def test_insert_with_db_allocated_pk(db: DB):
    """Omitted id triggers the DB's sequence (SERIAL on Postgres,
    INTEGER PRIMARY KEY on SQLite); the returned row has it populated."""
    new = await db.insert(User(name="Alice"))
    assert new.id is not None
    assert new.name == "Alice"

    found = await db.get(User, id=new.id)
    assert found is not None
    assert found.name == "Alice"


# --- get -----------------------------------------------------------


async def test_get_by_id(db: DB):
    await db.insert(User(id=1, name="Alice"))
    found = await db.get(User, id=1)
    assert found is not None
    assert found.name == "Alice"


async def test_get_returns_none_when_missing(db: DB):
    assert await db.get(User, id=999) is None


async def test_get_by_multiple_filters(db: DB):
    await db.insert(User(id=1, name="Alice", email="a@x"))
    await db.insert(User(id=2, name="Alice", email="b@x"))
    found = await db.get(User, name="Alice", email="b@x")
    assert found is not None
    assert found.id == 2


# --- query ---------------------------------------------------------


async def test_query_returns_list(db: DB):
    await db.insert(User(id=1, name="A"))
    await db.insert(User(id=2, name="B"))
    users = await db.query(User, order_by="id")
    assert [u.name for u in users] == ["A", "B"]


async def test_query_with_filter(db: DB):
    await db.insert(User(id=1, name="Alice"))
    await db.insert(User(id=2, name="Bob"))
    users = await db.query(User, name="Alice")
    assert len(users) == 1
    assert users[0].id == 1


async def test_query_pagination(db: DB):
    for i in range(1, 6):
        await db.insert(User(id=i, name=f"u{i}"))
    page = await db.query(User, order_by="id", limit=2, offset=2)
    assert [u.id for u in page] == [3, 4]


async def test_query_empty(db: DB):
    assert await db.query(User) == []


# --- update --------------------------------------------------------


async def test_update_returns_updated_row(db: DB):
    await db.insert(User(id=1, name="Alice", email="old@x"))
    updated = await db.update(User(id=1, name="Alice", email="new@x"))
    assert updated is not None
    assert updated.email == "new@x"
    again = await db.get(User, id=1)
    assert again is not None
    assert again.email == "new@x"


async def test_update_returns_none_when_no_match(db: DB):
    result = await db.update(User(id=999, name="Ghost"))
    assert result is None


async def test_update_partial_preserves_unset_fields(db: DB):
    """Fields not in model_fields_set are not in the SET clause, so
    columns the caller didn't touch keep their existing values."""
    await db.insert(User(id=1, name="Alice", email="original@x"))
    updated = await db.update(User(id=1, name="Alice B"))  # email NOT set
    assert updated is not None
    assert updated.name == "Alice B"
    assert updated.email == "original@x"


async def test_update_composite_pk(db: DB):
    await db.insert(UserRole(user_id=1, role_id=2, note="initial"))
    await db.update(UserRole(user_id=1, role_id=2, note="changed"))
    found = await db.get(UserRole, user_id=1, role_id=2)
    assert found is not None
    assert found.note == "changed"


# --- delete --------------------------------------------------------


async def test_delete(db: DB):
    await db.insert(User(id=1, name="Alice"))
    await db.delete(User(id=1, name="Alice"))
    assert await db.get(User, id=1) is None


async def test_delete_composite_pk(db: DB):
    await db.insert(UserRole(user_id=1, role_id=2))
    await db.delete(UserRole(user_id=1, role_id=2))
    assert await db.get(UserRole, user_id=1, role_id=2) is None


# --- compose -------------------------------------------------------


async def test_compose_returns_inspectable_sqlquery(db: DB):
    q = db.compose("get", User, id=1)
    assert "SELECT" in q.sql
    assert "users" in q.sql
    assert q.params == [1]


async def test_compose_uses_adapter_placeholder_style(db: DB):
    q = db.compose("get", User, id=1)
    expected = db._adapter.placeholder(0)
    assert expected in q.sql


async def test_compose_for_insert(db: DB):
    q = db.compose("insert", User(id=1, name="Alice", email="a@x"))
    assert q.sql.startswith("INSERT INTO users")
    assert q.params == [1, "Alice", "a@x"]


async def test_compose_for_query_with_filter(db: DB):
    q = db.compose("query", User, name="Alice", limit=5)
    assert "WHERE name" in q.sql
    assert "LIMIT" in q.sql
    assert q.params == ["Alice", 5]
