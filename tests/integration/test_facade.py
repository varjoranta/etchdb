"""Test the DB facade against both SQLite and Postgres.

Each test runs against both backends via the parametrized `db`
fixture. SQLite always runs (in-memory); Postgres runs only when
the Docker container is up.
"""

import pytest

from etchdb import DB
from tests._models import User

# --- raw SQL passthrough --------------------------------------------


async def test_execute_and_fetchval(db: DB):
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    val = await db.fetchval("SELECT count(*) FROM users")
    assert val == 1


async def test_fetchrow_returns_row_or_none(db: DB):
    none = await db.fetchrow("SELECT * FROM users WHERE name = 'nobody'")
    assert none is None

    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    row = await db.fetchrow("SELECT name FROM users WHERE name = 'Bob'")
    assert row == {"name": "Bob"}


async def test_fetch_returns_list_of_dicts(db: DB):
    await db.execute("INSERT INTO users (name) VALUES ('A'), ('B')")
    rows = await db.fetch("SELECT name FROM users ORDER BY name")
    assert [r["name"] for r in rows] == ["A", "B"]


# --- typed-result raw SQL -------------------------------------------


async def test_fetch_models(db: DB):
    await db.execute("INSERT INTO users (id, name) VALUES (1, 'A'), (2, 'B')")
    users = await db.fetch_models(User, "SELECT id, name, email FROM users ORDER BY id")
    assert len(users) == 2
    assert all(isinstance(u, User) for u in users)
    assert users[0].name == "A"
    assert users[1].name == "B"


async def test_fetch_model_returns_user(db: DB):
    await db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@x')")
    user = await db.fetch_model(User, "SELECT id, name, email FROM users WHERE id = 1")
    assert user is not None
    assert user.name == "Alice"
    assert user.email == "a@x"


async def test_fetch_model_returns_none_when_no_row(db: DB):
    user = await db.fetch_model(User, "SELECT id, name, email FROM users WHERE name = 'nobody'")
    assert user is None


# --- transaction ----------------------------------------------------


async def test_transaction_commits_on_clean_exit(db: DB):
    async with db.transaction() as tx:
        await tx.execute("INSERT INTO users (name) VALUES ('Tx')")
    val = await db.fetchval("SELECT count(*) FROM users WHERE name = 'Tx'")
    assert val == 1


async def test_transaction_rolls_back_on_exception(db: DB):
    with pytest.raises(RuntimeError, match="force rollback"):
        async with db.transaction() as tx:
            await tx.execute("INSERT INTO users (name) VALUES ('Bad')")
            raise RuntimeError("force rollback")
    val = await db.fetchval("SELECT count(*) FROM users WHERE name = 'Bad'")
    assert val == 0


async def test_transaction_yields_db(db: DB):
    """The tx context manager yields a DB, not a raw adapter."""
    async with db.transaction() as tx:
        assert isinstance(tx, DB)
        await tx.execute("INSERT INTO users (name) VALUES ('Inside')")
        rows = await tx.fetch("SELECT name FROM users WHERE name = 'Inside'")
        assert len(rows) == 1
