"""Smoke test for the README example.

The block in README.md should run verbatim (modulo a SQLite URL
substitution since the README uses a Postgres URL for narrative).
This test guards against the README drifting from the API.
"""

from etchdb import DB, Row


class User(Row):
    __table__ = "users"
    id: int | None = None
    name: str
    email: str | None = None


async def test_readme_example_round_trip():
    db = await DB.from_url("sqlite+aiosqlite:///:memory:")
    try:
        await db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)"
        )

        # Typed CRUD with DB-allocated id
        alice = await db.insert(User(name="Alice", email="a@example.com"))
        assert alice.id is not None
        assert alice.name == "Alice"

        user = await db.get(User, id=alice.id)
        assert user is not None and user.name == "Alice"

        users = await db.query(User)
        assert len(users) == 1

        # Partial update: email is preserved because we did not pass it.
        updated = await db.update(User(id=alice.id, name="Alice B"))
        assert updated is not None
        assert updated.name == "Alice B"
        assert updated.email == "a@example.com"

        await db.delete(alice)
        assert await db.get(User, id=alice.id) is None

        # Typed-result raw SQL
        bob = await db.insert(User(name="Bob"))
        rows = await db.fetch_models(User, "SELECT id, name, email FROM users")
        assert len(rows) == 1 and rows[0].name == "Bob"
        assert bob.id is not None

        # Untyped raw SQL
        val = await db.fetchval("SELECT count(*) FROM users")
        assert val == 1

        # Transaction
        async with db.transaction() as tx:
            await tx.insert(User(name="Carol"))
        assert (await db.fetchval("SELECT count(*) FROM users")) == 2

        # Inspect SQL before executing
        q = db.compose("get", User, id=1)
        assert "SELECT" in q.sql
        assert q.params == [1]
    finally:
        await db.close()
