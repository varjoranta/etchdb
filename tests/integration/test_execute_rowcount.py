"""Integration tests for db.execute rowcount normalization.

Each driver returns a different shape natively (asyncpg returns a
status string, psycopg and aiosqlite expose rowcount on the cursor).
db.execute normalises every backend to an int row count, with -1
for statements that have no rowcount (DDL)."""

from etchdb import DB
from tests._models import User


async def test_execute_insert_returns_one(db: DB):
    n = await db.execute(
        "INSERT INTO users (id, name) VALUES (1, 'alice')",
    )
    assert n == 1


async def test_execute_update_returns_affected_rows(db: DB):
    for i in range(1, 4):
        await db.insert(User(id=i, name=f"u{i}"))

    n = await db.execute("UPDATE users SET email = 'x@x' WHERE id < 3")

    assert n == 2


async def test_execute_update_no_match_returns_zero(db: DB):
    await db.insert(User(id=1, name="alice"))

    n = await db.execute("UPDATE users SET email = 'x@x' WHERE id = 999")

    assert n == 0


async def test_execute_delete_returns_count(db: DB):
    for i in range(1, 4):
        await db.insert(User(id=i, name=f"u{i}"))

    n = await db.execute("DELETE FROM users WHERE id <> 2")

    assert n == 2


async def test_execute_ddl_returns_minus_one(db: DB):
    """DDL statements (CREATE / DROP / ALTER) carry no rowcount; -1
    is the sentinel mirroring psycopg / sqlite3 native conventions.
    The drop-create-drop dance keeps the parametrized backends from
    seeing each other's leftover state on the shared Postgres."""
    await db.execute("DROP TABLE IF EXISTS etchdb_ddl_probe")
    try:
        n = await db.execute("CREATE TABLE etchdb_ddl_probe (id INT)")
        assert n == -1
    finally:
        await db.execute("DROP TABLE IF EXISTS etchdb_ddl_probe")


async def test_execute_select_returns_minus_one(db: DB):
    """SELECT through execute is explicitly not a row-count contract.
    asyncpg natively reports `SELECT N` as a count and would otherwise
    leak through; pin -1 across all three backends so callers either
    use fetch* (the right shape) or get a stable sentinel."""
    await db.insert(User(id=1, name="alice"))
    await db.insert(User(id=2, name="bob"))
    n = await db.execute("SELECT * FROM users")
    assert n == -1
