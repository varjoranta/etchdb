"""SQLite transaction boundaries.

The adapter shares one connection, so these tests read the file back
through a separate `sqlite3` connection: a write only counts once
another connection can see it.
"""

import sqlite3
from pathlib import Path

import aiosqlite
import pytest

from etchdb import DB, IntegrityError, Row, UndefinedColumnError
from etchdb.aiosqlite import AiosqliteAdapter


class Item(Row):
    __table__ = "items"
    id: int | None = None
    name: str | None = None


def _names(path: Path) -> list[str]:
    conn = sqlite3.connect(path)
    try:
        return [r[0] for r in conn.execute("SELECT name FROM items ORDER BY id")]
    finally:
        conn.close()


@pytest.fixture
async def file_db(tmp_path: Path):
    path = tmp_path / "app.db"
    db = await DB.from_url(f"sqlite:///{path}")
    await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    yield db, path
    await db.close()


async def test_insert_and_update_are_committed_without_a_transaction(file_db):
    db, path = file_db
    item = await db.insert(Item(name="a"))
    assert _names(path) == ["a"]
    await db.update(Item(id=item.id, name="b"))
    assert _names(path) == ["b"]


async def test_insert_survives_close(tmp_path: Path):
    path = tmp_path / "app.db"
    db = await DB.from_url(f"sqlite:///{path}")
    await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    await db.insert(Item(name="a"))
    await db.close()
    assert _names(path) == ["a"]


async def test_execute_with_returning_counts_rows_and_commits(file_db):
    db, path = file_db
    n = await db.execute("INSERT INTO items (name) VALUES ('a'), ('b') RETURNING id")
    assert n == 2
    assert _names(path) == ["a", "b"]
    async with db.transaction() as tx:
        assert await tx.execute("UPDATE items SET name = 'c' RETURNING id") == 2


async def test_rollback_covers_a_nested_transaction_opened_first(file_db):
    db, path = file_db
    with pytest.raises(RuntimeError):
        async with db.transaction() as tx:
            async with tx.transaction() as inner:
                await inner.execute("INSERT INTO items (name) VALUES ('inner')")
            raise RuntimeError("outer fails")
    assert _names(path) == []


async def test_rollback_covers_ddl_issued_first(file_db):
    db, path = file_db
    with pytest.raises(RuntimeError):
        async with db.transaction() as tx:
            await tx.execute("CREATE TABLE other (id INTEGER)")
            raise RuntimeError("fails")
    assert await db.fetchval("SELECT count(*) FROM sqlite_master WHERE name = 'other'") == 0


async def test_failed_commit_rolls_back_and_maps_the_error(file_db):
    db, path = file_db
    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute(
        "CREATE TABLE children (id INTEGER PRIMARY KEY, item_id INTEGER "
        "REFERENCES items (id) DEFERRABLE INITIALLY DEFERRED)"
    )
    with pytest.raises(IntegrityError):
        async with db.transaction() as tx:
            await tx.execute("INSERT INTO children (item_id) VALUES (99)")
    # The connection is usable and the rejected row is gone.
    assert await db.fetchval("SELECT count(*) FROM children") == 0
    await db.insert(Item(name="after"))
    assert _names(path) == ["after"]


async def test_failed_statement_leaves_no_transaction_open(tmp_path: Path):
    """A legacy-mode connection opens a transaction before DML; a failed
    standalone statement must not leave it for the next caller."""
    conn = await aiosqlite.connect(tmp_path / "legacy.db")
    try:
        db = DB(AiosqliteAdapter.from_connection(conn))
        await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        await db.insert(Item(name="a"))
        with pytest.raises(IntegrityError):
            await db.insert(Item(name="a"))
        assert not conn.in_transaction
    finally:
        await conn.close()


async def test_external_connection_with_default_row_factory(tmp_path: Path):
    path = tmp_path / "ext.db"
    conn = await aiosqlite.connect(path)
    try:
        db = DB(AiosqliteAdapter.from_connection(conn))
        await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        item = await db.insert(Item(name="a"))
        assert item == Item(id=1, name="a")
        assert _names(path) == ["a"]
        assert await db.fetch("SELECT id, name FROM items") == [{"id": 1, "name": "a"}]
        with pytest.raises(RuntimeError):
            async with db.transaction() as tx:
                await tx.insert(Item(name="b"))
                raise RuntimeError("fails")
        assert _names(path) == ["a"]
    finally:
        await conn.close()


async def test_insert_into_missing_column_maps_to_undefined_column(file_db):
    db, _ = file_db

    class Wider(Row):
        __table__ = "items"
        id: int | None = None
        nope: str | None = None

    with pytest.raises(UndefinedColumnError):
        await db.insert(Wider(nope="x"))


async def test_failed_migration_leaves_nothing_behind(file_db, tmp_path: Path):
    db, _ = file_db
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "0001_two_steps.sql").write_text(
        "CREATE TABLE first_step (id INTEGER);\nINSERT INTO no_such_table VALUES (1);\n"
    )
    with pytest.raises(Exception, match="no_such_table"):
        await db.migrate(migrations)
    assert await db.fetchval("SELECT count(*) FROM sqlite_master WHERE name = 'first_step'") == 0
    # Fixing the file and re-running applies it cleanly.
    (migrations / "0001_two_steps.sql").write_text("CREATE TABLE first_step (id INTEGER);\n")
    assert await db.migrate(migrations) == 1


@pytest.mark.parametrize("url", ["sqlite://app.db", "sqlite+aiosqlite://data/app.db"])
async def test_two_slash_url_is_rejected(url: str):
    with pytest.raises(ValueError, match="three slashes"):
        await DB.from_url(url)


async def test_direct_from_url_rejects_configure():
    async def configure(conn):
        pass

    with pytest.raises(TypeError):
        await AiosqliteAdapter.from_url("sqlite:///:memory:", configure=configure)  # ty: ignore[unknown-argument]
