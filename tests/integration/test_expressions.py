"""Integration tests for column-expression sentinels in UPDATE.

`Inc` produces atomic `column = column + $N` increments at the DB
level (no read-modify-write race). `Now` produces `column =
CURRENT_TIMESTAMP` so the timestamp is consistent with the
statement's commit time, not the Python call site."""

from etchdb import DB, Inc, Now, Row


class Counter(Row):
    __table__ = "counters"
    id: int
    n: int


class Article(Row):
    __table__ = "articles"
    id: int
    title: str
    updated_at: str  # ISO-8601 text on SQLite, timestamptz on PG


async def _make_counter_table(db: DB):
    await db.execute("DROP TABLE IF EXISTS counters")
    await db.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)")


async def _make_article_table(db: DB):
    await db.execute("DROP TABLE IF EXISTS articles")
    await db.execute(
        "CREATE TABLE articles ("
        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, updated_at TEXT NOT NULL"
        ")"
    )


async def test_inc_atomically_increments(db: DB):
    """The increment runs in SQL; if two callers race, the DB serialises
    them and both writes apply (where two read-modify-write Python
    flows would lose one)."""
    await _make_counter_table(db)
    await db.insert(Counter(id=1, n=10))

    await db.update(Counter.patch(id=1, n=Inc()))
    await db.update(Counter.patch(id=1, n=Inc(by=5)))

    fetched = await db.get(Counter, id=1)
    assert fetched is not None
    assert fetched.n == 16


async def test_inc_decrement(db: DB):
    """Nothing in the helper hard-codes the sign of `by`."""
    await _make_counter_table(db)
    await db.insert(Counter(id=1, n=10))

    await db.update(Counter.patch(id=1, n=Inc(by=-3)))

    fetched = await db.get(Counter, id=1)
    assert fetched is not None
    assert fetched.n == 7


async def test_now_writes_db_side_timestamp(db: DB):
    """The DB picks the timestamp inside the same statement that
    writes the row; no Python-side wall-clock skew."""
    await _make_article_table(db)
    await db.insert(Article(id=1, title="hello", updated_at="1970-01-01"))

    await db.update(Article.patch(id=1, title="hello v2", updated_at=Now()))

    fetched = await db.get(Article, id=1)
    assert fetched is not None
    assert fetched.title == "hello v2"
    assert fetched.updated_at != "1970-01-01"
    assert fetched.updated_at  # truthy, non-empty
