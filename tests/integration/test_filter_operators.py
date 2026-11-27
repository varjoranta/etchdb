"""Filter operators against every backend.

Each test runs on SQLite, asyncpg and psycopg through the parametrized
`db` fixture, and asserts the rows that come back, so a difference
between the dialects shows up as a failure rather than as different
SQL.
"""

import pytest

from etchdb import (
    DB,
    Contains,
    Ge,
    Gt,
    ILike,
    Le,
    Like,
    Lt,
    Not,
    Now,
    Range,
    Row,
    StartsWith,
)
from tests._models import User

# name, email by id. Rows 3, 5 and 6 carry the LIKE, GLOB and escape
# characters; row 4 differs from row 1 only by case; row 2 has NULL.
_USERS = {
    1: ("alice", "a@x.com"),
    2: ("Bob", None),
    3: ("carol", "100%_done@x.com"),
    4: ("ALICE2", "A@X.COM"),
    5: ("a*b[c]?", "star@x.com"),
    6: ("back\\slash", "slash@x.com"),
}


class Event(Row):
    """Declares only the key; the timestamp column is filtered on but
    never read, so its Python type does not depend on the backend."""

    __table__ = "events"
    id: int


def _is_sqlite(db: DB) -> bool:
    return db.compose("query", User, id=1).sql.endswith("?")


@pytest.fixture
async def users(db: DB) -> DB:
    await db.insert_many([User(id=i, name=n, email=e) for i, (n, e) in _USERS.items()])
    return db


async def _ids(db: DB, **filters) -> list[int]:
    return [u.id for u in await db.query(User, order_by="id", **filters)]


async def test_comparisons(users: DB):
    assert await _ids(users, id=Gt(4)) == [5, 6]
    assert await _ids(users, id=Ge(4)) == [4, 5, 6]
    assert await _ids(users, id=Lt(3)) == [1, 2]
    assert await _ids(users, id=Le(3)) == [1, 2, 3]
    assert await _ids(users, email=Ge("star@x.com")) == [5]


async def test_range(users: DB):
    assert await _ids(users, id=Range(ge=2, lt=5)) == [2, 3, 4]
    assert await _ids(users, id=Range(gt=2, le=5)) == [3, 4, 5]
    assert await _ids(users, id=Range(ge=5)) == [5, 6]
    assert await _ids(users, id=Range(lt=2, ge=None)) == [1]
    assert await _ids(users, id=Range(ge=0, lt=1)) == []


async def test_not(users: DB):
    assert await _ids(users, name=Not("alice")) == [2, 3, 4, 5, 6]
    assert await _ids(users, email=Not(None)) == [1, 3, 4, 5, 6]
    assert await _ids(users, id=Not([1, 2])) == [3, 4, 5, 6]
    assert await _ids(users, id=Not((1,))) == [2, 3, 4, 5, 6]


async def test_not_leaves_out_null_rows(users: DB):
    """SQL semantics: `<>` and `NOT IN` are not true for NULL, so the
    row with no email is in neither result."""
    assert await _ids(users, email=Not("a@x.com")) == [3, 4, 5, 6]
    assert await _ids(users, email=Not(["a@x.com", "A@X.COM"])) == [3, 5, 6]


async def test_starts_with_is_case_sensitive_on_every_backend(users: DB):
    assert await _ids(users, name=StartsWith("ali")) == [1]
    assert await _ids(users, name=StartsWith("ALI")) == [4]
    assert await _ids(users, name=StartsWith("ali", ignore_case=True)) == [1, 4]
    assert await _ids(users, name=StartsWith("")) == [1, 2, 3, 4, 5, 6]


async def test_contains_is_case_sensitive_on_every_backend(users: DB):
    assert await _ids(users, name=Contains("lic")) == [1]
    assert await _ids(users, name=Contains("LIC")) == [4]
    assert await _ids(users, name=Contains("LiC", ignore_case=True)) == [1, 4]
    assert await _ids(users, email=Contains("@")) == [1, 3, 4, 5, 6]


@pytest.mark.parametrize("ignore_case", [False, True])
async def test_wildcard_characters_in_the_text_match_literally(users: DB, ignore_case: bool):
    async def ids(column: str, operator) -> list[int]:
        return await _ids(users, **{column: operator})

    def contains(text: str) -> Contains:
        return Contains(text, ignore_case=ignore_case)

    def starts(text: str) -> StartsWith:
        return StartsWith(text, ignore_case=ignore_case)

    assert await ids("email", contains("%")) == [3]
    assert await ids("email", contains("_")) == [3]
    assert await ids("email", contains("0%_d")) == [3]
    assert await ids("name", contains("*")) == [5]
    assert await ids("name", contains("?")) == [5]
    assert await ids("name", contains("[c]")) == [5]
    assert await ids("name", contains("]")) == [5]
    assert await ids("name", contains("\\")) == [6]
    assert await ids("name", starts("a*b[")) == [5]
    assert await ids("name", starts("back\\s")) == [6]
    assert await ids("email", starts("100%")) == [3]
    # A wildcard that is not in any row matches nothing.
    assert await ids("name", starts("a%")) == []
    assert await ids("name", starts("a?")) == []
    assert await ids("name", contains("b_c")) == []


async def test_like_keeps_its_wildcards(users: DB):
    assert await _ids(users, email=Like("%@x.com")) == (
        [1, 3, 4, 5, 6] if _is_sqlite(users) else [1, 3, 5, 6]
    )
    assert await _ids(users, name=Like("_ob")) == [2]
    assert await _ids(users, email=Like("100\\%\\_%")) == [3]


async def test_like_case_sensitivity_differs_on_sqlite(users: DB):
    """Documented: SQLite's LIKE folds ASCII case, Postgres' does not."""
    expected = [1, 4] if _is_sqlite(users) else [1]
    assert await _ids(users, name=Like("ali%")) == expected


async def test_ilike_ignores_case_on_every_backend(users: DB):
    assert await _ids(users, name=ILike("ali%")) == [1, 4]
    assert await _ids(users, email=ILike("a@x.c_m")) == [1, 4]


async def test_operators_combine_with_plain_filters_limit_and_offset(users: DB):
    rows = await users.query(
        User,
        email=Not(None),
        id=Range(ge=2, le=6),
        name=Not(["carol"]),
        order_by="id",
        limit=2,
        offset=1,
    )
    assert [u.id for u in rows] == [5, 6]
    assert await _ids(users, id=[1, 2, 3, 4], email=Not(None), name=Contains("a")) == [1, 3]


async def test_get_rejects_operators(users: DB):
    with pytest.raises(ValueError, match="select_one does not accept filter operators"):
        await users.get(User, id=Gt(1))


async def test_update_where_with_operators(users: DB):
    n = await users.update_where(
        User.patch(email="bulk@x.com"),
        where={"id": Range(ge=2, lt=5), "email": Not(None)},
    )
    assert n == 2
    assert await _ids(users, email="bulk@x.com") == [3, 4]


async def test_update_with_operator_in_where(users: DB):
    kept = await users.update(User.patch(id=2, name="Robert"), where={"email": Not(None)})
    assert kept is None
    changed = await users.update(User.patch(id=1, name="Alice"), where={"email": Not(None)})
    assert changed == User(id=1, name="Alice", email="a@x.com")
    assert await _ids(users, name=StartsWith("Alice")) == [1]


async def test_delete_with_operator_in_where(users: DB):
    await users.delete(User.patch(id=1), where={"name": StartsWith("zzz")})
    await users.delete(User.patch(id=5), where={"name": Contains("*")})
    assert await _ids(users) == [1, 2, 3, 4, 6]


async def test_iter_rows_and_keyset_with_operators(users: DB):
    paged = [u.id async for u in users.iter_rows(User, batch_size=2, id=Gt(1), email=Not(None))]
    assert paged == [3, 4, 5, 6]
    keyset = [
        u.id
        async for u in users.iter_rows_keyset(
            User, batch_size=2, email=Not(None), name=Not("carol")
        )
    ]
    assert keyset == [1, 4, 5, 6]
    by_name = [
        u.id async for u in users.iter_rows_keyset(User, by="email", batch_size=2, id=Range(le=4))
    ]
    assert by_name[0] == 2  # NULL email first
    assert sorted(by_name) == [1, 2, 3, 4]


async def test_operators_inside_a_transaction(users: DB):
    async with users.transaction() as tx:
        assert await tx.update_where(User.patch(email=None), where={"id": Ge(5)}) == 2
        assert await _ids(tx, email=Not(None)) == [1, 3, 4]


async def test_comparison_with_now(db: DB):
    """`Lt(Now())` compares with the database clock in the statement."""
    await db.execute("DROP TABLE IF EXISTS events")
    if _is_sqlite(db):
        await db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, at TEXT NOT NULL)")
        past, future = "datetime('now', '-1 day')", "datetime('now', '+1 day')"
    else:
        await db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, at TIMESTAMPTZ NOT NULL)")
        past, future = "now() - interval '1 day'", "now() + interval '1 day'"
    await db.execute(f"INSERT INTO events (id, at) VALUES (1, {past}), (2, {future})")

    assert [e.id for e in await db.query(Event, at=Lt(Now()))] == [1]
    assert [e.id for e in await db.query(Event, at=Gt(Now()))] == [2]
    assert [e.id for e in await db.query(Event, at=Range(lt=Now()), id=Ge(1))] == [1]
    await db.execute("DROP TABLE events")


async def test_update_and_delete_reject_an_operator_in_the_primary_key(users: DB):
    with pytest.raises(ValueError, match="hold a sentinel"):
        await users.update(User.patch(id=Gt(1), name="X"))
    with pytest.raises(ValueError, match="hold a sentinel"):
        await users.delete(User.patch(id=Not(1)))
    with pytest.raises(ValueError, match="delete_many takes primary-key values"):
        await users.delete_many(User, [Gt(0)])
    assert await _ids(users, name=Not("X")) == [1, 2, 3, 4, 5, 6]


async def test_text_operators_with_standard_conforming_strings_off(pg_url: str):
    """Postgres still allows the old string syntax, where a backslash
    in a literal is an escape. The text operators must not depend on
    the setting."""
    db = await DB.from_url(pg_url, min_size=1, max_size=1)
    try:
        await db.execute("DROP TABLE IF EXISTS users CASCADE")
        await db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)"
        )
        await db.insert_many([User(id=i, name=n, email=e) for i, (n, e) in _USERS.items()])
        async with db.transaction() as tx:
            await tx.execute("SET LOCAL standard_conforming_strings = off")
            assert await _ids(tx, email=Contains("%_")) == [3]
            assert await _ids(tx, name=Contains("\\")) == [6]
            assert await _ids(tx, name=StartsWith("ALI", ignore_case=True)) == [1, 4]
            assert await _ids(tx, email=Like("100\\%\\_%")) == [3]
            assert await _ids(tx, name=ILike("ali%")) == [1, 4]
    finally:
        await db.execute("DROP TABLE IF EXISTS users CASCADE")
        await db.close()
