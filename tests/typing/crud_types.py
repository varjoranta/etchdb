"""Checked by `ty`, never run: the typed CRUD methods hand back the
model class they were given, so `user.name` type-checks downstream.
`make typecheck` covers this file; a signature that widens back to
`Row` fails there."""

from typing import assert_type

from etchdb import DB, Row


class User(Row):
    __table__ = "users"
    id: int | None = None
    name: str


async def check(db: DB) -> None:
    assert_type(await db.get(User, id=1), User | None)
    assert_type(await db.query(User), list[User])
    assert_type(await db.insert(User(name="a")), User)
    assert_type(await db.update(User(id=1, name="b")), User | None)
    assert_type(await db.fetch_model(User, "SELECT 1"), User | None)
    assert_type(await db.fetch_models(User, "SELECT 1"), list[User])
    async for user in db.iter_rows(User):
        assert_type(user, User)
    async for user in db.iter_rows_keyset(User):
        assert_type(user, User)
