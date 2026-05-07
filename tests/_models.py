"""Shared Row classes used across test files.

Defined once here and imported by test modules instead of redeclared
in each file, so the column lists and PK shape stay in lockstep.

`User.id` is `int | None = None` so the same model exercises both the
"caller supplies id" path and the "DB allocates id (SERIAL / INTEGER
PRIMARY KEY)" path that the README example showcases.
"""

from etchdb import Row


class User(Row):
    __table__ = "users"
    id: int | None = None
    name: str
    email: str | None = None


class UserRole(Row):
    __table__ = "user_roles"
    __pk__ = ("user_id", "role_id")
    user_id: int
    role_id: int
    note: str | None = None
