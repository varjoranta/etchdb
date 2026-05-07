"""Base class for typed table rows."""

from typing import ClassVar

from pydantic import BaseModel


class Row(BaseModel):
    """Base class for typed table rows.

    Subclass to declare a table:

        class User(Row):
            __table__ = "users"
            id: int
            name: str

    `__table__` is required. `__pk__` defaults to `("id",)`; override
    if your primary key is composite or named differently.

        class UserRole(Row):
            __table__ = "user_roles"
            __pk__ = ("user_id", "role_id")
            user_id: int
            role_id: int
    """

    __table__: ClassVar[str]
    __pk__: ClassVar[tuple[str, ...]] = ("id",)
