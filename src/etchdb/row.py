"""Base class for typed table rows."""

from typing import Any, ClassVar, Self

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

    `__fields_not_in_db__` lists fields that exist on the Pydantic
    model but should not map to columns -- computed values, transient
    state, anything you want to carry alongside DB-backed fields
    without etchdb sending it to or reading it from the database.
    Such fields need a default: read paths hydrate via `cls(**row)`
    from the SELECT result, which never carries the column, so
    Pydantic falls back to the declared default.

        class User(Row):
            __table__ = "users"
            __fields_not_in_db__ = ("display_name",)
            id: int
            name: str
            display_name: str = ""   # filled in by application code
    """

    __table__: ClassVar[str]
    __pk__: ClassVar[tuple[str, ...]] = ("id",)
    __fields_not_in_db__: ClassVar[tuple[str, ...]] = ()

    @classmethod
    def patch(cls, **fields: Any) -> Self:
        """Build a partial Row for `db.update` / `db.delete`, skipping
        Pydantic validation so partials with missing required fields
        work. NOT a general-purpose constructor: use `Cls(...)` for
        fully-formed rows that should be validated."""
        return cls.model_construct(**fields)
