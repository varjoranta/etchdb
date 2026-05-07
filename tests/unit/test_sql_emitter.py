"""Test the dialect-neutral SQL emitter.

Verifies the generated SQL string and parameter list for each typed
operation, against both a Postgres-style placeholder ($1, $2, ...)
and a SQLite-style placeholder (?). No database needed.
"""

import pytest

from etchdb import Row, sql
from tests._models import User, UserRole


def pg(i: int) -> str:
    return f"${i + 1}"


def lite(i: int) -> str:
    return "?"


# --- insert ----------------------------------------------------------


def test_insert_pg_default_returning_star():
    user = User(id=1, name="Alice", email="alice@example.com")
    q = sql.insert(user, placeholder=pg)

    assert q.sql == "INSERT INTO users (id, name, email) VALUES ($1, $2, $3) RETURNING *"
    assert q.params == [1, "Alice", "alice@example.com"]


def test_insert_pg_no_returning():
    user = User(id=1, name="Alice")
    q = sql.insert(user, placeholder=pg, returning=None)

    assert q.sql == "INSERT INTO users (id, name) VALUES ($1, $2)"
    assert q.params == [1, "Alice"]


def test_insert_pg_returning_specific_columns():
    user = User(id=1, name="Alice")
    q = sql.insert(user, placeholder=pg, returning=["id", "name"])

    assert q.sql.endswith("RETURNING id, name")


def test_insert_sqlite_default_returning_star():
    user = User(id=1, name="Alice", email="alice@example.com")
    q = sql.insert(user, placeholder=lite)

    assert q.sql == "INSERT INTO users (id, name, email) VALUES (?, ?, ?) RETURNING *"
    assert q.params == [1, "Alice", "alice@example.com"]


def test_insert_sqlite_no_returning():
    user = User(id=1, name="Alice")
    q = sql.insert(user, placeholder=lite, returning=None)

    assert q.sql == "INSERT INTO users (id, name) VALUES (?, ?)"
    assert q.params == [1, "Alice"]


# --- insert: model_fields_set semantics ------------------------------


class Counter(Row):
    """All fields defaulted; covers the empty-fields_set path."""

    __table__ = "counters"
    id: int | None = None
    n: int = 0


def test_insert_skips_unset_fields_pg():
    q = sql.insert(User(name="Alice"), placeholder=pg)

    assert q.sql == "INSERT INTO users (name) VALUES ($1) RETURNING *"
    assert q.params == ["Alice"]


def test_insert_skips_unset_fields_sqlite():
    q = sql.insert(User(name="Alice"), placeholder=lite)

    assert q.sql == "INSERT INTO users (name) VALUES (?) RETURNING *"
    assert q.params == ["Alice"]


def test_insert_explicit_none_is_kept():
    """Pydantic distinguishes defaulted from explicitly-None: both go to
    fields_set if set in the constructor, so the column is emitted."""
    q = sql.insert(User(id=None, name="Alice"), placeholder=pg)

    assert q.sql == "INSERT INTO users (id, name) VALUES ($1, $2) RETURNING *"
    assert q.params == [None, "Alice"]


def test_insert_attribute_assignment_tracks_in_fields_set():
    """Pin the Pydantic v2 behavior that direct attribute assignment
    after construction adds to model_fields_set, so a later insert
    sees the assigned field."""
    user = User(name="Alice")
    user.email = "a@example.com"
    q = sql.insert(user, placeholder=pg)

    assert q.sql == "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *"
    assert q.params == ["Alice", "a@example.com"]


def test_insert_no_fields_uses_default_values_pg():
    q = sql.insert(Counter(), placeholder=pg)

    assert q.sql == "INSERT INTO counters DEFAULT VALUES RETURNING *"
    assert q.params == []


def test_insert_no_fields_uses_default_values_sqlite():
    q = sql.insert(Counter(), placeholder=lite, returning=None)

    assert q.sql == "INSERT INTO counters DEFAULT VALUES"
    assert q.params == []


# --- select_one ------------------------------------------------------


def test_select_one_pg_by_id():
    q = sql.select_one(User, placeholder=pg, id=1)

    assert q.sql == "SELECT id, name, email FROM users WHERE id = $1 LIMIT 1"
    assert q.params == [1]


def test_select_one_pg_multiple_filters():
    q = sql.select_one(User, placeholder=pg, name="Alice", email="alice@example.com")

    assert q.sql == ("SELECT id, name, email FROM users WHERE name = $1 AND email = $2 LIMIT 1")
    assert q.params == ["Alice", "alice@example.com"]


def test_select_one_pg_no_filters():
    q = sql.select_one(User, placeholder=pg)

    assert q.sql == "SELECT id, name, email FROM users LIMIT 1"
    assert q.params == []


def test_select_one_sqlite_by_id():
    q = sql.select_one(User, placeholder=lite, id=1)

    assert q.sql == "SELECT id, name, email FROM users WHERE id = ? LIMIT 1"
    assert q.params == [1]


def test_select_one_sqlite_multiple_filters():
    q = sql.select_one(User, placeholder=lite, name="Alice", email="alice@example.com")

    assert q.sql == ("SELECT id, name, email FROM users WHERE name = ? AND email = ? LIMIT 1")
    assert q.params == ["Alice", "alice@example.com"]


# --- select_many -----------------------------------------------------


def test_select_many_pg_with_where():
    q = sql.select_many(User, placeholder=pg, name="Alice")

    assert q.sql == "SELECT id, name, email FROM users WHERE name = $1"
    assert q.params == ["Alice"]


def test_select_many_pg_pagination_and_order():
    q = sql.select_many(User, placeholder=pg, limit=10, offset=20, order_by="id")

    assert q.sql == "SELECT id, name, email FROM users ORDER BY id LIMIT $1 OFFSET $2"
    assert q.params == [10, 20]


def test_select_many_pg_filter_with_pagination():
    q = sql.select_many(User, placeholder=pg, name="Alice", limit=5)

    assert q.sql == "SELECT id, name, email FROM users WHERE name = $1 LIMIT $2"
    assert q.params == ["Alice", 5]


def test_select_many_pg_order_by_list():
    q = sql.select_many(User, placeholder=pg, order_by=["name ASC", "id DESC"])

    assert q.sql == "SELECT id, name, email FROM users ORDER BY name ASC, id DESC"


def test_select_many_sqlite_with_where():
    q = sql.select_many(User, placeholder=lite, name="Alice")

    assert q.sql == "SELECT id, name, email FROM users WHERE name = ?"
    assert q.params == ["Alice"]


def test_select_many_sqlite_pagination_and_order():
    q = sql.select_many(User, placeholder=lite, limit=10, offset=20, order_by="id")

    assert q.sql == "SELECT id, name, email FROM users ORDER BY id LIMIT ? OFFSET ?"
    assert q.params == [10, 20]


def test_select_many_sqlite_filter_with_pagination():
    q = sql.select_many(User, placeholder=lite, name="Alice", limit=5)

    assert q.sql == "SELECT id, name, email FROM users WHERE name = ? LIMIT ?"
    assert q.params == ["Alice", 5]


# --- update ----------------------------------------------------------


def test_update_pg_basic():
    user = User(id=1, name="Alice", email="new@example.com")
    q = sql.update(user, placeholder=pg)

    assert q.sql == "UPDATE users SET name = $1, email = $2 WHERE id = $3"
    assert q.params == ["Alice", "new@example.com", 1]


def test_update_pg_with_returning():
    user = User(id=1, name="Alice")
    q = sql.update(user, placeholder=pg, returning="*")

    assert q.sql == "UPDATE users SET name = $1 WHERE id = $2 RETURNING *"
    assert q.params == ["Alice", 1]


def test_update_pg_composite_pk():
    role = UserRole(user_id=1, role_id=2, note="admin")
    q = sql.update(role, placeholder=pg)

    assert q.sql == "UPDATE user_roles SET note = $1 WHERE user_id = $2 AND role_id = $3"
    assert q.params == ["admin", 1, 2]


def test_update_pg_with_only_pk_fields_raises():
    class IdOnly(Row):
        __table__ = "id_only"
        id: int

    with pytest.raises(ValueError, match="no non-PK fields"):
        sql.update(IdOnly(id=1), placeholder=pg)


def test_update_sqlite_basic():
    user = User(id=1, name="Alice", email="new@example.com")
    q = sql.update(user, placeholder=lite)

    assert q.sql == "UPDATE users SET name = ?, email = ? WHERE id = ?"
    assert q.params == ["Alice", "new@example.com", 1]


def test_update_sqlite_with_returning():
    user = User(id=1, name="Alice")
    q = sql.update(user, placeholder=lite, returning="*")

    assert q.sql.endswith("RETURNING *")


def test_update_sqlite_composite_pk():
    role = UserRole(user_id=1, role_id=2, note="admin")
    q = sql.update(role, placeholder=lite)

    assert q.sql == "UPDATE user_roles SET note = ? WHERE user_id = ? AND role_id = ?"


# --- update: model_fields_set semantics ------------------------------


def test_update_skips_unset_fields_pg():
    """Partial-update: an unmodified email stays unmodified rather than
    being clobbered to NULL/default."""
    user = User(id=1, name="NewName")
    q = sql.update(user, placeholder=pg)

    assert q.sql == "UPDATE users SET name = $1 WHERE id = $2"
    assert q.params == ["NewName", 1]


def test_update_skips_unset_fields_sqlite():
    user = User(id=1, name="NewName")
    q = sql.update(user, placeholder=lite)

    assert q.sql == "UPDATE users SET name = ? WHERE id = ?"
    assert q.params == ["NewName", 1]


def test_update_with_only_pk_set_raises():
    """Even on a model that defines non-PK fields, if none are set,
    there's nothing to update."""
    role = UserRole(user_id=1, role_id=2)  # note is defaulted, not in fields_set
    with pytest.raises(ValueError, match="no non-PK fields"):
        sql.update(role, placeholder=pg)


def test_update_attribute_assignment_tracks_in_fields_set():
    """A field set via attribute assignment after construction is
    treated as updated."""
    user = User(id=1, name="Alice")
    user.email = "new@example.com"
    q = sql.update(user, placeholder=pg)

    assert q.sql == "UPDATE users SET name = $1, email = $2 WHERE id = $3"
    assert q.params == ["Alice", "new@example.com", 1]


def test_update_unset_pk_raises():
    """Without an explicit id, WHERE would silently match nothing; raise
    instead so the caller notices."""
    user = User(name="Alice")
    with pytest.raises(ValueError, match="primary key"):
        sql.update(user, placeholder=pg)


def test_update_partial_composite_pk_raises():
    """Even one missing PK component is enough to identify nothing."""
    role = UserRole.model_construct(user_id=1, note="x")  # role_id not set
    with pytest.raises(ValueError, match="primary key"):
        sql.update(role, placeholder=pg)


# --- delete ----------------------------------------------------------


def test_delete_pg_simple_pk():
    user = User(id=1, name="Alice")
    q = sql.delete(user, placeholder=pg)

    assert q.sql == "DELETE FROM users WHERE id = $1"
    assert q.params == [1]


def test_delete_pg_composite_pk():
    role = UserRole(user_id=1, role_id=2)
    q = sql.delete(role, placeholder=pg)

    assert q.sql == "DELETE FROM user_roles WHERE user_id = $1 AND role_id = $2"


def test_delete_sqlite_simple_pk():
    user = User(id=1, name="Alice")
    q = sql.delete(user, placeholder=lite)

    assert q.sql == "DELETE FROM users WHERE id = ?"
    assert q.params == [1]


def test_delete_sqlite_composite_pk():
    role = UserRole(user_id=1, role_id=2)
    q = sql.delete(role, placeholder=lite)

    assert q.sql == "DELETE FROM user_roles WHERE user_id = ? AND role_id = ?"


def test_delete_unset_pk_raises():
    user = User(name="Alice")
    with pytest.raises(ValueError, match="primary key"):
        sql.delete(user, placeholder=pg)


# --- error cases -----------------------------------------------------


def test_row_without_table_raises():
    class NoTable(Row):
        id: int
        name: str

    with pytest.raises(ValueError, match="__table__"):
        sql.insert(NoTable(id=1, name="x"), placeholder=pg)


def test_select_without_table_raises():
    class NoTable(Row):
        id: int

    with pytest.raises(ValueError, match="__table__"):
        sql.select_one(NoTable, placeholder=pg, id=1)
