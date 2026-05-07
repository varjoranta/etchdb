"""Test the Postgres SQL emitter.

These tests run without a database. They verify the generated SQL
string and parameter list for each typed operation. The point of the
inspection feature is that you can test SQL generation in isolation.
"""

import pytest

from etchdb import Row
from etchdb.sql import postgres as pg


class User(Row):
    __table__ = "users"
    id: int
    name: str
    email: str | None = None


class UserRole(Row):
    __table__ = "user_roles"
    __pk__ = ("user_id", "role_id")
    user_id: int
    role_id: int
    note: str | None = None


# --- insert ----------------------------------------------------------


def test_insert_default_returning_star():
    user = User(id=1, name="Alice", email="alice@example.com")
    q = pg.insert(user)

    assert q.sql == ("INSERT INTO users (id, name, email) VALUES ($1, $2, $3) RETURNING *")
    assert q.params == [1, "Alice", "alice@example.com"]


def test_insert_no_returning():
    user = User(id=1, name="Alice")
    q = pg.insert(user, returning=None)

    assert q.sql == "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)"
    assert q.params == [1, "Alice", None]


def test_insert_returning_specific_columns():
    user = User(id=1, name="Alice")
    q = pg.insert(user, returning=["id", "name"])

    assert q.sql.endswith("RETURNING id, name")


# --- select_one ------------------------------------------------------


def test_select_one_by_id():
    q = pg.select_one(User, id=1)

    assert q.sql == "SELECT id, name, email FROM users WHERE id = $1 LIMIT 1"
    assert q.params == [1]


def test_select_one_multiple_filters():
    q = pg.select_one(User, name="Alice", email="alice@example.com")

    assert q.sql == ("SELECT id, name, email FROM users WHERE name = $1 AND email = $2 LIMIT 1")
    assert q.params == ["Alice", "alice@example.com"]


def test_select_one_no_filters():
    q = pg.select_one(User)

    assert q.sql == "SELECT id, name, email FROM users LIMIT 1"
    assert q.params == []


# --- select_many -----------------------------------------------------


def test_select_many_with_where():
    q = pg.select_many(User, name="Alice")

    assert q.sql == "SELECT id, name, email FROM users WHERE name = $1"
    assert q.params == ["Alice"]


def test_select_many_pagination_and_order():
    q = pg.select_many(User, limit=10, offset=20, order_by="id")

    assert q.sql == ("SELECT id, name, email FROM users ORDER BY id LIMIT $1 OFFSET $2")
    assert q.params == [10, 20]


def test_select_many_filter_with_pagination():
    q = pg.select_many(User, name="Alice", limit=5)

    assert q.sql == "SELECT id, name, email FROM users WHERE name = $1 LIMIT $2"
    assert q.params == ["Alice", 5]


def test_select_many_order_by_list():
    q = pg.select_many(User, order_by=["name ASC", "id DESC"])

    assert q.sql == "SELECT id, name, email FROM users ORDER BY name ASC, id DESC"


# --- update ----------------------------------------------------------


def test_update_basic():
    user = User(id=1, name="Alice", email="new@example.com")
    q = pg.update(user)

    assert q.sql == "UPDATE users SET name = $1, email = $2 WHERE id = $3"
    assert q.params == ["Alice", "new@example.com", 1]


def test_update_with_returning():
    user = User(id=1, name="Alice")
    q = pg.update(user, returning="*")

    assert q.sql.endswith("RETURNING *")
    assert q.params == ["Alice", None, 1]


def test_update_composite_pk():
    role = UserRole(user_id=1, role_id=2, note="admin")
    q = pg.update(role)

    assert q.sql == ("UPDATE user_roles SET note = $1 WHERE user_id = $2 AND role_id = $3")
    assert q.params == ["admin", 1, 2]


def test_update_with_only_pk_fields_raises():
    class IdOnly(Row):
        __table__ = "id_only"
        id: int

    with pytest.raises(ValueError, match="no non-PK fields"):
        pg.update(IdOnly(id=1))


# --- delete ----------------------------------------------------------


def test_delete_simple_pk():
    user = User(id=1, name="Alice")
    q = pg.delete(user)

    assert q.sql == "DELETE FROM users WHERE id = $1"
    assert q.params == [1]


def test_delete_composite_pk():
    role = UserRole(user_id=1, role_id=2)
    q = pg.delete(role)

    assert q.sql == "DELETE FROM user_roles WHERE user_id = $1 AND role_id = $2"
    assert q.params == [1, 2]


# --- error cases -----------------------------------------------------


def test_row_without_table_raises():
    class NoTable(Row):
        id: int
        name: str

    with pytest.raises(ValueError, match="__table__"):
        pg.insert(NoTable(id=1, name="x"))


def test_select_without_table_raises():
    class NoTable(Row):
        id: int

    with pytest.raises(ValueError, match="__table__"):
        pg.select_one(NoTable, id=1)
