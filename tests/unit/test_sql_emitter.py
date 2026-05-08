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


def test_select_one_none_filter_emits_is_null_pg():
    """A None value must produce `IS NULL`, not `= NULL` (which silently
    matches nothing in SQL). No placeholder is consumed."""
    q = sql.select_one(User, placeholder=pg, email=None)

    assert q.sql == "SELECT id, name, email FROM users WHERE email IS NULL LIMIT 1"
    assert q.params == []


def test_select_one_none_filter_emits_is_null_sqlite():
    q = sql.select_one(User, placeholder=lite, email=None)

    assert q.sql == "SELECT id, name, email FROM users WHERE email IS NULL LIMIT 1"
    assert q.params == []


def test_select_one_mixed_none_and_value_filter_pg():
    """Mixed Nones and bound values in one filter set: placeholder
    numbering tracks bound values only, IS NULL clauses are inlined."""
    q = sql.select_one(User, placeholder=pg, name="Alice", email=None)

    assert q.sql == ("SELECT id, name, email FROM users WHERE name = $1 AND email IS NULL LIMIT 1")
    assert q.params == ["Alice"]


def test_select_one_value_after_none_keeps_placeholder_numbering_pg():
    """A non-None after a None must still bind to $1, not $2."""
    q = sql.select_one(User, placeholder=pg, email=None, name="Alice")

    assert q.sql == ("SELECT id, name, email FROM users WHERE email IS NULL AND name = $1 LIMIT 1")
    assert q.params == ["Alice"]


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


def test_select_many_none_filter_emits_is_null_pg():
    q = sql.select_many(User, placeholder=pg, email=None)

    assert q.sql == "SELECT id, name, email FROM users WHERE email IS NULL"
    assert q.params == []


def test_select_many_none_filter_with_pagination_pg():
    """LIMIT/OFFSET placeholder numbering must skip the IS NULL field."""
    q = sql.select_many(User, placeholder=pg, email=None, limit=5, offset=10)

    assert q.sql == "SELECT id, name, email FROM users WHERE email IS NULL LIMIT $1 OFFSET $2"
    assert q.params == [5, 10]


def test_select_many_mixed_none_and_value_with_pagination_pg():
    q = sql.select_many(User, placeholder=pg, name="Alice", email=None, limit=5)

    assert q.sql == ("SELECT id, name, email FROM users WHERE name = $1 AND email IS NULL LIMIT $2")
    assert q.params == ["Alice", 5]


def test_select_many_negative_limit_raises():
    """LIMIT -1 is silently 'no limit' on SQLite and an error on PG;
    the inconsistency is exactly the kind of footgun etchdb should
    refuse rather than paper over."""
    with pytest.raises(ValueError, match="limit"):
        sql.select_many(User, placeholder=pg, limit=-1)


def test_select_many_negative_offset_raises():
    with pytest.raises(ValueError, match="offset"):
        sql.select_many(User, placeholder=pg, offset=-1)


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


def test_update_with_extra_where_pg():
    """`where=` AND's extra equality filters onto the PK-only WHERE."""
    user = User(id=1, name="Alice")
    q = sql.update(user, placeholder=pg, where={"tenant_id": 5})

    assert q.sql == "UPDATE users SET name = $1 WHERE id = $2 AND tenant_id = $3"
    assert q.params == ["Alice", 1, 5]


def test_update_with_extra_where_sqlite():
    user = User(id=1, name="Alice")
    q = sql.update(user, placeholder=lite, where={"tenant_id": 5})

    assert q.sql == "UPDATE users SET name = ? WHERE id = ? AND tenant_id = ?"
    assert q.params == ["Alice", 1, 5]


def test_update_with_returning_and_extra_where():
    user = User(id=1, name="Alice")
    q = sql.update(user, placeholder=pg, returning="*", where={"tenant_id": 5})

    assert q.sql == ("UPDATE users SET name = $1 WHERE id = $2 AND tenant_id = $3 RETURNING *")


def test_update_where_none_emits_is_null_pg():
    """A None in `where=` AND's an `IS NULL` predicate."""
    user = User(id=1, name="Alice B")
    q = sql.update(user, placeholder=pg, where={"deleted_at": None})

    assert q.sql == "UPDATE users SET name = $1 WHERE id = $2 AND deleted_at IS NULL"
    assert q.params == ["Alice B", 1]


def test_update_where_none_emits_is_null_sqlite():
    user = User(id=1, name="Alice B")
    q = sql.update(user, placeholder=lite, where={"deleted_at": None})

    assert q.sql == "UPDATE users SET name = ? WHERE id = ? AND deleted_at IS NULL"
    assert q.params == ["Alice B", 1]


def test_update_set_clause_none_value_stays_equals_null():
    """`field = NULL` is the right SQL inside SET (it sets the column
    to NULL), so SET-side Nones must keep using `= placeholder` and
    bind None as a parameter rather than becoming `IS NULL`."""
    q = sql.update(User.patch(id=1, email=None), placeholder=pg)

    assert q.sql == "UPDATE users SET email = $1 WHERE id = $2"
    assert q.params == [None, 1]


def test_update_where_overlapping_pk_raises():
    """Re-specifying a PK field in where= would produce a confusing
    `id = $a AND id = $b` clause; reject it."""
    user = User(id=1, name="Alice")
    with pytest.raises(ValueError, match=r"overlap with __pk__"):
        sql.update(user, placeholder=pg, where={"id": 99})


def test_update_with_empty_where_is_same_as_no_where():
    """An empty mapping adds no extra filters, identical to where=None."""
    user = User(id=1, name="Alice")
    q_none = sql.update(user, placeholder=pg)
    q_empty = sql.update(user, placeholder=pg, where={})
    assert q_empty.sql == q_none.sql
    assert q_empty.params == q_none.params


def test_delete_with_empty_where_is_same_as_no_where():
    user = User(id=1, name="Alice")
    q_none = sql.delete(user, placeholder=pg)
    q_empty = sql.delete(user, placeholder=pg, where={})
    assert q_empty.sql == q_none.sql
    assert q_empty.params == q_none.params


def test_update_unset_pk_raises():
    """Without an explicit id, WHERE would silently match nothing; raise
    instead so the caller notices."""
    user = User(name="Alice")
    with pytest.raises(ValueError, match="primary key"):
        sql.update(user, placeholder=pg)


def test_update_via_row_patch_emits_only_set_fields():
    """Row.patch lets a model with required fields produce a partial
    update without Optional-everywhere lying about the schema."""
    q = sql.update(User.patch(id=1, name="Alice B"), placeholder=pg)

    assert q.sql == "UPDATE users SET name = $1 WHERE id = $2"
    assert q.params == ["Alice B", 1]


def test_row_patch_skips_validation():
    """patch() uses model_construct, so required fields don't need
    placeholder values when only updating a subset."""

    class StrictNote(Row):
        __table__ = "notes"
        id: int
        body: str  # NOT NULL in DB
        status: str  # NOT NULL in DB

    # Plain construction would raise ValidationError for missing body.
    note = StrictNote.patch(id=1, status="archived")
    assert note.model_fields_set == {"id", "status"}
    q = sql.update(note, placeholder=pg)
    assert q.sql == "UPDATE notes SET status = $1 WHERE id = $2"
    assert q.params == ["archived", 1]


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


def test_delete_with_extra_where_pg():
    # delete only consults the PK + where=, but the Row's other fields
    # need to satisfy Pydantic validation at construction time.
    user = User(id=1, name="Alice")
    q = sql.delete(user, placeholder=pg, where={"tenant_id": 5})

    assert q.sql == "DELETE FROM users WHERE id = $1 AND tenant_id = $2"
    assert q.params == [1, 5]


def test_delete_with_extra_where_sqlite():
    user = User(id=1, name="Alice")
    q = sql.delete(user, placeholder=lite, where={"tenant_id": 5})

    assert q.sql == "DELETE FROM users WHERE id = ? AND tenant_id = ?"
    assert q.params == [1, 5]


def test_delete_where_none_emits_is_null_pg():
    user = User(id=1, name="Alice")
    q = sql.delete(user, placeholder=pg, where={"deleted_at": None})

    assert q.sql == "DELETE FROM users WHERE id = $1 AND deleted_at IS NULL"
    assert q.params == [1]


def test_delete_where_none_emits_is_null_sqlite():
    user = User(id=1, name="Alice")
    q = sql.delete(user, placeholder=lite, where={"deleted_at": None})

    assert q.sql == "DELETE FROM users WHERE id = ? AND deleted_at IS NULL"
    assert q.params == [1]


def test_delete_where_overlapping_pk_raises():
    user = User(id=1, name="Alice")
    with pytest.raises(ValueError, match=r"overlap with __pk__"):
        sql.delete(user, placeholder=pg, where={"id": 99})


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


# --- insert_many / delete_many ---------------------------------------


def test_insert_many_pg_basic():
    rows = [User(id=1, name="A"), User(id=2, name="B"), User(id=3, name="C")]
    q = sql.insert_many(rows, placeholder=pg)

    assert q.sql == ("INSERT INTO users (id, name) VALUES ($1, $2), ($3, $4), ($5, $6)")
    assert q.params == [1, "A", 2, "B", 3, "C"]


def test_insert_many_sqlite_basic():
    rows = [User(id=1, name="A"), User(id=2, name="B")]
    q = sql.insert_many(rows, placeholder=lite)

    assert q.sql == "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)"
    assert q.params == [1, "A", 2, "B"]


def test_insert_many_with_on_conflict_ignore():
    rows = [User(id=1, name="A")]
    q = sql.insert_many(rows, placeholder=pg, on_conflict="ignore")

    assert q.sql.endswith("ON CONFLICT DO NOTHING")


def test_insert_many_empty_raises():
    with pytest.raises(ValueError, match="at least one row"):
        sql.insert_many([], placeholder=pg)


def test_insert_many_mixed_shapes_raises():
    """All rows must share model_fields_set; mixing fields-set is rejected
    early instead of silently dropping columns."""
    rows = [User(id=1, name="A"), User(name="B", email="b@x")]
    with pytest.raises(ValueError, match="model_fields_set"):
        sql.insert_many(rows, placeholder=pg)


def test_insert_many_mixed_row_classes_raises():
    """Two Row subclasses with overlapping field names would otherwise
    silently insert into the first row's table using the first row's
    field layout. Reject the mismatch up front."""

    class Customer(Row):
        __table__ = "customers"
        id: int
        name: str

    rows = [User(id=1, name="A"), Customer(id=2, name="B")]
    with pytest.raises(ValueError, match="same Row subclass"):
        sql.insert_many(rows, placeholder=pg)


def test_insert_many_unknown_on_conflict_raises():
    """The Literal type only enforces statically; a typo at runtime
    would otherwise be silently dropped, leaving the duplicate-key
    handling expectation unmet."""
    from typing import Any

    rows = [User(id=1, name="A")]
    bad: Any = "ignour"
    with pytest.raises(ValueError, match="on_conflict"):
        sql.insert_many(rows, placeholder=pg, on_conflict=bad)


def test_delete_many_single_pk_pg():
    q = sql.delete_many(User, [1, 2, 3], placeholder=pg)

    assert q.sql == "DELETE FROM users WHERE id IN ($1, $2, $3)"
    assert q.params == [1, 2, 3]


def test_delete_many_single_pk_sqlite():
    q = sql.delete_many(User, [1, 2], placeholder=lite)

    assert q.sql == "DELETE FROM users WHERE id IN (?, ?)"
    assert q.params == [1, 2]


def test_delete_many_composite_pk_pg():
    q = sql.delete_many(
        UserRole,
        [{"user_id": 1, "role_id": 2}, {"user_id": 3, "role_id": 4}],
        placeholder=pg,
    )

    assert q.sql == ("DELETE FROM user_roles WHERE (user_id, role_id) IN (($1, $2), ($3, $4))")
    assert q.params == [1, 2, 3, 4]


def test_delete_many_composite_pk_requires_mapping():
    """Composite PK with scalar values rejected up front; the user's
    intent (which value goes with which column) is unrecoverable."""
    with pytest.raises(ValueError, match="mapping per row"):
        sql.delete_many(UserRole, [1, 2], placeholder=pg)


def test_delete_many_composite_pk_missing_field_raises():
    with pytest.raises(ValueError, match="Missing PK fields"):
        sql.delete_many(
            UserRole,
            [{"user_id": 1}],
            placeholder=pg,
        )


def test_delete_many_empty_raises():
    with pytest.raises(ValueError, match="at least one PK"):
        sql.delete_many(User, [], placeholder=pg)


# --- compose ---------------------------------------------------------


def test_sql_compose_get_without_adapter():
    """sql.compose works in plain Python: no DB, no adapter, just a
    placeholder callable."""
    q = sql.compose("get", User, placeholder=pg, id=1)

    assert q.sql == "SELECT id, name, email FROM users WHERE id = $1 LIMIT 1"
    assert q.params == [1]


def test_sql_compose_for_each_op():
    user = User(id=1, name="Alice", email="a@x")

    q_insert = sql.compose("insert", user, placeholder=pg)
    assert q_insert.sql.startswith("INSERT INTO users")

    q_update = sql.compose("update", user, placeholder=pg)
    assert q_update.sql.startswith("UPDATE users")

    q_delete = sql.compose("delete", user, placeholder=pg)
    assert q_delete.sql.startswith("DELETE FROM users")

    q_query = sql.compose("query", User, placeholder=pg, limit=5)
    assert "LIMIT" in q_query.sql


def test_sql_compose_unknown_op_raises():
    with pytest.raises(ValueError, match="Unknown op"):
        sql.compose("frobnicate", User, placeholder=pg)  # type: ignore[arg-type]
