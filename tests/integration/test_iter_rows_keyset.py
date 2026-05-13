"""Integration tests for db.iter_rows_keyset across all three backends.

The keyset iterator emits `WHERE <by> > <last_seen>` per page, so total
cost stays O(N) rather than O(N^2) like `iter_rows`. Tests pin: full
ordered yield, batch boundaries, filter propagation, default `by`
column, explicit `by` override, and the input validation."""

import pytest

from etchdb import DB
from tests._models import User


async def test_iter_rows_keyset_yields_all_rows_ordered(db: DB):
    for i in range(1, 8):
        await db.insert(User(id=i, name=f"u{i}"))

    rows = [u async for u in db.iter_rows_keyset(User, batch_size=3)]

    assert [u.id for u in rows] == [1, 2, 3, 4, 5, 6, 7]


async def test_iter_rows_keyset_uses_pk_by_default(db: DB):
    """`by=None` defaults to `model.__pk__[0]`; pagination still
    works when the user doesn't specify the column."""
    for i in range(1, 4):
        await db.insert(User(id=i, name=f"u{i}"))

    rows = [u async for u in db.iter_rows_keyset(User)]

    assert [u.id for u in rows] == [1, 2, 3]


async def test_iter_rows_keyset_filter_propagates(db: DB):
    """Extra filter kwargs are AND'd with the keyset cursor on every
    page. Only rows that match the filter are yielded."""
    await db.insert(User(id=1, name="alice"))
    await db.insert(User(id=2, name="bob"))
    await db.insert(User(id=3, name="alice"))
    await db.insert(User(id=4, name="bob"))

    rows = [u async for u in db.iter_rows_keyset(User, batch_size=2, name="alice")]

    assert [u.id for u in rows] == [1, 3]


async def test_iter_rows_keyset_empty_table_yields_nothing(db: DB):
    rows = [u async for u in db.iter_rows_keyset(User)]
    assert rows == []


async def test_iter_rows_keyset_rejects_non_positive_batch_size(db: DB):
    with pytest.raises(ValueError, match="batch_size"):
        async for _ in db.iter_rows_keyset(User, batch_size=0):
            pass


async def test_iter_rows_keyset_rejects_by_in_filters(db: DB):
    """If the user already constrains the keyset column via a filter,
    the cursor's `>` predicate plus their `=` predicate would be
    over-determined. Reject up front."""
    with pytest.raises(ValueError, match="cannot also appear in filters"):
        async for _ in db.iter_rows_keyset(User, by="id", id=5):
            pass


async def test_iter_rows_keyset_empty_list_filter_short_circuits(db: DB):
    """Empty-list filter follows the same short-circuit convention as
    iter_rows / query / get; nothing is yielded, no round trip."""
    await db.insert(User(id=1, name="alice"))
    rows = [u async for u in db.iter_rows_keyset(User, id=[])]
    assert rows == []


async def test_iter_rows_keyset_composite_pk_requires_explicit_by(db: DB):
    """Defaulting `by` to `__pk__[0]` on a composite PK can silently
    duplicate or skip rows when the leading column ties. Refuse and
    point the user at the explicit `by=` form."""
    from tests._models import UserRole

    with pytest.raises(ValueError, match="composite PK"):
        async for _ in db.iter_rows_keyset(UserRole):
            pass


async def test_iter_rows_keyset_rejects_non_db_column(db: DB):
    """If the user passes `by=<col>` for a field that isn't a DB
    column (e.g. excluded via __fields_not_in_db__), the row dict
    won't carry it and the cursor advance would KeyError mid-loop.
    Catch the misuse before the first query."""
    with pytest.raises(ValueError, match="not a DB column"):
        async for _ in db.iter_rows_keyset(User, by="nonexistent_col"):
            pass


async def test_iter_rows_keyset_paginates_through_all_nulls(db: DB):
    """Every row has `email=NULL`. Cursor advances via the PK tail
    (id) inside the NULL region and terminates correctly."""
    for i in range(1, 5):
        await db.insert(User(id=i, name=f"u{i}", email=None))

    rows = [u async for u in db.iter_rows_keyset(User, by="email", batch_size=2)]

    assert [u.id for u in rows] == [1, 2, 3, 4]
    assert all(u.email is None for u in rows)


async def test_iter_rows_keyset_paginates_through_mixed_nulls(db: DB):
    """Mixed NULL + non-NULL with NULL at a full-page boundary --
    the failure mode from issue #1. With explicit NULLS FIRST and a
    compound cursor, NULL rows come first and we paginate through
    cleanly."""
    await db.insert(User(id=1, name="u1", email=None))
    await db.insert(User(id=2, name="u2", email=None))
    await db.insert(User(id=3, name="u3", email="a@x"))
    await db.insert(User(id=4, name="u4", email="b@x"))

    rows = [u async for u in db.iter_rows_keyset(User, by="email", batch_size=2)]

    assert [u.id for u in rows] == [1, 2, 3, 4]
    assert [u.email for u in rows] == [None, None, "a@x", "b@x"]


async def test_iter_rows_keyset_paginates_through_tied_values(db: DB):
    """Same `email` on every row. The PK tail breaks ties so all
    rows yield in id order; with the pre-fix `WHERE by > last_seen`
    cursor, only the first page would be yielded."""
    for i in range(1, 5):
        await db.insert(User(id=i, name=f"u{i}", email="same@x"))

    rows = [u async for u in db.iter_rows_keyset(User, by="email", batch_size=2)]

    assert [u.id for u in rows] == [1, 2, 3, 4]


async def test_iter_rows_keyset_paginates_through_nulls_and_ties(db: DB):
    """Combination case: some NULLs, two distinct non-NULL values
    each repeated. Order must be NULLs first then non-NULL groups,
    tie-broken by id within each group."""
    await db.insert(User(id=1, name="u1", email=None))
    await db.insert(User(id=2, name="u2", email=None))
    await db.insert(User(id=3, name="u3", email="a@x"))
    await db.insert(User(id=4, name="u4", email="a@x"))
    await db.insert(User(id=5, name="u5", email="b@x"))
    await db.insert(User(id=6, name="u6", email="b@x"))

    rows = [u async for u in db.iter_rows_keyset(User, by="email", batch_size=2)]

    assert [u.id for u in rows] == [1, 2, 3, 4, 5, 6]


async def test_iter_rows_keyset_composite_pk_tie_break(db: DB):
    """Non-PK `by` on a composite-PK model. Ties in `by` resolve via
    both PK columns chained, so the cursor advances correctly when
    a non-PK column duplicates across PK combinations."""
    from tests._models import UserRole

    await db.insert(UserRole(user_id=1, role_id=10, note="x"))
    await db.insert(UserRole(user_id=1, role_id=20, note="x"))
    await db.insert(UserRole(user_id=2, role_id=10, note="x"))
    await db.insert(UserRole(user_id=2, role_id=20, note="y"))

    rows = [u async for u in db.iter_rows_keyset(UserRole, by="note", batch_size=2)]

    assert [(r.user_id, r.role_id, r.note) for r in rows] == [
        (1, 10, "x"),
        (1, 20, "x"),
        (2, 10, "x"),
        (2, 20, "y"),
    ]


async def test_iter_rows_keyset_filter_with_nulls_and_ties(db: DB):
    """Filter AND'd with the cursor predicate -- exercises the
    parenthesization of the OR-group inside the WHERE chain."""
    await db.insert(User(id=1, name="alice", email=None))
    await db.insert(User(id=2, name="bob", email=None))
    await db.insert(User(id=3, name="alice", email="a@x"))
    await db.insert(User(id=4, name="alice", email="a@x"))
    await db.insert(User(id=5, name="bob", email="a@x"))

    rows = [u async for u in db.iter_rows_keyset(User, by="email", batch_size=2, name="alice")]

    assert [u.id for u in rows] == [1, 3, 4]
