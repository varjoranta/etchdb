"""SQL emitted for filter operators, on both placeholder styles.

No database needed; `tests/integration/test_filter_operators.py` runs
the same operators against every backend.
"""

import pytest

from etchdb import (
    Contains,
    Ge,
    Gt,
    ILike,
    Inc,
    Le,
    Like,
    Lt,
    Not,
    Now,
    Range,
    StartsWith,
    sql,
)
from tests._models import User, UserRole
from tests.unit.test_sql_emitter import lite, pg


def _where(placeholder, **filters) -> tuple[str, list]:
    q = sql.select_many(User, placeholder=placeholder, **filters)
    return q.sql.removeprefix("SELECT id, name, email FROM users WHERE "), q.params


@pytest.mark.parametrize(("operator", "symbol"), [(Gt, ">"), (Ge, ">="), (Lt, "<"), (Le, "<=")])
def test_comparison(operator, symbol):
    assert _where(pg, id=operator(5)) == (f"id {symbol} $1", [5])
    assert _where(lite, id=operator(5)) == (f"id {symbol} ?", [5])


def test_comparison_with_now_binds_nothing():
    assert _where(pg, name=Lt(Now()), id=3) == ("name < CURRENT_TIMESTAMP AND id = $1", [3])


@pytest.mark.parametrize("operator", [Gt, Ge, Lt, Le])
def test_comparison_with_none_raises(operator):
    with pytest.raises(ValueError, match="never matches"):
        _where(pg, id=operator(None))


def test_comparison_rejects_other_sentinels():
    with pytest.raises(ValueError, match="takes a single value or Now"):
        _where(pg, id=Gt(Inc()))
    with pytest.raises(ValueError, match="takes a single value or Now"):
        _where(pg, id=Gt(Gt(1)))
    for collection in ([1, 2], (1, 2), {1, 2}):
        with pytest.raises(ValueError, match="takes a single value or Now"):
            _where(pg, id=Gt(collection))
        with pytest.raises(ValueError, match="takes a single value or Now"):
            _where(pg, id=Range(ge=collection))


def test_range_two_bounds():
    assert _where(pg, id=Range(ge=1, lt=9)) == ("id >= $1 AND id < $2", [1, 9])
    assert _where(lite, id=Range(gt=1, le=9)) == ("id > ? AND id <= ?", [1, 9])


def test_range_one_bound_and_falsy_bounds():
    assert _where(pg, id=Range(ge=0)) == ("id >= $1", [0])
    assert _where(pg, id=Range(ge=5, lt=None)) == ("id >= $1", [5])
    assert _where(pg, name=Range(ge="", lt="m")) == ("name >= $1 AND name < $2", ["", "m"])


def test_range_with_now():
    assert _where(pg, name=Range(ge="2026-01-01", lt=Now()), id=1) == (
        "name >= $1 AND name < CURRENT_TIMESTAMP AND id = $2",
        ["2026-01-01", 1],
    )


def test_range_rejects_bad_bounds():
    with pytest.raises(ValueError, match="at least one bound"):
        Range()
    with pytest.raises(ValueError, match="one lower bound"):
        Range(gt=1, ge=1)
    with pytest.raises(ValueError, match="one upper bound"):
        Range(lt=1, le=1)
    with pytest.raises(TypeError):
        Range(1, 2)  # ty: ignore[too-many-positional-arguments]


def test_not_value_none_and_list():
    assert _where(pg, name=Not("x")) == ("name <> $1", ["x"])
    assert _where(pg, email=Not(None)) == ("email IS NOT NULL", [])
    assert _where(pg, id=Not([1, 2])) == ("id NOT IN ($1, $2)", [1, 2])
    assert _where(lite, id=Not((1, 2))) == ("id NOT IN (?, ?)", [1, 2])


def test_not_rejects_empty_list_none_in_list_and_operators():
    with pytest.raises(ValueError, match="empty list in Not"):
        _where(pg, id=Not([]))
    with pytest.raises(ValueError, match="None inside a Not"):
        _where(pg, id=Not([1, None]))
    with pytest.raises(ValueError, match="Not takes a value"):
        Not(Like("a%"))
    with pytest.raises(ValueError, match="Not takes a value"):
        Not(Now())


def test_like_and_ilike():
    """Postgres escapes with backslash by default; the clause is left
    out there because its literal does not parse when
    `standard_conforming_strings` is off."""
    assert _where(pg, name=Like("a%")) == ("name LIKE $1", ["a%"])
    assert _where(lite, name=Like("a%")) == ("name LIKE ? ESCAPE '\\'", ["a%"])
    assert _where(pg, name=ILike("a%")) == ("name ILIKE $1", ["a%"])
    # SQLite has no ILIKE; its LIKE already folds ASCII case.
    assert _where(lite, name=ILike("a%")) == ("name LIKE ? ESCAPE '\\'", ["a%"])


def test_starts_with_and_contains_escape_like_wildcards():
    assert _where(pg, name=StartsWith("50%_a\\b")) == ("name LIKE $1", ["50\\%\\_a\\\\b%"])
    assert _where(pg, name=Contains("50%")) == ("name LIKE $1", ["%50\\%%"])
    assert _where(pg, name=Contains("a", ignore_case=True)) == ("name ILIKE $1", ["%a%"])


def test_starts_with_and_contains_on_sqlite():
    """Case-sensitive matching uses GLOB, since SQLite's LIKE ignores
    ASCII case; the case-insensitive form uses LIKE."""
    assert _where(lite, name=StartsWith("a*b?[c]%_")) == (
        "name GLOB ?",
        ["a[*]b[?][[]c]%_*"],
    )
    assert _where(lite, name=Contains("x")) == ("name GLOB ?", ["*x*"])
    assert _where(lite, name=Contains("50%", ignore_case=True)) == (
        "name LIKE ? ESCAPE '\\'",
        ["%50\\%%"],
    )
    assert _where(lite, name=StartsWith("a", ignore_case=True)) == (
        "name LIKE ? ESCAPE '\\'",
        ["a%"],
    )


@pytest.mark.parametrize("operator", [Like, ILike, StartsWith, Contains])
def test_text_operators_require_str(operator):
    with pytest.raises(TypeError, match="takes a str"):
        operator(5)
    with pytest.raises(TypeError, match="takes a str"):
        operator(None)


def test_operators_mix_with_plain_filters_and_number_placeholders_in_order():
    q = sql.select_many(
        User,
        placeholder=pg,
        limit=10,
        offset=20,
        order_by="id",
        name="alice",
        id=Range(ge=1, lt=9),
        email=Not(["a", "b"]),
    )
    assert q.sql == (
        "SELECT id, name, email FROM users WHERE name = $1 AND id >= $2 AND id < $3 "
        "AND email NOT IN ($4, $5) ORDER BY id LIMIT $6 OFFSET $7"
    )
    assert q.params == ["alice", 1, 9, "a", "b", 10, 20]


def test_update_where_numbers_operator_placeholders_after_set():
    q = sql.update_where(
        User.patch(name="x", email=None),
        placeholder=pg,
        where={"id": Range(ge=1, lt=9), "name": Not("x")},
    )
    assert q.sql == (
        "UPDATE users SET name = $1, email = $2 WHERE id >= $3 AND id < $4 AND name <> $5"
    )
    assert q.params == ["x", None, 1, 9, "x"]


def test_update_and_delete_take_operators_in_where():
    q = sql.update(User.patch(id=1, name="x"), placeholder=pg, where={"email": Not(None)})
    assert q.sql == "UPDATE users SET name = $1 WHERE id = $2 AND email IS NOT NULL"
    assert q.params == ["x", 1]

    q = sql.delete(User.patch(id=1), placeholder=pg, where={"name": StartsWith("tmp_")})
    assert q.sql == "DELETE FROM users WHERE id = $1 AND name LIKE $2"
    assert q.params == [1, "tmp\\_%"]


def test_keyset_numbers_cursor_after_operator_filters():
    q = sql.select_keyset(
        User,
        placeholder=pg,
        by="id",
        batch_size=2,
        last_seen=[7],
        name=Range(ge="a", lt="m"),
    )
    assert q.sql == (
        "SELECT id, name, email FROM users WHERE name >= $1 AND name < $2 AND id > $3 "
        "ORDER BY id LIMIT $4"
    )
    assert q.params == ["a", "m", 7, 2]


def test_select_one_rejects_operators():
    with pytest.raises(ValueError, match="select_one does not accept filter operators"):
        sql.select_one(User, placeholder=pg, id=Gt(1))


def test_set_expression_as_a_filter_value_raises():
    with pytest.raises(ValueError, match="SET expression"):
        _where(pg, name=Now())
    with pytest.raises(ValueError, match="SET expression"):
        sql.update_where(User.patch(name="x"), placeholder=pg, where={"id": Inc()})


def test_operator_as_a_stored_value_raises():
    with pytest.raises(ValueError, match="filter operator as the value"):
        sql.insert(User.patch(id=1, name=Gt(1)), placeholder=pg)
    with pytest.raises(ValueError, match="filter operator as the value"):
        sql.insert_many([User.patch(id=1, name=Gt(1))], placeholder=pg)
    with pytest.raises(ValueError, match="filter operator as the value"):
        sql.update(User.patch(id=1, name=Not("x")), placeholder=pg)
    with pytest.raises(ValueError, match="filter operator as the value"):
        sql.update_where(User.patch(name=Not("x")), placeholder=pg, where={"id": 1})


def test_filter_name_is_still_checked_with_an_operator():
    with pytest.raises(ValueError, match="plain column identifier"):
        sql.select_many(User, placeholder=pg, **{"id; DROP TABLE users": Gt(1)})


@pytest.mark.parametrize("pattern", ["a\\", "a\\\\\\", "\\"])
def test_like_pattern_ending_in_a_lone_escape_raises(pattern):
    for operator in (Like, ILike):
        with pytest.raises(ValueError, match="lone escape character"):
            operator(pattern)


def test_like_pattern_ending_in_an_escaped_backslash_is_accepted():
    assert _where(pg, name=Like("a\\\\")) == ("name LIKE $1", ["a\\\\"])


@pytest.mark.parametrize("sentinel", [Gt(1), Not(1), Not([1, 2]), Inc(), Now()])
def test_update_and_delete_reject_a_sentinel_in_the_primary_key(sentinel):
    """`update` and `delete` address one row; an operator in the key
    would turn them into bulk statements."""
    with pytest.raises(ValueError, match="hold a sentinel"):
        sql.update(User.patch(id=sentinel, name="x"), placeholder=pg)
    with pytest.raises(ValueError, match="hold a sentinel"):
        sql.delete(User.patch(id=sentinel), placeholder=pg)
    with pytest.raises(ValueError, match="hold a sentinel"):
        sql.delete(UserRole.patch(user_id=1, role_id=sentinel), placeholder=pg)


def test_delete_many_rejects_operators():
    with pytest.raises(ValueError, match="delete_many takes primary-key values"):
        sql.delete_many(User, [1, Gt(0)], placeholder=pg)
    with pytest.raises(ValueError, match="delete_many takes primary-key values"):
        sql.delete_many(UserRole, [{"user_id": 1, "role_id": Gt(0)}], placeholder=pg)


def test_operators_compare_by_type_and_arguments():
    assert Gt(5) == Gt(5)
    assert Gt(5) != Ge(5)
    assert Gt(5) != Gt(6)
    assert Gt(5) != 5
    assert Range(ge=1, lt=9) == Range(lt=9, ge=1)
    assert Range(ge=1) != Range(gt=1)
    assert Not([1, 2]) == Not([1, 2])
    assert Like("a%") != ILike("a%")
    assert Contains("a") != StartsWith("a")
    assert Contains("a") != Contains("a", ignore_case=True)
    assert len({Gt(5), Gt(5), Not([1])}) == 2


def test_ilike_is_not_a_like():
    assert not isinstance(ILike("a"), Like)


def test_operator_repr_reads_like_the_call():
    assert repr(Gt(5)) == "Gt(5)"
    assert repr(Range(ge=1, lt=9)) == "Range(ge=1, lt=9)"
    assert repr(Not([1, "a"])) == "Not([1, 'a'])"
    assert repr(ILike("a%")) == "ILike('a%')"
    assert repr(StartsWith("a")) == "StartsWith('a')"
    assert repr(Contains("a", ignore_case=True)) == "Contains('a', ignore_case=True)"
