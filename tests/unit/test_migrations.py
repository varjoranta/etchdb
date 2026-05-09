"""Unit tests for the migration helpers in `etchdb.migrations`.

No DB needed: these cover the regex-based transaction-keyword check,
the no-transaction marker detection, the file-loader, and the
MigrationStatus dataclass."""

import pytest

from etchdb.migrations import (
    MigrationStatus,
    _check_no_explicit_transaction,
    _has_no_transaction_marker,
    _load_migration_files,
    _strip_for_tx_check,
)

# --- transaction-keyword rejection -----------------------------------


def test_check_rejects_bare_begin():
    with pytest.raises(ValueError, match="explicit transaction control"):
        _check_no_explicit_transaction("0001_x.sql", "BEGIN;\nCREATE TABLE t (id INT);\nCOMMIT;")


def test_check_rejects_lowercase_commit():
    with pytest.raises(ValueError, match="explicit transaction control"):
        _check_no_explicit_transaction("0001_x.sql", "create table t (id int); commit;")


def test_check_rejects_start_transaction():
    with pytest.raises(ValueError, match="explicit transaction control"):
        _check_no_explicit_transaction("0001_x.sql", "START TRANSACTION;\nCREATE TABLE t (id INT);")


def test_check_allows_begin_inside_dollar_quoted_block():
    """`DO $$ BEGIN ... END $$` is PL/pgSQL block syntax, not a
    transaction keyword. The strip stage removes the dollar-quoted
    body before the regex runs."""
    sql = """
    DO $$
    BEGIN
        PERFORM 1;
    END $$;
    """
    _check_no_explicit_transaction("0001_x.sql", sql)  # no raise


def test_check_allows_commit_inside_string_literal():
    """A SQL string literal containing the word COMMIT must not trip
    the check."""
    _check_no_explicit_transaction(
        "0001_x.sql",
        "INSERT INTO log (msg) VALUES ('the COMMIT failed');",
    )


def test_check_allows_commit_inside_line_comment():
    _check_no_explicit_transaction("0001_x.sql", "-- COMMIT this later\nCREATE TABLE t (id INT);")


def test_strip_removes_dollar_quoted_blocks():
    s = _strip_for_tx_check("BEGIN;\nDO $$ BEGIN ... END $$;\nROLLBACK;")
    # The inner BEGIN inside DO $$ ... $$ is gone; outer BEGIN/ROLLBACK
    # remain.
    assert "BEGIN" in s
    assert s.count("BEGIN") == 1


# --- no-transaction marker -------------------------------------------


def test_marker_detected_when_first_nonblank_line():
    assert _has_no_transaction_marker("-- etchdb:no-transaction\nCREATE INDEX CONCURRENTLY ...")


def test_marker_detected_after_blank_lines():
    assert _has_no_transaction_marker("\n\n-- etchdb:no-transaction\nCREATE INDEX ...")


def test_marker_not_detected_when_buried():
    assert not _has_no_transaction_marker(
        "CREATE TABLE t (id INT);\n-- etchdb:no-transaction\nCREATE INDEX ..."
    )


def test_marker_not_detected_when_partial_match():
    assert not _has_no_transaction_marker("-- etchdb:no-transaction-please\n...")


# --- file loader -----------------------------------------------------


def test_load_returns_files_sorted_with_checksums(tmp_path):
    (tmp_path / "0002_b.sql").write_text("CREATE TABLE b (id INT);")
    (tmp_path / "0001_a.sql").write_text("CREATE TABLE a (id INT);")
    (tmp_path / "README.md").write_text("ignored")  # non-.sql

    files = _load_migration_files(tmp_path)

    assert [fn for fn, _, _ in files] == ["0001_a.sql", "0002_b.sql"]
    # Two distinct files, two distinct checksums.
    assert files[0][2] != files[1][2]


def test_load_raises_on_missing_directory(tmp_path):
    bad = tmp_path / "nope"
    with pytest.raises(ValueError, match="not found"):
        _load_migration_files(bad)


# --- MigrationStatus dataclass ---------------------------------------


def test_migration_status_is_consistent_when_no_drift_or_missing():
    s = MigrationStatus(pending=["0002_x.sql"], applied=["0001_init.sql"])
    assert s.is_consistent is True


def test_migration_status_inconsistent_on_drift():
    s = MigrationStatus(applied=["0001.sql"], drifted=["0001.sql"])
    assert s.is_consistent is False


def test_migration_status_inconsistent_on_missing():
    s = MigrationStatus(applied=[], missing=["0001_gone.sql"])
    assert s.is_consistent is False
