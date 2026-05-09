"""Integration tests for db.migrate / db.migration_status across all
three backends.

Each test writes a small migration directory to `tmp_path` and runs
the helper against the parametrized `db` fixture. The migration
tracking table is created lazily on first call and survives between
tests on the per-test SQLite / PG fixtures only because each fixture
drops the etchdb-owned tables on setup; for shared Postgres state we
explicitly drop the tracking table at the start of each test."""

from pathlib import Path

import pytest

from etchdb import DB, MigrationStatus


async def _reset_tracking(db: DB) -> None:
    await db.execute("DROP TABLE IF EXISTS _etchdb_migrations")


def _write(directory: Path, filename: str, content: str) -> None:
    (directory / filename).write_text(content)


# --- happy path ------------------------------------------------------


async def test_migrate_applies_pending_in_order(db: DB, tmp_path: Path):
    await _reset_tracking(db)
    _write(tmp_path, "0001_init.sql", "CREATE TABLE m_users (id INTEGER PRIMARY KEY, name TEXT)")
    _write(tmp_path, "0002_seed.sql", "INSERT INTO m_users (id, name) VALUES (1, 'alice')")

    try:
        n = await db.migrate(tmp_path)
        assert n == 2

        rows = await db.fetch("SELECT id, name FROM m_users ORDER BY id")
        assert rows == [{"id": 1, "name": "alice"}]
    finally:
        await db.execute("DROP TABLE IF EXISTS m_users")
        await _reset_tracking(db)


async def test_migrate_is_idempotent(db: DB, tmp_path: Path):
    await _reset_tracking(db)
    _write(tmp_path, "0001_init.sql", "CREATE TABLE m_idem (id INTEGER PRIMARY KEY)")

    try:
        first = await db.migrate(tmp_path)
        second = await db.migrate(tmp_path)
        assert first == 1
        assert second == 0
    finally:
        await db.execute("DROP TABLE IF EXISTS m_idem")
        await _reset_tracking(db)


async def test_migration_status_reports_pending_and_applied(db: DB, tmp_path: Path):
    await _reset_tracking(db)
    _write(tmp_path, "0001_a.sql", "CREATE TABLE m_status_a (id INTEGER PRIMARY KEY)")
    _write(tmp_path, "0002_b.sql", "CREATE TABLE m_status_b (id INTEGER PRIMARY KEY)")

    try:
        before = await db.migration_status(tmp_path)
        assert isinstance(before, MigrationStatus)
        assert before.pending == ["0001_a.sql", "0002_b.sql"]
        assert before.applied == []
        assert before.is_consistent

        await db.migrate(tmp_path)

        after = await db.migration_status(tmp_path)
        assert after.pending == []
        assert after.applied == ["0001_a.sql", "0002_b.sql"]
    finally:
        await db.execute("DROP TABLE IF EXISTS m_status_a")
        await db.execute("DROP TABLE IF EXISTS m_status_b")
        await _reset_tracking(db)


# --- consistency rejections -----------------------------------------


async def test_migrate_refuses_when_file_content_drifts(db: DB, tmp_path: Path):
    """Editing an applied migration is silent state corruption; the
    runner must refuse to do anything until the operator resolves it."""
    await _reset_tracking(db)
    _write(tmp_path, "0001_init.sql", "CREATE TABLE m_drift (id INTEGER PRIMARY KEY)")

    try:
        await db.migrate(tmp_path)

        # Change the file content after apply.
        _write(
            tmp_path,
            "0001_init.sql",
            "CREATE TABLE m_drift (id INTEGER PRIMARY KEY, name TEXT)",
        )

        with pytest.raises(RuntimeError, match="inconsistent"):
            await db.migrate(tmp_path)

        s = await db.migration_status(tmp_path)
        assert s.drifted == ["0001_init.sql"]
        assert not s.is_consistent
    finally:
        await db.execute("DROP TABLE IF EXISTS m_drift")
        await _reset_tracking(db)


async def test_migrate_refuses_when_applied_file_disappeared(db: DB, tmp_path: Path):
    """Deleting / renaming an already-applied migration loses the link
    between code and DB. Refuse loudly with a recovery hint."""
    await _reset_tracking(db)
    _write(tmp_path, "0001_init.sql", "CREATE TABLE m_gone (id INTEGER PRIMARY KEY)")

    try:
        await db.migrate(tmp_path)

        (tmp_path / "0001_init.sql").unlink()

        with pytest.raises(RuntimeError, match="no longer in directory"):
            await db.migrate(tmp_path)

        s = await db.migration_status(tmp_path)
        assert s.missing == ["0001_init.sql"]
        assert not s.is_consistent
    finally:
        await db.execute("DROP TABLE IF EXISTS m_gone")
        await _reset_tracking(db)


# --- transaction control rejection ----------------------------------


async def test_migrate_rejects_explicit_begin_commit(db: DB, tmp_path: Path):
    """The runner owns transaction control; a file with explicit
    BEGIN / COMMIT is rejected up front."""
    await _reset_tracking(db)
    _write(
        tmp_path,
        "0001_bad.sql",
        "BEGIN;\nCREATE TABLE m_bad (id INTEGER PRIMARY KEY);\nCOMMIT;",
    )

    try:
        with pytest.raises(ValueError, match="explicit transaction control"):
            await db.migrate(tmp_path)

        # Nothing was applied.
        s = await db.migration_status(tmp_path)
        assert s.pending == ["0001_bad.sql"]
        assert s.applied == []
    finally:
        await _reset_tracking(db)


# --- empty directory -------------------------------------------------


async def test_migrate_with_no_files_is_a_noop(db: DB, tmp_path: Path):
    await _reset_tracking(db)
    try:
        n = await db.migrate(tmp_path)
        assert n == 0
    finally:
        await _reset_tracking(db)
