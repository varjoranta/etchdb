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

from etchdb import DB, MigrationStatus, UndefinedTableError


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


# --- multi-statement files ------------------------------------------


async def test_migrate_multi_statement_file(db: DB, tmp_path: Path):
    """A single file with multiple `;`-separated statements applies
    as one logical unit. The README's headline migration example is
    multi-statement; this pins that it works on every backend."""
    await _reset_tracking(db)
    _write(
        tmp_path,
        "0001_users_and_index.sql",
        """
        CREATE TABLE m_multi (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE INDEX m_multi_name_idx ON m_multi (name);
        INSERT INTO m_multi (id, name) VALUES (1, 'alice'), (2, 'bob');
        """,
    )

    try:
        n = await db.migrate(tmp_path)
        assert n == 1

        rows = await db.fetch("SELECT id, name FROM m_multi ORDER BY id")
        assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    finally:
        await db.execute("DROP TABLE IF EXISTS m_multi")
        await _reset_tracking(db)


# --- failure / rollback ---------------------------------------------


async def test_migrate_failure_leaves_no_tracking_row(db: DB, tmp_path: Path):
    """A failing migration must not record a tracking row, so the
    next run sees the migration as still-pending. On Postgres the
    implicit transaction also rolls back any schema changes made
    before the failure (verified inside the same test). On SQLite
    `executescript` cannot offer that rollback guarantee for prior
    statements; documented limitation, not asserted here."""
    await _reset_tracking(db)
    is_postgres = "Aiosqlite" not in type(db._adapter).__name__
    _write(
        tmp_path,
        "0001_breaks.sql",
        """
        CREATE TABLE m_broken (id INTEGER PRIMARY KEY);
        INSERT INTO m_nonexistent_table (id) VALUES (1);
        """,
    )

    try:
        with pytest.raises(UndefinedTableError):
            await db.migrate(tmp_path)

        # No tracking row regardless of backend.
        s = await db.migration_status(tmp_path)
        assert s.pending == ["0001_breaks.sql"]
        assert s.applied == []

        # On Postgres the transaction rolled back the CREATE TABLE too.
        if is_postgres:
            with pytest.raises(UndefinedTableError):
                await db.execute("SELECT 1 FROM m_broken")
    finally:
        await db.execute("DROP TABLE IF EXISTS m_broken")
        await _reset_tracking(db)


# --- PL/pgSQL DO block ----------------------------------------------


async def test_migrate_pl_pgsql_do_block(db: DB, tmp_path: Path):
    """`DO $$ BEGIN ... END $$` contains the literal keyword BEGIN, but
    the runner's transaction-control check strips dollar-quoted blocks
    before scanning. Verifying the regex stripping holds end-to-end on
    a real Postgres connection (PL/pgSQL is PG-only syntax)."""
    if "Aiosqlite" in type(db._adapter).__name__:
        pytest.skip("PL/pgSQL DO block is Postgres-specific syntax")

    await _reset_tracking(db)
    _write(
        tmp_path,
        "0001_do_block.sql",
        """
        CREATE TABLE m_do (id INTEGER PRIMARY KEY, label TEXT);
        DO $$
        BEGIN
            INSERT INTO m_do (id, label) VALUES (1, 'from-do');
        END $$;
        """,
    )

    try:
        n = await db.migrate(tmp_path)
        assert n == 1

        rows = await db.fetch("SELECT id, label FROM m_do")
        assert rows == [{"id": 1, "label": "from-do"}]
    finally:
        await db.execute("DROP TABLE IF EXISTS m_do")
        await _reset_tracking(db)


# --- drift recovery flow --------------------------------------------


async def test_migrate_drift_recovery(db: DB, tmp_path: Path):
    """The drift-refusal error names a recovery
    (`DELETE FROM _etchdb_migrations WHERE filename = ...`); following
    it lets the new content apply on a re-run. End-to-end exercise of
    the recovery path the error message points at."""
    await _reset_tracking(db)
    _write(tmp_path, "0001_users.sql", "CREATE TABLE m_recover (id INTEGER PRIMARY KEY)")

    try:
        await db.migrate(tmp_path)

        # Edit the file -- now drift-detected.
        _write(
            tmp_path,
            "0001_users.sql",
            "CREATE TABLE m_recover (id INTEGER PRIMARY KEY, name TEXT)",
        )
        with pytest.raises(RuntimeError, match="inconsistent"):
            await db.migrate(tmp_path)

        # Recovery: drop the original table, delete the tracking row,
        # re-run. The new content applies.
        await db.execute("DROP TABLE m_recover")
        ph = db._adapter.placeholder
        await db.execute(
            f"DELETE FROM _etchdb_migrations WHERE filename = {ph(0)}",
            "0001_users.sql",
        )

        n = await db.migrate(tmp_path)
        assert n == 1

        # Verify the new schema is in place.
        await db.execute("INSERT INTO m_recover (id, name) VALUES (1, 'x')")
        rows = await db.fetch("SELECT id, name FROM m_recover")
        assert rows == [{"id": 1, "name": "x"}]
    finally:
        await db.execute("DROP TABLE IF EXISTS m_recover")
        await _reset_tracking(db)


# --- no-transaction marker end-to-end -------------------------------


async def test_migrate_no_transaction_marker_applies(db: DB, tmp_path: Path):
    """The `-- etchdb:no-transaction` marker on the first non-blank
    line skips the runner's transaction wrapping. End-to-end path
    coverage: the migration applies and a tracking row is recorded
    even though the runner did not open `db.transaction()`."""
    await _reset_tracking(db)
    _write(
        tmp_path,
        "0001_marked.sql",
        "-- etchdb:no-transaction\nCREATE TABLE m_marked (id INTEGER PRIMARY KEY)",
    )

    try:
        n = await db.migrate(tmp_path)
        assert n == 1

        s = await db.migration_status(tmp_path)
        assert s.applied == ["0001_marked.sql"]
        assert s.pending == []
    finally:
        await db.execute("DROP TABLE IF EXISTS m_marked")
        await _reset_tracking(db)
