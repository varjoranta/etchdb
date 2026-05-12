"""Integration tests for the CLI: call `main(argv=...)` against a
real SQLite database and verify exit codes + output. We use SQLite
because it's always available; the CLI uses the same DB.from_url
path as the rest of etchdb so PG-only verification would only
exercise the URL dispatch we already test elsewhere."""

from pathlib import Path

import pytest

from etchdb.cli import main


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    """Each test gets its own SQLite file URL so state doesn't leak."""
    return f"sqlite+aiosqlite:///{tmp_path}/test.db"


def _write_migration(directory: Path, name: str, sql: str) -> None:
    directory.mkdir(exist_ok=True)
    (directory / name).write_text(sql)


def test_cli_migrate_applies_pending(tmp_path, db_url, capsys):
    mig_dir = tmp_path / "migrations"
    _write_migration(mig_dir, "0001_init.sql", "CREATE TABLE m_cli (id INTEGER PRIMARY KEY)")

    rc = main(["migrate", str(mig_dir), "--url", db_url])

    assert rc == 0
    assert "applied 1 migration" in capsys.readouterr().out


def test_cli_migrate_via_env_var(tmp_path, monkeypatch, db_url, capsys):
    """DATABASE_URL is the fallback when --url is not given."""
    mig_dir = tmp_path / "migrations"
    _write_migration(mig_dir, "0001_init.sql", "CREATE TABLE m_env (id INTEGER PRIMARY KEY)")
    monkeypatch.setenv("DATABASE_URL", db_url)

    rc = main(["migrate", str(mig_dir)])

    assert rc == 0
    assert "applied 1 migration" in capsys.readouterr().out


def test_cli_status_consistent_returns_zero(tmp_path, db_url, capsys):
    mig_dir = tmp_path / "migrations"
    _write_migration(mig_dir, "0001_init.sql", "CREATE TABLE m_cli_s (id INTEGER PRIMARY KEY)")

    main(["migrate", str(mig_dir), "--url", db_url])
    capsys.readouterr()  # discard
    rc = main(["status", str(mig_dir), "--url", db_url])

    out = capsys.readouterr().out
    assert rc == 0
    assert "INCONSISTENT" not in out
    assert "applied (1)" in out


def test_cli_status_inconsistent_returns_one(tmp_path, db_url, capsys):
    """Drift makes status exit 1 -- CI / deploy scripts can use this
    as the gate before running migrate."""
    mig_dir = tmp_path / "migrations"
    _write_migration(mig_dir, "0001_init.sql", "CREATE TABLE m_cli_d (id INTEGER PRIMARY KEY)")
    main(["migrate", str(mig_dir), "--url", db_url])
    capsys.readouterr()

    # Drift the file content after applying.
    _write_migration(
        mig_dir,
        "0001_init.sql",
        "CREATE TABLE m_cli_d (id INTEGER PRIMARY KEY, name TEXT)",
    )

    rc = main(["status", str(mig_dir), "--url", db_url])

    out = capsys.readouterr().out
    assert rc == 1
    assert "INCONSISTENT" in out
    assert "drifted" in out


def test_cli_migrate_inconsistent_returns_one(tmp_path, db_url, capsys):
    mig_dir = tmp_path / "migrations"
    _write_migration(mig_dir, "0001_init.sql", "CREATE TABLE m_cli_m (id INTEGER PRIMARY KEY)")
    main(["migrate", str(mig_dir), "--url", db_url])
    capsys.readouterr()
    (mig_dir / "0001_init.sql").unlink()

    rc = main(["migrate", str(mig_dir), "--url", db_url])

    assert rc == 1
    assert "inconsistent" in capsys.readouterr().err.lower()


def test_cli_missing_directory_returns_two(tmp_path, db_url, capsys):
    """Pointing at a directory that doesn't exist is a usage error
    (exit 2), not a runtime error."""
    rc = main(["migrate", str(tmp_path / "no-such-dir"), "--url", db_url])

    assert rc == 2
    assert "not found" in capsys.readouterr().err.lower()
