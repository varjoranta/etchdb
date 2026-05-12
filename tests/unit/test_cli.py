"""Unit tests for the CLI entry point.

DB-touching paths are covered in tests/integration/test_cli.py.
These tests cover argument parsing, status formatting, and the
env-var / exit-code surface that doesn't need a live database."""

from pathlib import Path

import pytest

from etchdb.cli import _build_parser, _format_status, main
from etchdb.migrations import MigrationStatus


def test_parser_requires_subcommand():
    """`etchdb` with no args should error rather than do something
    surprising."""
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_parser_accepts_migrate_with_directory():
    parser = _build_parser()
    args = parser.parse_args(["migrate", "./migrations"])
    assert args.cmd == "migrate"
    assert args.directory == Path("./migrations")
    assert args.url is None


def test_parser_accepts_status_with_url():
    parser = _build_parser()
    args = parser.parse_args(["status", "./migrations", "--url", "sqlite:///:memory:"])
    assert args.cmd == "status"
    assert args.url == "sqlite:///:memory:"


def test_main_returns_2_when_no_url(monkeypatch, capsys):
    """Missing both --url and DATABASE_URL is a usage error (exit 2),
    not a runtime error (exit 1)."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    rc = main(["migrate", "./does-not-matter"])
    assert rc == 2
    assert "database URL required" in capsys.readouterr().err


def test_format_status_consistent_with_pending_and_applied(tmp_path):
    s = MigrationStatus(
        pending=["0003_x.sql"],
        applied=["0001_a.sql", "0002_b.sql"],
    )
    out = _format_status(s, tmp_path)
    assert "INCONSISTENT" not in out
    assert "applied (2)" in out
    assert "0001_a.sql" in out
    assert "pending (1)" in out
    assert "0003_x.sql" in out


def test_format_status_inconsistent_shows_drift_and_missing(tmp_path):
    s = MigrationStatus(
        applied=["0001_a.sql"],
        drifted=["0002_b.sql"],
        missing=["0003_c.sql"],
    )
    out = _format_status(s, tmp_path)
    assert "INCONSISTENT" in out
    assert "drifted (1)" in out
    assert "0002_b.sql" in out
    assert "missing (1)" in out
    assert "0003_c.sql" in out


def test_format_status_empty_directory(tmp_path):
    s = MigrationStatus()
    out = _format_status(s, tmp_path)
    assert "(no migrations found)" in out
