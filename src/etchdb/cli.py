"""etchdb command-line entry point.

Two subcommands:

    etchdb migrate ./migrations --url $DATABASE_URL
    etchdb status  ./migrations --url $DATABASE_URL

`--url` falls back to the `DATABASE_URL` env var if not given. Exit
codes: 0 on success, 1 on inconsistent migration state (drift or
disappearance), 2 on usage error (missing args, missing directory,
no database URL).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from etchdb import DB

if TYPE_CHECKING:
    from etchdb.migrations import MigrationStatus


def _format_status(status: MigrationStatus, directory: Path) -> str:
    lines: list[str] = [f"Migration status ({directory}):"]
    if not status.is_consistent:
        lines.append("  ! INCONSISTENT -- fix before applying")
    for label, names in (
        ("drifted", status.drifted),
        ("missing", status.missing),
        ("applied", status.applied),
        ("pending", status.pending),
    ):
        if names:
            lines.append(f"  {label} ({len(names)}):")
            lines.extend(f"    {fn}" for fn in names)
    if not any((status.applied, status.pending, status.drifted, status.missing)):
        lines.append("  (no migrations found)")
    return "\n".join(lines)


async def _run_migrate(url: str, directory: Path) -> int:
    db = await DB.from_url(url)
    try:
        applied = await db.migrate(directory)
        print(f"applied {applied} migration{'s' if applied != 1 else ''}")
        return 0
    finally:
        await db.close()


async def _run_status(url: str, directory: Path) -> int:
    db = await DB.from_url(url)
    try:
        status = await db.migration_status(directory)
        print(_format_status(status, directory))
        return 0 if status.is_consistent else 1
    finally:
        await db.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etchdb", description="etchdb command-line tools")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd_name, help_text in (
        ("migrate", "apply pending migrations"),
        ("status", "show migration state without applying"),
    ):
        p = sub.add_parser(cmd_name, help=help_text)
        p.add_argument(
            "directory",
            type=Path,
            help="migrations directory (e.g. ./migrations)",
        )
        p.add_argument(
            "--url",
            help="database URL (falls back to DATABASE_URL env var)",
        )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    url = args.url or os.environ.get("DATABASE_URL")
    if not url:
        print(
            "error: database URL required (pass --url or set DATABASE_URL)",
            file=sys.stderr,
        )
        return 2

    try:
        if args.cmd == "migrate":
            return asyncio.run(_run_migrate(url, args.directory))
        return asyncio.run(_run_status(url, args.directory))
    except RuntimeError as e:
        # Migration inconsistency (drift / disappearance) from db.migrate.
        print(f"error: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        # Missing directory, malformed migration file, etc.
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
