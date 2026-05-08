"""Fixtures for integration tests across SQLite (always) and Postgres (Docker).

The `sqlite_db` fixture runs against an in-memory aiosqlite database and
is always available. The `postgres_asyncpg_db` and `postgres_psycopg_db`
fixtures skip when Docker isn't running on localhost:5532. The
parametrized `db` fixture runs each test against every backend in turn;
per-param skipping means SQLite tests still run when Postgres is
unavailable.
"""

import os
import socket

import pytest
import pytest_asyncio

from etchdb import DB

POSTGRES_DSN = os.environ.get(
    "ETCHDB_TEST_POSTGRES_DSN",
    "etchdb:etchdb-test-password@localhost:5532/etchdb_test",
)
POSTGRES_ASYNCPG_URL = f"postgresql+asyncpg://{POSTGRES_DSN}"
POSTGRES_PSYCOPG_URL = f"postgresql+psycopg://{POSTGRES_DSN}"


def _postgres_available() -> bool:
    try:
        with socket.create_connection(("localhost", 5532), timeout=0.5):
            return True
    except OSError:
        return False


def _schema(pk_type: str) -> tuple[str, ...]:
    return (
        f"CREATE TABLE users (id {pk_type}, name TEXT NOT NULL, email TEXT)",
        (
            "CREATE TABLE user_roles ("
            "user_id INTEGER NOT NULL, role_id INTEGER NOT NULL, note TEXT,"
            "PRIMARY KEY (user_id, role_id))"
        ),
    )


SQLITE_SCHEMA = _schema("INTEGER PRIMARY KEY")
POSTGRES_SCHEMA = _schema("SERIAL PRIMARY KEY")


@pytest_asyncio.fixture
async def sqlite_db():
    db = await DB.from_url("sqlite+aiosqlite:///:memory:")
    for stmt in SQLITE_SCHEMA:
        await db.execute(stmt)
    yield db
    await db.close()


async def _open_postgres(url: str) -> DB:
    if not _postgres_available():
        pytest.skip("Postgres not available on localhost:5532. Run `make db-up`.")
    db = await DB.from_url(url)
    await db.execute("DROP TABLE IF EXISTS users CASCADE")
    await db.execute("DROP TABLE IF EXISTS user_roles CASCADE")
    for stmt in POSTGRES_SCHEMA:
        await db.execute(stmt)
    return db


@pytest_asyncio.fixture
async def postgres_asyncpg_db():
    db = await _open_postgres(POSTGRES_ASYNCPG_URL)
    yield db
    await db.close()


@pytest_asyncio.fixture
async def postgres_psycopg_db():
    db = await _open_postgres(POSTGRES_PSYCOPG_URL)
    yield db
    await db.close()


@pytest.fixture(params=["sqlite_db", "postgres_asyncpg_db", "postgres_psycopg_db"])
def db(request):
    """Parametrized fixture that yields each backend in turn.

    Lazy resolution via `getfixturevalue` means the postgres skips
    trigger per-param, not for the whole test.
    """
    return request.getfixturevalue(request.param)
