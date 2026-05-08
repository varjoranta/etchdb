"""Integration tests for the default JSONB codec on `from_url`.

Postgres-only: SQLite has no jsonb. Each test creates its own `doc`
table on the relevant backend, round-trips a Python dict containing
UUID, datetime, Enum, and a Pydantic BaseModel through a JSONB column,
and asserts the typed values come back as plain Python after the
codec encodes/decodes them.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from etchdb import DB


class Status(Enum):
    OPEN = "open"
    CLOSED = "closed"


class Note(BaseModel):
    text: str
    score: int


@pytest.fixture(params=["postgres_asyncpg_db", "postgres_psycopg_db"])
def pg_db(request):
    return request.getfixturevalue(request.param)


async def _setup_doc_table(db: DB) -> None:
    await db.execute("DROP TABLE IF EXISTS doc CASCADE")
    await db.execute("CREATE TABLE doc (id SERIAL PRIMARY KEY, payload JSONB)")


async def test_jsonb_round_trip_with_typed_values(pg_db: DB):
    await _setup_doc_table(pg_db)

    doc_id = uuid4()
    payload = {
        "id": doc_id,
        "name": "alice",
        "joined": datetime(2020, 1, 1, 12, 30, 0),
        "status": Status.OPEN,
        "note": Note(text="hello", score=42),
    }

    await pg_db.execute("INSERT INTO doc (payload) VALUES ($1)", payload)
    fetched = await pg_db.fetchval("SELECT payload FROM doc")

    # JSONB came back as a Python dict, not a JSON string.
    assert isinstance(fetched, dict)
    # UUID encoded as string.
    assert fetched["id"] == str(doc_id)
    assert fetched["name"] == "alice"
    # datetime encoded as ISO string.
    assert fetched["joined"] == "2020-01-01T12:30:00"
    # Enum encoded by value.
    assert fetched["status"] == "open"
    # Pydantic BaseModel encoded as dict.
    assert fetched["note"] == {"text": "hello", "score": 42}


async def test_jsonb_list_round_trip(pg_db: DB):
    """Top-level list works the same as top-level dict."""
    await _setup_doc_table(pg_db)

    payload = [Status.OPEN, uuid4(), {"nested": True}]
    await pg_db.execute("INSERT INTO doc (payload) VALUES ($1)", payload)
    fetched = await pg_db.fetchval("SELECT payload FROM doc")

    assert isinstance(fetched, list)
    assert fetched[0] == "open"
    assert isinstance(fetched[1], str)
    UUID(fetched[1])  # parses as a UUID
    assert fetched[2] == {"nested": True}


async def test_jsonb_unsupported_type_raises_clear_error(pg_db: DB):
    """A type the encoder doesn't know about surfaces a clear error
    rather than silent coercion. The underlying error is
    `pydantic_core.PydanticSerializationError`; asyncpg wraps it in a
    `DataError` whose message contains the original; psycopg lets it
    propagate. Match on the shared substring."""
    await _setup_doc_table(pg_db)

    class Unknown:
        pass

    with pytest.raises(Exception, match="serialize unknown type"):
        await pg_db.execute("INSERT INTO doc (payload) VALUES ($1)", {"x": Unknown()})
