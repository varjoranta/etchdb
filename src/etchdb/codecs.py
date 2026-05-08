"""JSONB serialisation helpers used by the Postgres adapters' `from_url`.

The default `json.dumps` raises on `UUID`, `datetime`, `date`, `Enum`,
and Pydantic v2 `BaseModel`. `json_dumps` here delegates to
`pydantic_core.to_json`, which handles those plus `bytes`, `Decimal`,
IP addresses, and more in C-implemented code.

Wired into `AsyncpgAdapter.from_url` via asyncpg's `init=` callback,
and into `PsycopgAdapter.from_url` via the pool's `configure=` callback.
The `from_pool` paths intentionally don't run this; users with their
own pool own their codec setup. They can import `json_dumps` here and
register it the same way if they want the same default behaviour.
"""

from __future__ import annotations

from typing import Any

from pydantic_core import to_json


def json_dumps(obj: Any) -> str:
    """Serialize `obj` for a JSONB column. Handles UUID, datetime, Enum,
    Pydantic `BaseModel`, `bytes`, `Decimal`, IP addresses, and other
    types `pydantic_core` knows about. Raises
    `pydantic_core.PydanticSerializationError` for unknown types."""
    return to_json(obj).decode()
