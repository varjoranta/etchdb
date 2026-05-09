"""Test URL-scheme dispatch and lazy driver-import behavior on DB.from_url.

These tests don't open a real connection; they exercise the dispatch
logic and error messages.
"""

import pytest

from etchdb import DB


async def test_from_url_unsupported_scheme_raises():
    with pytest.raises(ValueError, match="Unsupported URL scheme"):
        await DB.from_url("mysql://x@y/z")


async def test_from_url_psycopg_dispatches_to_adapter(monkeypatch):
    """Confirm the psycopg URL routes to PsycopgAdapter.from_url with the
    +psycopg prefix stripped, without opening a real connection."""
    from etchdb.psycopg import PsycopgAdapter

    captured: dict[str, object] = {}

    async def fake_from_url(cls, url: str, **kwargs: object) -> PsycopgAdapter:
        captured["url"] = url
        captured.update(kwargs)
        return cls.__new__(cls)

    monkeypatch.setattr(PsycopgAdapter, "from_url", classmethod(fake_from_url))

    await DB.from_url("postgresql+psycopg://user:pw@host:5432/dbname")
    assert captured["url"] == "postgresql://user:pw@host:5432/dbname"


async def test_from_url_pool_kwargs_forwarded(monkeypatch):
    """min_size / max_size pass through to the underlying adapter
    so callers can tune the Postgres pool without dropping to
    `from_pool`."""
    from etchdb.psycopg import PsycopgAdapter

    captured: dict[str, object] = {}

    async def fake_from_url(cls, url: str, **kwargs: object) -> PsycopgAdapter:
        captured.update(kwargs)
        return cls.__new__(cls)

    monkeypatch.setattr(PsycopgAdapter, "from_url", classmethod(fake_from_url))

    await DB.from_url("postgresql+psycopg://x@y/z", min_size=2, max_size=10)
    assert captured["min_size"] == 2
    assert captured["max_size"] == 10


async def test_from_url_pool_kwargs_default_to_none(monkeypatch):
    """Without explicit values, the kwargs are None so each adapter
    falls back to its driver's default pool sizing."""
    from etchdb.psycopg import PsycopgAdapter

    captured: dict[str, object] = {}

    async def fake_from_url(cls, url: str, **kwargs: object) -> PsycopgAdapter:
        captured.update(kwargs)
        return cls.__new__(cls)

    monkeypatch.setattr(PsycopgAdapter, "from_url", classmethod(fake_from_url))

    await DB.from_url("postgresql+psycopg://x@y/z")
    assert captured["min_size"] is None
    assert captured["max_size"] is None


@pytest.mark.parametrize("url", ["sqlite:///:memory:", "sqlite+aiosqlite:///:memory:"])
async def test_from_url_sqlite_schemes_work(url):
    db = await DB.from_url(url)
    try:
        val = await db.fetchval("SELECT 1")
        assert val == 1
    finally:
        await db.close()
