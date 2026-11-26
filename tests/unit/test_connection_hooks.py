"""Connection setup preserves codecs and native pool options without a server."""

from unittest.mock import AsyncMock

import pytest

from etchdb import DB
from etchdb.asyncpg import adapter as asyncpg_adapter
from etchdb.psycopg import adapter as psycopg_adapter


@pytest.mark.parametrize("module", [asyncpg_adapter, psycopg_adapter])
@pytest.mark.parametrize("custom", [False, True, "failure"])
async def test_connection_setup_composes_and_propagates_errors(monkeypatch, module, custom):
    events = []
    conn = object()

    async def codecs(connection):
        assert connection is conn
        events.append("codecs")

    async def configure(connection):
        assert connection is conn
        events.append("custom")
        if custom == "failure":
            raise RuntimeError("setup failed")

    monkeypatch.setattr(module, "configure_codecs", codecs)
    pool = AsyncMock()
    captured = {}

    if module is asyncpg_adapter:

        async def create_pool(url, **kwargs):
            captured.update(kwargs)
            return pool

        monkeypatch.setattr(module.asyncpg, "create_pool", create_pool)
        adapter_type = module.AsyncpgAdapter
        options = {"command_timeout": 12.0}
        callback_name = "init"
    else:

        def create_pool(url, **kwargs):
            captured.update(kwargs)
            return pool

        monkeypatch.setattr(module, "AsyncConnectionPool", create_pool)
        adapter_type = module.PsycopgAdapter
        options = {"check": AsyncMock()}
        callback_name = "configure"

    adapter = await adapter_type.from_url(
        "postgresql://unused",
        min_size=1,
        max_size=2,
        configure=configure if custom else None,
        **options,
    )
    assert captured["min_size"] == 1
    assert captured["max_size"] == 2
    for key, value in options.items():
        assert captured[key] == value
    if custom == "failure":
        with pytest.raises(RuntimeError, match="setup failed"):
            await captured[callback_name](conn)
    else:
        await captured[callback_name](conn)
    assert events == (["codecs", "custom"] if custom else ["codecs"])
    await adapter.close()
    pool.close.assert_awaited_once()


@pytest.mark.parametrize("driver", ["asyncpg", "psycopg"])
async def test_facade_forwards_configure(monkeypatch, driver):
    adapter_type = (
        asyncpg_adapter.AsyncpgAdapter if driver == "asyncpg" else psycopg_adapter.PsycopgAdapter
    )
    configure = AsyncMock()
    create = AsyncMock(return_value=object())
    monkeypatch.setattr(adapter_type, "from_url", create)
    await DB.from_url(f"postgresql+{driver}://unused", configure=configure)
    assert create.call_args.kwargs["configure"] is configure
    assert create.call_args.args == ("postgresql://unused",)


async def test_sqlite_rejects_postgres_configure():
    with pytest.raises(ValueError, match="only supported by the Postgres"):
        await DB.from_url("sqlite:///:memory:", configure=AsyncMock())
