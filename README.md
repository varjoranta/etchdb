# etchdb

Minimal async DB layer for Python. Typed CRUD over Pydantic. Raw SQL when you need it.

## Status

Pre-alpha. Not on PyPI yet. Built in public from day one.

## Example

```python
from etchdb import DB, Row

class User(Row):
    __table__ = "users"
    id: int | None = None             # leave unset and the DB allocates it (SERIAL / INTEGER PK)
    name: str
    email: str | None = None

# Connect (driver inferred from URL scheme)
db = await DB.from_url("postgresql+asyncpg://user@host/db")

# Typed CRUD
alice = await db.insert(User(name="Alice"))           # alice.id is now populated by the DB
user = await db.get(User, id=alice.id)                # one row or None
users = await db.query(User)                          # list of rows
await db.update(User(id=alice.id, name="Alice B"))    # partial: email is preserved
await db.delete(alice)

# Typed-result raw SQL (covers most joins)
users = await db.fetch_models(User, """
    SELECT u.* FROM users u JOIN orders o ON o.user_id = u.id
    WHERE o.created_at > $1
""", since)

# Untyped raw SQL (mirrors asyncpg)
rows = await db.fetch("SELECT count(*) FROM events WHERE site_id = $1", site_id)
val = await db.fetchval("SELECT count(*) FROM users")
await db.execute("UPDATE users SET active = false WHERE id = $1", uid)

# Transactions
async with db.transaction() as tx:
    await tx.insert(User(name="Carol"))
    await tx.execute("INSERT INTO audit_log (...) VALUES (...)")

# Inspect SQL before executing (etchdb's defining feature)
q = db.compose("get", User, id=1)
print(q.sql)     # SELECT id, name, email FROM users WHERE id = $1
print(q.params)  # [1]
```

`insert` only emits the columns you actually set, so an unset `id` lets the database allocate one (SERIAL or INTEGER PRIMARY KEY). `update` does the same: a column you didn't touch keeps its current value rather than being clobbered. An explicit `None` counts as set in both cases.

## Install

Drivers are optional extras. Install only what you use:

```bash
pip install etchdb[asyncpg]    # asyncpg + Postgres
pip install etchdb[psycopg]    # psycopg3 + Postgres
pip install etchdb[sqlite]     # aiosqlite + SQLite
pip install etchdb[all]        # everything
```

The top-level `etchdb` namespace depends only on Pydantic. Driver subpackages import their driver eagerly with a clear error if it is not installed.

```python
from etchdb import DB, Row                    # always safe
from etchdb.asyncpg import AsyncpgAdapter     # requires asyncpg

# Bring your own pool
db = DB(AsyncpgAdapter.from_pool(my_pool))
```

## Why

Most Python ORMs are heavy, opinionated, and leak at the seams when you reach for pgvector or PostGIS. Raw asyncpg works, but every project ends up writing the same Pydantic-bridge code. etchdb closes that gap without becoming a framework.

The design also targets AI-assisted development: predictable verbs, no metaclass magic, no implicit context vars, no lazy loading, every typed operation produces inspectable SQL. Code an LLM can write correctly on the first attempt.

## Goals

- Driver-agnostic (asyncpg or psycopg3, swap freely)
- Multi-dialect (Postgres primary, SQLite secondary, MySQL maybe)
- Async native, no sync wrappers
- Typed CRUD via Pydantic; raw SQL as first-class escape valve
- Inspectable SQL: every typed op exposes its `(sql, params)` without executing

## Non-goals

- Query builder beyond simple CRUD (use raw SQL for joins)
- Implicit relationships, lazy loading, eager loading
- Sync support
- A second canonical way to do anything

## Migrations

Out of scope for v0.1. A small forward-only, file-based migration helper (no autogenerate, no rollback, no DAG) is planned for a later release. etchdb owns no schema state today, so any external tool slots in fine in the meantime: Alembic if you also use SQLAlchemy, dbmate or sqitch if you don't, or a few `db.execute` calls in your bootstrap path.

## Built with AI assistance

Built with Claude Code as the primary development assistant. Design, code, and commits are reviewed and shipped by Hannu Varjoranta. Building in public, openly using AI tooling, is part of the project's premise.

## License

MIT.
