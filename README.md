# etchdb

Minimal async DB layer for Python. Typed CRUD over Pydantic. Raw SQL when you need it.

## Status

Alpha. v0.3.0 on PyPI. Built in public from day one; expect tightening between alpha releases.

## Example

```python
from etchdb import DB, Row

class User(Row):
    __table__ = "users"
    id: int | None = None             # leave unset and the DB allocates it (SERIAL / INTEGER PK)
    name: str
    email: str | None = None

# Connect (driver inferred from URL scheme)
#   postgresql+asyncpg://...   asyncpg + Postgres
#   postgresql+psycopg://...   psycopg3 + Postgres
#   sqlite+aiosqlite:///...    aiosqlite + SQLite
db = await DB.from_url("postgresql+asyncpg://user@host/db")

# Typed CRUD
alice = await db.insert(User(name="Alice"))           # alice.id is now populated by the DB
user = await db.get(User, id=alice.id)                # one row or None
users = await db.query(User)                          # list of rows
no_email = await db.query(User, email=None)           # IS NULL, not = NULL
recent = await db.query(User, id=[1, 5, 7])           # IN ($1, $2, $3)
await db.update(User(id=alice.id, name="Alice B"))    # partial: email is preserved
await db.delete(alice)

# Add `where=` to AND extra equality filters onto the PK. Atomic, so
# the scope check runs in the same statement as the update.
await db.update(User(id=alice.id, name="Alice B"),
                where={"email": "alice@example.com"})

# Partial update without making your model lie about the schema:
# patch() builds a Row with only the given fields set; no validation,
# so models with required NOT NULL columns still flow through.
await db.update(User.patch(id=alice.id, name="Alice B"))

# Atomic column expressions in SET (use Row.patch so the sentinels
# slip past validation). Counter / Article are user-defined Row
# subclasses alongside User, with an int counter and a timestamp:
from etchdb import Inc, Now
await db.update(Counter.patch(id=1, n=Inc()))           # n = n + 1
await db.update(Article.patch(id=1, updated_at=Now()))  # = CURRENT_TIMESTAMP

# Bulk insert / delete (chunked at the driver's parameter limit).
await db.insert_many([User(name=n) for n in names], on_conflict="ignore")
await db.insert_many(rows, on_conflict="upsert")    # ON CONFLICT (pk) DO UPDATE SET ...
await db.delete_many(User, [1, 2, 3])

# Upsert via single insert: same rules, returns the DB's view.
alice = await db.insert(User(id=1, name="Alice", email="a@x"),
                        on_conflict="upsert")

# Stream every matching row (paginated, won't load the whole table at once)
# Uses offset pagination, so cost scales O(N**2) on huge tables; for those,
# loop with a raw keyset query (WHERE id > last_id ORDER BY id LIMIT n).
async for user in db.iter_rows(User, batch_size=500):
    process(user)

# Typed-result raw SQL (covers most joins)
users = await db.fetch_models(User, """
    SELECT u.* FROM users u JOIN orders o ON o.user_id = u.id
    WHERE o.created_at > $1
""", since)
top = await db.fetch_model(User, "SELECT * FROM users ORDER BY id LIMIT 1")  # Row | None

# Untyped raw SQL (mirrors asyncpg)
rows = await db.fetch("SELECT count(*) FROM events WHERE site_id = $1", site_id)
val = await db.fetchval("SELECT count(*) FROM users")  # always returns the count;
                                                       # for non-aggregate selects,
                                                       # fetchval returns None on no row.
await db.execute("UPDATE users SET active = false WHERE id = $1", uid)

# Transactions
async with db.transaction() as tx:
    await tx.insert(User(name="Carol"))
    await tx.execute("INSERT INTO audit_log (...) VALUES (...)")

# Inspect SQL before executing (etchdb's defining feature)
q = db.compose("get", User, id=1)
print(q.sql)     # SELECT id, name, email FROM users WHERE id = $1
print(q.params)  # [1]

# Same inspector without a live DB - useful in tests:
from etchdb import sql
q = sql.compose("get", User, id=1, placeholder=lambda i: f"${i + 1}")
```

`insert` only emits the columns you actually set, so an unset `id` lets the database allocate one (SERIAL or INTEGER PRIMARY KEY); the returned Row reflects the DB's view (`RETURNING *`), so server-defaults like `id` and `created_at` are populated in place. `update` does the same: a column you didn't touch keeps its current value rather than being clobbered. An explicit `None` counts as set in both cases.

`update` and `delete` use the value of every column in `__pk__` (default: `("id",)`) as the WHERE clause; override the class attribute for a composite or differently-named primary key. Add extra equality filters with `where={...}` for guarded updates: multi-tenant scoping like `where={"user_id": current_user}` is the canonical use case. Raw SQL is still the right tool when you need anything richer than equality.

Use `Row.patch(**fields)` to build a partial Row that satisfies neither validation nor missing-required-field checks. It's the right shape when you want partial updates against a model with NOT NULL columns.

## Typed errors

Driver exceptions are mapped onto a small etchdb family so application code catches the same type regardless of the backend. The original driver exception is preserved as `__cause__`.

```python
from etchdb import (
    EtchdbError,            # base for everything etchdb raises
    IntegrityError,         # unique / FK / NOT NULL / check violation
    UndefinedTableError,    # table referenced by a query does not exist
    UndefinedColumnError,   # column referenced by a query does not exist
    OperationalError,       # connection-level / driver-level failure
)

try:
    await db.insert(User(id=1, name="Alice"))
except IntegrityError as e:
    log.warning("duplicate or constraint violation: %s", e)
    # e.__cause__ is the original asyncpg / psycopg / sqlite3 exception
```

`except EtchdbError` catches every member of the family at once.

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

`from_url` registers a JSONB codec on every connection so `dict` and `list` parameters round-trip with `UUID`, `datetime`, `Enum`, and Pydantic `BaseModel` values handled transparently. JSONB columns come back as Python objects directly: no `json.loads` at the call site, no manual `json.dumps` to insert. For pool-init concerns beyond that (pgvector tuning, custom ENUM codec, custom `min_size` / `max_size`), construct the pool yourself and pass it via `from_pool`. Example, registering a Postgres ENUM as a Python `str` via asyncpg's `set_type_codec`:

```python
import asyncpg
from etchdb import DB
from etchdb.asyncpg import AsyncpgAdapter

async def init_conn(conn):
    await conn.set_type_codec(
        "memory_domain",          # the ENUM type name
        encoder=str, decoder=str,
        schema="public",
        format="text",
    )

pool = await asyncpg.create_pool(url, init=init_conn, min_size=2, max_size=10)
db = DB(AsyncpgAdapter.from_pool(pool))
```

Both Postgres adapters take libpq-native `$1, $2, ...` placeholders in raw SQL. The psycopg adapter uses `AsyncRawCursor` so the `$N` form works there too; psycopg's default `%s` form is not used and will produce a Postgres syntax error.

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

Out of scope so far. A small forward-only, file-based migration helper (no autogenerate, no rollback, no DAG) is planned for a later release. etchdb owns no schema state today, so any external tool slots in fine in the meantime: Alembic if you also use SQLAlchemy, dbmate or sqitch if you don't, or a few `db.execute` calls in your bootstrap path.

## Under consideration

Comparison sentinels for `where=` filters (`Gt`, `Gte`, `Lt`, `Lte`, accepting either a scalar or the existing `Now()` sentinel) are on the table for a later release. Scope would stay narrow: single-column inequality only; compound predicates, `LIKE`, and `BETWEEN` would still go through raw SQL. Holding the API for more real use cases before committing. Until then, raw SQL via `db.fetch_models(User, "SELECT * FROM users WHERE expires_at > NOW()")` is the canonical answer; that's what etchdb's first-class raw-SQL escape valve is for.

`Inc` / `Now` composing with `on_conflict="upsert"` is a related deferred shape: the create-or-increment pattern would let `db.insert(RateCounter(...), on_conflict="upsert")` carry an `Inc(by=N)` for the conflict SET, but the INSERT branch still needs a literal initial value, so the row would have to express two values per column. Today the sentinels are rejected by `db.insert` / `db.insert_many` outright; raw SQL with `INSERT ... ON CONFLICT (key) DO UPDATE SET count = table.count + 1` covers the case in one statement.

## Built with AI assistance

Built with a mix of AI tools for writing and reviewing code: primarily Claude Code, with Codex and occasionally Gemini for second opinions, and not limited to these. spegl.ing pattern research is in active use alongside them. Design, code, and commits are reviewed and shipped by Hannu Varjoranta. Building in public, openly using AI tooling, is part of the project's premise.

## License

MIT.
