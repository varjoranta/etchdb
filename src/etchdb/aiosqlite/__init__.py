"""aiosqlite adapter for etchdb.

Importing this subpackage requires aiosqlite to be installed. The
top-level `etchdb` namespace does NOT depend on aiosqlite; only this
subpackage does.
"""

try:
    import aiosqlite as _aiosqlite  # noqa: F401
except ImportError as e:
    raise ImportError(
        "etchdb.aiosqlite requires the aiosqlite package. Install with: pip install etchdb[sqlite]"
    ) from e

from etchdb.aiosqlite.adapter import AiosqliteAdapter

__all__ = ["AiosqliteAdapter"]
