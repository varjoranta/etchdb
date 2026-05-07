"""asyncpg adapter for etchdb.

Importing this subpackage requires asyncpg to be installed. The
top-level `etchdb` namespace does NOT depend on asyncpg; only this
subpackage does.
"""

try:
    import asyncpg as _asyncpg  # noqa: F401
except ImportError as e:
    raise ImportError(
        "etchdb.asyncpg requires the asyncpg package. Install with: pip install etchdb[asyncpg]"
    ) from e

from etchdb.asyncpg.adapter import AsyncpgAdapter

__all__ = ["AsyncpgAdapter"]
