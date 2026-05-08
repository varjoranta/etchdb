"""psycopg adapter for etchdb.

Importing this subpackage requires both the psycopg driver and its
pool package: install with `pip install etchdb[psycopg]`. The
top-level `etchdb` namespace does NOT depend on either; only this
subpackage does.
"""

try:
    import psycopg as _psycopg  # noqa: F401
    import psycopg_pool as _psycopg_pool  # noqa: F401
except ImportError as e:
    raise ImportError(
        "etchdb.psycopg requires the psycopg and psycopg-pool packages. "
        "Install with: pip install etchdb[psycopg]"
    ) from e

from etchdb.psycopg.adapter import PsycopgAdapter

__all__ = ["PsycopgAdapter"]
