"""Allow `python -m etchdb` as an alias for the installed `etchdb` script."""

from etchdb.cli import main

raise SystemExit(main())
