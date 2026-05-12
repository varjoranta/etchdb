"""Allow `python -m etchdb` as an alias for the installed `etchdb` script."""

import sys

from etchdb.cli import main

sys.exit(main())
