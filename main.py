#!/usr/bin/env python3
"""Root entrypoint for the multi-brand migration engine.

All reusable implementation files live under helpers/.
Run examples:
  python3 main.py --brand inplay --migrate-all --date-from 2026-05-26 --date-to 2026-05-27 --dry-run
  python3 main.py --brand instaplay --migrate-all --date-from 2026-05-26 --date-to 2026-05-27 --dry-run
"""

from helpers.main import main


if __name__ == "__main__":
    main()
