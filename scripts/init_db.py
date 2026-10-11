"""Create the SQLite events bus (``data/events.sqlite`` by default).

Every watcher creates the schema on its own when it opens the database, so this is only a
convenience to verify the location and permissions up front.

    python -m scripts.init_db
    LATENCY_LAB_DB=/tmp/events.sqlite python -m scripts.init_db
"""
import sys
from pathlib import Path

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from watchers._db import DB_PATH, connect  # noqa: E402


def main():
    con = connect(DB_PATH)
    con.close()
    print(f"DB ready at {DB_PATH}")


if __name__ == "__main__":
    main()
