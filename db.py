import sqlite3
from pathlib import Path

DB_PATH = Path("data/stage2.sqlite")

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL,
    reactor TEXT NOT NULL,
    tag TEXT NOT NULL,
    nodeid TEXT NOT NULL,
    value REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts_utc);
CREATE INDEX IF NOT EXISTS idx_samples_reactor_tag ON samples(reactor, tag);
"""

def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH.as_posix(), check_same_thread=False)
    return conn

def init_db():
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()

def insert_samples(rows):
    """
    rows: list of tuples (ts_utc, reactor, tag, nodeid, value)
    """
    if not rows:
        return
    conn = get_conn()
    try:
        conn.executemany(
            "INSERT INTO samples (ts_utc, reactor, tag, nodeid, value) VALUES (?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
    finally:
        conn.close()
