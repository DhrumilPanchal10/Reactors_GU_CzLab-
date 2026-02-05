# db.py
import os
import sqlite3
from typing import Optional

DB_PATH = os.getenv("REACTORS_DB_SQLITE", "data/stage2.sqlite")


def _connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def ensure_db():
    with _connect() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS experiments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                reactor TEXT NOT NULL,
                started_at_utc TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment_id INTEGER NOT NULL,
                ts_utc TEXT NOT NULL,
                nodeid TEXT NOT NULL,
                tag TEXT NOT NULL,
                value REAL,
                FOREIGN KEY (experiment_id) REFERENCES experiments(id)
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_samples_exp_ts ON samples(experiment_id, ts_utc)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_samples_tag ON samples(tag)")
        con.commit()


def create_experiment(name: str, reactor: str, started_at_utc: str) -> int:
    with _connect() as con:
        cur = con.execute(
            "INSERT INTO experiments (name, reactor, started_at_utc) VALUES (?, ?, ?)",
            (name, reactor, started_at_utc),
        )
        con.commit()
        return int(cur.lastrowid)


def insert_sample(experiment_id: int, ts_utc: str, nodeid: str, tag: str, value: Optional[float]):
    with _connect() as con:
        con.execute(
            "INSERT INTO samples (experiment_id, ts_utc, nodeid, tag, value) VALUES (?, ?, ?, ?, ?)",
            (experiment_id, ts_utc, nodeid, tag, value),
        )
        con.commit()