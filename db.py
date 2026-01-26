# db.py
import sqlite3
from pathlib import Path
from typing import Dict, Any

DB_PATH = Path("data/stage2.sqlite")

def ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS experiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            reactor TEXT NOT NULL,
            started_at_utc TEXT NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id INTEGER NOT NULL,
            ts_utc TEXT NOT NULL,
            nodeid TEXT NOT NULL,
            tag TEXT NOT NULL,
            value REAL,
            FOREIGN KEY(experiment_id) REFERENCES experiments(id)
        );
        """)
        con.commit()

def create_experiment(name: str, reactor: str, started_at_utc: str) -> int:
    ensure_db()
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO experiments (name, reactor, started_at_utc) VALUES (?, ?, ?)",
            (name, reactor, started_at_utc),
        )
        con.commit()
        return int(cur.lastrowid)

def insert_sample(experiment_id: int, ts_utc: str, nodeid: str, tag: str, value: float):
    ensure_db()
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO samples (experiment_id, ts_utc, nodeid, tag, value) VALUES (?, ?, ?, ?, ?)",
            (experiment_id, ts_utc, nodeid, tag, float(value)),
        )
        con.commit()

def init_db():
    # Backwards-compatible alias for older code
    ensure_db()