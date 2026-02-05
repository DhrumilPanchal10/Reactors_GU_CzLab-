# db_pg.py
"""
Postgres-backed DB helper with SQLite fallback.
Functions used by sampler.py and app.py:
- ensure_db()
- create_experiment(name, reactor, started_at_utc) -> id
- insert_sample(experiment_id, ts_iso, nodeid, tag, value)
- insert_calibration(record dict) -> id
- list_experiments() -> list of dicts
- list_tags(experiment_id) -> list of tags
- load_timeseries(experiment_id, tags, minutes) -> pandas.DataFrame
- list_calibrations(reactor=None, sensor=None) -> list of dicts
"""

import os
import pwd
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

try:
    import psycopg
    HAS_PSYCOPG = True
except Exception:
    HAS_PSYCOPG = False

# default sqlite path (used as fallback)
SQLITE_PATH = os.environ.get("STAGE2_SQLITE", "data/stage2.sqlite")

# Postgres connection helper — follow Alexis example
def get_pg_conn():
    if not HAS_PSYCOPG:
        raise RuntimeError("psycopg not installed")
    username = pwd.getpwuid(os.getuid())[0]
    dbname = os.environ.get("BIO_DBNAME", "bioreactor_db")
    return psycopg.connect(dbname=dbname, user=username)


# ---------- convenience wrappers (try Postgres, else fallback to sqlite) ----------
def ensure_db():
    """
    Ensure database tables exist. Try Postgres first; if it fails, ensure SQLite tables using db.py
    """
    if HAS_PSYCOPG:
        try:
            with get_pg_conn() as conn:
                cur = conn.cursor()
                # experiments
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS experiments (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        reactor TEXT,
                        started_at_utc TIMESTAMPTZ
                    )
                    """
                )
                # samples
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS samples (
                        id BIGSERIAL PRIMARY KEY,
                        experiment_id INTEGER,
                        ts_utc TIMESTAMPTZ,
                        nodeid TEXT,
                        tag TEXT,
                        value DOUBLE PRECISION
                    )
                    """
                )
                # calibrations
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS calibrations (
                        id BIGSERIAL PRIMARY KEY,
                        ts_utc TIMESTAMPTZ,
                        reactor TEXT,
                        sensor TEXT,
                        cp TEXT,                  -- 'cp1' or 'cp2'
                        point DOUBLE PRECISION,
                        value DOUBLE PRECISION,
                        status TEXT,
                        quality DOUBLE PRECISION,
                        returned_value DOUBLE PRECISION,
                        method_nodeid TEXT
                    )
                    """
                )
                conn.commit()
                return "postgres"
        except Exception as e:
            # fallback to sqlite
            print(f"[db_pg] Postgres unavailable, falling back to sqlite: {e}")
    # sqlite fallback using local schema (compatible)
    _ensure_sqlite()
    return "sqlite"


def _ensure_sqlite():
    os.makedirs(os.path.dirname(SQLITE_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(SQLITE_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS experiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            reactor TEXT,
            started_at_utc TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id INTEGER,
            ts_utc TEXT,
            nodeid TEXT,
            tag TEXT,
            value REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT,
            reactor TEXT,
            sensor TEXT,
            cp TEXT,
            point REAL,
            value REAL,
            status TEXT,
            quality REAL,
            returned_value REAL,
            method_nodeid TEXT
        )
        """
    )
    con.commit()
    con.close()


# ---------- experiment/sample helpers ----------
def create_experiment(name: str, reactor: str, started_at_utc: str) -> int:
    """Create experiment record and return id. Try Postgres first, fallback to sqlite."""
    if HAS_PSYCOPG:
        try:
            with get_pg_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO experiments (name, reactor, started_at_utc) VALUES (%s,%s,%s) RETURNING id",
                    (name, reactor, started_at_utc),
                )
                eid = cur.fetchone()[0]
                conn.commit()
                return int(eid)
        except Exception:
            pass

    con = sqlite3.connect(SQLITE_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO experiments (name, reactor, started_at_utc) VALUES (?, ?, ?)",
        (name, reactor, started_at_utc),
    )
    eid = cur.lastrowid
    con.commit()
    con.close()
    return int(eid)


def insert_sample(experiment_id: int, ts_iso: str, nodeid: str, tag: str, value: float):
    if HAS_PSYCOPG:
        try:
            with get_pg_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO samples (experiment_id, ts_utc, nodeid, tag, value) VALUES (%s,%s,%s,%s,%s)",
                    (experiment_id, ts_iso, nodeid, tag, value),
                )
                conn.commit()
                return
        except Exception:
            pass

    con = sqlite3.connect(SQLITE_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO samples (experiment_id, ts_utc, nodeid, tag, value) VALUES (?, ?, ?, ?, ?)",
        (experiment_id, ts_iso, nodeid, tag, value),
    )
    con.commit()
    con.close()


# ---------- calibration helpers ----------
def insert_calibration(
    ts_iso: str,
    reactor: str,
    sensor: str,
    cp: str,
    point: float,
    value: float,
    status: str,
    quality: float,
    returned_value: float,
    method_nodeid: Optional[str],
):
    if HAS_PSYCOPG:
        try:
            with get_pg_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO calibrations
                      (ts_utc, reactor, sensor, cp, point, value, status, quality, returned_value, method_nodeid)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
                    """,
                    (ts_iso, reactor, sensor, cp, point, value, status, quality, returned_value, method_nodeid),
                )
                cid = cur.fetchone()[0]
                conn.commit()
                return int(cid)
        except Exception:
            pass

    con = sqlite3.connect(SQLITE_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO calibrations
         (ts_utc, reactor, sensor, cp, point, value, status, quality, returned_value, method_nodeid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ts_iso, reactor, sensor, cp, point, value, status, quality, returned_value, method_nodeid),
    )
    cid = cur.lastrowid
    con.commit()
    con.close()
    return int(cid)


def list_experiments() -> List[Dict[str, Any]]:
    if HAS_PSYCOPG:
        try:
            with get_pg_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, name, reactor, started_at_utc FROM experiments ORDER BY id DESC")
                rows = cur.fetchall()
                return [{"id": r[0], "name": r[1], "reactor": r[2], "started_at_utc": r[3].isoformat() if getattr(r[3], "isoformat", None) else r[3]} for r in rows]
        except Exception:
            pass

    con = sqlite3.connect(SQLITE_PATH)
    cur = con.cursor()
    rows = cur.execute("SELECT id, name, reactor, started_at_utc FROM experiments ORDER BY id DESC").fetchall()
    con.close()
    return [{"id": r[0], "name": r[1], "reactor": r[2], "started_at_utc": r[3]} for r in rows]


def list_tags(experiment_id: int) -> List[str]:
    if HAS_PSYCOPG:
        try:
            with get_pg_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT DISTINCT tag FROM samples WHERE experiment_id = %s ORDER BY tag", (experiment_id,))
                return [r[0] for r in cur.fetchall()]
        except Exception:
            pass

    con = sqlite3.connect(SQLITE_PATH)
    rows = con.execute("SELECT DISTINCT tag FROM samples WHERE experiment_id = ? ORDER BY tag", (experiment_id,)).fetchall()
    con.close()
    return [r[0] for r in rows]


def load_timeseries(experiment_id: int, tags: List[str], minutes: int):
    import pandas as pd

    if not tags:
        return pd.DataFrame(columns=["ts_utc", "tag", "value"])

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=int(minutes))
    placeholders = ",".join(["%s"] * len(tags)) if HAS_PSYCOPG else ",".join(["?"] * len(tags))
    params = [experiment_id, cutoff.isoformat()] + tags

    if HAS_PSYCOPG:
        query = f"""
            SELECT ts_utc, tag, value FROM samples
            WHERE experiment_id = %s AND ts_utc >= %s AND tag IN ({placeholders})
            ORDER BY ts_utc ASC
        """
        try:
            with get_pg_conn() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
                return df
        except Exception:
            pass

    # sqlite fallback
    query = f"""
        SELECT ts_utc, tag, value FROM samples
        WHERE experiment_id = ? AND ts_utc >= ? AND tag IN ({placeholders})
        ORDER BY ts_utc ASC
    """
    con = sqlite3.connect(SQLITE_PATH)
    df = pd.read_sql_query(query, con, params=params)
    con.close()
    if df.empty:
        return df
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts_utc"])
    return df


def list_calibrations(reactor: Optional[str] = None, sensor: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    if HAS_PSYCOPG:
        try:
            with get_pg_conn() as conn:
                cur = conn.cursor()
                q = "SELECT id, ts_utc, reactor, sensor, cp, point, value, status, quality, returned_value, method_nodeid FROM calibrations"
                conds = []
                params = []
                if reactor:
                    conds.append("reactor = %s"); params.append(reactor)
                if sensor:
                    conds.append("sensor = %s"); params.append(sensor)
                if conds:
                    q += " WHERE " + " AND ".join(conds)
                q += " ORDER BY ts_utc DESC LIMIT %s"
                params.append(limit)
                cur.execute(q, tuple(params))
                rows = cur.fetchall()
                out = []
                for r in rows:
                    out.append({
                        "id": r[0],
                        "ts_utc": r[1].isoformat() if getattr(r[1], "isoformat", None) else r[1],
                        "reactor": r[2],
                        "sensor": r[3],
                        "cp": r[4],
                        "point": r[5],
                        "value": r[6],
                        "status": r[7],
                        "quality": r[8],
                        "returned_value": r[9],
                        "method_nodeid": r[10],
                    })
                return out
        except Exception:
            pass

    con = sqlite3.connect(SQLITE_PATH)
    cur = con.cursor()
    q = "SELECT id, ts_utc, reactor, sensor, cp, point, value, status, quality, returned_value, method_nodeid FROM calibrations"
    conds = []
    params = []
    if reactor:
        conds.append("reactor = ?"); params.append(reactor)
    if sensor:
        conds.append("sensor = ?"); params.append(sensor)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY ts_utc DESC LIMIT ?"
    params.append(limit)
    rows = cur.execute(q, params).fetchall()
    con.close()
    out = []
    for r in rows:
        out.append({
            "id": r[0],
            "ts_utc": r[1],
            "reactor": r[2],
            "sensor": r[3],
            "cp": r[4],
            "point": r[5],
            "value": r[6],
            "status": r[7],
            "quality": r[8],
            "returned_value": r[9],
            "method_nodeid": r[10],
        })
    return out