# db.py
"""
DB layer for Reactors HMI

Default backend: PostgreSQL (required for multi-client concurrency).
SQLite is still available ONLY if you explicitly set REACTORS_DB_BACKEND=sqlite.

Env vars:
- REACTORS_DB_BACKEND: "postgres" (default) or "sqlite"
- REACTORS_DB_SQLITE: path for sqlite (default: data/stage2.sqlite)

Postgres connection options:
- REACTORS_PG_DBNAME: default "bioreactor_db"
- REACTORS_PG_USER: default current OS username
- REACTORS_PG_HOST: optional (socket/host)
- REACTORS_PG_PORT: optional
- REACTORS_PG_PASSWORD: optional
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any, Optional, Sequence

# ----------------------------
# Backend selection
# ----------------------------
DB_BACKEND = os.getenv("REACTORS_DB_BACKEND", "postgres").strip().lower()
SQLITE_PATH = os.getenv("REACTORS_DB_SQLITE", "data/stage2.sqlite")

# Postgres defaults per Alexis note
PG_DBNAME = os.getenv("REACTORS_PG_DBNAME", "bioreactor_db")
PG_USER = os.getenv("REACTORS_PG_USER", "")  # if empty, resolved at runtime
PG_HOST = os.getenv("REACTORS_PG_HOST", "")
PG_PORT = os.getenv("REACTORS_PG_PORT", "")
PG_PASSWORD = os.getenv("REACTORS_PG_PASSWORD", "")


# ----------------------------
# SQLite (legacy / optional)
# ----------------------------
def _sqlite_connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
    con = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def ensure_db_sqlite() -> None:
    with _sqlite_connect() as con:
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
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS calibrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                reactor TEXT NOT NULL,
                sensor TEXT NOT NULL,         -- "ph" / "do" / "biomass:415" etc
                cp INTEGER NOT NULL,          -- 1 or 2
                point REAL NOT NULL,
                input_value REAL NOT NULL,
                status TEXT NOT NULL,
                quality REAL,
                output_value REAL
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_samples_exp_ts ON samples(experiment_id, ts_utc)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_samples_tag ON samples(tag)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_cal_sensor_ts ON calibrations(reactor, sensor, ts_utc)")
        con.commit()


def create_experiment_sqlite(name: str, reactor: str, started_at_utc: str) -> int:
    with _sqlite_connect() as con:
        cur = con.execute(
            "INSERT INTO experiments (name, reactor, started_at_utc) VALUES (?, ?, ?)",
            (name, reactor, started_at_utc),
        )
        con.commit()
        return int(cur.lastrowid)


def insert_sample_sqlite(
    experiment_id: int, ts_utc: str, nodeid: str, tag: str, value: Optional[float]
) -> None:
    with _sqlite_connect() as con:
        con.execute(
            "INSERT INTO samples (experiment_id, ts_utc, nodeid, tag, value) VALUES (?, ?, ?, ?, ?)",
            (experiment_id, ts_utc, nodeid, tag, value),
        )
        con.commit()


def insert_calibration_sqlite(
    ts_utc: str,
    reactor: str,
    sensor: str,
    cp: int,
    point: float,
    input_value: float,
    status: str,
    quality: Optional[float],
    output_value: Optional[float],
) -> None:
    with _sqlite_connect() as con:
        con.execute(
            """
            INSERT INTO calibrations
            (ts_utc, reactor, sensor, cp, point, input_value, status, quality, output_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts_utc, reactor, sensor, int(cp), float(point), float(input_value), str(status), quality, output_value),
        )
        con.commit()


# ----------------------------
# PostgreSQL (primary)
# ----------------------------
def _pg_connect():
    try:
        import pwd
        import os as _os
        import psycopg
    except Exception as e:
        raise RuntimeError(
            "PostgreSQL backend selected but psycopg is not available. "
            'Install with: pip install "psycopg[binary]"'
        ) from e

    user = PG_USER or pwd.getpwuid(_os.getuid())[0]

    kwargs: dict[str, Any] = {"dbname": PG_DBNAME, "user": user}
    if PG_HOST:
        kwargs["host"] = PG_HOST
    if PG_PORT:
        kwargs["port"] = PG_PORT
    if PG_PASSWORD:
        kwargs["password"] = PG_PASSWORD

    return psycopg.connect(**kwargs)


def ensure_db_pg() -> None:
    """
    Creates tables in PostgreSQL.
    Types:
      - ts_utc: TIMESTAMPTZ
      - value: DOUBLE PRECISION
    """
    with _pg_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS experiments (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    reactor TEXT NOT NULL,
                    started_at_utc TIMESTAMPTZ NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS samples (
                    id SERIAL PRIMARY KEY,
                    experiment_id INTEGER NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
                    ts_utc TIMESTAMPTZ NOT NULL,
                    nodeid TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    value DOUBLE PRECISION
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS calibrations (
                    id SERIAL PRIMARY KEY,
                    ts_utc TIMESTAMPTZ NOT NULL,
                    reactor TEXT NOT NULL,
                    sensor TEXT NOT NULL,
                    cp SMALLINT NOT NULL CHECK (cp IN (1,2)),
                    point DOUBLE PRECISION NOT NULL,
                    input_value DOUBLE PRECISION NOT NULL,
                    status TEXT NOT NULL,
                    quality DOUBLE PRECISION,
                    output_value DOUBLE PRECISION
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_samples_exp_ts ON samples(experiment_id, ts_utc)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_samples_tag ON samples(tag)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cal_sensor_ts ON calibrations(reactor, sensor, ts_utc)")
        con.commit()


def create_experiment_pg(name: str, reactor: str, started_at_utc: str) -> int:
    with _pg_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO experiments (name, reactor, started_at_utc)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (name, reactor, started_at_utc),
            )
            exp_id = int(cur.fetchone()[0])
        con.commit()
        return exp_id


def insert_sample_pg(
    experiment_id: int, ts_utc: str, nodeid: str, tag: str, value: Optional[float]
) -> None:
    with _pg_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO samples (experiment_id, ts_utc, nodeid, tag, value)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (experiment_id, ts_utc, nodeid, tag, value),
            )
        con.commit()


def insert_calibration_pg(
    ts_utc: str,
    reactor: str,
    sensor: str,
    cp: int,
    point: float,
    input_value: float,
    status: str,
    quality: Optional[float],
    output_value: Optional[float],
) -> None:
    with _pg_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO calibrations
                (ts_utc, reactor, sensor, cp, point, input_value, status, quality, output_value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (ts_utc, reactor, sensor, int(cp), float(point), float(input_value), str(status), quality, output_value),
            )
        con.commit()


# ----------------------------
# Shared read helpers (used by UI)
# Return basic Python types (list of dict)
# ----------------------------
def list_experiments() -> list[dict[str, Any]]:
    if DB_BACKEND == "sqlite":
        try:
            with _sqlite_connect() as con:
                rows = con.execute(
                    "SELECT id, name, reactor, started_at_utc FROM experiments ORDER BY id DESC"
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    # postgres
    with _pg_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT id, name, reactor, started_at_utc FROM experiments ORDER BY id DESC"
            )
            rows = cur.fetchall()
    out = []
    for (i, name, reactor, ts) in rows:
        out.append({"id": int(i), "name": name, "reactor": reactor, "started_at_utc": str(ts)})
    return out


def list_tags(experiment_id: int) -> list[str]:
    if DB_BACKEND == "sqlite":
        try:
            with _sqlite_connect() as con:
                rows = con.execute(
                    "SELECT DISTINCT tag FROM samples WHERE experiment_id = ? ORDER BY tag",
                    (experiment_id,),
                ).fetchall()
            return [r["tag"] for r in rows if r["tag"]]
        except Exception:
            return []

    with _pg_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT tag FROM samples WHERE experiment_id = %s ORDER BY tag",
                (experiment_id,),
            )
            rows = cur.fetchall()
    return [r[0] for r in rows if r and r[0]]


def load_timeseries(experiment_id: int, tags: Sequence[str], minutes: int) -> list[dict[str, Any]]:
    """
    Returns list of dict rows: {ts_utc, tag, value}
    Filters to last N minutes.
    """
    if not tags:
        return []

    if DB_BACKEND == "sqlite":
        try:
            from datetime import datetime, timedelta, timezone
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=int(minutes))
            placeholders = ",".join(["?"] * len(tags))
            params = [experiment_id, cutoff.isoformat(), *tags]
            q = f"""
                SELECT ts_utc, tag, value
                FROM samples
                WHERE experiment_id = ?
                  AND ts_utc >= ?
                  AND tag IN ({placeholders})
                ORDER BY ts_utc ASC
            """
            with _sqlite_connect() as con:
                rows = con.execute(q, params).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    # postgres
    with _pg_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT ts_utc, tag, value
                FROM samples
                WHERE experiment_id = %s
                  AND ts_utc >= (NOW() AT TIME ZONE 'utc') - (%s || ' minutes')::interval
                  AND tag = ANY(%s)
                ORDER BY ts_utc ASC
                """,
                (experiment_id, int(minutes), list(tags)),
            )
            rows = cur.fetchall()
    return [{"ts_utc": str(ts), "tag": tag, "value": val} for (ts, tag, val) in rows]


def list_calibrations(reactor: str, sensor: str, limit: int = 50) -> list[dict[str, Any]]:
    if DB_BACKEND == "sqlite":
        try:
            with _sqlite_connect() as con:
                rows = con.execute(
                    """
                    SELECT ts_utc, reactor, sensor, cp, point, input_value, status, quality, output_value
                    FROM calibrations
                    WHERE reactor = ? AND sensor = ?
                    ORDER BY ts_utc DESC
                    LIMIT ?
                    """,
                    (reactor, sensor, int(limit)),
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    with _pg_connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT ts_utc, reactor, sensor, cp, point, input_value, status, quality, output_value
                FROM calibrations
                WHERE reactor = %s AND sensor = %s
                ORDER BY ts_utc DESC
                LIMIT %s
                """,
                (reactor, sensor, int(limit)),
            )
            rows = cur.fetchall()
    return [
        {
            "ts_utc": str(ts),
            "reactor": r,
            "sensor": s,
            "cp": int(cp),
            "point": float(point),
            "input_value": float(inp),
            "status": status,
            "quality": None if quality is None else float(quality),
            "output_value": None if outv is None else float(outv),
        }
        for (ts, r, s, cp, point, inp, status, quality, outv) in rows
    ]


# ----------------------------
# Public API (keeps old imports working)
# ----------------------------
def ensure_db() -> None:
    if DB_BACKEND == "sqlite":
        ensure_db_sqlite()
        return
    # postgres is required unless explicitly overridden
    ensure_db_pg()


def create_experiment(name: str, reactor: str, started_at_utc: str) -> int:
    if DB_BACKEND == "sqlite":
        return create_experiment_sqlite(name, reactor, started_at_utc)
    return create_experiment_pg(name, reactor, started_at_utc)


def insert_sample(experiment_id: int, ts_utc: str, nodeid: str, tag: str, value: Optional[float]) -> None:
    if DB_BACKEND == "sqlite":
        return insert_sample_sqlite(experiment_id, ts_utc, nodeid, tag, value)
    return insert_sample_pg(experiment_id, ts_utc, nodeid, tag, value)


def insert_calibration(
    ts_utc: str,
    reactor: str,
    sensor: str,
    cp: int,
    point: float,
    input_value: float,
    status: str,
    quality: Optional[float],
    output_value: Optional[float],
) -> None:
    if DB_BACKEND == "sqlite":
        return insert_calibration_sqlite(ts_utc, reactor, sensor, cp, point, input_value, status, quality, output_value)
    return insert_calibration_pg(ts_utc, reactor, sensor, cp, point, input_value, status, quality, output_value)